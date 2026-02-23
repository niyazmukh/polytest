from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .config import InvestigateConfig


_SLUG_START_TS_RE = re.compile(r"-(\d{9,12})$")
_SLUG_DURATION_RE = re.compile(r"-(\d+)m-\d{9,12}$")
_TITLE_DURATION_RE = re.compile(r"(\d+)\s*minute", re.IGNORECASE)

_BEHAVIOR_TAGS = {
    "near_50_bias",
    "last_minute_entries",
    "dual_side_under_one",
    "high_win_rate",
    "high_pnl_efficiency",
    "linear_pnl_curve",
}


@dataclass(slots=True)
class UserRawData:
    wallet: str
    profile: dict[str, Any] | None
    leaderboard_rows: list[dict[str, Any]]
    activity_rows: list[dict[str, Any]]
    closed_rows: list[dict[str, Any]]
    discovery_sources: tuple[str, ...]


@dataclass(slots=True)
class UserMetrics:
    wallet: str
    discovery_sources: tuple[str, ...]
    created_at: str | None
    account_age_hours: float | None
    last_activity_ts: int | None
    last_activity_at: str | None
    activity_bucket: str
    activity_peak_hours_utc: list[dict[str, Any]]
    total_trades: int
    total_buys: int
    total_sells: int
    buy_usdc_volume: float
    avg_buy_price: float | None
    median_buy_usdc: float | None
    near_50_buy_ratio: float
    last_minute_buy_ratio: float
    dual_side_conditions: int
    dual_side_under_one_conditions: int
    dual_side_under_one_ratio: float
    closed_positions: int
    wins: int
    losses: int
    win_rate: float | None
    realized_pnl: float
    realized_volume: float
    pnl_to_volume: float | None
    linearity_r2: float | None
    leaderboard_best_rank: int | None
    leaderboard_max_pnl: float | None
    leaderboard_max_vol: float | None
    suspicion_score: float
    tags: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _parse_trade_timestamp(row: dict[str, Any]) -> int | None:
    return _safe_int(row.get("timestamp"))


def is_crypto_row(row: dict[str, Any], keywords: Iterable[str]) -> bool:
    text = " ".join(
        str(row.get(k, "")).lower()
        for k in ("slug", "eventSlug", "title", "name")
    )
    return any(keyword in text for keyword in keywords)


def _infer_duration_seconds(row: dict[str, Any]) -> int | None:
    slug = str(row.get("slug", ""))
    title = str(row.get("title", ""))
    match = _SLUG_DURATION_RE.search(slug)
    if match:
        mins = _safe_int(match.group(1))
        if mins and mins > 0:
            return mins * 60
    match = _TITLE_DURATION_RE.search(title)
    if match:
        mins = _safe_int(match.group(1))
        if mins and mins > 0:
            return mins * 60
    return None


def _infer_market_start_ts(row: dict[str, Any]) -> int | None:
    slug = str(row.get("slug", ""))
    match = _SLUG_START_TS_RE.search(slug)
    if not match:
        return None
    start_ts = _safe_int(match.group(1))
    if not start_ts:
        return None
    return start_ts


def _seconds_to_market_close(row: dict[str, Any]) -> float | None:
    trade_ts = _parse_trade_timestamp(row)
    start_ts = _infer_market_start_ts(row)
    duration = _infer_duration_seconds(row)
    if trade_ts is None or start_ts is None or duration is None:
        return None
    return float(start_ts + duration - trade_ts)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    idx = max(0.0, min(1.0, q)) * (len(ordered) - 1)
    low = int(math.floor(idx))
    high = int(math.ceil(idx))
    if low == high:
        return ordered[low]
    frac = idx - low
    return ordered[low] * (1.0 - frac) + ordered[high] * frac


def _linearity_r2(points: list[tuple[int, float]]) -> float | None:
    if len(points) < 3:
        return None
    xs = [float(ts) for ts, _ in points]
    ys = [float(v) for _, v in points]
    min_x = min(xs)
    max_x = max(xs)
    span = max_x - min_x
    if span <= 0:
        return None
    norm_xs = [(x - min_x) / span for x in xs]
    mean_x = sum(norm_xs) / len(norm_xs)
    mean_y = sum(ys) / len(ys)
    ss_xx = sum((x - mean_x) ** 2 for x in norm_xs)
    if ss_xx <= 0:
        return None
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(norm_xs, ys)) / ss_xx
    intercept = mean_y - slope * mean_x
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    if ss_tot <= 1e-12:
        return 1.0
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(norm_xs, ys))
    r2 = 1.0 - (ss_res / ss_tot)
    return max(0.0, min(1.0, r2))


def _activity_bucket(now_ts: int, last_ts: int | None) -> str:
    if last_ts is None:
        return "no_activity"
    delta = max(0, now_ts - last_ts)
    if delta <= 3600:
        return "active_1h"
    if delta <= 4 * 3600:
        return "active_4h"
    if delta <= 12 * 3600:
        return "active_12h"
    if delta <= 24 * 3600:
        return "active_24h"
    return "stale_24h_plus"


def _top_hourly_peaks(trade_rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    counts: Counter[int] = Counter()
    for row in trade_rows:
        ts = _parse_trade_timestamp(row)
        if ts is None:
            continue
        hour_start = ts - (ts % 3600)
        counts[hour_start] += 1
    out = []
    for hour_start, count in counts.most_common(limit):
        out.append(
            {
                "hour_start_utc": _to_iso(hour_start),
                "count": int(count),
            },
        )
    return out


def _compute_suspicion_score(
    *,
    cfg: InvestigateConfig,
    account_age_hours: float | None,
    near_50_buy_ratio: float,
    last_minute_buy_ratio: float,
    dual_side_under_one_ratio: float,
    win_rate: float | None,
    pnl_to_volume: float | None,
    linearity_r2: float | None,
) -> float:
    score = 0.0
    if account_age_hours is not None:
        age_factor = max(0.0, min(1.0, (cfg.new_account_hours - account_age_hours) / cfg.new_account_hours))
        score += 0.2 * age_factor
    score += 0.12 * max(0.0, min(1.0, near_50_buy_ratio))
    score += 0.12 * max(0.0, min(1.0, last_minute_buy_ratio))
    score += 0.08 * max(0.0, min(1.0, dual_side_under_one_ratio))
    if win_rate is not None:
        score += 0.2 * max(0.0, min(1.0, (win_rate - 0.5) / 0.5))
    if pnl_to_volume is not None:
        baseline = max(1e-9, cfg.min_pnl_to_volume_tag)
        score += 0.16 * max(0.0, min(1.0, pnl_to_volume / baseline))
    if linearity_r2 is not None:
        score += 0.12 * max(0.0, min(1.0, (linearity_r2 - 0.7) / 0.3))
    return max(0.0, min(1.0, score))


def analyze_user(data: UserRawData, cfg: InvestigateConfig, now_ts: int) -> UserMetrics:
    trades = sorted(
        (row for row in data.activity_rows if _parse_trade_timestamp(row) is not None),
        key=lambda row: int(row["timestamp"]),
    )
    buys = [row for row in trades if str(row.get("side", "")).upper() == "BUY"]
    sells = [row for row in trades if str(row.get("side", "")).upper() == "SELL"]

    buy_prices = [_safe_float(row.get("price")) for row in buys]
    buy_prices = [v for v in buy_prices if v is not None]

    buy_usdc_sizes = []
    for row in buys:
        usdc = _safe_float(row.get("usdcSize"))
        if usdc is None:
            size = _safe_float(row.get("size"))
            price = _safe_float(row.get("price"))
            if size is not None and price is not None:
                usdc = size * price
        if usdc is not None:
            buy_usdc_sizes.append(max(0.0, usdc))

    near_50_count = sum(1 for p in buy_prices if abs(p - 0.5) <= cfg.near_fifty_band)
    near_50_buy_ratio = (near_50_count / len(buy_prices)) if buy_prices else 0.0

    close_times: list[float] = []
    for row in buys:
        seconds_to_close = _seconds_to_market_close(row)
        if seconds_to_close is None:
            continue
        if seconds_to_close < 0:
            continue
        close_times.append(seconds_to_close)
    last_minute_count = sum(1 for v in close_times if v <= cfg.last_minute_threshold_seconds)
    last_minute_buy_ratio = (last_minute_count / len(close_times)) if close_times else 0.0

    # Detect dual-side binary behavior per condition.
    per_condition_outcome: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    for row in buys:
        condition_id = str(row.get("conditionId", "")).strip()
        outcome = str(row.get("outcome", "")).strip().lower()
        price = _safe_float(row.get("price"))
        if not condition_id or not outcome or price is None:
            continue
        weight = _safe_float(row.get("usdcSize"))
        if weight is None:
            size = _safe_float(row.get("size"))
            weight = size if size is not None else 1.0
        per_condition_outcome[condition_id][outcome].append((price, max(1e-9, weight)))

    dual_side_conditions = 0
    dual_side_under_one_conditions = 0
    for per_outcome in per_condition_outcome.values():
        if len(per_outcome) < 2:
            continue
        dual_side_conditions += 1
        avg_prices: list[float] = []
        for price_weights in per_outcome.values():
            denom = sum(weight for _, weight in price_weights)
            if denom <= 0:
                continue
            avg = sum(price * weight for price, weight in price_weights) / denom
            avg_prices.append(avg)
        if len(avg_prices) < 2:
            continue
        pair_cost = sum(sorted(avg_prices)[:2])
        if pair_cost < (1.0 - cfg.pair_under_one_margin):
            dual_side_under_one_conditions += 1
    dual_side_under_one_ratio = (
        dual_side_under_one_conditions / dual_side_conditions
        if dual_side_conditions > 0
        else 0.0
    )

    created_dt = _parse_iso_datetime((data.profile or {}).get("createdAt"))
    account_age_hours = None
    if created_dt is not None:
        account_age_hours = max(0.0, (now_ts - int(created_dt.timestamp())) / 3600.0)

    last_activity_ts = _parse_trade_timestamp(trades[-1]) if trades else None
    activity_bucket = _activity_bucket(now_ts, last_activity_ts)

    closed_positions = [
        row
        for row in data.closed_rows
        if _safe_float(row.get("realizedPnl")) is not None
    ]
    wins = losses = 0
    realized_pnl = 0.0
    realized_volume = 0.0
    pnl_points: list[tuple[int, float]] = []
    for row in sorted(closed_positions, key=lambda item: _safe_int(item.get("timestamp")) or 0):
        pnl = _safe_float(row.get("realizedPnl")) or 0.0
        bought = _safe_float(row.get("totalBought")) or 0.0
        ts = _safe_int(row.get("timestamp"))
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        realized_pnl += pnl
        realized_volume += max(0.0, bought)
        if ts is not None:
            pnl_points.append((ts, pnl))
    outcome_count = wins + losses
    win_rate = (wins / outcome_count) if outcome_count > 0 else None
    pnl_to_volume = (realized_pnl / realized_volume) if realized_volume > 0 else None

    cumulative = 0.0
    cumulative_points: list[tuple[int, float]] = []
    for ts, pnl in pnl_points:
        cumulative += pnl
        cumulative_points.append((ts, cumulative))
    linearity_r2 = _linearity_r2(cumulative_points)

    leaderboard_ranks = [_safe_int(row.get("rank")) for row in data.leaderboard_rows]
    leaderboard_pnls = [_safe_float(row.get("pnl")) for row in data.leaderboard_rows]
    leaderboard_vols = [_safe_float(row.get("vol")) for row in data.leaderboard_rows]
    leaderboard_best_rank = min(v for v in leaderboard_ranks if v is not None) if any(v is not None for v in leaderboard_ranks) else None
    leaderboard_max_pnl = max(v for v in leaderboard_pnls if v is not None) if any(v is not None for v in leaderboard_pnls) else None
    leaderboard_max_vol = max(v for v in leaderboard_vols if v is not None) if any(v is not None for v in leaderboard_vols) else None

    suspicion_score = _compute_suspicion_score(
        cfg=cfg,
        account_age_hours=account_age_hours,
        near_50_buy_ratio=near_50_buy_ratio,
        last_minute_buy_ratio=last_minute_buy_ratio,
        dual_side_under_one_ratio=dual_side_under_one_ratio,
        win_rate=win_rate,
        pnl_to_volume=pnl_to_volume,
        linearity_r2=linearity_r2,
    )

    tags: list[str] = [activity_bucket]
    if account_age_hours is not None and account_age_hours <= cfg.new_account_hours:
        tags.append("new_wallet")
    if near_50_buy_ratio >= 0.35:
        tags.append("near_50_bias")
    if last_minute_buy_ratio >= 0.25:
        tags.append("last_minute_entries")
    if dual_side_under_one_ratio >= 0.35 or dual_side_under_one_conditions >= 3:
        tags.append("dual_side_under_one")
    if win_rate is not None and win_rate >= cfg.min_win_rate_tag:
        tags.append("high_win_rate")
    if pnl_to_volume is not None and pnl_to_volume >= cfg.min_pnl_to_volume_tag:
        tags.append("high_pnl_efficiency")
    if linearity_r2 is not None and linearity_r2 >= cfg.min_linearity_r2_tag:
        tags.append("linear_pnl_curve")

    return UserMetrics(
        wallet=data.wallet,
        discovery_sources=data.discovery_sources,
        created_at=created_dt.isoformat() if created_dt else None,
        account_age_hours=account_age_hours,
        last_activity_ts=last_activity_ts,
        last_activity_at=_to_iso(last_activity_ts),
        activity_bucket=activity_bucket,
        activity_peak_hours_utc=_top_hourly_peaks(trades),
        total_trades=len(trades),
        total_buys=len(buys),
        total_sells=len(sells),
        buy_usdc_volume=sum(buy_usdc_sizes),
        avg_buy_price=(sum(buy_prices) / len(buy_prices)) if buy_prices else None,
        median_buy_usdc=_percentile(buy_usdc_sizes, 0.5),
        near_50_buy_ratio=near_50_buy_ratio,
        last_minute_buy_ratio=last_minute_buy_ratio,
        dual_side_conditions=dual_side_conditions,
        dual_side_under_one_conditions=dual_side_under_one_conditions,
        dual_side_under_one_ratio=dual_side_under_one_ratio,
        closed_positions=len(closed_positions),
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        realized_pnl=realized_pnl,
        realized_volume=realized_volume,
        pnl_to_volume=pnl_to_volume,
        linearity_r2=linearity_r2,
        leaderboard_best_rank=leaderboard_best_rank,
        leaderboard_max_pnl=leaderboard_max_pnl,
        leaderboard_max_vol=leaderboard_max_vol,
        suspicion_score=suspicion_score,
        tags=sorted(set(tags)),
    )


def _metric_value(metric: UserMetrics, field: str) -> float:
    if field == "near_50_buy_ratio":
        return float(metric.near_50_buy_ratio)
    if field == "last_minute_buy_ratio":
        return float(metric.last_minute_buy_ratio)
    if field == "dual_side_under_one_ratio":
        return float(metric.dual_side_under_one_ratio)
    if field == "win_rate":
        return float(metric.win_rate if metric.win_rate is not None else 0.5)
    if field == "pnl_to_volume":
        return float(metric.pnl_to_volume if metric.pnl_to_volume is not None else 0.0)
    if field == "linearity_r2":
        return float(metric.linearity_r2 if metric.linearity_r2 is not None else 0.0)
    if field == "avg_buy_price":
        return float(metric.avg_buy_price if metric.avg_buy_price is not None else 0.5)
    if field == "median_buy_usdc_log":
        val = metric.median_buy_usdc if metric.median_buy_usdc is not None else 0.0
        return math.log1p(max(0.0, val))
    if field == "account_age_hours_log":
        age = metric.account_age_hours if metric.account_age_hours is not None else 9999.0
        return math.log1p(max(0.0, min(age, 24 * 365)))
    raise KeyError(field)


class _UnionFind:
    def __init__(self, values: list[str]) -> None:
        self.parent = {v: v for v in values}
        self.rank = {v: 0 for v in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        rank_a = self.rank[ra]
        rank_b = self.rank[rb]
        if rank_a < rank_b:
            self.parent[ra] = rb
            return
        if rank_a > rank_b:
            self.parent[rb] = ra
            return
        self.parent[rb] = ra
        self.rank[ra] += 1


def build_similarity_groups(
    metrics: list[UserMetrics],
    cfg: InvestigateConfig,
) -> list[dict[str, Any]]:
    min_group_score = max(0.2, cfg.min_suspicion_score * 0.6)
    candidates = [
        row
        for row in metrics
        if row.suspicion_score >= min_group_score and _BEHAVIOR_TAGS.intersection(row.tags)
    ]
    if len(candidates) < cfg.cluster_min_size:
        return []
    fields = [
        "near_50_buy_ratio",
        "last_minute_buy_ratio",
        "dual_side_under_one_ratio",
        "win_rate",
        "pnl_to_volume",
        "linearity_r2",
        "avg_buy_price",
        "median_buy_usdc_log",
        "account_age_hours_log",
    ]
    values_by_field: dict[str, list[float]] = {field: [] for field in fields}
    for item in candidates:
        for field in fields:
            values_by_field[field].append(_metric_value(item, field))

    means = {field: (sum(values) / len(values)) for field, values in values_by_field.items()}
    stds = {}
    for field, values in values_by_field.items():
        mean = means[field]
        var = sum((v - mean) ** 2 for v in values) / max(1, len(values))
        std = math.sqrt(var)
        stds[field] = std if std > 1e-9 else 1.0

    normalized: dict[str, dict[str, float]] = {}
    for item in candidates:
        vec = {}
        for field in fields:
            vec[field] = (_metric_value(item, field) - means[field]) / stds[field]
        normalized[item.wallet] = vec

    by_wallet = {item.wallet: item for item in candidates}
    uf = _UnionFind(list(by_wallet))
    wallets = list(by_wallet)
    for i in range(len(wallets)):
        wa = wallets[i]
        tags_a = _BEHAVIOR_TAGS.intersection(by_wallet[wa].tags)
        for j in range(i + 1, len(wallets)):
            wb = wallets[j]
            tags_b = _BEHAVIOR_TAGS.intersection(by_wallet[wb].tags)
            shared_behavior = tags_a.intersection(tags_b)
            if not shared_behavior:
                continue
            if len(shared_behavior) == 1 and not shared_behavior.intersection(
                {"near_50_bias", "last_minute_entries", "dual_side_under_one"},
            ):
                continue
            vec_a = normalized[wa]
            vec_b = normalized[wb]
            sq = 0.0
            for field in fields:
                sq += (vec_a[field] - vec_b[field]) ** 2
            distance = math.sqrt(sq / len(fields))
            if distance <= cfg.cluster_distance_threshold:
                uf.union(wa, wb)

    groups_map: dict[str, list[UserMetrics]] = defaultdict(list)
    for wallet in wallets:
        groups_map[uf.find(wallet)].append(by_wallet[wallet])

    groups: list[dict[str, Any]] = []
    next_id = 1
    for members in groups_map.values():
        if len(members) < cfg.cluster_min_size:
            continue
        tags_sets = [set(member.tags) for member in members]
        behavior_sets = [set(member.tags).intersection(_BEHAVIOR_TAGS) for member in members]
        common_behavior = sorted(set.intersection(*behavior_sets)) if behavior_sets else []
        if not common_behavior:
            continue
        common_tags = sorted(set.intersection(*tags_sets)) if tags_sets else []
        groups.append(
            {
                "group_id": f"G{next_id:03d}",
                "size": len(members),
                "wallets": sorted(member.wallet for member in members),
                "avg_suspicion_score": sum(member.suspicion_score for member in members) / len(members),
                "common_tags": common_tags if common_tags else common_behavior,
            },
        )
        next_id += 1
    groups.sort(key=lambda item: (-item["avg_suspicion_score"], -item["size"], item["group_id"]))
    return groups
