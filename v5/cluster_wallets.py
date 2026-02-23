from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .polymarket_api import ApiError, PolymarketApi


ASSETS = ("btc", "eth", "sol", "xrp", "other", "mixed")
TIMEFRAMES = ("5m", "15m", "30m", "1h", "4h", "other", "mixed")
ACTIVITY_BUCKETS = ("active_1h", "active_4h", "active_12h", "active_24h", "stale_24h_plus", "no_activity")
STRATEGIES = ("near_50_bias", "last_minute_entries", "dual_side_under_one")


@dataclass(slots=True)
class WalletClusterRecord:
    wallet: str
    cluster_id: int
    activity_status: str
    dominant_asset: str
    dominant_timeframe: str
    strategy_tags: list[str]
    win_rate_24h: float | None
    pnl_24h: float
    volume_24h: float
    pnl_to_volume_24h: float | None
    win_rate_total: float | None
    pnl_to_volume_total: float | None
    realized_pnl_total: float
    total_trades: int
    closed_positions: int
    suspicion_score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "cluster_id": self.cluster_id,
            "activity_status": self.activity_status,
            "dominant_asset": self.dominant_asset,
            "dominant_timeframe": self.dominant_timeframe,
            "strategy_tags": self.strategy_tags,
            "win_rate_24h": self.win_rate_24h,
            "pnl_24h": self.pnl_24h,
            "volume_24h": self.volume_24h,
            "pnl_to_volume_24h": self.pnl_to_volume_24h,
            "win_rate_total": self.win_rate_total,
            "pnl_to_volume_total": self.pnl_to_volume_total,
            "realized_pnl_total": self.realized_pnl_total,
            "total_trades": self.total_trades,
            "closed_positions": self.closed_positions,
            "suspicion_score": self.suspicion_score,
        }


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


def _detect_asset(text: str) -> str:
    t = text.lower()
    if "btc" in t or "bitcoin" in t:
        return "btc"
    if "eth" in t or "ethereum" in t:
        return "eth"
    if "sol" in t or "solana" in t:
        return "sol"
    if "xrp" in t:
        return "xrp"
    return "other"


def _detect_timeframe(text: str) -> str:
    t = text.lower()
    if "-5m-" in t or " 5 minute" in t:
        return "5m"
    if "-15m-" in t or " 15 minute" in t:
        return "15m"
    if "-30m-" in t or " 30 minute" in t:
        return "30m"
    if "-1h-" in t or " 1 hour" in t:
        return "1h"
    if "-4h-" in t or " 4 hour" in t:
        return "4h"
    return "other"


def _dominant_or_mixed(counter: Counter[str], allowed: tuple[str, ...], mixed_label: str) -> str:
    if not counter:
        return "other"
    items = [(k, v) for k, v in counter.items() if k in allowed]
    if not items:
        return "other"
    items.sort(key=lambda kv: kv[1], reverse=True)
    if len(items) >= 2 and items[1][1] > 0 and items[0][1] <= items[1][1] * 1.25:
        return mixed_label
    return items[0][0]


def _one_hot(value: str, options: tuple[str, ...]) -> list[float]:
    return [1.0 if value == item else 0.0 for item in options]


def _kmeans(points: list[list[float]], k: int, seed: int = 42, iters: int = 80) -> list[int]:
    n = len(points)
    if n == 0:
        return []
    if k <= 1 or n == 1:
        return [0] * n
    k = min(k, n)
    random.seed(seed)

    # kmeans++ init
    centers = [points[random.randrange(n)][:]]
    while len(centers) < k:
        d2 = []
        for p in points:
            best = min(sum((p[i] - c[i]) ** 2 for i in range(len(p))) for c in centers)
            d2.append(best)
        total = sum(d2)
        if total <= 0:
            centers.append(points[random.randrange(n)][:])
            continue
        pick = random.random() * total
        acc = 0.0
        idx = 0
        for i, val in enumerate(d2):
            acc += val
            if acc >= pick:
                idx = i
                break
        centers.append(points[idx][:])

    labels = [0] * n
    for _ in range(iters):
        changed = False
        groups: list[list[int]] = [[] for _ in range(k)]
        for i, p in enumerate(points):
            label = min(range(k), key=lambda c: sum((p[d] - centers[c][d]) ** 2 for d in range(len(p))))
            if labels[i] != label:
                changed = True
            labels[i] = label
            groups[label].append(i)
        new_centers = []
        for c in range(k):
            if not groups[c]:
                new_centers.append(points[random.randrange(n)][:])
                continue
            members = groups[c]
            dim = len(points[0])
            center = [sum(points[i][d] for i in members) / len(members) for d in range(dim)]
            new_centers.append(center)
        centers = new_centers
        if not changed:
            break
    return labels


def _std_scale(rows: list[list[float]]) -> list[list[float]]:
    if not rows:
        return rows
    dim = len(rows[0])
    means = [sum(row[d] for row in rows) / len(rows) for d in range(dim)]
    stds = []
    for d in range(dim):
        var = sum((row[d] - means[d]) ** 2 for row in rows) / len(rows)
        std = math.sqrt(var)
        stds.append(std if std > 1e-9 else 1.0)
    return [[(row[d] - means[d]) / stds[d] for d in range(dim)] for row in rows]


def _timestamp_to_iso(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _fetch_wallet_enrichment(
    api: PolymarketApi,
    wallet: str,
    now_ts: int,
) -> dict[str, Any]:
    activity_rows: list[dict[str, Any]] = []
    closed_rows: list[dict[str, Any]] = []

    try:
        activity_rows = api.get_activity(user=wallet, limit=1000, offset=0, activity_type="TRADE")
    except ApiError:
        activity_rows = []
    try:
        closed_rows = api.get_closed_positions(user=wallet, limit=500, offset=0)
    except ApiError:
        closed_rows = []

    cutoff_ts = now_ts - 24 * 3600

    asset_counter: Counter[str] = Counter()
    timeframe_counter: Counter[str] = Counter()
    for row in activity_rows:
        slug = str(row.get("slug", ""))
        title = str(row.get("title", ""))
        text = f"{slug} {title}"
        asset_counter[_detect_asset(text)] += 1
        timeframe_counter[_detect_timeframe(text)] += 1

    dominant_asset = _dominant_or_mixed(asset_counter, ASSETS, "mixed")
    dominant_timeframe = _dominant_or_mixed(timeframe_counter, TIMEFRAMES, "mixed")
    last_trade_ts = None
    ts_values: list[int] = []
    for row in activity_rows:
        ts = _safe_int(row.get("timestamp"))
        if ts is not None:
            ts_values.append(ts)
    if ts_values:
        last_trade_ts = max(ts_values)

    if last_trade_ts is None:
        activity_status_live = "no_activity"
    else:
        delta = max(0, now_ts - last_trade_ts)
        if delta <= 3600:
            activity_status_live = "active_1h"
        elif delta <= 4 * 3600:
            activity_status_live = "active_4h"
        elif delta <= 12 * 3600:
            activity_status_live = "active_12h"
        elif delta <= 24 * 3600:
            activity_status_live = "active_24h"
        else:
            activity_status_live = "stale_24h_plus"

    wins = losses = 0
    pnl_24h = 0.0
    vol_24h = 0.0
    for row in closed_rows:
        ts = _safe_int(row.get("timestamp"))
        if ts is None or ts < cutoff_ts:
            continue
        pnl = _safe_float(row.get("realizedPnl")) or 0.0
        vol = _safe_float(row.get("totalBought")) or 0.0
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        pnl_24h += pnl
        vol_24h += max(0.0, vol)

    outcomes = wins + losses
    win_rate_24h = (wins / outcomes) if outcomes > 0 else None
    pnl_to_volume_24h = (pnl_24h / vol_24h) if vol_24h > 0 else None

    return {
        "wallet": wallet,
        "dominant_asset": dominant_asset,
        "dominant_timeframe": dominant_timeframe,
        "activity_status_live": activity_status_live,
        "last_trade_ts": last_trade_ts,
        "last_trade_at_utc": _timestamp_to_iso(last_trade_ts),
        "asset_counts": dict(asset_counter),
        "timeframe_counts": dict(timeframe_counter),
        "win_rate_24h": win_rate_24h,
        "pnl_24h": pnl_24h,
        "volume_24h": vol_24h,
        "pnl_to_volume_24h": pnl_to_volume_24h,
        "closed_rows_24h": outcomes,
    }


def _build_html(
    out_path: Path,
    records: list[WalletClusterRecord],
    cluster_summaries: list[dict[str, Any]],
) -> None:
    width = 1200
    height = 640
    pad_left = 70
    pad_bottom = 45
    pad_top = 20
    pad_right = 20
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    xs = []
    ys = []
    for row in records:
        x = row.pnl_to_volume_24h
        y = row.win_rate_24h
        if x is None:
            x = row.pnl_to_volume_total if row.pnl_to_volume_total is not None else 0.0
        if y is None:
            y = row.win_rate_total if row.win_rate_total is not None else 0.5
        x = max(-1.0, min(1.0, x))
        y = max(0.0, min(1.0, y))
        xs.append(x)
        ys.append(y)

    min_x = -1.0
    max_x = 1.0
    min_y = 0.0
    max_y = 1.0

    palette = [
        "#e63946",
        "#f4a261",
        "#2a9d8f",
        "#264653",
        "#457b9d",
        "#8d99ae",
        "#ffb703",
        "#9b5de5",
        "#3a86ff",
        "#06d6a0",
        "#ef476f",
        "#118ab2",
        "#8338ec",
        "#ff006e",
        "#2b9348",
        "#6a4c93",
    ]

    def map_x(x: float) -> float:
        return pad_left + (x - min_x) / (max_x - min_x) * plot_w

    def map_y(y: float) -> float:
        return pad_top + (1.0 - (y - min_y) / (max_y - min_y)) * plot_h

    circles = []
    for row, x, y in zip(records, xs, ys):
        cx = map_x(x)
        cy = map_y(y)
        color = palette[row.cluster_id % len(palette)]
        title = (
            f"wallet={row.wallet} cluster=C{row.cluster_id:02d} "
            f"status={row.activity_status} asset={row.dominant_asset} tf={row.dominant_timeframe} "
            f"win24={row.win_rate_24h} pnl24={row.pnl_24h:.4f} p2v24={row.pnl_to_volume_24h}"
        )
        circles.append(
            f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="3.4" fill="{color}" fill-opacity="0.72">'
            f"<title>{title}</title></circle>",
        )

    bars = []
    bar_w = 28
    bar_gap = 10
    chart_h = 260
    max_size = max((item["size"] for item in cluster_summaries), default=1)
    for idx, item in enumerate(cluster_summaries):
        h = (item["size"] / max_size) * (chart_h - 24)
        x = 40 + idx * (bar_w + bar_gap)
        y = chart_h - h
        color = palette[item["cluster_id"] % len(palette)]
        bars.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w}" height="{h:.2f}" fill="{color}">'
            f'<title>C{item["cluster_id"]:02d} size={item["size"]}</title></rect>'
            f'<text x="{x + bar_w / 2:.2f}" y="{chart_h + 14}" font-size="10" text-anchor="middle">C{item["cluster_id"]:02d}</text>'
            f'<text x="{x + bar_w / 2:.2f}" y="{y - 4:.2f}" font-size="10" text-anchor="middle">{item["size"]}</text>',
        )

    rows_html = []
    for item in cluster_summaries:
        rows_html.append(
            "<tr>"
            f"<td>C{item['cluster_id']:02d}</td>"
            f"<td>{item['size']}</td>"
            f"<td>{item['activity_top']}</td>"
            f"<td>{item['asset_top']}</td>"
            f"<td>{item['timeframe_top']}</td>"
            f"<td>{item['strategy_top']}</td>"
            f"<td>{item['median_win_rate_24h']}</td>"
            f"<td>{item['median_pnl_24h']}</td>"
            f"<td>{item['median_p2v_24h']}</td>"
            f"<td>{item['median_p2v_total']}</td>"
            "</tr>",
        )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>v5 Wallet Clusters</title>
  <style>
    body {{ font-family: Arial, Helvetica, sans-serif; margin: 20px; color: #1b263b; }}
    h1 {{ margin: 0 0 4px 0; }}
    .sub {{ color: #475569; margin-bottom: 16px; }}
    .panel {{ border: 1px solid #cbd5e1; border-radius: 8px; padding: 10px; margin-bottom: 16px; background: #fff; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border: 1px solid #dbeafe; padding: 6px 8px; text-align: left; }}
    th {{ background: #eff6ff; }}
    .axis {{ stroke: #64748b; stroke-width: 1; }}
    .grid {{ stroke: #e2e8f0; stroke-width: 1; }}
  </style>
</head>
<body>
  <h1>Wallet Clustering Report</h1>
  <div class="sub">Scatter: x = pnl_to_volume_24h (fallback total), y = win_rate_24h (fallback total), color = cluster.</div>

  <div class="panel">
    <svg width="{width}" height="{height}">
      <line class="axis" x1="{pad_left}" y1="{pad_top + plot_h}" x2="{pad_left + plot_w}" y2="{pad_top + plot_h}" />
      <line class="axis" x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{pad_top + plot_h}" />
      <line class="grid" x1="{map_x(0):.2f}" y1="{pad_top}" x2="{map_x(0):.2f}" y2="{pad_top + plot_h}" />
      <line class="grid" x1="{pad_left}" y1="{map_y(0.5):.2f}" x2="{pad_left + plot_w}" y2="{map_y(0.5):.2f}" />
      <text x="{pad_left + plot_w - 180}" y="{pad_top + plot_h - 8}" font-size="12">pnl_to_volume_24h</text>
      <text x="{pad_left + 8}" y="{pad_top + 16}" font-size="12">win_rate_24h</text>
      {''.join(circles)}
    </svg>
  </div>

  <div class="panel">
    <h3>Cluster Sizes</h3>
    <svg width="1180" height="290">
      {''.join(bars)}
    </svg>
  </div>

  <div class="panel">
    <h3>Cluster Summary Table</h3>
    <table>
      <thead>
        <tr>
          <th>Cluster</th>
          <th>Size</th>
          <th>Top Activity</th>
          <th>Top Asset</th>
          <th>Top Timeframe</th>
          <th>Top Strategy</th>
          <th>Median Win24</th>
          <th>Median P/L 24h</th>
          <th>Median PnL/Vol 24h</th>
          <th>Median PnL/Vol Total</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    n = len(vals)
    if n % 2 == 1:
        return vals[n // 2]
    return (vals[n // 2 - 1] + vals[n // 2]) / 2.0


def _summarize_cluster(records: list[WalletClusterRecord], cluster_id: int) -> dict[str, Any]:
    size = len(records)
    activity = Counter(r.activity_status for r in records)
    assets = Counter(r.dominant_asset for r in records)
    timeframes = Counter(r.dominant_timeframe for r in records)
    strategies = Counter(tag for r in records for tag in r.strategy_tags)
    win24 = [r.win_rate_24h for r in records if r.win_rate_24h is not None]
    pnl24 = [r.pnl_24h for r in records]
    p2v24 = [r.pnl_to_volume_24h for r in records if r.pnl_to_volume_24h is not None]
    p2v_total = [r.pnl_to_volume_total for r in records if r.pnl_to_volume_total is not None]

    top_wallets = sorted(records, key=lambda r: r.suspicion_score, reverse=True)[:25]
    return {
        "cluster_id": cluster_id,
        "size": size,
        "activity_counts": dict(activity),
        "asset_counts": dict(assets),
        "timeframe_counts": dict(timeframes),
        "strategy_counts": dict(strategies),
        "activity_top": activity.most_common(1)[0][0] if activity else None,
        "asset_top": assets.most_common(1)[0][0] if assets else None,
        "timeframe_top": timeframes.most_common(1)[0][0] if timeframes else None,
        "strategy_top": strategies.most_common(1)[0][0] if strategies else None,
        "median_win_rate_24h": _median([float(v) for v in win24]),
        "median_pnl_24h": _median([float(v) for v in pnl24]),
        "median_p2v_24h": _median([float(v) for v in p2v24]),
        "median_p2v_total": _median([float(v) for v in p2v_total]),
        "top_wallets": [r.wallet for r in top_wallets],
    }


def run(
    input_report_path: Path,
    output_prefix: str,
    clusters_k: int,
    request_rps: float,
    request_retries: int,
    timeout_seconds: float,
    workers: int,
) -> dict[str, Any]:
    report = json.loads(input_report_path.read_text(encoding="utf-8"))
    users = report.get("users", [])
    wallets = [str(u.get("wallet", "")).lower() for u in users if str(u.get("wallet", "")).startswith("0x")]
    wallets = sorted(set(wallets))
    user_map = {str(u.get("wallet", "")).lower(): u for u in users}

    now_ts = int(time.time())
    api = PolymarketApi(timeout_seconds=timeout_seconds, retries=request_retries, rps=request_rps)

    enrich: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_wallet_enrichment, api, wallet, now_ts): wallet for wallet in wallets}
        total = len(futures)
        done = 0
        for fut in as_completed(futures):
            wallet = futures[fut]
            done += 1
            try:
                enrich[wallet] = fut.result()
            except Exception as err:
                failures.append({"wallet": wallet, "error": str(err)})
            if done % 100 == 0 or done == total:
                print(f"[v5-cluster] fetched {done}/{total}")

    records: list[WalletClusterRecord] = []
    vectors: list[list[float]] = []

    for wallet in wallets:
        u = user_map.get(wallet, {})
        e = enrich.get(wallet, {})

        activity_status = str(e.get("activity_status_live") or u.get("activity_bucket", "no_activity"))
        if activity_status not in ACTIVITY_BUCKETS:
            activity_status = "no_activity"
        dominant_asset = str(e.get("dominant_asset", "other"))
        if dominant_asset not in ASSETS:
            dominant_asset = "other"
        dominant_timeframe = str(e.get("dominant_timeframe", "other"))
        if dominant_timeframe not in TIMEFRAMES:
            dominant_timeframe = "other"

        tags = set(u.get("tags", []))
        strategy_tags = sorted(tag for tag in STRATEGIES if tag in tags)

        win_rate_24h = _safe_float(e.get("win_rate_24h"))
        pnl_24h = _safe_float(e.get("pnl_24h")) or 0.0
        volume_24h = _safe_float(e.get("volume_24h")) or 0.0
        pnl_to_volume_24h = _safe_float(e.get("pnl_to_volume_24h"))

        win_rate_total = _safe_float(u.get("win_rate"))
        pnl_to_volume_total = _safe_float(u.get("pnl_to_volume"))
        realized_pnl_total = _safe_float(u.get("realized_pnl")) or 0.0
        total_trades = _safe_int(u.get("total_trades")) or 0
        closed_positions = _safe_int(u.get("closed_positions")) or 0
        suspicion_score = _safe_float(u.get("suspicion_score")) or 0.0

        rec = WalletClusterRecord(
            wallet=wallet,
            cluster_id=-1,
            activity_status=activity_status,
            dominant_asset=dominant_asset,
            dominant_timeframe=dominant_timeframe,
            strategy_tags=strategy_tags,
            win_rate_24h=win_rate_24h,
            pnl_24h=pnl_24h,
            volume_24h=volume_24h,
            pnl_to_volume_24h=pnl_to_volume_24h,
            win_rate_total=win_rate_total,
            pnl_to_volume_total=pnl_to_volume_total,
            realized_pnl_total=realized_pnl_total,
            total_trades=total_trades,
            closed_positions=closed_positions,
            suspicion_score=suspicion_score,
        )
        records.append(rec)

        # Feature vector
        vec: list[float] = []
        vec.extend(_one_hot(activity_status, ACTIVITY_BUCKETS))
        vec.extend(_one_hot(dominant_asset, ASSETS))
        vec.extend(_one_hot(dominant_timeframe, TIMEFRAMES))
        vec.extend([1.0 if tag in strategy_tags else 0.0 for tag in STRATEGIES])
        vec.append(win_rate_24h if win_rate_24h is not None else (win_rate_total if win_rate_total is not None else 0.5))
        vec.append(max(-1.0, min(1.0, pnl_to_volume_24h if pnl_to_volume_24h is not None else 0.0)))
        vec.append(max(-1.0, min(1.0, pnl_to_volume_total if pnl_to_volume_total is not None else 0.0)))
        vec.append(math.copysign(math.log1p(abs(pnl_24h)), pnl_24h))
        vec.append(math.copysign(math.log1p(abs(realized_pnl_total)), realized_pnl_total))
        vec.append(math.log1p(max(0.0, total_trades)))
        vec.append(math.log1p(max(0.0, closed_positions)))
        vec.append(suspicion_score)
        vectors.append(vec)

    vectors_scaled = _std_scale(vectors)
    labels = _kmeans(vectors_scaled, k=clusters_k)
    for rec, label in zip(records, labels):
        rec.cluster_id = int(label)

    by_cluster: dict[int, list[WalletClusterRecord]] = defaultdict(list)
    for rec in records:
        by_cluster[rec.cluster_id].append(rec)

    cluster_summaries = [_summarize_cluster(rows, cluster_id=cid) for cid, rows in by_cluster.items()]
    cluster_summaries.sort(key=lambda item: item["size"], reverse=True)

    ts_label = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = f"{output_prefix}_{ts_label}"
    out_json = Path(f"v5/{base}.json")
    out_csv = Path(f"v5/{base}.csv")
    out_md = Path(f"v5/{base}.md")
    out_html = Path(f"v5/{base}.html")

    output = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_report": str(input_report_path),
        "wallet_count": len(records),
        "clusters_k": clusters_k,
        "failures_count": len(failures),
        "failures": failures,
        "cluster_summaries": cluster_summaries,
        "wallets": [rec.to_dict() for rec in records],
    }
    out_json.write_text(json.dumps(output, indent=2), encoding="utf-8")

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "wallet",
                "cluster_id",
                "activity_status",
                "dominant_asset",
                "dominant_timeframe",
                "strategy_tags",
                "win_rate_24h",
                "pnl_24h",
                "volume_24h",
                "pnl_to_volume_24h",
                "win_rate_total",
                "pnl_to_volume_total",
                "realized_pnl_total",
                "total_trades",
                "closed_positions",
                "suspicion_score",
            ],
        )
        writer.writeheader()
        for rec in records:
            row = rec.to_dict()
            row["strategy_tags"] = ",".join(row["strategy_tags"])
            writer.writerow(row)

    md_lines = [
        f"# Wallet Cluster Report ({len(records)} wallets)",
        f"- Generated: {output['generated_at_utc']}",
        f"- Source report: `{input_report_path}`",
        f"- K clusters: {clusters_k}",
        f"- Failures: {len(failures)}",
        "",
    ]
    for item in cluster_summaries:
        cid = item["cluster_id"]
        rows = by_cluster[cid]
        rows_sorted = sorted(rows, key=lambda r: r.suspicion_score, reverse=True)
        md_lines.append(f"## Cluster C{cid:02d} (size={item['size']})")
        md_lines.append(f"- Top activity status: `{item['activity_top']}`")
        md_lines.append(f"- Top asset: `{item['asset_top']}`")
        md_lines.append(f"- Top timeframe: `{item['timeframe_top']}`")
        md_lines.append(f"- Top strategy: `{item['strategy_top']}`")
        md_lines.append(f"- Median win rate 24h: `{item['median_win_rate_24h']}`")
        md_lines.append(f"- Median pnl 24h: `{item['median_pnl_24h']}`")
        md_lines.append(f"- Median pnl/vol 24h: `{item['median_p2v_24h']}`")
        md_lines.append(f"- Median pnl/vol total: `{item['median_p2v_total']}`")
        md_lines.append("- Wallet list (top 30 by suspicion):")
        for rec in rows_sorted[:30]:
            md_lines.append(
                f"  - `{rec.wallet}` | activity={rec.activity_status} asset={rec.dominant_asset} "
                f"tf={rec.dominant_timeframe} win24={rec.win_rate_24h} p2v24={rec.pnl_to_volume_24h} "
                f"score={rec.suspicion_score:.3f}",
            )
        md_lines.append("")
    out_md.write_text("\n".join(md_lines), encoding="utf-8")

    _build_html(out_html, records, cluster_summaries)

    print(f"[v5-cluster] JSON: {out_json}")
    print(f"[v5-cluster] CSV: {out_csv}")
    print(f"[v5-cluster] MD: {out_md}")
    print(f"[v5-cluster] HTML: {out_html}")

    return {
        "json": str(out_json),
        "csv": str(out_csv),
        "md": str(out_md),
        "html": str(out_html),
        "cluster_summaries": cluster_summaries,
        "wallet_count": len(records),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Cluster wallets by activity windows, market buckets (asset/timeframe), "
            "strategy tags, 24h win rate, activity status, P/L and pnl/volume."
        ),
    )
    parser.add_argument("--input-report", default="v5/report_broad_20260222.json")
    parser.add_argument("--output-prefix", default="cluster_wallets")
    parser.add_argument("--k", type=int, default=12, help="KMeans cluster count")
    parser.add_argument("--request-rps", type=float, default=14.0)
    parser.add_argument("--request-retries", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--workers", type=int, default=18)
    args = parser.parse_args(argv)

    run(
        input_report_path=Path(args.input_report),
        output_prefix=args.output_prefix,
        clusters_k=max(2, int(args.k)),
        request_rps=max(0.1, float(args.request_rps)),
        request_retries=max(0, int(args.request_retries)),
        timeout_seconds=max(1.0, float(args.timeout_seconds)),
        workers=max(1, int(args.workers)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
