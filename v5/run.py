from __future__ import annotations

import json
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analysis import UserMetrics, UserRawData, analyze_user, build_similarity_groups, is_crypto_row
from .config import InvestigateConfig, parse_args
from .polymarket_api import ApiError, PolymarketApi


LEADERBOARD_PAGE_SIZE = 50
TRADES_PAGE_SIZE = 1000


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _discover_wallets(
    api: PolymarketApi,
    cfg: InvestigateConfig,
    now_ts: int,
) -> tuple[list[str], dict[str, dict[str, Any]], dict[str, Any]]:
    info: dict[str, dict[str, Any]] = {}
    discovery_stats: dict[str, Any] = {
        "leaderboard_rows_scanned": 0,
        "trades_rows_scanned": 0,
        "trades_rows_within_horizon": 0,
        "trades_stop_reason": None,
        "wallet_counts_by_source": {},
    }
    cutoff_ts = int(now_ts - cfg.recent_trade_lookback_hours * 3600.0)

    def ensure_wallet(wallet: str) -> dict[str, Any]:
        wallet = wallet.lower().strip()
        row = info.get(wallet)
        if row is None:
            row = {
                "wallet": wallet,
                "sources": set(),
                "leaderboard_rows": [],
                "recent_trade_count": 0,
                "best_rank": None,
                "max_leaderboard_pnl": None,
            }
            info[wallet] = row
        return row

    for wallet in cfg.seed_wallets:
        ensure_wallet(wallet)["sources"].add("seed")

    for category in cfg.categories:
        for period in cfg.time_periods:
            for order_by in cfg.leaderboard_order_by:
                source_key = f"leaderboard:{category}:{period}:{order_by}"
                for page in range(cfg.leaderboard_pages_per_board):
                    offset = page * LEADERBOARD_PAGE_SIZE
                    rows = api.get_leaderboard(
                        category=category,
                        time_period=period,
                        order_by=order_by,
                        limit=LEADERBOARD_PAGE_SIZE,
                        offset=offset,
                    )
                    if not rows:
                        break
                    discovery_stats["leaderboard_rows_scanned"] += len(rows)
                    for row in rows:
                        wallet = str(row.get("proxyWallet", "")).lower().strip()
                        if not wallet:
                            continue
                        current = ensure_wallet(wallet)
                        current["sources"].add(source_key)
                        current["leaderboard_rows"].append(row)
                        rank = _safe_int(row.get("rank"))
                        pnl = row.get("pnl")
                        try:
                            pnl_val = float(pnl)
                        except (TypeError, ValueError):
                            pnl_val = None
                        best_rank = current["best_rank"]
                        if rank is not None and (best_rank is None or rank < best_rank):
                            current["best_rank"] = rank
                        if pnl_val is not None:
                            max_pnl = current["max_leaderboard_pnl"]
                            if max_pnl is None or pnl_val > max_pnl:
                                current["max_leaderboard_pnl"] = pnl_val
                    if len(rows) < LEADERBOARD_PAGE_SIZE:
                        break

    for page in range(cfg.recent_trades_pages):
        offset = page * TRADES_PAGE_SIZE
        try:
            rows = api.get_trades(limit=TRADES_PAGE_SIZE, offset=offset)
        except ApiError as err:
            err_text = str(err).lower()
            if "offset of 3000 exceeded" in err_text or "status=400" in err_text:
                discovery_stats["trades_stop_reason"] = f"api_limit_at_offset_{offset}"
                break
            raise
        if not rows:
            if discovery_stats["trades_stop_reason"] is None:
                discovery_stats["trades_stop_reason"] = "empty_page"
            break
        discovery_stats["trades_rows_scanned"] += len(rows)
        older_found = False
        for row in rows:
            ts = _safe_int(row.get("timestamp"))
            if ts is None:
                continue
            if ts < cutoff_ts:
                older_found = True
            else:
                discovery_stats["trades_rows_within_horizon"] += 1
            wallet = str(row.get("proxyWallet", "")).lower().strip()
            if not wallet:
                continue
            current = ensure_wallet(wallet)
            current["sources"].add("recent_trades")
            if ts >= cutoff_ts:
                current["recent_trade_count"] += 1
        if older_found:
            discovery_stats["trades_stop_reason"] = f"lookback_cutoff_at_offset_{offset}"
            break

    source_counts = Counter()
    for row in info.values():
        for source in row["sources"]:
            source_counts[source] += 1
    discovery_stats["wallet_counts_by_source"] = dict(sorted(source_counts.items()))

    def wallet_priority(row: dict[str, Any]) -> tuple[float, float, float, str]:
        score = 0.0
        if "seed" in row["sources"]:
            score += 10_000.0
        score += float(row["recent_trade_count"]) * 2.0
        best_rank = row["best_rank"]
        if best_rank is not None:
            score += max(0.0, 1200.0 - float(best_rank))
        max_pnl = row["max_leaderboard_pnl"]
        if max_pnl is not None:
            score += min(2_000.0, max(0.0, max_pnl) * 0.05)
        return (score, float(row["recent_trade_count"]), float(-(best_rank or 10**9)), row["wallet"])

    ranked = sorted(info.values(), key=wallet_priority, reverse=True)
    selected = ranked[: cfg.max_users]
    wallets = [row["wallet"] for row in selected]
    selected_info = {row["wallet"]: row for row in selected}
    return wallets, selected_info, discovery_stats


def _collect_wallet(
    api: PolymarketApi,
    cfg: InvestigateConfig,
    wallet: str,
    discovery_row: dict[str, Any],
) -> UserRawData:
    profile = api.get_public_profile(address=wallet)

    activity_rows: list[dict[str, Any]] = []
    for page in range(cfg.max_activity_pages):
        offset = page * cfg.activity_page_limit
        rows = api.get_activity(
            user=wallet,
            activity_type="TRADE",
            limit=cfg.activity_page_limit,
            offset=offset,
        )
        if not rows:
            break
        activity_rows.extend(rows)
        if len(rows) < cfg.activity_page_limit:
            break

    closed_rows: list[dict[str, Any]] = []
    for page in range(cfg.max_closed_pages):
        offset = page * cfg.closed_page_limit
        rows = api.get_closed_positions(
            user=wallet,
            limit=cfg.closed_page_limit,
            offset=offset,
        )
        if not rows:
            break
        closed_rows.extend(rows)
        if len(rows) < cfg.closed_page_limit:
            break

    if cfg.crypto_only:
        activity_rows = [row for row in activity_rows if is_crypto_row(row, cfg.crypto_keywords)]
        closed_rows = [row for row in closed_rows if is_crypto_row(row, cfg.crypto_keywords)]

    leaderboard_rows = list(discovery_row.get("leaderboard_rows", []))
    sources = tuple(sorted(discovery_row.get("sources", [])))
    return UserRawData(
        wallet=wallet,
        profile=profile,
        leaderboard_rows=leaderboard_rows,
        activity_rows=activity_rows,
        closed_rows=closed_rows,
        discovery_sources=sources,
    )


def _is_flagged(metric: UserMetrics, cfg: InvestigateConfig) -> bool:
    if metric.suspicion_score < cfg.min_suspicion_score:
        return False
    perf_tags = {"high_win_rate", "high_pnl_efficiency", "linear_pnl_curve"}
    behavior_tags = {"near_50_bias", "last_minute_entries", "dual_side_under_one"}
    tags = set(metric.tags)
    return bool(tags.intersection(perf_tags) and tags.intersection(behavior_tags))


def run(cfg: InvestigateConfig) -> dict[str, Any]:
    now_ts = int(time.time())
    api = PolymarketApi(
        timeout_seconds=cfg.request_timeout_seconds,
        retries=cfg.request_retries,
        rps=cfg.request_rps,
    )

    wallets, discovery_rows, discovery_stats = _discover_wallets(api, cfg, now_ts=now_ts)
    print(
        f"[v5] discovered_wallets={len(discovery_rows)} selected={len(wallets)} "
        f"leaderboard_rows={discovery_stats['leaderboard_rows_scanned']} "
        f"trades_rows={discovery_stats['trades_rows_scanned']}",
        file=sys.stderr,
    )

    raw_data: list[UserRawData] = []
    failures: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=cfg.concurrent_workers) as pool:
        futures = {
            pool.submit(_collect_wallet, api, cfg, wallet, discovery_rows[wallet]): wallet
            for wallet in wallets
        }
        for idx, future in enumerate(as_completed(futures), start=1):
            wallet = futures[future]
            try:
                raw = future.result()
            except ApiError as err:
                failures.append({"wallet": wallet, "error": str(err)})
                continue
            except Exception as err:  # defensive guard for long runs
                failures.append({"wallet": wallet, "error": f"unexpected_error: {err}"})
                continue
            raw_data.append(raw)
            if idx % 10 == 0 or idx == len(futures):
                print(
                    f"[v5] fetched {idx}/{len(futures)} wallets",
                    file=sys.stderr,
                )

    metrics = [analyze_user(item, cfg=cfg, now_ts=now_ts) for item in raw_data]
    metrics.sort(key=lambda row: row.suspicion_score, reverse=True)
    flagged = [row for row in metrics if _is_flagged(row, cfg)]
    groups = build_similarity_groups(metrics, cfg)

    bucket_counts = Counter(row.activity_bucket for row in metrics)
    out = {
        "generated_at_utc": _now_iso(),
        "official_sources": {
            "leaderboard": "https://docs.polymarket.com/developers/CLOB/prices-books/get-leaderboard",
            "trades": "https://docs.polymarket.com/developers/CLOB/trades/get-trades",
            "activity": "https://docs.polymarket.com/developers/CLOB/trades/get-user-activity",
            "closed_positions": "https://docs.polymarket.com/developers/CLOB/trades/get-closed-positions",
            "public_profile": "https://docs.polymarket.com/api-reference/profile/get-profile",
        },
        "config": asdict(cfg),
        "discovery_stats": discovery_stats,
        "summary": {
            "selected_wallets": len(wallets),
            "investigated_wallets": len(metrics),
            "failed_wallets": len(failures),
            "flagged_wallets": len(flagged),
            "groups": len(groups),
            "activity_buckets": dict(sorted(bucket_counts.items())),
        },
        "failures": failures,
        "flagged_users": [row.to_dict() for row in flagged],
        "groups": groups,
        "users": [row.to_dict() for row in metrics],
    }
    return out


def _print_console_summary(report: dict[str, Any]) -> None:
    summary = report.get("summary", {})
    print(
        (
            "Investigated {investigated_wallets} wallets; flagged {flagged_wallets}; "
            "groups {groups}; failures {failed_wallets}"
        ).format(**summary),
    )
    flagged = report.get("flagged_users", [])
    if flagged:
        print("\nTop flagged wallets:")
        for idx, row in enumerate(flagged[:12], start=1):
            tags = ",".join(row.get("tags", []))
            print(
                f"{idx:02d}. {row.get('wallet')} score={row.get('suspicion_score'):.3f} "
                f"win_rate={row.get('win_rate')} pnl_to_vol={row.get('pnl_to_volume')} "
                f"activity={row.get('activity_bucket')} tags=[{tags}]",
            )
    groups = report.get("groups", [])
    if groups:
        print("\nSimilarity groups:")
        for group in groups[:10]:
            wallets = ",".join(group.get("wallets", []))
            tags = ",".join(group.get("common_tags", []))
            print(
                f"{group.get('group_id')} size={group.get('size')} "
                f"avg_score={group.get('avg_suspicion_score'):.3f} tags=[{tags}] wallets={wallets}",
            )


def main(argv: list[str] | None = None) -> int:
    cfg = parse_args(argv)
    report = run(cfg)

    out_path = Path(cfg.output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    _print_console_summary(report)
    print(f"\nSaved report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
