from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_CRYPTO_KEYWORDS = (
    "btc",
    "bitcoin",
    "eth",
    "ethereum",
    "sol",
    "xrp",
    "doge",
    "crypto",
    "updown",
)


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _normalize_wallets(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        wallet = raw.strip().lower()
        if not wallet:
            continue
        if not wallet.startswith("0x"):
            continue
        if wallet in seen:
            continue
        seen.add(wallet)
        out.append(wallet)
    return tuple(out)


def _read_wallets_file(path: str | None) -> tuple[str, ...]:
    if not path:
        return ()
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        row = line.strip()
        if not row or row.startswith("#"):
            continue
        rows.append(row)
    return _normalize_wallets(rows)


@dataclass(slots=True)
class InvestigateConfig:
    seed_wallets: tuple[str, ...]
    categories: tuple[str, ...]
    time_periods: tuple[str, ...]
    leaderboard_order_by: tuple[str, ...]
    leaderboard_pages_per_board: int
    recent_trades_pages: int
    recent_trade_lookback_hours: float
    max_users: int
    max_activity_pages: int
    activity_page_limit: int
    max_closed_pages: int
    closed_page_limit: int
    request_timeout_seconds: float
    request_retries: int
    request_rps: float
    concurrent_workers: int
    crypto_only: bool
    crypto_keywords: tuple[str, ...]
    near_fifty_band: float
    last_minute_threshold_seconds: float
    pair_under_one_margin: float
    new_account_hours: float
    min_win_rate_tag: float
    min_pnl_to_volume_tag: float
    min_linearity_r2_tag: float
    cluster_distance_threshold: float
    cluster_min_size: int
    min_suspicion_score: float
    output_path: str


def parse_args(argv: list[str] | None = None) -> InvestigateConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Investigate Polymarket wallets and identify probable burner-wallet families "
            "with similar crypto trading behavior."
        ),
    )
    parser.add_argument(
        "--seed-wallets",
        default="",
        help="Comma separated wallet list to force-include.",
    )
    parser.add_argument(
        "--seed-wallets-file",
        default="",
        help="Path to wallet list file (one wallet per line, # comments allowed).",
    )
    parser.add_argument(
        "--categories",
        default="CRYPTO",
        help="Leaderboard categories CSV (default: CRYPTO).",
    )
    parser.add_argument(
        "--time-periods",
        default="DAY,WEEK,MONTH,ALL",
        help="Leaderboard time periods CSV.",
    )
    parser.add_argument(
        "--leaderboard-order-by",
        default="PNL,VOL",
        help="Leaderboard ordering CSV (PNL,VOL).",
    )
    parser.add_argument(
        "--leaderboard-pages-per-board",
        type=int,
        default=4,
        help="Pages per category/time-period/order-by board (50 rows per page).",
    )
    parser.add_argument(
        "--recent-trades-pages",
        type=int,
        default=8,
        help="Pages of /trades to scan for active wallets (up to 1000 rows each).",
    )
    parser.add_argument(
        "--recent-trade-lookback-hours",
        type=float,
        default=48.0,
        help="Stop reading /trades when rows get older than this horizon.",
    )
    parser.add_argument(
        "--max-users",
        type=int,
        default=160,
        help="Maximum number of wallets to investigate deeply.",
    )
    parser.add_argument(
        "--max-activity-pages",
        type=int,
        default=4,
        help="Per-user max pages from /activity.",
    )
    parser.add_argument(
        "--activity-page-limit",
        type=int,
        default=1000,
        help="Per /activity request row limit.",
    )
    parser.add_argument(
        "--max-closed-pages",
        type=int,
        default=4,
        help="Per-user max pages from /closed-positions.",
    )
    parser.add_argument(
        "--closed-page-limit",
        type=int,
        default=500,
        help="Per /closed-positions request row limit.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout per request.",
    )
    parser.add_argument(
        "--request-retries",
        type=int,
        default=4,
        help="HTTP retries for retryable statuses (429/5xx/network).",
    )
    parser.add_argument(
        "--request-rps",
        type=float,
        default=10.0,
        help="Client-side request rate cap across all workers.",
    )
    parser.add_argument(
        "--concurrent-workers",
        type=int,
        default=10,
        help="Concurrent wallet workers.",
    )
    parser.add_argument(
        "--crypto-only",
        action="store_true",
        default=True,
        help="Analyze only crypto-related rows (default enabled).",
    )
    parser.add_argument(
        "--no-crypto-only",
        action="store_false",
        dest="crypto_only",
        help="Disable crypto-only filtering.",
    )
    parser.add_argument(
        "--crypto-keywords",
        default=",".join(DEFAULT_CRYPTO_KEYWORDS),
        help="Keyword CSV used to classify crypto markets.",
    )
    parser.add_argument(
        "--near-fifty-band",
        type=float,
        default=0.05,
        help="Near-0.50 detector band; 0.05 means [0.45,0.55].",
    )
    parser.add_argument(
        "--last-minute-threshold-seconds",
        type=float,
        default=45.0,
        help="Trade counted as last-minute if buy is within this many seconds of market close.",
    )
    parser.add_argument(
        "--pair-under-one-margin",
        type=float,
        default=0.01,
        help="Dual-side price pair threshold: (outcomeA+outcomeB) < 1 - margin.",
    )
    parser.add_argument(
        "--new-account-hours",
        type=float,
        default=48.0,
        help="Account age threshold for 'new wallet' tag.",
    )
    parser.add_argument(
        "--min-win-rate-tag",
        type=float,
        default=0.8,
        help="Win-rate threshold for high-win tag.",
    )
    parser.add_argument(
        "--min-pnl-to-volume-tag",
        type=float,
        default=0.08,
        help="PnL-to-volume threshold for high-efficiency tag.",
    )
    parser.add_argument(
        "--min-linearity-r2-tag",
        type=float,
        default=0.9,
        help="R2 threshold for near-linear cumulative pnl.",
    )
    parser.add_argument(
        "--cluster-distance-threshold",
        type=float,
        default=1.3,
        help="Distance threshold used to connect wallets into similarity groups.",
    )
    parser.add_argument(
        "--cluster-min-size",
        type=int,
        default=2,
        help="Minimum users per emitted group.",
    )
    parser.add_argument(
        "--min-suspicion-score",
        type=float,
        default=0.45,
        help="Minimum score for flagged wallet list.",
    )
    parser.add_argument(
        "--output-path",
        default="v5/report.json",
        help="Output JSON path.",
    )

    args = parser.parse_args(argv)

    seed_csv = _split_csv(args.seed_wallets)
    seed_file = _read_wallets_file(args.seed_wallets_file or None)
    seed_wallets = _normalize_wallets((*seed_csv, *seed_file))

    return InvestigateConfig(
        seed_wallets=seed_wallets,
        categories=_split_csv(args.categories),
        time_periods=_split_csv(args.time_periods),
        leaderboard_order_by=_split_csv(args.leaderboard_order_by),
        leaderboard_pages_per_board=max(1, int(args.leaderboard_pages_per_board)),
        recent_trades_pages=max(0, int(args.recent_trades_pages)),
        recent_trade_lookback_hours=max(0.0, float(args.recent_trade_lookback_hours)),
        max_users=max(1, int(args.max_users)),
        max_activity_pages=max(1, int(args.max_activity_pages)),
        activity_page_limit=max(1, int(args.activity_page_limit)),
        max_closed_pages=max(1, int(args.max_closed_pages)),
        closed_page_limit=max(1, int(args.closed_page_limit)),
        request_timeout_seconds=max(1.0, float(args.request_timeout_seconds)),
        request_retries=max(0, int(args.request_retries)),
        request_rps=max(0.1, float(args.request_rps)),
        concurrent_workers=max(1, int(args.concurrent_workers)),
        crypto_only=bool(args.crypto_only),
        crypto_keywords=_split_csv(args.crypto_keywords.lower()),
        near_fifty_band=max(0.0, min(0.49, float(args.near_fifty_band))),
        last_minute_threshold_seconds=max(0.0, float(args.last_minute_threshold_seconds)),
        pair_under_one_margin=max(0.0, min(0.5, float(args.pair_under_one_margin))),
        new_account_hours=max(1.0, float(args.new_account_hours)),
        min_win_rate_tag=max(0.0, min(1.0, float(args.min_win_rate_tag))),
        min_pnl_to_volume_tag=max(0.0, float(args.min_pnl_to_volume_tag)),
        min_linearity_r2_tag=max(0.0, min(1.0, float(args.min_linearity_r2_tag))),
        cluster_distance_threshold=max(0.01, float(args.cluster_distance_threshold)),
        cluster_min_size=max(2, int(args.cluster_min_size)),
        min_suspicion_score=max(0.0, min(1.0, float(args.min_suspicion_score))),
        output_path=args.output_path,
    )

