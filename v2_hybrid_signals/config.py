from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Optional

from .env import DEFAULT_ENV_FILE, load_env_file


_Argv = Optional[list[str]]


@dataclass(slots=True)
class HybridConfig:
    env_file: str
    user: str
    users: tuple[str, ...]

    runtime_seconds: Optional[float]
    progress_interval_seconds: float
    emit_source_events: bool
    queue_maxsize: int

    activity_limit_per_10s: float
    activity_edge_fraction: float
    activity_http_timeout_seconds: float
    activity_lookback_seconds: int
    activity_page_size: int
    activity_max_pages: int

    subgraph_limit_per_10s: float
    subgraph_edge_fraction: float
    subgraph_http_timeout_seconds: float
    subgraph_page_size: int

    gamma_limit_per_10s: float
    gamma_events_limit_per_10s: float
    gamma_edge_fraction: float
    gamma_http_timeout_seconds: float
    gamma_refresh_interval_seconds: float
    gamma_warmup_pages: int
    gamma_warmup_page_size: int

    live_market_symbol: str
    live_market_timeframe_minutes: int
    live_market_timeframes_minutes: tuple[int, ...]
    live_market_refresh_interval_seconds: float
    live_market_event_pages: int
    live_market_event_page_size: int
    live_market_slug_probe_span: int
    live_market_grace_seconds: float

    actionable_require_outcome: bool

    @property
    def activity_target_rps(self) -> float:
        return max(0.01, (self.activity_limit_per_10s / 10.0) * self.activity_edge_fraction)

    @property
    def subgraph_target_rps(self) -> float:
        return max(0.01, (self.subgraph_limit_per_10s / 10.0) * self.subgraph_edge_fraction)

    @property
    def gamma_target_rps(self) -> float:
        return max(0.01, (self.gamma_limit_per_10s / 10.0) * self.gamma_edge_fraction)

    @property
    def gamma_events_target_rps(self) -> float:
        return max(0.01, (self.gamma_events_limit_per_10s / 10.0) * self.gamma_edge_fraction)


def _normalize_wallet_list(raw: str) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for part in str(raw).split(","):
        wallet = part.strip().lower()
        if not wallet:
            continue
        if wallet not in seen:
            seen.add(wallet)
            out.append(wallet)
    return tuple(out)


def parse_args(argv: _Argv = None) -> HybridConfig:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=os.environ.get("ENV_FILE", DEFAULT_ENV_FILE))
    pre_args, _ = pre.parse_known_args(argv)
    env_file = str(pre_args.env_file).strip() or DEFAULT_ENV_FILE
    load_env_file(env_file)

    default_user = (
        os.environ.get("TRACKED_WALLET")
        or os.environ.get("V2_TRACKED_WALLET")
        or ""
    ).strip().lower()
    default_users = (
        os.environ.get("TRACKED_WALLETS")
        or os.environ.get("V2_TRACKED_WALLETS")
        or ""
    ).strip()

    parser = argparse.ArgumentParser(description="v2 hybrid detector")
    parser.add_argument("--env-file", default=env_file)
    parser.add_argument("--user", default=default_user, help="single tracked source wallet")
    parser.add_argument(
        "--users",
        default=default_users,
        help="comma-separated tracked source wallets (includes --user if provided)",
    )

    parser.add_argument("--runtime-seconds", type=float, default=0.0, help="<=0 means run forever")
    parser.add_argument("--progress-interval-seconds", type=float, default=15.0)
    parser.add_argument("--emit-source-events", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--queue-maxsize", type=int, default=200000)

    parser.add_argument("--activity-limit-per-10s", type=float, default=1000.0)
    parser.add_argument("--activity-edge-fraction", type=float, default=0.98)
    parser.add_argument("--activity-http-timeout-seconds", type=float, default=4.0)
    parser.add_argument("--activity-lookback-seconds", type=int, default=30)
    parser.add_argument("--activity-page-size", type=int, default=500)
    parser.add_argument("--activity-max-pages", type=int, default=2)

    parser.add_argument("--subgraph-limit-per-10s", type=float, default=200.0)
    parser.add_argument("--subgraph-edge-fraction", type=float, default=0.95)
    parser.add_argument("--subgraph-http-timeout-seconds", type=float, default=4.0)
    parser.add_argument("--subgraph-page-size", type=int, default=500)

    parser.add_argument("--gamma-limit-per-10s", type=float, default=300.0)
    parser.add_argument("--gamma-events-limit-per-10s", type=float, default=500.0)
    parser.add_argument("--gamma-edge-fraction", type=float, default=0.95)
    parser.add_argument("--gamma-http-timeout-seconds", type=float, default=4.0)
    parser.add_argument("--gamma-refresh-interval-seconds", type=float, default=15.0)
    parser.add_argument("--gamma-warmup-pages", type=int, default=2)
    parser.add_argument("--gamma-warmup-page-size", type=int, default=500)

    parser.add_argument("--live-market-symbol", default="btc")
    parser.add_argument("--live-market-timeframe-minutes", type=int, default=5)
    parser.add_argument(
        "--live-market-timeframes-minutes",
        default="5,15",
        help="comma-separated list of timeframes to track (must include primary timeframe)",
    )
    parser.add_argument("--live-market-refresh-interval-seconds", type=float, default=1.0)
    parser.add_argument("--live-market-event-pages", type=int, default=2)
    parser.add_argument("--live-market-event-page-size", type=int, default=100)
    parser.add_argument("--live-market-slug-probe-span", type=int, default=1)
    parser.add_argument("--live-market-grace-seconds", type=float, default=45.0)

    parser.add_argument("--actionable-require-outcome", action=argparse.BooleanOptionalAction, default=True)

    args = parser.parse_args(argv)

    cli_env_file = str(args.env_file).strip() or DEFAULT_ENV_FILE
    if cli_env_file != env_file:
        load_env_file(cli_env_file)
        env_file = cli_env_file

    merged_users: list[str] = []
    seen_wallets: set[str] = set()
    for wallet in _normalize_wallet_list(str(args.users)):
        if wallet not in seen_wallets:
            seen_wallets.add(wallet)
            merged_users.append(wallet)
    user = str(args.user).strip().lower()
    if user and user not in seen_wallets:
        seen_wallets.add(user)
        merged_users.insert(0, user)
    if not merged_users and default_user:
        merged_users.append(default_user)

    if not merged_users:
        raise SystemExit("missing tracked wallet: provide --user/--users or set TRACKED_WALLET")

    primary_timeframe = max(1, int(args.live_market_timeframe_minutes))
    timeframes: set[int] = {primary_timeframe}
    for part in str(args.live_market_timeframes_minutes).split(","):
        text = part.strip()
        if not text:
            continue
        try:
            value = int(text)
        except ValueError:
            continue
        if value > 0:
            timeframes.add(value)

    runtime_seconds: Optional[float]
    runtime_seconds = None if float(args.runtime_seconds) <= 0 else float(args.runtime_seconds)

    return HybridConfig(
        env_file=env_file,
        user=merged_users[0],
        users=tuple(merged_users),
        runtime_seconds=runtime_seconds,
        progress_interval_seconds=max(1.0, float(args.progress_interval_seconds)),
        emit_source_events=bool(args.emit_source_events),
        queue_maxsize=max(1000, int(args.queue_maxsize)),
        activity_limit_per_10s=max(1.0, float(args.activity_limit_per_10s)),
        activity_edge_fraction=min(1.0, max(0.01, float(args.activity_edge_fraction))),
        activity_http_timeout_seconds=max(0.25, float(args.activity_http_timeout_seconds)),
        activity_lookback_seconds=max(2, int(args.activity_lookback_seconds)),
        activity_page_size=max(10, min(1000, int(args.activity_page_size))),
        activity_max_pages=max(1, min(20, int(args.activity_max_pages))),
        subgraph_limit_per_10s=max(1.0, float(args.subgraph_limit_per_10s)),
        subgraph_edge_fraction=min(1.0, max(0.01, float(args.subgraph_edge_fraction))),
        subgraph_http_timeout_seconds=max(0.25, float(args.subgraph_http_timeout_seconds)),
        subgraph_page_size=max(10, min(1000, int(args.subgraph_page_size))),
        gamma_limit_per_10s=max(1.0, float(args.gamma_limit_per_10s)),
        gamma_events_limit_per_10s=max(1.0, float(args.gamma_events_limit_per_10s)),
        gamma_edge_fraction=min(1.0, max(0.01, float(args.gamma_edge_fraction))),
        gamma_http_timeout_seconds=max(0.25, float(args.gamma_http_timeout_seconds)),
        gamma_refresh_interval_seconds=max(1.0, float(args.gamma_refresh_interval_seconds)),
        gamma_warmup_pages=max(1, min(20, int(args.gamma_warmup_pages))),
        gamma_warmup_page_size=max(10, min(1000, int(args.gamma_warmup_page_size))),
        live_market_symbol=str(args.live_market_symbol).strip().lower() or "btc",
        live_market_timeframe_minutes=primary_timeframe,
        live_market_timeframes_minutes=tuple(sorted(timeframes)),
        live_market_refresh_interval_seconds=max(0.25, float(args.live_market_refresh_interval_seconds)),
        live_market_event_pages=max(1, min(10, int(args.live_market_event_pages))),
        live_market_event_page_size=max(10, min(500, int(args.live_market_event_page_size))),
        live_market_slug_probe_span=max(0, min(5, int(args.live_market_slug_probe_span))),
        live_market_grace_seconds=max(0.0, float(args.live_market_grace_seconds)),
        actionable_require_outcome=bool(args.actionable_require_outcome),
    )
