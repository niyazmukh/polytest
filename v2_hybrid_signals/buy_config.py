from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Optional

from .env import DEFAULT_ENV_FILE, load_env_file


_Argv = Optional[list[str]]


@dataclass(slots=True)
class BuyConfig:
    env_file: str
    dry_run: bool
    clob_host: str
    ws_market_url: str
    private_key: str
    funder: str
    api_key: str
    api_secret: str
    api_passphrase: str
    chain_id: int
    signature_type: int

    order_type: str
    order_usdc: float
    copy_scale: float
    max_single_order_usdc: float
    min_order_usdc: float
    min_source_signal_price: float
    max_signal_age_seconds: float
    max_price: float
    min_price: float
    max_slippage_abs: float

    max_usdc_per_market: float
    max_usdc_per_token: float
    allow_sell_signals: bool
    actionable_require_live_market_match: bool
    actionable_sources: tuple[str, ...]

    quote_wait_timeout_seconds: float
    quote_stale_after_seconds: float
    quote_fallback_stale_seconds: float
    book_poll_interval_seconds: float
    use_ws_market_feed: bool
    ws_heartbeat_seconds: float
    ws_reconnect_seconds: float

    input_path: Optional[str]


def parse_args(argv: _Argv = None) -> BuyConfig:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=os.environ.get("ENV_FILE", DEFAULT_ENV_FILE))
    pre_args, _ = pre.parse_known_args(argv)
    env_file = str(pre_args.env_file).strip() or DEFAULT_ENV_FILE
    load_env_file(env_file)

    parser = argparse.ArgumentParser(
        description="v2 buy engine: consume ACTIONABLE_SIGNAL and submit immediate marketable BUY orders"
    )
    parser.add_argument("--env-file", default=env_file, help="path to env file (default: .env.v2.local)")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--clob-host", default=os.environ.get("CLOB_HOST", "https://clob.polymarket.com"))
    parser.add_argument("--ws-market-url", default="wss://ws-subscriptions-clob.polymarket.com/ws/market")
    parser.add_argument(
        "--private-key",
        default=os.environ.get("PRIVATE_KEY", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument(
        "--funder",
        default=os.environ.get("FUNDER_ADDRESS", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("API_KEY", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument(
        "--api-secret",
        default=os.environ.get("SECRET", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument(
        "--api-passphrase",
        default=os.environ.get("PASSPHRASE", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument("--chain-id", type=int, default=int(os.environ.get("CHAIN_ID", "137")))
    parser.add_argument("--signature-type", type=int, default=int(os.environ.get("SIGNATURE_TYPE", "1")))

    parser.add_argument("--order-type", choices=["FAK", "FOK"], default="FAK")
    parser.add_argument(
        "--order-usdc",
        type=float,
        default=2.0,
        help="fallback USDC notional when source notional is unavailable",
    )
    parser.add_argument(
        "--copy-scale",
        type=float,
        default=0.1,
        help="copy fraction of source usdc_size (0.1 means 10%%); <=0 disables scaling and uses --order-usdc",
    )
    parser.add_argument(
        "--max-single-order-usdc",
        type=float,
        default=2.0,
        help="hard cap per copied trade (0 disables cap)",
    )
    parser.add_argument("--min-order-usdc", type=float, default=1.0)
    parser.add_argument(
        "--min-source-signal-price",
        type=float,
        default=0.57,
        help="require actionable signal price >= this threshold (supports 0..1 or 0..100 style input)",
    )
    parser.add_argument(
        "--max-signal-age-seconds",
        type=float,
        default=7.0,
        help="hard guard: only copy signals younger than this age; <=0 disables",
    )
    parser.add_argument("--max-price", type=float, default=0.97)
    parser.add_argument("--min-price", type=float, default=0.57)
    parser.add_argument("--max-slippage-abs", type=float, default=0.02, help="worst_price = best_ask + this")

    parser.add_argument("--max-usdc-per-market", type=float, default=10.0, help="0 disables")
    parser.add_argument("--max-usdc-per-token", type=float, default=6.0, help="0 disables")
    parser.add_argument("--allow-sell-signals", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--actionable-require-live-market-match",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="only trade when signal slug/condition matches detected live market",
    )
    parser.add_argument(
        "--actionable-sources",
        default="orderbook_subgraph,activity",
        help="comma-separated allowed ACTIONABLE_SIGNAL source values",
    )

    parser.add_argument("--quote-wait-timeout-seconds", type=float, default=0.12)
    parser.add_argument("--quote-stale-after-seconds", type=float, default=0.8)
    parser.add_argument(
        "--quote-fallback-stale-seconds",
        type=float,
        default=2.5,
        help="if fresh quote is unavailable, allow this much staleness before /book fallback",
    )
    parser.add_argument(
        "--book-poll-interval-seconds",
        type=float,
        default=0.007,
        help="fallback /books poll cadence (0.007s ~= 95 pct of docs 1500/10s)",
    )
    parser.add_argument("--use-ws-market-feed", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ws-heartbeat-seconds", type=float, default=10.0)
    parser.add_argument("--ws-reconnect-seconds", type=float, default=0.4)

    parser.add_argument(
        "--input-path",
        default=None,
        help="optional JSONL file path; default reads newline-delimited JSON from stdin",
    )

    args = parser.parse_args(argv)
    cli_env_file = str(args.env_file).strip() or DEFAULT_ENV_FILE
    if cli_env_file != env_file:
        load_env_file(cli_env_file)
        env_file = cli_env_file

    sources = tuple(
        s.strip().lower()
        for s in str(args.actionable_sources).split(",")
        if s.strip()
    )
    if not sources:
        sources = ("orderbook_subgraph", "activity")
    dry_run = bool(args.dry_run)
    api_key = str(args.api_key).strip()
    api_secret = str(args.api_secret).strip()
    api_passphrase = str(args.api_passphrase).strip()
    min_source_signal_price = float(args.min_source_signal_price)
    if min_source_signal_price > 1.0:
        min_source_signal_price = min_source_signal_price / 100.0

    return BuyConfig(
        env_file=env_file,
        dry_run=dry_run,
        clob_host=str(args.clob_host).strip(),
        ws_market_url=str(args.ws_market_url).strip(),
        private_key=str(args.private_key).strip(),
        funder=str(args.funder).strip().lower(),
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        chain_id=int(args.chain_id),
        signature_type=int(args.signature_type),
        order_type=str(args.order_type).strip().upper(),
        order_usdc=max(0.0, float(args.order_usdc)),
        copy_scale=max(0.0, float(args.copy_scale)),
        max_single_order_usdc=max(0.0, float(args.max_single_order_usdc)),
        min_order_usdc=max(0.0, float(args.min_order_usdc)),
        min_source_signal_price=min(1.0, max(0.0, min_source_signal_price)),
        max_signal_age_seconds=max(0.0, float(args.max_signal_age_seconds)),
        max_price=min(0.9999, max(0.001, float(args.max_price))),
        min_price=min(0.9999, max(0.0, float(args.min_price))),
        max_slippage_abs=max(0.0, float(args.max_slippage_abs)),
        max_usdc_per_market=max(0.0, float(args.max_usdc_per_market)),
        max_usdc_per_token=max(0.0, float(args.max_usdc_per_token)),
        allow_sell_signals=bool(args.allow_sell_signals),
        actionable_require_live_market_match=bool(args.actionable_require_live_market_match),
        actionable_sources=sources,
        quote_wait_timeout_seconds=max(0.01, float(args.quote_wait_timeout_seconds)),
        quote_stale_after_seconds=max(0.05, float(args.quote_stale_after_seconds)),
        quote_fallback_stale_seconds=max(0.05, float(args.quote_fallback_stale_seconds)),
        book_poll_interval_seconds=max(0.001, float(args.book_poll_interval_seconds)),
        use_ws_market_feed=bool(args.use_ws_market_feed),
        ws_heartbeat_seconds=max(1.0, float(args.ws_heartbeat_seconds)),
        ws_reconnect_seconds=max(0.05, float(args.ws_reconnect_seconds)),
        input_path=None if args.input_path is None else str(args.input_path),
    )
