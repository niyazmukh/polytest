from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Optional

from .config_helpers import (
    bootstrap_env_file,
    env_bool,
    env_float,
    env_int,
    env_str,
    reload_env_file_if_changed,
)


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
    skip_book_enabled: bool
    fak_retry_enabled: bool
    fak_retry_max_attempts: int
    fak_retry_delay_seconds: float
    fak_retry_max_window_seconds: float

    max_usdc_per_market: float
    max_usdc_per_token: float
    allow_sell_signals: bool
    actionable_require_live_market_match: bool
    require_market_open: bool
    market_close_guard_seconds: float
    actionable_sources: tuple[str, ...]

    quote_wait_timeout_seconds: float
    quote_stale_after_seconds: float
    quote_fallback_stale_seconds: float
    book_poll_interval_seconds: float
    use_ws_market_feed: bool
    ws_heartbeat_seconds: float
    ws_reconnect_seconds: float
    feed_max_tracked_tokens: int
    feed_track_ttl_seconds: float
    feed_poll_batch_size: int
    feed_ws_subscribe_batch_size: int

    input_path: Optional[str]


def parse_args(argv: _Argv = None) -> BuyConfig:
    env_file = bootstrap_env_file(argv)

    parser = argparse.ArgumentParser(
        description="v2 buy engine: consume ACTIONABLE_SIGNAL and submit immediate marketable BUY orders"
    )
    parser.add_argument("--env-file", default=env_file, help="path to env file (default: .env.v2.local)")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--clob-host", default=os.environ.get("CLOB_HOST", "https://clob.polymarket.com"))
    parser.add_argument(
        "--ws-market-url",
        default=env_str("WS_MARKET_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
    )
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

    parser.add_argument("--order-type", choices=["FAK", "FOK"], default=env_str("BUY_ORDER_TYPE", "FAK"))
    parser.add_argument(
        "--order-usdc",
        type=float,
        default=env_float("BUY_ORDER_USDC", 2.0),
        help="fallback USDC notional when source notional is unavailable",
    )
    parser.add_argument(
        "--copy-scale",
        type=float,
        default=env_float("BUY_COPY_SCALE", 0.1),
        help="copy fraction of source usdc_size (0.1 means 10%%); <=0 disables scaling and uses --order-usdc",
    )
    parser.add_argument(
        "--max-single-order-usdc",
        type=float,
        default=env_float("BUY_MAX_SINGLE_ORDER_USDC", 2.0),
        help="hard cap per copied trade (0 disables cap)",
    )
    parser.add_argument("--min-order-usdc", type=float, default=env_float("BUY_MIN_ORDER_USDC", 1.0))
    parser.add_argument(
        "--min-source-signal-price",
        type=float,
        default=env_float("BUY_MIN_SOURCE_SIGNAL_PRICE", 0.57),
        help="require actionable signal price >= this threshold (supports 0..1 or 0..100 style input)",
    )
    parser.add_argument(
        "--max-signal-age-seconds",
        type=float,
        default=env_float("BUY_MAX_SIGNAL_AGE_SECONDS", 7.0),
        help="hard guard: only copy signals younger than this age; <=0 disables",
    )
    parser.add_argument("--max-price", type=float, default=env_float("BUY_MAX_PRICE", 0.97))
    parser.add_argument("--min-price", type=float, default=env_float("BUY_MIN_PRICE", 0.57))
    parser.add_argument(
        "--max-slippage-abs",
        type=float,
        default=env_float("BUY_MAX_SLIPPAGE_ABS", 0.10),
        help="worst_price = source_price + this",
    )
    parser.add_argument(
        "--skip-book-enabled",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_SKIP_BOOK_ENABLED", True),
        help="use source-price-driven submit path; disable to use quote/book resolution path",
    )
    parser.add_argument(
        "--fak-retry-enabled",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_FAK_RETRY_ENABLED", True),
        help="retry only no-match FAK placement errors",
    )
    parser.add_argument(
        "--fak-retry-max-attempts",
        type=int,
        default=env_int("BUY_FAK_RETRY_MAX_ATTEMPTS", 3),
        help="total attempts for one signal (includes first try)",
    )
    parser.add_argument(
        "--fak-retry-delay-seconds",
        type=float,
        default=env_float("BUY_FAK_RETRY_DELAY_SECONDS", 0.03),
        help="pause between no-match retry attempts",
    )
    parser.add_argument(
        "--fak-retry-max-window-seconds",
        type=float,
        default=env_float("BUY_FAK_RETRY_MAX_WINDOW_SECONDS", 0.35),
        help="max elapsed wall-clock time across no-match retries",
    )

    parser.add_argument(
        "--max-usdc-per-market",
        type=float,
        default=env_float("BUY_MAX_USDC_PER_MARKET", 10.0),
        help="0 disables",
    )
    parser.add_argument(
        "--max-usdc-per-token",
        type=float,
        default=env_float("BUY_MAX_USDC_PER_TOKEN", 6.0),
        help="0 disables",
    )
    parser.add_argument(
        "--allow-sell-signals",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_ALLOW_SELL_SIGNALS", False),
    )
    parser.add_argument(
        "--actionable-require-live-market-match",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_REQUIRE_LIVE_MARKET_MATCH", True),
        help="only trade when signal slug/condition matches detected live market",
    )
    parser.add_argument(
        "--require-market-open",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_REQUIRE_MARKET_OPEN", True),
        help="skip if matched live market reports inactive/closed or has reached end_ts",
    )
    parser.add_argument(
        "--market-close-guard-seconds",
        type=float,
        default=env_float("BUY_MARKET_CLOSE_GUARD_SECONDS", 0.0),
        help="treat market as closed this many seconds before end_ts",
    )
    parser.add_argument(
        "--actionable-sources",
        default=env_str("BUY_ACTIONABLE_SOURCES", "orderbook_subgraph,activity"),
        help="comma-separated allowed ACTIONABLE_SIGNAL source values",
    )

    parser.add_argument(
        "--quote-wait-timeout-seconds",
        type=float,
        default=env_float("BUY_QUOTE_WAIT_TIMEOUT_SECONDS", 0.12),
    )
    parser.add_argument(
        "--quote-stale-after-seconds",
        type=float,
        default=env_float("BUY_QUOTE_STALE_AFTER_SECONDS", 0.8),
    )
    parser.add_argument(
        "--quote-fallback-stale-seconds",
        type=float,
        default=env_float("BUY_QUOTE_FALLBACK_STALE_SECONDS", 2.5),
        help="if fresh quote is unavailable, allow this much staleness before /book fallback",
    )
    parser.add_argument(
        "--book-poll-interval-seconds",
        type=float,
        default=env_float("BUY_BOOK_POLL_INTERVAL_SECONDS", 0.007),
        help="fallback /books poll cadence (0.007s ~= 95 pct of docs 1500/10s)",
    )
    parser.add_argument(
        "--use-ws-market-feed",
        action=argparse.BooleanOptionalAction,
        default=env_bool("BUY_USE_WS_MARKET_FEED", True),
    )
    parser.add_argument(
        "--ws-heartbeat-seconds",
        type=float,
        default=env_float("BUY_WS_HEARTBEAT_SECONDS", 10.0),
    )
    parser.add_argument(
        "--ws-reconnect-seconds",
        type=float,
        default=env_float("BUY_WS_RECONNECT_SECONDS", 0.4),
    )
    parser.add_argument(
        "--feed-max-tracked-tokens",
        type=int,
        default=env_int("BUY_FEED_MAX_TRACKED_TOKENS", 256),
        help="max tracked tokens kept hot in quote feed (<=0 disables cap)",
    )
    parser.add_argument(
        "--feed-track-ttl-seconds",
        type=float,
        default=env_float("BUY_FEED_TRACK_TTL_SECONDS", 1800.0),
        help="drop tokens from quote feed if not touched for this duration (<=0 disables ttl prune)",
    )
    parser.add_argument(
        "--feed-poll-batch-size",
        type=int,
        default=env_int("BUY_FEED_POLL_BATCH_SIZE", 120),
        help="max tokens per get_order_books poll call",
    )
    parser.add_argument(
        "--feed-ws-subscribe-batch-size",
        type=int,
        default=env_int("BUY_FEED_WS_SUBSCRIBE_BATCH_SIZE", 256),
        help="max assets_ids included in ws subscribe payload",
    )

    parser.add_argument(
        "--input-path",
        default=None,
        help="optional JSONL file path; default reads newline-delimited JSON from stdin",
    )

    args = parser.parse_args(argv)
    env_file = reload_env_file_if_changed(env_file, str(args.env_file))

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
        skip_book_enabled=bool(args.skip_book_enabled),
        fak_retry_enabled=bool(args.fak_retry_enabled),
        fak_retry_max_attempts=max(1, int(args.fak_retry_max_attempts)),
        fak_retry_delay_seconds=max(0.0, float(args.fak_retry_delay_seconds)),
        fak_retry_max_window_seconds=max(0.0, float(args.fak_retry_max_window_seconds)),
        max_usdc_per_market=max(0.0, float(args.max_usdc_per_market)),
        max_usdc_per_token=max(0.0, float(args.max_usdc_per_token)),
        allow_sell_signals=bool(args.allow_sell_signals),
        actionable_require_live_market_match=bool(args.actionable_require_live_market_match),
        require_market_open=bool(args.require_market_open),
        market_close_guard_seconds=max(0.0, float(args.market_close_guard_seconds)),
        actionable_sources=sources,
        quote_wait_timeout_seconds=max(0.01, float(args.quote_wait_timeout_seconds)),
        quote_stale_after_seconds=max(0.05, float(args.quote_stale_after_seconds)),
        quote_fallback_stale_seconds=max(0.05, float(args.quote_fallback_stale_seconds)),
        book_poll_interval_seconds=max(0.001, float(args.book_poll_interval_seconds)),
        use_ws_market_feed=bool(args.use_ws_market_feed),
        ws_heartbeat_seconds=max(1.0, float(args.ws_heartbeat_seconds)),
        ws_reconnect_seconds=max(0.05, float(args.ws_reconnect_seconds)),
        feed_max_tracked_tokens=max(0, int(args.feed_max_tracked_tokens)),
        feed_track_ttl_seconds=max(0.0, float(args.feed_track_ttl_seconds)),
        feed_poll_batch_size=max(1, int(args.feed_poll_batch_size)),
        feed_ws_subscribe_batch_size=max(1, int(args.feed_ws_subscribe_batch_size)),
        input_path=None if args.input_path is None else str(args.input_path),
    )
