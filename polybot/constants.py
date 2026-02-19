DATA_API_BASE = "https://data-api.polymarket.com"  # Base URL for Polymarket Data API reads.
DEFAULT_USERS = [
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a",
]  # Default target wallets for multi-user listening.
DEFAULT_USER = DEFAULT_USERS[0]  # Backward-compatible single-user fallback.
ACTIVITY_TYPES = ["TRADE", "SPLIT", "MERGE", "REDEEM", "REWARD", "CONVERSION", "MAKER_REBATE"]  # Allowed --type values.
HTTP_RETRIES = 3  # HTTP attempts for retryable Data API calls.

COPY_DEFAULTS = {
    # Position sizing and exposure caps.
    "copy_scale": 0.05,  # Copy this fraction of target notional (0.05 = 5%).
    "copy_max_usdc": 0.0,  # Max USDC per copied order (0 disables this cap).
    "copy_max_buy_price_usdc": 0.95,  # Never place BUY above this price.
    "copy_max_combined_market_usdc": 6.0,  # Max combined exposure per market (0 disables).
    "copy_max_outcome_usdc": 0.0,  # Max exposure per outcome token (0 disables).
    "copy_min_usdc": 0.5,  # Minimum copied notional to submit an order.
    "copy_min_order_size": 5.0,  # Legacy compatibility knob (no client-side min-size blocking; venue min is token-specific and may differ).
    "copy_venue_min_notional_usdc": 1.0,  # Legacy compatibility knob (no client-side venue-min-notional blocking).
    # Signing and chain settings for live order submission.
    "copy_private_key": "",  # EOA private key for order signing (required when copy_execute=true).
    "copy_funder": "",  # Polymarket funder address tied to the signing key (required when copy_execute=true).
    "copy_signature_type": 1,  # Polymarket signature type: 0 EOA, 1 proxy, 2 browser wallet.
    "copy_chain_id": 137,  # Chain id for Polygon mainnet CLOB.
    # Freshness and price-quality guards.
    "copy_max_event_age_seconds": 7,  # Skip target events older than this many seconds.
    "copy_max_slippage_pct": 0.07,  # Skip when live price drift exceeds this fraction.
    # Rate limits and cooldowns.
    "copy_max_orders_per_minute": 60,  # Global submit cap per rolling minute.
    "copy_global_cooldown_seconds": 0.0,  # Min delay between any two copied orders.
    "copy_market_cooldown_seconds": 0.0,  # Min delay between orders on same token.
    "copy_max_usdc_per_market_per_minute": 0.0,  # Per-market notional cap per minute (0 disables).
    # Market allowlist and dedupe journal.
    "copy_allowed_slug_prefixes": [],  # Restrict copied markets by slug prefix (empty = allow all).
    "copy_idempotency_path": "copy_idempotency.jsonl",  # Append-only journal path for dedupe keys (empty disables file journal).
    "copy_idempotency_max_entries": 200_000,  # In-memory max dedupe keys to retain.
    # Submit failure handling and circuit breaker.
    "copy_submit_error_hold_seconds": 0.0,  # Retry hold time after non-deterministic submit failures.
    "copy_cb_window_seconds": 15.0,  # Failure lookback window for circuit breaker.
    "copy_cb_failure_threshold": 5,  # Failures in window needed to trip breaker.
    "copy_cb_cooldown_seconds": 5.0,  # Pause duration while breaker is open.
    # Take-profit and near-expiry exit behavior.
    "copy_take_profit_enable": True,  # Enable TP worker (active only when copy mode executes live).
    "copy_take_profit_multiplier": 1.25,  # TP target = avg entry * this multiplier.
    "copy_take_profit_price_cap_threshold": 0.97,  # If TP target exceeds this, use fallback price instead.
    "copy_take_profit_price_fallback": 0.95,  # Fallback TP sell price when cap threshold is exceeded.
    "copy_take_profit_fak_trigger_enable": True,  # If true, may replace resting TP GTC with immediate FAK when bid reaches target.
    "copy_take_profit_disable_backoff": True,  # Disable TP retry backoffs (aggressive retries, lowest lag).
    "copy_take_profit_check_interval_seconds": 0.5,  # Seconds between TP sweep cycles.
    "copy_force_exit_before_expiry_seconds": 55.0,  # Force exit inside this pre-expiry window (0 disables).
    "copy_force_exit_hold_price_threshold": 0.75,  # Near expiry, hold instead of force-exit when mark is above this.
    # Background maintenance workers and local caches.
    "copy_stale_buy_order_max_age_seconds": 3.0,  # Cancel open BUY orders older than this (0 disables worker action).
    "copy_stale_buy_sweep_interval_seconds": 1.5,  # Seconds between stale BUY cleanup sweeps.
    "copy_positions_cache_ttl_seconds": 2.0,  # Position snapshot cache TTL for exposure checks.
    "copy_local_market_submission_ttl_seconds": 5.0,  # TTL for local submitted-notional buffer in cap checks.
    # Portfolio drawdown kill-switch.
    "copy_portfolio_guard_enable": False,  # Enable drawdown guard stop trigger.
    "copy_portfolio_guard_user": "",  # Wallet to monitor (empty -> copy_funder, then --user).
    "copy_portfolio_check_interval_seconds": 10.0,  # Seconds between guard portfolio checks.
    "copy_portfolio_lookback_seconds": 600,  # Baseline lookback window for drawdown measurement.
    "copy_portfolio_max_drawdown_pct": 0.10,  # Max allowed drawdown fraction (0.10 = 10%).
}
