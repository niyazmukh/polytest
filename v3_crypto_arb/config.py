"""Configuration for v3 crypto arb engine."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from typing import Sequence


# ──────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────

BINANCE_WS = "wss://stream.binance.com:9443/ws"
POLYMARKET_RTDS_WS = "wss://ws-live-data.polymarket.com"
POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com"


# ──────────────────────────────────────────────────────────────
# Assets
# ──────────────────────────────────────────────────────────────

ALL_ASSETS = ("btc", "eth", "sol", "xrp")

BINANCE_SYMBOLS = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

CHAINLINK_SYMBOLS = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}


# ──────────────────────────────────────────────────────────────
# Config dataclass
# ──────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Config:
    """Runtime configuration — populated from CLI + env."""

    # Asset scope
    assets: Sequence[str] = ("btc", "eth", "sol", "xrp")

    # Window durations to track (seconds)
    window_durations: Sequence[int] = (300, 900)  # 5m, 15m

    # ── Arb engine thresholds ──
    # Minimum edge (Pr(win) - market_ask) to emit signal
    edge_threshold: float = 0.03
    # Minimum BPS displacement to even evaluate directional
    min_displacement_bps: float = 10.0
    # Minimum seconds remaining in window to trade
    min_time_remaining_s: float = 5.0
    # Maximum seconds into window to still discover start price
    start_price_grace_s: float = 3.0

    # ── Complementary arb ──
    # Maximum combined ask for complement arb (must be < 1.0 - fees)
    complement_max_ask: float = 0.98

    # ── Stale quote snipe ──
    # Minimum quote age (seconds) to consider it stale
    stale_quote_age_s: float = 0.5
    # Minimum edge when quote is stale
    stale_snipe_edge: float = 0.02

    # ── Near-expiry certainty ──
    # Maximum seconds remaining for near-expiry pattern
    near_expiry_max_remaining_s: float = 30.0
    # Minimum probability to declare "near-certain"
    near_expiry_min_prob: float = 0.92
    # Minimum edge for near-expiry entry
    near_expiry_min_edge: float = 0.02

    # ── Sell / exit thresholds ──
    # Sell to take profit when bid >= entry_cost + this edge
    profit_take_min_edge: float = 0.02
    # Sell to cut losses when prob of our outcome < this
    mean_reversion_prob_floor: float = 0.40

    # ── Fees ──
    fee_rate: float = 0.0175
    fee_exponent: float = 1.0  # from Polymarket docs

    # ── Volatility ──
    # Default annualised BTC vol (calibrated later from live data)
    default_vol_btc: float = 0.60
    default_vol_eth: float = 0.75
    default_vol_sol: float = 1.20
    default_vol_xrp: float = 1.00
    # Rolling window for realised vol (seconds of trade data)
    vol_window_s: float = 300.0

    # ── Gamma API polling ──
    gamma_poll_interval_s: float = 30.0

    # ── Logging ──
    log_level: str = "INFO"

    def default_vol(self, asset: str) -> float:
        return {
            "btc": self.default_vol_btc,
            "eth": self.default_vol_eth,
            "sol": self.default_vol_sol,
            "xrp": self.default_vol_xrp,
        }.get(asset, 0.80)


def parse_args(argv: Sequence[str] | None = None) -> Config:
    """Build Config from CLI args + environment variables."""
    p = argparse.ArgumentParser(description="v3 crypto arb listener")
    p.add_argument("--assets", nargs="+", default=None,
                   help="Assets to track (default: btc eth sol xrp)")
    p.add_argument("--edge-threshold", type=float, default=None)
    p.add_argument("--min-displacement-bps", type=float, default=None)
    p.add_argument("--min-time-remaining", type=float, default=None)
    p.add_argument("--vol-window", type=float, default=None)
    p.add_argument("--gamma-poll-interval", type=float, default=None)
    p.add_argument("--log-level", default=None)
    args = p.parse_args(argv)

    cfg = Config()

    # CLI overrides
    if args.assets:
        cfg = Config(assets=tuple(a.lower() for a in args.assets))
    if args.edge_threshold is not None:
        cfg.edge_threshold = args.edge_threshold
    if args.min_displacement_bps is not None:
        cfg.min_displacement_bps = args.min_displacement_bps
    if args.min_time_remaining is not None:
        cfg.min_time_remaining_s = args.min_time_remaining
    if args.vol_window is not None:
        cfg.vol_window_s = args.vol_window
    if args.gamma_poll_interval is not None:
        cfg.gamma_poll_interval_s = args.gamma_poll_interval
    if args.log_level is not None:
        cfg.log_level = args.log_level.upper()

    # Env overrides (lowest priority)
    if (v := os.environ.get("V3_EDGE_THRESHOLD")) and args.edge_threshold is None:
        cfg.edge_threshold = float(v)
    if (v := os.environ.get("V3_LOG_LEVEL")) and args.log_level is None:
        cfg.log_level = v.upper()

    return cfg
