"""Configuration for v4 data logger.

Standalone config — no arb thresholds, just data-collection params.
For feed components, we construct a v3 Config internally.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Sequence


ALL_ASSETS = ("btc", "eth", "sol", "xrp")

GAMMA_API = "https://gamma-api.polymarket.com"

# Symbol mappings (duplicated from v3 to avoid import-time coupling)
CHAINLINK_SYMBOLS = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}


@dataclass(slots=True)
class LoggerConfig:
    """Runtime config for the data logger."""

    # Assets to track
    assets: Sequence[str] = ALL_ASSETS

    # Window durations to scan (seconds)
    window_durations: Sequence[int] = (300, 900)  # 5m, 15m

    # Output directory for JSONL files
    output_dir: str = "v4_data"

    # Binance tick sampling: log every Nth tick (1 = all, 10 = 10%)
    # Internal latency tracking uses every tick regardless.
    binance_sample_rate: int = 1

    # Gamma API scanner poll interval
    gamma_poll_interval_s: float = 15.0

    # Resolution poller
    resolution_poll_interval_s: float = 30.0
    resolution_delay_s: float = 60.0       # wait after window end
    resolution_timeout_s: float = 600.0    # give up

    # Start price capture grace period
    start_price_grace_s: float = 5.0

    # Periodic stats to stderr
    stats_interval_s: float = 60.0

    # Logging level
    log_level: str = "INFO"


def parse_args(argv: Sequence[str] | None = None) -> LoggerConfig:
    """Parse CLI arguments."""
    p = argparse.ArgumentParser(description="v4 crypto data logger")
    p.add_argument("--assets", nargs="+", default=None,
                   help="Assets to track (default: btc eth sol xrp)")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--binance-sample-rate", type=int, default=None,
                   help="Log every Nth Binance tick (default: 1 = all)")
    p.add_argument("--gamma-poll-interval", type=float, default=None)
    p.add_argument("--stats-interval", type=float, default=None)
    p.add_argument("--log-level", default=None)
    args = p.parse_args(argv)

    cfg = LoggerConfig()
    if args.assets:
        cfg = LoggerConfig(assets=tuple(a.lower() for a in args.assets))
    if args.output_dir is not None:
        cfg.output_dir = args.output_dir
    if args.binance_sample_rate is not None:
        cfg.binance_sample_rate = args.binance_sample_rate
    if args.gamma_poll_interval is not None:
        cfg.gamma_poll_interval_s = args.gamma_poll_interval
    if args.stats_interval is not None:
        cfg.stats_interval_s = args.stats_interval
    if args.log_level is not None:
        cfg.log_level = args.log_level.upper()

    return cfg
