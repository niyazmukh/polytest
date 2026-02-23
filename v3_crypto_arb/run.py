"""Entry point — wires all components and runs the crypto arb listener.

Architecture:
    ┌─────────────┐
    │ Binance WS   │──aggTrade──→ VolatilityEstimator
    │ (lowest lat) │──price────→ ArbEngine.tick()
    └─────────────┘
    ┌─────────────┐
    │ RTDS WS      │──chainlink──→ start price capture
    └─────────────┘
    ┌─────────────┐
    │ Gamma API    │──poll──→ MarketScanner → new windows
    └─────────────┘                              ↓
    ┌─────────────┐                    OrderbookFeed.subscribe()
    │ Market WS    │──bid/ask──→ ArbEngine quotes
    └─────────────┘
                                 ArbEngine
                                    ↓
                              SignalEmitter → stdout (JSONL)

Usage:
    python -m v3_crypto_arb
    python -m v3_crypto_arb --assets btc eth --edge-threshold 0.05
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from .arb_engine import ArbEngine
from .binance_feed import BinanceFeed
from .config import Config, parse_args
from .market_scanner import MarketScanner
from .models import CryptoWindow
from .orderbook_feed import OrderbookFeed
from .polymarket_feeds import PolymarketRTDSFeed
from .signal_emitter import SignalEmitter
from .volatility import VolatilityEstimator

log = logging.getLogger(__name__)


class CryptoArbRunner:
    """Orchestrates all components."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

        # Components
        self.binance = BinanceFeed(cfg)
        self.rtds = PolymarketRTDSFeed(cfg)
        self.scanner = MarketScanner(cfg)
        self.orderbook = OrderbookFeed(cfg)
        self.engine = ArbEngine(cfg)
        self.emitter = SignalEmitter()

        # Per-asset volatility estimators
        self.vols: dict[str, VolatilityEstimator] = {
            a: VolatilityEstimator(
                window_s=cfg.vol_window_s,
                default_vol=cfg.default_vol(a),
            )
            for a in cfg.assets
        }

        self._wire()

    def _wire(self) -> None:
        """Connect callbacks between components."""

        # Binance → volatility + arb engine
        def on_binance_price(asset: str, price: float,
                             trade_ts: float, recv_ts: float) -> None:
            # Update volatility estimator
            vol = self.vols.get(asset)
            if vol:
                vol.add_trade(price, trade_ts)

            # Tick the arb engine (this is the hot path)
            self.engine.tick(asset, price, recv_ts)

        self.binance.on_price(on_binance_price)

        # RTDS → capture start prices (handled via engine's chainlink source)
        # We also record chainlink prices on the CryptoWindow objects
        def on_rtds_price(source: str, symbol: str, price: float, ts: float) -> None:
            if source != "chainlink":
                return
            # Update start prices on active windows that haven't been set yet
            for window in self.scanner.active_windows.values():
                if window.chainlink_symbol == symbol and window.chainlink_start_price is None:
                    elapsed = ts - window.window_start_ts
                    if elapsed <= self.cfg.start_price_grace_s:
                        window.chainlink_start_price = price
                        log.info("captured chainlink start price for %s: %.2f",
                                 window.slug, price)

        self.rtds.on_price(on_rtds_price)

        # Scanner → orderbook subscriptions
        def on_new_window(window: CryptoWindow) -> None:
            self.orderbook.subscribe_token(window.token_id_up)
            self.orderbook.subscribe_token(window.token_id_down)
            log.info("subscribed orderbook for %s", window.slug)

        self.scanner.on_new_window(on_new_window)

        # Arb engine wiring
        self.engine.set_windows_source(lambda: self.scanner.active_windows)
        self.engine.set_quote_source(self.orderbook.quote)
        self.engine.set_vol_source(
            lambda asset: self.vols[asset].vol_annualized()
            if asset in self.vols else self.cfg.default_vol(asset)
        )
        self.engine.set_chainlink_source(self.rtds.chainlink_price)

        # Engine → emitter
        self.engine.on_signal(self.emitter.emit)

    async def run(self) -> None:
        """Start all components concurrently."""
        log.info("starting crypto arb listener")
        log.info("assets: %s", list(self.cfg.assets))
        log.info("edge threshold: %.4f", self.cfg.edge_threshold)
        log.info("min displacement: %.1f bps", self.cfg.min_displacement_bps)

        tasks = [
            asyncio.create_task(self.binance.run(), name="binance"),
            asyncio.create_task(self.rtds.run(), name="rtds"),
            asyncio.create_task(self.scanner.run(), name="scanner"),
            asyncio.create_task(self.orderbook.run(), name="orderbook"),
        ]

        try:
            # Wait for any task to complete (they should all run forever)
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if t.exception():
                    log.error("task %s failed: %s", t.get_name(), t.exception())
        except asyncio.CancelledError:
            log.info("runner cancelled")
        finally:
            # Cancel all tasks
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info("all tasks stopped. signals emitted: %d",
                     self.emitter.signal_count)


def main() -> None:
    cfg = parse_args()

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    runner = CryptoArbRunner(cfg)

    loop = asyncio.new_event_loop()

    # Graceful shutdown on SIGINT/SIGTERM
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: loop.stop())
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        loop.run_until_complete(runner.run())
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
