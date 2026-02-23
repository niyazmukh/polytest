"""Binance aggTrade WebSocket feed — lowest-latency price source.

Subscribes to Binance aggTrade streams for all configured assets.
Maintains latest price per symbol and feeds the volatility estimator.

Data pipeline position:
    Binance matching engine (~0ms)
  → This listener (~5ms network)
  → [Chainlink DON aggregation: +200-1000ms behind us]
  → [Polymarket RTDS: +300-1200ms behind us]
  → [Polymarket MM adjustment: +500-2000ms behind us]
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from .config import BINANCE_WS, BINANCE_SYMBOLS, Config

log = logging.getLogger(__name__)

# Type for price callbacks: (asset, price, trade_ts, recv_ts)
PriceCallback = Callable[[str, float, float, float], None]


@dataclass(slots=True)
class BinancePrice:
    """Atomic snapshot of latest price for one asset."""
    asset: str
    price: float = 0.0
    trade_ts: float = 0.0   # exchange timestamp (ms→s)
    recv_ts: float = 0.0    # our receipt timestamp


class BinanceFeed:
    """Multi-symbol Binance aggTrade WebSocket listener.

    Usage:
        feed = BinanceFeed(cfg)
        feed.on_price(callback)     # register handler
        await feed.run()            # blocks forever, auto-reconnects
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._prices: dict[str, BinancePrice] = {
            a: BinancePrice(asset=a) for a in cfg.assets
        }
        self._callbacks: list[PriceCallback] = []
        self._running = False

    # ── Public API ──

    def on_price(self, cb: PriceCallback) -> None:
        """Register a callback fired on every trade update."""
        self._callbacks.append(cb)

    def price(self, asset: str) -> Optional[float]:
        """Latest Binance price for asset, or None if no data yet."""
        bp = self._prices.get(asset)
        if bp and bp.price > 0:
            return bp.price
        return None

    def price_snapshot(self, asset: str) -> Optional[BinancePrice]:
        bp = self._prices.get(asset)
        if bp and bp.price > 0:
            return bp
        return None

    def all_prices(self) -> dict[str, float]:
        return {a: bp.price for a, bp in self._prices.items() if bp.price > 0}

    async def run(self) -> None:
        """Connect and stream forever with auto-reconnect."""
        self._running = True
        while self._running:
            try:
                await self._stream()
            except asyncio.CancelledError:
                log.info("binance feed cancelled")
                self._running = False
                return
            except Exception:
                log.exception("binance feed error, reconnecting in 2s")
                await asyncio.sleep(2.0)

    async def stop(self) -> None:
        self._running = False

    # ── Internals ──

    def _build_url(self) -> str:
        """Combined stream URL: wss://stream.binance.com:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade/..."""
        streams = "/".join(
            f"{BINANCE_SYMBOLS[a]}@aggTrade" for a in self._cfg.assets
        )
        return f"{BINANCE_WS}/{streams}"

    async def _stream(self) -> None:
        url = self._build_url()
        log.info("binance connecting: %s", url)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                log.info("binance connected")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                        log.warning("binance ws closed/error: %s", msg.type)
                        break

    def _handle(self, raw: str) -> None:
        """Parse aggTrade message and update state.

        aggTrade payload:
        {
          "e": "aggTrade",
          "E": 1672515782136,    // Event time (ms)
          "s": "BTCUSDT",       // Symbol
          "p": "97123.45",      // Price
          "q": "0.001",         // Quantity
          "T": 1672515782135,   // Trade time (ms)
          "m": false            // Is buyer maker
        }
        """
        recv_ts = time.time()
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        symbol = d.get("s", "").lower()  # e.g. "btcusdt"
        price_str = d.get("p")
        trade_time_ms = d.get("T", 0)

        if not symbol or not price_str:
            return

        # Reverse-lookup asset from symbol
        asset = self._symbol_to_asset(symbol)
        if asset is None:
            return

        price = float(price_str)
        trade_ts = trade_time_ms / 1000.0

        bp = self._prices.get(asset)
        if bp is None:
            return

        bp.price = price
        bp.trade_ts = trade_ts
        bp.recv_ts = recv_ts

        # Fire callbacks (synchronous — keep fast)
        for cb in self._callbacks:
            try:
                cb(asset, price, trade_ts, recv_ts)
            except Exception:
                log.exception("binance price callback error")

    _asset_lookup: dict[str, str] | None = None

    def _symbol_to_asset(self, symbol: str) -> Optional[str]:
        """Reverse lookup: 'btcusdt' → 'btc'."""
        if self._asset_lookup is None:
            self._asset_lookup = {
                v: k for k, v in BINANCE_SYMBOLS.items()
                if k in self._cfg.assets
            }
        return self._asset_lookup.get(symbol)
