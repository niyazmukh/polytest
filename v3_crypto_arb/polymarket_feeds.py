"""Polymarket RTDS WebSocket feed — Chainlink + crypto prices.

Subscribes to two RTDS topics:
  1. crypto_prices_chainlink — the Chainlink oracle price used for resolution
  2. crypto_prices — Binance-sourced reference prices (for comparison)

Primary use: capture the Chainlink start price at each window boundary
so we know the exact reference point the market resolves against.

RTDS endpoint: wss://ws-live-data.polymarket.com
Subscribe message format:
    {"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices_chainlink", "type": "crypto_prices",
         "filters": {"symbols": ["btc/usd", "eth/usd"]}}
    ]}
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

import aiohttp

from .config import POLYMARKET_RTDS_WS, CHAINLINK_SYMBOLS, Config

log = logging.getLogger(__name__)

# Callback: (source, symbol, price, ts)
#  source = "chainlink" | "binance_rtds"
RTDSPriceCallback = Callable[[str, str, float, float], None]


@dataclass(slots=True)
class RTDSPrice:
    """Latest price from one RTDS source for one symbol."""
    source: str       # "chainlink" | "binance_rtds"
    symbol: str       # "btc/usd" or "btcusdt"
    price: float = 0.0
    ts: float = 0.0


class PolymarketRTDSFeed:
    """Polymarket Real-Time Data Service WebSocket listener.

    Captures Chainlink oracle prices (the actual resolution source)
    and Binance reference prices from the RTDS feed.

    Usage:
        feed = PolymarketRTDSFeed(cfg)
        feed.on_price(callback)
        await feed.run()
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._callbacks: list[RTDSPriceCallback] = []
        self._running = False

        # State: latest prices
        self._chainlink: dict[str, RTDSPrice] = {}
        self._binance_rtds: dict[str, RTDSPrice] = {}

    def on_price(self, cb: RTDSPriceCallback) -> None:
        self._callbacks.append(cb)

    def chainlink_price(self, asset: str) -> Optional[float]:
        """Latest Chainlink price for asset (e.g. 'btc')."""
        sym = CHAINLINK_SYMBOLS.get(asset)
        if sym and sym in self._chainlink:
            p = self._chainlink[sym]
            if p.price > 0:
                return p.price
        return None

    def chainlink_snapshot(self, asset: str) -> Optional[RTDSPrice]:
        sym = CHAINLINK_SYMBOLS.get(asset)
        if sym:
            return self._chainlink.get(sym)
        return None

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._stream()
            except asyncio.CancelledError:
                log.info("rtds feed cancelled")
                self._running = False
                return
            except Exception:
                log.exception("rtds feed error, reconnecting in 3s")
                await asyncio.sleep(3.0)

    async def stop(self) -> None:
        self._running = False

    # ── Internals ──

    def _subscription_msg(self) -> str:
        """Build RTDS subscription JSON."""
        chainlink_symbols = [
            CHAINLINK_SYMBOLS[a] for a in self._cfg.assets
            if a in CHAINLINK_SYMBOLS
        ]
        # Binance-source symbols on RTDS use 'btcusdt' format
        binance_symbols = [f"{a}usdt" for a in self._cfg.assets]

        subs = []
        if chainlink_symbols:
            subs.append({
                "topic": "crypto_prices_chainlink",
                "type": "crypto_prices",
                "filters": {"symbols": chainlink_symbols},
            })
        if binance_symbols:
            subs.append({
                "topic": "crypto_prices",
                "type": "crypto_prices",
                "filters": {"symbols": binance_symbols},
            })

        return json.dumps({"action": "subscribe", "subscriptions": subs})

    async def _stream(self) -> None:
        log.info("rtds connecting: %s", POLYMARKET_RTDS_WS)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                POLYMARKET_RTDS_WS, heartbeat=30
            ) as ws:
                log.info("rtds connected, subscribing")
                await ws.send_str(self._subscription_msg())

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                        log.warning("rtds ws closed/error: %s", msg.type)
                        break

    def _handle(self, raw: str) -> None:
        """Parse RTDS price message.

        Expected format (observed from Polymarket):
        {
          "topic": "crypto_prices_chainlink",
          "data": {
            "symbol": "btc/usd",
            "price": "97123.45",
            "timestamp": 1672515782
          }
        }
        or for crypto_prices:
        {
          "topic": "crypto_prices",
          "data": {
            "symbol": "btcusdt",
            "price": "97123.45",
            "timestamp": 1672515782
          }
        }
        """
        recv_ts = time.time()
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        topic = d.get("topic", "")
        data = d.get("data")
        if not data or not isinstance(data, dict):
            # Could be subscription ack or heartbeat
            return

        symbol = data.get("symbol", "")
        price_str = data.get("price")
        if not symbol or not price_str:
            return

        try:
            price = float(price_str)
        except (ValueError, TypeError):
            return

        if topic == "crypto_prices_chainlink":
            source = "chainlink"
            entry = self._chainlink.setdefault(
                symbol, RTDSPrice(source="chainlink", symbol=symbol)
            )
        elif topic == "crypto_prices":
            source = "binance_rtds"
            entry = self._binance_rtds.setdefault(
                symbol, RTDSPrice(source="binance_rtds", symbol=symbol)
            )
        else:
            return

        entry.price = price
        entry.ts = recv_ts

        for cb in self._callbacks:
            try:
                cb(source, symbol, price, recv_ts)
            except Exception:
                log.exception("rtds price callback error")
