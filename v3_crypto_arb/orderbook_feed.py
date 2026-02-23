"""Polymarket CLOB orderbook feed — best bid/ask for active tokens.

Subscribes to the Market Channel WebSocket for token IDs of active
crypto windows. Tracks best_bid_ask events for each token.

Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Subscribe: {"action": "subscribe", "channel": "market",
            "markets": ["<token_id_1>", "<token_id_2>"]}
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

from .config import POLYMARKET_MARKET_WS, Config
from .models import Quote

log = logging.getLogger(__name__)


class OrderbookFeed:
    """Tracks best bid/ask for active crypto up/down tokens.

    Dynamically subscribes to new token IDs as windows are discovered
    by the MarketScanner.

    Usage:
        feed = OrderbookFeed(cfg)
        feed.subscribe_token(token_id)
        await feed.run()
        quote = feed.quote(token_id)
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._quotes: dict[str, Quote] = {}       # token_id → Quote
        self._subscribed: set[str] = set()
        self._pending_subs: list[str] = []
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False

    def subscribe_token(self, token_id: str) -> None:
        """Schedule subscription for a token ID."""
        if token_id in self._subscribed:
            return
        self._subscribed.add(token_id)
        self._quotes.setdefault(token_id, Quote())
        self._pending_subs.append(token_id)

    def quote(self, token_id: str) -> Optional[Quote]:
        return self._quotes.get(token_id)

    def best_ask(self, token_id: str) -> Optional[float]:
        q = self._quotes.get(token_id)
        if q and q.best_ask is not None:
            return q.best_ask
        return None

    def best_bid(self, token_id: str) -> Optional[float]:
        q = self._quotes.get(token_id)
        if q and q.best_bid is not None:
            return q.best_bid
        return None

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self._stream()
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception:
                log.exception("orderbook feed error, reconnecting in 3s")
                await asyncio.sleep(3.0)

    async def stop(self) -> None:
        self._running = False

    # ── Internals ──

    async def _stream(self) -> None:
        log.info("orderbook connecting: %s", POLYMARKET_MARKET_WS)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                POLYMARKET_MARKET_WS, heartbeat=20
            ) as ws:
                self._ws = ws
                log.info("orderbook connected")

                # Subscribe any tokens already registered
                if self._pending_subs:
                    await self._send_subscribe(ws, self._pending_subs)
                    self._pending_subs.clear()

                # Process messages + periodically flush pending subs
                while self._running:
                    # Check for pending subscriptions
                    if self._pending_subs:
                        await self._send_subscribe(ws, self._pending_subs)
                        self._pending_subs.clear()

                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                      aiohttp.WSMsgType.ERROR):
                        log.warning("orderbook ws closed/error")
                        break

                self._ws = None

    async def _send_subscribe(self, ws: aiohttp.ClientWebSocketResponse,
                              token_ids: list[str]) -> None:
        """Send subscription message for token IDs."""
        if not token_ids:
            return
        msg = json.dumps({
            "action": "subscribe",
            "channel": "market",
            "markets": token_ids,
        })
        await ws.send_str(msg)
        log.info("orderbook subscribed to %d tokens", len(token_ids))

    def _handle(self, raw: str) -> None:
        """Parse market channel message.

        Expected events:
        - best_bid_ask: {"event": "best_bid_ask", "market": "<token_id>",
                         "best_bid": "0.55", "best_ask": "0.56"}
        - price_change:  similar structure
        - book:          full orderbook snapshot
        """
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Handle array of events
        events = d if isinstance(d, list) else [d]

        for event in events:
            if not isinstance(event, dict):
                continue
            ev_type = event.get("event_type") or event.get("event", "")

            if ev_type in ("best_bid_ask", "price_change"):
                self._update_quote(event)
            elif ev_type == "book":
                self._handle_book(event)

    def _update_quote(self, event: dict) -> None:  # type: ignore[type-arg]
        """Update quote from best_bid_ask or price_change event."""
        token_id = event.get("market") or event.get("asset_id", "")
        if not token_id or token_id not in self._quotes:
            return

        q = self._quotes[token_id]
        now = time.time()

        bid_str = event.get("best_bid")
        ask_str = event.get("best_ask")

        if bid_str is not None:
            try:
                q.best_bid = float(bid_str)
            except (ValueError, TypeError):
                pass
        if ask_str is not None:
            try:
                q.best_ask = float(ask_str)
            except (ValueError, TypeError):
                pass

        if q.best_bid is not None and q.best_ask is not None:
            q.spread = q.best_ask - q.best_bid
        q.ts = now

    def _handle_book(self, event: dict) -> None:  # type: ignore[type-arg]
        """Extract best bid/ask from full book snapshot."""
        token_id = event.get("market") or event.get("asset_id", "")
        if not token_id or token_id not in self._quotes:
            return

        q = self._quotes[token_id]

        bids = event.get("bids", [])
        asks = event.get("asks", [])

        if bids:
            # Best bid = highest
            try:
                best = max(bids, key=lambda x: float(x.get("price", 0)))
                q.best_bid = float(best["price"])
            except (ValueError, TypeError, KeyError):
                pass

        if asks:
            # Best ask = lowest
            try:
                best = min(asks, key=lambda x: float(x.get("price", 999)))
                q.best_ask = float(best["price"])
            except (ValueError, TypeError, KeyError):
                pass

        if q.best_bid is not None and q.best_ask is not None:
            q.spread = q.best_ask - q.best_bid
        q.ts = time.time()
