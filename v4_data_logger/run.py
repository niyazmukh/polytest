"""Entry point — wires v3 feeds to the data recorder.

Architecture:
    ┌────────────────┐
    │  Binance WS     │──aggTrade──→ Recorder (binance.jsonl)
    │  (from v3)      │             LatencyTracker
    └────────────────┘
    ┌────────────────┐
    │  RTDS WS        │──chainlink──→ Recorder (chainlink.jsonl)
    │  (from v3)      │               LatencyTracker → cross_lag.jsonl
    └────────────────┘
    ┌────────────────┐
    │  Gamma API      │──poll──→ MarketScanner → windows.jsonl
    │  (from v3)      │
    └────────────────┘
    ┌────────────────┐
    │  Market WS      │──bid/ask──→ Recorder (orderbook.jsonl)
    │  (instrumented) │              LatencyTracker → cross_lag.jsonl
    └────────────────┘
                        ┌────────────────┐
                        │ ResolutionPoller│──→ windows.jsonl (resolution)
                        └────────────────┘

Usage:
    python -m v4_data_logger
    python -m v4_data_logger --assets btc eth --binance-sample-rate 10
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Optional

from v3_crypto_arb.binance_feed import BinanceFeed
from v3_crypto_arb.config import Config as V3Config
from v3_crypto_arb.market_scanner import MarketScanner
from v3_crypto_arb.models import CryptoWindow, Quote
from v3_crypto_arb.orderbook_feed import OrderbookFeed
from v3_crypto_arb.polymarket_feeds import PolymarketRTDSFeed, RTDSPrice

from .config import LoggerConfig, parse_args
from .latency_tracker import LatencyTracker
from .recorder import EventRecorder
from .resolution_poller import ResolutionPoller

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Instrumented feeds (enrich v3 feeds without modifying v3)
# ──────────────────────────────────────────────────────────────


class InstrumentedBinanceFeed(BinanceFeed):
    """BinanceFeed enriched with trade qty + is_buyer_maker.

    Overrides ``_handle`` to extract the ``q`` (quantity) and ``m``
    (is_buyer_maker) fields from Binance aggTrade messages that
    the v3 base class ignores.
    """

    def __init__(self, cfg: object) -> None:
        super().__init__(cfg)  # type: ignore[arg-type]
        self._enriched_cbs: list[object] = []

    def on_enriched_tick(self, cb: object) -> None:
        """Register: cb(asset, price, trade_ts, recv_ts, qty, is_buyer_maker)."""
        self._enriched_cbs.append(cb)

    def _handle(self, raw: str) -> None:
        recv_ts = time.time()
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        symbol = d.get("s", "").lower()
        price_str = d.get("p")
        qty_str = d.get("q", "0")
        trade_time_ms = d.get("T", 0)
        is_buyer_maker = bool(d.get("m", False))

        if not symbol or not price_str:
            return

        asset = self._symbol_to_asset(symbol)
        if asset is None:
            return

        price = float(price_str)
        qty = float(qty_str)
        trade_ts = trade_time_ms / 1000.0

        bp = self._prices.get(asset)
        if bp is None:
            return
        bp.price = price
        bp.trade_ts = trade_ts
        bp.recv_ts = recv_ts

        for cb in self._enriched_cbs:
            try:
                cb(asset, price, trade_ts, recv_ts, qty, is_buyer_maker)  # type: ignore[operator]
            except Exception:
                log.exception("enriched binance callback error")


class InstrumentedRTDSFeed(PolymarketRTDSFeed):
    """RTDSFeed enriched with origin + server timestamps.

    The RTDS message has two timestamps we capture:
      - wrapper ``timestamp`` (ms): when RTDS server sent it
      - payload ``timestamp`` (ms): when the source recorded the price
    The v3 base class discards both and uses ``time.time()`` only.

    Also fixes the subscription format to match current Polymarket docs:
      - Binance: type="update", filters="btcusdt,ethusdt,..."
      - Chainlink: type="*", filters=""
    """

    def __init__(self, cfg: object) -> None:
        super().__init__(cfg)  # type: ignore[arg-type]
        self._enriched_cbs: list[object] = []

    def on_enriched_price(self, cb: object) -> None:
        """Register: cb(source, symbol, price, recv_ts, origin_ts, server_ts)."""
        self._enriched_cbs.append(cb)

    def _subscription_msg(self) -> str:
        """Override to use current RTDS subscription format.

        Docs format:
          filters for Binance = comma-separated string: "btcusdt,ethusdt"
          filters for Chainlink = "" (all) or JSON string: '{"symbol":"btc/usd"}'
          type for Binance = "update"
          type for Chainlink = "*"
        """
        from v3_crypto_arb.config import CHAINLINK_SYMBOLS

        chainlink_symbols = [
            CHAINLINK_SYMBOLS[a] for a in self._cfg.assets
            if a in CHAINLINK_SYMBOLS
        ]
        binance_symbols = [f"{a}usdt" for a in self._cfg.assets]

        subs: list[dict[str, str]] = []
        if chainlink_symbols:
            subs.append({
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "",  # subscribe to all chainlink symbols
            })
        if binance_symbols:
            subs.append({
                "topic": "crypto_prices",
                "type": "update",
                "filters": "",  # subscribe to all (comma format rejected by API)
            })

        return json.dumps({"action": "subscribe", "subscriptions": subs})

    def _handle(self, raw: str) -> None:
        recv_ts = time.time()
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        topic = d.get("topic", "")

        # Server timestamp (when RTDS sent the message)
        server_ts_raw = d.get("timestamp", 0)
        if isinstance(server_ts_raw, (int, float)) and server_ts_raw > 1e12:
            server_ts = server_ts_raw / 1000.0  # milliseconds → seconds
        elif isinstance(server_ts_raw, (int, float)) and server_ts_raw > 1e9:
            server_ts = float(server_ts_raw)     # already seconds
        else:
            server_ts = 0.0

        # Handle both observed v3 format ("data") and docs format ("payload")
        data = d.get("data") or d.get("payload")
        if not data or not isinstance(data, dict):
            return

        symbol = data.get("symbol", "")
        price_raw = data.get("price") or data.get("value")
        if not symbol or price_raw is None:
            return

        try:
            price = float(price_raw)
        except (ValueError, TypeError):
            return

        # Origin timestamp (when source recorded the price)
        origin_ts_raw = data.get("timestamp", 0)
        if isinstance(origin_ts_raw, (int, float)) and origin_ts_raw > 1e12:
            origin_ts = origin_ts_raw / 1000.0  # milliseconds → seconds
        elif isinstance(origin_ts_raw, (int, float)) and origin_ts_raw > 1e9:
            origin_ts = float(origin_ts_raw)  # already seconds
        else:
            origin_ts = 0.0

        if topic == "crypto_prices_chainlink":
            source = "chainlink"
            entry = self._chainlink.setdefault(
                symbol, RTDSPrice(source="chainlink", symbol=symbol),
            )
        elif topic == "crypto_prices":
            source = "binance_rtds"
            entry = self._binance_rtds.setdefault(
                symbol, RTDSPrice(source="binance_rtds", symbol=symbol),
            )
        else:
            return

        entry.price = price
        entry.ts = recv_ts

        for cb in self._enriched_cbs:
            try:
                cb(source, symbol, price, recv_ts, origin_ts, server_ts)  # type: ignore[operator]
            except Exception:
                log.exception("enriched rtds callback error")


class InstrumentedOrderbookFeed(OrderbookFeed):
    """OrderbookFeed enriched with sizes, trades, and WS resolutions.

    Enhancements over v3 base class:
      - Extracts bid/ask sizes from ``book`` snapshots
      - Captures exchange timestamps from event ``timestamp`` field
      - Intercepts ``last_trade_price`` events (Polymarket fills)
      - Intercepts ``market_resolved`` events (instant resolution)
      - Subscribes with ``custom_feature_enabled: true``
    """

    def __init__(self, cfg: object) -> None:
        super().__init__(cfg)  # type: ignore[arg-type]
        self._change_cbs: list[object] = []
        self._trade_cbs: list[object] = []
        self._resolution_cbs: list[object] = []
        self._bid_sizes: dict[str, Optional[float]] = {}
        self._ask_sizes: dict[str, Optional[float]] = {}

    def on_quote_change(self, cb: object) -> None:
        """Register: cb(token_id, quote, bid_size, ask_size, exchange_ts)."""
        self._change_cbs.append(cb)

    def on_trade(self, cb: object) -> None:
        """Register: cb(token_id, price, side, size, fee_bps, exchange_ts)."""
        self._trade_cbs.append(cb)

    def on_ws_resolution(self, cb: object) -> None:
        """Register: cb(cond_id, winning_asset_id, winning_outcome, exchange_ts)."""
        self._resolution_cbs.append(cb)

    async def _send_subscribe(  # type: ignore[override]
        self,
        ws: object,
        token_ids: list[str],
    ) -> None:
        """Override to use modern format with custom_feature_enabled."""
        import aiohttp

        if not token_ids:
            return
        msg = json.dumps({
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        })
        await ws.send_str(msg)  # type: ignore[union-attr]
        log.info("orderbook subscribed to %d tokens (custom_feature)", len(token_ids))

    def _handle(self, raw: str) -> None:
        """Override to intercept trade + resolution + enrich quotes."""
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return

        events = d if isinstance(d, list) else [d]
        recv_ts = time.time()

        for event in events:
            if not isinstance(event, dict):
                continue
            ev_type = event.get("event_type") or event.get("event", "")

            if ev_type in ("best_bid_ask", "price_change"):
                ex_ts = self._parse_exchange_ts(event)
                self._update_quote(event)  # type: ignore[arg-type]
                tid = event.get("asset_id") or event.get("market", "")
                self._fire(tid, ex_ts)
            elif ev_type == "book":
                ex_ts = self._parse_exchange_ts(event)
                self._handle_book_enriched(event)
                tid = event.get("asset_id") or event.get("market", "")
                self._fire(tid, ex_ts)
            elif ev_type == "last_trade_price":
                self._handle_trade(event, recv_ts)
            elif ev_type == "market_resolved":
                self._handle_ws_resolution(event, recv_ts)

    @staticmethod
    def _parse_exchange_ts(event: dict[str, object]) -> Optional[float]:
        ts_raw = event.get("timestamp")
        if ts_raw is None:
            return None
        try:
            ts_val = float(ts_raw)  # type: ignore[arg-type]
            return ts_val / 1000.0 if ts_val > 1e12 else ts_val
        except (ValueError, TypeError):
            return None

    def _update_quote(self, event: dict) -> None:  # type: ignore[override, type-arg]
        """Override to prefer ``asset_id`` over ``market`` for token lookup.

        The v4 subscription format (``assets_ids`` + ``custom_feature_enabled``)
        causes Polymarket to return events with ``asset_id`` as the token
        identifier.  The v3 base class looks for ``market`` first, which
        may contain the *condition* id — causing a lookup miss and leaving
        the Quote object uninitialised (bid=None, ask=None, ts=0).
        """
        token_id = str(event.get("asset_id") or event.get("market", ""))
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

    def _handle_book_enriched(self, event: dict[str, object]) -> None:
        """Extract best bid/ask with sizes from full book snapshot.

        Does NOT delegate to ``super()._handle_book`` because the v3
        method looks for ``market`` first, which may resolve to the
        condition id instead of the token id under the v4 subscription
        format.  Instead, updates the Quote directly using ``asset_id``.
        """
        tid = str(event.get("asset_id") or event.get("market", ""))
        if not tid or tid not in self._quotes:
            return

        q = self._quotes[tid]

        bids = event.get("bids", [])
        asks = event.get("asks", [])

        if bids and isinstance(bids, list):
            try:
                best = max(bids, key=lambda x: float(x.get("price", 0)))  # type: ignore[union-attr]
                q.best_bid = float(best["price"])  # type: ignore[call-overload]
                self._bid_sizes[tid] = float(best.get("size", 0))  # type: ignore[union-attr]
            except (ValueError, TypeError, KeyError, AttributeError):
                pass

        if asks and isinstance(asks, list):
            try:
                best = min(asks, key=lambda x: float(x.get("price", 999)))  # type: ignore[union-attr]
                q.best_ask = float(best["price"])  # type: ignore[call-overload]
                self._ask_sizes[tid] = float(best.get("size", 0))  # type: ignore[union-attr]
            except (ValueError, TypeError, KeyError, AttributeError):
                pass

        if q.best_bid is not None and q.best_ask is not None:
            q.spread = q.best_ask - q.best_bid
        q.ts = time.time()

    def _handle_trade(self, event: dict[str, object], recv_ts: float) -> None:
        """Process last_trade_price event."""
        tid = str(event.get("asset_id", ""))
        if not tid:
            return
        try:
            price = float(event.get("price", 0))  # type: ignore[arg-type]
            side = str(event.get("side", ""))
            size = float(event.get("size", 0))  # type: ignore[arg-type]
            fee_bps = int(event.get("fee_rate_bps", 0))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return
        ex_ts = self._parse_exchange_ts(event) or recv_ts  # type: ignore[arg-type]
        for cb in self._trade_cbs:
            try:
                cb(tid, price, side, size, fee_bps, ex_ts)  # type: ignore[operator]
            except Exception:
                log.exception("trade callback error")

    def _handle_ws_resolution(self, event: dict[str, object],
                              recv_ts: float) -> None:
        """Process market_resolved event pushed via WebSocket."""
        cond_id = str(event.get("market", ""))
        winning_asset = str(event.get("winning_asset_id", ""))
        winning_outcome = str(event.get("winning_outcome", ""))
        ex_ts = self._parse_exchange_ts(event) or recv_ts  # type: ignore[arg-type]
        for cb in self._resolution_cbs:
            try:
                cb(cond_id, winning_asset, winning_outcome, ex_ts)  # type: ignore[operator]
            except Exception:
                log.exception("ws resolution callback error")

    def _fire(self, token_id: str,
              exchange_ts: Optional[float] = None) -> None:
        q = self._quotes.get(token_id)
        if q is None:
            return
        bid_sz = self._bid_sizes.get(token_id)
        ask_sz = self._ask_sizes.get(token_id)
        for cb in self._change_cbs:
            try:
                cb(token_id, q, bid_sz, ask_sz, exchange_ts)  # type: ignore[operator]
            except Exception:
                log.exception("quote change callback error")


class InstrumentedMarketScanner(MarketScanner):
    """MarketScanner that uses timestamp-based exact slug queries.

    The Gamma API only supports exact slug matching — ``slug=btc-updown-5m``
    returns 0 results because the full slug is ``btc-updown-5m-{timestamp}``.
    This override generates candidate timestamps for current + upcoming
    windows and queries each exact slug.
    """

    async def _poll_asset(  # type: ignore[override]
        self,
        session: object,
        asset: str,
        dur_label: str,
        now: float,
    ) -> None:
        """Query exact slugs for current ± nearby window timestamps."""
        import aiohttp as _aio
        from v3_crypto_arb.config import GAMMA_API

        dur_s = 300 if dur_label == "5m" else 900
        base_ts = int(now) // dur_s * dur_s

        # Check 2 past + 3 future windows (6 queries per asset/duration)
        for offset in range(-2, 4):
            window_ts = base_ts + offset * dur_s
            slug = f"{asset}-updown-{dur_label}-{window_ts}"

            # Skip if we already know this window
            if slug in self._windows:
                continue

            url = (
                f"{GAMMA_API}/events"
                f"?slug={slug}"
                f"&active=true"
                f"&closed=false"
                f"&limit=1"
            )
            try:
                async with session.get(  # type: ignore[union-attr]
                    url, timeout=_aio.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        continue
                    events = await resp.json()
            except Exception:
                log.debug("gamma api error for %s", slug)
                continue

            if not isinstance(events, list):
                continue
            for event in events:
                self._process_event(event, now)


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────


class DataLoggerRunner:
    """Orchestrates all components for data collection."""

    def __init__(self, cfg: LoggerConfig) -> None:
        self.cfg = cfg
        self._v3_cfg = self._make_v3_config()

        # Instrumented feeds (enriched v3 feeds)
        self.binance = InstrumentedBinanceFeed(self._v3_cfg)
        self.rtds = InstrumentedRTDSFeed(self._v3_cfg)
        self.scanner = InstrumentedMarketScanner(self._v3_cfg)
        self.orderbook = InstrumentedOrderbookFeed(self._v3_cfg)

        # v4 components
        self.recorder = EventRecorder(cfg.output_dir)
        self.tracker = LatencyTracker()
        self.resolver = ResolutionPoller(cfg)

        # State
        self._tick_counts: dict[str, int] = {}
        self._wire()

    def _make_v3_config(self) -> V3Config:
        """Construct a v3 Config for the feed components."""
        return V3Config(
            assets=self.cfg.assets,
            window_durations=self.cfg.window_durations,
            gamma_poll_interval_s=self.cfg.gamma_poll_interval_s,
            start_price_grace_s=self.cfg.start_price_grace_s,
        )

    def _wire(self) -> None:
        """Connect all callbacks between feeds and recorder/tracker."""
        sample = self.cfg.binance_sample_rate

        # ── Binance → recorder + tracker (enriched: includes qty) ──

        def on_binance(asset: str, price: float, trade_ts: float,
                       recv_ts: float, qty: float,
                       is_buyer_maker: bool) -> None:
            # Full fidelity for tracker (now with qty for displacement sizing)
            self.tracker.on_binance_tick(asset, price, trade_ts, recv_ts, qty)

            # JSONL is sampled
            cnt = self._tick_counts.get(asset, 0) + 1
            self._tick_counts[asset] = cnt
            if cnt % sample == 0:
                self.recorder.write_binance_tick(
                    asset, price, trade_ts, recv_ts, qty, is_buyer_maker,
                )

        self.binance.on_enriched_tick(on_binance)

        # ── RTDS → recorder + tracker (enriched: origin + server ts) ──

        def on_rtds(source: str, symbol: str, price: float,
                    recv_ts: float, origin_ts: float,
                    server_ts: float) -> None:
            if source == "chainlink":
                self.recorder.write_chainlink(
                    symbol, price, recv_ts, origin_ts, server_ts,
                )

                # Cross-lag observation (pass origin_ts for true source timing)
                obs = self.tracker.on_chainlink_update(
                    symbol, price, recv_ts, origin_ts,
                )
                if obs:
                    self.recorder.write_cross_lag(
                        obs.asset, obs.binance_price, obs.chainlink_price,
                        obs.bn_origin_ts, obs.bn_recv_ts,
                        obs.cl_origin_ts, obs.cl_recv_ts,
                    )

                # Capture start prices on active windows
                for w in self.scanner.active_windows.values():
                    if (w.chainlink_symbol == symbol
                            and w.chainlink_start_price is None):
                        elapsed = recv_ts - w.window_start_ts
                        if elapsed <= self.cfg.start_price_grace_s:
                            w.chainlink_start_price = price
                            self.recorder.write_start_price(
                                w.slug, "chainlink", price, recv_ts,
                            )
            elif source == "binance_rtds":
                # Record RTDS Binance mirror for pipeline latency analysis
                self.recorder.write_rtds_binance(
                    symbol, price, origin_ts, server_ts, recv_ts,
                )

        self.rtds.on_enriched_price(on_rtds)

        # ── Scanner → window tracking ──

        def on_new_window(window: CryptoWindow) -> None:
            self.orderbook.subscribe_token(window.token_id_up)
            self.orderbook.subscribe_token(window.token_id_down)

            self.tracker.register_token(
                window.token_id_up, window.asset, window.slug,
            )
            self.tracker.register_token(
                window.token_id_down, window.asset, window.slug,
            )

            self.recorder.write_window_open(
                slug=window.slug,
                asset=window.asset,
                duration_s=window.duration_seconds,
                start_ts=window.window_start_ts,
                end_ts=window.window_end_ts,
                token_up=window.token_id_up,
                token_down=window.token_id_down,
                condition_id=window.condition_id,
            )

            self.resolver.track_window(
                window.slug, window.condition_id, window.asset,
                window.window_end_ts, window.token_id_up,
                window.token_id_down,
            )
            log.info("tracking window: %s", window.slug)

        self.scanner.on_new_window(on_new_window)

        # ── Orderbook → recorder + tracker (enriched: sizes + exchange_ts) ──

        def on_quote(token_id: str, q: Quote,
                     bid_size: float | None,
                     ask_size: float | None,
                     exchange_ts: float | None) -> None:
            self.recorder.write_orderbook(
                token_id, q.best_bid, q.best_ask, q.ts,
                bid_size, ask_size, exchange_ts,
            )
            obs = self.tracker.on_quote_change(
                token_id, q.best_bid, q.best_ask, q.ts,
            )
            if obs:
                self.recorder.write_orderbook_lag(
                    obs.token_id, obs.slug,
                    obs.binance_move_ts, obs.quote_change_ts,
                    obs.displacement_bps, obs.displacement_qty,
                )

        self.orderbook.on_quote_change(on_quote)

        # ── Trades → recorder (Polymarket fill events) ──

        def on_trade(token_id: str, price: float, side: str,
                     size: float, fee_bps: int,
                     exchange_ts: float) -> None:
            self.recorder.write_trade(
                token_id, price, side, size, fee_bps,
                exchange_ts, time.time(),
            )

        self.orderbook.on_trade(on_trade)

        # ── WS Resolution → recorder (instant push, no polling) ──

        def on_ws_resolution(condition_id: str, winning_asset_id: str,
                             winning_outcome: str,
                             exchange_ts: float) -> None:
            self.recorder.write_ws_resolution(
                condition_id, winning_asset_id, winning_outcome,
                exchange_ts, time.time(),
            )

        self.orderbook.on_ws_resolution(on_ws_resolution)

        # ── Resolution Poller → recorder (fallback) ──

        def on_resolution(slug: str, condition_id: str,
                          outcome: str, outcome_prices: str) -> None:
            self.recorder.write_resolution(
                slug, condition_id, outcome, outcome_prices,
                time.time(),
            )

        self.resolver.on_resolution(on_resolution)

    async def run(self) -> None:
        """Start all components concurrently."""
        log.info("=" * 60)
        log.info("  v4 data logger starting")
        log.info("=" * 60)
        log.info("assets:       %s", list(self.cfg.assets))
        log.info("output:       %s", self.cfg.output_dir)
        log.info("sample rate:  1/%d ticks", self.cfg.binance_sample_rate)

        tasks = [
            asyncio.create_task(self.binance.run(), name="binance"),
            asyncio.create_task(self.rtds.run(), name="rtds"),
            asyncio.create_task(self.scanner.run(), name="scanner"),
            asyncio.create_task(self.orderbook.run(), name="orderbook"),
            asyncio.create_task(self.resolver.run(), name="resolver"),
            asyncio.create_task(self._stats_loop(), name="stats"),
        ]

        try:
            done, _pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED,
            )
            for t in done:
                exc = t.exception()
                if exc:
                    log.error("task %s failed: %s", t.get_name(), exc)
        except asyncio.CancelledError:
            log.info("runner cancelled")
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.recorder.close()
            log.info("data logger stopped — totals: %s", self.recorder.counts)

    async def _stats_loop(self) -> None:
        """Periodically log collection statistics."""
        while True:
            await asyncio.sleep(self.cfg.stats_interval_s)
            c = self.recorder.counts
            up = self.recorder.uptime_s
            ticks = sum(self._tick_counts.values())
            log.info(
                "STATS [%.0fs] ticks=%d(logged=%d) chainlink=%d "
                "orderbook=%d trades=%d rtds_bn=%d windows=%d cross_lag=%d "
                "pending_res=%d resolved=%d",
                up, ticks, c.get("binance", 0), c.get("chainlink", 0),
                c.get("orderbook", 0), c.get("trades", 0),
                c.get("rtds_binance", 0), c.get("windows", 0),
                c.get("cross_lag", 0),
                self.resolver.pending_count, self.resolver.resolved_count,
            )
            self.recorder.flush()


def main() -> None:
    """CLI entry point."""
    cfg = parse_args()

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    runner = DataLoggerRunner(cfg)

    loop = asyncio.new_event_loop()

    for sig_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig_name, lambda: loop.stop())
        except NotImplementedError:
            pass  # Windows

    try:
        loop.run_until_complete(runner.run())
    except KeyboardInterrupt:
        log.info("interrupted")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
