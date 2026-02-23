"""Cross-source latency tracker.

Maintains state to measure timing relationships between:
  1. Binance aggTrade → Chainlink RTDS (price oracle lag)
  2. Binance moves → Orderbook quote adjustment (MM response lag)

Stores BOTH origin timestamps (from exchange/oracle) AND receive
timestamps (when our process received the data) so that analysis
can distinguish genuine source-level timing from pipeline latency.

Does NOT write to files — returns observations for the recorder.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from .config import CHAINLINK_SYMBOLS

log = logging.getLogger(__name__)

# Reverse: "btc/usd" → "btc"
_CHAINLINK_TO_ASSET = {v: k for k, v in CHAINLINK_SYMBOLS.items()}


@dataclass(slots=True)
class CrossLagObs:
    """One Binance-vs-Chainlink latency observation.

    Four timestamps track the full provenance chain:
      bn_origin_ts — Binance aggTrade ``T`` field (exchange timestamp)
      bn_recv_ts   — when our process received the Binance tick
      cl_origin_ts — Chainlink oracle timestamp from RTDS payload
      cl_recv_ts   — when our process received the Chainlink update
    """
    asset: str
    binance_price: float
    chainlink_price: float
    bn_origin_ts: float
    bn_recv_ts: float
    cl_origin_ts: float
    cl_recv_ts: float


@dataclass(slots=True)
class OBLagObs:
    """One orderbook adjustment lag observation."""
    token_id: str
    slug: str
    displacement_bps: float
    binance_move_ts: float
    quote_change_ts: float
    displacement_qty: float = 0.0


class LatencyTracker:
    """Tracks cross-source timing for latency measurement.

    Usage:
        tracker = LatencyTracker()
        tracker.register_token(token_id, asset, slug)
        tracker.on_binance_tick(asset, price, trade_ts, recv_ts)
        obs = tracker.on_chainlink_update(symbol, price, recv_ts, origin_ts)
        obs = tracker.on_quote_change(token_id, bid, ask, recv_ts)
    """

    def __init__(self) -> None:
        # Latest Binance state per asset (both origin + receive timestamps)
        self._bn_price: dict[str, float] = {}
        self._bn_trade_ts: dict[str, float] = {}   # exchange timestamp
        self._bn_recv_ts: dict[str, float] = {}     # our receive timestamp

        # Displacement detection
        self._last_price: dict[str, float] = {}
        self._last_move_ts: dict[str, float] = {}
        self._last_move_bps: dict[str, float] = {}
        self._last_move_qty: dict[str, float] = {}
        self._displacement_thresh_bps: float = 5.0

        # Token → asset / slug mapping
        self._token_asset: dict[str, str] = {}
        self._token_slug: dict[str, str] = {}

        # Previous quote state for change detection
        self._prev_bid: dict[str, Optional[float]] = {}
        self._prev_ask: dict[str, Optional[float]] = {}

    def register_token(self, token_id: str, asset: str, slug: str) -> None:
        """Map a token ID to its asset and slug for lag tracking."""
        self._token_asset[token_id] = asset
        self._token_slug[token_id] = slug

    def on_binance_tick(self, asset: str, price: float,
                        trade_ts: float, recv_ts: float,
                        qty: float = 0.0) -> None:
        """Record latest Binance state, detect displacement events."""
        self._bn_price[asset] = price
        self._bn_trade_ts[asset] = trade_ts
        self._bn_recv_ts[asset] = recv_ts

        # Detect single-tick displacement (> threshold bps)
        prev = self._last_price.get(asset)
        if prev and prev > 0:
            bps = abs(price - prev) / prev * 10_000
            if bps >= self._displacement_thresh_bps:
                self._last_move_ts[asset] = recv_ts
                self._last_move_bps[asset] = bps
                self._last_move_qty[asset] = qty
        self._last_price[asset] = price

    def on_chainlink_update(self, symbol: str, price: float,
                            recv_ts: float,
                            origin_ts: float = 0.0) -> Optional[CrossLagObs]:
        """Compare Chainlink update with latest Binance price.

        Args:
            symbol: Chainlink symbol (e.g. "btc/usd")
            price: Chainlink oracle price
            recv_ts: when we received this Chainlink message
            origin_ts: Chainlink oracle timestamp from RTDS payload
                       (integer-second precision, may be rounded up)

        Returns a CrossLagObs if we have a matching Binance price,
        else None.
        """
        asset = _CHAINLINK_TO_ASSET.get(symbol)
        if not asset:
            return None
        bn_price = self._bn_price.get(asset)
        bn_trade_ts = self._bn_trade_ts.get(asset)
        bn_recv_ts = self._bn_recv_ts.get(asset)
        if bn_price is None or bn_recv_ts is None or bn_trade_ts is None:
            return None
        return CrossLagObs(
            asset=asset,
            binance_price=bn_price,
            chainlink_price=price,
            bn_origin_ts=bn_trade_ts,
            bn_recv_ts=bn_recv_ts,
            cl_origin_ts=origin_ts,
            cl_recv_ts=recv_ts,
        )

    def on_quote_change(self, token_id: str,
                        bid: Optional[float], ask: Optional[float],
                        recv_ts: float) -> Optional[OBLagObs]:
        """Check if a quote change follows a recent displacement.

        Returns an OBLagObs if the quote actually changed AND
        there was a recent Binance displacement for the same asset.
        """
        # Detect actual change vs duplicate
        prev_b = self._prev_bid.get(token_id)
        prev_a = self._prev_ask.get(token_id)
        self._prev_bid[token_id] = bid
        self._prev_ask[token_id] = ask

        if bid == prev_b and ask == prev_a:
            return None  # no change

        asset = self._token_asset.get(token_id)
        if not asset:
            return None

        move_ts = self._last_move_ts.get(asset)
        move_bps = self._last_move_bps.get(asset)
        if move_ts is None or move_bps is None:
            return None

        lag = recv_ts - move_ts
        if lag < 0 or lag > 10.0:
            return None  # too old or future, ignore

        slug = self._token_slug.get(token_id, "?")
        move_qty = self._last_move_qty.get(asset, 0.0)
        return OBLagObs(
            token_id=token_id,
            slug=slug,
            displacement_bps=move_bps,
            binance_move_ts=move_ts,
            quote_change_ts=recv_ts,
            displacement_qty=move_qty,
        )
