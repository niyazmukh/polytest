"""Event recorder — writes timestamped events to JSONL files.

Seven output files per session:
    binance_{ts}.jsonl       — Binance aggTrade ticks (sampled)
    chainlink_{ts}.jsonl     — Chainlink RTDS price updates
    orderbook_{ts}.jsonl     — Polymarket orderbook quote updates
    windows_{ts}.jsonl       — Window lifecycle (open, start_price, resolved)
    cross_lag_{ts}.jsonl     — Cross-source latency observations
    trades_{ts}.jsonl        — Polymarket trade executions (last_trade_price)
    rtds_binance_{ts}.jsonl  — RTDS Binance-mirror price updates

Line format: compact JSON, one object per line.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional, TextIO

log = logging.getLogger(__name__)

# File categories
BINANCE = "binance"
CHAINLINK = "chainlink"
ORDERBOOK = "orderbook"
WINDOWS = "windows"
CROSS_LAG = "cross_lag"
TRADES = "trades"
RTDS_BINANCE = "rtds_binance"

_CATEGORIES = (BINANCE, CHAINLINK, ORDERBOOK, WINDOWS, CROSS_LAG, TRADES, RTDS_BINANCE)


class EventRecorder:
    """Writes timestamped events to categorized JSONL files."""

    def __init__(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)
        self._dir = output_dir
        ts = time.strftime("%Y%m%d_%H%M%S")
        self._files: dict[str, TextIO] = {}
        self._paths: dict[str, str] = {}
        for cat in _CATEGORIES:
            path = os.path.join(output_dir, f"{cat}_{ts}.jsonl")
            self._files[cat] = open(path, "w", encoding="utf-8")
            self._paths[cat] = path
        self._counts: dict[str, int] = {cat: 0 for cat in _CATEGORIES}
        self._start_ts = time.time()
        log.info("recorder output: %s (session %s)", output_dir, ts)

    # ── Writers ──

    def write_binance_tick(self, asset: str, price: float,
                           trade_ts: float, recv_ts: float,
                           qty: float = 0.0,
                           is_buyer_maker: bool = False) -> None:
        """Log one Binance aggTrade tick."""
        self._write(BINANCE, {
            "event": "binance_tick",
            "asset": asset,
            "price": round(price, 8),
            "trade_ts": round(trade_ts, 4),
            "recv_ts": round(recv_ts, 4),
            "qty": round(qty, 8),
            "is_buyer_maker": is_buyer_maker,
        })

    def write_chainlink(self, symbol: str, price: float,
                        recv_ts: float,
                        origin_ts: float = 0.0,
                        server_ts: float = 0.0) -> None:
        """Log one Chainlink RTDS price update."""
        d: Dict[str, Any] = {
            "event": "chainlink_update",
            "symbol": symbol,
            "price": round(price, 8),
            "recv_ts": round(recv_ts, 4),
        }
        if origin_ts > 0:
            d["origin_ts"] = round(origin_ts, 4)
        if server_ts > 0:
            d["server_ts"] = round(server_ts, 4)
        self._write(CHAINLINK, d)

    def write_orderbook(self, token_id: str, bid: Optional[float],
                        ask: Optional[float], recv_ts: float,
                        bid_size: Optional[float] = None,
                        ask_size: Optional[float] = None,
                        exchange_ts: Optional[float] = None) -> None:
        """Log one orderbook quote update."""
        d: Dict[str, Any] = {
            "event": "orderbook_update",
            "token_id": token_id,
            "recv_ts": round(recv_ts, 4),
        }
        if bid is not None:
            d["bid"] = round(bid, 4)
        if ask is not None:
            d["ask"] = round(ask, 4)
        if bid_size is not None:
            d["bid_size"] = round(bid_size, 2)
        if ask_size is not None:
            d["ask_size"] = round(ask_size, 2)
        if exchange_ts is not None:
            d["exchange_ts"] = round(exchange_ts, 4)
        self._write(ORDERBOOK, d)

    def write_window_open(self, slug: str, asset: str, duration_s: int,
                          start_ts: int, end_ts: int,
                          token_up: str, token_down: str,
                          condition_id: str) -> None:
        """Log discovery of a new market window."""
        self._write(WINDOWS, {
            "event": "window_open",
            "slug": slug,
            "asset": asset,
            "duration_s": duration_s,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "token_up": token_up,
            "token_down": token_down,
            "condition_id": condition_id,
            "logged_ts": round(time.time(), 4),
        })

    def write_start_price(self, slug: str, source: str,
                          price: float, ts: float) -> None:
        """Log start price capture for a window."""
        self._write(WINDOWS, {
            "event": "start_price",
            "slug": slug,
            "source": source,
            "price": round(price, 8),
            "ts": round(ts, 4),
        })

    def write_resolution(self, slug: str, condition_id: str,
                         outcome: str, outcome_prices: str,
                         resolved_ts: float) -> None:
        """Log window resolution (outcome determined)."""
        self._write(WINDOWS, {
            "event": "resolution",
            "slug": slug,
            "condition_id": condition_id,
            "outcome": outcome,
            "outcome_prices": outcome_prices,
            "resolved_ts": round(resolved_ts, 4),
        })

    def write_cross_lag(self, asset: str,
                        bn_price: float, cl_price: float,
                        bn_origin_ts: float, bn_recv_ts: float,
                        cl_origin_ts: float, cl_recv_ts: float) -> None:
        """Log Binance-Chainlink cross-source latency observation.

        Stores four timestamps for full provenance:
          bn_origin_ts — Binance aggTrade exchange timestamp (T field)
          bn_recv_ts   — when we received the Binance tick
          cl_origin_ts — Chainlink oracle timestamp (payload timestamp)
          cl_recv_ts   — when we received the Chainlink update

        Computes two lag metrics:
          origin_lag_ms — true source-level lag (CL oracle − BN trade)
          recv_lag_ms   — pipeline-level lag (CL recv − BN recv)
        """
        price_delta = cl_price - bn_price
        recv_lag_ms = (cl_recv_ts - bn_recv_ts) * 1000
        d: Dict[str, Any] = {
            "event": "binance_chainlink_lag",
            "asset": asset,
            "binance_price": round(bn_price, 8),
            "chainlink_price": round(cl_price, 8),
            "price_delta": round(price_delta, 8),
            "bn_origin_ts": round(bn_origin_ts, 4),
            "bn_recv_ts": round(bn_recv_ts, 4),
            "cl_origin_ts": round(cl_origin_ts, 4),
            "cl_recv_ts": round(cl_recv_ts, 4),
            "recv_lag_ms": round(recv_lag_ms, 1),
            "lag_ms": round(recv_lag_ms, 1),  # backward compat
        }
        if cl_origin_ts > 0 and bn_origin_ts > 0:
            origin_lag_ms = (cl_origin_ts - bn_origin_ts) * 1000
            d["origin_lag_ms"] = round(origin_lag_ms, 1)
        self._write(CROSS_LAG, d)

    def write_trade(self, token_id: str, price: float, side: str,
                    size: float, fee_rate_bps: int,
                    exchange_ts: float, recv_ts: float) -> None:
        """Log one Polymarket trade execution (last_trade_price)."""
        self._write(TRADES, {
            "event": "trade",
            "token_id": token_id,
            "price": round(price, 4),
            "side": side,
            "size": round(size, 6),
            "fee_rate_bps": fee_rate_bps,
            "exchange_ts": round(exchange_ts, 4),
            "recv_ts": round(recv_ts, 4),
        })

    def write_rtds_binance(self, symbol: str, price: float,
                           origin_ts: float, server_ts: float,
                           recv_ts: float) -> None:
        """Log one RTDS Binance-mirror price update."""
        self._write(RTDS_BINANCE, {
            "event": "rtds_binance",
            "symbol": symbol,
            "price": round(price, 8),
            "origin_ts": round(origin_ts, 4),
            "server_ts": round(server_ts, 4),
            "recv_ts": round(recv_ts, 4),
        })

    def write_ws_resolution(self, condition_id: str,
                            winning_asset_id: str,
                            winning_outcome: str,
                            exchange_ts: float,
                            recv_ts: float) -> None:
        """Log market resolution from WS push (instant vs polling)."""
        self._write(WINDOWS, {
            "event": "ws_resolution",
            "condition_id": condition_id,
            "winning_asset_id": winning_asset_id,
            "winning_outcome": winning_outcome,
            "exchange_ts": round(exchange_ts, 4),
            "recv_ts": round(recv_ts, 4),
        })

    def write_orderbook_lag(self, token_id: str, slug: str,
                            binance_move_ts: float,
                            quote_change_ts: float,
                            displacement_bps: float,
                            displacement_qty: float = 0.0) -> None:
        """Log orderbook adjustment lag after a Binance displacement."""
        lag_ms = (quote_change_ts - binance_move_ts) * 1000
        d: Dict[str, Any] = {
            "event": "orderbook_adjustment_lag",
            "token_id": token_id,
            "slug": slug,
            "displacement_bps": round(displacement_bps, 2),
            "binance_move_ts": round(binance_move_ts, 4),
            "quote_change_ts": round(quote_change_ts, 4),
            "lag_ms": round(lag_ms, 1),
        }
        if displacement_qty > 0:
            d["displacement_qty"] = round(displacement_qty, 8)
        self._write(CROSS_LAG, d)

    # ── Stats ──

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    @property
    def uptime_s(self) -> float:
        return time.time() - self._start_ts

    # ── Internals ──

    def _write(self, category: str, data: Dict[str, Any]) -> None:
        f = self._files.get(category)
        if f is None:
            return
        f.write(json.dumps(data, separators=(",", ":")) + "\n")
        self._counts[category] += 1
        # Flush every 100 writes for durability
        if self._counts[category] % 100 == 0:
            f.flush()

    def flush(self) -> None:
        """Flush all file buffers."""
        for f in self._files.values():
            f.flush()

    def close(self) -> None:
        """Flush and close all files."""
        for f in self._files.values():
            try:
                f.flush()
                f.close()
            except Exception:
                pass
        log.info("recorder closed — totals: %s", self._counts)
