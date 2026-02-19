#!/usr/bin/env python
"""
Minimal fast copy bot.

Behavior is intentionally simple:
- Copies both BUY and SELL trades from target wallets.
- Applies copy scale to source size/notional.
- BUY-only caps: min and max USDC notional.
- Fast signal ingest path polls Data API user activity at a fixed interval.
- Adds only two lightweight protections:
  - slippage guard (quote vs source price)
  - stale BUY sweep (cancel old live BUY orders submitted by this script)
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from polybot.data_api import fetch_activity_window, fetch_positions_snapshot
from polybot.utils import make_row_key, normalize_unix, safe_stdout_flush


# ---------------------------------------------------------------------------
# Config (keep all runtime constants here)
# ---------------------------------------------------------------------------

USERS: List[str] = [
    "0x732f189193d7a8c8bc8d8eb91f501a22736af081",
]

LOOKBACK_SECONDS = 30
OVERLAP_SECONDS = 1
PAGE_SIZE = 200
BOOTSTRAP_IGNORE_HISTORY = True
QUIET_STATUS = False
MAX_SIGNAL_AGE_SECONDS = 10.0

POLL_INTERVAL_SECONDS = 0.05

COPY_SCALE = 0.05
BUY_MIN_USDC = 1.0
BUY_MAX_USDC = 1.5  # 0 disables max cap.

BUY_PRICE_MULTIPLIER = 1.0
SELL_PRICE_MULTIPLIER = 1.0
BUY_ORDER_TYPE = "FAK"  # FAK or GTC
SELL_ORDER_TYPE = "FAK"  # FAK or GTC
SIZE_DECIMALS = 6

ENABLE_SLIPPAGE_GUARD = True
MAX_SLIPPAGE_PCT = 0.07  # 7%
REQUIRE_QUOTE_FOR_SLIPPAGE = True
QUOTE_CACHE_TTL_SECONDS = 0.2

ENABLE_STALE_BUY_SWEEP = True
STALE_BUY_ORDER_MAX_AGE_SECONDS = 2.0
STALE_BUY_SWEEP_INTERVAL_SECONDS = 0.5
POSITIONS_CACHE_TTL_SECONDS = 0.5

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
SIGNATURE_TYPE = 1

# Credentials:
# - Fast path: paste directly into INLINE_* below.
# - Safer path: keep INLINE_* empty and set env vars COPY_PRIVATE_KEY/COPY_FUNDER.
INLINE_PRIVATE_KEY = ""
INLINE_FUNDER = ""

PRIVATE_KEY = INLINE_PRIVATE_KEY or os.environ.get("COPY_PRIVATE_KEY", "")
FUNDER = INLINE_FUNDER or os.environ.get("COPY_FUNDER", "")

DEDUPE_MAX_KEYS = 200_000


def _log_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    safe_stdout_flush()


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class UserState:
    user: str
    last_seen_ts: Optional[int] = None
    seen_keys: Set[str] = field(default_factory=set)
    seen_order: Deque[str] = field(default_factory=deque)

    def remember_key(self, key: str) -> None:
        if key in self.seen_keys:
            return
        self.seen_keys.add(key)
        self.seen_order.append(key)
        while len(self.seen_order) > DEDUPE_MAX_KEYS:
            oldest = self.seen_order.popleft()
            self.seen_keys.discard(oldest)


class FastClobExecutor:
    def __init__(self, *, private_key: str, funder: str, chain_id: int, signature_type: int) -> None:
        from py_clob_client.client import ClobClient

        self.client = ClobClient(
            CLOB_HOST,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

        self.public_client = ClobClient(CLOB_HOST, chain_id=chain_id)
        self.meta_cache: Dict[str, Tuple[Any, float]] = {}
        self.quote_cache: Dict[Tuple[str, str], Tuple[float, float]] = {}
        self.live_buy_orders: Dict[str, Dict[str, Any]] = {}
        self.positions_cache_at: float = 0.0
        self.positions_by_asset: Dict[str, float] = {}

    def _get_meta(self, token_id: str) -> Tuple[Any, float]:
        cached = self.meta_cache.get(token_id)
        if cached is not None:
            return cached

        from py_clob_client.clob_types import PartialCreateOrderOptions, TickSize

        book = self.public_client.get_order_book(token_id)
        tick_raw = str(book.tick_size)
        tick_map: Dict[str, TickSize] = {
            "0.1": "0.1",
            "0.01": "0.01",
            "0.001": "0.001",
            "0.0001": "0.0001",
        }
        tick = tick_map.get(tick_raw)
        if tick is None:
            raise ValueError(f"unsupported_tick_size:{tick_raw}")

        meta = PartialCreateOrderOptions(tick_size=tick, neg_risk=bool(book.neg_risk))
        tick_float = float(tick_raw)
        self.meta_cache[token_id] = (meta, tick_float)
        return meta, tick_float

    def round_price(self, token_id: str, raw_price: float) -> float:
        _meta, tick = self._get_meta(token_id)
        if tick <= 0:
            tick = 0.01
        min_price = tick
        max_price = max(tick, 1.0 - tick)
        bounded = min(max(raw_price, min_price), max_price)
        steps = int((bounded + 1e-12) / tick)
        rounded = round(steps * tick, 6)
        if rounded < min_price:
            rounded = min_price
        if rounded > max_price:
            rounded = max_price
        return rounded

    @staticmethod
    def _extract_level_price(level: Any) -> Optional[float]:
        raw: Any = None
        if isinstance(level, dict):
            raw = level.get("price")
        else:
            raw = getattr(level, "price", None)
        return _safe_float(raw)

    @classmethod
    def _extract_quote_from_book(cls, book: Any, side: str) -> Optional[float]:
        side_upper = side.upper()
        if side_upper not in {"BUY", "SELL"}:
            return None
        if isinstance(book, dict):
            levels = book.get("bids") if side_upper == "BUY" else book.get("asks")
        else:
            levels = getattr(book, "bids", None) if side_upper == "BUY" else getattr(book, "asks", None)
        if not isinstance(levels, list):
            return None
        prices: List[float] = []
        for level in levels:
            p = cls._extract_level_price(level)
            if p is None or p <= 0:
                continue
            prices.append(p)
        if not prices:
            return None
        return max(prices) if side_upper == "BUY" else min(prices)

    def get_quote_price(self, token_id: str, side: str) -> Optional[float]:
        side_upper = side.upper()
        cache_key = (token_id, side_upper)
        now = time.time()
        cached = self.quote_cache.get(cache_key)
        if cached is not None and (now - cached[0]) < QUOTE_CACHE_TTL_SECONDS:
            return cached[1]

        quote: Any = None
        try:
            quote = self.public_client.get_price(token_id, side=side_upper)
        except Exception:
            quote = None

        quote_price: Optional[float] = None
        if isinstance(quote, dict):
            quote_price = _safe_float(quote.get("price"))
        elif quote is not None:
            quote_price = _safe_float(quote)

        if quote_price is None or quote_price <= 0:
            try:
                book = self.public_client.get_order_book(token_id)
            except Exception:
                book = None
            if book is not None:
                quote_price = self._extract_quote_from_book(book, side_upper)

        if quote_price is not None and quote_price > 0:
            self.quote_cache[cache_key] = (now, quote_price)
            return quote_price
        return None

    @staticmethod
    def _parse_order_created_ts(order: Dict[str, Any]) -> Optional[float]:
        candidates = (
            order.get("createdAt"),
            order.get("created"),
            order.get("timestamp"),
            order.get("time"),
        )
        for value in candidates:
            unixish = normalize_unix(value)
            if unixish is not None:
                return float(unixish)
            if isinstance(value, str):
                text = value.strip().replace("Z", "+00:00")
                if not text:
                    continue
                try:
                    return datetime.fromisoformat(text).timestamp()
                except ValueError:
                    pass
        return None

    def _refresh_positions_cache(self) -> None:
        now = time.time()
        if (now - self.positions_cache_at) < POSITIONS_CACHE_TTL_SECONDS and self.positions_by_asset:
            return
        rows = fetch_positions_snapshot(FUNDER, limit=500, size_threshold=0.0)
        by_asset: Dict[str, float] = {}
        for row in rows:
            token_id = str(row.get("asset") or row.get("asset_id") or row.get("token_id") or "").strip()
            if not token_id:
                continue
            size = _safe_float(row.get("size"))
            if size is None:
                continue
            by_asset[token_id] = by_asset.get(token_id, 0.0) + float(size)
        self.positions_by_asset = by_asset
        self.positions_cache_at = now

    def get_position_size(self, token_id: str) -> float:
        self._refresh_positions_cache()
        return float(self.positions_by_asset.get(token_id, 0.0))

    def track_buy_order(self, *, token_id: str, price: float, size: float, order_response: Dict[str, Any]) -> Optional[str]:
        order_id = str(order_response.get("orderID") or "").strip()
        if not order_id:
            return None
        status = str(order_response.get("status") or "").strip().lower()
        if status in {"matched", "filled", "cancelled", "canceled", "killed"}:
            self.live_buy_orders.pop(order_id, None)
            return None
        self.live_buy_orders[order_id] = {
            "token_id": token_id,
            "price": price,
            "size": size,
            "submitted_at": time.time(),
            "source": "local_submit",
        }
        return order_id

    def sweep_stale_buy_orders(self, *, max_age_seconds: float) -> Dict[str, Any]:
        if max_age_seconds <= 0:
            return {"orders_scanned": 0, "orders_cancelled": 0, "errors": 0}

        now = time.time()
        scanned = 0
        cancelled = 0
        errors = 0
        stale_ids: List[str] = []
        stale_meta: Dict[str, Dict[str, Any]] = {}

        for order_id, meta in list(self.live_buy_orders.items()):
            submitted_at = _safe_float(meta.get("submitted_at"))
            if submitted_at is None:
                continue
            scanned += 1
            age = now - submitted_at
            if age >= max_age_seconds:
                stale_ids.append(order_id)
                stale_meta[order_id] = meta

        for order_id in stale_ids:
            meta = stale_meta.get(order_id, {})
            try:
                response = self.client.cancel(order_id)
                unresolved = False
                if isinstance(response, dict):
                    not_canceled = response.get("not_canceled")
                    if isinstance(not_canceled, dict):
                        unresolved = len(not_canceled) > 0
                    elif isinstance(not_canceled, list):
                        unresolved = len(not_canceled) > 0
                    elif bool(not_canceled):
                        unresolved = True
                if unresolved:
                    errors += 1
                    _log_json(
                        {
                            "copy_status": "STALE_BUY_CANCEL_ERROR",
                            "order_id": order_id,
                            "token_id": meta.get("token_id"),
                            "response": response,
                        }
                    )
                    continue
                cancelled += 1
                self.live_buy_orders.pop(order_id, None)
                _log_json(
                    {
                        "copy_status": "STALE_BUY_CANCELLED",
                        "order_id": order_id,
                        "token_id": meta.get("token_id"),
                        "price": meta.get("price"),
                        "size": meta.get("size"),
                        "age_seconds": round(now - float(meta.get("submitted_at", now)), 3),
                    }
                )
            except Exception as exc:
                errors += 1
                _log_json(
                    {
                        "copy_status": "STALE_BUY_CANCEL_ERROR",
                        "order_id": order_id,
                        "token_id": meta.get("token_id"),
                        "error": str(exc),
                    }
                )

        return {"orders_scanned": scanned, "orders_cancelled": cancelled, "errors": errors}

    def submit_order(self, *, token_id: str, side: str, price: float, size: float, order_type_name: str) -> Dict[str, Any]:
        from py_clob_client.clob_types import OrderArgs, OrderType

        meta, _tick = self._get_meta(token_id)
        order = OrderArgs(token_id=token_id, price=price, size=size, side=side)
        signed = self.client.create_order(order, meta)
        order_type: OrderType = OrderType.FAK if order_type_name.upper() == "FAK" else OrderType.GTC  # type: ignore[assignment]
        response = self.client.post_order(signed, order_type)
        if isinstance(response, dict) and response.get("success") is False:
            raise RuntimeError(f"post_order_failed:{json.dumps(response, separators=(',', ':'), ensure_ascii=False)}")
        if not isinstance(response, dict):
            return {"raw_response": response}
        return response


def _extract_signal(*, user: str, row: Dict[str, Any], seen_at_unix: Optional[float] = None) -> Optional[Dict[str, Any]]:
    row_type = str(row.get("type", "TRADE")).upper()
    if row_type and row_type != "TRADE":
        return None
    side = str(row.get("side", "")).upper()
    token_id = str(row.get("asset", "")).strip()
    if side not in {"BUY", "SELL"} or not token_id:
        return None

    price = _safe_float(row.get("price"))
    size = _safe_float(row.get("size"))
    ts = normalize_unix(row.get("timestamp"))
    if price is None or size is None or ts is None or price <= 0 or size <= 0:
        return None
    usdc_size = _safe_float(row.get("usdcSize"))
    if usdc_size is None or usdc_size <= 0:
        usdc_size = price * size
    source_tx = str(row.get("transactionHash", "")).strip().lower()
    seen_at = float(seen_at_unix) if seen_at_unix is not None else time.time()
    age = max(0.0, seen_at - ts)
    fetch_request_sent_at = _safe_float(row.get("_fetch_request_sent_at_unix"))
    fetch_response_received_at = _safe_float(row.get("_fetch_response_received_at_unix"))
    fetch_parse_done_at = _safe_float(row.get("_fetch_parse_done_at_unix"))
    fetch_rtt = _safe_float(row.get("_fetch_rtt_seconds"))
    api_visibility_at_response: Optional[float] = None
    if fetch_response_received_at is not None:
        api_visibility_at_response = max(0.0, fetch_response_received_at - ts)
    response_to_seen: Optional[float] = None
    if fetch_response_received_at is not None:
        response_to_seen = max(0.0, seen_at - fetch_response_received_at)
    return {
        "source_user": user,
        "source_tx": source_tx,
        "side": side,
        "token_id": token_id,
        "price": float(price),
        "size": float(size),
        "usdc_size": float(usdc_size),
        "timestamp": int(ts),
        "age_seconds": age,
        "signal_seen_at_unix": seen_at,
        "fetch_source": str(row.get("_fetch_source") or "unknown"),
        "fetch_request_sent_at_unix": fetch_request_sent_at,
        "fetch_response_received_at_unix": fetch_response_received_at,
        "fetch_parse_done_at_unix": fetch_parse_done_at,
        "fetch_rtt_seconds": fetch_rtt,
        "api_visibility_at_response_seconds": api_visibility_at_response,
        "api_visibility_at_seen_seconds": age,
        "response_to_seen_seconds": response_to_seen,
    }


def _collect_user_signals_from_activity(state: UserState) -> List[Dict[str, Any]]:
    now = int(time.time())
    if state.last_seen_ts is None:
        if BOOTSTRAP_IGNORE_HISTORY:
            state.last_seen_ts = now
        start = now - LOOKBACK_SECONDS
    else:
        start = max(now - LOOKBACK_SECONDS, state.last_seen_ts - OVERLAP_SECONDS)
    end = now
    stop_before = (state.last_seen_ts - OVERLAP_SECONDS) if state.last_seen_ts is not None else None

    rows = fetch_activity_window(
        user=state.user,
        start=start,
        end=end,
        page_size=PAGE_SIZE,
        event_type="TRADE",
        side=None,
        stop_before_ts=stop_before,
    )
    rows.reverse()

    emitted = 0
    fresh = 0
    max_ts = state.last_seen_ts
    signals: List[Dict[str, Any]] = []

    for row in rows:
        ts = normalize_unix(row.get("timestamp"))
        if ts is None:
            continue
        if max_ts is None or ts > max_ts:
            max_ts = ts

        key = make_row_key(row)
        if key in state.seen_keys:
            continue
        state.remember_key(key)
        emitted += 1
        seen_at = time.time()
        signal = _extract_signal(user=state.user, row=row, seen_at_unix=seen_at)
        if signal is None:
            continue
        signal["signal_source"] = "poll_activity"
        signal["trigger_market"] = None
        if signal["side"] == "BUY" and signal["age_seconds"] > MAX_SIGNAL_AGE_SECONDS:
            _log_json(
                {
                    "copy_status": "SKIPPED",
                    "reason": "stale_signal",
                    "source_user": signal["source_user"],
                    "source_tx": signal["source_tx"],
                    "token_id": signal["token_id"],
                    "side": signal["side"],
                    "event_age_seconds": round(signal["age_seconds"], 3),
                    "max_signal_age_seconds": MAX_SIGNAL_AGE_SECONDS,
                    "signal_seen_at_unix": round(float(signal["signal_seen_at_unix"]), 3),
                    "fetch_source": signal.get("fetch_source"),
                    "fetch_request_sent_at_unix": signal.get("fetch_request_sent_at_unix"),
                    "fetch_response_received_at_unix": signal.get("fetch_response_received_at_unix"),
                    "fetch_rtt_seconds": signal.get("fetch_rtt_seconds"),
                    "api_visibility_at_response_seconds": signal.get("api_visibility_at_response_seconds"),
                    "api_visibility_at_seen_seconds": round(float(signal["api_visibility_at_seen_seconds"]), 3),
                    "response_to_seen_seconds": signal.get("response_to_seen_seconds"),
                }
            )
            continue
        signals.append(signal)
        fresh += 1

    if max_ts is not None:
        state.last_seen_ts = max_ts

    if not QUIET_STATUS:
        print(
            (
                f"fast_copy poll user={state.user} window=[{start},{end}] rows={len(rows)} "
                f"emitted={emitted} fresh={fresh} last_seen={state.last_seen_ts}"
            ),
            file=sys.stderr,
        )
    return signals


def _aggregate_signals(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in signals:
        seen_age = _safe_float(s.get("api_visibility_at_seen_seconds"))
        if seen_age is None:
            seen_age = _safe_float(s.get("age_seconds"))
        response_age = _safe_float(s.get("api_visibility_at_response_seconds"))
        fetch_rtt = _safe_float(s.get("fetch_rtt_seconds"))
        response_to_seen = _safe_float(s.get("response_to_seen_seconds"))
        key = (str(s["token_id"]), str(s["side"]))
        b = buckets.get(key)
        if b is None:
            b = {
                "token_id": s["token_id"],
                "side": s["side"],
                "count": 0,
                "source_users": set(),
                "signal_sources": set(),
                "trigger_markets": set(),
                "source_usdc_sum": 0.0,
                "source_size_sum": 0.0,
                "source_price_min": float(s["price"]),
                "source_price_max": float(s["price"]),
                "first_ts": int(s["timestamp"]),
                "last_ts": int(s["timestamp"]),
                "first_tx": s["source_tx"],
                "last_tx": s["source_tx"],
                "api_visibility_seen_min_seconds": seen_age,
                "api_visibility_seen_max_seconds": seen_age,
                "api_visibility_response_min_seconds": response_age,
                "api_visibility_response_max_seconds": response_age,
                "fetch_rtt_min_seconds": fetch_rtt,
                "fetch_rtt_max_seconds": fetch_rtt,
                "response_to_seen_min_seconds": response_to_seen,
                "response_to_seen_max_seconds": response_to_seen,
            }
            buckets[key] = b
        b["count"] += 1
        b["source_users"].add(s["source_user"])
        b["signal_sources"].add(str(s.get("signal_source") or "unknown"))
        trigger_market = str(s.get("trigger_market") or "").strip()
        if trigger_market:
            b["trigger_markets"].add(trigger_market)
        b["source_usdc_sum"] += float(s["usdc_size"])
        b["source_size_sum"] += float(s["size"])
        b["source_price_min"] = min(float(b["source_price_min"]), float(s["price"]))
        b["source_price_max"] = max(float(b["source_price_max"]), float(s["price"]))
        ts = int(s["timestamp"])
        if ts < int(b["first_ts"]):
            b["first_ts"] = ts
            b["first_tx"] = s["source_tx"]
        if ts >= int(b["last_ts"]):
            b["last_ts"] = ts
            b["last_tx"] = s["source_tx"]

        if seen_age is not None:
            cur_min = _safe_float(b.get("api_visibility_seen_min_seconds"))
            cur_max = _safe_float(b.get("api_visibility_seen_max_seconds"))
            b["api_visibility_seen_min_seconds"] = seen_age if cur_min is None else min(cur_min, seen_age)
            b["api_visibility_seen_max_seconds"] = seen_age if cur_max is None else max(cur_max, seen_age)
        if response_age is not None:
            cur_min = _safe_float(b.get("api_visibility_response_min_seconds"))
            cur_max = _safe_float(b.get("api_visibility_response_max_seconds"))
            b["api_visibility_response_min_seconds"] = response_age if cur_min is None else min(cur_min, response_age)
            b["api_visibility_response_max_seconds"] = response_age if cur_max is None else max(cur_max, response_age)
        if fetch_rtt is not None:
            cur_min = _safe_float(b.get("fetch_rtt_min_seconds"))
            cur_max = _safe_float(b.get("fetch_rtt_max_seconds"))
            b["fetch_rtt_min_seconds"] = fetch_rtt if cur_min is None else min(cur_min, fetch_rtt)
            b["fetch_rtt_max_seconds"] = fetch_rtt if cur_max is None else max(cur_max, fetch_rtt)
        if response_to_seen is not None:
            cur_min = _safe_float(b.get("response_to_seen_min_seconds"))
            cur_max = _safe_float(b.get("response_to_seen_max_seconds"))
            b["response_to_seen_min_seconds"] = response_to_seen if cur_min is None else min(cur_min, response_to_seen)
            b["response_to_seen_max_seconds"] = response_to_seen if cur_max is None else max(cur_max, response_to_seen)

    now = time.time()
    out: List[Dict[str, Any]] = []
    for b in buckets.values():
        side = str(b["side"])
        selected_price = float(b["source_price_min"]) if side == "BUY" else float(b["source_price_max"])
        out.append(
            {
                "token_id": b["token_id"],
                "side": side,
                "aggregated_count": int(b["count"]),
                "source_users": sorted(list(b["source_users"])),
                "signal_sources": sorted(list(b["signal_sources"])),
                "trigger_markets": sorted(list(b["trigger_markets"])),
                "source_usdc_sum": float(b["source_usdc_sum"]),
                "source_size_sum": float(b["source_size_sum"]),
                "source_price_min": float(b["source_price_min"]),
                "source_price_max": float(b["source_price_max"]),
                "source_price_selected": selected_price,
                "aggregated_first_tx": b["first_tx"],
                "aggregated_last_tx": b["last_tx"],
                "oldest_signal_age_seconds": max(0.0, now - float(b["first_ts"])),
                "latest_signal_age_seconds": max(0.0, now - float(b["last_ts"])),
                "api_visibility_seen_min_seconds": _safe_float(b.get("api_visibility_seen_min_seconds")),
                "api_visibility_seen_max_seconds": _safe_float(b.get("api_visibility_seen_max_seconds")),
                "api_visibility_response_min_seconds": _safe_float(b.get("api_visibility_response_min_seconds")),
                "api_visibility_response_max_seconds": _safe_float(b.get("api_visibility_response_max_seconds")),
                "fetch_rtt_min_seconds": _safe_float(b.get("fetch_rtt_min_seconds")),
                "fetch_rtt_max_seconds": _safe_float(b.get("fetch_rtt_max_seconds")),
                "response_to_seen_min_seconds": _safe_float(b.get("response_to_seen_min_seconds")),
                "response_to_seen_max_seconds": _safe_float(b.get("response_to_seen_max_seconds")),
            }
        )
    out.sort(key=lambda x: (x["side"], x["token_id"]))
    return out


def _execute_aggregated_signal(signal: Dict[str, Any], executor: FastClobExecutor) -> None:
    side = str(signal["side"])
    token_id = str(signal["token_id"])
    source_price = float(signal["source_price_selected"])
    source_tx = (
        f"agg_batch:{side.lower()}:{token_id}:{int(signal['aggregated_count'])}:"
        f"{signal['aggregated_first_tx']}:{signal['aggregated_last_tx']}"
    )

    if side == "BUY":
        target_usdc_raw = float(signal["source_usdc_sum"]) * COPY_SCALE
        target_usdc = target_usdc_raw
        if BUY_MAX_USDC > 0:
            target_usdc = min(target_usdc, BUY_MAX_USDC)
        if target_usdc < BUY_MIN_USDC or target_usdc <= 0:
            _log_json(
                {
                    "copy_status": "SKIPPED",
                    "reason": "below_min_or_max_buy_usdc",
                    "source_tx": source_tx,
                    "token_id": token_id,
                    "side": side,
                    "aggregated_count": int(signal["aggregated_count"]),
                    "source_price_selected": round(source_price, 6),
                    "target_usdc_raw": round(target_usdc_raw, 6),
                    "target_usdc": round(target_usdc, 6),
                }
            )
            return
        copy_size = target_usdc / source_price
        order_price_raw = source_price * BUY_PRICE_MULTIPLIER
        order_type = BUY_ORDER_TYPE.upper()
    else:
        position_size = executor.get_position_size(token_id)
        if position_size <= 0:
            _log_json(
                {
                    "copy_status": "SKIPPED",
                    "reason": "no_position_to_sell",
                    "source_tx": source_tx,
                    "token_id": token_id,
                    "side": side,
                    "aggregated_count": int(signal["aggregated_count"]),
                    "source_price_selected": round(source_price, 6),
                }
            )
            return
        copy_size = position_size
        target_usdc = copy_size * source_price
        order_price_raw = source_price * SELL_PRICE_MULTIPLIER
        order_type = SELL_ORDER_TYPE.upper()

    copy_size = round(copy_size, SIZE_DECIMALS)
    if copy_size <= 0:
        _log_json(
            {
                "copy_status": "SKIPPED",
                "reason": "non_positive_copy_size",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
            }
        )
        return

    try:
        order_price = executor.round_price(token_id, order_price_raw)
    except Exception as exc:
        _log_json(
            {
                "copy_status": "ERROR",
                "reason": "price_rounding_failed",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
                "error": str(exc),
            }
        )
        return

    live_quote: Optional[float] = None
    if ENABLE_SLIPPAGE_GUARD and MAX_SLIPPAGE_PCT > 0:
        quote_side = "SELL" if side == "BUY" else "BUY"
        try:
            live_quote = executor.get_quote_price(token_id, quote_side)
        except Exception:
            live_quote = None

        if live_quote is None:
            if REQUIRE_QUOTE_FOR_SLIPPAGE:
                _log_json(
                    {
                        "copy_status": "SKIPPED",
                        "reason": "slippage_quote_unavailable",
                        "source_tx": source_tx,
                        "token_id": token_id,
                        "side": side,
                        "aggregated_count": int(signal["aggregated_count"]),
                        "source_price_selected": round(source_price, 6),
                    }
                )
                return
        else:
            if side == "BUY":
                threshold = source_price * (1.0 + MAX_SLIPPAGE_PCT)
                slippage_ok = live_quote <= threshold
            else:
                threshold = source_price * (1.0 - MAX_SLIPPAGE_PCT)
                slippage_ok = live_quote >= threshold
            if not slippage_ok:
                _log_json(
                    {
                        "copy_status": "SKIPPED",
                        "reason": "slippage_guard",
                        "source_tx": source_tx,
                        "token_id": token_id,
                        "side": side,
                        "aggregated_count": int(signal["aggregated_count"]),
                        "source_price_selected": round(source_price, 6),
                        "live_quote": round(live_quote, 6),
                        "slippage_threshold": round(threshold, 6),
                    }
                )
                return

    try:
        response = executor.submit_order(
            token_id=token_id,
            side=side,
            price=order_price,
            size=copy_size,
            order_type_name=order_type,
        )
    except Exception as exc:
        _log_json(
            {
                "copy_status": "ERROR",
                "reason": "submit_failed",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
                "source_price_selected": round(source_price, 6),
                "order_price": round(order_price, 6),
                "size": copy_size,
                "target_usdc": round(target_usdc, 6),
                "order_type": order_type,
                "error": str(exc),
            }
        )
        return

    tracked_buy_order_id: Optional[str] = None
    if side == "BUY":
        tracked_buy_order_id = executor.track_buy_order(
            token_id=token_id,
            price=order_price,
            size=copy_size,
            order_response=response,
        )
    else:
        executor.positions_cache_at = 0.0

    _log_json(
        {
            "copy_status": "SUBMITTED",
            "source_tx": source_tx,
            "token_id": token_id,
            "side": side,
            "aggregated_count": int(signal["aggregated_count"]),
            "source_users": signal["source_users"],
            "signal_sources": signal.get("signal_sources", []),
            "trigger_markets": signal.get("trigger_markets", []),
            "aggregated_first_tx": signal["aggregated_first_tx"],
            "aggregated_last_tx": signal["aggregated_last_tx"],
            "source_price_min": round(float(signal["source_price_min"]), 6),
            "source_price_max": round(float(signal["source_price_max"]), 6),
            "source_price_selected": round(source_price, 6),
            "order_price": round(order_price, 6),
            "size": copy_size,
            "target_usdc": round(target_usdc, 6),
            "order_type": order_type,
            "oldest_signal_age_seconds": round(float(signal["oldest_signal_age_seconds"]), 3),
            "latest_signal_age_seconds": round(float(signal["latest_signal_age_seconds"]), 3),
            "api_visibility_seen_min_seconds": signal.get("api_visibility_seen_min_seconds"),
            "api_visibility_seen_max_seconds": signal.get("api_visibility_seen_max_seconds"),
            "api_visibility_response_min_seconds": signal.get("api_visibility_response_min_seconds"),
            "api_visibility_response_max_seconds": signal.get("api_visibility_response_max_seconds"),
            "fetch_rtt_min_seconds": signal.get("fetch_rtt_min_seconds"),
            "fetch_rtt_max_seconds": signal.get("fetch_rtt_max_seconds"),
            "response_to_seen_min_seconds": signal.get("response_to_seen_min_seconds"),
            "response_to_seen_max_seconds": signal.get("response_to_seen_max_seconds"),
            "live_quote": None if live_quote is None else round(live_quote, 6),
            "tracked_buy_order_id": tracked_buy_order_id,
            "order_response": response,
        }
    )


def _validate_config() -> None:
    if not PRIVATE_KEY:
        raise ValueError("COPY_PRIVATE_KEY is required")
    if not FUNDER:
        raise ValueError("COPY_FUNDER is required")
    if not USERS:
        raise ValueError("USERS cannot be empty")
    if COPY_SCALE <= 0:
        raise ValueError("COPY_SCALE must be > 0")
    if MAX_SIGNAL_AGE_SECONDS <= 0:
        raise ValueError("MAX_SIGNAL_AGE_SECONDS must be > 0")
    if POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be > 0")
    if BUY_ORDER_TYPE.upper() not in {"FAK", "GTC"}:
        raise ValueError("BUY_ORDER_TYPE must be FAK or GTC")
    if SELL_ORDER_TYPE.upper() not in {"FAK", "GTC"}:
        raise ValueError("SELL_ORDER_TYPE must be FAK or GTC")
    if MAX_SLIPPAGE_PCT < 0:
        raise ValueError("MAX_SLIPPAGE_PCT must be >= 0")
    if STALE_BUY_ORDER_MAX_AGE_SECONDS < 0:
        raise ValueError("STALE_BUY_ORDER_MAX_AGE_SECONDS must be >= 0")
    if STALE_BUY_SWEEP_INTERVAL_SECONDS <= 0:
        raise ValueError("STALE_BUY_SWEEP_INTERVAL_SECONDS must be > 0")
    if POSITIONS_CACHE_TTL_SECONDS <= 0:
        raise ValueError("POSITIONS_CACHE_TTL_SECONDS must be > 0")


def main() -> int:
    _validate_config()
    try:
        import py_clob_client.client  # noqa: F401
    except ModuleNotFoundError as exc:
        print("Missing dependency py_clob_client. Run: pip install -r requirements.txt", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        return 2

    users = [u.strip().lower() for u in USERS if str(u).strip()]
    states = [UserState(user=u) for u in users]
    executor = FastClobExecutor(
        private_key=PRIVATE_KEY,
        funder=FUNDER,
        chain_id=CHAIN_ID,
        signature_type=SIGNATURE_TYPE,
    )

    print(
        json.dumps(
            {
                "event": "fast_copy_started",
                "users": users,
                "signal_source_mode": "poll_activity",
                "copy_scale": COPY_SCALE,
                "max_signal_age_seconds": MAX_SIGNAL_AGE_SECONDS,
                "buy_min_usdc": BUY_MIN_USDC,
                "buy_max_usdc": BUY_MAX_USDC,
                "buy_order_type": BUY_ORDER_TYPE,
                "sell_order_type": SELL_ORDER_TYPE,
                "aggregate_mode": "buy_min_price_sell_max_price",
                "poll_interval_seconds": POLL_INTERVAL_SECONDS,
                "slippage_guard_enabled": ENABLE_SLIPPAGE_GUARD,
                "max_slippage_pct": MAX_SLIPPAGE_PCT,
                "stale_buy_sweep_enabled": ENABLE_STALE_BUY_SWEEP,
                "stale_buy_order_max_age_seconds": STALE_BUY_ORDER_MAX_AGE_SECONDS,
                "stale_buy_sweep_interval_seconds": STALE_BUY_SWEEP_INTERVAL_SECONDS,
                "positions_cache_ttl_seconds": POSITIONS_CACHE_TTL_SECONDS,
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )

    next_tick = time.monotonic()
    next_stale_buy_sweep_at = time.monotonic()
    while True:
        cycle_signals: List[Dict[str, Any]] = []
        for state in states:
            cycle_signals.extend(_collect_user_signals_from_activity(state))

        aggregated = _aggregate_signals(cycle_signals)
        for signal in aggregated:
            _execute_aggregated_signal(signal, executor)

        if not QUIET_STATUS:
            print(
                f"fast_copy cycle raw_signals={len(cycle_signals)} aggregated_orders={len(aggregated)}",
                file=sys.stderr,
            )

        now_mono = time.monotonic()
        if (
            ENABLE_STALE_BUY_SWEEP
            and STALE_BUY_ORDER_MAX_AGE_SECONDS > 0
            and now_mono >= next_stale_buy_sweep_at
        ):
            sweep = executor.sweep_stale_buy_orders(max_age_seconds=STALE_BUY_ORDER_MAX_AGE_SECONDS)
            _log_json(
                {
                    "copy_status": "STALE_BUY_SWEEP",
                    "orders_scanned": int(sweep.get("orders_scanned", 0)),
                    "orders_cancelled": int(sweep.get("orders_cancelled", 0)),
                    "errors": int(sweep.get("errors", 0)),
                    "max_age_seconds": STALE_BUY_ORDER_MAX_AGE_SECONDS,
                }
            )
            next_stale_buy_sweep_at = now_mono + STALE_BUY_SWEEP_INTERVAL_SECONDS

        next_tick += POLL_INTERVAL_SECONDS
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.monotonic()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
