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
from concurrent.futures import Future, ThreadPoolExecutor
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, cast

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
QUIET_STATUS = True
MAX_SIGNAL_AGE_SECONDS = 10.0

POLL_INTERVAL_SECONDS = 0.05
AGGREGATE_NEIGHBOR_POLLS = 3  # Deliberately batch across adjacent poll cycles.
MAX_PENDING_SIGNALS = 5_000  # Safety cap; force flush if queue grows too much.

COPY_SCALE = 0.005
BUY_MIN_USDC = 1.0
BUY_MAX_USDC = 1.5  # 0 disables max cap.
SELL_SIZING_MODE = "scaled"  # scaled | all
SELL_MIN_USDC = 1.0

BUY_PRICE_MULTIPLIER = 1.0
SELL_PRICE_MULTIPLIER = 1.0
BUY_ORDER_TYPE = "FAK"  # FAK or GTC
SELL_ORDER_TYPE = "FAK"  # FAK or GTC
SIZE_DECIMALS = 2
BUY_USDC_DECIMALS = 2

ENABLE_SLIPPAGE_GUARD = True
MAX_SLIPPAGE_PCT = 0.07  # 7%
REQUIRE_QUOTE_FOR_SLIPPAGE = True
QUOTE_CACHE_TTL_SECONDS = 0.2

ENABLE_STALE_BUY_SWEEP = True
STALE_BUY_ORDER_MAX_AGE_SECONDS = 2.0
STALE_BUY_SWEEP_INTERVAL_SECONDS = 0.5
POSITIONS_CACHE_TTL_SECONDS = 0.5
LOG_EMPTY_STALE_SWEEPS = False

ENABLE_MIN_NOTIONAL_ADVANCE = True
ADVANCE_LEDGER_MAX_AGE_SECONDS = 900.0
ENABLE_CASH_GUARD = True
MIN_USDC_CASH_STOP = 20.0
CASH_GUARD_CHECK_INTERVAL_SECONDS = 1.0
CASH_GUARD_FAIL_CLOSED = True

ENABLE_AUTO_REDEEM = True
AUTO_REDEEM_INTERVAL_SECONDS = 300.0
AUTO_REDEEM_RPC_URL = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")
AUTO_REDEEM_REQUIRE_SIGNER_EQUALS_FUNDER = True
AUTO_REDEEM_FAIL_CLOSED = False

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
USDC_BASE_UNITS = 1_000_000.0


def _log_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    safe_stdout_flush()


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_to_decimals(value: float, decimals: int) -> float:
    if decimals < 0:
        return float(value)
    return round(float(value), decimals)


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

        self.private_key = private_key
        self.funder = str(funder).strip().lower()
        self.chain_id = int(chain_id)
        self.signature_type = int(signature_type)
        self.client = ClobClient(
            CLOB_HOST,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder,
        )
        self.client.set_api_creds(self.client.create_or_derive_api_creds())
        self.signer_address = str(self.client.get_address() or "").strip().lower()

        self.public_client = ClobClient(CLOB_HOST, chain_id=chain_id)
        self.meta_cache: Dict[str, Tuple[Any, float]] = {}
        self.quote_cache: Dict[Tuple[str, str], Tuple[float, float]] = {}
        self.live_buy_orders: Dict[str, Dict[str, Any]] = {}
        self.advance_notional_debt: Dict[Tuple[str, str], Tuple[float, float]] = {}
        self.positions_cache_at: float = 0.0
        self.positions_by_asset: Dict[str, float] = {}

    def _get_advance_debt(self, *, token_id: str, side: str, now: float) -> float:
        key = (token_id, side.upper())
        current = self.advance_notional_debt.get(key)
        if current is None:
            return 0.0
        debt, updated_at = float(current[0]), float(current[1])
        if debt <= 0:
            self.advance_notional_debt.pop(key, None)
            return 0.0
        if (now - updated_at) > ADVANCE_LEDGER_MAX_AGE_SECONDS:
            self.advance_notional_debt.pop(key, None)
            return 0.0
        return debt

    def consume_advance_debt(self, *, token_id: str, side: str, fresh_notional: float) -> Tuple[float, float, float]:
        now = time.time()
        fresh = max(0.0, float(fresh_notional))
        debt = self._get_advance_debt(token_id=token_id, side=side, now=now)
        if fresh <= 0:
            return 0.0, 0.0, debt
        if debt <= 0:
            return fresh, 0.0, 0.0
        consumed = min(fresh, debt)
        remaining = max(0.0, debt - fresh)
        key = (token_id, side.upper())
        if remaining > 0:
            self.advance_notional_debt[key] = (remaining, now)
        else:
            self.advance_notional_debt.pop(key, None)
        net = max(0.0, fresh - consumed)
        return net, consumed, remaining

    def add_advance_debt(self, *, token_id: str, side: str, debt_notional: float) -> float:
        delta = max(0.0, float(debt_notional))
        if delta <= 0:
            return 0.0
        now = time.time()
        key = (token_id, side.upper())
        existing = self._get_advance_debt(token_id=token_id, side=side, now=now)
        updated = existing + delta
        self.advance_notional_debt[key] = (updated, now)
        return updated

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

    def submit_order(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type_name: str,
        buy_notional_usdc: Optional[float] = None,
    ) -> Dict[str, Any]:
        from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType

        meta, _tick = self._get_meta(token_id)
        order_type: OrderType = OrderType.FAK if order_type_name.upper() == "FAK" else OrderType.GTC  # type: ignore[assignment]
        if order_type == OrderType.FAK:
            if side == "BUY":
                amount = _round_to_decimals(
                    float(buy_notional_usdc if buy_notional_usdc is not None else (price * size)),
                    BUY_USDC_DECIMALS,
                )
            else:
                amount = round(float(size), SIZE_DECIMALS)
            market_order = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=side,
                price=price,
                order_type=order_type,
            )
            signed = self.client.create_market_order(market_order, meta)
        else:
            order = OrderArgs(token_id=token_id, price=price, size=size, side=side)
            signed = self.client.create_order(order, meta)
        response = self.client.post_order(signed, order_type)
        if isinstance(response, dict) and response.get("success") is False:
            raise RuntimeError(f"post_order_failed:{json.dumps(response, separators=(',', ':'), ensure_ascii=False)}")
        if not isinstance(response, dict):
            return {"raw_response": response}
        return response

    def get_collateral_balance_usdc(self) -> Optional[float]:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

        collateral_asset_type = cast(Any, getattr(AssetType, "COLLATERAL", "COLLATERAL"))
        resp = self.client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=collateral_asset_type,
                token_id="",
                signature_type=SIGNATURE_TYPE,
            )
        )
        if not isinstance(resp, dict):
            return None
        bal_raw = resp.get("balance")
        if bal_raw is None:
            return None
        try:
            base_units = float(bal_raw)
        except (TypeError, ValueError):
            return None
        return base_units / USDC_BASE_UNITS

    def redeem_redeemable_positions(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "positions_scanned": 0,
            "redeemable_positions": 0,
            "condition_groups": 0,
            "attempted": 0,
            "submitted": 0,
            "skipped_proxy_wallet": 0,
            "skipped_missing_data": 0,
            "errors": 0,
            "tx_hashes": [],
        }
        if AUTO_REDEEM_REQUIRE_SIGNER_EQUALS_FUNDER and self.signer_address != self.funder:
            out["skipped_proxy_wallet"] = 1
            out["reason"] = "signer_not_funder_proxy_wallet_mode"
            return out

        rows = fetch_positions_snapshot(self.funder, limit=500, size_threshold=0.0)
        out["positions_scanned"] = len(rows)

        groups: Dict[Tuple[str, bool], Set[int]] = {}
        for row in rows:
            if not bool(row.get("redeemable")):
                continue
            size = _safe_float(row.get("size"))
            if size is None or size <= 0:
                continue
            condition_id = str(row.get("conditionId") or "").strip().lower()
            if not condition_id:
                out["skipped_missing_data"] = int(out["skipped_missing_data"]) + 1
                continue
            outcome_idx_raw = row.get("outcomeIndex")
            outcome_idx_val = _safe_float(outcome_idx_raw)
            if outcome_idx_val is None or not float(outcome_idx_val).is_integer():
                out["skipped_missing_data"] = int(out["skipped_missing_data"]) + 1
                continue
            outcome_idx = int(outcome_idx_val)
            if outcome_idx < 0 or outcome_idx > 255:
                out["skipped_missing_data"] = int(out["skipped_missing_data"]) + 1
                continue
            index_set = 1 << outcome_idx
            neg_risk = bool(row.get("negativeRisk"))
            key = (condition_id, neg_risk)
            if key not in groups:
                groups[key] = set()
            groups[key].add(index_set)
            out["redeemable_positions"] = int(out["redeemable_positions"]) + 1

        out["condition_groups"] = len(groups)
        if not groups:
            return out

        try:
            import importlib

            web3_mod = importlib.import_module("web3")
            Web3 = getattr(web3_mod, "Web3")
            from py_clob_client.config import get_contract_config
        except Exception as exc:
            out["errors"] = len(groups)
            out["error"] = f"redeem_dependency_missing:{exc}"
            return out

        w3 = Web3(Web3.HTTPProvider(AUTO_REDEEM_RPC_URL, request_kwargs={"timeout": 20}))
        if not w3.is_connected():
            out["errors"] = len(groups)
            out["error"] = "redeem_rpc_unreachable"
            return out

        redeem_abi = [
            {
                "inputs": [
                    {"internalType": "address", "name": "collateralToken", "type": "address"},
                    {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
                    {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
                    {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
                ],
                "name": "redeemPositions",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ]

        try:
            nonce = int(w3.eth.get_transaction_count(Web3.to_checksum_address(self.signer_address), "pending"))
        except Exception as exc:
            out["errors"] = len(groups)
            out["error"] = f"redeem_nonce_failed:{exc}"
            return out

        for (condition_id, neg_risk), index_sets in groups.items():
            out["attempted"] = int(out["attempted"]) + 1
            try:
                cfg = get_contract_config(self.chain_id, bool(neg_risk))
                conditional_tokens = Web3.to_checksum_address(str(cfg.conditional_tokens))
                collateral = Web3.to_checksum_address(str(cfg.collateral))
                if not (condition_id.startswith("0x") and len(condition_id) == 66):
                    raise ValueError(f"invalid_condition_id:{condition_id}")
                condition_bytes = bytes.fromhex(condition_id[2:])
                parent = b"\x00" * 32
                index_list = sorted(int(v) for v in index_sets if int(v) > 0)
                if not index_list:
                    raise ValueError("empty_index_sets")

                contract = w3.eth.contract(address=conditional_tokens, abi=redeem_abi)
                tx = contract.functions.redeemPositions(
                    collateral,
                    parent,
                    condition_bytes,
                    index_list,
                ).build_transaction(
                    {
                        "from": Web3.to_checksum_address(self.signer_address),
                        "nonce": nonce,
                        "chainId": self.chain_id,
                    }
                )

                try:
                    gas_est = int(w3.eth.estimate_gas(tx))
                except Exception:
                    gas_est = 350_000
                tx["gas"] = max(250_000, min(1_200_000, int(gas_est * 1.3)))

                try:
                    latest = w3.eth.get_block("latest")
                    base_fee = int(latest.get("baseFeePerGas") or 0)
                except Exception:
                    base_fee = 0
                if base_fee > 0:
                    priority = int(w3.to_wei(2, "gwei"))
                    tx["maxPriorityFeePerGas"] = priority
                    tx["maxFeePerGas"] = int(base_fee * 2 + priority)
                else:
                    tx["gasPrice"] = int(w3.eth.gas_price * 1.2)

                signed = w3.eth.account.sign_transaction(tx, private_key=self.private_key)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
                out["submitted"] = int(out["submitted"]) + 1
                out["tx_hashes"].append(tx_hash)
                nonce += 1
            except Exception as exc:
                out["errors"] = int(out["errors"]) + 1
                if "error_details" not in out:
                    out["error_details"] = []
                if isinstance(out["error_details"], list) and len(out["error_details"]) < 20:
                    out["error_details"].append(
                        {
                            "condition_id": condition_id,
                            "neg_risk": bool(neg_risk),
                            "index_sets": sorted(list(index_sets)),
                            "error": str(exc),
                        }
                    )
        return out


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
    if not rows:
        if not QUIET_STATUS:
            print(
                (
                    f"fast_copy poll user={state.user} window=[{start},{end}] rows=0 "
                    f"emitted=0 fresh=0 last_seen={state.last_seen_ts}"
                ),
                file=sys.stderr,
            )
        return []

    emitted = 0
    fresh = 0
    max_ts = state.last_seen_ts
    signals: List[Dict[str, Any]] = []
    seen_at_batch = time.time()

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
        signal = _extract_signal(user=state.user, row=row, seen_at_unix=seen_at_batch)
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
        source_size_sum = float(b["source_size_sum"])
        source_usdc_sum = float(b["source_usdc_sum"])
        weighted_price = source_usdc_sum / source_size_sum if source_size_sum > 0 else float(b["source_price_min"])
        out.append(
            {
                "token_id": b["token_id"],
                "side": side,
                "aggregated_count": int(b["count"]),
                "source_users": list(b["source_users"]),
                "signal_sources": list(b["signal_sources"]),
                "trigger_markets": list(b["trigger_markets"]),
                "source_usdc_sum": source_usdc_sum,
                "source_size_sum": source_size_sum,
                "source_price_min": float(b["source_price_min"]),
                "source_price_max": float(b["source_price_max"]),
                "source_price_weighted": weighted_price,
                "source_price_selected": weighted_price,
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
    return out


def _execute_aggregated_signal(signal: Dict[str, Any], executor: FastClobExecutor) -> None:
    side = str(signal["side"])
    token_id = str(signal["token_id"])
    source_price = float(signal["source_price_selected"])
    position_size: Optional[float] = None
    sell_mode = SELL_SIZING_MODE.strip().lower()
    effective_target_usdc_raw = 0.0
    advance_consumed = 0.0
    advance_debt_after = 0.0
    advance_created = 0.0
    source_tx = (
        f"agg_batch:{side.lower()}:{token_id}:{int(signal['aggregated_count'])}:"
        f"{signal['aggregated_first_tx']}:{signal['aggregated_last_tx']}"
    )

    if side == "BUY":
        target_usdc_raw = float(signal["source_usdc_sum"]) * COPY_SCALE
        if ENABLE_MIN_NOTIONAL_ADVANCE:
            effective_target_usdc_raw, advance_consumed, advance_debt_after = executor.consume_advance_debt(
                token_id=token_id,
                side=side,
                fresh_notional=target_usdc_raw,
            )
            if effective_target_usdc_raw <= 0:
                _log_json(
                    {
                        "copy_status": "SKIPPED",
                        "reason": "advance_credit_consumed",
                        "source_tx": source_tx,
                        "token_id": token_id,
                        "side": side,
                        "aggregated_count": int(signal["aggregated_count"]),
                        "target_usdc_raw": round(target_usdc_raw, 6),
                        "advance_consumed": round(advance_consumed, 6),
                        "advance_debt_after": round(advance_debt_after, 6),
                    }
                )
                return
        else:
            effective_target_usdc_raw = target_usdc_raw

        target_usdc = effective_target_usdc_raw
        if BUY_MAX_USDC > 0:
            target_usdc = min(target_usdc, BUY_MAX_USDC)
        target_usdc = _round_to_decimals(target_usdc, BUY_USDC_DECIMALS)
        if target_usdc < BUY_MIN_USDC or target_usdc <= 0:
            if ENABLE_MIN_NOTIONAL_ADVANCE and BUY_MIN_USDC > 0:
                promoted_target = BUY_MIN_USDC
                if BUY_MAX_USDC > 0:
                    promoted_target = min(promoted_target, BUY_MAX_USDC)
                promoted_target = _round_to_decimals(promoted_target, BUY_USDC_DECIMALS)
                if promoted_target <= 0 or promoted_target < BUY_MIN_USDC:
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
                            "target_usdc_effective": round(effective_target_usdc_raw, 6),
                            "target_usdc": round(target_usdc, 6),
                        }
                    )
                    return
                target_usdc = promoted_target
            else:
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
                        "target_usdc_effective": round(effective_target_usdc_raw, 6),
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
        target_usdc_raw = float(signal["source_usdc_sum"]) * COPY_SCALE
        if sell_mode == "all":
            effective_target_usdc_raw = target_usdc_raw
            copy_size = position_size
            target_usdc = copy_size * source_price
        else:
            if ENABLE_MIN_NOTIONAL_ADVANCE:
                effective_target_usdc_raw, advance_consumed, advance_debt_after = executor.consume_advance_debt(
                    token_id=token_id,
                    side=side,
                    fresh_notional=target_usdc_raw,
                )
                if effective_target_usdc_raw <= 0:
                    _log_json(
                        {
                            "copy_status": "SKIPPED",
                            "reason": "advance_credit_consumed",
                            "source_tx": source_tx,
                            "token_id": token_id,
                            "side": side,
                            "aggregated_count": int(signal["aggregated_count"]),
                            "target_usdc_raw": round(target_usdc_raw, 6),
                            "advance_consumed": round(advance_consumed, 6),
                            "advance_debt_after": round(advance_debt_after, 6),
                        }
                    )
                    return
            else:
                effective_target_usdc_raw = target_usdc_raw
            target_usdc = _round_to_decimals(effective_target_usdc_raw, BUY_USDC_DECIMALS)
            if target_usdc <= 0 or target_usdc < SELL_MIN_USDC:
                if ENABLE_MIN_NOTIONAL_ADVANCE and SELL_MIN_USDC > 0:
                    target_usdc = _round_to_decimals(SELL_MIN_USDC, BUY_USDC_DECIMALS)
                else:
                    _log_json(
                        {
                            "copy_status": "SKIPPED",
                            "reason": "below_min_scaled_sell_usdc",
                            "source_tx": source_tx,
                            "token_id": token_id,
                            "side": side,
                            "aggregated_count": int(signal["aggregated_count"]),
                            "source_price_selected": round(source_price, 6),
                            "target_usdc_raw": round(target_usdc_raw, 6),
                            "target_usdc_effective": round(effective_target_usdc_raw, 6),
                            "target_usdc": round(target_usdc, 6),
                            "sell_min_usdc": SELL_MIN_USDC,
                        }
                    )
                    return
            copy_size = min(position_size, (target_usdc / source_price))
            target_usdc = copy_size * source_price
        order_price_raw = source_price * SELL_PRICE_MULTIPLIER
        order_type = SELL_ORDER_TYPE.upper()

    copy_size = round(copy_size, SIZE_DECIMALS)
    if side == "SELL" and position_size is not None and copy_size > position_size:
        copy_size = round(position_size, SIZE_DECIMALS)
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
    target_usdc = _round_to_decimals(copy_size * source_price, BUY_USDC_DECIMALS)
    if side == "BUY" and target_usdc < BUY_MIN_USDC:
        _log_json(
            {
                "copy_status": "SKIPPED",
                "reason": "below_min_buy_after_rounding",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
                "target_usdc": round(target_usdc, 6),
                "buy_min_usdc": BUY_MIN_USDC,
            }
        )
        return
    if side == "SELL" and sell_mode == "scaled" and target_usdc < SELL_MIN_USDC:
        _log_json(
            {
                "copy_status": "SKIPPED",
                "reason": "below_min_scaled_sell_after_rounding",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
                "target_usdc": round(target_usdc, 6),
                "sell_min_usdc": SELL_MIN_USDC,
            }
        )
        return

    if ENABLE_MIN_NOTIONAL_ADVANCE and side == "BUY":
        rounded_effective = _round_to_decimals(effective_target_usdc_raw, BUY_USDC_DECIMALS)
        advance_created = max(0.0, target_usdc - rounded_effective)
        if advance_created > 0:
            advance_debt_after = executor.add_advance_debt(token_id=token_id, side=side, debt_notional=advance_created)
    if ENABLE_MIN_NOTIONAL_ADVANCE and side == "SELL" and sell_mode == "scaled":
        rounded_effective = _round_to_decimals(effective_target_usdc_raw, BUY_USDC_DECIMALS)
        advance_created = max(0.0, target_usdc - rounded_effective)
        if advance_created > 0:
            advance_debt_after = executor.add_advance_debt(token_id=token_id, side=side, debt_notional=advance_created)

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
            buy_notional_usdc=target_usdc if side == "BUY" else None,
        )
    except Exception as exc:
        error_text = str(exc)
        no_liquidity_fak = ("no orders found to match with FAK order" in error_text) and (order_type == "FAK")
        _log_json(
            {
                "copy_status": "SKIPPED" if no_liquidity_fak else "ERROR",
                "reason": "fak_no_liquidity" if no_liquidity_fak else "submit_failed",
                "source_tx": source_tx,
                "token_id": token_id,
                "side": side,
                "aggregated_count": int(signal["aggregated_count"]),
                "source_price_selected": round(source_price, 6),
                "order_price": round(order_price, 6),
                "size": copy_size,
                "target_usdc": round(target_usdc, 6),
                "order_type": order_type,
                "advance_consumed": round(advance_consumed, 6),
                "advance_created": round(advance_created, 6),
                "advance_debt_after": round(advance_debt_after, 6),
                "error": error_text,
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
            "sell_sizing_mode": SELL_SIZING_MODE,
            "advance_consumed": round(advance_consumed, 6),
            "advance_created": round(advance_created, 6),
            "advance_debt_after": round(advance_debt_after, 6),
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
    if AGGREGATE_NEIGHBOR_POLLS <= 0:
        raise ValueError("AGGREGATE_NEIGHBOR_POLLS must be > 0")
    if BUY_ORDER_TYPE.upper() not in {"FAK", "GTC"}:
        raise ValueError("BUY_ORDER_TYPE must be FAK or GTC")
    if SELL_ORDER_TYPE.upper() not in {"FAK", "GTC"}:
        raise ValueError("SELL_ORDER_TYPE must be FAK or GTC")
    if SELL_SIZING_MODE.strip().lower() not in {"scaled", "all"}:
        raise ValueError("SELL_SIZING_MODE must be 'scaled' or 'all'")
    if BUY_MIN_USDC < 0 or BUY_MAX_USDC < 0:
        raise ValueError("BUY_MIN_USDC/BUY_MAX_USDC must be >= 0")
    if SELL_MIN_USDC < 0:
        raise ValueError("SELL_MIN_USDC must be >= 0")
    if SIZE_DECIMALS < 0:
        raise ValueError("SIZE_DECIMALS must be >= 0")
    if BUY_USDC_DECIMALS < 0:
        raise ValueError("BUY_USDC_DECIMALS must be >= 0")
    if ADVANCE_LEDGER_MAX_AGE_SECONDS <= 0:
        raise ValueError("ADVANCE_LEDGER_MAX_AGE_SECONDS must be > 0")
    if MIN_USDC_CASH_STOP < 0:
        raise ValueError("MIN_USDC_CASH_STOP must be >= 0")
    if CASH_GUARD_CHECK_INTERVAL_SECONDS <= 0:
        raise ValueError("CASH_GUARD_CHECK_INTERVAL_SECONDS must be > 0")
    if AUTO_REDEEM_INTERVAL_SECONDS <= 0:
        raise ValueError("AUTO_REDEEM_INTERVAL_SECONDS must be > 0")
    if ENABLE_AUTO_REDEEM and not AUTO_REDEEM_RPC_URL:
        raise ValueError("AUTO_REDEEM_RPC_URL is required when ENABLE_AUTO_REDEEM=true")
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
                "aggregate_mode": "weighted_average_price",
                "aggregate_neighbor_polls": AGGREGATE_NEIGHBOR_POLLS,
                "max_pending_signals": MAX_PENDING_SIGNALS,
                "poll_interval_seconds": POLL_INTERVAL_SECONDS,
                "buy_usdc_decimals": BUY_USDC_DECIMALS,
                "size_decimals": SIZE_DECIMALS,
                "sell_sizing_mode": SELL_SIZING_MODE,
                "sell_min_usdc": SELL_MIN_USDC,
                "enable_min_notional_advance": ENABLE_MIN_NOTIONAL_ADVANCE,
                "advance_ledger_max_age_seconds": ADVANCE_LEDGER_MAX_AGE_SECONDS,
                "enable_cash_guard": ENABLE_CASH_GUARD,
                "min_usdc_cash_stop": MIN_USDC_CASH_STOP,
                "cash_guard_check_interval_seconds": CASH_GUARD_CHECK_INTERVAL_SECONDS,
                "cash_guard_fail_closed": CASH_GUARD_FAIL_CLOSED,
                "enable_auto_redeem": ENABLE_AUTO_REDEEM,
                "auto_redeem_interval_seconds": AUTO_REDEEM_INTERVAL_SECONDS,
                "auto_redeem_rpc_url": AUTO_REDEEM_RPC_URL,
                "auto_redeem_require_signer_equals_funder": AUTO_REDEEM_REQUIRE_SIGNER_EQUALS_FUNDER,
                "auto_redeem_fail_closed": AUTO_REDEEM_FAIL_CLOSED,
                "slippage_guard_enabled": ENABLE_SLIPPAGE_GUARD,
                "max_slippage_pct": MAX_SLIPPAGE_PCT,
                "stale_buy_sweep_enabled": ENABLE_STALE_BUY_SWEEP,
                "stale_buy_order_max_age_seconds": STALE_BUY_ORDER_MAX_AGE_SECONDS,
                "stale_buy_sweep_interval_seconds": STALE_BUY_SWEEP_INTERVAL_SECONDS,
                "positions_cache_ttl_seconds": POSITIONS_CACHE_TTL_SECONDS,
                "log_empty_stale_sweeps": LOG_EMPTY_STALE_SWEEPS,
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )

    next_tick = time.monotonic()
    next_stale_buy_sweep_at = time.monotonic()
    next_cash_guard_check_at = time.monotonic()
    next_redeem_check_at = time.monotonic()
    pending_signals: List[Dict[str, Any]] = []
    polls_since_flush = 0
    poll_workers = max(1, min(len(states), 8))
    poll_pool = ThreadPoolExecutor(max_workers=poll_workers, thread_name_prefix="poll")
    maintenance_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="maint")
    sweep_future: Optional[Future] = None
    cash_guard_future: Optional[Future] = None
    redeem_future: Optional[Future] = None
    try:
        if ENABLE_CASH_GUARD:
            try:
                startup_cash = executor.get_collateral_balance_usdc()
            except Exception as exc:
                _log_json(
                    {
                        "copy_status": "ERROR",
                        "reason": "cash_guard_check_failed",
                        "phase": "startup",
                        "error": str(exc),
                    }
                )
                if CASH_GUARD_FAIL_CLOSED:
                    _log_json(
                        {
                            "copy_status": "STOPPED",
                            "reason": "cash_guard_fail_closed",
                            "phase": "startup",
                        }
                    )
                    return 1
                startup_cash = None
            if startup_cash is None:
                _log_json(
                    {
                        "copy_status": "WARN",
                        "reason": "cash_guard_unavailable",
                        "phase": "startup",
                    }
                )
            else:
                _log_json(
                    {
                        "copy_status": "CASH_GUARD_CHECK",
                        "phase": "startup",
                        "usdc_balance": round(float(startup_cash), 6),
                        "threshold": MIN_USDC_CASH_STOP,
                    }
                )
                if float(startup_cash) <= MIN_USDC_CASH_STOP:
                    _log_json(
                        {
                            "copy_status": "STOPPED",
                            "reason": "cash_guard_trigger",
                            "phase": "startup",
                            "usdc_balance": round(float(startup_cash), 6),
                            "threshold": MIN_USDC_CASH_STOP,
                        }
                    )
                    return 0

        while True:
            cycle_signals: List[Dict[str, Any]] = []
            poll_futures: Dict[Future, str] = {}
            for state in states:
                fut = poll_pool.submit(_collect_user_signals_from_activity, state)
                poll_futures[fut] = state.user
            for fut, user in poll_futures.items():
                try:
                    cycle_signals.extend(fut.result())
                except Exception as exc:
                    _log_json(
                        {
                            "copy_status": "ERROR",
                            "reason": "poll_failed",
                            "source_user": user,
                            "error": str(exc),
                        }
                    )

            if cycle_signals:
                pending_signals.extend(cycle_signals)
            polls_since_flush += 1
            force_flush = len(pending_signals) >= MAX_PENDING_SIGNALS
            should_flush = force_flush or (polls_since_flush >= AGGREGATE_NEIGHBOR_POLLS)

            aggregated_count = 0
            if should_flush:
                aggregated = _aggregate_signals(pending_signals)
                aggregated_count = len(aggregated)
                for signal in aggregated:
                    _execute_aggregated_signal(signal, executor)
                pending_signals.clear()
                polls_since_flush = 0

            if not QUIET_STATUS:
                print(
                    (
                        f"fast_copy cycle raw_signals={len(cycle_signals)} pending={len(pending_signals)} "
                        f"aggregated_orders={aggregated_count} polls_to_flush={AGGREGATE_NEIGHBOR_POLLS - polls_since_flush}"
                    ),
                    file=sys.stderr,
                )

            if sweep_future is not None and sweep_future.done():
                try:
                    sweep = sweep_future.result()
                    orders_scanned = int(sweep.get("orders_scanned", 0))
                    orders_cancelled = int(sweep.get("orders_cancelled", 0))
                    errors = int(sweep.get("errors", 0))
                    if LOG_EMPTY_STALE_SWEEPS or orders_scanned > 0 or orders_cancelled > 0 or errors > 0:
                        _log_json(
                            {
                                "copy_status": "STALE_BUY_SWEEP",
                                "orders_scanned": orders_scanned,
                                "orders_cancelled": orders_cancelled,
                                "errors": errors,
                                "max_age_seconds": STALE_BUY_ORDER_MAX_AGE_SECONDS,
                            }
                        )
                except Exception as exc:
                    _log_json(
                        {
                            "copy_status": "ERROR",
                            "reason": "stale_buy_sweep_failed",
                            "error": str(exc),
                        }
                    )
                sweep_future = None

            if cash_guard_future is not None and cash_guard_future.done():
                try:
                    cash_balance = cash_guard_future.result()
                    if cash_balance is None:
                        _log_json(
                            {
                                "copy_status": "WARN",
                                "reason": "cash_guard_unavailable",
                                "phase": "runtime",
                            }
                        )
                        if CASH_GUARD_FAIL_CLOSED:
                            _log_json(
                                {
                                    "copy_status": "STOPPED",
                                    "reason": "cash_guard_fail_closed",
                                    "phase": "runtime",
                                }
                            )
                            return 1
                    else:
                        cash_value = float(cash_balance)
                        _log_json(
                            {
                                "copy_status": "CASH_GUARD_CHECK",
                                "phase": "runtime",
                                "usdc_balance": round(cash_value, 6),
                                "threshold": MIN_USDC_CASH_STOP,
                            }
                        )
                        if cash_value <= MIN_USDC_CASH_STOP:
                            _log_json(
                                {
                                    "copy_status": "STOPPED",
                                    "reason": "cash_guard_trigger",
                                    "phase": "runtime",
                                    "usdc_balance": round(cash_value, 6),
                                    "threshold": MIN_USDC_CASH_STOP,
                                }
                            )
                            return 0
                except Exception as exc:
                    _log_json(
                        {
                            "copy_status": "ERROR",
                            "reason": "cash_guard_check_failed",
                            "phase": "runtime",
                            "error": str(exc),
                        }
                    )
                    if CASH_GUARD_FAIL_CLOSED:
                        _log_json(
                            {
                                "copy_status": "STOPPED",
                                "reason": "cash_guard_fail_closed",
                                "phase": "runtime",
                            }
                        )
                        return 1
                cash_guard_future = None

            if redeem_future is not None and redeem_future.done():
                try:
                    redeem_result = redeem_future.result()
                    if not isinstance(redeem_result, dict):
                        redeem_result = {"raw_result": str(redeem_result)}
                    _log_json(
                        {
                            "copy_status": "AUTO_REDEEM",
                            "positions_scanned": int(redeem_result.get("positions_scanned", 0)),
                            "redeemable_positions": int(redeem_result.get("redeemable_positions", 0)),
                            "condition_groups": int(redeem_result.get("condition_groups", 0)),
                            "attempted": int(redeem_result.get("attempted", 0)),
                            "submitted": int(redeem_result.get("submitted", 0)),
                            "skipped_proxy_wallet": int(redeem_result.get("skipped_proxy_wallet", 0)),
                            "skipped_missing_data": int(redeem_result.get("skipped_missing_data", 0)),
                            "errors": int(redeem_result.get("errors", 0)),
                            "tx_hashes": redeem_result.get("tx_hashes", []),
                            "error": redeem_result.get("error"),
                            "error_details": redeem_result.get("error_details", []),
                        }
                    )
                    if int(redeem_result.get("submitted", 0)) > 0:
                        executor.positions_cache_at = 0.0
                    if AUTO_REDEEM_FAIL_CLOSED and (
                        int(redeem_result.get("errors", 0)) > 0 or bool(redeem_result.get("error"))
                    ):
                        _log_json(
                            {
                                "copy_status": "STOPPED",
                                "reason": "auto_redeem_fail_closed",
                            }
                        )
                        return 1
                except Exception as exc:
                    _log_json(
                        {
                            "copy_status": "ERROR",
                            "reason": "auto_redeem_failed",
                            "error": str(exc),
                        }
                    )
                    if AUTO_REDEEM_FAIL_CLOSED:
                        _log_json(
                            {
                                "copy_status": "STOPPED",
                                "reason": "auto_redeem_fail_closed",
                            }
                        )
                        return 1
                redeem_future = None

            now_mono = time.monotonic()
            if (
                ENABLE_STALE_BUY_SWEEP
                and STALE_BUY_ORDER_MAX_AGE_SECONDS > 0
                and now_mono >= next_stale_buy_sweep_at
                and sweep_future is None
            ):
                sweep_future = maintenance_pool.submit(
                    executor.sweep_stale_buy_orders,
                    max_age_seconds=STALE_BUY_ORDER_MAX_AGE_SECONDS,
                )
                next_stale_buy_sweep_at = now_mono + STALE_BUY_SWEEP_INTERVAL_SECONDS

            if (
                ENABLE_CASH_GUARD
                and now_mono >= next_cash_guard_check_at
                and cash_guard_future is None
            ):
                cash_guard_future = maintenance_pool.submit(executor.get_collateral_balance_usdc)
                next_cash_guard_check_at = now_mono + CASH_GUARD_CHECK_INTERVAL_SECONDS

            if (
                ENABLE_AUTO_REDEEM
                and now_mono >= next_redeem_check_at
                and redeem_future is None
            ):
                redeem_future = maintenance_pool.submit(executor.redeem_redeemable_positions)
                next_redeem_check_at = now_mono + AUTO_REDEEM_INTERVAL_SECONDS

            next_tick += POLL_INTERVAL_SECONDS
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.monotonic()
    finally:
        poll_pool.shutdown(wait=False, cancel_futures=True)
        maintenance_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
