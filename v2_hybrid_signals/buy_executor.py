from __future__ import annotations

import math
import time
from typing import Any, Dict, Iterable, List, Optional

from .buy_config import BuyConfig
from .price_feed import MarketPriceFeed
from .utils import as_float as _as_float

try:
    from py_clob_client.client import ClobClient as _ClobClient
    from py_clob_client.clob_types import ApiCreds as _ApiCreds
    from py_clob_client.clob_types import MarketOrderArgs as _MarketOrderArgs
    _CLOB_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - dependency/environment failure path
    _ClobClient = None  # type: ignore[assignment]
    _ApiCreds = None  # type: ignore[assignment]
    _MarketOrderArgs = None  # type: ignore[assignment]
    _CLOB_IMPORT_ERROR = exc


def _normalize_prob(raw: Any) -> Optional[float]:
    value = _as_float(raw)
    if value is None:
        return None
    if value > 1.0:
        value = value / 100.0
    return max(0.0, min(1.0, value))


def _round_up_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return value
    units = math.ceil(value / tick)
    return round(units * tick, 10)


class BuyExecutor:
    def __init__(self, cfg: BuyConfig, price_feed: MarketPriceFeed) -> None:
        self.cfg = cfg
        self.price_feed = price_feed

        if _ClobClient is not None:
            self.readonly_client = _ClobClient(cfg.clob_host, chain_id=cfg.chain_id)
        else:
            self.readonly_client = None
        self.client: Optional[Any] = None
        self.api_creds_source = "none"

        self.accepted = 0
        self.submitted = 0
        self.skipped = 0
        self.errors = 0

        self._seen_match_keys: set[str] = set()
        self._spent_market: Dict[str, float] = {}
        self._spent_token: Dict[str, float] = {}

        self.meta_prefetch_ok = 0
        self.meta_prefetch_errors = 0

        self._tick_cache: Dict[str, float] = {}

        if not cfg.dry_run:
            if _ClobClient is None:
                raise RuntimeError(f"py_clob_client_unavailable: {_CLOB_IMPORT_ERROR}")
            self.client = _ClobClient(
                cfg.clob_host,
                chain_id=cfg.chain_id,
                key=cfg.private_key,
                signature_type=cfg.signature_type,
                funder=cfg.funder,
            )
            if cfg.api_key and cfg.api_secret and cfg.api_passphrase:
                if _ApiCreds is None:
                    raise RuntimeError(f"py_clob_client_types_unavailable: {_CLOB_IMPORT_ERROR}")
                self.client.set_api_creds(
                    _ApiCreds(api_key=cfg.api_key, api_secret=cfg.api_secret, api_passphrase=cfg.api_passphrase)
                )
                self.api_creds_source = "env"
            else:
                creds = self.client.create_or_derive_api_creds()
                if creds is None:
                    raise RuntimeError("failed_to_create_or_derive_api_creds")
                self.client.set_api_creds(creds)
                self.api_creds_source = "derived"

    def prefetch_market_tokens(self, live_markets: Iterable[Dict[str, Any]]) -> None:
        for market in live_markets:
            tokens = market.get("tokens")
            if not isinstance(tokens, list):
                continue
            for token in tokens:
                if not isinstance(token, dict):
                    continue
                token_id = str(token.get("token_id") or "").strip()
                if not token_id:
                    continue
                self.prewarm_token(token_id)

    def prewarm_token(self, token_id: str) -> None:
        try:
            self.price_feed.track_token(token_id)
            self.meta_prefetch_ok += 1
        except Exception:
            self.meta_prefetch_errors += 1

    def _skip(self, reason: str, **extra: Any) -> Dict[str, Any]:
        self.skipped += 1
        payload: Dict[str, Any] = {"decision": "skip", "reason": reason}
        payload.update(extra)
        return payload

    def _token_tick_size(self, token_id: str, quote: Optional[Dict[str, Any]]) -> float:
        cached = self._tick_cache.get(token_id)
        if cached is not None and cached > 0:
            return cached

        if quote is not None:
            q_tick = _as_float(quote.get("tick_size"))
            if q_tick is not None and q_tick > 0:
                self._tick_cache[token_id] = q_tick
                return q_tick

        try:
            if self.readonly_client is None:
                return 0.01
            tick_raw = self.readonly_client.get_tick_size(token_id)
            tick = _as_float(tick_raw)
            if tick is not None and tick > 0:
                self._tick_cache[token_id] = tick
                return tick
        except Exception:
            pass

        return 0.01

    def _signal_matches_live_market(self, signal: Dict[str, Any], live_markets: List[Dict[str, Any]]) -> bool:
        if not live_markets:
            return False
        signal_condition = str(signal.get("condition_id") or "").strip().lower()
        signal_slug = str(signal.get("slug") or "").strip().lower()
        signal_token = str(signal.get("token_id") or "").strip()

        for market in live_markets:
            condition = str(market.get("condition_id") or "").strip().lower()
            slug = str(market.get("slug") or "").strip().lower()
            if signal_condition and condition and signal_condition == condition:
                return True
            if signal_slug and slug and signal_slug == slug:
                return True
            tokens = market.get("tokens")
            if signal_token and isinstance(tokens, list):
                for token in tokens:
                    if not isinstance(token, dict):
                        continue
                    if str(token.get("token_id") or "").strip() == signal_token:
                        return True
        return False

    def _requested_usdc(self, signal: Dict[str, Any]) -> tuple[float, float, str]:
        source_usdc = _as_float(signal.get("usdc_size")) or 0.0
        if self.cfg.copy_scale > 0 and source_usdc > 0:
            requested = source_usdc * self.cfg.copy_scale
            sizing_mode = "scaled"
        else:
            requested = float(self.cfg.order_usdc)
            sizing_mode = "fixed"
        if self.cfg.max_single_order_usdc > 0:
            requested = min(requested, float(self.cfg.max_single_order_usdc))
        return requested, source_usdc, sizing_mode

    def _resolve_quote(self, token_id: str, source_price: Optional[float]) -> Optional[Dict[str, Any]]:
        self.price_feed.track_token(token_id)
        quote = self.price_feed.wait_for_quote(
            token_id,
            timeout_seconds=self.cfg.quote_wait_timeout_seconds,
            max_age_seconds=self.cfg.quote_stale_after_seconds,
        )
        if quote is not None:
            return quote

        quote = self.price_feed.get_quote(token_id, max_age_seconds=self.cfg.quote_fallback_stale_seconds)
        if quote is not None:
            return quote

        # In dry-run mode, allow source price fallback so replay tests still produce decisions.
        if self.cfg.dry_run and source_price is not None:
            return {
                "token_id": token_id,
                "best_bid": source_price,
                "best_ask": source_price,
                "tick_size": 0.01,
                "updated_at_unix": time.time(),
                "source": "source_price_fallback",
            }

        return self.price_feed.fetch_book_once(token_id)

    def process_actionable(
        self,
        payload: Dict[str, Any],
        *,
        live_markets: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        match_key = str(payload.get("match_key") or "").strip()
        if not match_key:
            return self._skip("missing_match_key")
        if match_key in self._seen_match_keys:
            return self._skip("duplicate_match_key", match_key=match_key)
        self._seen_match_keys.add(match_key)
        self.accepted += 1

        source = str(payload.get("source") or "").strip().lower()
        if source and source not in self.cfg.actionable_sources:
            return self._skip(
                "source_not_allowed",
                source=source,
                allowed_sources=list(self.cfg.actionable_sources),
            )

        signal = payload.get("signal")
        if not isinstance(signal, dict):
            return self._skip("malformed_actionable_signal")

        side = str(signal.get("side") or "").strip().upper()
        if side != "BUY" and not self.cfg.allow_sell_signals:
            return self._skip("non_buy_signal")

        event_ts = _as_float(payload.get("event_ts"))
        if self.cfg.max_signal_age_seconds > 0 and event_ts is not None and event_ts > 0:
            age = max(0.0, time.time() - event_ts)
            if age > self.cfg.max_signal_age_seconds:
                return self._skip(
                    "signal_too_old",
                    signal_age_seconds=round(age, 4),
                    max_signal_age_seconds=self.cfg.max_signal_age_seconds,
                )

        source_price = _normalize_prob(signal.get("price"))
        if source_price is not None and source_price < self.cfg.min_source_signal_price:
            return self._skip(
                "source_price_below_min",
                source_price=source_price,
                min_source_signal_price=self.cfg.min_source_signal_price,
            )

        token_id = str(signal.get("token_id") or "").strip()
        if not token_id:
            return self._skip("missing_token_id")

        if self.cfg.actionable_require_live_market_match:
            if not self._signal_matches_live_market(signal, list(live_markets or [])):
                return self._skip("live_market_mismatch")

        requested_usdc, source_usdc, sizing_mode = self._requested_usdc(signal)
        if requested_usdc < self.cfg.min_order_usdc:
            return self._skip(
                "order_too_small_after_sizing",
                requested_usdc=round(requested_usdc, 6),
                source_usdc=round(source_usdc, 6),
                copy_scale=self.cfg.copy_scale,
            )

        condition_id = str(signal.get("condition_id") or "").strip().lower()
        market_key = condition_id or str(signal.get("slug") or "").strip().lower() or token_id

        spent_market = self._spent_market.get(market_key, 0.0)
        spent_token = self._spent_token.get(token_id, 0.0)

        if self.cfg.max_usdc_per_market > 0 and spent_market + requested_usdc > self.cfg.max_usdc_per_market + 1e-12:
            return self._skip(
                "market_budget_exceeded",
                market_key=market_key,
                spent_market=round(spent_market, 6),
                requested_usdc=round(requested_usdc, 6),
                max_usdc_per_market=self.cfg.max_usdc_per_market,
            )
        if self.cfg.max_usdc_per_token > 0 and spent_token + requested_usdc > self.cfg.max_usdc_per_token + 1e-12:
            return self._skip(
                "token_budget_exceeded",
                token_id=token_id,
                spent_token=round(spent_token, 6),
                requested_usdc=round(requested_usdc, 6),
                max_usdc_per_token=self.cfg.max_usdc_per_token,
            )

        quote = self._resolve_quote(token_id, source_price)
        if quote is None:
            return self._skip("quote_unavailable")

        best_ask = _normalize_prob(quote.get("best_ask"))
        if best_ask is None:
            return self._skip("quote_missing_best_ask")

        if best_ask < self.cfg.min_price or best_ask > self.cfg.max_price:
            return self._skip(
                "best_ask_out_of_range",
                best_ask=best_ask,
                min_price=self.cfg.min_price,
                max_price=self.cfg.max_price,
            )

        tick_size = self._token_tick_size(token_id, quote)
        worst_price = min(self.cfg.max_price, best_ask + self.cfg.max_slippage_abs)
        worst_price = max(best_ask, worst_price)
        worst_price = _round_up_to_tick(worst_price, tick_size)
        worst_price = min(self.cfg.max_price, worst_price)

        decision: Dict[str, Any] = {
            "decision": "buy",
            "dry_run": self.cfg.dry_run,
            "match_key": match_key,
            "token_id": token_id,
            "condition_id": condition_id or None,
            "source": source,
            "source_signal_price": source_price,
            "best_ask": round(best_ask, 9),
            "order_usdc": round(requested_usdc, 6),
            "requested_usdc": round(requested_usdc, 6),
            "source_usdc": round(source_usdc, 6),
            "copy_scale": self.cfg.copy_scale,
            "sizing_mode": sizing_mode,
            "worst_price": round(worst_price, 9),
            "tick_size": round(tick_size, 6),
        }

        if self.cfg.dry_run:
            decision["status"] = "dry_run_ok"
            return decision

        if self.client is None:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = "trading_client_unavailable"
            return decision

        if _MarketOrderArgs is None:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = f"py_clob_client_types_unavailable: {_CLOB_IMPORT_ERROR}"
            return decision

        try:
            order_args = _MarketOrderArgs(
                token_id=token_id,
                amount=round(requested_usdc, 6),
                side="BUY",
                price=round(worst_price, 9),
                order_type=self.cfg.order_type,  # type: ignore[arg-type]
            )
            signed_order = self.client.create_market_order(order_args)
            response = self.client.post_order(signed_order, orderType=self.cfg.order_type)

            decision["status"] = "submitted"
            decision["response"] = response
            self.submitted += 1
            self._spent_market[market_key] = spent_market + requested_usdc
            self._spent_token[token_id] = spent_token + requested_usdc
            return decision
        except Exception as exc:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = str(exc)
            return decision

    def snapshot(self) -> Dict[str, Any]:
        spent_market_total = sum(self._spent_market.values())
        spent_token_total = sum(self._spent_token.values())
        return {
            "accepted": self.accepted,
            "submitted": self.submitted,
            "skipped": self.skipped,
            "errors": self.errors,
            "api_creds_source": self.api_creds_source,
            "seen_match_keys": len(self._seen_match_keys),
            "spent_market_total_usdc": round(spent_market_total, 6),
            "spent_token_total_usdc": round(spent_token_total, 6),
            "meta_prefetch_ok": self.meta_prefetch_ok,
            "meta_prefetch_errors": self.meta_prefetch_errors,
        }
