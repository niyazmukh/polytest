from __future__ import annotations

import math
import time
from collections import deque
from typing import Any, Dict, Iterable, List, Optional

from .buy_config import BuyConfig
from .price_feed import MarketPriceFeed, NullMarketPriceFeed
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


_FAK_NO_MATCH_ERROR_PATTERNS: tuple[str, ...] = (
    "no orders found to match with fak order",
    "there are no matching orders",
    "no matching orders",
)


def _is_fak_no_match_error(error_text: str) -> bool:
    text = error_text.strip().lower()
    if not text:
        return False
    return any(pattern in text for pattern in _FAK_NO_MATCH_ERROR_PATTERNS)


class BuyExecutor:
    def __init__(self, cfg: BuyConfig, price_feed: MarketPriceFeed | NullMarketPriceFeed) -> None:
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
        self._seen_burst_keys: Dict[str, float] = {}
        self._seen_burst_order: deque[tuple[str, float]] = deque()
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
        if self.cfg.skip_book_enabled:
            return
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
        if self.cfg.skip_book_enabled:
            return
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

    def _prune_seen_burst_keys(self, now_mono: float) -> None:
        ttl = max(1.0, float(self.cfg.burst_dedupe_ttl_seconds))
        while self._seen_burst_order:
            key, stamped_at = self._seen_burst_order[0]
            if now_mono - stamped_at <= ttl:
                break
            self._seen_burst_order.popleft()
            if self._seen_burst_keys.get(key) == stamped_at:
                del self._seen_burst_keys[key]

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

    def _find_matching_live_market(
        self,
        signal: Dict[str, Any],
        live_markets: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not live_markets:
            return None
        signal_condition = str(signal.get("condition_id") or "").strip().lower()
        signal_slug = str(signal.get("slug") or "").strip().lower()
        signal_token = str(signal.get("token_id") or "").strip()

        for market in live_markets:
            condition = str(market.get("condition_id") or "").strip().lower()
            slug = str(market.get("slug") or "").strip().lower()
            if signal_condition and condition and signal_condition == condition:
                return market
            if signal_slug and slug and signal_slug == slug:
                return market
            tokens = market.get("tokens")
            if signal_token and isinstance(tokens, list):
                for token in tokens:
                    if not isinstance(token, dict):
                        continue
                    if str(token.get("token_id") or "").strip() == signal_token:
                        return market
        return None

    def _is_market_open(self, market: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        now = time.time()
        details: Dict[str, Any] = {}

        active = market.get("active")
        if isinstance(active, bool):
            details["market_active"] = active
            if not active:
                return False, details

        closed = market.get("closed")
        if isinstance(closed, bool):
            details["market_closed"] = closed
            if closed:
                return False, details

        end_ts = _as_float(market.get("end_ts"))
        if end_ts is not None and end_ts > 0:
            details["market_end_ts"] = round(float(end_ts), 3)
            seconds_to_end = float(end_ts) - now
            details["seconds_to_end"] = round(seconds_to_end, 3)
            guard = max(0.0, float(self.cfg.market_close_guard_seconds))
            if now + guard >= float(end_ts):
                details["market_close_guard_seconds"] = round(guard, 3)
                return False, details

        return True, details

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

    def _resolve_quote(self, token_id: str, source_price: Optional[float]) -> tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        started = time.perf_counter()
        meta: Dict[str, Any] = {}
        self.price_feed.track_token(token_id)
        quote = self.price_feed.wait_for_quote(
            token_id,
            timeout_seconds=self.cfg.quote_wait_timeout_seconds,
            max_age_seconds=self.cfg.quote_stale_after_seconds,
        )
        if quote is not None:
            meta["quote_source"] = str(quote.get("source") or "wait_for_quote")
            meta["quote_resolve_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
            return quote, meta

        quote = self.price_feed.get_quote(token_id, max_age_seconds=self.cfg.quote_fallback_stale_seconds)
        if quote is not None:
            meta["quote_source"] = str(quote.get("source") or "cached_quote")
            meta["quote_resolve_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
            return quote, meta

        # In dry-run mode, allow source price fallback so replay tests still produce decisions.
        if self.cfg.dry_run and source_price is not None:
            quote = {
                "token_id": token_id,
                "best_bid": source_price,
                "best_ask": source_price,
                "tick_size": 0.01,
                "updated_at_unix": time.time(),
                "source": "source_price_fallback",
            }
            meta["quote_source"] = "source_price_fallback"
            meta["quote_resolve_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
            return quote, meta

        quote = self.price_feed.fetch_book_once(token_id)
        meta["quote_resolve_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
        if quote is not None:
            meta["quote_source"] = str(quote.get("source") or "books_poll")
            return quote, meta

        meta["quote_source"] = "none"
        fetch_error = self.price_feed.get_last_book_fetch_error(token_id)
        if fetch_error:
            meta["book_fetch_error"] = fetch_error
        feed_diag = self.price_feed.snapshot()
        meta["feed_ws_connected"] = bool(feed_diag.get("ws_connected"))
        meta["feed_tracked_tokens"] = int(feed_diag.get("tracked_tokens", 0) or 0)
        meta["feed_last_poll_token_count"] = int(feed_diag.get("last_poll_token_count", 0) or 0)
        meta["feed_poll_errors"] = int(feed_diag.get("poll_errors", 0) or 0)
        return None, meta

    def process_actionable(
        self,
        payload: Dict[str, Any],
        *,
        live_markets: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        decision_started = time.perf_counter()
        queue_lag_seconds = _as_float(payload.get("_queue_lag_seconds"))
        queue_depth_raw = payload.get("_queue_depth")
        queue_depth = 0
        try:
            queue_depth = int(queue_depth_raw) if queue_depth_raw is not None else 0
        except Exception:
            queue_depth = 0
        if queue_depth < 0:
            queue_depth = 0
        event_ts = _as_float(payload.get("event_ts"))

        def _skip_with_latency(reason: str, **extra: Any) -> Dict[str, Any]:
            decision = self._skip(reason, **extra)
            if queue_lag_seconds is not None and queue_lag_seconds >= 0:
                if "queue_lag_seconds" not in decision:
                    decision["queue_lag_seconds"] = round(queue_lag_seconds, 4)
            if event_ts is not None and event_ts > 0:
                if "signal_age_seconds" not in decision:
                    decision["signal_age_seconds"] = round(max(0.0, time.time() - event_ts), 4)
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
            return decision

        match_key = str(payload.get("match_key") or "").strip()
        if not match_key:
            return _skip_with_latency("missing_match_key")
        if match_key in self._seen_match_keys:
            return _skip_with_latency("duplicate_match_key", match_key=match_key)
        self._seen_match_keys.add(match_key)
        self.accepted += 1

        source = str(payload.get("source") or "").strip().lower()
        if source and source not in self.cfg.actionable_sources:
            return _skip_with_latency(
                "source_not_allowed",
                source=source,
                allowed_sources=list(self.cfg.actionable_sources),
            )

        signal = payload.get("signal")
        if not isinstance(signal, dict):
            return _skip_with_latency("malformed_actionable_signal")

        side = str(signal.get("side") or "").strip().upper()
        if side != "BUY" and not self.cfg.allow_sell_signals:
            return _skip_with_latency("non_buy_signal")

        if self.cfg.max_signal_age_seconds > 0 and event_ts is not None and event_ts > 0:
            age = max(0.0, time.time() - event_ts)
            if age > self.cfg.max_signal_age_seconds:
                return _skip_with_latency(
                    "signal_too_old",
                    signal_age_seconds=round(age, 4),
                    max_signal_age_seconds=self.cfg.max_signal_age_seconds,
                )

        source_price = _normalize_prob(signal.get("price"))
        if source_price is not None and source_price < self.cfg.min_source_signal_price:
            return _skip_with_latency(
                "source_price_below_min",
                source_price=source_price,
                min_source_signal_price=self.cfg.min_source_signal_price,
            )

        token_id = str(signal.get("token_id") or "").strip()
        if not token_id:
            return _skip_with_latency("missing_token_id")

        matched_live_market: Optional[Dict[str, Any]] = self._find_matching_live_market(signal, list(live_markets or []))
        if self.cfg.actionable_require_live_market_match:
            if matched_live_market is None:
                return _skip_with_latency("live_market_mismatch")
        if self.cfg.require_market_open and matched_live_market is not None:
            market_open, market_meta = self._is_market_open(matched_live_market)
            if not market_open:
                return _skip_with_latency("live_market_closing_or_closed", **market_meta)

        requested_usdc, source_usdc, sizing_mode = self._requested_usdc(signal)
        if requested_usdc < self.cfg.min_order_usdc:
            return _skip_with_latency(
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
            return _skip_with_latency(
                "market_budget_exceeded",
                market_key=market_key,
                spent_market=round(spent_market, 6),
                requested_usdc=round(requested_usdc, 6),
                max_usdc_per_market=self.cfg.max_usdc_per_market,
            )
        if self.cfg.max_usdc_per_token > 0 and spent_token + requested_usdc > self.cfg.max_usdc_per_token + 1e-12:
            return _skip_with_latency(
                "token_budget_exceeded",
                token_id=token_id,
                spent_token=round(spent_token, 6),
                requested_usdc=round(requested_usdc, 6),
                max_usdc_per_token=self.cfg.max_usdc_per_token,
            )

        if self.cfg.burst_dedupe_enabled and event_ts is not None and event_ts > 0:
            tracked_user = str(payload.get("tracked_user") or "").strip().lower()
            if tracked_user:
                burst_key = f"{tracked_user}|{token_id}|{side}|{int(event_ts)}"
                now_mono = time.monotonic()
                self._prune_seen_burst_keys(now_mono)
                if burst_key in self._seen_burst_keys:
                    return _skip_with_latency("duplicate_burst_key", burst_key=burst_key)
                self._seen_burst_keys[burst_key] = now_mono
                self._seen_burst_order.append((burst_key, now_mono))

        pricing_reference = "source_price"
        quote: Optional[Dict[str, Any]] = None
        best_ask: Optional[float] = None
        quote_meta: Dict[str, Any] = {}
        if self.cfg.skip_book_enabled:
            # Fast path: use source trade price as reference and set worst-price cap.
            if source_price is None:
                return _skip_with_latency("source_price_unavailable")
            if source_price < self.cfg.min_price or source_price > self.cfg.max_price:
                return _skip_with_latency(
                    "source_price_out_of_range",
                    source_price=source_price,
                    min_price=self.cfg.min_price,
                    max_price=self.cfg.max_price,
                )
            best_ask = source_price
        else:
            pricing_reference = "best_ask_quote"
            quote, quote_meta = self._resolve_quote(token_id, source_price)
            if quote is None:
                return _skip_with_latency("quote_unavailable", **quote_meta)
            best_ask = _normalize_prob(quote.get("best_ask"))
            if best_ask is None:
                return _skip_with_latency("quote_without_best_ask", **quote_meta)
            if best_ask < self.cfg.min_price or best_ask > self.cfg.max_price:
                return _skip_with_latency(
                    "quote_price_out_of_range",
                    best_ask=best_ask,
                    min_price=self.cfg.min_price,
                    max_price=self.cfg.max_price,
                    **quote_meta,
                )

        tick_size = self._token_tick_size(token_id, quote)
        worst_price = min(self.cfg.max_price, float(best_ask) + self.cfg.max_slippage_abs)
        worst_price = max(float(best_ask), worst_price)
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
            "pricing_reference": pricing_reference,
            "best_ask_reference": round(float(best_ask), 9),
            "order_usdc": round(requested_usdc, 6),
            "requested_usdc": round(requested_usdc, 6),
            "source_usdc": round(source_usdc, 6),
            "copy_scale": self.cfg.copy_scale,
            "sizing_mode": sizing_mode,
            "worst_price": round(worst_price, 9),
            "tick_size": round(tick_size, 6),
        }
        if quote_meta:
            decision.update(quote_meta)
        if event_ts is not None and event_ts > 0:
            decision["signal_age_seconds"] = round(max(0.0, time.time() - event_ts), 4)
        if queue_lag_seconds is not None and queue_lag_seconds >= 0:
            decision["queue_lag_seconds"] = round(queue_lag_seconds, 4)

        if self.cfg.dry_run:
            decision["status"] = "dry_run_ok"
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
            return decision

        if self.client is None:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = "trading_client_unavailable"
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
            return decision

        if _MarketOrderArgs is None:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = f"py_clob_client_types_unavailable: {_CLOB_IMPORT_ERROR}"
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
            return decision

        try:
            submit_started = time.perf_counter()
            retry_enabled = (
                self.cfg.order_type == "FAK"
                and self.cfg.fak_retry_enabled
                and self.cfg.fak_retry_max_attempts > 1
            )
            max_attempts = self.cfg.fak_retry_max_attempts if retry_enabled else 1
            retry_delay_seconds = max(0.0, float(self.cfg.fak_retry_delay_seconds))
            retry_window_seconds = max(0.0, float(self.cfg.fak_retry_max_window_seconds))
            retry_delay_suppressed_for_queue = False
            if queue_depth > 0 and retry_delay_seconds > 0:
                retry_delay_seconds = 0.0
                retry_delay_suppressed_for_queue = True

            submit_attempts = 0
            retryable_no_match_count = 0
            attempt_latencies_ms: List[float] = []
            attempt_errors: List[str] = []
            final_exc: Optional[Exception] = None

            while submit_attempts < max_attempts:
                if submit_attempts > 0 and retry_window_seconds > 0:
                    elapsed_before_attempt = time.perf_counter() - submit_started
                    if elapsed_before_attempt >= retry_window_seconds:
                        break
                submit_attempts += 1
                attempt_started = time.perf_counter()
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
                    attempt_latencies_ms.append(round((time.perf_counter() - attempt_started) * 1000.0, 3))

                    decision["status"] = "submitted"
                    decision["response"] = response
                    decision["submit_attempts"] = submit_attempts
                    if retryable_no_match_count > 0:
                        decision["retryable_no_match_count"] = retryable_no_match_count
                    decision["fak_retry_effective_delay_seconds"] = round(retry_delay_seconds, 6)
                    if retry_delay_suppressed_for_queue:
                        decision["fak_retry_delay_suppressed_for_queue"] = True
                    decision["submit_attempt_latencies_ms"] = attempt_latencies_ms
                    decision["submit_latency_ms"] = round((time.perf_counter() - submit_started) * 1000.0, 3)
                    decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
                    self.submitted += 1
                    self._spent_market[market_key] = spent_market + requested_usdc
                    self._spent_token[token_id] = spent_token + requested_usdc
                    return decision
                except Exception as exc:
                    final_exc = exc
                    error_text = str(exc)
                    attempt_errors.append(error_text)
                    attempt_latencies_ms.append(round((time.perf_counter() - attempt_started) * 1000.0, 3))

                    is_no_match = _is_fak_no_match_error(error_text)
                    if is_no_match:
                        retryable_no_match_count += 1

                    should_retry = retry_enabled and is_no_match and submit_attempts < max_attempts
                    if should_retry and retry_window_seconds > 0:
                        elapsed = time.perf_counter() - submit_started
                        should_retry = elapsed < retry_window_seconds

                    if not should_retry:
                        break

                    if retry_delay_seconds > 0:
                        sleep_seconds = retry_delay_seconds
                        if retry_window_seconds > 0:
                            elapsed = time.perf_counter() - submit_started
                            remaining = max(0.0, retry_window_seconds - elapsed)
                            if remaining <= 0:
                                break
                            sleep_seconds = min(sleep_seconds, remaining)
                        if sleep_seconds > 0:
                            time.sleep(sleep_seconds)

            all_no_match = retryable_no_match_count > 0 and retryable_no_match_count >= submit_attempts
            if all_no_match:
                self.skipped += 1
                decision["status"] = "no_fill"
                decision["reason"] = "fak_no_match_exhausted"
            else:
                self.errors += 1
                decision["status"] = "error"
            decision["error"] = str(final_exc) if final_exc is not None else "order_submit_failed"
            decision["submit_attempts"] = submit_attempts
            decision["submit_attempt_latencies_ms"] = attempt_latencies_ms
            if retryable_no_match_count > 0:
                decision["retryable_no_match_count"] = retryable_no_match_count
                if retryable_no_match_count >= submit_attempts:
                    decision["error_kind"] = "fak_no_match_exhausted"
            decision["fak_retry_effective_delay_seconds"] = round(retry_delay_seconds, 6)
            if retry_delay_suppressed_for_queue:
                decision["fak_retry_delay_suppressed_for_queue"] = True
            if attempt_errors:
                decision["last_submit_error"] = attempt_errors[-1]
            decision["submit_latency_ms"] = round((time.perf_counter() - submit_started) * 1000.0, 3)
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
            return decision
        except Exception as exc:
            self.errors += 1
            decision["status"] = "error"
            decision["error"] = str(exc)
            decision["decision_latency_ms"] = round((time.perf_counter() - decision_started) * 1000.0, 3)
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
            "seen_burst_keys": len(self._seen_burst_keys),
            "spent_market_total_usdc": round(spent_market_total, 6),
            "spent_token_total_usdc": round(spent_token_total, 6),
            "meta_prefetch_ok": self.meta_prefetch_ok,
            "meta_prefetch_errors": self.meta_prefetch_errors,
        }
