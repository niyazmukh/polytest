from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Iterable, Optional

from .buy_config import BuyConfig
from .utils import as_float as _as_float

try:
    import websocket as _websocket  # type: ignore
    _WS_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - dependency/environment failure path
    _websocket = None  # type: ignore[assignment]
    _WS_IMPORT_ERROR = exc

try:
    from py_clob_client.client import ClobClient as _ClobClient
    from py_clob_client.clob_types import BookParams as _BookParams
    _CLOB_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - dependency/environment failure path
    _ClobClient = None  # type: ignore[assignment]
    _BookParams = None  # type: ignore[assignment]
    _CLOB_IMPORT_ERROR = exc


def _extract_px_from_level(level: Any) -> Optional[float]:
    if isinstance(level, dict):
        return _as_float(level.get("price") or level.get("px") or level.get("p"))
    if isinstance(level, (list, tuple)) and level:
        return _as_float(level[0])
    return None


class MarketPriceFeed:
    def __init__(self, cfg: BuyConfig, stop_event: threading.Event) -> None:
        self.cfg = cfg
        self.stop_event = stop_event
        self.client = _ClobClient(cfg.clob_host, chain_id=cfg.chain_id) if _ClobClient is not None else None

        self._lock = threading.RLock()
        self._quote_cond = threading.Condition(self._lock)
        self._tracked_tokens: set[str] = set()
        self._tracked_touch: Dict[str, float] = {}
        self._quotes: Dict[str, Dict[str, Any]] = {}
        self._book_fetch_errors: Dict[str, str] = {}

        self._poll_thread: Optional[threading.Thread] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws: Optional[Any] = None
        self._ws_send_lock = threading.Lock()

        self.ws_connected = False
        self.ws_connects = 0
        self.ws_reconnects = 0
        self.ws_errors = 0
        self.ws_messages = 0
        self.ws_last_connect_at: Optional[float] = None
        self.ws_last_message_at: Optional[float] = None

        self.polls = 0
        self.poll_errors = 0
        self.updated_quotes = 0
        self.last_poll_at: Optional[float] = None
        self.last_poll_rtt_seconds: Optional[float] = None
        self.last_poll_token_count = 0

        self._started = False
        self.tracked_pruned_ttl = 0
        self.tracked_pruned_capacity = 0
        self.last_ws_subscribe_assets = 0

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True

        self._poll_thread = threading.Thread(target=self._poll_loop, name="v2-buy-books", daemon=True)
        self._poll_thread.start()

        if self.cfg.use_ws_market_feed and _websocket is not None:
            self._ws_thread = threading.Thread(target=self._ws_loop, name="v2-buy-ws", daemon=True)
            self._ws_thread.start()
        elif self.cfg.use_ws_market_feed:
            self.ws_errors += 1

    def close(self) -> None:
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=2.0)
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._ws_thread is not None:
            self._ws_thread.join(timeout=2.0)

    # Keep compatibility with older orchestration that expected a join() method.
    def join(self, timeout: Optional[float] = None) -> None:
        self.stop_event.set()
        self.close()

    def _prune_tracked_locked(self, now: float) -> bool:
        changed = False

        ttl = float(self.cfg.feed_track_ttl_seconds)
        if ttl > 0:
            cutoff = now - ttl
            stale = [token for token, touched in self._tracked_touch.items() if touched < cutoff]
            if stale:
                for token in stale:
                    self._tracked_touch.pop(token, None)
                    self._tracked_tokens.discard(token)
                    self._quotes.pop(token, None)
                    self._book_fetch_errors.pop(token, None)
                self.tracked_pruned_ttl += len(stale)
                changed = True

        cap = int(self.cfg.feed_max_tracked_tokens)
        if cap > 0 and len(self._tracked_tokens) > cap:
            ranked = sorted(
                self._tracked_tokens,
                key=lambda token: self._tracked_touch.get(token, 0.0),
                reverse=True,
            )
            keep = set(ranked[:cap])
            removed = [token for token in self._tracked_tokens if token not in keep]
            if removed:
                for token in removed:
                    self._tracked_touch.pop(token, None)
                    self._tracked_tokens.discard(token)
                    self._quotes.pop(token, None)
                    self._book_fetch_errors.pop(token, None)
                self.tracked_pruned_capacity += len(removed)
                changed = True

        return changed

    def _active_tokens_locked(self, limit: int) -> list[str]:
        ranked = sorted(
            self._tracked_tokens,
            key=lambda token: self._tracked_touch.get(token, 0.0),
            reverse=True,
        )
        if limit > 0:
            ranked = ranked[:limit]
        return ranked

    def track_token(self, token_id: str) -> None:
        token = str(token_id).strip()
        if not token or token == "0":
            return
        changed = False
        with self._lock:
            now = time.time()
            already = token in self._tracked_tokens
            self._tracked_tokens.add(token)
            self._tracked_touch[token] = now
            changed = self._prune_tracked_locked(now)
        if not already or changed:
            self._send_ws_subscribe()

    def track_tokens(self, token_ids: Iterable[str]) -> None:
        changed = False
        with self._lock:
            now = time.time()
            for token_id in token_ids:
                token = str(token_id).strip()
                if not token or token == "0":
                    continue
                self._tracked_touch[token] = now
                if token not in self._tracked_tokens:
                    self._tracked_tokens.add(token)
                    changed = True
            if self._prune_tracked_locked(now):
                changed = True
        if changed:
            self._send_ws_subscribe()

    def get_quote(self, token_id: str, *, max_age_seconds: Optional[float] = None) -> Optional[Dict[str, Any]]:
        token = str(token_id).strip()
        if not token:
            return None
        with self._lock:
            row = self._quotes.get(token)
            if row is None:
                return None
            out = dict(row)
        if max_age_seconds is not None:
            age = time.time() - float(out.get("updated_at_unix") or 0.0)
            if age > float(max_age_seconds):
                return None
        return out

    def wait_for_quote(self, token_id: str, *, timeout_seconds: float, max_age_seconds: float) -> Optional[Dict[str, Any]]:
        deadline = time.time() + max(0.01, float(timeout_seconds))
        while not self.stop_event.is_set():
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            quote = self.get_quote(token_id, max_age_seconds=max_age_seconds)
            if quote is not None:
                return quote
            with self._quote_cond:
                self._quote_cond.wait(timeout=min(remaining, 0.25))
        return None

    def fetch_book_once(self, token_id: str) -> Optional[Dict[str, Any]]:
        token = str(token_id).strip()
        if not token:
            return None
        if self.client is None:
            with self._lock:
                self._book_fetch_errors[token] = "book_client_unavailable"
            return None
        try:
            book = self.client.get_order_book(token)
        except Exception as exc:
            with self._lock:
                self._book_fetch_errors[token] = f"book_fetch_exception:{type(exc).__name__}:{str(exc)[:220]}"
            return None
        self._ingest_book(token, book, source="books_poll")
        quote = self.get_quote(token)
        with self._lock:
            if quote is None:
                self._book_fetch_errors[token] = "book_without_best_bid_or_ask"
            else:
                self._book_fetch_errors.pop(token, None)
        return quote

    def get_last_book_fetch_error(self, token_id: str) -> Optional[str]:
        token = str(token_id).strip()
        if not token:
            return None
        with self._lock:
            return self._book_fetch_errors.get(token)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            tracked_count = len(self._tracked_tokens)
            quote_count = len(self._quotes)
        return {
            "ws_connected": bool(self.ws_connected),
            "tracked_tokens": tracked_count,
            "quotes": quote_count,
            "last_poll_token_count": self.last_poll_token_count,
            "last_ws_subscribe_assets": self.last_ws_subscribe_assets,
            "tracked_pruned_ttl": self.tracked_pruned_ttl,
            "tracked_pruned_capacity": self.tracked_pruned_capacity,
            "ws_connects": self.ws_connects,
            "ws_reconnects": self.ws_reconnects,
            "ws_errors": self.ws_errors,
            "ws_messages": self.ws_messages,
            "ws_last_connect_at_unix": None if self.ws_last_connect_at is None else round(self.ws_last_connect_at, 6),
            "ws_last_message_at_unix": None if self.ws_last_message_at is None else round(self.ws_last_message_at, 6),
            "polls": self.polls,
            "poll_errors": self.poll_errors,
            "updated_quotes": self.updated_quotes,
            "last_poll_at_unix": None if self.last_poll_at is None else round(self.last_poll_at, 6),
            "last_poll_rtt_seconds": None
            if self.last_poll_rtt_seconds is None
            else round(self.last_poll_rtt_seconds, 4),
        }

    def _set_quote(
        self,
        token_id: str,
        *,
        best_bid: Optional[float],
        best_ask: Optional[float],
        tick_size: Optional[float],
        source: str,
    ) -> None:
        if best_bid is None and best_ask is None:
            return
        now = time.time()
        with self._quote_cond:
            current = self._quotes.get(token_id, {"token_id": token_id})
            if best_bid is not None:
                current["best_bid"] = max(0.0, float(best_bid))
            if best_ask is not None:
                current["best_ask"] = max(0.0, float(best_ask))
            if tick_size is not None and tick_size > 0:
                current["tick_size"] = float(tick_size)
            current["updated_at_unix"] = now
            current["source"] = source
            self._quotes[token_id] = current
            self._quote_cond.notify_all()
        self.updated_quotes += 1

    def _ingest_book(self, token_id: str, book: Any, *, source: str) -> None:
        tick_size = None
        bids = None
        asks = None

        if isinstance(book, dict):
            tick_size = _as_float(book.get("tick_size") or book.get("tickSize"))
            bids = book.get("bids")
            asks = book.get("asks")
        else:
            # dataclass-like from py_clob_client.
            tick_size = _as_float(getattr(book, "tick_size", None))
            bids = getattr(book, "bids", None)
            asks = getattr(book, "asks", None)

        best_bid = None
        best_ask = None
        if isinstance(bids, list) and bids:
            best_bid = _extract_px_from_level(bids[0])
        if isinstance(asks, list) and asks:
            best_ask = _extract_px_from_level(asks[0])

        self._set_quote(
            token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=tick_size,
            source=source,
        )

    def _poll_loop(self) -> None:
        while not self.stop_event.is_set():
            resubscribe = False
            with self._lock:
                now = time.time()
                resubscribe = self._prune_tracked_locked(now)
                tokens = self._active_tokens_locked(int(self.cfg.feed_poll_batch_size))
                self.last_poll_token_count = len(tokens)
            if resubscribe:
                self._send_ws_subscribe()
            if not tokens:
                self.stop_event.wait(0.05)
                continue
            if self.client is None or _BookParams is None:
                self.poll_errors += 1
                self.stop_event.wait(max(0.01, float(self.cfg.book_poll_interval_seconds)))
                continue

            started = time.time()
            self.last_poll_at = started
            try:
                params = [_BookParams(token_id=t) for t in tokens]
                books = self.client.get_order_books(params)
                if isinstance(books, list):
                    for row in books:
                        token_id = None
                        if isinstance(row, dict):
                            token_id = str(
                                row.get("asset_id") or row.get("assetId") or row.get("token_id") or ""
                            ).strip()
                        else:
                            token_id = str(getattr(row, "asset_id", "") or "").strip()
                        if not token_id:
                            continue
                        self._ingest_book(token_id, row, source="books_poll")
                self.polls += 1
            except Exception:
                self.poll_errors += 1
            finally:
                self.last_poll_rtt_seconds = time.time() - started

            self.stop_event.wait(max(0.001, float(self.cfg.book_poll_interval_seconds)))

    def _send_ws_subscribe(self) -> None:
        ws = self._ws
        if ws is None:
            return
        with self._lock:
            now = time.time()
            self._prune_tracked_locked(now)
            limit = int(self.cfg.feed_ws_subscribe_batch_size)
            assets = self._active_tokens_locked(limit)
            self.last_ws_subscribe_assets = len(assets)
        if not assets:
            return
        payload = {"assets_ids": assets, "type": "market"}
        try:
            with self._ws_send_lock:
                ws.send(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        except Exception:
            pass

    def _handle_ws_payload(self, payload: Dict[str, Any]) -> None:
        token_id = str(
            payload.get("asset_id")
            or payload.get("assetId")
            or payload.get("token_id")
            or payload.get("tokenId")
            or ""
        ).strip()
        if not token_id:
            market = payload.get("market")
            if isinstance(market, dict):
                token_id = str(
                    market.get("asset_id")
                    or market.get("assetId")
                    or market.get("token_id")
                    or market.get("tokenId")
                    or ""
                ).strip()
        if not token_id:
            return

        best_bid = _as_float(payload.get("best_bid") or payload.get("bestBid") or payload.get("bid"))
        best_ask = _as_float(payload.get("best_ask") or payload.get("bestAsk") or payload.get("ask"))

        bids = payload.get("bids")
        asks = payload.get("asks")
        if best_bid is None and isinstance(bids, list) and bids:
            best_bid = _extract_px_from_level(bids[0])
        if best_ask is None and isinstance(asks, list) and asks:
            best_ask = _extract_px_from_level(asks[0])

        tick_size = _as_float(payload.get("tick_size") or payload.get("tickSize"))
        self._set_quote(
            token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=tick_size,
            source="ws_market",
        )

    def _ws_loop(self) -> None:
        if _websocket is None:
            self.ws_errors += 1
            return

        first_connect = True
        while not self.stop_event.is_set():
            def on_open(ws_app: Any) -> None:
                self.ws_connected = True
                self.ws_connects += 1
                self.ws_last_connect_at = time.time()
                self._send_ws_subscribe()

            def on_message(_: Any, message: str) -> None:
                self.ws_messages += 1
                self.ws_last_message_at = time.time()
                try:
                    payload = json.loads(message)
                except Exception:
                    return
                if isinstance(payload, list):
                    for row in payload:
                        if isinstance(row, dict):
                            self._handle_ws_payload(row)
                    return
                if isinstance(payload, dict):
                    self._handle_ws_payload(payload)

            def on_error(_: Any, __: Any) -> None:
                self.ws_errors += 1

            def on_close(*_args: Any) -> None:
                self.ws_connected = False

            try:
                self._ws = _websocket.WebSocketApp(
                    self.cfg.ws_market_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._ws.run_forever(
                    ping_interval=max(1, int(self.cfg.ws_heartbeat_seconds)),
                    ping_timeout=max(1, int(self.cfg.ws_heartbeat_seconds // 2)),
                )
            except Exception:
                self.ws_errors += 1
            finally:
                self.ws_connected = False
                self._ws = None

            if self.stop_event.is_set():
                break
            if not first_connect:
                self.ws_reconnects += 1
            first_connect = False
            self.stop_event.wait(max(0.05, float(self.cfg.ws_reconnect_seconds)))


class NullMarketPriceFeed:
    """No-op price feed used when buy path does not require quotes/books."""

    def __init__(self, cfg: BuyConfig, stop_event: threading.Event) -> None:
        self.cfg = cfg
        self.stop_event = stop_event
        self._tracked_tokens: set[str] = set()

    def start(self) -> None:
        return

    def close(self) -> None:
        return

    def join(self, timeout: Optional[float] = None) -> None:
        _ = timeout
        return

    def track_token(self, token_id: str) -> None:
        token = str(token_id).strip()
        if token:
            self._tracked_tokens.add(token)

    def track_tokens(self, token_ids: Iterable[str]) -> None:
        for token_id in token_ids:
            self.track_token(token_id)

    def get_quote(self, token_id: str, *, max_age_seconds: Optional[float] = None) -> Optional[Dict[str, Any]]:
        _ = token_id
        _ = max_age_seconds
        return None

    def wait_for_quote(self, token_id: str, *, timeout_seconds: float, max_age_seconds: float) -> Optional[Dict[str, Any]]:
        _ = token_id
        _ = timeout_seconds
        _ = max_age_seconds
        return None

    def fetch_book_once(self, token_id: str) -> Optional[Dict[str, Any]]:
        _ = token_id
        return None

    def get_last_book_fetch_error(self, token_id: str) -> Optional[str]:
        _ = token_id
        return None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "disabled": True,
            "reason": "skip_book_enabled",
            "tracked_tokens": len(self._tracked_tokens),
            "quotes": 0,
            "ws_connected": False,
            "ws_connects": 0,
            "ws_reconnects": 0,
            "ws_errors": 0,
            "ws_messages": 0,
            "last_poll_token_count": 0,
            "last_ws_subscribe_assets": 0,
            "tracked_pruned_ttl": 0,
            "tracked_pruned_capacity": 0,
            "polls": 0,
            "poll_errors": 0,
            "updated_quotes": 0,
            "ws_last_connect_at_unix": None,
            "ws_last_message_at_unix": None,
            "last_poll_at_unix": None,
            "last_poll_rtt_seconds": None,
        }
