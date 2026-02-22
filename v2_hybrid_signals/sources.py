from __future__ import annotations

import time
import threading
from collections import deque
from queue import Full, Queue
from typing import Any, Deque, Dict, Iterable, List, Optional

from .config import HybridConfig
from .http_json import HttpStatusError, JsonHttpClient
from .market_cache import GammaMarketCache
from .models import SourceSignal
from .rate_gate import RateGate
from .utils import as_float as _parse_amount, parse_unixish


AMOUNT_SCALE = 1_000_000.0

# Collateral can appear as numeric asset id ("0") or as token contract aliases.
COLLATERAL_ASSET_IDS = {
    "0",
    "usdc",
    "usd",
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
}


def _parse_price(raw: Any) -> Optional[float]:
    value = _parse_amount(raw)
    if value is None:
        return None
    # Polymarket prices are always 0..1 (probability).  Clamp rather than
    # guessing if the value is in cents — dividing by 100 would silently
    # corrupt legitimate edge-case values.
    return max(0.0, min(1.0, value))


def _as_unix(raw: Any) -> Optional[int]:
    value = parse_unixish(raw)
    if value is None:
        return None
    return int(value)


def _decode_activity_row(
    *,
    user: str,
    row: Dict[str, Any],
    market_cache: GammaMarketCache,
) -> Dict[str, Any]:
    side = str(row.get("side") or "").strip().upper()
    token_id = (
        str(
            row.get("asset")
            or row.get("assetId")
            or row.get("token_id")
            or row.get("clobTokenId")
            or ""
        )
        .strip()
    )
    outcome = str(row.get("outcome") or "").strip() or None
    slug = str(row.get("slug") or row.get("marketSlug") or "").strip().lower() or None
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip().lower() or None

    price = _parse_price(row.get("price"))
    size = _parse_amount(row.get("size"))
    usdc_size = _parse_amount(row.get("usdcSize") if "usdcSize" in row else row.get("usdc_size"))
    if usdc_size is None:
        usdc_size = _parse_amount(row.get("amount"))

    if size is not None and size > 0 and usdc_size is not None and usdc_size >= 0:
        price = usdc_size / size
    elif price is not None and size is not None and size > 0 and usdc_size is None:
        usdc_size = price * size
    elif price is not None and usdc_size is not None and usdc_size >= 0 and (size is None or size <= 0):
        if price > 0:
            size = usdc_size / price

    if token_id:
        market_cache.hint_from_activity(token_id, outcome, slug, condition_id)
        meta = market_cache.lookup_token(token_id, queue_on_miss=True, fetch_on_miss=False)
        if meta is not None:
            if not outcome:
                maybe = str(meta.get("outcome") or "").strip()
                outcome = maybe or None
            if not slug:
                maybe = str(meta.get("slug") or "").strip().lower()
                slug = maybe or None
            if not condition_id:
                maybe = str(meta.get("condition_id") or "").strip().lower()
                condition_id = maybe or None

    return {
        "tracked_user": user,
        "side": side,
        "token_id": token_id or None,
        "outcome": outcome,
        "slug": slug,
        "condition_id": condition_id,
        "price": None if price is None else round(float(price), 9),
        "size": None if size is None else round(float(size), 6),
        "usdc_size": None if usdc_size is None else round(float(usdc_size), 6),
        "decode_actionable": side in {"BUY", "SELL"} and bool(token_id),
    }


class DataApiActivitySource(threading.Thread):
    def __init__(
        self,
        cfg: HybridConfig,
        tracked_user: str,
        out_queue: "Queue[SourceSignal]",
        stop_event: threading.Event,
        market_cache: GammaMarketCache,
        rate: Optional[RateGate] = None,
    ) -> None:
        self.user = str(tracked_user).strip().lower()
        user_tag = (self.user[-6:] if len(self.user) >= 6 else self.user).replace("0x", "")
        super().__init__(name=f"v2-activity-{user_tag}", daemon=True)
        self.cfg = cfg
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.market_cache = market_cache
        self.rate = rate if rate is not None else RateGate(cfg.activity_target_rps)
        self.http = JsonHttpClient(
            host="data-api.polymarket.com",
            timeout_seconds=cfg.activity_http_timeout_seconds,
            user_agent="poly-v2-hybrid-activity/1.0",
        )

        self.start_ts = int(time.time()) - int(cfg.activity_lookback_seconds)
        self.seen_row_keys: Dict[str, float] = {}  # key → monotonic ts
        self.seen_order: Deque[tuple[str, float]] = deque()
        self._dedupe_ttl = max(3600.0, 2.0 * float(cfg.activity_lookback_seconds))
        self.max_seen_keys = 500_000

        self.polls = 0
        self.errors = 0
        self.signals = 0
        self.dropped = 0
        self.last_poll_rtt: Optional[float] = None
        self.idle_polls = 0
        self.idle_backoff_seconds = 0.0

    def _remember(self, row_key: str) -> bool:
        if row_key in self.seen_row_keys:
            return False
        now = time.monotonic()
        cutoff = now - self._dedupe_ttl
        while self.seen_order and self.seen_order[0][1] < cutoff:
            old_key, _ = self.seen_order.popleft()
            self.seen_row_keys.pop(old_key, None)
        while len(self.seen_order) > self.max_seen_keys:
            old_key, _ = self.seen_order.popleft()
            self.seen_row_keys.pop(old_key, None)
        self.seen_row_keys[row_key] = now
        self.seen_order.append((row_key, now))
        return True

    def _emit(self, signal: SourceSignal) -> None:
        try:
            self.out_queue.put_nowait(signal)
            self.signals += 1
        except Full:
            self.dropped += 1

    def _decode_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return _decode_activity_row(
            user=self.user,
            row=row,
            market_cache=self.market_cache,
        )

    def _poll_once(self) -> int:
        start_signals = self.signals
        started = time.time()

        latest_event_ts = self.start_ts
        aggregated: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        seen_at = started

        for page_idx in range(int(self.cfg.activity_max_pages)):
            offset = page_idx * int(self.cfg.activity_page_size)
            params = {
                "user": self.user,
                "type": "TRADE",
                "limit": int(self.cfg.activity_page_size),
                "offset": offset,
            }
            path = (
                f"/activity?user={params['user']}&type={params['type']}&limit={params['limit']}"
                f"&offset={params['offset']}"
            )

            self.rate.wait_turn()
            body = self.http.get_json(path)
            self.rate.on_success()
            if not isinstance(body, list):
                raise RuntimeError("activity_payload_not_list")
            rows = [row for row in body if isinstance(row, dict)]
            if not rows:
                break

            for row in rows:
                tx_hash = str(row.get("transactionHash") or row.get("txHash") or row.get("hash") or "").strip().lower()
                if not tx_hash:
                    continue
                event_ts = _as_unix(row.get("timestamp"))
                if event_ts is None:
                    continue
                if event_ts < self.start_ts:
                    continue
                latest_event_ts = max(latest_event_ts, event_ts)

                row_id = (
                    str(row.get("id") or "").strip()
                    or "|".join(
                        [
                            tx_hash,
                            str(event_ts),
                            str(row.get("side") or ""),
                            str(row.get("asset") or row.get("assetId") or ""),
                            str(row.get("size") or ""),
                            str(row.get("usdcSize") if "usdcSize" in row else row.get("usdc_size") or ""),
                        ]
                    )
                )
                if not self._remember(row_id):
                    continue

                decoded = self._decode_row(row)
                side = str(decoded.get("side") or "").strip().upper()
                token_id = str(decoded.get("token_id") or "").strip()
                if side not in {"BUY", "SELL"} or not token_id:
                    continue

                agg_key = (tx_hash, side, token_id)
                current = aggregated.get(agg_key)
                if current is None:
                    current = {
                        "tx_hash": tx_hash,
                        "event_ts": event_ts,
                        "side": side,
                        "token_id": token_id,
                        "outcome": decoded.get("outcome"),
                        "slug": decoded.get("slug"),
                        "condition_id": decoded.get("condition_id"),
                        "size": 0.0,
                        "usdc_size": 0.0,
                        "event_count": 0,
                    }
                    aggregated[agg_key] = current

                current["event_ts"] = min(int(current["event_ts"]), event_ts)
                if not current.get("outcome") and decoded.get("outcome"):
                    current["outcome"] = decoded.get("outcome")
                if not current.get("slug") and decoded.get("slug"):
                    current["slug"] = decoded.get("slug")
                if not current.get("condition_id") and decoded.get("condition_id"):
                    current["condition_id"] = decoded.get("condition_id")

                size = _parse_amount(decoded.get("size"))
                usdc_size = _parse_amount(decoded.get("usdc_size"))
                if size is not None and size > 0:
                    current["size"] = float(current["size"]) + size
                if usdc_size is not None and usdc_size >= 0:
                    current["usdc_size"] = float(current["usdc_size"]) + usdc_size
                current["event_count"] = int(current["event_count"]) + 1

            if len(rows) < int(self.cfg.activity_page_size):
                break

        for (tx_hash, side, token_id), agg in aggregated.items():
            total_size = float(agg.get("size") or 0.0)
            total_usdc = float(agg.get("usdc_size") or 0.0)
            price: Optional[float] = None
            if total_size > 0 and total_usdc >= 0:
                price = total_usdc / total_size
            payload = {
                "side": side,
                "token_id": token_id,
                "outcome": agg.get("outcome"),
                "slug": agg.get("slug"),
                "condition_id": agg.get("condition_id"),
                "price": None if price is None else round(price, 9),
                "size": None if total_size <= 0 else round(total_size, 6),
                "usdc_size": None if total_usdc < 0 else round(total_usdc, 6),
                "decode_actionable": True,
                "aggregated_fill_count": int(agg.get("event_count") or 0),
            }
            signal = SourceSignal(
                tracked_user=self.user,
                source="activity",
                tx_hash=tx_hash,
                event_ts=int(agg.get("event_ts") or self.start_ts),
                seen_at=seen_at,
                payload=payload,
            )
            self._emit(signal)

        if latest_event_ts > self.start_ts:
            # Keep a small overlap window to avoid missing borderline-indexed events.
            self.start_ts = max(self.start_ts, latest_event_ts - 2)

        self.last_poll_rtt = time.time() - started
        self.polls += 1
        return max(0, self.signals - start_signals)

    def _on_poll_result(self, emitted_count: int) -> None:
        if emitted_count > 0:
            self.idle_polls = 0
            self.idle_backoff_seconds = 0.0
            return
        self.idle_polls += 1
        if self.idle_polls < 6:
            self.idle_backoff_seconds = 0.0
            return
        self.idle_backoff_seconds = min(0.3, 0.01 * float(self.idle_polls - 5))

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                emitted_count = self._poll_once()
                self._on_poll_result(emitted_count)
                if not self.cfg.activity_saturate_rps and self.idle_backoff_seconds > 0:
                    self.stop_event.wait(self.idle_backoff_seconds)
            except HttpStatusError as exc:
                self.errors += 1
                if exc.status == 429:
                    self.rate.on_throttle()
                else:
                    self.rate.on_error()
            except Exception:
                self.errors += 1
                self.rate.on_error()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "source": "activity",
            "tracked_user": self.user,
            "batch_mode": False,
            "saturate_rps": bool(self.cfg.activity_saturate_rps),
            "target_rps": round(self.cfg.activity_target_rps, 3),
            "polls": self.polls,
            "errors": self.errors,
            "signals": self.signals,
            "dropped": self.dropped,
            "rate_instance_id": id(self.rate),
            "rate": self.rate.snapshot(),
            "last_poll_rtt_seconds": None if self.last_poll_rtt is None else round(self.last_poll_rtt, 4),
            "dedupe_keys": len(self.seen_row_keys),
        }

    def close(self) -> None:
        self.http.close()


class DataApiActivityBatchSource(threading.Thread):
    """Shared Activity API scheduler with active-wallet prioritization."""

    def __init__(
        self,
        cfg: HybridConfig,
        tracked_users: Iterable[str],
        out_queue: "Queue[SourceSignal]",
        stop_event: threading.Event,
        market_cache: GammaMarketCache,
        rate: Optional[RateGate] = None,
    ) -> None:
        users: list[str] = []
        seen_users: set[str] = set()
        for raw in tracked_users:
            user = str(raw).strip().lower()
            if not user or user in seen_users:
                continue
            seen_users.add(user)
            users.append(user)
        if not users:
            raise ValueError("activity_batch_requires_users")

        self.users = tuple(users)
        super().__init__(name="v2-activity-batch", daemon=True)
        self.cfg = cfg
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.market_cache = market_cache
        self.rate = rate if rate is not None else RateGate(cfg.activity_target_rps)
        self.http = JsonHttpClient(
            host="data-api.polymarket.com",
            timeout_seconds=cfg.activity_http_timeout_seconds,
            user_agent="poly-v2-hybrid-activity/1.0",
        )

        base_start = int(time.time()) - int(cfg.activity_lookback_seconds)
        self.start_ts_by_user: Dict[str, int] = {user: base_start for user in self.users}
        self.last_signal_at_by_user: Dict[str, float] = {user: 0.0 for user in self.users}
        self.last_polled_at_by_user: Dict[str, float] = {user: 0.0 for user in self.users}
        self.next_due_at_by_user: Dict[str, float] = {user: 0.0 for user in self.users}
        self.idle_polls_by_user: Dict[str, int] = {user: 0 for user in self.users}

        self.polls_by_user: Dict[str, int] = {user: 0 for user in self.users}
        self.errors_by_user: Dict[str, int] = {user: 0 for user in self.users}
        self.signals_by_user: Dict[str, int] = {user: 0 for user in self.users}
        self.dropped_by_user: Dict[str, int] = {user: 0 for user in self.users}
        self.last_poll_rtt_by_user: Dict[str, Optional[float]] = {user: None for user in self.users}

        self.active_window_seconds = max(60.0, float(cfg.activity_priority_active_window_seconds))
        self.max_probe_seconds = max(0.05, float(cfg.activity_priority_max_probe_seconds))

        self.seen_row_keys: Dict[str, float] = {}  # key → monotonic ts
        self.seen_order: Deque[tuple[str, float]] = deque()
        self._dedupe_ttl = max(3600.0, 2.0 * float(cfg.activity_lookback_seconds))
        self.max_seen_keys = max(500_000, 100_000 * len(self.users))

        self.polls = 0
        self.errors = 0
        self.signals = 0
        self.dropped = 0
        self.last_poll_rtt: Optional[float] = None

    def _remember(self, user: str, row_key: str) -> bool:
        dedupe_key = f"{user}|{row_key}"
        if dedupe_key in self.seen_row_keys:
            return False
        now = time.monotonic()
        cutoff = now - self._dedupe_ttl
        while self.seen_order and self.seen_order[0][1] < cutoff:
            old_key, _ = self.seen_order.popleft()
            self.seen_row_keys.pop(old_key, None)
        while len(self.seen_order) > self.max_seen_keys:
            old_key, _ = self.seen_order.popleft()
            self.seen_row_keys.pop(old_key, None)
        self.seen_row_keys[dedupe_key] = now
        self.seen_order.append((dedupe_key, now))
        return True

    def _emit(self, user: str, signal: SourceSignal) -> None:
        try:
            self.out_queue.put_nowait(signal)
            self.signals += 1
            self.signals_by_user[user] = int(self.signals_by_user.get(user, 0)) + 1
        except Full:
            self.dropped += 1
            self.dropped_by_user[user] = int(self.dropped_by_user.get(user, 0)) + 1

    def _is_recently_active(self, user: str, now: float) -> bool:
        last_signal = float(self.last_signal_at_by_user.get(user, 0.0) or 0.0)
        if last_signal <= 0.0:
            return False
        return (now - last_signal) <= self.active_window_seconds

    def _select_user(self, now: float) -> Optional[str]:
        forced: list[str] = []
        for user in self.users:
            last_polled = float(self.last_polled_at_by_user.get(user, 0.0) or 0.0)
            if last_polled <= 0.0 or (now - last_polled) >= self.max_probe_seconds:
                forced.append(user)
        if forced:
            return min(forced, key=lambda u: float(self.last_polled_at_by_user.get(u, 0.0) or 0.0))

        def _score(user: str) -> tuple[int, float, float]:
            active_bonus = 1 if self._is_recently_active(user, now) else 0
            due_at = float(self.next_due_at_by_user.get(user, 0.0) or 0.0)
            overdue = max(0.0, now - due_at)
            last_polled = float(self.last_polled_at_by_user.get(user, 0.0) or 0.0)
            stale_for = max(0.0, now - last_polled) if last_polled > 0 else now
            return (active_bonus, overdue, stale_for)

        # Full-edge mode: keep the shared endpoint hot continuously while still
        # prioritizing recently active wallets and stale wallets for fairness.
        if self.cfg.activity_saturate_rps:
            return max(self.users, key=_score)

        due = [user for user in self.users if now >= float(self.next_due_at_by_user.get(user, 0.0) or 0.0)]
        if not due:
            return None
        return max(due, key=_score)

    def _idle_interval_seconds(self, user: str, now: float) -> float:
        idle_polls = int(self.idle_polls_by_user.get(user, 0))
        if self._is_recently_active(user, now):
            # Keep active wallets hot but still give scheduler room for fairness.
            return min(0.25, 0.01 * float(max(1, min(25, idle_polls))))
        return min(self.max_probe_seconds, 0.05 * float(max(1, min(25, idle_polls))))

    def _on_poll_result(self, user: str, emitted_count: int, now: float) -> None:
        self.last_polled_at_by_user[user] = now
        if emitted_count > 0:
            self.last_signal_at_by_user[user] = now
            self.idle_polls_by_user[user] = 0
            self.next_due_at_by_user[user] = now
            return

        idle_polls = int(self.idle_polls_by_user.get(user, 0)) + 1
        self.idle_polls_by_user[user] = idle_polls
        self.next_due_at_by_user[user] = now + self._idle_interval_seconds(user, now)

    def _on_poll_error(self, user: str, *, status: Optional[int] = None) -> None:
        now = time.time()
        self.errors += 1
        self.errors_by_user[user] = int(self.errors_by_user.get(user, 0)) + 1
        self.last_polled_at_by_user[user] = now

        idle_polls = int(self.idle_polls_by_user.get(user, 0)) + 1
        self.idle_polls_by_user[user] = idle_polls
        base = 0.1 if status == 429 else 0.2
        error_interval = min(self.max_probe_seconds, base + (0.05 * float(min(25, idle_polls))))
        self.next_due_at_by_user[user] = now + error_interval

    def _poll_user_once(self, user: str) -> int:
        start_signals = int(self.signals_by_user.get(user, 0))
        started = time.time()

        user_start_ts = int(self.start_ts_by_user.get(user, int(started) - int(self.cfg.activity_lookback_seconds)))
        latest_event_ts = user_start_ts
        aggregated: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        seen_at = started

        for page_idx in range(int(self.cfg.activity_max_pages)):
            offset = page_idx * int(self.cfg.activity_page_size)
            path = (
                f"/activity?user={user}&type=TRADE&limit={int(self.cfg.activity_page_size)}"
                f"&offset={offset}"
            )

            self.rate.wait_turn()
            body = self.http.get_json(path)
            self.rate.on_success()
            if not isinstance(body, list):
                raise RuntimeError("activity_payload_not_list")
            rows = [row for row in body if isinstance(row, dict)]
            if not rows:
                break

            for row in rows:
                tx_hash = str(row.get("transactionHash") or row.get("txHash") or row.get("hash") or "").strip().lower()
                if not tx_hash:
                    continue
                event_ts = _as_unix(row.get("timestamp"))
                if event_ts is None or event_ts < user_start_ts:
                    continue
                latest_event_ts = max(latest_event_ts, event_ts)

                row_id = (
                    str(row.get("id") or "").strip()
                    or "|".join(
                        [
                            tx_hash,
                            str(event_ts),
                            str(row.get("side") or ""),
                            str(row.get("asset") or row.get("assetId") or ""),
                            str(row.get("size") or ""),
                            str(row.get("usdcSize") if "usdcSize" in row else row.get("usdc_size") or ""),
                        ]
                    )
                )
                if not self._remember(user, row_id):
                    continue

                decoded = _decode_activity_row(
                    user=user,
                    row=row,
                    market_cache=self.market_cache,
                )
                side = str(decoded.get("side") or "").strip().upper()
                token_id = str(decoded.get("token_id") or "").strip()
                if side not in {"BUY", "SELL"} or not token_id:
                    continue

                agg_key = (tx_hash, side, token_id)
                current = aggregated.get(agg_key)
                if current is None:
                    current = {
                        "tx_hash": tx_hash,
                        "event_ts": event_ts,
                        "side": side,
                        "token_id": token_id,
                        "outcome": decoded.get("outcome"),
                        "slug": decoded.get("slug"),
                        "condition_id": decoded.get("condition_id"),
                        "size": 0.0,
                        "usdc_size": 0.0,
                        "event_count": 0,
                    }
                    aggregated[agg_key] = current

                current["event_ts"] = min(int(current["event_ts"]), event_ts)
                if not current.get("outcome") and decoded.get("outcome"):
                    current["outcome"] = decoded.get("outcome")
                if not current.get("slug") and decoded.get("slug"):
                    current["slug"] = decoded.get("slug")
                if not current.get("condition_id") and decoded.get("condition_id"):
                    current["condition_id"] = decoded.get("condition_id")

                size = _parse_amount(decoded.get("size"))
                usdc_size = _parse_amount(decoded.get("usdc_size"))
                if size is not None and size > 0:
                    current["size"] = float(current["size"]) + size
                if usdc_size is not None and usdc_size >= 0:
                    current["usdc_size"] = float(current["usdc_size"]) + usdc_size
                current["event_count"] = int(current["event_count"]) + 1

            if len(rows) < int(self.cfg.activity_page_size):
                break

        for (tx_hash, side, token_id), agg in aggregated.items():
            total_size = float(agg.get("size") or 0.0)
            total_usdc = float(agg.get("usdc_size") or 0.0)
            price: Optional[float] = None
            if total_size > 0 and total_usdc >= 0:
                price = total_usdc / total_size
            payload = {
                "side": side,
                "token_id": token_id,
                "outcome": agg.get("outcome"),
                "slug": agg.get("slug"),
                "condition_id": agg.get("condition_id"),
                "price": None if price is None else round(price, 9),
                "size": None if total_size <= 0 else round(total_size, 6),
                "usdc_size": None if total_usdc < 0 else round(total_usdc, 6),
                "decode_actionable": True,
                "aggregated_fill_count": int(agg.get("event_count") or 0),
            }
            signal = SourceSignal(
                tracked_user=user,
                source="activity",
                tx_hash=tx_hash,
                event_ts=int(agg.get("event_ts") or user_start_ts),
                seen_at=seen_at,
                payload=payload,
            )
            self._emit(user, signal)

        if latest_event_ts > user_start_ts:
            self.start_ts_by_user[user] = max(user_start_ts, latest_event_ts - 2)

        poll_rtt = time.time() - started
        self.last_poll_rtt = poll_rtt
        self.last_poll_rtt_by_user[user] = poll_rtt
        self.polls += 1
        self.polls_by_user[user] = int(self.polls_by_user.get(user, 0)) + 1
        return max(0, int(self.signals_by_user.get(user, 0)) - start_signals)

    def run(self) -> None:
        while not self.stop_event.is_set():
            now = time.time()
            user = self._select_user(now)
            if not user:
                next_due = min(float(self.next_due_at_by_user.get(u, now)) for u in self.users)
                sleep_for = max(0.001, min(0.2, next_due - now))
                self.stop_event.wait(sleep_for)
                continue

            try:
                emitted_count = self._poll_user_once(user)
                self._on_poll_result(user, emitted_count, time.time())
            except HttpStatusError as exc:
                if exc.status == 429:
                    self.rate.on_throttle()
                else:
                    self.rate.on_error()
                self._on_poll_error(user, status=exc.status)
            except Exception:
                self.rate.on_error()
                self._on_poll_error(user)

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        per_user: Dict[str, Dict[str, Any]] = {}
        for user in self.users:
            last_signal_at = float(self.last_signal_at_by_user.get(user, 0.0) or 0.0)
            last_signal_age = None if last_signal_at <= 0 else max(0.0, now - last_signal_at)
            per_user[user] = {
                "polls": int(self.polls_by_user.get(user, 0)),
                "errors": int(self.errors_by_user.get(user, 0)),
                "signals": int(self.signals_by_user.get(user, 0)),
                "dropped": int(self.dropped_by_user.get(user, 0)),
                "idle_polls": int(self.idle_polls_by_user.get(user, 0)),
                "next_due_in_seconds": round(max(0.0, float(self.next_due_at_by_user.get(user, now)) - now), 4),
                "last_signal_age_seconds": None if last_signal_age is None else round(last_signal_age, 4),
                "last_poll_rtt_seconds": None
                if self.last_poll_rtt_by_user.get(user) is None
                else round(float(self.last_poll_rtt_by_user[user] or 0.0), 4),
            }

        return {
            "source": "activity",
            "tracked_users": list(self.users),
            "batch_mode": True,
            "priority_mode": "active_wallet_weighted",
            "saturate_rps": bool(self.cfg.activity_saturate_rps),
            "activity_priority_active_window_seconds": round(self.active_window_seconds, 3),
            "activity_priority_max_probe_seconds": round(self.max_probe_seconds, 3),
            "target_rps": round(self.cfg.activity_target_rps, 3),
            "polls": self.polls,
            "errors": self.errors,
            "signals": self.signals,
            "dropped": self.dropped,
            "rate_instance_id": id(self.rate),
            "rate": self.rate.snapshot(),
            "last_poll_rtt_seconds": None if self.last_poll_rtt is None else round(self.last_poll_rtt, 4),
            "dedupe_keys": len(self.seen_row_keys),
            "per_user": per_user,
        }

    def close(self) -> None:
        self.http.close()


def _decode_subgraph_payload(
    *,
    event: Dict[str, Any],
    user: str,
    market_cache: GammaMarketCache,
) -> Dict[str, Any]:
    maker = str(event.get("maker") or "").strip().lower()
    taker = str(event.get("taker") or "").strip().lower()
    maker_asset_id = str(event.get("makerAssetId") or "").strip()
    taker_asset_id = str(event.get("takerAssetId") or "").strip()
    maker_amount = _parse_amount(event.get("makerAmountFilled"))
    taker_amount = _parse_amount(event.get("takerAmountFilled"))

    user_role = "unknown"
    given_asset_id = ""
    recv_asset_id = ""
    given_amount: Optional[float] = None
    recv_amount: Optional[float] = None
    if maker == user:
        user_role = "maker"
        given_asset_id = maker_asset_id
        recv_asset_id = taker_asset_id
        given_amount = maker_amount
        recv_amount = taker_amount
    elif taker == user:
        user_role = "taker"
        given_asset_id = taker_asset_id
        recv_asset_id = maker_asset_id
        given_amount = taker_amount
        recv_amount = maker_amount

    given_meta = market_cache.lookup_token(given_asset_id, queue_on_miss=True, fetch_on_miss=False)
    recv_meta = market_cache.lookup_token(recv_asset_id, queue_on_miss=True, fetch_on_miss=False)
    given_is_collateral = given_asset_id.lower() in COLLATERAL_ASSET_IDS
    recv_is_collateral = recv_asset_id.lower() in COLLATERAL_ASSET_IDS
    given_is_token = given_meta is not None
    recv_is_token = recv_meta is not None

    side = "UNKNOWN"
    token_id = ""
    token_meta: Optional[Dict[str, Any]] = None
    token_amount_raw: Optional[float] = None
    usdc_amount_raw: Optional[float] = None

    if given_is_collateral and not recv_is_collateral and (recv_is_token or not given_is_token):
        side = "BUY"
        token_id = recv_asset_id
        token_meta = recv_meta
        token_amount_raw = recv_amount
        usdc_amount_raw = given_amount
    elif recv_is_collateral and not given_is_collateral and (given_is_token or not recv_is_token):
        side = "SELL"
        token_id = given_asset_id
        token_meta = given_meta
        token_amount_raw = given_amount
        usdc_amount_raw = recv_amount
    elif recv_is_token and not given_is_token:
        side = "BUY"
        token_id = recv_asset_id
        token_meta = recv_meta
        token_amount_raw = recv_amount
        usdc_amount_raw = given_amount if given_is_collateral else None
    elif given_is_token and not recv_is_token:
        side = "SELL"
        token_id = given_asset_id
        token_meta = given_meta
        token_amount_raw = given_amount
        usdc_amount_raw = recv_amount if recv_is_collateral else None

    if token_id and token_meta is None:
        token_meta = market_cache.lookup_token(token_id, queue_on_miss=True, fetch_on_miss=True)

    token_size: Optional[float] = None
    usdc_size: Optional[float] = None
    price: Optional[float] = None
    if token_amount_raw is not None and token_amount_raw > 0:
        token_size = token_amount_raw / AMOUNT_SCALE
    if usdc_amount_raw is not None and usdc_amount_raw >= 0:
        usdc_size = usdc_amount_raw / AMOUNT_SCALE
    if token_amount_raw and token_amount_raw > 0 and usdc_amount_raw is not None and usdc_amount_raw >= 0:
        price = usdc_amount_raw / token_amount_raw

    return {
        "user_role": user_role,
        "side": side,
        "token_id": token_id or None,
        "outcome": None if token_meta is None else token_meta.get("outcome"),
        "slug": None if token_meta is None else token_meta.get("slug"),
        "condition_id": None if token_meta is None else token_meta.get("condition_id"),
        "price": None if price is None else round(price, 9),
        "size": None if token_size is None else round(token_size, 6),
        "usdc_size": None if usdc_size is None else round(usdc_size, 6),
        "decode_actionable": side in {"BUY", "SELL"} and bool(token_id),
    }


class OrderbookSubgraphSource(threading.Thread):
    ORDERBOOK_PATH_CANDIDATES = (
        # Current public Polymarket orderbook subgraph endpoint (Goldsky project id from docs).
        "/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn",
        "/api/public/project_clob-orderbook-subgraph/subgraphs/clob-orderbook-v2-polygon/latest/gn",
        "/api/public/project_clob-orderbook-subgraph/subgraphs/clob-orderbook-v2-polygon/gn",
        "/api/public/project_clob-orderbook-subgraph/subgraphs/clob-orderbook/latest/gn",
        "/api/public/project_clob-subgraph/subgraphs/clob-orderbook-v2-polygon/latest/gn",
    )

    QUERY = """
query OrderFilledEvents($user: String!, $start: BigInt!, $first: Int!) {
  orderFilledEvents(
    first: $first
    orderBy: timestamp
    orderDirection: desc
    where: {or: [{maker: $user, timestamp_gte: $start}, {taker: $user, timestamp_gte: $start}]}
  ) {
    id
    timestamp
    transactionHash
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
  }
  _meta { block { number timestamp hash } }
}
""".strip()

    def __init__(
        self,
        cfg: HybridConfig,
        tracked_user: str,
        out_queue: "Queue[SourceSignal]",
        stop_event: threading.Event,
        market_cache: GammaMarketCache,
        rate: Optional[RateGate] = None,
    ) -> None:
        self.user = str(tracked_user).strip().lower()
        user_tag = (self.user[-6:] if len(self.user) >= 6 else self.user).replace("0x", "")
        super().__init__(name=f"v2-subgraph-{user_tag}", daemon=True)
        self.cfg = cfg
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.market_cache = market_cache
        self.rate = rate if rate is not None else RateGate(cfg.subgraph_target_rps)
        self.http = JsonHttpClient(
            host="api.goldsky.com",
            timeout_seconds=cfg.subgraph_http_timeout_seconds,
            user_agent="poly-v2-hybrid-subgraph/1.0",
        )
        self.start_ts = int(time.time()) - 2
        self.seen_event_ids: Dict[str, float] = {}  # id → monotonic ts
        self.seen_order: Deque[tuple[str, float]] = deque()
        self._dedupe_ttl = 3600.0
        self.max_seen_ids = 500_000

        self.polls = 0
        self.errors = 0
        self.signals = 0
        self.dropped = 0
        self.last_poll_rtt: Optional[float] = None
        self.last_head_lag_seconds: Optional[float] = None
        self.idle_polls = 0
        self.idle_backoff_seconds = 0.0

        self._working_path: Optional[str] = None

    def _remember(self, event_id: str) -> bool:
        if event_id in self.seen_event_ids:
            return False
        now = time.monotonic()
        cutoff = now - self._dedupe_ttl
        while self.seen_order and self.seen_order[0][1] < cutoff:
            old_key, _ = self.seen_order.popleft()
            self.seen_event_ids.pop(old_key, None)
        while len(self.seen_order) > self.max_seen_ids:
            old_key, _ = self.seen_order.popleft()
            self.seen_event_ids.pop(old_key, None)
        self.seen_event_ids[event_id] = now
        self.seen_order.append((event_id, now))
        return True

    def _emit(self, signal: SourceSignal) -> None:
        try:
            self.out_queue.put_nowait(signal)
            self.signals += 1
        except Full:
            self.dropped += 1

    def _decode_payload(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return _decode_subgraph_payload(
            event=event,
            user=self.user,
            market_cache=self.market_cache,
        )

    def _post_subgraph(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        paths: List[str] = []
        if self._working_path:
            paths.append(self._working_path)
        paths.extend([p for p in self.ORDERBOOK_PATH_CANDIDATES if p != self._working_path])

        last_error: Optional[Exception] = None
        for path in paths:
            try:
                body = self.http.post_json(path, payload)
                self._working_path = path
                if isinstance(body, dict):
                    return body
                raise RuntimeError("subgraph_payload_not_dict")
            except HttpStatusError as exc:
                last_error = exc
                # 404/5xx can happen when trying candidate paths.
                if exc.status in {404, 500, 502, 503, 504}:
                    continue
                raise
            except Exception as exc:
                last_error = exc
                continue

        if last_error is not None:
            raise last_error
        raise RuntimeError("subgraph_post_failed")

    def _poll_once(self) -> int:
        start_signals = self.signals
        started = time.time()
        self.rate.wait_turn()
        payload = {
            "query": self.QUERY,
            "variables": {
                "user": self.user,
                "start": str(self.start_ts),
                "first": int(self.cfg.subgraph_page_size),
            },
        }
        body = self._post_subgraph(payload)
        self.rate.on_success()
        if "errors" in body:
            raise RuntimeError(f"subgraph_query_error: {body['errors']}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("subgraph_data_missing")

        meta = data.get("_meta")
        if isinstance(meta, dict):
            block = meta.get("block")
            if isinstance(block, dict):
                ts_raw = block.get("timestamp")
                if ts_raw is None:
                    head_ts = 0
                else:
                    try:
                        head_ts = int(ts_raw)
                    except (TypeError, ValueError):
                        head_ts = 0
                if head_ts > 0:
                    self.last_head_lag_seconds = max(0.0, time.time() - head_ts)

        events = data.get("orderFilledEvents")
        if not isinstance(events, list):
            raise RuntimeError("subgraph_events_not_list")
        seen_at = time.time()
        latest_event_ts = self.start_ts
        aggregated: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for event in events:
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("id") or "").strip()
            if not event_id:
                continue
            if not self._remember(event_id):
                continue
            tx_hash = str(event.get("transactionHash") or "").strip().lower()
            if not tx_hash:
                continue
            ts_raw_ev = event.get("timestamp")
            if ts_raw_ev is None:
                continue
            try:
                event_ts = int(ts_raw_ev)
            except (TypeError, ValueError):
                continue
            if event_ts < self.start_ts:
                continue
            if event_ts > latest_event_ts:
                latest_event_ts = event_ts

            decoded_payload = self._decode_payload(event)
            side = str(decoded_payload.get("side") or "").strip().upper()
            token_id = str(decoded_payload.get("token_id") or "").strip()
            size = _parse_amount(decoded_payload.get("size"))
            usdc_size = _parse_amount(decoded_payload.get("usdc_size"))

            if side in {"BUY", "SELL"} and token_id and (size is not None or usdc_size is not None):
                agg_key = (tx_hash, side, token_id)
                current = aggregated.get(agg_key)
                if current is None:
                    current = {
                        "tx_hash": tx_hash,
                        "event_ts": event_ts,
                        "side": side,
                        "token_id": token_id,
                        "outcome": decoded_payload.get("outcome"),
                        "slug": decoded_payload.get("slug"),
                        "condition_id": decoded_payload.get("condition_id"),
                        "size": 0.0,
                        "usdc_size": 0.0,
                        "event_count": 0,
                    }
                    aggregated[agg_key] = current
                current["event_ts"] = min(int(current["event_ts"]), event_ts)
                if not current.get("outcome") and decoded_payload.get("outcome"):
                    current["outcome"] = decoded_payload.get("outcome")
                if not current.get("slug") and decoded_payload.get("slug"):
                    current["slug"] = decoded_payload.get("slug")
                if not current.get("condition_id") and decoded_payload.get("condition_id"):
                    current["condition_id"] = decoded_payload.get("condition_id")
                if size is not None and size > 0:
                    current["size"] = float(current["size"]) + float(size)
                if usdc_size is not None and usdc_size >= 0:
                    current["usdc_size"] = float(current["usdc_size"]) + float(usdc_size)
                current["event_count"] = int(current["event_count"]) + 1
                continue

            decoded_payload["order_filled_event_id"] = event_id
            signal = SourceSignal(
                tracked_user=self.user,
                source="orderbook_subgraph",
                tx_hash=tx_hash,
                event_ts=event_ts,
                seen_at=seen_at,
                payload=decoded_payload,
            )
            self._emit(signal)

        for (tx_hash, side, token_id), agg in aggregated.items():
            total_size = float(agg.get("size") or 0.0)
            total_usdc = float(agg.get("usdc_size") or 0.0)
            price: Optional[float] = None
            if total_size > 0 and total_usdc >= 0:
                price = total_usdc / total_size
            payload = {
                "side": side,
                "token_id": token_id,
                "outcome": agg.get("outcome"),
                "slug": agg.get("slug"),
                "condition_id": agg.get("condition_id"),
                "price": None if price is None else round(price, 9),
                "size": None if total_size <= 0 else round(total_size, 6),
                "usdc_size": None if total_usdc < 0 else round(total_usdc, 6),
                "decode_actionable": True,
                "aggregated_fill_count": int(agg.get("event_count") or 0),
            }
            signal = SourceSignal(
                tracked_user=self.user,
                source="orderbook_subgraph",
                tx_hash=tx_hash,
                event_ts=int(agg.get("event_ts") or self.start_ts),
                seen_at=seen_at,
                payload=payload,
            )
            self._emit(signal)

        if latest_event_ts > self.start_ts:
            # Keep a small overlap window to avoid missing borderline-indexed events.
            self.start_ts = max(self.start_ts, latest_event_ts - 2)

        self.last_poll_rtt = time.time() - started
        self.polls += 1
        return max(0, self.signals - start_signals)

    def _on_poll_result(self, emitted_count: int) -> None:
        if emitted_count > 0:
            self.idle_polls = 0
            self.idle_backoff_seconds = 0.0
            return
        self.idle_polls += 1
        if self.idle_polls < 6:
            self.idle_backoff_seconds = 0.0
            return
        self.idle_backoff_seconds = min(0.4, 0.02 * float(self.idle_polls - 5))

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                emitted_count = self._poll_once()
                self._on_poll_result(emitted_count)
                if not self.cfg.subgraph_saturate_rps and self.idle_backoff_seconds > 0:
                    self.stop_event.wait(self.idle_backoff_seconds)
            except HttpStatusError as exc:
                self.errors += 1
                if exc.status == 429:
                    self.rate.on_throttle()
                else:
                    self.rate.on_error()
            except Exception:
                self.errors += 1
                self.rate.on_error()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "source": "orderbook_subgraph",
            "tracked_user": self.user,
            "batch_mode": False,
            "saturate_rps": bool(self.cfg.subgraph_saturate_rps),
            "target_rps": round(self.cfg.subgraph_target_rps, 3),
            "polls": self.polls,
            "errors": self.errors,
            "signals": self.signals,
            "dropped": self.dropped,
            "rate_instance_id": id(self.rate),
            "rate": self.rate.snapshot(),
            "last_poll_rtt_seconds": None if self.last_poll_rtt is None else round(self.last_poll_rtt, 4),
            "last_head_lag_seconds": None
            if self.last_head_lag_seconds is None
            else round(self.last_head_lag_seconds, 4),
            "dedupe_event_ids": len(self.seen_event_ids),
        }

    def close(self) -> None:
        self.http.close()


class OrderbookSubgraphBatchSource(threading.Thread):
    """Batch subgraph polling for multiple wallets in one GraphQL request."""

    ORDERBOOK_PATH_CANDIDATES = OrderbookSubgraphSource.ORDERBOOK_PATH_CANDIDATES

    def __init__(
        self,
        cfg: HybridConfig,
        tracked_users: Iterable[str],
        out_queue: "Queue[SourceSignal]",
        stop_event: threading.Event,
        market_cache: GammaMarketCache,
        rate: Optional[RateGate] = None,
    ) -> None:
        users: list[str] = []
        seen: set[str] = set()
        for raw in tracked_users:
            user = str(raw).strip().lower()
            if not user or user in seen:
                continue
            seen.add(user)
            users.append(user)
        if not users:
            raise ValueError("subgraph_batch_requires_users")

        self.users = tuple(users)
        super().__init__(name="v2-subgraph-batch", daemon=True)
        self.cfg = cfg
        self.out_queue = out_queue
        self.stop_event = stop_event
        self.market_cache = market_cache
        self.rate = rate if rate is not None else RateGate(cfg.subgraph_target_rps)
        self.http = JsonHttpClient(
            host="api.goldsky.com",
            timeout_seconds=cfg.subgraph_http_timeout_seconds,
            user_agent="poly-v2-hybrid-subgraph/1.0",
        )

        base_start = int(time.time()) - 2
        self.start_ts_by_user: Dict[str, int] = {user: base_start for user in self.users}
        self.seen_event_ids: Dict[str, float] = {}  # id → monotonic ts
        self.seen_order: Deque[tuple[str, float]] = deque()
        self._dedupe_ttl = 3600.0
        self.max_seen_ids = max(500_000, 100_000 * len(self.users))

        self.polls = 0
        self.errors = 0
        self.signals = 0
        self.dropped = 0
        self.last_poll_rtt: Optional[float] = None
        self.last_head_lag_seconds: Optional[float] = None
        self.idle_polls = 0
        self.idle_backoff_seconds = 0.0

        self._working_path: Optional[str] = None
        self._query = self._build_query()

    def _build_query(self) -> str:
        var_defs = ["$first: Int!"]
        segments: List[str] = []
        for idx, _user in enumerate(self.users):
            alias = f"u{idx}"
            user_var = f"u{idx}"
            start_var = f"s{idx}"
            var_defs.append(f"${user_var}: String!")
            var_defs.append(f"${start_var}: BigInt!")
            segments.append(
                (
                    f"  {alias}: orderFilledEvents(\n"
                    f"    first: $first\n"
                    f"    orderBy: timestamp\n"
                    f"    orderDirection: desc\n"
                    f"    where: {{or: [{{maker: ${user_var}, timestamp_gte: ${start_var}}}, "
                    f"{{taker: ${user_var}, timestamp_gte: ${start_var}}}]}}\n"
                    f"  ) {{\n"
                    f"    id\n"
                    f"    timestamp\n"
                    f"    transactionHash\n"
                    f"    maker\n"
                    f"    taker\n"
                    f"    makerAssetId\n"
                    f"    takerAssetId\n"
                    f"    makerAmountFilled\n"
                    f"    takerAmountFilled\n"
                    f"  }}\n"
                )
            )
        return (
            f"query OrderFilledEventsBatch({', '.join(var_defs)}) {{\n"
            + "".join(segments)
            + "  _meta { block { number timestamp hash } }\n"
            + "}\n"
        )

    def _remember(self, user: str, event_id: str) -> bool:
        dedupe_key = f"{user}|{event_id}"
        if dedupe_key in self.seen_event_ids:
            return False
        now = time.monotonic()
        cutoff = now - self._dedupe_ttl
        while self.seen_order and self.seen_order[0][1] < cutoff:
            old_key, _ = self.seen_order.popleft()
            self.seen_event_ids.pop(old_key, None)
        while len(self.seen_order) > self.max_seen_ids:
            old_key, _ = self.seen_order.popleft()
            self.seen_event_ids.pop(old_key, None)
        self.seen_event_ids[dedupe_key] = now
        self.seen_order.append((dedupe_key, now))
        return True

    def _emit(self, signal: SourceSignal) -> None:
        try:
            self.out_queue.put_nowait(signal)
            self.signals += 1
        except Full:
            self.dropped += 1

    def _post_subgraph(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        paths: List[str] = []
        if self._working_path:
            paths.append(self._working_path)
        paths.extend([p for p in self.ORDERBOOK_PATH_CANDIDATES if p != self._working_path])

        last_error: Optional[Exception] = None
        for path in paths:
            try:
                body = self.http.post_json(path, payload)
                self._working_path = path
                if isinstance(body, dict):
                    return body
                raise RuntimeError("subgraph_payload_not_dict")
            except HttpStatusError as exc:
                last_error = exc
                if exc.status in {404, 500, 502, 503, 504}:
                    continue
                raise
            except Exception as exc:
                last_error = exc
                continue

        if last_error is not None:
            raise last_error
        raise RuntimeError("subgraph_post_failed")

    def _poll_once(self) -> int:
        start_signals = self.signals
        started = time.time()
        self.rate.wait_turn()

        variables: Dict[str, Any] = {"first": int(self.cfg.subgraph_page_size)}
        for idx, user in enumerate(self.users):
            variables[f"u{idx}"] = user
            variables[f"s{idx}"] = str(int(self.start_ts_by_user.get(user, int(time.time()) - 2)))
        payload = {"query": self._query, "variables": variables}

        body = self._post_subgraph(payload)
        self.rate.on_success()
        if "errors" in body:
            raise RuntimeError(f"subgraph_query_error: {body['errors']}")
        data = body.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("subgraph_data_missing")

        meta = data.get("_meta")
        if isinstance(meta, dict):
            block = meta.get("block")
            if isinstance(block, dict):
                ts_raw = block.get("timestamp")
                if ts_raw is None:
                    head_ts = 0
                else:
                    try:
                        head_ts = int(ts_raw)
                    except (TypeError, ValueError):
                        head_ts = 0
                if head_ts > 0:
                    self.last_head_lag_seconds = max(0.0, time.time() - head_ts)

        seen_at = time.time()
        latest_event_ts_by_user = {user: int(self.start_ts_by_user.get(user, 0)) for user in self.users}
        aggregated: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}

        for idx, user in enumerate(self.users):
            alias = f"u{idx}"
            events = data.get(alias)
            if not isinstance(events, list):
                continue
            user_start = int(self.start_ts_by_user.get(user, 0))

            for event in events:
                if not isinstance(event, dict):
                    continue
                event_id = str(event.get("id") or "").strip()
                if not event_id:
                    continue
                if not self._remember(user, event_id):
                    continue
                tx_hash = str(event.get("transactionHash") or "").strip().lower()
                if not tx_hash:
                    continue
                ts_raw = event.get("timestamp")
                if ts_raw is None:
                    continue
                try:
                    event_ts = int(ts_raw)
                except (TypeError, ValueError):
                    continue
                if event_ts < user_start:
                    continue
                if event_ts > latest_event_ts_by_user[user]:
                    latest_event_ts_by_user[user] = event_ts

                decoded_payload = _decode_subgraph_payload(
                    event=event,
                    user=user,
                    market_cache=self.market_cache,
                )
                side = str(decoded_payload.get("side") or "").strip().upper()
                token_id = str(decoded_payload.get("token_id") or "").strip()
                size = _parse_amount(decoded_payload.get("size"))
                usdc_size = _parse_amount(decoded_payload.get("usdc_size"))

                if side in {"BUY", "SELL"} and token_id and (size is not None or usdc_size is not None):
                    agg_key = (user, tx_hash, side, token_id)
                    current = aggregated.get(agg_key)
                    if current is None:
                        current = {
                            "user": user,
                            "tx_hash": tx_hash,
                            "event_ts": event_ts,
                            "side": side,
                            "token_id": token_id,
                            "outcome": decoded_payload.get("outcome"),
                            "slug": decoded_payload.get("slug"),
                            "condition_id": decoded_payload.get("condition_id"),
                            "size": 0.0,
                            "usdc_size": 0.0,
                            "event_count": 0,
                        }
                        aggregated[agg_key] = current
                    current["event_ts"] = min(int(current["event_ts"]), event_ts)
                    if not current.get("outcome") and decoded_payload.get("outcome"):
                        current["outcome"] = decoded_payload.get("outcome")
                    if not current.get("slug") and decoded_payload.get("slug"):
                        current["slug"] = decoded_payload.get("slug")
                    if not current.get("condition_id") and decoded_payload.get("condition_id"):
                        current["condition_id"] = decoded_payload.get("condition_id")
                    if size is not None and size > 0:
                        current["size"] = float(current["size"]) + float(size)
                    if usdc_size is not None and usdc_size >= 0:
                        current["usdc_size"] = float(current["usdc_size"]) + float(usdc_size)
                    current["event_count"] = int(current["event_count"]) + 1
                    continue

                decoded_payload["order_filled_event_id"] = event_id
                signal = SourceSignal(
                    tracked_user=user,
                    source="orderbook_subgraph",
                    tx_hash=tx_hash,
                    event_ts=event_ts,
                    seen_at=seen_at,
                    payload=decoded_payload,
                )
                self._emit(signal)

        for (_user, tx_hash, side, token_id), agg in aggregated.items():
            total_size = float(agg.get("size") or 0.0)
            total_usdc = float(agg.get("usdc_size") or 0.0)
            price: Optional[float] = None
            if total_size > 0 and total_usdc >= 0:
                price = total_usdc / total_size
            payload = {
                "side": side,
                "token_id": token_id,
                "outcome": agg.get("outcome"),
                "slug": agg.get("slug"),
                "condition_id": agg.get("condition_id"),
                "price": None if price is None else round(price, 9),
                "size": None if total_size <= 0 else round(total_size, 6),
                "usdc_size": None if total_usdc < 0 else round(total_usdc, 6),
                "decode_actionable": True,
                "aggregated_fill_count": int(agg.get("event_count") or 0),
            }
            signal = SourceSignal(
                tracked_user=str(agg.get("user") or ""),
                source="orderbook_subgraph",
                tx_hash=tx_hash,
                event_ts=int(agg.get("event_ts") or 0),
                seen_at=seen_at,
                payload=payload,
            )
            self._emit(signal)

        for user, latest_event_ts in latest_event_ts_by_user.items():
            current_start = int(self.start_ts_by_user.get(user, latest_event_ts))
            if latest_event_ts > current_start:
                self.start_ts_by_user[user] = max(current_start, latest_event_ts - 2)

        self.last_poll_rtt = time.time() - started
        self.polls += 1
        return max(0, self.signals - start_signals)

    def _on_poll_result(self, emitted_count: int) -> None:
        if emitted_count > 0:
            self.idle_polls = 0
            self.idle_backoff_seconds = 0.0
            return
        self.idle_polls += 1
        if self.idle_polls < 6:
            self.idle_backoff_seconds = 0.0
            return
        self.idle_backoff_seconds = min(0.4, 0.02 * float(self.idle_polls - 5))

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                emitted_count = self._poll_once()
                self._on_poll_result(emitted_count)
                if not self.cfg.subgraph_saturate_rps and self.idle_backoff_seconds > 0:
                    self.stop_event.wait(self.idle_backoff_seconds)
            except HttpStatusError as exc:
                self.errors += 1
                if exc.status == 429:
                    self.rate.on_throttle()
                else:
                    self.rate.on_error()
            except Exception:
                self.errors += 1
                self.rate.on_error()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "source": "orderbook_subgraph",
            "tracked_users": list(self.users),
            "batch_mode": True,
            "saturate_rps": bool(self.cfg.subgraph_saturate_rps),
            "target_rps": round(self.cfg.subgraph_target_rps, 3),
            "polls": self.polls,
            "errors": self.errors,
            "signals": self.signals,
            "dropped": self.dropped,
            "rate_instance_id": id(self.rate),
            "rate": self.rate.snapshot(),
            "last_poll_rtt_seconds": None if self.last_poll_rtt is None else round(self.last_poll_rtt, 4),
            "last_head_lag_seconds": None
            if self.last_head_lag_seconds is None
            else round(self.last_head_lag_seconds, 4),
            "dedupe_event_ids": len(self.seen_event_ids),
        }

    def close(self) -> None:
        self.http.close()
