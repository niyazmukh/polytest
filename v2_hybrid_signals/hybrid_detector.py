from __future__ import annotations

import json
import threading
import time
from collections import Counter
from queue import Empty, Queue
from typing import Any, Callable, Dict, Iterable, List, Optional

from .config import HybridConfig
from .market_cache import GammaMarketCache
from .models import SourceSignal
from .rate_gate import RateGate
from .sources import (
    DataApiActivityBatchSource,
    DataApiActivitySource,
    OrderbookSubgraphBatchSource,
    OrderbookSubgraphSource,
)
from .utils import safe_stdout_flush


def _as_float(raw: Any) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


class HybridDetector:
    def __init__(
        self,
        cfg: HybridConfig,
        *,
        signal_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.cfg = cfg
        self._signal_callback = signal_callback
        self.stop_event = stop_event or threading.Event()
        self.signal_queue: "Queue[SourceSignal]" = Queue(maxsize=int(cfg.queue_maxsize))

        # Shared rate gates — one per API endpoint, not per wallet.
        # Without sharing, N wallets would hit N× the intended rate budget.
        self._activity_rate = RateGate(cfg.activity_target_rps)
        self._subgraph_rate = RateGate(cfg.subgraph_target_rps)

        self.market_cache = GammaMarketCache(cfg=cfg, stop_event=self.stop_event)
        if bool(cfg.activity_batch_enabled) and len(cfg.users) > 1:
            self.activities = [
                DataApiActivityBatchSource(
                    cfg=cfg,
                    tracked_users=cfg.users,
                    out_queue=self.signal_queue,
                    stop_event=self.stop_event,
                    market_cache=self.market_cache,
                    rate=self._activity_rate,
                )
            ]
        else:
            self.activities = [
                DataApiActivitySource(
                    cfg=cfg,
                    tracked_user=user,
                    out_queue=self.signal_queue,
                    stop_event=self.stop_event,
                    market_cache=self.market_cache,
                    rate=self._activity_rate,
                )
                for user in cfg.users
            ]
        if bool(cfg.subgraph_batch_enabled) and len(cfg.users) > 1:
            self.subgraphs = [
                OrderbookSubgraphBatchSource(
                    cfg=cfg,
                    tracked_users=cfg.users,
                    out_queue=self.signal_queue,
                    stop_event=self.stop_event,
                    market_cache=self.market_cache,
                    rate=self._subgraph_rate,
                )
            ]
        else:
            self.subgraphs = [
                OrderbookSubgraphSource(
                    cfg=cfg,
                    tracked_user=user,
                    out_queue=self.signal_queue,
                    stop_event=self.stop_event,
                    market_cache=self.market_cache,
                    rate=self._subgraph_rate,
                )
                for user in cfg.users
            ]

        self.started_at = 0.0
        self.deadline_at: float | None = None

        self.signals_processed = 0
        self.confirmations = 0
        self.actionable_emitted = 0
        self.redundant_dropped = 0

        self.match_state: Dict[str, Dict[str, Any]] = {}
        self.tx_keys: set[str] = set()

        self.first_source_counts: Counter[str] = Counter()
        self.confirm_by_second_source: Counter[str] = Counter()
        self.actionable_emitted_by_source: Counter[str] = Counter()

    def _emit(self, payload: Dict[str, Any]) -> None:
        print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        safe_stdout_flush()
        if self._signal_callback is not None:
            try:
                self._signal_callback(payload)
            except Exception:
                pass

    @staticmethod
    def _canonical_signal_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "side": payload.get("side"),
            "token_id": payload.get("token_id"),
            "outcome": payload.get("outcome"),
            "condition_id": payload.get("condition_id"),
            "slug": payload.get("slug"),
            "price": payload.get("price"),
            "size": payload.get("size"),
            "usdc_size": payload.get("usdc_size"),
        }

    def _build_match_key(self, signal: SourceSignal, payload: Dict[str, Any]) -> str:
        side = str(payload.get("side") or "").strip().upper()
        token_id = str(payload.get("token_id") or "").strip()
        if side and token_id:
            return "|".join([signal.tracked_user, signal.tx_hash, side, token_id])
        return "|".join([signal.tracked_user, signal.tx_hash, str(signal.event_ts)])

    def _is_actionable_payload(self, payload: Dict[str, Any]) -> bool:
        side = str(payload.get("side") or "").strip().upper()
        token_id = str(payload.get("token_id") or "").strip()
        if side not in {"BUY", "SELL"}:
            return False
        if not token_id:
            return False
        if self.cfg.actionable_require_outcome:
            outcome = str(payload.get("outcome") or "").strip()
            if not outcome:
                return False
        return True

    def _emit_source_event(self, signal: SourceSignal, match_key: str, payload: Dict[str, Any], event_age: float) -> None:
        if not self.cfg.emit_source_events:
            return
        self._emit(
            {
                "event": "SOURCE_EVENT",
                "match_key": match_key,
                "tracked_user": signal.tracked_user,
                "source": signal.source,
                "tx_hash": signal.tx_hash,
                "event_ts": signal.event_ts,
                "seen_at_unix": round(signal.seen_at, 6),
                "event_age_seconds": round(event_age, 6),
                "payload": payload,
            }
        )

    @staticmethod
    def _aggregate_source_snapshots(snapshots: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        rows = [row for row in snapshots if isinstance(row, dict)]
        if not rows:
            return {}

        out: Dict[str, Any] = {
            "source": rows[0].get("source"),
            "tracked_users": [],
            "target_rps": 0.0,
            "polls": 0,
            "errors": 0,
            "signals": 0,
            "dropped": 0,
            "last_poll_rtt_seconds": None,
        }

        target_rps_max = 0.0
        rate_target = 0.0
        rate_effective = 0.0
        rate_factor_sum = 0.0
        rate_count = 0
        dedupe_keys_total = 0
        dedupe_event_ids_total = 0
        last_head_lag: float | None = None
        per_user: Dict[str, Dict[str, Any]] = {}
        rate_seen: set[str] = set()

        for idx, row in enumerate(rows):
            tracked_user = row.get("tracked_user")
            if tracked_user:
                out["tracked_users"].append(str(tracked_user))
            tracked_users = row.get("tracked_users")
            if isinstance(tracked_users, list):
                for user in tracked_users:
                    if user:
                        out["tracked_users"].append(str(user))

            row_target_rps = _as_float(row.get("target_rps"))
            if row_target_rps > target_rps_max:
                target_rps_max = row_target_rps
            out["polls"] += int(_as_float(row.get("polls")))
            out["errors"] += int(_as_float(row.get("errors")))
            out["signals"] += int(_as_float(row.get("signals")))
            out["dropped"] += int(_as_float(row.get("dropped")))

            if row.get("batch_mode"):
                out["batch_mode"] = True
            if row.get("priority_mode"):
                out["priority_mode"] = row.get("priority_mode")
            if row.get("activity_priority_active_window_seconds") is not None:
                out["activity_priority_active_window_seconds"] = row.get("activity_priority_active_window_seconds")
            if row.get("activity_priority_max_probe_seconds") is not None:
                out["activity_priority_max_probe_seconds"] = row.get("activity_priority_max_probe_seconds")

            row_per_user = row.get("per_user")
            if isinstance(row_per_user, dict):
                for user, value in row_per_user.items():
                    if isinstance(user, str) and isinstance(value, dict):
                        per_user[user] = value

            poll_rtt = row.get("last_poll_rtt_seconds")
            if poll_rtt is not None:
                poll_rtt_f = _as_float(poll_rtt)
                prev = out.get("last_poll_rtt_seconds")
                if prev is None or poll_rtt_f > _as_float(prev):
                    out["last_poll_rtt_seconds"] = round(poll_rtt_f, 4)

            dedupe_keys_total += int(_as_float(row.get("dedupe_keys")))
            dedupe_event_ids_total += int(_as_float(row.get("dedupe_event_ids")))

            lag = row.get("last_head_lag_seconds")
            if lag is not None:
                lag_f = _as_float(lag)
                if last_head_lag is None or lag_f > last_head_lag:
                    last_head_lag = lag_f

            rate = row.get("rate")
            if isinstance(rate, dict):
                rate_key = row.get("rate_instance_id")
                if rate_key is None:
                    rate_id = f"row:{idx}"
                else:
                    rate_id = str(rate_key)
                if rate_id in rate_seen:
                    continue
                rate_seen.add(rate_id)
                rate_target += _as_float(rate.get("target_rps"))
                rate_effective += _as_float(rate.get("effective_rps"))
                rate_factor_sum += _as_float(rate.get("throttle_factor"))
                rate_count += 1

        out["target_rps"] = round(target_rps_max, 3)
        if rows and rows[0].get("source") == "orderbook_subgraph":
            out["dedupe_event_ids"] = dedupe_event_ids_total
        else:
            out["dedupe_keys"] = dedupe_keys_total
        if last_head_lag is not None:
            out["last_head_lag_seconds"] = round(last_head_lag, 4)
        if per_user:
            out["per_user"] = per_user
        if rate_count > 0:
            out["rate"] = {
                "target_rps": round(rate_target, 6),
                "effective_rps": round(rate_effective, 6),
                "throttle_factor": round(rate_factor_sum / rate_count, 6),
            }
        else:
            out["rate"] = {}

        # Preserve stable order while removing duplicates.
        seen_users: set[str] = set()
        unique_users: list[str] = []
        for user in out["tracked_users"]:
            if user in seen_users:
                continue
            seen_users.add(user)
            unique_users.append(user)
        out["tracked_users"] = unique_users

        if len(out["tracked_users"]) == 1:
            out["tracked_user"] = out["tracked_users"][0]
            out.pop("tracked_users", None)
        return out

    def _process_signal(self, signal: SourceSignal) -> None:
        now = time.time()
        self.signals_processed += 1

        payload = dict(signal.payload or {})
        match_key = self._build_match_key(signal, payload)
        event_age = max(0.0, now - float(signal.event_ts))
        self.tx_keys.add(f"{signal.tracked_user}|{signal.tx_hash}")

        self._emit_source_event(signal, match_key, payload, event_age)

        if not self._is_actionable_payload(payload):
            self.redundant_dropped += 1
            return

        state = self.match_state.get(match_key)
        if state is None:
            state = {
                "tracked_user": signal.tracked_user,
                "tx_hash": signal.tx_hash,
                "event_ts": signal.event_ts,
                "first_source": signal.source,
                "first_seen_at": float(signal.seen_at),
                "first_payload": payload,
                "sources": {signal.source},
                "confirmed": False,
            }
            self.match_state[match_key] = state

            self.first_source_counts[signal.source] += 1
            self.actionable_emitted += 1
            self.actionable_emitted_by_source[signal.source] += 1

            self._emit(
                {
                    "event": "HYBRID_SIGNAL_FIRST",
                    "match_key": match_key,
                    "tracked_user": signal.tracked_user,
                    "tx_hash": signal.tx_hash,
                    "event_ts": signal.event_ts,
                    "first_source": signal.source,
                    "first_seen_at_unix": round(float(signal.seen_at), 6),
                    "event_age_seconds": round(event_age, 6),
                    "payload": payload,
                }
            )
            self._emit(
                {
                    "event": "ACTIONABLE_SIGNAL",
                    "match_key": match_key,
                    "tracked_user": signal.tracked_user,
                    "tx_hash": signal.tx_hash,
                    "event_ts": signal.event_ts,
                    "source": signal.source,
                    "stage": "first",
                    "seen_at_unix": round(float(signal.seen_at), 6),
                    "event_age_seconds": round(event_age, 6),
                    "signal": self._canonical_signal_payload(payload),
                }
            )
            return

        if signal.source in state["sources"]:
            self.redundant_dropped += 1
            return

        state["sources"].add(signal.source)
        second_seen_at = float(signal.seen_at)
        second_minus_first = second_seen_at - float(state["first_seen_at"])

        self.confirmations += 1
        self.confirm_by_second_source[signal.source] += 1
        state["confirmed"] = True

        self._emit(
            {
                "event": "HYBRID_SIGNAL_CONFIRM",
                "match_key": match_key,
                "tracked_user": state["tracked_user"],
                "tx_hash": state["tx_hash"],
                "event_ts": int(state["event_ts"]),
                "first_source": state["first_source"],
                "second_source": signal.source,
                "first_seen_at_unix": round(float(state["first_seen_at"]), 6),
                "second_seen_at_unix": round(second_seen_at, 6),
                "second_minus_first_seconds": round(second_minus_first, 6),
                "first_payload": state["first_payload"],
                "second_payload": payload,
            }
        )

    # ------------------------------------------------------------------
    # Prune stale match / tx entries to prevent unbounded memory growth.
    # Called once per progress interval. Entries older than 2× the
    # effective signal window are removed.
    # ------------------------------------------------------------------
    _MATCH_TTL_S = 600.0  # fixed 10-minute TTL regardless of runtime

    def _prune_stale(self, now: float) -> None:
        ttl = self._MATCH_TTL_S
        cutoff = now - ttl
        stale_keys = [
            k for k, v in self.match_state.items()
            if float(v.get("first_seen_at", now)) < cutoff
        ]
        for k in stale_keys:
            del self.match_state[k]
        # tx_keys don't carry timestamps; cap size instead.
        if len(self.tx_keys) > 200_000:
            self.tx_keys.clear()

    def _summary_payload(self) -> Dict[str, Any]:
        elapsed = max(0.0, time.time() - self.started_at)
        return {
            "event": "HYBRID_SUMMARY",
            "elapsed_seconds": round(elapsed, 3),
            "tracked_wallets": list(self.cfg.users),
            "match_unique": len(self.match_state),
            "tx_unique": len(self.tx_keys),
            "signals_processed": self.signals_processed,
            "signals_per_second": round(self.signals_processed / elapsed, 4) if elapsed > 0 else 0.0,
            "confirmations": self.confirmations,
            "actionable_emitted": self.actionable_emitted,
            "actionable_emitted_by_source": dict(self.actionable_emitted_by_source),
            "redundant_dropped": self.redundant_dropped,
            "first_source_counts": dict(self.first_source_counts),
            "confirm_by_second_source": dict(self.confirm_by_second_source),
            "activity": self._aggregate_source_snapshots([src.snapshot() for src in self.activities]),
            "orderbook_subgraph": self._aggregate_source_snapshots([src.snapshot() for src in self.subgraphs]),
            "gamma_market_cache": self.market_cache.snapshot(),
        }

    def run(self) -> int:
        self.started_at = time.time()
        self.deadline_at = None if self.cfg.runtime_seconds is None else (self.started_at + self.cfg.runtime_seconds)

        self.market_cache.start()
        for source in self.activities:
            source.start()
        for source in self.subgraphs:
            source.start()

        next_progress_at = self.started_at + self.cfg.progress_interval_seconds

        try:
            while not self.stop_event.is_set():
                now = time.time()
                if self.deadline_at is not None and now >= self.deadline_at:
                    break

                try:
                    signal = self.signal_queue.get(timeout=0.05)
                except Empty:
                    signal = None

                if signal is not None:
                    self._process_signal(signal)

                now = time.time()
                if now >= next_progress_at:
                    self._prune_stale(now)
                    self._emit(
                        {
                            "event": "HYBRID_PROGRESS",
                            "elapsed_seconds": round(now - self.started_at, 3),
                            "match_unique": len(self.match_state),
                            "tx_unique": len(self.tx_keys),
                            "signals_processed": self.signals_processed,
                            "confirmations": self.confirmations,
                            "actionable_emitted": self.actionable_emitted,
                            "activity": self._aggregate_source_snapshots([src.snapshot() for src in self.activities]),
                            "orderbook_subgraph": self._aggregate_source_snapshots(
                                [src.snapshot() for src in self.subgraphs]
                            ),
                            "gamma_market_cache": self.market_cache.snapshot(),
                        }
                    )
                    next_progress_at = now + self.cfg.progress_interval_seconds

            return 0
        finally:
            self.stop_event.set()
            self.market_cache.join(timeout=5.0)
            for source in self.activities:
                source.join(timeout=5.0)
            for source in self.subgraphs:
                source.join(timeout=5.0)
            self.market_cache.close()
            for source in self.activities:
                source.close()
            for source in self.subgraphs:
                source.close()
            self._emit(self._summary_payload())
