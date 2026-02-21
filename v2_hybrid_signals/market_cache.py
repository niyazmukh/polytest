from __future__ import annotations

import json
import re
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional
from urllib.parse import urlencode

from .config import HybridConfig
from .http_json import HttpStatusError, JsonHttpClient
from .rate_gate import RateGate
from .utils import as_bool as _as_bool, parse_iso8601_ts as _parse_iso8601_ts


def _parse_json_list(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    text = str(raw).strip()
    if not text:
        return []
    if text[0] == "[" and text[-1] == "]":
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
    return [part.strip() for part in text.split(",") if part.strip()]


class GammaMarketCache(threading.Thread):
    def __init__(
        self,
        cfg: HybridConfig,
        stop_event: threading.Event,
        markets_rate: Optional[RateGate] = None,
        events_rate: Optional[RateGate] = None,
    ) -> None:
        super().__init__(name="v2-gamma-cache", daemon=True)
        self.cfg = cfg
        self.stop_event = stop_event
        self.http = JsonHttpClient(
            host="gamma-api.polymarket.com",
            timeout_seconds=cfg.gamma_http_timeout_seconds,
            user_agent="poly-v2-hybrid-gamma/1.0",
        )
        self.markets_rate = markets_rate if markets_rate is not None else RateGate(cfg.gamma_target_rps)
        self.events_rate = events_rate if events_rate is not None else RateGate(cfg.gamma_events_target_rps)

        self._lock = threading.RLock()
        self._token_meta: Dict[str, Dict[str, Any]] = {}
        self._pending_tokens: Deque[str] = deque()
        self._pending_set: set[str] = set()

        self._live_slug_re_by_tf: Dict[int, re.Pattern[str]] = {
            tf: re.compile(rf"^{re.escape(cfg.live_market_symbol)}-updown-{int(tf)}m-[0-9]+$")
            for tf in cfg.live_market_timeframes_minutes
        }
        self._live_market: Optional[Dict[str, Any]] = None
        self._live_markets: Dict[int, Dict[str, Any]] = {}

        self.polls = 0
        self.errors = 0
        self.upserts = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.inline_fetches = 0
        self.last_poll_rtt: Optional[float] = None
        self.last_full_refresh_at: Optional[float] = None
        self.last_live_refresh_at: Optional[float] = None

    def _lookup_token_no_side_effect(self, token_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._token_meta.get(token_id)
            return None if row is None else dict(row)

    def queue_token(self, token_id: str) -> None:
        token_id = str(token_id).strip()
        if not token_id or token_id == "0":
            return
        with self._lock:
            if token_id in self._pending_set:
                return
            self._pending_set.add(token_id)
            self._pending_tokens.append(token_id)

    def lookup_token(
        self,
        token_id: str,
        *,
        queue_on_miss: bool = True,
        fetch_on_miss: bool = False,
    ) -> Optional[Dict[str, Any]]:
        token_id = str(token_id).strip()
        if not token_id or token_id == "0":
            return None

        meta = self._lookup_token_no_side_effect(token_id)
        if meta is not None:
            self.cache_hits += 1
            return meta

        self.cache_misses += 1
        if queue_on_miss:
            self.queue_token(token_id)

        if fetch_on_miss:
            try:
                self.inline_fetches += 1
                self._refresh_token(token_id)
            except Exception:
                pass
            meta = self._lookup_token_no_side_effect(token_id)
            if meta is not None:
                self.cache_hits += 1
                return meta
        return None

    def hint_from_activity(
        self,
        token_id: str,
        outcome: Optional[str],
        slug: Optional[str],
        condition_id: Optional[str],
    ) -> None:
        token_id = token_id.strip()
        if not token_id or token_id == "0":
            return
        changed = False
        with self._lock:
            existing = self._token_meta.get(token_id)
            if existing is None:
                existing = {"token_id": token_id}
                self._token_meta[token_id] = existing
                changed = True
            if outcome:
                value = str(outcome).strip()
                if value and existing.get("outcome") != value:
                    existing["outcome"] = value
                    changed = True
            if slug:
                value = str(slug).strip().lower()
                if value and existing.get("slug") != value:
                    existing["slug"] = value
                    changed = True
            if condition_id:
                value = str(condition_id).strip().lower()
                if value and existing.get("condition_id") != value:
                    existing["condition_id"] = value
                    changed = True
        if changed:
            self.upserts += 1

    def _ingest_market_row(self, row: Dict[str, Any]) -> int:
        token_ids = [str(v).strip() for v in _parse_json_list(row.get("clobTokenIds"))]
        outcomes = [str(v).strip() for v in _parse_json_list(row.get("outcomes"))]
        if not token_ids:
            return 0

        condition_id = str(row.get("conditionId") or "").strip().lower()
        slug = str(row.get("slug") or "").strip().lower()
        market_id = str(row.get("id") or "").strip()
        active = _as_bool(row.get("active"))
        closed = _as_bool(row.get("closed"))
        enable_order_book = _as_bool(row.get("enableOrderBook"))
        start_ts = _parse_iso8601_ts(row.get("startDate"))
        end_ts = _parse_iso8601_ts(row.get("endDate"))
        updated_at = time.time()

        changed = 0
        with self._lock:
            for idx, token_id in enumerate(token_ids):
                if not token_id or token_id == "0":
                    continue
                outcome = outcomes[idx] if idx < len(outcomes) else ""
                current = self._token_meta.get(token_id)
                next_meta = {
                    "token_id": token_id,
                    "outcome": outcome,
                    "slug": slug,
                    "condition_id": condition_id,
                    "market_id": market_id,
                    "active": active,
                    "closed": closed,
                    "enable_order_book": enable_order_book,
                    "start_ts": None if start_ts is None else round(start_ts, 6),
                    "end_ts": None if end_ts is None else round(end_ts, 6),
                    "updated_at_unix": round(updated_at, 6),
                }
                if current != next_meta:
                    self._token_meta[token_id] = next_meta
                    changed += 1
        return changed

    def _fetch_markets(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        path = f"/markets?{urlencode(params)}"
        self.markets_rate.wait_turn()
        try:
            body = self.http.get_json(path)
            self.markets_rate.on_success()
        except HttpStatusError as exc:
            if exc.status == 429:
                self.markets_rate.on_throttle()
            else:
                self.markets_rate.on_error()
            raise
        except Exception:
            self.markets_rate.on_error()
            raise
        if not isinstance(body, list):
            raise RuntimeError("gamma_markets_payload_not_list")
        return [row for row in body if isinstance(row, dict)]

    def _fetch_events(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        path = f"/events?{urlencode(params)}"
        self.events_rate.wait_turn()
        try:
            body = self.http.get_json(path)
            self.events_rate.on_success()
        except HttpStatusError as exc:
            if exc.status == 429:
                self.events_rate.on_throttle()
            else:
                self.events_rate.on_error()
            raise
        except Exception:
            self.events_rate.on_error()
            raise
        if not isinstance(body, list):
            raise RuntimeError("gamma_events_payload_not_list")
        return [row for row in body if isinstance(row, dict)]

    def _warmup(self) -> None:
        total_changed = 0
        page_size = int(self.cfg.gamma_warmup_page_size)
        for page_idx in range(int(self.cfg.gamma_warmup_pages)):
            offset = page_idx * page_size
            rows = self._fetch_markets(
                {
                    "active": "true",
                    "limit": page_size,
                    "offset": offset,
                }
            )
            if not rows:
                break
            for row in rows:
                total_changed += self._ingest_market_row(row)
            if len(rows) < page_size:
                break
        if total_changed > 0:
            self.upserts += total_changed
        self.last_full_refresh_at = time.time()

    def _refresh_token(self, token_id: str) -> None:
        rows = self._fetch_markets({"clob_token_ids": token_id})
        changed = 0
        for row in rows:
            changed += self._ingest_market_row(row)
        if changed > 0:
            self.upserts += changed

    def _market_is_live_candidate(self, row: Dict[str, Any], now_ts: float, timeframe_minutes: int) -> bool:
        slug = str(row.get("slug") or "").strip().lower()
        slug_re = self._live_slug_re_by_tf.get(int(timeframe_minutes))
        if not slug or slug_re is None or not slug_re.match(slug):
            return False
        if not _as_bool(row.get("active")):
            return False
        if _as_bool(row.get("closed")):
            return False
        if "enableOrderBook" in row and not _as_bool(row.get("enableOrderBook")):
            return False

        start_ts = _parse_iso8601_ts(row.get("startDate"))
        end_ts = _parse_iso8601_ts(row.get("endDate"))
        grace = float(self.cfg.live_market_grace_seconds)
        if start_ts is not None and start_ts > (now_ts + grace):
            return False
        if end_ts is not None and end_ts < (now_ts - grace):
            return False
        return True

    def _probe_live_market_slugs(self, now_ts: float, timeframe_minutes: int) -> List[Dict[str, Any]]:
        interval_seconds = int(timeframe_minutes) * 60
        if interval_seconds <= 0:
            return []
        bucket = int(now_ts // interval_seconds) * interval_seconds
        span = int(self.cfg.live_market_slug_probe_span)
        out: List[Dict[str, Any]] = []
        for offset in range(-span, span + 1):
            start_bucket = bucket + (offset * interval_seconds)
            if start_bucket <= 0:
                continue
            slug = f"{self.cfg.live_market_symbol}-updown-{int(timeframe_minutes)}m-{start_bucket}"
            rows = self._fetch_markets({"slug": slug})
            for row in rows:
                out.append(row)
        return out

    @staticmethod
    def _select_live_candidate(candidates: List[Dict[str, Any]], now_ts: float) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None

        def _rank(m: Dict[str, Any]) -> tuple[int, float]:
            end_ts = _parse_iso8601_ts(m.get("endDate"))
            if end_ts is None:
                return (2, 0.0)
            if end_ts >= now_ts:
                return (0, end_ts - now_ts)
            return (1, now_ts - end_ts)

        return min(candidates, key=_rank)

    def _build_live_snapshot(self, selected: Dict[str, Any], timeframe_minutes: int) -> Dict[str, Any]:
        token_ids = [str(v).strip() for v in _parse_json_list(selected.get("clobTokenIds")) if str(v).strip()]
        outcomes = [str(v).strip() for v in _parse_json_list(selected.get("outcomes"))]
        per_token: List[Dict[str, Any]] = []
        for idx, token_id in enumerate(token_ids):
            meta = self._lookup_token_no_side_effect(token_id)
            outcome = outcomes[idx] if idx < len(outcomes) else None
            if meta is not None and not outcome:
                outcome = str(meta.get("outcome") or "").strip() or None
            per_token.append({"token_id": token_id, "outcome": outcome})

        return {
            "symbol": self.cfg.live_market_symbol,
            "timeframe_minutes": int(timeframe_minutes),
            "slug": str(selected.get("slug") or "").strip().lower() or None,
            "condition_id": str(selected.get("conditionId") or "").strip().lower() or None,
            "market_id": str(selected.get("id") or "").strip() or None,
            "start_ts": _parse_iso8601_ts(selected.get("startDate")),
            "end_ts": _parse_iso8601_ts(selected.get("endDate")),
            "detected_at_unix": round(time.time(), 6),
            "tokens": per_token,
        }

    def _refresh_live_market(self) -> None:
        now_ts = time.time()
        total_changed = 0
        timeframes = tuple(int(tf) for tf in self.cfg.live_market_timeframes_minutes)
        candidates_by_tf: Dict[int, List[Dict[str, Any]]] = {tf: [] for tf in timeframes}

        # Direct slug probes are usually enough and low-latency.
        for timeframe in timeframes:
            for row in self._probe_live_market_slugs(now_ts, timeframe):
                total_changed += self._ingest_market_row(row)
                if self._market_is_live_candidate(row, now_ts, timeframe_minutes=timeframe):
                    candidates_by_tf[timeframe].append(row)

        # Fallback: scan a small number of /events pages for any unresolved timeframe.
        unresolved = [tf for tf, rows in candidates_by_tf.items() if not rows]
        if unresolved:
            page_size = int(self.cfg.live_market_event_page_size)
            for page_idx in range(int(self.cfg.live_market_event_pages)):
                events = self._fetch_events(
                    {
                        "limit": page_size,
                        "offset": page_idx * page_size,
                        "active": "true",
                        "closed": "false",
                    }
                )
                if not events:
                    break
                for event in events:
                    markets = event.get("markets")
                    if not isinstance(markets, list):
                        continue
                    for market in markets:
                        if not isinstance(market, dict):
                            continue
                        total_changed += self._ingest_market_row(market)
                        for timeframe in unresolved:
                            if self._market_is_live_candidate(market, now_ts, timeframe_minutes=timeframe):
                                candidates_by_tf[timeframe].append(market)
                if len(events) < page_size:
                    break
                unresolved = [tf for tf, rows in candidates_by_tf.items() if not rows]
                if not unresolved:
                    break

        if total_changed > 0:
            self.upserts += total_changed

        snapshots_by_tf: Dict[int, Dict[str, Any]] = {}
        for timeframe in timeframes:
            selected = self._select_live_candidate(candidates_by_tf.get(timeframe, []), now_ts=now_ts)
            if selected is None:
                continue
            snapshots_by_tf[timeframe] = self._build_live_snapshot(selected, timeframe_minutes=timeframe)

        primary_timeframe = int(self.cfg.live_market_timeframe_minutes)
        primary_market = snapshots_by_tf.get(primary_timeframe)
        if primary_market is None and snapshots_by_tf:
            primary_market = snapshots_by_tf[min(snapshots_by_tf)]

        with self._lock:
            self._live_market = None if primary_market is None else dict(primary_market)
            self._live_markets = {tf: dict(market) for tf, market in snapshots_by_tf.items()}
        self.last_live_refresh_at = time.time()

    def _take_pending_token(self) -> Optional[str]:
        with self._lock:
            if not self._pending_tokens:
                return None
            token_id = self._pending_tokens.popleft()
            self._pending_set.discard(token_id)
            return token_id

    def live_market_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._live_market is None:
                return None
            return dict(self._live_market)

    def live_markets_snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(self._live_markets[tf]) for tf in sorted(self._live_markets)]

    def run(self) -> None:
        next_full_refresh_at = time.time()
        next_live_refresh_at = time.time()
        while not self.stop_event.is_set():
            started = time.time()
            try:
                now = time.time()
                did_work = False
                if now >= next_live_refresh_at:
                    self._refresh_live_market()
                    next_live_refresh_at = now + self.cfg.live_market_refresh_interval_seconds
                    did_work = True
                if now >= next_full_refresh_at:
                    self._warmup()
                    next_full_refresh_at = now + self.cfg.gamma_refresh_interval_seconds
                    did_work = True
                token_id = self._take_pending_token()
                if token_id:
                    self._refresh_token(token_id)
                    did_work = True
                if not did_work:
                    self.stop_event.wait(0.05)

                self.last_poll_rtt = time.time() - started
                self.polls += 1
            except Exception:
                self.errors += 1
                self.stop_event.wait(0.1)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            token_count = len(self._token_meta)
            pending_count = len(self._pending_tokens)
            live_market = None if self._live_market is None else dict(self._live_market)
            live_markets = [dict(self._live_markets[tf]) for tf in sorted(self._live_markets)]
        return {
            "source": "gamma_market_cache",
            "markets_target_rps": round(self.cfg.gamma_target_rps, 3),
            "markets_rate": self.markets_rate.snapshot(),
            "events_target_rps": round(self.cfg.gamma_events_target_rps, 3),
            "events_rate": self.events_rate.snapshot(),
            "polls": self.polls,
            "errors": self.errors,
            "upserts": self.upserts,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "inline_fetches": self.inline_fetches,
            "token_count": token_count,
            "pending_tokens": pending_count,
            "last_poll_rtt_seconds": None if self.last_poll_rtt is None else round(self.last_poll_rtt, 4),
            "last_full_refresh_at_unix": None
            if self.last_full_refresh_at is None
            else round(self.last_full_refresh_at, 6),
            "last_live_refresh_at_unix": None
            if self.last_live_refresh_at is None
            else round(self.last_live_refresh_at, 6),
            "live_market": live_market,
            "live_markets": live_markets,
        }

    def close(self) -> None:
        self.http.close()
