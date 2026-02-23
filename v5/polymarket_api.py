from __future__ import annotations

import json
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
USER_AGENT = "poly-v5-investigator/1.0"


class ApiError(RuntimeError):
    """Raised when a request cannot be completed successfully."""


class RateLimiter:
    """Thread-safe fixed-interval rate limiter."""

    def __init__(self, rps: float) -> None:
        self._interval = 0.0 if rps <= 0 else 1.0 / rps
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self._interval <= 0:
            return
        delay = 0.0
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                delay = self._next_allowed - now
                now = self._next_allowed
            self._next_allowed = now + self._interval
        if delay > 0:
            time.sleep(delay)


@dataclass(slots=True)
class PolymarketApi:
    timeout_seconds: float = 20.0
    retries: int = 4
    rps: float = 10.0
    _rate: RateLimiter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rate = RateLimiter(self.rps)

    def _request_json(self, url: str) -> Any:
        last_err: Exception | None = None
        for attempt in range(self.retries + 1):
            self._rate.wait()
            req = Request(url, headers={"User-Agent": USER_AGENT})
            try:
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    payload = resp.read().decode("utf-8")
                return json.loads(payload)
            except HTTPError as err:
                last_err = err
                status = int(getattr(err, "code", 0) or 0)
                if status == 404:
                    return None
                retryable = status == 429 or 500 <= status <= 599
                if not retryable or attempt >= self.retries:
                    body = ""
                    try:
                        body = err.read().decode("utf-8", errors="ignore")
                    except Exception:
                        body = ""
                    raise ApiError(f"http_error status={status} url={url} body={body[:500]}") from err
                self._sleep_backoff(attempt)
            except (URLError, TimeoutError, json.JSONDecodeError) as err:
                last_err = err
                if attempt >= self.retries:
                    raise ApiError(f"request_failed url={url} error={err}") from err
                self._sleep_backoff(attempt)
        raise ApiError(f"request_failed url={url} error={last_err}")

    @staticmethod
    def _sleep_backoff(attempt: int) -> None:
        base = min(2.0, 0.25 * (2**attempt))
        jitter = random.uniform(0.0, 0.2)
        time.sleep(base + jitter)

    @staticmethod
    def _build_url(base: str, path: str, params: dict[str, Any] | None = None) -> str:
        if not params:
            return f"{base}{path}"
        qs = urlencode({k: v for k, v in params.items() if v is not None})
        return f"{base}{path}?{qs}"

    def get_leaderboard(
        self,
        *,
        category: str,
        time_period: str,
        order_by: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        url = self._build_url(
            DATA_API_BASE,
            "/v1/leaderboard",
            {
                "category": category,
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": int(limit),
                "offset": int(offset),
            },
        )
        rows = self._request_json(url)
        return rows if isinstance(rows, list) else []

    def get_trades(
        self,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        url = self._build_url(
            DATA_API_BASE,
            "/trades",
            {"limit": int(limit), "offset": int(offset)},
        )
        rows = self._request_json(url)
        return rows if isinstance(rows, list) else []

    def get_activity(
        self,
        *,
        user: str,
        limit: int,
        offset: int,
        activity_type: str = "TRADE",
    ) -> list[dict[str, Any]]:
        url = self._build_url(
            DATA_API_BASE,
            "/activity",
            {
                "user": user,
                "type": activity_type,
                "limit": int(limit),
                "offset": int(offset),
            },
        )
        rows = self._request_json(url)
        return rows if isinstance(rows, list) else []

    def get_closed_positions(
        self,
        *,
        user: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        url = self._build_url(
            DATA_API_BASE,
            "/closed-positions",
            {
                "user": user,
                "limit": int(limit),
                "offset": int(offset),
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
        )
        rows = self._request_json(url)
        return rows if isinstance(rows, list) else []

    def get_public_profile(self, *, address: str) -> dict[str, Any] | None:
        url = self._build_url(
            GAMMA_API_BASE,
            "/public-profile",
            {"address": address},
        )
        row = self._request_json(url)
        if isinstance(row, dict):
            return row
        return None
