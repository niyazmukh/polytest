import json
import sys
import time
from socket import timeout as socket_timeout
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .constants import DATA_API_BASE, HTTP_RETRIES
from .utils import normalize_unix


def build_activity_url(
    user: str,
    limit: int,
    offset: int,
    start: int,
    end: int,
    event_type: Optional[str],
    side: Optional[str],
) -> str:
    params = {
        "user": user,
        "limit": max(1, min(limit, 500)),
        "offset": max(0, offset),
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
        "start": int(start),
        "end": int(end),
    }
    if event_type:
        params["type"] = event_type
    if side:
        params["side"] = side
    return f"{DATA_API_BASE}/activity?{urlencode(params)}"


def fetch_activity_page(
    user: str,
    limit: int,
    offset: int,
    start: int,
    end: int,
    event_type: Optional[str],
    side: Optional[str],
) -> List[Dict[str, Any]]:
    url = build_activity_url(
        user=user,
        limit=limit,
        offset=offset,
        start=start,
        end=end,
        event_type=event_type,
        side=side,
    )
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "poly-user-activity-listener/1.0"})

    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES):
        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if not isinstance(data, list):
                raise ValueError("Unexpected response format from Data API: expected JSON list")
            return data
        except (socket_timeout, TimeoutError) as exc:
            last_error = exc
            if attempt < HTTP_RETRIES - 1:
                time.sleep(0.25 * (attempt + 1))
                continue
            raise

    if last_error:
        raise last_error
    return []


def fetch_activity_window(
    user: str,
    start: int,
    end: int,
    page_size: int,
    event_type: Optional[str],
    side: Optional[str],
    stop_before_ts: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    should_stop = False

    while True:
        try:
            page = fetch_activity_page(
                user=user,
                limit=page_size,
                offset=offset,
                start=start,
                end=end,
                event_type=event_type,
                side=side,
            )
        except HTTPError as err:
            if err.code == 400 and offset > 0:
                print(
                    f"Pagination boundary reached for window {start}-{end} at offset={offset}. Returning partial window.",
                    file=sys.stderr,
                )
                break
            raise

        if not page:
            break

        if stop_before_ts is None:
            rows.extend(page)
        else:
            for row in page:
                ts = normalize_unix(row.get("timestamp"))
                if ts is not None and ts < stop_before_ts:
                    should_stop = True
                    break
                rows.append(row)

        if should_stop:
            break

        if len(page) < page_size:
            break
        offset += page_size

    return rows


def fetch_portfolio_value(user: str) -> Optional[float]:
    if not user:
        return None
    url = f"{DATA_API_BASE}/value?{urlencode({'user': user})}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "poly-user-activity-listener/1.0"})

    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES):
        try:
            with urlopen(request, timeout=10) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if isinstance(data, list):
                if not data:
                    return None
                first = data[0]
                if isinstance(first, dict) and first.get("value") is not None:
                    return float(first.get("value"))
                if isinstance(first, (int, float)):
                    return float(first)
            if isinstance(data, dict) and data.get("value") is not None:
                return float(data.get("value"))
            if isinstance(data, (int, float)):
                return float(data)
            return None
        except (socket_timeout, TimeoutError, ValueError, TypeError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < HTTP_RETRIES - 1:
                time.sleep(0.25 * (attempt + 1))
                continue
        except Exception as exc:
            last_error = exc
            if attempt < HTTP_RETRIES - 1:
                time.sleep(0.25 * (attempt + 1))
                continue

    if last_error is not None:
        print(f"portfolio guard value fetch failed for user={user}: {last_error}", file=sys.stderr)
    return None


def fetch_positions_snapshot(user: str, limit: int = 500, size_threshold: float = 0.0) -> List[Dict[str, Any]]:
    if not user:
        return []

    bounded_limit = max(1, min(int(limit), 500))
    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "user": user,
            "limit": bounded_limit,
            "offset": offset,
            "sizeThreshold": size_threshold,
        }
        url = f"{DATA_API_BASE}/positions?{urlencode(params)}"
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "poly-user-activity-listener/1.0"})

        page: Optional[List[Dict[str, Any]]] = None
        last_error: Optional[Exception] = None
        for attempt in range(HTTP_RETRIES):
            try:
                with urlopen(request, timeout=10) as response:
                    payload = response.read().decode("utf-8")
                data = json.loads(payload)
                if not isinstance(data, list):
                    raise ValueError("Unexpected response format from Data API positions: expected JSON list")
                page = data
                break
            except (socket_timeout, TimeoutError, ValueError, TypeError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < HTTP_RETRIES - 1:
                    time.sleep(0.25 * (attempt + 1))
                    continue
            except Exception as exc:
                last_error = exc
                if attempt < HTTP_RETRIES - 1:
                    time.sleep(0.25 * (attempt + 1))
                    continue

        if page is None:
            if last_error is not None:
                raise last_error
            break

        rows.extend(page)
        if len(page) < bounded_limit:
            break
        offset += bounded_limit

    return rows
