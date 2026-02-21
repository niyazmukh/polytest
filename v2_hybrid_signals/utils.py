from __future__ import annotations

import datetime as dt
import sys
from typing import Any, Optional


def safe_stdout_flush() -> None:
    try:
        sys.stdout.flush()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Generic coercion helpers — consolidated from module-level duplicates in
# buy_executor, price_feed, redeem_worker, sources, and market_cache.
# ---------------------------------------------------------------------------


def as_float(raw: Any) -> Optional[float]:
    """Coerce *raw* to ``float``, returning ``None`` on failure."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def as_bool(raw: Any) -> bool:
    """Coerce *raw* to ``bool`` using common truthy strings."""
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    text = str(raw).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def as_int(raw: Any) -> Optional[int]:
    """Coerce *raw* to ``int``, returning ``None`` on failure."""
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def parse_iso8601_ts(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        # Support trailing Z and offsets.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        value = dt.datetime.fromisoformat(text)
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return float(value.timestamp())
    except Exception:
        return None


def parse_unixish(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError:
            iso = parse_iso8601_ts(text)
            return iso

    # Milliseconds to seconds normalization.
    if value > 1e12:
        value = value / 1000.0
    return value

