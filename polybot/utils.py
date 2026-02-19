import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def normalize_unix(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    try:
        value = int(ts)
    except (TypeError, ValueError):
        return None
    if value > 10_000_000_000:
        value = int(value / 1000)
    return value


def parse_unixish(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
        if value > 10_000_000_000:
            value /= 1000.0
        return value
    text = str(raw).strip()
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if value > 10_000_000_000:
        value /= 1000.0
    return value


def parse_iso8601_ts(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def iso_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def make_row_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("transactionHash", "")),
            str(row.get("timestamp", "")),
            str(row.get("type", "")),
            str(row.get("asset", "")),
            str(row.get("side", "")),
            str(row.get("size", "")),
            str(row.get("price", "")),
            str(row.get("slug", "")),
        ]
    )


def compact_activity_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tx": row.get("transactionHash"),
        "timestamp": normalize_unix(row.get("timestamp")),
        "type": row.get("type"),
        "side": row.get("side"),
        "asset": row.get("asset"),
        "price": row.get("price"),
        "size": row.get("size"),
        "usdcSize": row.get("usdcSize"),
        "slug": row.get("slug"),
        "outcome": row.get("outcome"),
        "proxyWallet": row.get("proxyWallet"),
    }


def safe_stdout_flush() -> None:
    stream = sys.stdout
    if stream is None:
        return
    try:
        stream.flush()
    except Exception:
        return
