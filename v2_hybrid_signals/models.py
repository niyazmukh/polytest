from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(slots=True)
class SourceSignal:
    tracked_user: str
    source: str
    tx_hash: str
    event_ts: int
    seen_at: float
    payload: Dict[str, Any]
