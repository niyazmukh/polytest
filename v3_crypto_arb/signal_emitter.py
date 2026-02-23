"""Signal emitter — writes JSON signals to stdout.

Each signal is one JSON line (JSONL format), compatible with
downstream consumers (buy_executor, log analysis, etc.).
"""
from __future__ import annotations

import json
import logging
import sys
from typing import TextIO

from .models import ArbSignal

log = logging.getLogger(__name__)


class SignalEmitter:
    """Writes ArbSignal objects as JSON lines to a stream.

    Default stream is stdout. Can be redirected to a file
    or piped to another process.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream or sys.stdout
        self._count = 0

    @property
    def signal_count(self) -> int:
        return self._count

    def emit(self, signal: ArbSignal) -> None:
        """Write signal as JSON line."""
        try:
            line = json.dumps(signal.to_dict(), separators=(",", ":"))
            self._stream.write(line + "\n")
            self._stream.flush()
            self._count += 1
        except Exception:
            log.exception("signal emit error")
