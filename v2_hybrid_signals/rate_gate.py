from __future__ import annotations

import threading
import time


class RateGate:
    """Thread-safe adaptive pacer with throttle backoff + slow recovery."""

    def __init__(
        self,
        target_rps: float,
        min_factor: float = 0.2,
        max_factor: float = 1.0,
        recover_step: float = 0.01,
        throttle_mult: float = 0.7,
        error_mult: float = 0.9,
    ) -> None:
        self.target_rps = max(0.01, float(target_rps))
        self.min_factor = min(1.0, max(0.01, float(min_factor)))
        self.max_factor = max(self.min_factor, float(max_factor))
        self.recover_step = max(0.0001, float(recover_step))
        self.throttle_mult = min(0.99, max(0.01, float(throttle_mult)))
        self.error_mult = min(0.999, max(0.01, float(error_mult)))
        self._factor = self.max_factor
        self._lock = threading.Lock()
        self._next_at = time.monotonic()

    def _effective_interval(self) -> float:
        eff_rps = max(0.01, self.target_rps * self._factor)
        return 1.0 / eff_rps

    def wait_turn(self) -> None:
        with self._lock:
            now = time.monotonic()
            interval = self._effective_interval()
            if now < self._next_at:
                sleep_for = self._next_at - now
                self._next_at += interval
            else:
                sleep_for = 0.0
                self._next_at = now + interval
        if sleep_for > 0:
            time.sleep(sleep_for)

    def on_success(self) -> None:
        with self._lock:
            self._factor = min(self.max_factor, self._factor + self.recover_step)

    def on_throttle(self) -> None:
        with self._lock:
            self._factor = max(self.min_factor, self._factor * self.throttle_mult)

    def on_error(self) -> None:
        with self._lock:
            self._factor = max(self.min_factor, self._factor * self.error_mult)

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            eff_rps = max(0.01, self.target_rps * self._factor)
            return {
                "target_rps": round(self.target_rps, 6),
                "effective_rps": round(eff_rps, 6),
                "throttle_factor": round(self._factor, 6),
            }
