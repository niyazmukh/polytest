"""Unified service: detector + buy + redeem in one process.

Runs HybridDetector, BuyExecutor, and (optionally) RedeemWorker sharing
a single ``stop_event`` and shared rate gates.  SIGTERM triggers graceful
shutdown of all components.

Usage (standalone)::

    python -m v2_hybrid_signals.run_service [--env-file .env.v2.local] [--dry-run]

Or via systemd — see deploy/v2-signals.service.
"""
from __future__ import annotations

import argparse
import json
from queue import Empty, Full, Queue
import signal as _signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from .buy_config import parse_args as _parse_buy_args
from .buy_executor import BuyExecutor
from .config import parse_args as _parse_detector_args
from .hybrid_detector import HybridDetector
from .price_feed import MarketPriceFeed
from .redeem_config import parse_args as _parse_redeem_args
from .redeem_worker import RedeemWorker
from .utils import safe_stdout_flush


# ------------------------------------------------------------------
# Helpers (duplicated from buy_main to avoid importing private names)
# ------------------------------------------------------------------

def _emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    safe_stdout_flush()


def _extract_live_markets(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    gamma = payload.get("gamma_market_cache")
    if isinstance(gamma, dict):
        live_markets = gamma.get("live_markets")
        if isinstance(live_markets, list):
            rows = [row for row in live_markets if isinstance(row, dict)]
            if rows:
                return rows
        live = gamma.get("live_market")
        if isinstance(live, dict):
            return [live]
    return []


def _prewarm_live_markets(
    executor: BuyExecutor,
    feed: MarketPriceFeed,
    live_markets: List[Dict[str, Any]],
) -> None:
    executor.prefetch_market_tokens(live_markets)
    for live_market in live_markets:
        tokens = live_market.get("tokens")
        if not isinstance(tokens, list):
            continue
        for row in tokens:
            token_id = ""
            if isinstance(row, dict):
                token_id = str(row.get("token_id") or "").strip()
            elif row is not None:
                token_id = str(row).strip()
            if not token_id:
                continue
            feed.track_token(token_id)


# ------------------------------------------------------------------
# ServiceRunner
# ------------------------------------------------------------------

class ServiceRunner:
    """Runs detector + buy engine + redeem worker in a single process."""

    def __init__(
        self,
        *,
        env_file: Optional[str] = None,
        dry_run: Optional[bool] = None,
        runtime_seconds: Optional[float] = None,
    ) -> None:
        self.stop_event = threading.Event()
        self._live_markets_lock = threading.Lock()
        self.live_markets: List[Dict[str, Any]] = []
        self._actionable_queue: "Queue[Dict[str, Any]]" = Queue(maxsize=20_000)
        self._buy_worker: Optional[threading.Thread] = None
        self.actionable_enqueued = 0
        self.actionable_processed = 0
        self.actionable_dropped = 0
        self.actionable_errors = 0
        self.redeem_disabled_reason: Optional[str] = None

        # Build argv lists for each sub-parser (env-driven, no sys.argv).
        det_argv: list[str] = []
        buy_argv: list[str] = []
        redeem_argv: list[str] = []

        if env_file is not None:
            for av in (det_argv, buy_argv, redeem_argv):
                av.extend(["--env-file", env_file])
        if dry_run is True:
            buy_argv.append("--dry-run")
            redeem_argv.append("--dry-run")
        elif dry_run is False:
            buy_argv.append("--no-dry-run")
            redeem_argv.append("--no-dry-run")
        if runtime_seconds is not None:
            det_argv.extend(["--runtime-seconds", str(runtime_seconds)])

        # Parse configs from env only (no sys.argv leakage).
        self.detector_cfg = _parse_detector_args(det_argv)
        self.buy_cfg = _parse_buy_args(buy_argv)

        # Buy engine (price feed + executor).
        self.price_feed = MarketPriceFeed(cfg=self.buy_cfg, stop_event=self.stop_event)
        self.buy_executor = BuyExecutor(cfg=self.buy_cfg, price_feed=self.price_feed)

        # Detector with shared stop_event and in-process signal callback.
        self.detector = HybridDetector(
            self.detector_cfg,
            signal_callback=self._on_signal,
            stop_event=self.stop_event,
        )

        # Redeem worker (optional — requires wallet + API credentials).
        self.redeem_worker: Optional[RedeemWorker] = None
        try:
            redeem_cfg = _parse_redeem_args(redeem_argv)
            if redeem_cfg.redeem_wallet:
                self.redeem_worker = RedeemWorker(redeem_cfg)
            else:
                self.redeem_disabled_reason = "redeem_wallet_empty"
        except SystemExit as exc:
            text = str(exc).strip()
            self.redeem_disabled_reason = text or "redeem_parse_error"
        except Exception as exc:
            self.redeem_disabled_reason = str(exc) or "redeem_init_error"

    # ------------------------------------------------------------------
    # Signal callback wired to HybridDetector._emit
    # ------------------------------------------------------------------

    def _on_signal(self, payload: Dict[str, Any]) -> None:
        """Route detector events to the buy engine in-process."""
        event = str(payload.get("event") or "")

        if event in ("HYBRID_PROGRESS", "HYBRID_SUMMARY"):
            next_live = _extract_live_markets(payload)
            if next_live:
                with self._live_markets_lock:
                    self.live_markets = next_live
                _prewarm_live_markets(
                    executor=self.buy_executor,
                    feed=self.price_feed,
                    live_markets=next_live,
                )
            return

        if event != "ACTIONABLE_SIGNAL":
            return

        # Track the token in the price feed immediately; enqueue execution work.
        signal_data = payload.get("signal")
        if isinstance(signal_data, dict):
            token_id = str(signal_data.get("token_id") or "").strip()
            if token_id:
                self.price_feed.track_token(token_id)

        try:
            self._actionable_queue.put_nowait(payload)
            self.actionable_enqueued += 1
        except Full:
            self.actionable_dropped += 1
            _emit(
                {
                    "event": "BUY_ENGINE_QUEUE_DROP",
                    "received_at_unix": round(time.time(), 6),
                    "actionable_source": payload.get("source"),
                    "match_key": payload.get("match_key"),
                    "tracked_user": payload.get("tracked_user"),
                    "tx_hash": payload.get("tx_hash"),
                    "event_ts": payload.get("event_ts"),
                    "reason": "buy_actionable_queue_full",
                }
            )

    def _buy_worker_loop(self) -> None:
        while not self.stop_event.is_set() or not self._actionable_queue.empty():
            try:
                payload = self._actionable_queue.get(timeout=0.1)
            except Empty:
                continue

            try:
                with self._live_markets_lock:
                    live_markets = list(self.live_markets)
                decision = self.buy_executor.process_actionable(payload, live_markets=live_markets)
                self.actionable_processed += 1
                _emit(
                    {
                        "event": "BUY_ENGINE_DECISION",
                        "received_at_unix": round(time.time(), 6),
                        "actionable_source": payload.get("source"),
                        "match_key": payload.get("match_key"),
                        "tracked_user": payload.get("tracked_user"),
                        "tx_hash": payload.get("tx_hash"),
                        "event_ts": payload.get("event_ts"),
                        "decision": decision,
                    }
                )
            except Exception as exc:
                self.actionable_errors += 1
                _emit({"event": "BUY_ENGINE_FATAL", "error": str(exc), "where": "run_service_buy_worker"})
            finally:
                self._actionable_queue.task_done()

    # ------------------------------------------------------------------
    # Background threads
    # ------------------------------------------------------------------

    def _run_redeem(self) -> None:
        if self.redeem_worker is None:
            return
        try:
            self.redeem_worker.run(_emit)
        except Exception as exc:
            _emit({"event": "REDEEM_FATAL", "error": str(exc)})

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> int:
        # Signal handlers (main thread only).
        _signal.signal(_signal.SIGTERM, lambda *_: self.stop_event.set())
        _signal.signal(_signal.SIGINT, lambda *_: self.stop_event.set())

        _emit({
            "event": "SERVICE_START",
            "tracked_wallets": list(self.detector_cfg.users),
            "buy_dry_run": self.buy_cfg.dry_run,
            "has_redeem": self.redeem_worker is not None,
            "redeem_disabled_reason": self.redeem_disabled_reason,
            "runtime_seconds": self.detector_cfg.runtime_seconds,
        })
        if self.redeem_worker is None and self.redeem_disabled_reason:
            _emit({"event": "REDEEM_DISABLED", "reason": self.redeem_disabled_reason})

        # Start price feed (WebSocket + REST book polling).
        self.price_feed.start()
        self._buy_worker = threading.Thread(
            target=self._buy_worker_loop,
            name="buy-worker",
            daemon=True,
        )
        self._buy_worker.start()

        # Start redeem in background daemon thread (if configured).
        redeem_thread: Optional[threading.Thread] = None
        if self.redeem_worker is not None:
            redeem_thread = threading.Thread(
                target=self._run_redeem, name="redeem-worker", daemon=True,
            )
            redeem_thread.start()

        # Run detector — blocks main thread until stop_event or deadline.
        exit_code = 1
        try:
            exit_code = self.detector.run()
        finally:
            self.stop_event.set()
            if self._buy_worker is not None:
                self._buy_worker.join(timeout=10.0)
            self.price_feed.join(timeout=3.0)
            if self.redeem_worker is not None:
                self.redeem_worker.close()
            if redeem_thread is not None:
                redeem_thread.join(timeout=5.0)
            _emit({
                "event": "SERVICE_STOP",
                "exit_code": exit_code,
                "buy_executor": self.buy_executor.snapshot(),
                "price_feed": self.price_feed.snapshot(),
                "buy_queue": {
                    "enqueued": self.actionable_enqueued,
                    "processed": self.actionable_processed,
                    "dropped": self.actionable_dropped,
                    "errors": self.actionable_errors,
                    "pending": self._actionable_queue.qsize(),
                },
                "redeem_disabled_reason": self.redeem_disabled_reason,
            })

        return exit_code


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified signal service (detector + buy + redeem)",
    )
    parser.add_argument(
        "--env-file", default=None,
        help="Path to .env file (default: .env.v2.local)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Enable dry-run mode for buy/redeem",
    )
    parser.add_argument(
        "--no-dry-run", action="store_false", dest="dry_run",
        help="Disable dry-run mode",
    )
    parser.add_argument(
        "--runtime-seconds", type=float, default=None,
        help="Stop after N seconds (default: run forever)",
    )
    args = parser.parse_args()

    runner = ServiceRunner(
        env_file=args.env_file,
        dry_run=args.dry_run,
        runtime_seconds=args.runtime_seconds,
    )
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
