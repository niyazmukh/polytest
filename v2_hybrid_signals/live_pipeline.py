from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO, TextIO

_STDOUT_LOCK = threading.Lock()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run detector -> buy -> redeem pipeline with tee logs.")
    parser.add_argument("--wallet", required=True, help="single tracked wallet")
    parser.add_argument("--runtime-seconds", type=float, default=600.0)
    parser.add_argument("--progress-interval-seconds", type=float, default=10.0)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--redeem",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run redeem worker in parallel with detector/buy",
    )
    parser.add_argument("--output-dir", default="v2_hybrid_signals")
    parser.add_argument("--label", default="")
    return parser.parse_args()


def _pump_stdout(handle: TextIO, log_handle: TextIO) -> None:
    for line in handle:
        log_handle.write(line)
        log_handle.flush()
        with _STDOUT_LOCK:
            sys.stdout.write(line)
            sys.stdout.flush()


def _terminate(proc: subprocess.Popen[str] | None) -> None:
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
    except Exception:
        pass


def _safe_close(handle: IO[str] | TextIO | None) -> None:
    if handle is None:
        return
    try:
        handle.close()
    except Exception:
        pass


def main() -> int:
    args = _parse_args()
    wallet = str(args.wallet).strip().lower()
    if not wallet:
        raise SystemExit("wallet is required")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    label = f"_{args.label.strip()}" if str(args.label).strip() else ""

    detector_log_path = output_dir / f"live_pipeline_{ts}{label}_detector.jsonl"
    buy_log_path = output_dir / f"live_pipeline_{ts}{label}_buy.jsonl"
    redeem_log_path = output_dir / f"live_pipeline_{ts}{label}_redeem.jsonl"
    detector_err_path = output_dir / f"live_pipeline_{ts}{label}_detector.err"
    buy_err_path = output_dir / f"live_pipeline_{ts}{label}_buy.err"
    redeem_err_path = output_dir / f"live_pipeline_{ts}{label}_redeem.err"
    meta_path = output_dir / f"live_pipeline_{ts}{label}_meta.json"

    py = sys.executable
    detector_cmd = [
        py,
        "-m",
        "v2_hybrid_signals.main",
        "--user",
        wallet,
        "--users",
        wallet,
        "--runtime-seconds",
        str(float(args.runtime_seconds)),
        "--progress-interval-seconds",
        str(float(args.progress_interval_seconds)),
    ]
    buy_cmd = [py, "-m", "v2_hybrid_signals.buy_main", "--dry-run" if args.dry_run else "--no-dry-run"]
    redeem_cmd = [
        py,
        "-m",
        "v2_hybrid_signals.redeem_main",
        "--dry-run" if args.dry_run else "--no-dry-run",
        "--runtime-seconds",
        str(float(args.runtime_seconds)),
    ]

    meta_path.write_text(
        json.dumps(
            {
                "wallet": wallet,
                "runtime_seconds": float(args.runtime_seconds),
                "progress_interval_seconds": float(args.progress_interval_seconds),
                "dry_run": bool(args.dry_run),
                "redeem_enabled": bool(args.redeem),
                "detector_cmd": detector_cmd,
                "buy_cmd": buy_cmd,
                "redeem_cmd": redeem_cmd if args.redeem else None,
                "detector_log": str(detector_log_path),
                "buy_log": str(buy_log_path),
                "redeem_log": str(redeem_log_path) if args.redeem else None,
                "detector_err": str(detector_err_path),
                "buy_err": str(buy_err_path),
                "redeem_err": str(redeem_err_path) if args.redeem else None,
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    with ExitStack() as stack:
        detector_log = stack.enter_context(detector_log_path.open("w", encoding="utf-8", newline=""))
        buy_log = stack.enter_context(buy_log_path.open("w", encoding="utf-8", newline=""))
        detector_err = stack.enter_context(detector_err_path.open("w", encoding="utf-8", newline=""))
        buy_err = stack.enter_context(buy_err_path.open("w", encoding="utf-8", newline=""))
        redeem_log: TextIO | None = None
        redeem_err: TextIO | None = None
        if args.redeem:
            redeem_log = stack.enter_context(redeem_log_path.open("w", encoding="utf-8", newline=""))
            redeem_err = stack.enter_context(redeem_err_path.open("w", encoding="utf-8", newline=""))

        detector = subprocess.Popen(
            detector_cmd,
            stdout=subprocess.PIPE,
            stderr=detector_err,
            text=True,
            bufsize=1,
        )
        buy = subprocess.Popen(
            buy_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=buy_err,
            text=True,
            bufsize=1,
        )
        redeem: subprocess.Popen[str] | None = None
        if args.redeem:
            redeem = subprocess.Popen(
                redeem_cmd,
                stdout=subprocess.PIPE,
                stderr=redeem_err,
                text=True,
                bufsize=1,
            )

        assert detector.stdout is not None
        assert buy.stdin is not None
        assert buy.stdout is not None

        buy_thread = threading.Thread(
            target=_pump_stdout,
            args=(buy.stdout, buy_log),
            name="buy-stdout-pump",
            daemon=True,
        )
        buy_thread.start()

        redeem_thread: threading.Thread | None = None
        if redeem is not None:
            assert redeem.stdout is not None
            assert redeem_log is not None
            redeem_thread = threading.Thread(
                target=_pump_stdout,
                args=(redeem.stdout, redeem_log),
                name="redeem-stdout-pump",
                daemon=True,
            )
            redeem_thread.start()

        try:
            for line in detector.stdout:
                detector_log.write(line)
                detector_log.flush()
                try:
                    buy.stdin.write(line)
                    buy.stdin.flush()
                except BrokenPipeError:
                    _terminate(detector)
                    _terminate(redeem)
                    break
        except KeyboardInterrupt:
            _terminate(detector)
            _terminate(buy)
            _terminate(redeem)
            raise
        finally:
            _safe_close(buy.stdin)
            _safe_close(detector.stdout)
            if redeem is not None:
                _safe_close(redeem.stdout)

        det_rc = detector.wait()
        buy_rc = buy.wait()
        redeem_rc = 0
        redeem_stopped_by_pipeline = False
        if redeem is not None:
            if redeem.poll() is None:
                redeem_stopped_by_pipeline = True
                _terminate(redeem)
                try:
                    redeem.wait(timeout=10.0)
                except subprocess.TimeoutExpired:
                    try:
                        redeem.kill()
                    except Exception:
                        pass
                    redeem.wait()
            if redeem.returncode is not None:
                redeem_rc = int(redeem.returncode)
        buy_thread.join(timeout=2.0)
        if redeem_thread is not None:
            redeem_thread.join(timeout=2.0)

    redeem_ok = redeem_rc == 0 or redeem_stopped_by_pipeline
    return 0 if det_rc == 0 and buy_rc == 0 and redeem_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
