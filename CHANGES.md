# Change Summary — v2_hybrid_signals Production Hardening

**Date**: 2026-02-21  
**Scope**: `v2_hybrid_signals/` package + `tests/` + `deploy/`  
**Validation**: pyright 0 errors, pytest 16/16 pass

---

## 1. Critical Bugfix: Shared Rate Gates

**Problem**: `HybridDetector.__init__` created per-wallet `DataApiActivitySource` and `OrderbookSubgraphSource` threads without passing a shared `RateGate`. Each source constructed its own internal `RateGate`, so N tracked wallets produced N× the intended API request rate — triggering 429 throttles at scale.

**Fix** (`hybrid_detector.py`):
- Added `from .rate_gate import RateGate` import.
- Created two shared instances in `__init__`: `self._activity_rate = RateGate(cfg.activity_target_rps)` and `self._subgraph_rate = RateGate(cfg.subgraph_target_rps)`.
- Passed `rate=self._activity_rate` to every `DataApiActivitySource` and `rate=self._subgraph_rate` to every `OrderbookSubgraphSource`.
- The sources already accepted an optional `rate` parameter — this change simply wires it.

**Test coverage**: `TestSharedRateGates` (3 tests) — verifies all activity sources share one gate, all subgraph sources share another, and the two gates are distinct objects.

---

## 2. Signal Callback + External Stop Event

**Problem**: `HybridDetector` created its own `stop_event` internally and emitted signals only to stdout via `@staticmethod _emit`. There was no way to wire the detector into another component in-process (e.g., a buy engine) without subprocess pipes.

**Fix** (`hybrid_detector.py`):
- `__init__` now accepts `signal_callback: Optional[Callable[[Dict[str, Any]], None]] = None` and `stop_event: Optional[threading.Event] = None` as keyword-only arguments.
- `self.stop_event = stop_event or threading.Event()` — callers can share a single event across components.
- `_emit` is no longer `@staticmethod`. After writing to stdout, it calls `self._signal_callback(payload)` if set (wrapped in bare `except Exception: pass` to prevent callback errors from crashing the detector loop).
- Added `Optional, Callable` to typing imports.

**Test coverage**: `TestSignalCallback` (2 tests) — verifies callback fires on ACTIONABLE_SIGNAL and does not fire for non-actionable payloads. `TestStopEvent` (2 tests) — verifies external event is propagated to market_cache and all sources, and that a default event is created when none is provided.

---

## 3. Programmatic Config (argv parameter)

**Problem**: All three `parse_args()` functions consumed `sys.argv` unconditionally. When creating configs programmatically (e.g., from a unified orchestrator), `sys.argv` would leak unrelated CLI flags.

**Fix** (`config.py`, `buy_config.py`, `redeem_config.py`):
- Added type alias `_Argv = Optional[list[str]]` to each module.
- Changed signature from `def parse_args() -> XConfig` to `def parse_args(argv: _Argv = None) -> XConfig`.
- Passed `argv` to both `pre.parse_known_args(argv)` and `parser.parse_args(argv)`.
- When `argv` is `None` (default), argparse reads `sys.argv` as before — fully backward-compatible.
- When `argv=[]`, config is driven entirely from environment variables.

**Test coverage**: `TestConfigArgv` (4 tests) — verifies env-driven config, multi-wallet parsing, dry-run defaults, and CLI override.

---

## 4. Prune TTL Fix for Long-Running Service

**Problem**: `_prune_stale` computed TTL as `(runtime_seconds or 600) * 2`. When `runtime_seconds=None` (run forever), the fallback was 1200s — but this coupled pruning to an unrelated config value. If someone set `runtime_seconds=60` for a test, stale entries would be pruned after only 120s in production.

**Fix** (`hybrid_detector.py`):
- Replaced `_MATCH_TTL_MULTIPLIER = 2.0` and `_DEFAULT_MATCH_TTL_S = 600.0` with a single `_MATCH_TTL_S = 600.0`.
- TTL is now a fixed 600s regardless of `runtime_seconds`.

**Test coverage**: `TestPruneStale` (2 tests) — verifies old entries are removed, recent ones kept, and `tx_keys` cap at 200K triggers clear.

---

## 5. SIGTERM Handlers on All Entry Points

**Problem**: Default SIGTERM behavior kills the process immediately without thread cleanup — problematic for systemd's `KillSignal=SIGTERM`.

**Fix**:
- `main.py`: `signal.signal(signal.SIGTERM, lambda *_: detector.stop_event.set())` after constructing the detector.
- `buy_main.py`: `signal.signal(signal.SIGTERM, lambda *_: stop_event.set())` at the top of `run()`.
- `redeem_main.py`: `signal.signal(signal.SIGTERM, lambda *_: worker.close())` after constructing the worker.
- All three files: added `import signal as _signal` (aliased to avoid shadowing variables named `signal`).

---

## 6. Unified Orchestrator: `run_service.py` (new file)

**Purpose**: Replaces `live_pipeline.py` (subprocess pipe approach) with an in-process architecture. Detector, buy engine, and redeem worker share one `stop_event` and communicate via the `signal_callback` mechanism — no stdin/stdout pipes.

**Architecture**:
```
ServiceRunner.__init__:
  - Parses detector/buy/redeem configs from env (using argv=[])
  - Creates shared stop_event
  - Creates MarketPriceFeed + BuyExecutor
  - Creates HybridDetector with signal_callback=self._on_signal, stop_event=shared
  - Optionally creates RedeemWorker (skipped silently if not configured)

ServiceRunner._on_signal(payload):
  - Routes HYBRID_PROGRESS/HYBRID_SUMMARY → _prewarm_live_markets
  - Routes ACTIONABLE_SIGNAL → BuyExecutor.process_actionable()
  - Emits BUY_ENGINE_DECISION to stdout

ServiceRunner.run():
  - Installs SIGTERM + SIGINT handlers → stop_event.set()
  - Starts price_feed thread
  - Starts redeem worker in daemon thread (if configured)
  - Runs detector.run() on main thread (blocks until stop_event or deadline)
  - On exit: sets stop_event, joins all threads, emits SERVICE_STOP summary
```

**CLI**: `python -m v2_hybrid_signals.run_service [--env-file X] [--dry-run] [--runtime-seconds N]`

**Emitted events**: `SERVICE_START`, `SERVICE_STOP` (wrapping `buy_executor.snapshot()` and `price_feed.snapshot()`), plus all detector and buy engine events on stdout.

---

## 7. Systemd Unit: `deploy/v2-signals.service` (new file)

- `Type=simple`, `User=poly`, `WorkingDirectory=/home/poly/poly`
- `ExecStart=.venv/bin/python -m v2_hybrid_signals.run_service`
- `EnvironmentFile=/home/poly/poly/.env.v2.local`
- `KillSignal=SIGTERM`, `TimeoutStopSec=15`
- `Restart=on-failure`, `RestartSec=5`
- `MemoryMax=2G`, `LimitNOFILE=65536`
- Logs to journald (`StandardOutput=journal`, `SyslogIdentifier=v2-signals`)

---

## 8. Minor Fix: live_pipeline.py Type Errors

- `_safe_close` parameter type changed from `TextIO | None` to `IO[str] | TextIO | None`.
- Added `IO` to typing imports.
- Fixes 3 pre-existing pyright `reportArgumentType` errors (subprocess `.stdin`/`.stdout` are `IO[str]`, not `TextIO`).

---

## 9. Test Suite: `tests/test_v2_hybrid_signals.py` (new file)

16 offline tests (no network, runs in 0.11s):

| Class | Tests | What it covers |
|---|---|---|
| `TestRateGate` | 3 | Thread-safety (5 threads, no deadlock), throttle factor decrease, success recovery |
| `TestConfigArgv` | 4 | Env-driven config, multi-wallet parsing, dry-run default, CLI override |
| `TestSignalCallback` | 2 | Callback fires on ACTIONABLE_SIGNAL, silent on non-actionable |
| `TestPruneStale` | 2 | Old entry removal, tx_keys 200K cap |
| `TestSharedRateGates` | 3 | Activity sources share one gate, subgraph share another, gates are distinct |
| `TestStopEvent` | 2 | External event propagation, default event creation |

All tests use `@patch.dict(os.environ, ...)` for isolation. Multi-wallet tests explicitly clear `TRACKED_WALLET=""` to prevent merging with `TRACKED_WALLETS`.

---

## Files Modified

| File | Nature |
|---|---|
| `v2_hybrid_signals/config.py` | `parse_args(argv=None)`, `_Argv` alias |
| `v2_hybrid_signals/buy_config.py` | `parse_args(argv=None)`, `_Argv` alias |
| `v2_hybrid_signals/redeem_config.py` | `parse_args(argv=None)`, `_Argv` alias |
| `v2_hybrid_signals/hybrid_detector.py` | Shared rate gates, signal_callback, stop_event param, prune TTL fix, _emit no longer static |
| `v2_hybrid_signals/main.py` | SIGTERM handler |
| `v2_hybrid_signals/buy_main.py` | SIGTERM handler |
| `v2_hybrid_signals/redeem_main.py` | SIGTERM handler |
| `v2_hybrid_signals/live_pipeline.py` | `_safe_close` type fix |

## Files Created

| File | Purpose |
|---|---|
| `v2_hybrid_signals/run_service.py` | Unified in-process orchestrator |
| `deploy/v2-signals.service` | Systemd unit file |
| `tests/test_v2_hybrid_signals.py` | 16 regression tests |

## Backward Compatibility

All changes are backward-compatible:
- `parse_args()` with no arguments behaves identically (reads `sys.argv`).
- `HybridDetector(cfg)` with no extra kwargs behaves identically (creates internal stop_event, no callback).
- Existing entry points (`main.py`, `buy_main.py`, `redeem_main.py`, `live_pipeline.py`) work unchanged.
- `run_service.py` is additive — it's a new entry point.
