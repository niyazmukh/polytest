# v2 Hybrid Signals

Recovered modular v2 pipeline for Polymarket hybrid signal detection, buy execution, and redeem automation.

## Modules

- `main.py`: starts hybrid detector (activity + subgraph + gamma cache).
- `buy_main.py`: consumes `ACTIONABLE_SIGNAL` and executes buy decisions.
- `redeem_main.py`: polls redeemable positions and submits redeem calls.
- `run_service.py`: **unified in-process service** (detector + buy + redeem).
- `live_pipeline.py`: legacy subprocess pipeline (superseded by `run_service`).

## Unified Service (recommended)

Run detector + buy + redeem in a single process with shared rate gates
and SIGTERM-safe shutdown:

```bash
python -m v2_hybrid_signals.run_service --env-file .env.v2.local
```

`run_service` defaults to live mode (`--no-dry-run`). Use `--dry-run`
when replaying or smoke-testing.

Dry-run mode:

```bash
python -m v2_hybrid_signals.run_service --dry-run
```

Time-bound test (60 seconds):

```bash
python -m v2_hybrid_signals.run_service --runtime-seconds 60 --dry-run
```

## EC2 / systemd Deployment

1. Copy repo to `/home/poly/poly` on the EC2 instance.
2. Create venv and install dependencies:

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

3. Copy env file:

   ```bash
   cp env.example .env.v2.local
   # edit .env.v2.local with real credentials
   ```

4. Install the systemd unit:

   ```bash
   sudo cp deploy/v2-signals.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now v2-signals
   ```

5. Check status and logs:

   ```bash
   sudo systemctl status v2-signals
   journalctl -u v2-signals -f
   ```

## Legacy Pipeline

Run all three workers via subprocess pipe (superseded by `run_service`):

```bash
python -m v2_hybrid_signals.live_pipeline --wallet 0x... --no-dry-run
```

## Detector

Run detector only:

```bash
python -m v2_hybrid_signals.main --user 0x...
```

Key events:

- `ACTIONABLE_SIGNAL`
- `HYBRID_SIGNAL_FIRST`
- `HYBRID_SIGNAL_CONFIRM`
- `HYBRID_PROGRESS`
- `HYBRID_SUMMARY`

## Buy Engine

Pipe detector output into buy engine:

```bash
python -m v2_hybrid_signals.main --user 0x... \
| python -m v2_hybrid_signals.buy_main --dry-run
```

Buy engine emits:

- `BUY_ENGINE_START`
- `BUY_ENGINE_DECISION`
- `BUY_ENGINE_SUMMARY`

## Redeem Worker

Dry run:

```bash
python -m v2_hybrid_signals.redeem_main --dry-run
```

Live relayer mode:

```bash
python -m v2_hybrid_signals.redeem_main --no-dry-run --redeem-submit-mode relayer
```

Redeem events:

- `REDEEM_START`
- `REDEEM_CYCLE`
- `REDEEM_PROGRESS`
- `REDEEM_SUMMARY`
- `REDEEM_CYCLE_ERROR`

## Environment

Use `env.example` as template. Default env file is `.env.v2.local`.

Required for live buy/redeem:

- `PRIVATE_KEY`
- `FUNDER_ADDRESS`
- `CHAIN_ID`
- `SIGNATURE_TYPE`

Required for relayer redeem auth:

- `BUILDER_API_KEY` (or `API_KEY`)
- `BUILDER_SECRET` (or `SECRET`)
- `BUILDER_PASSPHRASE` (or `PASSPHRASE`)

Detector scaling option:

- `SUBGRAPH_BATCH_ENABLED` (default `1`): batch multi-wallet subgraph polling into one request loop.
- `ACTIVITY_BATCH_ENABLED` (default `1`): use shared multi-wallet scheduler for Activity API.
- `ACTIVITY_SATURATE_RPS` (default `1`): keep activity polling continuously at the rate-gate edge.
- `SUBGRAPH_SATURATE_RPS` (default `1`): keep subgraph polling continuously at the rate-gate edge.
- `ACTIVITY_PRIORITY_ACTIVE_WINDOW_SECONDS` (default `3600`): wallets active in this window are prioritized.
- `ACTIVITY_PRIORITY_MAX_PROBE_SECONDS` (default `0.15`): max interval before probing any wallet again.
- `ACTIVITY_LIMIT_PER_10S`, `ACTIVITY_EDGE_FRACTION`
- `SUBGRAPH_LIMIT_PER_10S`, `SUBGRAPH_EDGE_FRACTION`
- `GAMMA_LIMIT_PER_10S`, `GAMMA_EVENTS_LIMIT_PER_10S`, `GAMMA_EDGE_FRACTION`

Useful live-buy tuning knobs (optional):

- `BUY_COPY_SCALE` (default `0.10`)
- `BUY_MIN_ORDER_USDC` (default `1.0`)
- `BUY_MAX_SIGNAL_AGE_SECONDS` (default `7.0`)
- `BUY_SKIP_BOOK_ENABLED` (default `1`)
- `BUY_REQUIRE_MARKET_OPEN` (default `1`)
- `BUY_MARKET_CLOSE_GUARD_SECONDS` (default `0.0`)
- `BUY_QUOTE_WAIT_TIMEOUT_SECONDS` (default `0.12`)
- `BUY_QUOTE_STALE_AFTER_SECONDS` (default `0.8`)
- `BUY_QUOTE_FALLBACK_STALE_SECONDS` (default `2.5`)
- `BUY_FEED_MAX_TRACKED_TOKENS` (default `256`)
- `BUY_FEED_TRACK_TTL_SECONDS` (default `1800`)
- `BUY_FEED_POLL_BATCH_SIZE` (default `120`)
- `BUY_FEED_WS_SUBSCRIBE_BATCH_SIZE` (default `256`)

## Notes

- The `_recovered_fragments` folder contains non-runtime forensic fragments.
- Keep this package isolated from corrupted historical artifacts/log files.

## Testing

```bash
python -m pytest tests/test_v2_hybrid_signals.py -v
```

24 offline tests covering: shared rate gates, signal callback wiring,
config-from-env parsing, prune-stale logic, stop-event propagation.
