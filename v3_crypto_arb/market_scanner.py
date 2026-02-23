"""Market scanner — discovers active crypto up/down windows.

Polls the Gamma API for active 5m and 15m crypto up/down events
and maintains a registry of CryptoWindow objects.

Slug pattern: {btc|eth|sol|xrp}-updown-{5m|15m}-{unix_start_ts}
Example: btc-updown-5m-1771848900

Each event has:
  - condition_id (for resolution)
  - two tokens: Up (outcomes[0]) and Down (outcomes[1])
  - automaticallyResolved: true
  - negRisk: false
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Optional

import aiohttp

from .config import GAMMA_API, Config
from .models import CryptoWindow

log = logging.getLogger(__name__)

# Parse slug: btc-updown-5m-1771848900
_SLUG_RE = re.compile(
    r"^(btc|eth|sol|xrp)-updown-(5m|15m)-(\d+)$"
)

_DURATION_MAP = {"5m": 300, "15m": 900}

_HEADERS = {"User-Agent": "Mozilla/5.0 (v3-crypto-arb)"}


class MarketScanner:
    """Discovers and tracks active crypto up/down windows.

    Usage:
        scanner = MarketScanner(cfg)
        scanner.on_new_window(callback)
        await scanner.run()   # polls forever
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._windows: dict[str, CryptoWindow] = {}  # slug → window
        self._callbacks: list[object] = []
        self._running = False

    def on_new_window(self, cb: object) -> None:
        """Register callback for new window discovery: cb(window: CryptoWindow)."""
        self._callbacks.append(cb)

    @property
    def active_windows(self) -> dict[str, CryptoWindow]:
        """All currently tracked windows (may include recently expired)."""
        return self._windows

    def get_windows_for_asset(self, asset: str) -> list[CryptoWindow]:
        """Active windows for a specific asset."""
        return [w for w in self._windows.values() if w.asset == asset]

    def get_window(self, slug: str) -> Optional[CryptoWindow]:
        return self._windows.get(slug)

    async def run(self) -> None:
        self._running = True
        log.info("market scanner starting (poll interval: %.0fs)",
                 self._cfg.gamma_poll_interval_s)
        while self._running:
            try:
                await self._poll()
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception:
                log.exception("market scanner poll error")
            # Prune expired windows
            self._prune()
            await asyncio.sleep(self._cfg.gamma_poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    # ── Gamma API polling ──

    async def _poll(self) -> None:
        """Poll Gamma API for all configured assets × durations."""
        now = time.time()
        async with aiohttp.ClientSession(headers=_HEADERS) as session:
            tasks = []
            for asset in self._cfg.assets:
                for dur in self._cfg.window_durations:
                    dur_label = "5m" if dur == 300 else "15m"
                    tasks.append(self._poll_asset(session, asset, dur_label, now))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_asset(self, session: aiohttp.ClientSession,
                          asset: str, dur_label: str, now: float) -> None:
        """Fetch active events matching slug pattern for one asset+duration."""
        # Search for events with matching slug prefix
        slug_prefix = f"{asset}-updown-{dur_label}"
        url = (
            f"{GAMMA_API}/events"
            f"?slug={slug_prefix}"
            f"&active=true"
            f"&closed=false"
            f"&limit=10"
        )

        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.warning("gamma api %d for %s", resp.status, slug_prefix)
                    return
                events = await resp.json()
        except Exception:
            log.exception("gamma api fetch error for %s", slug_prefix)
            return

        if not isinstance(events, list):
            return

        for event in events:
            self._process_event(event, now)

    def _process_event(self, event: dict, now: float) -> None:  # type: ignore[type-arg]
        """Extract CryptoWindow from a Gamma API event object."""
        slug = event.get("slug", "")
        m = _SLUG_RE.match(slug)
        if not m:
            return

        asset = m.group(1)
        dur_label = m.group(2)
        start_ts = int(m.group(3))
        duration_s = _DURATION_MAP[dur_label]
        end_ts = start_ts + duration_s

        if asset not in self._cfg.assets:
            return

        # Already tracking?
        if slug in self._windows:
            return

        # Extract market (first market in the event)
        markets = event.get("markets", [])
        if not markets:
            log.debug("no markets in event %s", slug)
            return

        market = markets[0]
        condition_id = market.get("conditionId", "")
        if not condition_id:
            return

        # Token IDs — outcomes order: ["Up", "Down"]
        clob_ids = market.get("clobTokenIds")
        if not clob_ids or len(clob_ids) < 2:
            log.debug("missing clobTokenIds in %s", slug)
            return

        # Parse token IDs (could be JSON string or list)
        if isinstance(clob_ids, str):
            import json
            try:
                clob_ids = json.loads(clob_ids)
            except json.JSONDecodeError:
                return

        outcomes = market.get("outcomes")
        if isinstance(outcomes, str):
            import json
            try:
                outcomes = json.loads(outcomes)
            except json.JSONDecodeError:
                outcomes = ["Up", "Down"]

        # Map Up/Down to correct token IDs
        if isinstance(outcomes, list) and len(outcomes) >= 2:
            up_idx = 0
            down_idx = 1
            for i, o in enumerate(outcomes):
                if isinstance(o, str) and o.lower() == "up":
                    up_idx = i
                elif isinstance(o, str) and o.lower() == "down":
                    down_idx = i
            token_id_up = clob_ids[up_idx]
            token_id_down = clob_ids[down_idx]
        else:
            token_id_up = clob_ids[0]
            token_id_down = clob_ids[1]

        neg_risk = bool(market.get("negRisk", False))
        tick_str = market.get("orderPriceMinTickSize", "0.01")
        try:
            tick_size = float(tick_str)
        except (ValueError, TypeError):
            tick_size = 0.01

        window = CryptoWindow(
            slug=slug,
            asset=asset,
            duration_seconds=duration_s,
            window_start_ts=start_ts,
            window_end_ts=end_ts,
            condition_id=condition_id,
            token_id_up=token_id_up,
            token_id_down=token_id_down,
            neg_risk=neg_risk,
            tick_size=tick_size,
        )

        self._windows[slug] = window
        log.info("new window: %s  start=%d end=%d  up=%s down=%s",
                 slug, start_ts, end_ts, token_id_up[:12], token_id_down[:12])

        # Notify callbacks
        for cb in self._callbacks:
            try:
                cb(window)  # type: ignore[operator]
            except Exception:
                log.exception("new window callback error")

    def _prune(self) -> None:
        """Remove windows that ended more than 60s ago."""
        cutoff = time.time() - 60.0
        expired = [s for s, w in self._windows.items() if w.window_end_ts < cutoff]
        for s in expired:
            del self._windows[s]
            log.debug("pruned window %s", s)
