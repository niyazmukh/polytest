"""Resolution poller — tracks how crypto windows resolved.

After a window expires, polls the Gamma API to determine the outcome
(Up or Down).  Records the resolution for model calibration.

This is essential for answering: "Was prob_up_gbm correct?"
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from .config import GAMMA_API, LoggerConfig

log = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (v4-data-logger)"}


@dataclass(slots=True)
class PendingResolution:
    """A window that has expired and awaits resolution."""
    slug: str
    condition_id: str
    asset: str
    window_end_ts: float
    first_check_ts: float      # when we first started polling
    token_id_up: str
    token_id_down: str


class ResolutionPoller:
    """Polls Gamma API for resolved window outcomes.

    Usage:
        poller = ResolutionPoller(cfg)
        poller.on_resolution(callback)
        poller.track_window(slug, condition_id, asset, end_ts, tok_up, tok_down)
        await poller.run()
    """

    def __init__(self, cfg: LoggerConfig) -> None:
        self._cfg = cfg
        self._pending: dict[str, PendingResolution] = {}
        self._resolved: set[str] = set()
        self._callbacks: list[object] = []
        self._running = False

    def on_resolution(self, cb: object) -> None:
        self._callbacks.append(cb)

    def track_window(self, slug: str, condition_id: str, asset: str,
                     end_ts: float, token_id_up: str,
                     token_id_down: str) -> None:
        """Register a window for future resolution checking."""
        if slug in self._pending or slug in self._resolved:
            return
        self._pending[slug] = PendingResolution(
            slug=slug,
            condition_id=condition_id,
            asset=asset,
            window_end_ts=end_ts,
            first_check_ts=0.0,
            token_id_up=token_id_up,
            token_id_down=token_id_down,
        )

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def resolved_count(self) -> int:
        return len(self._resolved)

    async def run(self) -> None:
        """Poll loop — runs forever until cancelled."""
        self._running = True
        while self._running:
            try:
                await self._poll_round()
            except asyncio.CancelledError:
                self._running = False
                return
            except Exception:
                log.exception("resolution poller error")
            await asyncio.sleep(self._cfg.resolution_poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    async def _poll_round(self) -> None:
        """Check each pending window that's past the delay period."""
        now = time.time()
        to_check: list[PendingResolution] = []
        to_remove: list[str] = []

        for slug, pr in self._pending.items():
            # Wait for delay after window end
            if now < pr.window_end_ts + self._cfg.resolution_delay_s:
                continue
            # Timeout — give up
            if pr.first_check_ts > 0 and \
               (now - pr.first_check_ts > self._cfg.resolution_timeout_s):
                log.warning("resolution timeout: %s", slug)
                to_remove.append(slug)
                continue
            if pr.first_check_ts == 0.0:
                pr.first_check_ts = now
            to_check.append(pr)

        for slug in to_remove:
            self._pending.pop(slug, None)

        if not to_check:
            return

        async with aiohttp.ClientSession(headers=_HEADERS) as session:
            for pr in to_check:
                await self._check_one(session, pr)

    async def _check_one(self, session: aiohttp.ClientSession,
                         pr: PendingResolution) -> None:
        """Check Gamma API for a single resolved window."""
        url = f"{GAMMA_API}/events?slug={pr.slug}"
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return
                events = await resp.json()
        except Exception:
            log.debug("resolution check failed for %s", pr.slug)
            return

        if not isinstance(events, list) or not events:
            return

        event = events[0]
        markets = event.get("markets", [])
        if not markets:
            return

        market = markets[0]

        # Check if resolved (closed + not active)
        closed = market.get("closed", False)
        active = market.get("active", True)
        if not closed and active:
            return  # not yet resolved

        # Parse outcome prices  (e.g. "[1.0, 0.0]" or [1.0, 0.0])
        outcome_prices_raw = market.get("outcomePrices", "")
        outcome_prices: list[float] = []
        if isinstance(outcome_prices_raw, str):
            try:
                outcome_prices = [float(x) for x in json.loads(outcome_prices_raw)]
            except (json.JSONDecodeError, ValueError):
                pass
        elif isinstance(outcome_prices_raw, list):
            try:
                outcome_prices = [float(x) for x in outcome_prices_raw]
            except (ValueError, TypeError):
                pass

        # Parse outcome labels  (e.g. '["Up","Down"]' or ["Up","Down"])
        outcomes_raw = market.get("outcomes", "")
        outcomes: list[str] = ["Up", "Down"]
        if isinstance(outcomes_raw, str):
            try:
                parsed = json.loads(outcomes_raw)
                if isinstance(parsed, list):
                    outcomes = [str(x) for x in parsed]
            except json.JSONDecodeError:
                pass
        elif isinstance(outcomes_raw, list):
            outcomes = [str(x) for x in outcomes_raw]

        if not outcome_prices or len(outcome_prices) < 2:
            return

        # Both zero → not actually resolved yet
        if outcome_prices[0] == 0 and outcome_prices[1] == 0:
            return

        winner_idx = 0
        if outcome_prices[1] > outcome_prices[0]:
            winner_idx = 1
        outcome = outcomes[winner_idx] if winner_idx < len(outcomes) else "Unknown"

        log.info("RESOLVED: %s → %s (prices=%s)", pr.slug, outcome, outcome_prices)

        self._pending.pop(pr.slug, None)
        self._resolved.add(pr.slug)

        for cb in self._callbacks:
            try:
                cb(pr.slug, pr.condition_id, outcome, str(outcome_prices))  # type: ignore[operator]
            except Exception:
                log.exception("resolution callback error")
