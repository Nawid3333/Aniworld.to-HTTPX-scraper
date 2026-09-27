"""A burst of 500/502/504 parks the whole pool, not just the request that saw it.

Before this, only a 429/503 paused the pool. A server error was retried by
the one request that got it, with its own 0.5 s / 1 s back-off, while every
other worker kept firing into the struggling site at full speed. The full-
catalogue soak run met exactly such a wave (#4).

Now five of them inside five seconds get the same pool-wide pause a 429 does.
A lone one still only retries, and the pause changes pacing only: nothing
here decides what gets stored.
"""

from __future__ import annotations

import asyncio
import contextlib
import unittest
from unittest import mock

import httpx

import src.scraper as sc

SCRAPER_CLS = sc.AniWorldScraper


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class BurstCase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.guard = sc.RateGuard(clock=self.clock)

    def errors(self, n: int, at: float | None = None):
        """Report n server errors at the current (or given) time; return each pause."""
        if at is not None:
            self.clock.now = at
        return [self.guard.note_server_error() for _ in range(n)]

    def parked_for(self) -> float:
        return max(0.0, self.guard._resume_at - self.clock.now)


class TestABurstParksThePool(BurstCase):
    def test_a_few_errors_only_retry(self):
        self.assertEqual(self.errors(sc._SERVER_ERROR_BURST - 1), [None] * (sc._SERVER_ERROR_BURST - 1))
        self.assertEqual(self.parked_for(), 0.0)

    def test_a_burst_pauses_every_worker_once(self):
        with self.assertLogs(sc.logger, "WARNING") as logs:
            pauses = self.errors(sc._SERVER_ERROR_BURST)
        self.assertEqual(pauses[-1], 1.0)
        self.assertEqual(pauses[:-1], [None] * (sc._SERVER_ERROR_BURST - 1))
        self.assertEqual(self.parked_for(), 1.0)
        self.assertTrue(any("Site pushed back" in line for line in logs.output), logs.output)

    def test_errors_spread_out_are_not_a_burst(self):
        step = sc._SERVER_ERROR_WINDOW / (sc._SERVER_ERROR_BURST - 1) + 0.1
        for i in range(sc._SERVER_ERROR_BURST * 3):
            self.assertIsNone(self.guard.note_server_error(), f"error {i} at +{i * step:.1f}s")
            self.clock.now += step
        self.assertEqual(self.parked_for(), 0.0)

    def test_the_window_slides(self):
        start = self.clock.now
        for t in (0.0, 1.0, 2.0, 3.0):
            self.errors(1, at=start + t)
        # The first error has aged out: four in the window.
        self.assertEqual(self.errors(1, at=start + sc._SERVER_ERROR_WINDOW + 0.5), [None])
        # One more inside the window makes five.
        self.assertEqual(self.errors(1, at=start + sc._SERVER_ERROR_WINDOW + 0.6), [1.0])


class TestOneWaveCostsOnePause(BurstCase):
    def test_errors_during_the_pause_are_not_counted(self):
        self.errors(sc._SERVER_ERROR_BURST)
        # Requests sent before the pause keep coming back as errors.
        self.assertEqual(self.errors(20, at=self.clock.now + 0.5), [None] * 20)
        self.assertAlmostEqual(self.parked_for(), 0.5)
        # Once the pool resumes it takes a fresh burst to pause again...
        resumed = self.guard._resume_at
        self.assertEqual(self.errors(sc._SERVER_ERROR_BURST - 1, at=resumed), [None] * (sc._SERVER_ERROR_BURST - 1))

    def test_a_wave_that_carries_on_doubles_the_pause(self):
        self.errors(sc._SERVER_ERROR_BURST)
        self.assertEqual(self.errors(sc._SERVER_ERROR_BURST, at=self.guard._resume_at)[-1], 2.0)
        self.assertEqual(self.errors(sc._SERVER_ERROR_BURST, at=self.guard._resume_at)[-1], 4.0)

    def test_clean_responses_let_the_pause_decay(self):
        self.errors(sc._SERVER_ERROR_BURST)
        for _ in range(10):
            self.guard.reward()
        self.assertEqual(self.errors(sc._SERVER_ERROR_BURST, at=self.guard._resume_at)[-1], 1.0)


class SpyGuard(sc.RateGuard):
    """The real guard, minus the sleeping, recording what _get reports to it."""

    def __init__(self) -> None:
        super().__init__()
        self.penalised = 0
        self.server_errors = 0

    async def wait(self) -> None:
        return None

    def penalise(self, retry_after: float | None = None) -> float:
        self.penalised += 1
        return 0.0

    def note_server_error(self) -> float | None:
        self.server_errors += 1
        return None


class _StatusClient:
    def __init__(self, *statuses: int) -> None:
        self.statuses = list(statuses)

    async def get(self, url, **kwargs):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return httpx.Response(status, text="<html></html>", request=httpx.Request("GET", url))


class TestGetReportsServerErrors(unittest.TestCase):
    def fetch(self, *statuses: int) -> SpyGuard:
        scraper = SCRAPER_CLS()
        guard = scraper._rate_guard = SpyGuard()
        with mock.patch.object(sc, "_BACKOFF_BASE", 0), contextlib.suppress(httpx.HTTPStatusError):
            asyncio.run(scraper._get(_StatusClient(*statuses), "https://example.invalid/x"))
        return guard

    def test_500_502_504_are_reported_as_server_errors(self):
        for status in (500, 502, 504):
            guard = self.fetch(status, 200)
            self.assertEqual((guard.server_errors, guard.penalised), (1, 0), status)

    def test_every_failed_attempt_is_reported(self):
        guard = self.fetch(502)
        self.assertEqual(guard.server_errors, sc._MAX_ATTEMPTS)

    def test_429_and_503_still_pause_directly(self):
        for status in (429, 503):
            guard = self.fetch(status, 200)
            self.assertEqual((guard.server_errors, guard.penalised), (0, 1), status)

    def test_real_answers_are_not_errors(self):
        for status in (200, 404):
            guard = self.fetch(status)
            self.assertEqual((guard.server_errors, guard.penalised), (0, 0), status)


if __name__ == "__main__":
    unittest.main()
