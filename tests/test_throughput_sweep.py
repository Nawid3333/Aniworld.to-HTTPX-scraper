"""The throughput sweep must measure the scraper, not misread it.

tests/throughput_sweep.py drives the scraper's own _scrape_one_series against
the live site, so it cannot run here -- but everything it decides from what
comes back can: which series failed, when a setting stops, what is skipped
after a stop, and what is saved. Each class pins a bug its first version
shipped with, found by reading it against the scraper:

* It looked for result["error"]; the scraper marks a failure "_error". Every
  failed series therefore counted as a success with zero episodes, the
  >10%-failed early stop could never fire, and a series that failed in one
  setting and not in another was reported as a data mismatch.
* One unexpected exception in a scrape ended the whole sweep, every worker
  with it, and lost every measurement taken so far.
* After a stop, careful mode skipped only the same season concurrency, so a
  setting with just as many requests in flight could still run.
* The report was written only at the very end, so Ctrl+C after twenty
  minutes of load kept nothing.
* A 5xx counted as a served page, a 5xx that outlived the retries counted as
  a transport error, --transport h3 silently meant HTTP/1.1, --workers 0
  crashed the verdict, and a host that declined HTTP/2 made the "h2" rows
  HTTP/1.1 rows without a word.

The scrape itself is stubbed here, but it still fetches through the scraper's
real _get, so the sweep's meter sees real httpx responses and the real
retry-and-push-back path.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

import src.scraper as sc
from tests import throughput_sweep as sweep

SCRAPER_CLS = sweep.SCRAPER_CLS
REPO_ROOT = Path(__file__).resolve().parent.parent
# The one line that differs between the three sibling copies of this file.
ENV_PREFIX = "ANIWORLD"
HOST = "https://host.example"


def series_info(n: int) -> dict:
    return {"title": f"Series {n}", "link": f"/series/s{n}", "url": f"{HOST}/series/s{n}"}


def sample_of(count: int) -> list[dict]:
    return [series_info(n) for n in range(count)]


def serve_ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text="<html><body>ok</body></html>")


def measured_row(**overrides) -> dict:
    """One setting's measurements: what run_setting returns plus what sweep_transport adds."""
    row = {
        "transport": "h2",
        "repeat": 0,
        "workers": 8,
        "season_concurrency": 4,
        "series": 50,
        "series_ok": 50,
        "series_failed": 0,
        "failure_reasons": {},
        "pages": 200,
        "wall_s": 10.0,
        "pages_per_s": 20.0,
        "series_per_s": 5.0,
        "mbit_per_s": 8.0,
        "ttfb_p50_ms": 200,
        "ttfb_p90_ms": 400,
        "pushback_429_503": 0,
        "transport_errors": 0,
        "status_counts": {200: 200},
        "protocols": {"HTTP/2": 200},
        "cpu_pct_one_core": 20.0,
        "mismatches": 0,
        "mismatch_detail": [],
        "stopped": None,
    }
    row.update(overrides)
    return row


class NoWaitGuard:
    """RateGuard without the pauses: push-back is still seen, never slept on."""

    async def wait(self) -> None:
        return None

    def penalise(self, retry_after: float | None = None) -> float:
        return 0.0

    def reward(self) -> None:
        return None


class SweepCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        sink = contextlib.redirect_stdout(io.StringIO())
        sink.__enter__()
        self.addCleanup(sink.__exit__, None, None, None)
        # The sweep rewrites these module globals as it goes; put them back.
        for name, value in (
            ("RateGuard", NoWaitGuard),
            ("_BACKOFF_BASE", 0),
            ("SEASON_CONCURRENCY", sc.SEASON_CONCURRENCY),
            ("USE_HTTP2", sc.USE_HTTP2),
        ):
            patcher = mock.patch.object(sc, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def scraper(self, outcomes: dict | None = None):
        """A real scraper whose series scrape fetches through the real _get.

        `outcomes` maps a series url to what its scrape then reports: an
        episode count (default 12), "fail" for the scraper's own error
        result, or "raise" for an exception nothing inside caught.
        """
        scraper = SCRAPER_CLS()
        outcomes = outcomes or {}

        async def scrape(client, info):
            outcome = outcomes.get(info["url"], 12)
            if outcome == "raise":
                raise ValueError("a page nothing could parse")
            try:
                await scraper._get(client, info.get("scrape_url", info["url"]))
            except httpx.HTTPError as exc:
                return SCRAPER_CLS._error_result(info, str(exc))
            if outcome == "fail":
                return SCRAPER_CLS._error_result(info, "no seasons found")
            return {
                "title": info["title"],
                "link": info["link"],
                "url": info["url"],
                "total_seasons": 1,
                "total_episodes": outcome,
                "watched_episodes": 0,
            }

        scraper._scrape_one_series = scrape
        return scraper

    def run_setting(self, scraper, sample, *, workers=1, reference=None, handler=None) -> dict:
        """One setting against a mock site. The client's hooks afterwards land in self.hooks_after."""

        async def go():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler or serve_ok)) as client:
                ref = {} if reference is None else reference
                row = await sweep.run_setting(scraper, client, sample, workers, 4, 0, ref)
                self.hooks_after = dict(client.event_hooks)
                return row

        return asyncio.run(go())


class TestAFailureIsNotData(SweepCase):
    def test_the_scrapers_own_failure_marker_is_recognised(self):
        failed = SCRAPER_CLS._error_result(series_info(1), "no seasons found")
        self.assertIsNone(sweep.fingerprint(failed), "a failed series was read as data")

    def test_a_result_is_its_counts(self):
        result = {"total_seasons": 2, "total_episodes": 24, "watched_episodes": 5}
        self.assertEqual(sweep.fingerprint(result), (2, 24, 5))

    def test_failed_series_are_counted_as_failed(self):
        sample = sample_of(30)
        scraper = self.scraper({sample[3]["url"]: "fail", sample[9]["url"]: "fail"})
        row = self.run_setting(scraper, sample)
        self.assertEqual((row["series_ok"], row["series_failed"]), (28, 2))
        self.assertEqual(row["failure_reasons"], {"no seasons found": 2})
        self.assertIsNone(row["stopped"], "two failures in thirty is under the stop ratio")

    def test_a_wave_of_failures_stops_the_setting(self):
        sample = sample_of(40)
        row = self.run_setting(self.scraper({i["url"]: "fail" for i in sample}), sample)
        self.assertIn("series failed", row["stopped"] or "", "the failure stop never fired")
        self.assertIn("no seasons found", row["stopped"], "the stop should say why the series failed")
        self.assertEqual(row["series"], 20, "one worker must stop as soon as the ratio is judged")


class TestTheAccuracyCheck(SweepCase):
    def test_failing_in_one_setting_only_is_not_a_mismatch(self):
        sample = sample_of(10)
        flaky = sample[4]["url"]
        reference: dict = {}
        rows = [
            self.run_setting(self.scraper({flaky: "fail"}), sample, reference=reference),
            self.run_setting(self.scraper(), sample, reference=reference),
            self.run_setting(self.scraper({flaky: "fail"}), sample, reference=reference),
        ]
        self.assertEqual([r["mismatches"] for r in rows], [0, 0, 0])

    def test_different_counts_are_still_a_mismatch(self):
        sample = sample_of(10)
        changed = sample[2]["url"]
        reference: dict = {}
        self.run_setting(self.scraper(), sample, reference=reference)
        row = self.run_setting(self.scraper({changed: 13}), sample, reference=reference)
        self.assertEqual(row["mismatches"], 1)
        self.assertIn(changed, row["mismatch_detail"][0])


class TestAnUnexpectedErrorCostsOneSeries(SweepCase):
    def test_the_rest_of_the_setting_still_runs(self):
        sample = sample_of(10)
        row = self.run_setting(self.scraper({sample[5]["url"]: "raise"}), sample, workers=3)
        self.assertEqual((row["series_ok"], row["series_failed"]), (9, 1))
        (reason,) = row["failure_reasons"]
        self.assertTrue(reason.startswith("unexpected error"), reason)

    def test_the_scraper_and_client_are_put_back(self):
        scraper = self.scraper({series_info(0)["url"]: "raise"})
        self.run_setting(scraper, sample_of(3))
        self.assertIs(scraper._get.__func__, SCRAPER_CLS._get, "the counting wrapper was left on the scraper")
        self.assertEqual(self.hooks_after, {"request": [], "response": []})


class TestWhatTheMeterCounts(SweepCase):
    def test_push_back_stops_the_setting(self):
        row = self.run_setting(self.scraper(), sample_of(10), handler=lambda r: httpx.Response(429))
        self.assertIn("pushed back", row["stopped"] or "")
        self.assertEqual(row["pages"], 0, "a refusal is not a page")
        self.assertEqual(row["status_counts"], {429: sc._MAX_ATTEMPTS})

    def test_a_server_error_is_neither_a_page_nor_a_transport_error(self):
        row = self.run_setting(self.scraper(), sample_of(1), handler=lambda r: httpx.Response(502))
        self.assertEqual(row["pages"], 0)
        self.assertEqual(row["transport_errors"], 0)
        self.assertEqual(row["status_counts"], {502: sc._MAX_ATTEMPTS}, "every retry is still visible by code")
        self.assertEqual(row["series_failed"], 1)

    def test_a_dropped_connection_is_a_transport_error(self):
        def refuse(request):
            raise httpx.ConnectError("connection reset", request=request)

        row = self.run_setting(self.scraper(), sample_of(1), handler=refuse)
        self.assertEqual((row["transport_errors"], row["series_failed"]), (1, 1))

    def test_served_pages_and_the_protocol_they_came_over(self):
        row = self.run_setting(self.scraper(), sample_of(5))
        self.assertEqual(row["pages"], 5)
        self.assertEqual(row["protocols"], {"HTTP/1.1": 5})

    def test_pages_leave_out_refusals_and_failures(self):
        meter = sweep.Meter()
        meter.status = {200: 5, 301: 1, 404: 1, 429: 2, 500: 1, 502: 1, 503: 1}
        self.assertEqual(meter.pages, 7)


class TestTheSampleIsFetchedFromTheSessionHost(SweepCase):
    def test_a_mirror_url_moves_to_the_session_host(self):
        info = {"title": "X", "link": "/anime/stream/x", "url": "https://old-mirror.example/anime/stream/x"}
        job = sweep.on_host(info, "https://live.example/")
        self.assertEqual(job["scrape_url"], "https://live.example/anime/stream/x")
        self.assertEqual(job["url"], info["url"], "results must still line up by the stored url")
        self.assertNotIn("scrape_url", info, "the sample itself must not change")

    def test_every_request_goes_to_the_session_host(self):
        hosts = []

        def record(request):
            hosts.append(request.url.host)
            return serve_ok(request)

        scraper = self.scraper()
        scraper.site_url = "https://live.example"
        self.run_setting(scraper, sample_of(3), handler=record)
        self.assertEqual(set(hosts), {"live.example"})


class TestCarefulModeSkipsByLoad(SweepCase):
    """After a stop, nothing with at least as many requests in flight runs again."""

    def sweep(self, *, stop_at_load, careful=True, warm_ok=True):
        argv = ["--workers", "4,8,16", "--season-concurrency", "2,4", "--repeats", "2", "--transport", "h2"]
        args = sweep.build_parser().parse_args(argv + ([] if careful else ["--no-careful"]))
        ran: list[int] = []

        async def fake_run_setting(scraper, client, sample, workers, season_conc, cooldown, reference):
            load = workers * season_conc
            ran.append(load)
            stopped = "site pushed back 3x (429/503)" if load >= stop_at_load else None
            return measured_row(workers=workers, season_concurrency=season_conc, stopped=stopped)

        async def fake_login(self_, verify=True):
            return httpx.AsyncClient(transport=httpx.MockTransport(serve_ok))

        async def fake_scrape(self_, client, info):
            if not warm_ok:
                return SCRAPER_CLS._error_result(info, "session expired — not logged in")
            return {"total_seasons": 1, "total_episodes": 12, "watched_episodes": 0}

        rows: list[dict] = []
        with (
            mock.patch.object(sweep, "run_setting", fake_run_setting),
            mock.patch.object(SCRAPER_CLS, "_create_logged_in_client", fake_login),
            mock.patch.object(SCRAPER_CLS, "_scrape_one_series", fake_scrape),
        ):
            asyncio.run(sweep.sweep_transport("h2", args, sample_of(6), rows, {}))
        return ran, rows

    def test_nothing_with_as_much_load_runs_after_a_stop(self):
        # Loads: 4x2=8, 4x4=16, 8x2=16, 8x4=32, 16x2=32, 16x4=64.
        ran, _ = self.sweep(stop_at_load=32)
        self.assertEqual(sum(1 for load in ran if load >= 32), 1, f"ran loads {ran}")
        self.assertEqual(
            sorted(load for load in ran if load < 32), [8, 8, 16, 16, 16, 16], "lighter settings still run"
        )

    def test_without_careful_every_setting_runs(self):
        ran, rows = self.sweep(stop_at_load=32, careful=False)
        self.assertEqual(len(ran), 12)
        self.assertEqual(len(rows), 12)

    def test_a_transport_whose_warmup_all_fails_is_not_swept(self):
        ran, rows = self.sweep(stop_at_load=10**6, warm_ok=False)
        self.assertEqual((ran, rows), ([], []))


class TestAnInterruptedSweepStillSaves(SweepCase):
    def setUp(self):
        super().setUp()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.report = Path(tmp.name) / "throughput_report.json"
        patcher = mock.patch.object(sweep, "REPORT_FILE", self.report)
        patcher.start()
        self.addCleanup(patcher.stop)

    def main(self, sweep_transport):
        args = sweep.build_parser().parse_args(["--transport", "h2,h1", "--cooldown", "0"])
        with (
            mock.patch.object(sweep, "load_sample", return_value=sample_of(3)),
            mock.patch.object(sweep, "sweep_transport", sweep_transport),
        ):
            asyncio.run(sweep.main_async(args))

    def saved(self) -> dict:
        return json.loads(self.report.read_text(encoding="utf-8"))

    @staticmethod
    def h2_then(exc):
        async def sweep_transport(transport, args, sample, rows, reference):
            if transport == "h2":
                rows.append(measured_row(transport="h2"))
                return sample
            if exc is not None:
                raise exc
            rows.append(measured_row(transport="h1", protocols={"HTTP/1.1": 200}))
            return sample

        return sweep_transport

    def test_a_crash_keeps_what_finished(self):
        with self.assertRaises(RuntimeError):
            self.main(self.h2_then(RuntimeError("h1 login refused")))
        saved = self.saved()
        self.assertFalse(saved["complete"])
        self.assertEqual([r["transport"] for r in saved["runs"]], ["h2"])

    def test_ctrl_c_keeps_what_finished(self):
        # Inside asyncio.run, Ctrl+C arrives as a cancellation of the main task.
        with self.assertRaises(asyncio.CancelledError):
            self.main(self.h2_then(asyncio.CancelledError()))
        self.assertFalse(self.saved()["complete"])

    def test_a_finished_sweep_is_marked_complete(self):
        self.main(self.h2_then(None))
        saved = self.saved()
        self.assertTrue(saved["complete"])
        self.assertEqual(len(saved["runs"]), 2)


class TestArguments(unittest.TestCase):
    def parse(self, *argv):
        with contextlib.redirect_stderr(io.StringIO()):
            return sweep.build_parser().parse_args(list(argv))

    def test_defaults(self):
        args = self.parse()
        self.assertEqual(args.workers, [4, 8, 12, 16, 24])
        self.assertEqual(args.season_concurrency, [4])
        self.assertEqual(args.transport, ["h2", "h1"])
        self.assertTrue(args.careful)

    def test_lists_are_tidied(self):
        args = self.parse("--workers", "8, 4,8,", "--transport", "h1, h2")
        self.assertEqual(args.workers, [4, 8])
        self.assertEqual(args.transport, ["h1", "h2"], "the order given is the order swept")

    def test_nonsense_is_refused_up_front(self):
        for argv in (
            ["--workers", "0"],
            ["--workers", "four"],
            ["--season-concurrency", "0"],
            ["--transport", "h3"],
            ["--transport", ""],
            ["--sample", "0"],
            ["--repeats", "0"],
            ["--cooldown", "-1"],
        ):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                self.parse(*argv)


class TestTheVerdict(unittest.TestCase):
    def verdict(self, *rows) -> str:
        return "\n".join(sweep.diagnose(sweep.summarise(list(rows))))

    def test_a_stop_above_the_only_clean_count_is_still_reported(self):
        text = self.verdict(
            measured_row(workers=4),
            measured_row(workers=8, pushback_429_503=3, stopped="site pushed back 3x (429/503)"),
        )
        self.assertIn("pushed back (429/503) from workers=8", text)

    def test_failures_are_not_called_push_back(self):
        text = self.verdict(
            measured_row(workers=4),
            measured_row(workers=8, stopped="21/25 series failed (mostly: session expired)"),
        )
        self.assertIn("started failing series from workers=8", text)
        self.assertNotIn("pushed back", text)

    def test_a_queue_on_the_site_is_named(self):
        text = self.verdict(
            measured_row(workers=4, pages_per_s=50.0, ttfb_p50_ms=200),
            measured_row(workers=8, pages_per_s=51.0, ttfb_p50_ms=400),
            measured_row(workers=16, pages_per_s=51.0, ttfb_p50_ms=800),
        )
        self.assertIn("within 5% of the best from workers=4", text)
        self.assertIn("Requests queue ON THE SITE", text)

    def test_http2_the_site_never_spoke_is_flagged(self):
        text = self.verdict(
            measured_row(transport="h2", protocols={"HTTP/1.1": 200}),
            measured_row(transport="h1", protocols={"HTTP/1.1": 200}),
        )
        self.assertIn("WARNING: asked for HTTP/2", text)
        self.assertNotIn("vs best HTTP/2", text, "an HTTP/1.1-vs-HTTP/1.1 comparison must not be offered")

    def test_a_real_http2_comparison_is_made(self):
        text = self.verdict(
            measured_row(transport="h2", pages_per_s=20.0),
            measured_row(transport="h1", pages_per_s=30.0, protocols={"HTTP/1.1": 200}),
        )
        self.assertIn("Best HTTP/1.1 30.0 vs best HTTP/2 20.0", text)
        self.assertNotIn("WARNING", text)


class TestTheHttp2Switch(unittest.TestCase):
    """HTTP/1.1 unless HTTP/2 is asked for by an explicit on value; "0" was once the only off."""

    def resolve(self, values: list[str | None]) -> dict[str, bool]:
        """USE_HTTP2 for each value, from config imported in a clean subprocess."""
        name = f"{ENV_PREFIX}_HTTP2"
        env = dict(os.environ)
        env.pop(name, None)
        env["PYTHONPATH"] = str(REPO_ROOT)
        code = (
            "import importlib, json, os, sys\n"
            "import config.config as c\n"
            "out = {}\n"
            "for v in json.loads(sys.argv[1]):\n"
            f"    os.environ.pop({name!r}, None) if v is None else os.environ.__setitem__({name!r}, v)\n"
            "    out[repr(v)] = importlib.reload(c).USE_HTTP2\n"
            "print('<<RESULT>>' + json.dumps(out))\n"
        )
        with tempfile.TemporaryDirectory() as home:
            # A home of its own, so a real .env can never decide the answer.
            env[f"{ENV_PREFIX}_HOME"] = home
            proc = subprocess.run(
                [sys.executable, "-c", code, json.dumps(values)],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        line = next(x for x in proc.stdout.splitlines() if x.startswith("<<RESULT>>"))
        return json.loads(line[len("<<RESULT>>") :])

    def test_off_and_on(self):
        # HTTP/1.1 is the default (measured faster on this site); only an
        # explicit "on" value brings HTTP/2 back.
        off = ["0", "false", "FALSE", " off ", "no", "", None]
        on = ["1", "true", "TRUE", " on ", "yes"]
        result = self.resolve(off + on)
        self.assertEqual({v: result[repr(v)] for v in off}, dict.fromkeys(off, False))
        self.assertEqual({v: result[repr(v)] for v in on}, dict.fromkeys(on, True))


if __name__ == "__main__":
    unittest.main()
