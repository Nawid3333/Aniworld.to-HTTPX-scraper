"""The startup host check waits and checks again when no host serves.

A site under maintenance answers its login and catalogue with a 500 for a while
and then comes back. Before this, every host failing dropped the program into
the menu with nothing fetched -- and with every host still shown as "OK" -- and
the only way to check again was to restart it. The check now counts down on
one line and tries again, until a host serves or the user skips to the menu.
"""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import types
import unittest
from unittest import mock

import main
import src.scraper as sc

SCRAPER_CLS = sc.AniWorldScraper
HOSTS = ["https://a.test", "https://b.test", "https://c.test"]


def _empty_index():
    idx = mock.Mock()
    idx.series_index = {}
    return idx


class _OutputCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.out = io.StringIO()
        sink = contextlib.redirect_stdout(self.out)
        sink.__enter__()
        self.addCleanup(sink.__exit__, None, None, None)


class _StartupCase(_OutputCase):
    def setUp(self):
        super().setUp()
        # The check publishes the host it picks, and a host that serves can
        # write a mismatch report into DATA_DIR; keep both away from the real
        # program state.
        self.addCleanup(setattr, main, "ACTIVE_SITE_URL", getattr(main, "ACTIVE_SITE_URL", None))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.patch(main, "DATA_DIR", tmp.name)
        self.patch(main, "SITE_URLS", HOSTS)

    def patch(self, target, name, value):
        patcher = mock.patch.object(target, name, value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def table(self):
        """The host table as printed: {host: (status, series count)}."""
        rows = {}
        for line in self.out.getvalue().splitlines():
            cells = line.split()
            if cells and cells[0].endswith(".test"):
                rows[cells[0]] = (cells[1], cells[2])
        return rows


class TestStartupCheckRetries(_StartupCase):
    def run_check(self, rounds, answers=(), answering=HOSTS):
        """Run the check; round i serves the hosts in rounds[i].

        answers: what each countdown returns, in order (True = check again).
        Running out of answers fails the test, so an unexpected wait shows.
        Returns (active host, [(attempt, reachable, total) per countdown]).
        """
        answers = list(answers)
        waits = []
        probes = []

        def fake_probe(scraper, site_urls):
            probes.append(1)
            return [{"site_url": url, "ok": url in answering, "status_code": 200} for url in site_urls]

        def fake_fetch(scraper, site_urls):
            served = rounds[len(probes) - 1]
            return {url: ((10, set()) if url in served else (None, set())) for url in site_urls}

        def fake_wait(attempt, reachable, total):
            waits.append((attempt, reachable, total))
            return answers.pop(0)

        scraper = SCRAPER_CLS()
        with (
            mock.patch.object(main, "_probe_hosts", fake_probe),
            mock.patch.object(main, "_fetch_catalogue_info_for_hosts", fake_fetch),
            mock.patch.object(main, "_wait_before_host_retry", fake_wait),
        ):
            main._probe_sites_before_scrape(scraper, idx_mgr=_empty_index())
        self.probes = len(probes)
        return scraper.site_url, waits

    def test_a_serving_host_never_waits(self):
        active, waits = self.run_check([{HOSTS[0]}])
        self.assertEqual(waits, [])
        self.assertEqual(active, HOSTS[0])

    def test_one_serving_host_is_enough(self):
        """A mirror that is down for good must not hold up every start."""
        active, waits = self.run_check([{HOSTS[2]}])
        self.assertEqual(waits, [])
        self.assertEqual(active, HOSTS[2])

    def test_it_checks_again_until_a_host_serves(self):
        active, waits = self.run_check([set(), set(), {HOSTS[1]}], answers=[True, True])
        self.assertEqual([attempt for attempt, _, _ in waits], [0, 1])
        self.assertEqual(self.probes, 3)
        self.assertEqual(active, HOSTS[1])

    def test_skipping_goes_on_to_the_menu_as_before(self):
        active, waits = self.run_check([set()], answers=[False])
        self.assertEqual(len(waits), 1)
        self.assertEqual(self.probes, 1)
        # Nothing served, so the probe order decides -- the old behaviour.
        self.assertEqual(active, HOSTS[0])

    def test_the_countdown_is_told_how_many_hosts_answered(self):
        _, waits = self.run_check([set()], answers=[False], answering=HOSTS[:1])
        self.assertEqual(waits, [(0, 1, len(HOSTS))])

    def test_the_table_is_printed_once_for_the_round_that_counts(self):
        self.run_check([set(), {HOSTS[0]}], answers=[True])
        text = self.out.getvalue()
        self.assertEqual(text.count("Status"), 1)
        self.assertIn("Checking host availability again", text)

    def test_a_host_that_answers_but_serves_nothing_is_not_ok(self):
        """The probe only sees the login page; the 500 came on the login itself."""
        self.run_check([{HOSTS[0]}])
        status = {host: cells[0] for host, cells in self.table().items()}
        self.assertEqual(status, {"a.test": "OK", "b.test": "FAILED", "c.test": "FAILED"})


class TestTheMaintenanceStartFromTheReport(_StartupCase):
    """Replays the start that prompted the retry: every host answers the probe,
    then every login comes back 500.

    Only the network edges are faked -- the reachability probe, the login and
    the catalogue page. Everything between them is the real code: the
    concurrent per-host fetch, the error handling that turns a failed login
    into "no list", the retry loop and the table.
    """

    def setUp(self):
        super().setUp()
        self.site_down = True
        self.logins = []

        async def probe(scraper, site_url):
            return {"site_url": site_url, "ok": True, "status_code": 200, "reason": "reachable"}

        async def login(scraper, client, *args, **kwargs):
            self.logins.append(scraper.site_url)
            if self.site_down:
                # What the real login does when the login page answers 500.
                await client.aclose()
                raise RuntimeError("Login page returned status 500")

        async def catalogue(scraper, client):
            return [{"title": "One", "link": ""}, {"title": "Two", "link": ""}]

        self.patch(SCRAPER_CLS, "_probe_one_site", probe)
        self.patch(SCRAPER_CLS, "_login_client", login)
        self.patch(SCRAPER_CLS, "_get_all_series", catalogue)

    def start(self, during_the_wait):
        """Run the startup check; `during_the_wait` answers each countdown."""
        self.waits = []

        def wait(attempt, reachable, total):
            self.waits.append((attempt, reachable, total))
            return during_the_wait()

        self.patch(main, "_wait_before_host_retry", wait)
        scraper = SCRAPER_CLS()
        with self.assertLogs(sc.logger, level="ERROR") as logs:
            main._probe_sites_before_scrape(scraper, idx_mgr=_empty_index())
        self.assertTrue(any("Login page returned status 500" in line for line in logs.output))
        return scraper.site_url

    def test_the_site_coming_back_during_the_countdown_is_picked_up(self):
        def site_comes_back():
            self.site_down = False
            return True

        active = self.start(site_comes_back)

        self.assertEqual(self.waits, [(0, len(HOSTS), len(HOSTS))])
        self.assertEqual(len(self.logins), 2 * len(HOSTS))
        self.assertEqual(active, HOSTS[0])
        self.assertEqual(self.table(), {"a.test": ("OK", "2"), "b.test": ("OK", "2"), "c.test": ("OK", "2")})

    def test_it_keeps_checking_while_the_site_stays_down(self):
        answers = [True, True, False]
        active = self.start(lambda: answers.pop(0))

        self.assertEqual([attempt for attempt, _, _ in self.waits], [0, 1, 2])
        self.assertEqual(len(self.logins), 3 * len(HOSTS))
        # Skipped in the end: the menu opens as it always did, with every host
        # shown as what it was -- failed -- rather than "OK".
        self.assertEqual(active, HOSTS[0])
        self.assertEqual(self.table(), dict.fromkeys(("a.test", "b.test", "c.test"), ("FAILED", "-")))


class TestWaitBeforeHostRetry(_OutputCase):
    @staticmethod
    def keys(*pressed):
        """A key source that hands out `pressed` and then nothing."""
        queue = list(pressed)
        return lambda: queue.pop(0) if queue else None

    def wait(self, attempt=0, *pressed):
        slept = []
        again = main._wait_before_host_retry(
            attempt, reachable=3, total=3, read_key=self.keys(*pressed), sleep=slept.append
        )
        return again, round(sum(slept), 1)

    def test_running_out_the_clock_checks_again(self):
        again, slept = self.wait(0)
        self.assertTrue(again)
        self.assertEqual(slept, main._HOST_RETRY_DELAYS[0])

    def test_enter_checks_again_at_once(self):
        again, slept = self.wait(0, None, "\n")
        self.assertTrue(again)
        self.assertEqual(slept, 0.1)

    def test_s_skips_to_the_menu(self):
        for key in ("s", "S"):
            with self.subTest(key=key):
                again, slept = self.wait(0, key)
                self.assertFalse(again)
                self.assertEqual(slept, 0)

    def test_other_keys_do_not_cut_the_wait_short(self):
        again, slept = self.wait(0, "x", "q", " ")
        self.assertTrue(again)
        self.assertEqual(slept, main._HOST_RETRY_DELAYS[0])

    def test_the_wait_grows_and_then_holds(self):
        delays = []

        def record(seconds, *args, **kwargs):
            delays.append(seconds)

        with mock.patch.object(main, "_countdown", record):
            for attempt in range(6):
                main._wait_before_host_retry(attempt, reachable=0, total=3)

        steps = list(main._HOST_RETRY_DELAYS)
        self.assertEqual(delays, steps + [steps[-1]] * (6 - len(steps)))
        self.assertEqual(delays, sorted(delays))

    def test_the_message_names_the_attempt_and_the_reachable_hosts(self):
        self.wait(2, "s")
        self.assertIn("Attempt 3", self.out.getvalue())
        self.assertIn("3 of 3 reachable", self.out.getvalue())


class TestCountdown(_OutputCase):
    def test_every_second_is_shown_on_the_same_line(self):
        main._countdown(3, "in {seconds}s", keys=("\n",), read_key=lambda: None, sleep=lambda _: None)
        text = self.out.getvalue()
        self.assertEqual(text, "\r  in 3s\r  in 2s\r  in 1s\n")

    def test_the_seconds_keep_their_width(self):
        """Going from 10 to 9 must not leave a stale digit at the end of the line."""
        main._countdown(10, "{seconds}|", keys=(), read_key=lambda: None, sleep=lambda _: None)
        self.assertIn("\r   9|", self.out.getvalue())

    def test_the_line_ends_even_when_a_key_ends_the_wait(self):
        key = main._countdown(5, "{seconds}", keys=("s",), read_key=lambda: "s", sleep=lambda _: None)
        self.assertEqual(key, "s")
        self.assertTrue(self.out.getvalue().endswith("\n"))


class _TTY(io.StringIO):
    def isatty(self):
        return True


class TestPollKey(unittest.TestCase):
    def test_without_a_terminal_nothing_is_pressed(self):
        """A piped run must never block on, or consume, its input."""
        with mock.patch.object(sys, "stdin", io.StringIO("s\n")):
            self.assertIsNone(main._poll_key())

    def windows_keys(self, *codes):
        queue = list(codes)
        fake = types.SimpleNamespace(kbhit=lambda: bool(queue), getwch=lambda: queue.pop(0))
        return mock.patch.dict(sys.modules, {"msvcrt": fake})

    def test_windows_enter_comes_back_as_a_newline(self):
        with mock.patch.object(sys, "stdin", _TTY()), self.windows_keys("\r"):
            self.assertEqual(main._poll_key(), "\n")

    def test_windows_letters_come_back_as_typed(self):
        with mock.patch.object(sys, "stdin", _TTY()), self.windows_keys("s"):
            self.assertEqual(main._poll_key(), "s")

    def test_windows_nothing_pressed(self):
        with mock.patch.object(sys, "stdin", _TTY()), self.windows_keys():
            self.assertIsNone(main._poll_key())

    def test_windows_arrow_keys_are_dropped_whole(self):
        with mock.patch.object(sys, "stdin", _TTY()), self.windows_keys("\xe0", "H", "s"):
            self.assertIsNone(main._poll_key())
            self.assertEqual(main._poll_key(), "s")

    def test_windows_ctrl_c_still_stops_the_program(self):
        with mock.patch.object(sys, "stdin", _TTY()), self.windows_keys("\x03"), self.assertRaises(KeyboardInterrupt):
            main._poll_key()


if __name__ == "__main__":
    unittest.main()
