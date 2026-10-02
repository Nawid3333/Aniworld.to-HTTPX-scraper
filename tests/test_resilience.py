"""Regression tests for failures that used to lose data or fail a whole run.

Each class here pins one bug that was found by reproducing it, not by reading:
a failed save deleting the index, a corrupt or missing index loading as empty,
one transient error failing an entire series, an unexpected worker exception
discarding the run, a mid-run session expiry failing every series after it, a
fuzzy title guess silently skipping a new series, a truncated catalogue
making the whole index look vanished, one malformed element in the index file
discarding every good entry alongside it, and a useless newest backup hiding
a good older one.

They are written against the observable behaviour rather than the internals,
so a future refactor that keeps the guarantees keeps the tests.

Run with:  python -m unittest discover -s tests
"""

import asyncio
import contextlib
import copy
import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

import main  # noqa: E402
import src.index_manager as im  # noqa: E402
import src.scraper as sc  # noqa: E402
from config.config import SITE_URLS, VALID_SERIES_HOSTS  # noqa: E402
from src.atomic_io import atomic_write_json  # noqa: E402
from src.scraper import ScrapingPausedError  # noqa: E402
from tests import test_season_parse_once as site  # noqa: E402

SCRAPER_CLS = sc.AniWorldScraper
SERIES_PATH = "/anime/stream/"
HOST = sorted(VALID_SERIES_HOSTS)[0]


def series_url(slug):
    return f"https://{HOST}{SERIES_PATH}{slug}"


def _set_module_index_path(module, path):
    """Point a module's index-path global at `path`, if it has one.

    The three projects differ here: bs.to's index_manager reads a module
    global, the other two take the path per instance. Returning the old value
    lets the caller restore it either way.
    """
    if not hasattr(module, "SERIES_INDEX_FILE"):
        return None
    previous = module.SERIES_INDEX_FILE
    module.SERIES_INDEX_FILE = path
    return previous


def _restore_module_index_path(module, previous):
    if previous is not None and hasattr(module, "SERIES_INDEX_FILE"):
        module.SERIES_INDEX_FILE = previous


def make_index_manager(path):
    """IndexManager takes an explicit path in all three projects."""
    _set_module_index_path(im, path)
    return im.IndexManager(path)


class QuietCase(unittest.TestCase):
    """Swallow the progress bars and warning banners these paths print.

    None of these tests assert on stdout, and the banners are loud by design,
    so letting them through would bury the actual test results.
    """

    def setUp(self):
        super().setUp()
        sink = contextlib.redirect_stdout(io.StringIO())
        sink.__enter__()
        self.addCleanup(sink.__exit__, None, None, None)


class TempDirCase(QuietCase):
    def setUp(self):
        super().setUp()
        self._d = tempfile.TemporaryDirectory()
        self.addCleanup(self._d.cleanup)
        self.dir = self._d.name
        self.index_path = os.path.join(self.dir, "series_index.json")
        previous = _set_module_index_path(im, self.index_path)
        self.addCleanup(_restore_module_index_path, im, previous)

    def write_backup(self, entries):
        with open(self.index_path + ".bak1", "w", encoding="utf-8") as fh:
            json.dump(entries, fh)


class TestFailedSaveKeepsTheIndex(TempDirCase):
    """A save that dies on the final swap must not leave the path empty.

    The outgoing file used to be renamed into .bak1 before the new one was
    moved into place, so a failure between those two steps left no file at
    all at the index path. The swap is now the first thing that moves, so
    these fail exactly that rename: the temp file onto the index path.
    """

    def _failing_swap(self):
        real = os.replace

        def replace(src, dst):
            if str(src).endswith(".tmp") and str(dst) == self.index_path:
                raise OSError("simulated failure")
            return real(src, dst)

        return replace

    def test_original_survives_a_failed_final_swap(self):
        with open(self.index_path, "w", encoding="utf-8") as fh:
            json.dump([{"title": "Important"}], fh)
        with (
            mock.patch("src.atomic_io.os.replace", side_effect=self._failing_swap()),
            self.assertRaises(OSError),
        ):
            atomic_write_json(self.index_path, [{"title": "New"}])
        self.assertTrue(os.path.exists(self.index_path), "the index file was deleted")
        with open(self.index_path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh), [{"title": "Important"}], "old content must be intact")

    def test_no_temp_file_is_left_behind(self):
        with open(self.index_path, "w", encoding="utf-8") as fh:
            json.dump([{"title": "Important"}], fh)
        with (
            mock.patch("src.atomic_io.os.replace", side_effect=self._failing_swap()),
            self.assertRaises(OSError),
        ):
            atomic_write_json(self.index_path, [{"title": "New"}])
        self.assertEqual([f for f in os.listdir(self.dir) if f.endswith(".tmp")], [])
        self.assertEqual([f for f in os.listdir(self.dir) if "pending" in f], [], "the spare backup name stayed")

    def test_a_normal_write_still_rotates_a_backup(self):
        atomic_write_json(self.index_path, [{"title": "One"}])
        atomic_write_json(self.index_path, [{"title": "Two"}])
        with open(self.index_path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh), [{"title": "Two"}])
        self.assertTrue(os.path.exists(self.index_path + ".bak1"))


class TestIndexRecoversFromBackup(TempDirCase):
    """An unreadable index must not silently load as empty.

    Loading nothing makes every series look brand new, which is the worst
    possible reading of "the file is damaged".
    """

    GOOD = None

    def setUp(self):
        super().setUp()
        url = series_url("good-show")
        self.GOOD = [{"title": "GoodShow", "seasons": [], "url": url, "link": url}]

    def test_corrupt_index_is_restored(self):
        self.write_backup(self.GOOD)
        with open(self.index_path, "w", encoding="utf-8") as fh:
            fh.write('[{"title": "Broken", ')
        mgr = make_index_manager(self.index_path)
        self.assertEqual(len(mgr.series_index), 1, "corrupt index should restore from .bak1")

    def test_missing_index_is_restored(self):
        self.write_backup(self.GOOD)
        self.assertFalse(os.path.exists(self.index_path))
        mgr = make_index_manager(self.index_path)
        self.assertEqual(len(mgr.series_index), 1, "missing index should restore from .bak1")

    def test_a_genuinely_first_run_stays_empty(self):
        """No index and no backup is a new install, not a disaster."""
        mgr = make_index_manager(self.index_path)
        self.assertEqual(len(mgr.series_index), 0)


class TestTransientErrorDoesNotFailTheSeries(QuietCase):
    """One dropped connection used to put a whole series on the failed list."""

    class FlakyClient:
        def __init__(self, fail_times=1):
            self.calls = 0
            self.fail_times = fail_times

        async def get(self, url, **kwargs):
            self.calls += 1
            if self.calls <= self.fail_times:
                raise httpx.ConnectError("transient reset")
            return httpx.Response(200, text="<html><body>ok</body></html>", request=httpx.Request("GET", url))

    def test_series_page_is_retried(self):
        scraper = SCRAPER_CLS()
        client = self.FlakyClient()
        info = {"url": series_url("demo"), "link": series_url("demo"), "title": "Demo"}
        asyncio.run(scraper._scrape_one_series(client, info))  # type: ignore[arg-type]
        self.assertGreater(client.calls, 1, "the series page must go through the retrying fetch")

    def test_retry_eventually_gives_up(self):
        """A permanently broken host must still end as an error, not hang."""
        scraper = SCRAPER_CLS()
        client = self.FlakyClient(fail_times=99)
        info = {"url": series_url("demo"), "link": series_url("demo"), "title": "Demo"}
        result = asyncio.run(scraper._scrape_one_series(client, info))  # type: ignore[arg-type]
        self.assertTrue(result.get("_error"))
        self.assertLessEqual(client.calls, sc._MAX_ATTEMPTS, "must not retry forever")


class TestWorkerCrashKeepsScrapedWork(QuietCase):
    """An unexpected exception used to discard everything scraped so far."""

    @staticmethod
    def _scraper_with_crash(crash_after):
        scraper = SCRAPER_CLS()
        scraper.series_data = []
        done = {"n": 0}

        async def fake_scrape(client, info):
            done["n"] += 1
            if done["n"] > crash_after:
                raise KeyError("parser bug")
            return {
                "title": info["title"],
                "url": info["url"],
                "link": info["link"],
                "total_episodes": 1,
                "watched_episodes": 0,
                "seasons": [],
            }

        scraper._scrape_one_series = fake_scrape  # type: ignore[method-assign]
        scraper._acquire_client = lambda: asyncio.sleep(0, result=object())  # type: ignore[method-assign]
        scraper._release_client = lambda: asyncio.sleep(0)
        return scraper

    @staticmethod
    def _items(n):
        return [{"url": series_url(f"s{i}"), "link": series_url(f"s{i}"), "title": f"S{i}"} for i in range(n)]

    def test_a_series_level_crash_is_contained(self):
        """One unparseable series must cost that series, not the whole queue."""
        scraper = self._scraper_with_crash(crash_after=6)
        asyncio.run(scraper._scrape_list(self._items(12), num_workers=2))
        self.assertEqual(len(scraper.series_data), 6, "the good series must be kept")
        self.assertEqual(len(scraper.failed_links), 6, "the broken ones must be recorded as failed")

    def test_an_escaping_exception_still_keeps_the_work(self):
        """Belt and braces: if something escapes the worker anyway, _scrape_list
        must still store what was scraped rather than discard the run."""
        scraper = self._scraper_with_crash(crash_after=999)
        real = scraper._scrape_one_series
        done = {"n": 0}

        async def explode(client, info):
            done["n"] += 1
            if done["n"] > 4:
                raise ScrapingPausedError("simulated escape")
            return await real(client, info)

        scraper._scrape_one_series = explode
        with self.assertRaises(ScrapingPausedError):
            asyncio.run(scraper._scrape_list(self._items(12), num_workers=2))
        self.assertEqual(len(scraper.series_data), 4, "work before the escape must survive")

    def test_a_clean_run_still_stores_everything(self):
        scraper = self._scraper_with_crash(crash_after=999)
        asyncio.run(scraper._scrape_list(self._items(5), num_workers=2))
        self.assertEqual(len(scraper.series_data), 5)


class TestRescrapeEmptySeriesLeavesFailedListConsistent(QuietCase):
    """A 0-episode series that recovers on the re-scrape must not stay on the
    failed list -- reconcile_failed_series() re-persists every in-memory
    failed_links entry, so a stale empty_placeholder would survive forever and
    the retry-failed menu option would re-scrape a healthy series."""

    class _FakeClient:
        is_closed = False

        async def aclose(self):
            self.is_closed = True

    @staticmethod
    def _empty_result(url, title="S"):
        return {"url": url, "title": title, "link": url, "total_episodes": 0, "watched_episodes": 0, "seasons": []}

    def _scraper(self, scraped):
        """Scraper whose re-scrape answers from the `scraped` url->result map."""
        scraper = SCRAPER_CLS()
        client = self._FakeClient()

        async def fake_client():
            return client

        async def fake_scrape(_client, info):
            return scraped[info["url"]]

        scraper._create_logged_in_client = fake_client  # type: ignore[method-assign]
        scraper._scrape_one_series = fake_scrape  # type: ignore[method-assign]
        return scraper

    def test_a_recovery_drops_the_stale_placeholder(self):
        url = series_url("demo")
        scraper = self._scraper({url: {**self._empty_result(url), "total_episodes": 5}})
        scraper.series_data = [self._empty_result(url)]
        scraper.failed_links = [{"url": url, "title": "S", "link": url, "reason": "empty_placeholder"}]

        still_empty = asyncio.run(scraper._rescrape_empty_series(list(scraper.series_data)))  # type: ignore[arg-type]

        self.assertEqual(still_empty, [], "a recovered series is not empty any more")
        self.assertEqual(scraper.failed_links, [], "the stale empty_placeholder must be dropped")
        self.assertEqual(scraper.series_data[0]["total_episodes"], 5, "the fresh result replaces the placeholder")

    def test_a_still_empty_series_keeps_its_placeholder(self):
        url = series_url("demo")
        scraper = self._scraper({url: self._empty_result(url)})
        scraper.series_data = [self._empty_result(url)]
        entry = {"url": url, "title": "S", "link": url, "reason": "empty_placeholder"}
        scraper.failed_links = [entry]

        still_empty = asyncio.run(scraper._rescrape_empty_series(list(scraper.series_data)))  # type: ignore[arg-type]

        self.assertEqual(len(still_empty), 1, "a genuinely empty series must still be reported")
        self.assertEqual(scraper.failed_links, [entry], "the genuine placeholder must survive")

    def test_an_error_result_keeps_the_placeholder(self):
        url = series_url("demo")
        scraped = {**self._empty_result(url), "_error": True, "_error_reason": "server error"}
        scraper = self._scraper({url: scraped})
        scraper.series_data = [self._empty_result(url)]
        entry = {"url": url, "title": "S", "link": url, "reason": "empty_placeholder"}
        scraper.failed_links = [entry]

        still_empty = asyncio.run(scraper._rescrape_empty_series(list(scraper.series_data)))  # type: ignore[arg-type]

        self.assertEqual(len(still_empty), 1, "an error result is a failure, not a recovery")
        self.assertEqual(scraper.failed_links, [entry], "the placeholder must survive a failed re-scrape")

    def test_a_recovered_series_does_not_touch_other_entries(self):
        url_a, url_b = series_url("a"), series_url("b")
        scraper = self._scraper({url_a: {**self._empty_result(url_a), "total_episodes": 3}})
        scraper.series_data = [self._empty_result(url_a), self._empty_result(url_b)]
        keep_b = {"url": url_b, "title": "B", "link": url_b, "reason": "empty_placeholder"}
        scraper.failed_links = [
            {"url": url_a, "title": "A", "link": url_a, "reason": "empty_placeholder"},
            keep_b,
        ]

        still_empty = asyncio.run(scraper._rescrape_empty_series([self._empty_result(url_a)]))  # type: ignore[arg-type]

        self.assertEqual(still_empty, [])
        self.assertEqual(scraper.failed_links, [keep_b], "only the recovered series' entry is dropped")


class TestSessionExpiryRecovers(QuietCase):
    """One shared session serves the run, so an expiry must be recoverable."""

    def test_relogin_is_attempted_and_capped(self):
        scraper = SCRAPER_CLS()
        logins = {"n": 0}

        async def fake_login(client, *a, **kw):
            logins["n"] += 1

        scraper._login_client = fake_login
        client = object()
        for _ in range(sc._MAX_RELOGINS + 3):
            asyncio.run(scraper._relogin_shared_client(client))
        self.assertEqual(logins["n"], sc._MAX_RELOGINS, "re-login must be capped per run")

    def test_a_failed_relogin_reports_false(self):
        scraper = SCRAPER_CLS()

        async def boom(client, *a, **kw):
            raise RuntimeError("login refused")

        scraper._login_client = boom
        self.assertFalse(asyncio.run(scraper._relogin_shared_client(object())))

    def test_the_relogin_really_calls_login_and_not_just_something_shaped_like_it(self):
        """Enforce the real _login_client signature, not a permissive stub.

        The two tests above hand this path a `(client, *a, **kw)` stub, which
        accepts any call at all -- including one the real method rejects. That
        is exactly how s.to shipped a re-login that called _login_client with
        too few arguments: the TypeError landed in the broad `except Exception`
        below it, so the run logged "re-login after session expiry failed" and
        gave up without ever sending a login. Every worker shares the one
        session, so every remaining series in the run failed with it.

        autospec builds the double from the real signature, so a call the real
        method could not accept fails here too.
        """
        scraper = SCRAPER_CLS()
        with mock.patch.object(SCRAPER_CLS, "_login_client", autospec=True) as login:
            recovered = asyncio.run(scraper._relogin_shared_client(object()))

        self.assertTrue(recovered, "re-login reported failure")
        self.assertEqual(login.await_count, 1, "no login was actually attempted")


class TestRenameGuessNeverSkipsAScrape(QuietCase):
    """A fuzzy title score must not decide what gets scraped."""

    def test_unrelated_titles_are_not_called_renames(self):
        for a, b in (
            ("One Piece", "One Punch Man"),
            ("Death Note", "Deadman Wonderland"),
            ("Bleach", "Beelzebub"),
        ):
            with self.subTest(pair=(a, b)):
                hits = sc._find_vanished_renames(
                    [(a, series_url(a.lower().replace(" ", "-")))],
                    [{"title": b, "url": series_url(b.lower().replace(" ", "-"))}],
                )
                self.assertEqual(hits, set(), f"{a!r} and {b!r} are different shows")

    def test_an_obvious_rename_is_still_flagged(self):
        hits = sc._find_vanished_renames(
            [("Steins;Gate", series_url("steins-gate"))],
            [{"title": "Steins;Gate 0", "url": series_url("steins-gate-0")}],
        )
        self.assertEqual(hits, {"Steins;Gate 0"})

    def test_a_flagged_rename_is_still_scraped(self):
        scraper = SCRAPER_CLS()
        scraper._vanished_index_entries = lambda all_series: [("Steins;Gate", series_url("steins-gate"))]
        new_entries = [
            {"title": "Steins;Gate 0", "url": series_url("steins-gate-0")},
            {"title": "Unrelated Show", "url": series_url("unrelated-show")},
        ]
        to_scrape, renames = scraper._filter_new_entries(new_entries, [])
        self.assertEqual(renames, {"Steins;Gate 0"}, "the suspicion is still reported")
        self.assertEqual(
            [s["title"] for s in to_scrape],
            ["Steins;Gate 0", "Unrelated Show"],
            "nothing may be dropped from the scrape on a guess",
        )


class TestShortCatalogueIsQueried(TempDirCase):
    """A truncated catalogue makes every absent series look vanished."""

    def _scraper_with_index(self, count):
        entries = [
            {"title": f"S{i}", "url": series_url(f"s{i}"), "link": series_url(f"s{i}"), "seasons": []}
            for i in range(count)
        ]
        with open(self.index_path, "w", encoding="utf-8") as fh:
            json.dump(entries, fh)
        previous = _set_module_index_path(sc, self.index_path)
        self.addCleanup(_restore_module_index_path, sc, previous)
        return SCRAPER_CLS()

    @staticmethod
    def _catalogue(n):
        return [{"title": f"S{i}", "link": series_url(f"s{i}")} for i in range(n)]

    def test_a_full_catalogue_asks_nothing(self):
        scraper = self._scraper_with_index(100)
        with mock.patch("builtins.input", side_effect=AssertionError("must not prompt")):
            self.assertTrue(scraper._confirm_catalogue_size(self._catalogue(100)))

    def test_a_slightly_smaller_catalogue_asks_nothing(self):
        scraper = self._scraper_with_index(100)
        with mock.patch("builtins.input", side_effect=AssertionError("must not prompt")):
            self.assertTrue(scraper._confirm_catalogue_size(self._catalogue(96)))

    def test_a_short_catalogue_asks_and_can_continue(self):
        scraper = self._scraper_with_index(100)
        with mock.patch("builtins.input", return_value="y"):
            self.assertTrue(scraper._confirm_catalogue_size(self._catalogue(50)))

    def test_a_short_catalogue_can_be_cancelled(self):
        scraper = self._scraper_with_index(100)
        with mock.patch("builtins.input", return_value="n"):
            self.assertFalse(scraper._confirm_catalogue_size(self._catalogue(50)))

    def test_an_empty_catalogue_asks(self):
        scraper = self._scraper_with_index(100)
        with mock.patch("builtins.input", return_value="n"):
            self.assertFalse(scraper._confirm_catalogue_size([]))

    def test_a_small_index_is_not_nagged(self):
        """A fresh install has almost nothing indexed; do not prompt then."""
        scraper = self._scraper_with_index(5)
        with mock.patch("builtins.input", side_effect=AssertionError("must not prompt")):
            self.assertTrue(scraper._confirm_catalogue_size([]))


class TestMergeDoesNotMutateItsInputs(QuietCase):
    """Merging twice must give the same answer twice.

    The merge resolves each episode's watch flag by writing it into the new
    entry, so it used to hand the caller back rewritten data -- the scraper's
    own series_data was altered as a side effect of saving.
    """

    @staticmethod
    def _fixture():
        url = series_url("show")
        return {
            "Show": {
                "title": "Show",
                "url": url,
                "link": url,
                "subscribed": False,
                "watchlist": False,
                "seasons": [
                    {
                        "season": "S1",
                        "url": url,
                        "episodes": [{"number": 1, "watched": True}],
                    }
                ],
            }
        }

    @staticmethod
    def _merge(old, new, allowed):
        return im._build_merged_data(old, new, allowed)

    def test_the_new_dict_is_not_rewritten(self):
        old = {}
        new = self._fixture()
        snapshot = copy.deepcopy(new)
        allowed = dict.fromkeys(
            [
                "new_series",
                "new_episodes",
                "watched",
                "unwatched",
                "subscribe",
                "unsubscribe",
                "watchlist_add",
                "watchlist_remove",
                "title_ger",
                "title_eng",
                "episode_remove",
                "season_remove",
            ],
            True,
        )
        self._merge(old, new, allowed)
        self.assertEqual(new, snapshot, "the caller's data must come back unchanged")

    def test_merging_twice_gives_the_same_result(self):
        old = {}
        new = self._fixture()
        deny = dict.fromkeys(
            [
                "new_series",
                "new_episodes",
                "watched",
                "unwatched",
                "subscribe",
                "unsubscribe",
                "watchlist_add",
                "watchlist_remove",
                "title_ger",
                "title_eng",
                "episode_remove",
                "season_remove",
            ],
            False,
        )
        allow = {**deny, "new_series": True, "new_episodes": True, "watched": True}
        first = self._merge(old, new, allow)
        second = self._merge(old, new, allow)
        self.assertEqual(
            [e["watched"] for e in first["Show"]["seasons"][0]["episodes"]],
            [e["watched"] for e in second["Show"]["seasons"][0]["episodes"]],
            "a second identical merge must not drift",
        )


class TestVanishedDecisionPrompt(QuietCase):
    """The vanished-series prompt uses a side-by-side table with per-row actions."""

    @staticmethod
    def _entries(n):
        return [(f"Show{i}", "gone", series_url(f"s{i}")) for i in range(n)]

    def test_keep_is_typed_not_assumed(self):
        # Enter used to keep a row. There are no defaults now: it is asked
        # again, and only a typed "k" keeps.
        with mock.patch("builtins.input", side_effect=["", "k", "k", "k", "k", "k"]) as feeder:
            self.assertEqual(im._prompt_vanished_table(self._entries(5), {}, {}), [])
        self.assertEqual(feeder.call_count, 6)

    def test_delete_per_item_with_confirmation(self):
        # "d" triggers a y/n confirmation prompt
        inputs = ["d", "y", "k", "d", "y", "k", "k"]
        with mock.patch("builtins.input", side_effect=inputs):
            result = im._prompt_vanished_table(self._entries(5), {}, {})
            self.assertEqual(len(result), 2)

    def test_an_endless_stream_of_unrecognized_answers_terminates(self):
        """Regression: a constant answer that is not a row action must not
        re-prompt forever.  A scripted feed of the removed "y"/"n" vocabulary
        used to spin this loop, and its accumulating prompts ate tens of
        gigabytes of memory before the run was killed.  The prompt now stops
        re-asking after a bounded number of bad answers and keeps the entry.
        """
        with mock.patch("builtins.input", return_value="n") as feeder:
            result = im._prompt_vanished_table(self._entries(5), {}, {})
        self.assertEqual(result, [], "a stalled feed must keep every entry")
        # Every row was reached: the bounded loop did not stop the whole table,
        # and no row needed more than the cap plus its final keep answer.
        self.assertLessEqual(feeder.call_count, len(self._entries(5)) * (im._PROMPT_MAX_UNRECOGNIZED + 1))

    def test_a_few_bad_answers_still_get_another_chance(self):
        # Typos re-prompt; only a sustained stream gives up. "d" then a
        # confirmation after two mistakes proves the counter resets per row.
        inputs = ["x", "x", "d", "y", "k"]
        with mock.patch("builtins.input", side_effect=inputs):
            result = im._prompt_vanished_table(self._entries(2), {}, {})
            self.assertEqual(result, ["Show0"])

    def test_eof_on_delete_confirmation_keeps_the_entry(self):
        """stdin closing right after a "d" must not crash the prompt.

        The row prompt is EOF-guarded, but the delete confirmation used to
        call input() bare: an EOF there escaped _prompt_vanished_table and
        killed the whole cleanup, losing the keep decisions already made.
        """
        with mock.patch("builtins.input", side_effect=["d", EOFError, "k"]) as feeder:
            result = im._prompt_vanished_table(self._entries(2), {}, {})
        self.assertEqual(result, [], "the EOF'd row must be kept, not deleted")
        self.assertLess(feeder.call_count, 10, "the prompt must not loop after EOF")

    def test_open_all_opens_this_and_remaining_rows(self):
        """'oa' hands the current row and every remaining row to the confirmed-
        batch helper -- not one tab per keystroke. After it, row 1 still needs
        its own decision, so 'oa' never deletes or keeps anything by itself."""
        with (
            mock.patch.object(im, "_open_rows_in_browser", autospec=True) as bulk,
            mock.patch.object(im, "_open_urls_for_comparison", autospec=True) as single,
            mock.patch("builtins.input", side_effect=["oa", "k", "k", "k", "k", "k"]) as feeder,
        ):
            result = im._prompt_vanished_table(self._entries(5), {}, {})
        self.assertEqual(result, [], "no explicit delete means everything is kept")
        single.assert_not_called()  # 'oa' must go through the batched helper
        opened_rows = bulk.call_args[0][0]
        self.assertEqual([r["v_title"] for r in opened_rows], [f"Show{i}" for i in range(5)])
        self.assertEqual(feeder.call_count, 6, "'oa' for row 1, then one answer per row")

    def test_open_all_then_normal_decisions_still_work(self):
        """After an 'oa' the per-row prompt keeps walking: row 1 can still be
        deleted right after the bulk open, and later rows keep their own
        decisions."""
        with (
            mock.patch.object(im, "_open_rows_in_browser", autospec=True) as bulk,
            mock.patch("builtins.input", side_effect=["oa", "d", "y", "k", "k", "k"]) as feeder,
        ):
            result = im._prompt_vanished_table(self._entries(4), {}, {})
        self.assertEqual(result, ["Show0"], "only the explicitly-deleted row goes")
        self.assertEqual(len(bulk.call_args[0][0]), 4, "all 4 rows went to the bulk open")
        self.assertEqual(feeder.call_count, 6)


class TestVerifyAcceptsBothVanishedShapes(QuietCase):
    """The index hands verification 3-tuples; the row prompt hands it 2-tuples.

    Unpacking only the 2-tuple shape crashed the whole verification step the
    moment the user answered "y" to the re-scrape prompt.
    """

    def _verify(self, entries):
        scraper = SCRAPER_CLS()
        # Empty URLs short-circuit before any request, so this stays offline.
        with mock.patch.object(SCRAPER_CLS, "_login_client", new=mock.AsyncMock()):
            return asyncio.run(scraper.verify_vanished_and_candidates(entries, []))

    # With no URL nothing is fetched, so the verdict is None ("could not tell"),
    # not False ("the site says it is gone"); see TestVanishedCheckTellsGoneFromUnknown.
    def test_three_tuple_entries_do_not_raise(self):
        verified, _ = self._verify([("Show", "not found on aniworld.to", "")])
        self.assertEqual(verified, [("Show", "", None)])

    def test_two_tuple_entries_do_not_raise(self):
        verified, _ = self._verify([("Show", "")])
        self.assertEqual(verified, [("Show", "", None)])


class TestRescrapeTrustsReachability(QuietCase):
    """A rescrape may only rewrite a row when the page was actually reached."""

    class _FakeScraper:
        """Mirrors the real contract: every entry comes back either way, and
        only the flag says whether the fetch landed."""

        def __init__(self, reachable):
            self.reachable = reachable

        async def verify_vanished_and_candidates(self, vanished, candidates):
            verified_vanished = [("Renamed Title", url, self.reachable) for _title, url in vanished]
            verified_candidates = []
            for entry in candidates:
                verified = dict(entry)
                verified["title"] = "Verified New Title"
                verified["_verified_reachable"] = self.reachable
                verified_candidates.append(verified)
            return verified_vanished, verified_candidates

    @staticmethod
    def _row():
        return {
            "v_title": "Old Title",
            "v_url": series_url("old"),
            "old_entry": {},
            "n_title": "New Title",
            "n_url": series_url("new"),
            "new_entry": {"title": "New Title", "url": series_url("new")},
            "reason": "weak",
        }

    def test_unreachable_leaves_row_untouched(self):
        row = self._row()
        self.assertFalse(im._rescrape_row(row, self._FakeScraper(False), {}))
        self.assertEqual(row["v_title"], "Old Title")
        self.assertEqual(row["n_title"], "New Title")

    def test_reachable_updates_row(self):
        row = self._row()
        self.assertTrue(im._rescrape_row(row, self._FakeScraper(True), {}))
        self.assertEqual(row["v_title"], "Renamed Title")
        self.assertEqual(row["n_title"], "Verified New Title")

    def test_missing_scraper_is_reported_not_raised(self):
        row = self._row()
        self.assertFalse(im._rescrape_row(row, None, {}))
        self.assertEqual(row["v_title"], "Old Title")


class TestBatchFileExportAppends(QuietCase):
    """Exporting ongoing URLs must not wipe a hand-curated batch file.

    The export used to open the file in "w" mode, so a list built up by hand
    -- comments included -- was replaced by whatever that one report happened
    to consider ongoing.
    """

    def setUp(self):
        super().setUp()
        self._d = tempfile.TemporaryDirectory()
        self.addCleanup(self._d.cleanup)
        self.path = os.path.join(self._d.name, "series_urls.txt")

    def _append(self, urls):
        return main._append_urls_to_batch_file(self.path, urls)

    def _read(self):
        with open(self.path, encoding="utf-8") as fh:
            return fh.read()

    def test_the_file_is_created_when_missing(self):
        added, skipped = self._append([series_url("alpha")])
        self.assertEqual((len(added), skipped), (1, 0))
        self.assertTrue(os.path.exists(self.path))

    def test_existing_entries_and_comments_survive(self):
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write("# my notes\n" + series_url("mine") + "\n")
        self._append([series_url("alpha")])
        content = self._read()
        self.assertIn("# my notes", content, "comments must be kept")
        self.assertIn(series_url("mine"), content, "hand-added URLs must be kept")
        self.assertIn(series_url("alpha"), content, "the new URL must be appended")

    def test_a_repeated_export_adds_nothing(self):
        self._append([series_url("alpha")])
        added, skipped = self._append([series_url("alpha")])
        self.assertEqual((len(added), skipped), (0, 1))
        self.assertEqual(self._read().count(series_url("alpha")), 1)

    def test_a_commented_out_url_is_not_revived(self):
        """Commenting a line out is a decision to skip it; keep it that way."""
        commented = "# " + series_url("paused") + "\n"
        with open(self.path, "w", encoding="utf-8") as fh:
            fh.write(commented)
        added, skipped = self._append([series_url("paused")])
        self.assertEqual((len(added), skipped), (0, 1))
        self.assertEqual(self._read(), commented)


class TestSingleUrlRunReportsProgress(unittest.TestCase):
    """A one-series scrape used to finish silently.

    The single-URL path called _scrape_one_series directly, so it never
    entered the worker pool -- and the progress line, the episode counts and
    the empty-page warnings all live in the pool. Every other mode reported;
    this one printed nothing between "logged in" and the save.

    Asserted on the printed line rather than on which method gets called, so
    a later refactor that keeps the reporting keeps the test.
    """

    def _run(self, result):
        scraper = SCRAPER_CLS()
        tmp = mock.AsyncMock()
        tmp.is_closed = False

        async def fake_scrape(_client, info):
            return dict(result, url=info["url"], link=info["link"])

        scraper._scrape_one_series = fake_scrape  # type: ignore[method-assign]
        scraper._acquire_client = lambda: asyncio.sleep(0, result=object())  # type: ignore[method-assign]
        scraper._release_client = lambda: asyncio.sleep(0)
        scraper.clear_checkpoint = lambda: None

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            asyncio.run(scraper._async_run_inner(tmp, single_url=series_url("some-show")))
        return buf.getvalue()

    def test_a_successful_single_scrape_prints_the_progress_line(self):
        out = self._run(
            {
                "title": "Some Show",
                "total_episodes": 11,
                "watched_episodes": 11,
                "seasons": [{"season": "1"}],
            }
        )
        self.assertIn("[1/1]", out)
        self.assertIn("100%", out)
        self.assertIn("ETA:", out)
        self.assertIn("Some Show", out)
        self.assertIn("11/11 watched", out)

    def test_a_failed_single_scrape_says_so_instead_of_nothing(self):
        out = self._run(
            {
                "title": "Some Show",
                "_error": True,
                "_error_reason": "network unreachable",
                "total_episodes": 0,
                "watched_episodes": 0,
                "seasons": [],
            }
        )
        self.assertIn("[1/1]", out)
        self.assertIn("network unreachable", out)

    def test_an_empty_series_is_flagged_rather_than_stored_quietly(self):
        out = self._run(
            {
                "title": "Some Show",
                "total_episodes": 0,
                "watched_episodes": 0,
                "seasons": [],
            }
        )
        self.assertIn("[1/1]", out)
        self.assertIn("No episodes", out)


class TestStartupProbeFetchesHostsTogether(QuietCase):
    """Startup used to download every host's catalogue one after another.

    Three multi-megabyte catalogue pages in series were most of the wait
    between launching the program and seeing the menu, for no reason: the
    hosts are independent servers. They now go out at once.

    That is only safe if each host gets its own scraper.
    get_catalogue_info_for_site sets self.site_url for the duration of the
    call, so concurrent hosts sharing one scraper would overwrite each other's
    target -- and a count cross-checked against a different host's slug set is
    wrong in a way that still looks like a plausible number, which is the
    worst kind of wrong for this program.
    """

    HOSTS = ["https://a.test", "https://b.test", "https://c.test"]

    def setUp(self):
        super().setUp()
        # _probe_sites_before_scrape publishes the chosen host globally.
        previous = getattr(main, "ACTIVE_SITE_URL", None)
        self.addCleanup(setattr, main, "ACTIVE_SITE_URL", previous)

    @staticmethod
    def _empty_index():
        idx = mock.Mock()
        idx.series_index = {}
        return idx

    def test_every_host_is_fetched_and_its_result_stays_with_it(self):
        async def fake(self_, site_url):
            await asyncio.sleep(0)
            return len(site_url), {site_url}

        with mock.patch.object(SCRAPER_CLS, "get_catalogue_info_for_site", fake):
            result = main._fetch_catalogue_info_for_hosts(SCRAPER_CLS(), self.HOSTS)

        self.assertEqual(sorted(result), sorted(self.HOSTS))
        for host in self.HOSTS:
            count, slugs = result[host]
            self.assertEqual(count, len(host))
            self.assertEqual(slugs, {host})

    def test_the_fetches_overlap_instead_of_running_one_at_a_time(self):
        events = []

        async def fake(self_, site_url):
            events.append(("start", site_url))
            await asyncio.sleep(0.02)
            events.append(("end", site_url))
            return 1, set()

        with mock.patch.object(SCRAPER_CLS, "get_catalogue_info_for_site", fake):
            main._fetch_catalogue_info_for_hosts(SCRAPER_CLS(), self.HOSTS)

        # Ordering, not wall time, so this cannot go flaky on a slow machine:
        # if the hosts ran in series the first "end" would land before the
        # second "start".
        self.assertEqual([kind for kind, _ in events[:3]], ["start"] * 3)

    def test_each_host_gets_its_own_scraper(self):
        used = []

        async def fake(self_, site_url):
            used.append(self_)  # a strong ref, so ids cannot be recycled
            return 1, set()

        shared = SCRAPER_CLS()
        with mock.patch.object(SCRAPER_CLS, "get_catalogue_info_for_site", fake):
            main._fetch_catalogue_info_for_hosts(shared, self.HOSTS)

        self.assertEqual(len({id(scraper) for scraper in used}), len(self.HOSTS))
        self.assertNotIn(id(shared), [id(scraper) for scraper in used])

    def test_one_hosts_failure_does_not_take_the_others_down(self):
        async def fake(self_, site_url):
            if site_url == self.HOSTS[1]:
                raise RuntimeError("host exploded")
            return 7, {"slug"}

        with mock.patch.object(SCRAPER_CLS, "get_catalogue_info_for_site", fake):
            result = main._fetch_catalogue_info_for_hosts(SCRAPER_CLS(), self.HOSTS)

        self.assertEqual(result[self.HOSTS[1]], (None, set()))
        self.assertEqual(result[self.HOSTS[0]], (7, {"slug"}))
        self.assertEqual(result[self.HOSTS[2]], (7, {"slug"}))

    def test_no_reachable_hosts_means_no_fetch_at_all(self):
        called = []

        async def fake(self_, site_url):
            called.append(site_url)
            return 1, set()

        with mock.patch.object(SCRAPER_CLS, "get_catalogue_info_for_site", fake):
            self.assertEqual(main._fetch_catalogue_info_for_hosts(SCRAPER_CLS(), []), {})

        self.assertEqual(called, [])

    def test_an_unreachable_host_is_never_asked_for_its_catalogue(self):
        """A dead mirror should cost its probe, not a second full timeout."""
        asked = {}

        def fake_probe(scraper, site_urls):
            return [
                {"site_url": site_urls[0], "ok": True, "status_code": 200},
                {"site_url": site_urls[1], "ok": False, "status_code": None},
            ]

        def fake_fetch(scraper, site_urls):
            asked["hosts"] = list(site_urls)
            return {url: (1, set()) for url in site_urls}

        with (
            mock.patch.object(main, "SITE_URLS", self.HOSTS[:2]),
            mock.patch.object(main, "_probe_hosts", fake_probe),
            mock.patch.object(main, "_fetch_catalogue_info_for_hosts", fake_fetch),
        ):
            main._probe_sites_before_scrape(SCRAPER_CLS(), idx_mgr=self._empty_index())

        self.assertEqual(asked["hosts"], [self.HOSTS[:1][0]])

    def test_the_probe_reuses_the_index_main_already_loaded(self):
        """Reloading it here parsed the same file a second time for the same
        result -- half a second of startup on the larger indexes."""

        def fake_probe(scraper, site_urls):
            return [{"site_url": url, "ok": True, "status_code": 200} for url in site_urls]

        def fake_fetch(scraper, site_urls):
            return {url: (1, set()) for url in site_urls}

        def no_reload(*args, **kwargs):
            raise AssertionError("the probe reloaded the index instead of reusing it")

        with (
            mock.patch.object(main, "SITE_URLS", self.HOSTS[:1]),
            mock.patch.object(main, "_probe_hosts", fake_probe),
            mock.patch.object(main, "_fetch_catalogue_info_for_hosts", fake_fetch),
            mock.patch.object(main, "IndexManager", no_reload),
        ):
            main._probe_sites_before_scrape(SCRAPER_CLS(), idx_mgr=self._empty_index())

    # ── which host ends up active ──────────────────────────────────────────

    def _choose_host(self, served):
        """Run the probe with every host reachable and `served` deciding which
        ones actually return a catalogue; return the host left active."""

        def fake_probe(scraper, site_urls):
            return [{"site_url": url, "ok": True, "status_code": 200} for url in site_urls]

        def fake_fetch(scraper, site_urls):
            return {url: ((10, {"a"}) if served.get(url) else (None, set())) for url in site_urls}

        scraper = SCRAPER_CLS()
        with (
            mock.patch.object(main, "SITE_URLS", self.HOSTS),
            mock.patch.object(main, "_probe_hosts", fake_probe),
            mock.patch.object(main, "_fetch_catalogue_info_for_hosts", fake_fetch),
            # No host serving now starts a countdown; answer it with "skip to
            # the menu" so the fallback choice below is what gets tested.
            mock.patch.object(main, "_wait_before_host_retry", lambda *args, **kwargs: False),
        ):
            main._probe_sites_before_scrape(scraper, idx_mgr=self._empty_index())
        return scraper.site_url

    def test_a_host_that_failed_its_catalogue_is_not_made_active(self):
        """Reachable is not the same as serving.

        The active host used to be the first one that answered the probe, even
        when that host had just failed to return a catalogue and another had
        succeeded. Scraping it then fails outright, or -- worse -- returns a
        short catalogue, and a short catalogue makes every indexed series look
        vanished and offers thousands of good entries for deletion.
        """
        active = self._choose_host({self.HOSTS[0]: False, self.HOSTS[1]: True, self.HOSTS[2]: True})
        self.assertEqual(active, self.HOSTS[1])

    def test_the_first_serving_host_is_still_preferred(self):
        active = self._choose_host(dict.fromkeys(self.HOSTS, True))
        self.assertEqual(active, self.HOSTS[0])

    def test_when_no_host_serves_the_probe_order_still_decides(self):
        """With nothing to choose between, behave exactly as before."""
        active = self._choose_host(dict.fromkeys(self.HOSTS, False))
        self.assertEqual(active, self.HOSTS[0])


class TestCatalogueLoginSkipsTheSecondDownload(QuietCase):
    """The startup catalogue fetch downloaded its page twice per host.

    _login_client proves a login worked by fetching a known-good page and
    checking it looks logged in. For two of these three sites that page IS the
    catalogue -- which _get_all_series then downloads again and checks again,
    with the same predicate. Once per host, on the largest page of the run.
    The third verifies on the homepage, so it downloaded a second large page
    it then discarded.

    The verify is now optional and only the catalogue path turns it off. What
    must not change is the guarantee behind it: a login that did not work has
    to come back as "no catalogue", never as an empty or partial one, because
    an empty catalogue makes every indexed series look vanished.
    """

    HOST = "https://probe.test"

    def _scraper(self, series=None, series_error=None):
        """A scraper whose login and catalogue fetch are both stubbed out."""
        scraper = SCRAPER_CLS()
        self.seen = {}

        async def record_login(client, *args, verify=True, **kwargs):
            self.seen["verify"] = verify

        async def fake_series(client):
            if series_error is not None:
                raise series_error
            return series or []

        scraper._login_client = record_login
        scraper._get_all_series = fake_series
        return scraper

    def test_the_catalogue_path_asks_login_not_to_verify(self):
        scraper = self._scraper([{"title": "A", "link": series_url("a")}])
        count, slugs = asyncio.run(scraper.get_catalogue_info_for_site(self.HOST))

        self.assertIs(self.seen["verify"], False, "the verify fetch was not skipped")
        self.assertEqual(count, 1)
        self.assertEqual(slugs, {"a"})

    def test_every_other_caller_still_gets_the_verification(self):
        scraper = self._scraper()
        client = asyncio.run(scraper._create_logged_in_client())
        asyncio.run(client.aclose())

        self.assertIs(self.seen["verify"], True, "the default must still verify")

    def test_a_login_that_did_not_work_is_reported_as_no_catalogue(self):
        """Skipping the verify must not turn a failed login into 0 series."""
        scraper = self._scraper(series_error=RuntimeError("Not logged in"))

        self.assertEqual(
            asyncio.run(scraper.get_catalogue_info_for_site(self.HOST)),
            (None, set()),
        )

    def test_a_genuinely_empty_catalogue_is_not_confused_with_a_failure(self):
        scraper = self._scraper([])

        self.assertEqual(
            asyncio.run(scraper.get_catalogue_info_for_site(self.HOST)),
            (0, set()),
        )

    def test_the_active_host_is_put_back_afterwards(self):
        """Each host is probed on its own scraper now, but this one is shared
        with the caller in every other mode, so it must come back unchanged."""
        scraper = self._scraper([{"title": "A", "link": series_url("a")}])
        before = scraper.site_url
        asyncio.run(scraper.get_catalogue_info_for_site(self.HOST))

        self.assertEqual(scraper.site_url, before)


class _CannedResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


class _RecordingClient:
    """Stands in for httpx.AsyncClient and records what it was asked to fetch."""

    def __init__(self, requests, response):
        self._requests = requests
        self._response = response
        # httpx.AsyncClient exposes this and one of these scrapers checks it
        # while cleaning up, so the double has to carry it too.
        self.is_closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        self._requests.append(str(url))
        return self._response

    async def aclose(self):
        self.is_closed = True


class TestHostChecksTargetTheRightHost(QuietCase):
    """A per-host check has to actually talk to that host.

    aniworld built its login, catalogue and account URLs from module constants
    baked to SITE_URL, so get_catalogue_info_for_site(host) logged in to the
    primary host and fetched the primary host's catalogue no matter which host
    it was asked about. The startup table then showed one host's count in all
    three rows, "cross-host counts: match" compared the primary against
    itself, and a run whose primary was down would pick a working mirror and
    then ignore it. Both sibling scrapers already passed the host through;
    these tests keep all three honest.
    """

    HOST = "https://mirror.test"

    def _probe(self, status=200, body='<form action="/login"><input type="password"></form>'):
        requests = []
        response = _CannedResponse(status, body)
        with mock.patch.object(sc.httpx, "AsyncClient", lambda *a, **kw: _RecordingClient(requests, response)):
            result = asyncio.run(SCRAPER_CLS()._probe_one_site(self.HOST))
        return result, requests

    def test_the_probe_reads_the_login_page_of_the_host_it_was_given(self):
        _result, requests = self._probe()

        self.assertEqual(len(requests), 1, requests)
        self.assertTrue(requests[0].startswith(self.HOST), requests[0])
        self.assertIn("login", requests[0].lower(), requests[0])

    def test_a_host_that_answers_with_something_else_is_not_reachable(self):
        """A stale mirror serving a 200 placeholder is not a working host."""
        result, _requests = self._probe(status=200, body="<html>parked domain</html>")

        self.assertFalse(result["ok"])

    def test_a_real_login_page_is_reachable(self):
        result, _requests = self._probe(status=200, body='<form action="/login"><input type="password"></form>')

        self.assertTrue(result["ok"])

    def test_a_server_error_is_not_reachable(self):
        result, _requests = self._probe(status=503, body="Login")

        self.assertFalse(result["ok"])

    def test_the_catalogue_is_fetched_from_the_host_it_was_asked_about(self):
        scraper = SCRAPER_CLS()
        scraper.site_url = self.HOST
        fetched = []

        async def record_get(client, url, *args, **kwargs):
            fetched.append(str(url))
            raise RuntimeError("stop once the request is recorded")

        scraper._get = record_get
        with contextlib.suppress(RuntimeError):
            asyncio.run(scraper._get_all_series(object()))  # type: ignore[arg-type]

        self.assertTrue(fetched, "no catalogue request was made at all")
        self.assertTrue(fetched[0].startswith(self.HOST), fetched[0])

    # ── what counts as a login page ────────────────────────────────────────

    ACCEPTED = {
        "a reworded page that still has a password field": '<html><form><input type="password" name="p"></form></html>',
        "single-quoted type": "<input type='password'>",
        "unquoted type": "<input type=password>",
        "a form posting to the login endpoint": '<html><form action="/login" method="post"></form></html>',
        "an absolute login action": "<html><form action='https://mirror.test/login'></form></html>",
    }

    REJECTED = {
        "an empty body": "",
        "a parked domain": "<html><body><h1>Domain for sale</h1></body></html>",
        "a bare gateway error": "<html><body>502 Bad Gateway</body></html>",
        "something that is not markup": "not markup at all",
    }

    def test_a_real_login_page_is_recognised_however_it_is_worded(self):
        """The probe used to ask only whether the word "login" appeared.

        One English substring decided which mirrors were usable, so rewording
        or translating that page would have taken every host down at once. A
        password field carries the same meaning without depending on wording.
        """
        for label, html in self.ACCEPTED.items():
            with self.subTest(label):
                self.assertTrue(sc._looks_like_login_page(html))

    def test_a_host_serving_something_else_is_still_rejected(self):
        for label, html in self.REJECTED.items():
            with self.subTest(label):
                self.assertFalse(sc._looks_like_login_page(html))

    def test_wording_alone_no_longer_makes_a_host_usable(self):
        """Deliberately narrower than the old word test, which was too wide.

        This used to assert the opposite -- that the check may only ever grow
        more accepting -- on the grounds that a working host must never start
        reading as down. That ratchet was the defect: "login" appears in the
        nav of a parked domain and in the body of a Cloudflare block page, so
        accepting the bare word accepted exactly the impostors the probe
        exists to screen out, and the host it picked became the active one.

        Nothing real is lost by tightening. A login page has a password field
        -- that is how a browser is told to mask the input -- and a form
        posting to /login is kept as the structural alternative, so both
        signals survive a rewording or a translation. What no longer counts
        is the word on its own.
        """
        for html in (
            "<p>Login</p>",
            "please LOGIN here",
            "<form>login</form>",
            "<html><body>Anmelden oder Login</body></html>",
            "<html><title>Attention Required! | Cloudflare</title>"
            "<body>Error 1020<a href='/login'>Login</a></body></html>",
        ):
            with self.subTest(html):
                self.assertIn("login", html.lower(), "sample must match the old rule")
                self.assertFalse(sc._looks_like_login_page(html))


class TestIndexEntriesSurviveAMirrorChange(QuietCase):
    """Retiring a mirror must not delete the series scraped from it.

    Index entries store an absolute URL carrying whatever host was live when
    they were scraped. Validation rejected any host missing from the current
    _SITE_URLS, load_index dropped those entries, and the very next save
    wrote the shortened index back to disk -- taking the series and every
    watched episode with it. Because an index is normally uniformly on one
    host, one config edit could take all of it.
    """

    RETIRED = "a-retired-mirror.example"

    def _index(self, *entries) -> str:
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, "series_index.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(list(entries), fh)
        return path

    def _series(self, title: str, host: str, watched: int = 12):
        slug = title.lower().replace(" ", "-")
        path = im._VALID_SERIES_PATH_RE.pattern.split("[")[0]
        return {
            "title": title,
            "url": f"https://{host}{path}{slug}",
            "seasons": [
                {
                    "season": "Season 1",
                    "episodes": [{"number": n, "watched": n <= watched} for n in range(1, 13)],
                    "total_episodes": 12,
                    "watched_episodes": watched,
                }
            ],
        }

    def test_an_entry_on_a_retired_mirror_is_kept_not_dropped(self):
        live = sorted(im.VALID_SERIES_HOSTS)[0]
        path = self._index(self._series("Kept", live), self._series("Stale", self.RETIRED))

        manager = im.IndexManager(path)

        self.assertEqual(sorted(manager.series_index), ["Kept", "Stale"])

    def test_its_watch_history_survives(self):
        path = self._index(self._series("Stale", self.RETIRED, watched=7))

        manager = im.IndexManager(path)
        total, watched = im.get_episode_counts(manager.series_index["Stale"])

        self.assertEqual((total, watched), (12, 7))

    def test_its_host_is_repointed_to_a_configured_one(self):
        path = self._index(self._series("Stale", self.RETIRED))

        manager = im.IndexManager(path)

        self.assertNotIn(self.RETIRED, manager.series_index["Stale"]["url"])
        self.assertTrue(im._is_valid_series_url(manager.series_index["Stale"]["url"]))

    def test_a_later_save_does_not_write_the_entry_out_of_the_index(self):
        path = self._index(self._series("Kept", sorted(im.VALID_SERIES_HOSTS)[0]), self._series("Stale", self.RETIRED))

        manager = im.IndexManager(path)
        manager.save_index()

        with open(path, encoding="utf-8") as fh:
            on_disk = json.load(fh)
        self.assertEqual(sorted(entry["title"] for entry in on_disk), ["Kept", "Stale"])

    def test_a_genuinely_broken_url_is_still_rejected(self):
        """Only the host became forgiving. Dangerous schemes still go."""
        for url in ("javascript:alert(1)", "data:text/html,x", "file:///etc/passwd", "https://host/not-a-series"):
            with self.subTest(url):
                self.assertIsNone(im._series_path_of(url))


class TestRateGuardHoldsParkedWorkers(QuietCase):
    """An escalating penalty has to reach the workers already waiting.

    wait() computed its sleep once, so a second 429 arriving while a worker
    was parked -- which pushes the resume time out and doubles the penalty --
    never reached it: it woke at the original time and sent anyway, exactly
    when the site was pushing back hardest.
    """

    def test_a_penalty_raised_mid_sleep_still_holds_the_pool(self):
        async def scenario():
            guard = sc.RateGuard()
            sent = []

            async def worker(n):
                await guard.wait()
                sent.append(time.monotonic())

            guard.penalise(retry_after=0.20)
            tasks = [asyncio.create_task(worker(i)) for i in range(4)]
            await asyncio.sleep(0.05)
            pause = guard.penalise(retry_after=0.50)
            resume_at = time.monotonic() + pause
            await asyncio.gather(*tasks)
            return sent, resume_at

        sent, resume_at = asyncio.run(scenario())

        early = [t for t in sent if t < resume_at - 0.02]
        self.assertEqual(early, [], f"{len(early)} of {len(sent)} workers sent before the pool was released")

    def test_it_still_returns_promptly_when_nothing_is_pending(self):
        async def scenario():
            guard = sc.RateGuard()
            start = time.monotonic()
            await guard.wait()
            return time.monotonic() - start

        self.assertLess(asyncio.run(scenario()), 0.05)


if __name__ == "__main__":
    unittest.main()


class _IndexLoadCase(TempDirCase):
    """Shared scaffolding for the two index-loading regressions below.

    Not a test case itself: the helpers live here so that neither class
    inherits the other one's tests and runs them a second time.
    """

    JUNK = ["a bare string", 42, None, ["nested", "list"], True]

    def _series(self, title, watched=12):
        host = sorted(im.VALID_SERIES_HOSTS)[0]
        path = im._VALID_SERIES_PATH_RE.pattern.split("[")[0]
        return {
            "title": title,
            "url": f"https://{host}{path}{title.lower()}",
            "seasons": [
                {
                    "season": "Season 1",
                    "episodes": [{"number": n, "watched": n <= watched} for n in range(1, 13)],
                    "total_episodes": 12,
                    "watched_episodes": watched,
                }
            ],
        }

    def _write(self, data):
        with open(self.index_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

    def _corrupt_the_index(self):
        with open(self.index_path, "w", encoding="utf-8") as fh:
            fh.write("{ not json")

    def _write_bak2(self, data):
        with open(self.index_path + ".bak2", "w", encoding="utf-8") as fh:
            json.dump(data, fh)

    def _load(self):
        manager = im.IndexManager(self.index_path)
        manager.load_index()
        return manager


class TestJunkEntriesDoNotDiscardTheIndex(_IndexLoadCase):
    """One malformed element must cost that element, not the whole index.

    The list branch of the loader called ``.get("title")`` before checking
    ``isinstance(..., dict)``, so a stray string or number raised
    AttributeError. The broad handler around the load turned that into an
    empty index, and the next save wrote the emptiness to disk: one junk
    element silently destroyed every watch record in the file. The dict
    branch three lines below always had the guards the right way round,
    which is what makes this a slip rather than a design.
    """

    def test_a_stray_element_does_not_take_the_good_entries_with_it(self):
        self._write([self._series("Kept"), "a bare string", self._series("Also")])
        self.assertEqual(sorted(self._load().series_index), ["Also", "Kept"])

    def test_every_kind_of_junk_is_skipped_rather_than_fatal(self):
        for junk in self.JUNK:
            with self.subTest(junk=junk):
                self._write([self._series("Kept"), junk])
                self.assertEqual(sorted(self._load().series_index), ["Kept"])

    def test_the_surviving_entry_keeps_its_watch_history(self):
        """Reading the title back is not enough if the episodes came back blank."""
        self._write([self._series("Kept", watched=7), "a bare string"])
        total, watched = im.get_episode_counts(self._load().series_index["Kept"])
        self.assertEqual((total, watched), (12, 7))

    def test_an_index_of_nothing_but_junk_loads_empty_instead_of_raising(self):
        """Empty is the right answer; how it is reached is the point.

        Before the fix this also ended up empty -- by raising and being
        swallowed -- so asserting only on the result would pass against the
        bug. The warning is what separates "skipped the junk" from "gave up".
        """
        self._write(list(self.JUNK))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            manager = self._load()
        self.assertEqual(dict(manager.series_index), {})
        self.assertNotIn("Error loading index", out.getvalue())

    def test_a_title_less_entry_is_dropped_not_stored_under_a_none_key(self):
        """A None key breaks every later sorted() over the index."""
        self._write([self._series("Kept"), {"url": "https://example.invalid/x", "seasons": []}])
        self.assertNotIn(None, self._load().series_index)

    def test_junk_in_a_dict_shaped_index_is_skipped_too(self):
        self._write({"one": self._series("Kept"), "two": "a bare string"})
        index = self._load().series_index
        self.assertNotIn("two", index)
        self.assertEqual(len(index), 1)

    def test_junk_in_a_backup_does_not_abandon_the_rest_of_it(self):
        """The restore path is where this bit hardest.

        Its except clause catches only JSONDecodeError and OSError, so the
        AttributeError escaped the backup loop entirely -- the good entries
        in that backup were lost and no later backup was tried either.
        """
        self._corrupt_the_index()
        self.write_backup([self._series("FromBackup"), "a bare string"])
        self.assertEqual(sorted(self._load().series_index), ["FromBackup"])

    def test_a_backup_recovered_through_junk_keeps_its_watch_history(self):
        self._corrupt_the_index()
        self.write_backup([self._series("FromBackup", watched=3), 42])
        total, watched = im.get_episode_counts(self._load().series_index["FromBackup"])
        self.assertEqual((total, watched), (12, 3))


class TestAUselessBackupDoesNotHideAGoodOne(_IndexLoadCase):
    """The backup search must skip a readable backup that restores nothing.

    .bak1 is the newest copy, so it is tried first -- but a save that failed
    early can leave it truncated to an empty list, or holding only elements
    the loader skips. Returning that as the restore ended the search, so
    .bak2 and .bak3 were never opened even when one of them held the real
    index. Empty is a legitimate answer only once every backup has been
    tried.
    """

    def test_a_backup_of_only_junk_falls_through_to_the_next(self):
        self._corrupt_the_index()
        self.write_backup(["a bare string", 42])
        self._write_bak2([self._series("FromBak2")])
        self.assertEqual(sorted(self._load().series_index), ["FromBak2"])

    def test_a_backup_truncated_to_an_empty_list_falls_through_too(self):
        """The likeliest shape: a save that died before writing any entries."""
        self._corrupt_the_index()
        self.write_backup([])
        self._write_bak2([self._series("FromBak2")])
        self.assertEqual(sorted(self._load().series_index), ["FromBak2"])

    def test_the_fallthrough_also_runs_when_the_index_is_missing_entirely(self):
        """The missing-file branch reaches the backups by a different route."""
        if os.path.exists(self.index_path):
            os.remove(self.index_path)
        self.write_backup([])
        self._write_bak2([self._series("FromBak2")])
        self.assertEqual(sorted(self._load().series_index), ["FromBak2"])

    def test_the_recovered_backup_keeps_its_watch_history(self):
        self._corrupt_the_index()
        self.write_backup([])
        self._write_bak2([self._series("FromBak2", watched=5)])
        total, watched = im.get_episode_counts(self._load().series_index["FromBak2"])
        self.assertEqual((total, watched), (12, 5))

    def test_a_good_first_backup_is_still_preferred(self):
        """The fallthrough must not reorder the backups it does accept."""
        self._corrupt_the_index()
        self.write_backup([self._series("FromBak1")])
        self._write_bak2([self._series("FromBak2")])
        self.assertEqual(sorted(self._load().series_index), ["FromBak1"])

    def test_when_no_backup_holds_anything_the_index_is_empty(self):
        self._corrupt_the_index()
        self.write_backup([])
        self._write_bak2(["a bare string"])
        self.assertEqual(dict(self._load().series_index), {})

    def test_and_no_restore_is_claimed_in_that_case(self):
        """Announcing a restore that recovered nothing is worse than silence."""
        self._corrupt_the_index()
        self.write_backup([])
        self._write_bak2(["a bare string"])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self._load()
        self.assertNotIn("restored", out.getvalue().lower())


class TestAnUnreadableIndexIsNeitherEmptiedNorOverwritten(_IndexLoadCase):
    """Every way the index file can fail to read gets the same, safe handling.

    Only a JSON error used to reach the backups. A file another program had
    locked, one an editor had re-saved in another encoding, or one bad entry
    that tripped the validator loaded as an EMPTY index -- and the next save
    wrote that run's few series over the whole file. Now the newest usable
    backup is loaded instead, and a save asks before it replaces a file this
    session could not read.
    """

    def _three(self):
        return [self._series("Alpha"), self._series("Beta"), self._series("Gamma")]

    def _lock_the_first_read(self):
        """open() refuses the index file once, the way a scanner's lock does."""
        real_open = open
        refused = []

        def fake_open(file, *args, **kwargs):
            if not refused and os.path.abspath(str(file)) == os.path.abspath(self.index_path):
                refused.append(file)
                raise PermissionError(13, "The process cannot access the file because it is being used")
            return real_open(file, *args, **kwargs)

        return mock.patch("builtins.open", side_effect=fake_open)

    def _write_bytes(self, raw):
        with open(self.index_path, "wb") as fh:
            fh.write(raw)

    def test_an_index_re_saved_in_another_encoding_loads_the_backup(self):
        self.write_backup(self._three())
        self._write_bytes(json.dumps(self._three(), ensure_ascii=False).replace("Alpha", "Grüße").encode("cp1252"))
        manager = self._load()
        self.assertEqual(sorted(manager.series_index), ["Alpha", "Beta", "Gamma"])
        self.assertIn("UTF-8", manager.unreadable or "")

    def test_a_locked_index_loads_the_backup_not_an_empty_index(self):
        self._write(self._three())
        self.write_backup(self._three()[:2])
        with self._lock_the_first_read():
            manager = im.IndexManager(self.index_path)
        self.assertEqual(sorted(manager.series_index), ["Alpha", "Beta"])
        self.assertEqual(manager.restored_from, "series_index.json.bak1")

    def test_a_file_that_is_not_a_list_loads_the_backup(self):
        self._write("just a string")
        self.write_backup(self._three())
        self.assertEqual(len(self._load().series_index), 3)

    def test_one_entry_with_a_non_string_url_no_longer_empties_the_index(self):
        """The repro: `"url": 123` raised inside validation, and that was everything."""
        self._write([*self._three(), {"title": "Broken", "url": 123, "seasons": []}])
        manager = self._load()
        self.assertEqual(sorted(manager.series_index), ["Alpha", "Beta", "Gamma"])
        self.assertIsNone(manager.unreadable)

    def test_saving_over_an_unreadable_file_asks_and_n_leaves_it_alone(self):
        raw = json.dumps(self._three(), ensure_ascii=False).replace("Alpha", "Grüße").encode("cp1252")
        self._write_bytes(raw)
        manager = self._load()
        manager.series_index["New"] = self._series("New")
        with mock.patch("builtins.input", side_effect=["n"]):
            self.assertFalse(manager.save_index())
        with open(self.index_path, "rb") as fh:
            self.assertEqual(fh.read(), raw, "the unreadable file was written over")

    def test_y_replaces_it_and_keeps_the_unreadable_file_as_bak1(self):
        raw = json.dumps(self._three(), ensure_ascii=False).replace("Alpha", "Grüße").encode("cp1252")
        self._write_bytes(raw)
        manager = self._load()
        with mock.patch("builtins.input", side_effect=["y"]):
            self.assertTrue(manager.save_index())
        with open(self.index_path + ".bak1", "rb") as fh:
            self.assertEqual(fh.read(), raw)

    def test_end_of_input_keeps_the_file(self):
        self._write_bytes(b"\xff\xfe not text")
        manager = self._load()
        with mock.patch("builtins.input", side_effect=[EOFError]):
            self.assertFalse(manager.save_index())
        with open(self.index_path, "rb") as fh:
            self.assertEqual(fh.read(), b"\xff\xfe not text")

    def test_a_one_series_run_after_a_locked_read_no_longer_truncates_the_index(self):
        """The reported scenario, end to end: lock, no backup, a one-series save."""
        self._write(self._three())
        with self._lock_the_first_read():
            manager = im.IndexManager(self.index_path)
        self.assertEqual(dict(manager.series_index), {})
        scrape = [self._series("Alpha")]
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict.fromkeys(_GATES, True)),
            # "Save these changes?" y, then "Replace the index file anyway?" n.
            mock.patch("builtins.input", side_effect=["y", "n"]),
        ):
            self.assertFalse(im.confirm_and_save_changes(scrape, "test", manager))
        with open(self.index_path, encoding="utf-8") as fh:
            self.assertEqual(sorted(entry["title"] for entry in json.load(fh)), ["Alpha", "Beta", "Gamma"])


_GATES = (
    "new_series",
    "new_episodes",
    "watched",
    "unwatched",
    "subscribe",
    "unsubscribe",
    "watchlist_add",
    "watchlist_remove",
    "title_ger",
    "title_eng",
    "episode_remove",
    "season_remove",
)


class TestUnusableEntriesStayInTheFile(_IndexLoadCase):
    """An entry the loader cannot use is set aside and written back, never deleted.

    The loader used to drop such entries, and the next save wrote the index
    without them: one corrupt season cost a series and all its other
    seasons' watch history, without a word.
    """

    def _saved_titles(self):
        with open(self.index_path, encoding="utf-8") as fh:
            return sorted(
                str(entry.get("title")) if isinstance(entry, dict) else repr(entry) for entry in json.load(fh)
            )

    def _load_and_save(self):
        manager = self._load()
        manager.save_index()
        return manager

    def test_a_series_with_one_corrupt_season_survives_the_next_save(self):
        """The repro's "Beta": a good season 1 plus a season whose episodes are a dict."""
        beta = self._series("Beta", watched=7)
        beta["seasons"].append({"season": "Season 2", "episodes": {"1": {"watched": True}}})
        self._write([self._series("Alpha"), beta])
        manager = self._load_and_save()
        self.assertEqual(sorted(manager.series_index), ["Alpha"])
        self.assertEqual(self._saved_titles(), ["Alpha", "Beta"])
        with open(self.index_path, encoding="utf-8") as fh:
            saved_beta = next(entry for entry in json.load(fh) if entry["title"] == "Beta")
        self.assertEqual(saved_beta, beta, "the set-aside entry must come back exactly as it was")

    def test_a_blank_url_with_a_good_link_is_a_usable_entry(self):
        """The repro's "Gamma", and the bs.to sibling's rule: either field names the series."""
        gamma = self._series("Gamma")
        gamma["link"], gamma["url"] = gamma["url"], ""
        self._write([gamma])
        self.assertEqual(sorted(self._load().series_index), ["Gamma"])

    def test_seasons_null_no_longer_breaks_every_save(self):
        self._write([self._series("Alpha"), {**self._series("Delta"), "seasons": None}])
        self._load_and_save()
        self.assertEqual(self._saved_titles(), ["Alpha", "Delta"])

    def test_junk_elements_are_written_back_too(self):
        self._write([self._series("Alpha"), "a bare string", 42])
        self._load_and_save()
        self.assertEqual(self._saved_titles(), ["'a bare string'", "42", "Alpha"])

    def test_a_second_entry_with_the_same_title_and_link_is_kept(self):
        """Keying kept only the later copy, so the next save deleted the other."""
        first, second = self._series("Alpha", watched=9), self._series("Alpha", watched=2)
        self._write([first, second])
        self._load_and_save()
        with open(self.index_path, encoding="utf-8") as fh:
            watched = sorted(im.get_episode_counts(entry)[1] for entry in json.load(fh))
        self.assertEqual(watched, [2, 9])

    def test_the_user_is_told_which_entries_were_set_aside_and_why(self):
        self._write([self._series("Alpha"), {**self._series("Delta"), "seasons": None}])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self._load()
        self.assertIn("Delta", out.getvalue())
        self.assertIn("'seasons' is NoneType", out.getvalue())


class TestASaveNeverOverwritesAnotherWritersChanges(_IndexLoadCase):
    """A file rewritten since it was loaded is not quietly written over.

    Two runs of the program, or a hand edit during a long scrape, each load
    the index and save their own copy: the last save silently undid the
    other's. The save now notices (modification time and size taken at load)
    and asks; anything but y leaves the other writer's file as it is.
    """

    def setUp(self):
        super().setUp()
        self._write([self._series("Alpha"), self._series("Beta")])
        self.session = self._load()
        other = im.IndexManager(self.index_path)
        other.series_index["Other"] = self._series("Other")
        other.save_index()
        self.session.series_index["Mine"] = self._series("Mine")

    def _titles_on_disk(self):
        with open(self.index_path, encoding="utf-8") as fh:
            return sorted(entry["title"] for entry in json.load(fh))

    def test_an_unchanged_file_saves_without_asking(self):
        manager = self._load()
        with mock.patch("builtins.input", side_effect=[]):
            self.assertTrue(manager.save_index())

    def test_n_keeps_the_other_writers_file(self):
        with mock.patch("builtins.input", side_effect=["n"]):
            self.assertFalse(self.session.save_index())
        self.assertEqual(self._titles_on_disk(), ["Alpha", "Beta", "Other"])

    def test_end_of_input_keeps_it_too(self):
        with mock.patch("builtins.input", side_effect=[EOFError]):
            self.assertFalse(self.session.save_index())
        self.assertEqual(self._titles_on_disk(), ["Alpha", "Beta", "Other"])

    def test_a_typo_is_asked_again_not_taken_as_an_answer(self):
        with mock.patch("builtins.input", side_effect=["", "yes", "n"]) as feeder:
            self.assertFalse(self.session.save_index())
        self.assertEqual(feeder.call_count, 3)

    def test_y_replaces_it_with_this_sessions_index(self):
        with mock.patch("builtins.input", side_effect=["y"]):
            self.assertTrue(self.session.save_index())
        self.assertEqual(self._titles_on_disk(), ["Alpha", "Beta", "Mine"])

    def test_the_question_names_what_the_file_gained(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), mock.patch("builtins.input", side_effect=["n"]):
            self.session.save_index()
        self.assertIn("changed on disk", out.getvalue())
        self.assertIn("Other", out.getvalue())

    def test_once_saved_the_next_save_does_not_ask_again(self):
        with mock.patch("builtins.input", side_effect=["y"]):
            self.session.save_index()
        with mock.patch("builtins.input", side_effect=[]):
            self.assertTrue(self.session.save_index())

    def test_a_declined_save_at_the_end_of_a_scrape_leaves_the_manager_as_it_was(self):
        before = dict(self.session.series_index)
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict.fromkeys(_GATES, True)),
            mock.patch("builtins.input", side_effect=["y", "n"]),
        ):
            result = im.confirm_and_save_changes([self._series("Zeta")], "test", self.session)
        self.assertFalse(result)
        self.assertEqual(self.session.series_index, before)
        self.assertEqual(self._titles_on_disk(), ["Alpha", "Beta", "Other"])


class TestAFailedVerificationDoesNotSinkTheSave(QuietCase):
    """The optional live check in the vanished table must not abort the run.

    A failed sign-in there escaped into the run's catch-all, which threw away
    the save that follows -- every approval of the run with it.
    """

    def test_the_table_still_runs_on_the_runs_own_data(self):
        old = {"Gone": {"title": "Gone", "url": series_url("gone"), "link": series_url("gone"), "seasons": []}}
        fresh = {"title": "Fresh", "url": series_url("fresh"), "link": series_url("fresh"), "seasons": []}

        class _Scraper:
            async def verify_vanished_and_candidates(self, *_args):
                raise RuntimeError("Login failed — check credentials")

        # Re-verify: y. Row "Gone": keep.
        with mock.patch("builtins.input", side_effect=["y", "k"]) as feeder:
            kept = im.show_vanished_series(old, set(), "all", new_data=[fresh], scraper=_Scraper())
        self.assertEqual(kept, [("Gone", "not found on aniworld.to")])
        self.assertEqual(feeder.call_count, 2)


class TestASeriesPageThatIsNotMarkupIsAParseFailure(QuietCase):
    """A body lxml cannot build a tree from ends the series, and says so.

    This is the one behaviour the move off BeautifulSoup deliberately changed.
    make_soup handed back an empty tree for an empty or non-markup body, so
    the run fell through to the login check and reported "session expired --
    not logged in" for what was really a broken response. make_doc returns
    None, and the caller now names the actual problem. The distinction is not
    cosmetic: a session expiry triggers a re-login and a retry of every
    remaining series, which is a lot of work to do about a truncated body.
    """

    class EmptyBodyClient:
        def __init__(self, body=""):
            self.body = body
            self.calls = 0

        async def get(self, url, **kwargs):
            self.calls += 1
            return httpx.Response(200, text=self.body, request=httpx.Request("GET", url))

    def _scrape(self, body):
        scraper = SCRAPER_CLS()
        info = {"url": series_url("demo"), "link": series_url("demo"), "title": "Demo"}
        client = self.EmptyBodyClient(body)
        return scraper._scrape_one_series(client, info), client  # noqa: SLF001

    def _run(self, body):
        coro, client = self._scrape(body)
        return asyncio.run(coro), client

    def test_an_empty_body_is_reported_as_a_parse_failure(self):
        result, _ = self._run("")
        self.assertTrue(result.get("_error"))
        self.assertIn("not markup", result.get("_error_reason", ""))

    def test_a_whitespace_only_body_is_the_same(self):
        result, _ = self._run("   \n\t  ")
        self.assertTrue(result.get("_error"))
        self.assertIn("not markup", result.get("_error_reason", ""))

    def test_it_is_not_mistaken_for_a_session_expiry(self):
        """The old path called this a logout, which triggered a needless re-login."""
        result, _ = self._run("")
        self.assertNotIn("logged in", result.get("_error_reason", ""))

    def test_real_markup_still_gets_past_this_check(self):
        """The guard must reject only unparseable bodies, not thin ones."""
        result, _ = self._run("<html><body>ok</body></html>")
        self.assertNotIn("not markup", result.get("_error_reason", ""))


# ── a fake site over real httpx ──────────────────────────────────────────────
# The classes below drive the scraper through httpx.MockTransport rather than a
# stub client: closing a client, retrying through _get and following the
# active host are httpx behaviour, and a permissive double hides exactly that.

_REAL_ASYNC_CLIENT = httpx.AsyncClient
ANON_SERIES_HTML = site.SERIES_HTML.replace(site.LOGGED_IN, "")
LOGIN_FORM = '<form action="/login" method="post"><input type="password" name="password"></form>'
BAD_GATEWAY = "<html><head><title>502 Bad Gateway</title></head></html>"


class _FakeSite:
    """Login, one series and its seasons, served by an httpx.MockTransport.

    `session` is whether the server still knows the session; a login POST
    revives it unless `login_works` is False. `expire_after` series-page
    requests end the session (a mid-run expiry). `fail_after_login` serves that
    many 502s to the first requests after login number `from_login`, which is
    where that login's own check lands. `faults` maps a URL to status codes
    served, one per request, before that URL answers normally.
    """

    def __init__(self, *, session=True, login_works=True, expire_after=0, fail_after_login=0, from_login=1):
        self.session = session
        self.login_works = login_works
        self.expire_after = expire_after
        self.fail_after_login = fail_after_login
        self.from_login = from_login
        self.faults: dict[str, list[int]] = {}
        self.requests: list[str] = []
        self.posts = 0
        self.series_requests = 0
        self.series_path = urlparse(site.SERIES_URL).path

    def handler(self, request):
        url, path = str(request.url), request.url.path
        self.requests.append(url)
        codes = self.faults.get(url)
        if codes:
            return httpx.Response(codes.pop(0), text=BAD_GATEWAY)
        if request.method == "POST":
            self.posts += 1
            self.session = self.session or self.login_works
            return httpx.Response(200, text="")
        if "login" in path:
            return httpx.Response(200, text=LOGIN_FORM)
        if self.posts >= self.from_login and self.fail_after_login:
            self.fail_after_login -= 1
            return httpx.Response(502, text=BAD_GATEWAY)
        if path.rstrip("/") == self.series_path:
            self.series_requests += 1
            if self.expire_after and self.series_requests == self.expire_after:
                self.session = False
            return httpx.Response(200, text=site.SERIES_HTML if self.session else ANON_SERIES_HTML)
        if path.startswith(self.series_path):
            return httpx.Response(200, text=site.season_html(True, logged_in=self.session))
        return httpx.Response(200, text=site.SERIES_HTML if self.session else ANON_SERIES_HTML)

    def client(self, *args, **kwargs):
        """A real AsyncClient on this site; takes and ignores the scraper's own settings."""
        return _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(self.handler), follow_redirects=True)


def _no_backoff(case):
    """Retries without the real back-off, so a retried 502 costs no wall time."""
    patcher = mock.patch.object(sc, "_BACKOFF_BASE", 0.0)
    patcher.start()
    case.addCleanup(patcher.stop)


def _series_infos(count):
    """`count` queue entries that all read the fake site's one series."""
    path = urlparse(site.SERIES_URL).path
    return [{"url": site.SERIES_URL, "link": f"{path}#{n}", "title": f"Series {n}"} for n in range(count)]


class TestAFailedReloginKeepsTheSharedSession(QuietCase):
    """A re-login that failed its check used to close the session every worker shares.

    _login_client closed the client it was handed when the check page did not
    read as logged in -- and mid-run that client is the pool's one session.
    Every remaining series then failed with "Cannot send a request, as the
    client has been closed", and one 502 on the check page was enough, since
    the check was a single bare GET.
    """

    def setUp(self):
        super().setUp()
        _no_backoff(self)

    def _relogin(self, fake):
        client = fake.client()
        scraper = SCRAPER_CLS()
        recovered = asyncio.run(scraper._relogin_shared_client(client))
        return recovered, client

    def test_one_bad_gateway_on_the_check_page_is_retried(self):
        recovered, client = self._relogin(_FakeSite(session=False, fail_after_login=1))
        self.assertTrue(recovered, "a single 502 on the check page failed the re-login")
        self.assertFalse(client.is_closed)

    def test_a_login_that_did_not_take_leaves_the_session_open(self):
        recovered, client = self._relogin(_FakeSite(session=False, login_works=False))
        self.assertFalse(recovered)
        self.assertFalse(client.is_closed, "the failed re-login closed the shared session")

    def test_a_failed_first_login_still_closes_its_own_client(self):
        """The client a fresh login built is nobody else's, so it is still closed."""
        fake = _FakeSite(session=False, login_works=False)
        built = []

        def factory(*args, **kwargs):
            built.append(fake.client())
            return built[-1]

        with mock.patch.object(sc.httpx, "AsyncClient", factory), self.assertRaises(RuntimeError):
            asyncio.run(SCRAPER_CLS()._create_logged_in_client())
        self.assertTrue(built and built[0].is_closed)

    def test_a_mid_run_expiry_no_longer_fails_the_rest_of_the_run(self):
        # The pool's own login is the first; the 502 lands on the re-login's check.
        fake = _FakeSite(expire_after=3, fail_after_login=1, from_login=2)

        async def pool_client(self_, verify=True):
            client = fake.client()
            await self_._login_client(client, verify=verify)
            return client

        scraper = SCRAPER_CLS()
        with mock.patch.object(SCRAPER_CLS, "_create_logged_in_client", pool_client):
            asyncio.run(scraper._scrape_list(_series_infos(12), num_workers=4))

        reasons = [entry["reason"] for entry in scraper.failed_links]
        self.assertFalse([r for r in reasons if "closed" in r], reasons)
        self.assertEqual(len(scraper.series_data), 12, reasons)


class TestAPoolThatCannotLogInStopsTheRun(QuietCase):
    """Every worker used to retry the login on its own and then quietly return.

    With 16 workers that was 32 logins in a burst, and then a run that
    "finished" with the queue untouched and nothing on the failed list. A
    resumed run went on to merge its old checkpoint, report the scrape
    complete and delete the checkpoint.
    """

    def setUp(self):
        super().setUp()
        self.attempts = 0

        async def failing_login(_self, verify=True):
            self.attempts += 1
            raise RuntimeError("Login failed - check credentials")

        async def no_wait(*_args, **_kwargs):
            return None

        for target, name, value in (
            (SCRAPER_CLS, "_create_logged_in_client", failing_login),
            (sc.asyncio, "sleep", no_wait),
        ):
            patcher = mock.patch.object(target, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self._d = tempfile.TemporaryDirectory()
        self.addCleanup(self._d.cleanup)

    def _scraper(self):
        scraper = SCRAPER_CLS()
        scraper.checkpoint_file = os.path.join(self._d.name, ".scrape_checkpoint.json")
        scraper.failed_file = os.path.join(self._d.name, ".failed_series.json")
        scraper.ignore_file = os.path.join(self._d.name, ".ignored_series.json")
        scraper.pause_file = os.path.join(self._d.name, ".pause_scraping")
        return scraper

    def test_the_login_error_ends_the_run(self):
        with self.assertRaises(RuntimeError):
            asyncio.run(self._scraper()._scrape_list(_series_infos(40), num_workers=16))

    def test_the_pool_logs_in_once_and_retries_once(self):
        with contextlib.suppress(RuntimeError):
            asyncio.run(self._scraper()._scrape_list(_series_infos(40), num_workers=16))
        self.assertEqual(self.attempts, 2)

    def test_work_from_before_the_failure_is_kept(self):
        scraper = self._scraper()
        resumed = {"title": "Done earlier", "link": "/done", "url": series_url("done"), "total_episodes": 3}
        scraper.series_data = [resumed]
        with contextlib.suppress(RuntimeError):
            asyncio.run(scraper._scrape_list(_series_infos(5), num_workers=4))
        self.assertEqual(scraper.series_data, [resumed])
        self.assertEqual(scraper.attempted_urls, set(), "nothing was attempted, so nothing may look done")

    def test_run_raises_and_keeps_the_checkpoint_for_a_resume(self):
        scraper = self._scraper()
        scraper.series_data = [
            {"title": "Done earlier", "link": "/done", "url": series_url("done"), "total_episodes": 3}
        ]
        scraper.completed_links = {"/done"}

        async def run_pool(**_kwargs):
            await scraper._scrape_list(_series_infos(5), num_workers=4)

        scraper._async_run = run_pool  # type: ignore[method-assign]
        with self.assertRaises(RuntimeError):
            scraper.run(resume_only=False)
        with open(scraper.checkpoint_file, encoding="utf-8") as fh:
            saved = json.load(fh)
        self.assertEqual([s["title"] for s in saved["series_data"]], ["Done earlier"])

    def test_an_empty_series_recheck_that_cannot_log_in_keeps_the_run(self):
        """The re-check after a finished scrape used to raise and lose the save."""
        empty = [{"title": "Empty", "link": "/empty", "url": series_url("empty"), "total_episodes": 0}]
        self.assertEqual(asyncio.run(self._scraper()._rescrape_empty_series(empty)), empty)


class TestVanishedCheckTellsGoneFromUnknown(QuietCase):
    """One 502 used to be reported as "the series really is gone".

    verify_series_url made one bare GET, no retry, no pacing, and anything but
    a served page came back as not reachable -- which the vanished prompt
    announces as gone, right before the user decides whether to delete.
    """

    def setUp(self):
        super().setUp()
        _no_backoff(self)

    def _verdict(self, statuses):
        fake = _FakeSite()
        fake.faults[site.SERIES_URL] = list(statuses)
        with mock.patch.object(sc.httpx, "AsyncClient", fake.client):
            verified, _ = asyncio.run(SCRAPER_CLS().verify_vanished_and_candidates([("Show", site.SERIES_URL)], []))
        return verified[0][2]

    def test_one_bad_gateway_is_retried_not_reported_gone(self):
        self.assertIs(self._verdict([502]), True)

    def test_a_site_that_keeps_failing_is_unknown_not_gone(self):
        self.assertIsNone(self._verdict([502] * sc._MAX_ATTEMPTS))

    def test_a_series_the_site_says_is_missing_is_gone(self):
        self.assertIs(self._verdict([404]), False)

    def test_the_prompt_does_not_call_an_unknown_gone(self):
        class _Unknown:
            async def verify_vanished_and_candidates(self, vanished, candidates):
                return [(title, url, None) for title, url in vanished], []

        row = {"v_title": "Old Title", "v_url": series_url("old"), "new_entry": None}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertFalse(im._rescrape_row(row, _Unknown(), {}))
        self.assertIn("could not be checked", out.getvalue())
        self.assertNotIn("really is gone", out.getvalue())
        self.assertEqual(row["v_title"], "Old Title")


class TestAMirrorRunReadsTheMirror(QuietCase):
    """A run on a mirror used to send every series request to the primary.

    Login and catalogue followed the active host, but the series and season
    pages were fetched from the stored URL, which always names SITE_URL. With
    the primary down -- the reason a mirror is active at all -- every series
    failed; with it up, the mirror's session cookie never went there and every
    page read logged out.
    """

    PRIMARY, MIRROR = SITE_URLS[0], SITE_URLS[1]

    def _scrape(self, active):
        hosts = []
        fake = _FakeSite()
        primary_host = urlparse(self.PRIMARY).netloc

        def handler(request):
            hosts.append(request.url.host)
            if active != self.PRIMARY and request.url.host == primary_host:
                raise httpx.ConnectError("the primary is down", request=request)
            return fake.handler(request)

        scraper = SCRAPER_CLS()
        scraper.site_url = active
        path = urlparse(site.SERIES_URL).path
        info = {"url": f"{self.PRIMARY}{path}", "link": path, "title": "Test Series"}
        client = _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler), follow_redirects=True)
        try:
            result = asyncio.run(scraper._scrape_one_series(client, info))
        finally:
            asyncio.run(client.aclose())
        return result, hosts, info

    def test_series_and_season_pages_come_from_the_active_host(self):
        result, hosts, _info = self._scrape(self.MIRROR)
        self.assertFalse(result.get("_error"), result.get("_error_reason"))
        self.assertEqual(set(hosts), {urlparse(self.MIRROR).netloc}, hosts)

    def test_the_stored_urls_stay_canonical(self):
        """The index reads the same whichever mirror a run went through."""
        result, _hosts, info = self._scrape(self.MIRROR)
        self.assertFalse(result.get("_error"), result.get("_error_reason"))
        self.assertTrue(result["seasons"])
        self.assertEqual(result["url"], info["url"])
        for season in result["seasons"]:
            self.assertTrue(season["url"].startswith(self.PRIMARY), season["url"])

    def test_the_primary_is_still_read_when_it_is_active(self):
        result, hosts, _info = self._scrape(self.PRIMARY)
        self.assertFalse(result.get("_error"))
        self.assertEqual(set(hosts), {urlparse(self.PRIMARY).netloc})

    def test_a_url_on_a_host_this_program_does_not_know_is_left_alone(self):
        self.assertEqual(
            sc._on_host("https://example.test/anime/stream/x", self.MIRROR), "https://example.test/anime/stream/x"
        )


class TestAnAccountPageThatFailsIsNotAnEmptyOne(QuietCase):
    """A timeout on one account page used to be logged and skipped.

    The list that came back short was then the evidence for "no longer on the
    watchlist" (main._inject_disappeared_series): every watchlist-only series
    was offered for un-flagging because one page did not load.
    """

    PAGE = (
        f"<html><body>{site.LOGGED_IN}<div class='seriesListContainer'>"
        "<div><a href='/anime/stream/sub-a'><h3>Sub A</h3></a></div></div></body></html>"
    )

    def setUp(self):
        super().setUp()
        _no_backoff(self)

    def _fetch(self, watchlist):
        def handler(request):
            if request.url.path.endswith("/watchlist"):
                return watchlist(request)
            return httpx.Response(200, text=self.PAGE)

        async def go():
            async with _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler)) as client:
                return await SCRAPER_CLS()._get_account_series(client, source="both")

        return asyncio.run(go())

    def test_a_page_that_never_loads_stops_the_run(self):
        def timeout(request):
            raise httpx.ReadTimeout("timed out", request=request)

        with self.assertRaises(RuntimeError) as caught:
            self._fetch(timeout)
        self.assertIn("Watchlist", str(caught.exception))

    def test_a_page_that_fails_once_is_retried(self):
        answers = [httpx.Response(502, text=BAD_GATEWAY)]

        def flaky(request):
            return answers.pop(0) if answers else httpx.Response(200, text=self.PAGE)

        self.assertEqual([s["link"] for s in self._fetch(flaky)], ["/anime/stream/sub-a"])


class TestTheStartupCheckKeepsThePasswordOffPlainHttp(QuietCase):
    """The startup check used to log in to every host that answered.

    That included the plain-HTTP IP fallback, on every start, with the HTTPS
    hosts serving fine -- the account password went out in cleartext each
    time the program was opened. An HTTP host is now signed in to only when
    no HTTPS host served, and only after an explicit yes.
    """

    HOSTS = ["https://a.test", "https://b.test", "http://c.test"]

    def setUp(self):
        super().setUp()
        previous = getattr(main, "ACTIVE_SITE_URL", None)
        self.addCleanup(setattr, main, "ACTIVE_SITE_URL", previous)

    def _check(self, served, answers):
        logged_into = []
        asked = []

        def fake_probe(scraper, site_urls):
            return [{"site_url": url, "ok": True, "status_code": 200} for url in site_urls]

        def fake_fetch(scraper, site_urls):
            logged_into.extend(site_urls)
            return {url: ((10, {"a"}) if served.get(url) else (None, set())) for url in site_urls}

        limit = len(answers) + 1

        def fake_input(prompt=""):
            asked.append(prompt)
            # Bounded: a prompt that kept asking after end of input fails here
            # instead of hanging the suite.
            if len(asked) > limit:
                raise AssertionError(f"asked again after end of input: {prompt!r}")
            if not answers:
                raise EOFError
            return answers.pop(0)

        idx = mock.Mock()
        idx.series_index = {}
        scraper = SCRAPER_CLS()
        out = io.StringIO()
        with (
            mock.patch.object(main, "SITE_URLS", self.HOSTS),
            mock.patch.object(main, "_probe_hosts", fake_probe),
            mock.patch.object(main, "_fetch_catalogue_info_for_hosts", fake_fetch),
            mock.patch.object(main, "_wait_before_host_retry", lambda *a, **k: False),
            mock.patch("builtins.input", fake_input),
            contextlib.redirect_stdout(out),
        ):
            main._probe_sites_before_scrape(scraper, idx_mgr=idx)
        return scraper.site_url, logged_into, asked, out.getvalue()

    def test_with_an_https_host_serving_http_is_never_logged_in_to_or_asked_about(self):
        active, logged_into, asked, out = self._check(dict.fromkeys(self.HOSTS, True), [])
        self.assertNotIn(self.HOSTS[2], logged_into)
        self.assertEqual(asked, [])
        self.assertEqual(active, self.HOSTS[0])
        self.assertIn("SKIPPED (HTTP)", out)

    def test_no_keeps_the_password_off_the_wire(self):
        active, logged_into, asked, _out = self._check({self.HOSTS[2]: True}, ["n"])
        self.assertNotIn(self.HOSTS[2], logged_into)
        self.assertEqual(len(asked), 1)
        self.assertFalse(active.startswith("http://"), active)

    def test_end_of_input_counts_as_no(self):
        active, logged_into, _asked, _out = self._check({self.HOSTS[2]: True}, [])
        self.assertNotIn(self.HOSTS[2], logged_into)
        self.assertFalse(active.startswith("http://"), active)

    def test_a_typo_is_asked_again(self):
        _active, logged_into, asked, _out = self._check({self.HOSTS[2]: True}, ["yes", "n"])
        self.assertEqual(len(asked), 2)
        self.assertNotIn(self.HOSTS[2], logged_into)

    def test_an_explicit_yes_uses_the_http_host(self):
        active, logged_into, _asked, _out = self._check({self.HOSTS[2]: True}, ["y"])
        self.assertIn(self.HOSTS[2], logged_into)
        self.assertEqual(active, self.HOSTS[2])


class TestTheProbeLooksLikeABrowser(QuietCase):
    """The probe went out with httpx's own "python-httpx" User-Agent.

    That is the first thing a bot filter turns away, and a host that refuses
    the probe is never used, however well it would have served the session.
    Both sibling scrapers already sent the browser agent here.
    """

    def test_the_probe_sends_the_scrapers_user_agent(self):
        seen = {}

        def factory(*args, **kwargs):
            seen.update(kwargs)
            return _RecordingClient([], _CannedResponse(200, LOGIN_FORM))

        with mock.patch.object(sc.httpx, "AsyncClient", factory):
            asyncio.run(SCRAPER_CLS()._probe_one_site("https://probe.test"))
        self.assertEqual(seen.get("headers", {}).get("User-Agent"), sc.UA)
