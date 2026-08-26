"""Regression tests for failures that used to lose data or fail a whole run.

Each class here pins one bug that was found by reproducing it, not by reading:
a failed save deleting the index, a corrupt or missing index loading as empty,
one transient error failing an entire series, an unexpected worker exception
discarding the run, a mid-run session expiry failing every series after it, a
fuzzy title guess silently skipping a new series, and a truncated catalogue
making the whole index look vanished.

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
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

import main  # noqa: E402
import src.index_manager as im  # noqa: E402
import src.scraper as sc  # noqa: E402
from config.config import VALID_SERIES_HOSTS  # noqa: E402
from src.atomic_io import atomic_write_json  # noqa: E402
from src.scraper import ScrapingPausedError  # noqa: E402

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
    """IndexManager takes an explicit path in two projects, a global in bs.to."""
    _set_module_index_path(im, path)
    try:
        return im.IndexManager(path)
    except TypeError:
        return im.IndexManager()


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

    The outgoing file is renamed into .bak1 before the new one is moved into
    place, so a failure between those two steps used to leave no file at all
    at the index path.
    """

    def _flaky_replace(self, fail_on):
        real = os.replace
        calls = {"n": 0}

        def replace(src, dst):
            calls["n"] += 1
            if calls["n"] == fail_on:
                raise OSError("simulated failure")
            return real(src, dst)

        return replace

    def test_original_survives_a_failed_final_swap(self):
        with open(self.index_path, "w", encoding="utf-8") as fh:
            json.dump([{"title": "Important"}], fh)
        with (
            mock.patch("src.atomic_io.os.replace", side_effect=self._flaky_replace(2)),
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
            mock.patch("src.atomic_io.os.replace", side_effect=self._flaky_replace(2)),
            self.assertRaises(OSError),
        ):
            atomic_write_json(self.index_path, [{"title": "New"}])
        self.assertEqual([f for f in os.listdir(self.dir) if f.endswith(".tmp")], [])

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
        asyncio.run(scraper._scrape_one_series(client, info))
        self.assertGreater(client.calls, 1, "the series page must go through the retrying fetch")

    def test_retry_eventually_gives_up(self):
        """A permanently broken host must still end as an error, not hang."""
        scraper = SCRAPER_CLS()
        client = self.FlakyClient(fail_times=99)
        info = {"url": series_url("demo"), "link": series_url("demo"), "title": "Demo"}
        result = asyncio.run(scraper._scrape_one_series(client, info))
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

        scraper._scrape_one_series = fake_scrape
        scraper._acquire_client = lambda: asyncio.sleep(0, result=object())
        scraper._release_client = lambda: asyncio.sleep(0)
        return scraper

    @staticmethod
    def _items(n):
        return [
            {"url": series_url(f"s{i}"), "link": series_url(f"s{i}"), "title": f"S{i}"}
            for i in range(n)
        ]

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
        fn = getattr(im, "_build_merged_data", None) or im._merge_series_data
        return fn(old, new, allowed)

    def test_the_new_dict_is_not_rewritten(self):
        old = {}
        new = self._fixture()
        snapshot = copy.deepcopy(new)
        allowed = dict.fromkeys(
            ["new_series", "new_episodes", "watched", "unwatched", "subscribe",
             "unsubscribe", "watchlist_add", "watchlist_remove", "title_ger",
             "title_eng", "episode_remove", "season_remove"],
            True,
        )
        self._merge(old, new, allowed)
        self.assertEqual(new, snapshot, "the caller's data must come back unchanged")

    def test_merging_twice_gives_the_same_result(self):
        old = {}
        new = self._fixture()
        deny = dict.fromkeys(
            ["new_series", "new_episodes", "watched", "unwatched", "subscribe",
             "unsubscribe", "watchlist_add", "watchlist_remove", "title_ger",
             "title_eng", "episode_remove", "season_remove"],
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


class TestDeleteAllNeedsConfirmation(QuietCase):
    """"a" deletes every remaining entry unseen; it has to be confirmed."""

    @staticmethod
    def _entries(n):
        return [(f"Show{i}", "gone", series_url(f"s{i}")) for i in range(n)]

    def test_wrong_confirmation_deletes_nothing(self):
        with mock.patch("builtins.input", side_effect=["a", "nope", "n", "n", "n", "n"]):
            self.assertEqual(im._prompt_vanished_deletions(self._entries(5)), [])

    def test_exact_confirmation_deletes_all(self):
        with mock.patch("builtins.input", side_effect=["a", "DELETE 5"]):
            self.assertEqual(len(im._prompt_vanished_deletions(self._entries(5))), 5)

    def test_per_item_answers_still_work(self):
        with mock.patch("builtins.input", side_effect=["y", "n", "s"]):
            self.assertEqual(len(im._prompt_vanished_deletions(self._entries(5))), 1)


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

        scraper._scrape_one_series = fake_scrape
        scraper._acquire_client = lambda: asyncio.sleep(0, result=object())
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


if __name__ == "__main__":
    unittest.main()
