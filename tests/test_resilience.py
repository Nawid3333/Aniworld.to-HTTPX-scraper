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
import time
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


class TestDeleteAllNeedsConfirmation(QuietCase):
    """ "a" deletes every remaining entry unseen; it has to be confirmed."""

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
