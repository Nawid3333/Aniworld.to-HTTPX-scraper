"""The on-disk state a run carries between invocations.

The checkpoint, failed list, ignore list and pause file are what make a scrape
resumable and interruptible. They were almost entirely untested, and every one
of them is read at the start of a run to decide what work to skip -- so a
misread here silently changes what gets scraped, without an error anywhere.

Every test points the scraper's state files at a tmp_path, so nothing here can
touch the real data/ directory.

Style note for future edits
---------------------------
The scraper reads its file paths from instance attributes set in __init__, so
redirecting them is a matter of assigning four attributes -- that is what
``scraper`` does below. Prefer that over monkeypatching module constants: it
survives a refactor that moves where the defaults come from.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from unittest import mock

import pytest

import src.scraper as sc
from src.scraper import _retry_after_seconds
from tests._support import FakeResponse

from src.scraper import AniWorldScraper as Scraper  # isort: skip


@pytest.fixture
def scraper(tmp_path):
    """A scraper whose every state file lives in tmp_path."""
    instance = Scraper()
    instance.checkpoint_file = str(tmp_path / ".scrape_checkpoint.json")
    instance.failed_file = str(tmp_path / ".failed_series.json")
    instance.ignore_file = str(tmp_path / ".ignored_series.json")
    instance.pause_file = str(tmp_path / ".pause_scraping")
    return instance


# ── checkpoint ──────────────────────────────────────────────────────────────


class TestCheckpoint:
    def test_no_checkpoint_reads_as_nothing_completed(self, scraper):
        assert scraper.load_checkpoint() is False
        assert scraper.completed_links == set()

    def test_a_saved_checkpoint_round_trips(self, scraper):
        scraper.completed_links = {"/anime/stream/a", "/anime/stream/b"}
        scraper._checkpoint_mode = "all"
        scraper.save_checkpoint()

        fresh = Scraper()
        fresh.checkpoint_file = scraper.checkpoint_file
        assert fresh.load_checkpoint() is True
        assert fresh.completed_links == {"/anime/stream/a", "/anime/stream/b"}
        assert fresh._checkpoint_mode == "all"

    def test_series_data_is_only_stored_when_asked_for(self, scraper):
        """The frequent writer omits the payload; the final one includes it."""
        scraper.completed_links = {"/anime/stream/a"}
        scraper.series_data = [{"title": "Alpha"}]

        def stored() -> dict:
            with open(scraper.checkpoint_file, encoding="utf-8") as fh:
                return json.load(fh)

        scraper.save_checkpoint(include_data=False)
        assert "series_data" not in stored()

        scraper.save_checkpoint(include_data=True)
        assert stored()["series_data"]

    def test_an_empty_checkpoint_is_not_treated_as_a_resume(self, scraper):
        scraper.save_checkpoint()
        assert scraper.load_checkpoint() is False, "nothing completed means there is nothing to resume"

    def test_a_legacy_bare_list_checkpoint_still_loads(self, scraper):
        """Older runs wrote a plain list of links."""
        with open(scraper.checkpoint_file, "w", encoding="utf-8") as fh:
            json.dump(["/anime/stream/a"], fh)
        assert scraper.load_checkpoint() is True
        assert scraper.completed_links == {"/anime/stream/a"}

    def test_a_corrupt_checkpoint_does_not_abort_the_run(self, scraper):
        """Resuming is an optimisation; a bad file must cost the resume, not the run."""
        with open(scraper.checkpoint_file, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        assert scraper.load_checkpoint() is False

    def test_clearing_removes_the_file_and_is_safe_to_repeat(self, scraper):
        scraper.completed_links = {"/anime/stream/a"}
        scraper.save_checkpoint()
        scraper.clear_checkpoint()
        assert not os.path.exists(scraper.checkpoint_file)
        scraper.clear_checkpoint()  # must not raise on an already-absent file

    def test_the_mode_can_be_read_without_building_a_scraper(self, scraper, tmp_path):
        """main.py asks this before deciding whether to offer a resume."""
        assert Scraper.get_checkpoint_mode(str(tmp_path)) is None
        scraper._checkpoint_mode = "new_only"
        scraper.completed_links = {"/anime/stream/a"}
        scraper.save_checkpoint()
        assert Scraper.get_checkpoint_mode(str(tmp_path)) == "new_only"

    def test_reading_the_mode_of_a_corrupt_file_returns_none(self, tmp_path):
        (tmp_path / ".scrape_checkpoint.json").write_text("{not json", encoding="utf-8")
        assert Scraper.get_checkpoint_mode(str(tmp_path)) is None


# ── failed and ignored lists ────────────────────────────────────────────────


class TestFailedSeries:
    def test_an_absent_file_reads_as_no_failures(self, scraper):
        assert scraper.load_failed_series() == []

    def test_failures_round_trip(self, scraper):
        scraper.failed_links = [{"url": "https://x/anime/stream/a", "title": "A", "reason": "timeout"}]
        scraper.save_failed_series()
        assert [entry["title"] for entry in scraper.load_failed_series()] == ["A"]

    def test_a_corrupt_file_reads_as_empty_rather_than_raising(self, scraper):
        with open(scraper.failed_file, "w", encoding="utf-8") as fh:
            fh.write("[[[")
        assert scraper.load_failed_series() == []

    def test_a_file_holding_the_wrong_shape_is_ignored(self, scraper):
        with open(scraper.failed_file, "w", encoding="utf-8") as fh:
            json.dump({"not": "a list"}, fh)
        assert scraper.load_failed_series() == []

    def test_writing_an_empty_list_removes_the_file(self, scraper):
        """A stale file would read as failures that no longer exist.

        Targets _write_failed_entries rather than save_failed_series because
        that is the primitive all three scrapers share; their save_* wrappers
        deliberately differ (S.to is merge-only and takes no replace flag).
        """
        scraper.failed_links = [{"url": "https://x/anime/stream/a", "title": "A", "reason": "timeout"}]
        scraper.save_failed_series()
        assert os.path.exists(scraper.failed_file)
        with scraper._lock:
            scraper._write_failed_entries([])
        assert not os.path.exists(scraper.failed_file)


class TestIgnoredSeries:
    def test_an_absent_file_reads_as_nothing_ignored(self, scraper):
        assert scraper.load_ignored_series() == []
        assert scraper.get_ignored_slugs() == set()

    def test_slugs_are_extracted_from_ignored_urls(self, scraper):
        with open(scraper.ignore_file, "w", encoding="utf-8") as fh:
            json.dump([{"url": "https://x/anime/stream/naruto"}, {"url": "https://x/anime/stream/bleach"}], fh)
        assert scraper.get_ignored_slugs() == {"naruto", "bleach"}

    def test_an_unparseable_url_does_not_become_a_slug(self, scraper):
        """'unknown' must never end up in the set and silently ignore a real series."""
        with open(scraper.ignore_file, "w", encoding="utf-8") as fh:
            json.dump([{"url": "not-a-url"}, {"url": "https://x/anime/stream/naruto"}], fh)
        assert scraper.get_ignored_slugs() == {"naruto"}

    def test_a_corrupt_ignore_file_ignores_nothing(self, scraper):
        with open(scraper.ignore_file, "w", encoding="utf-8") as fh:
            fh.write("nope")
        assert scraper.load_ignored_series() == []


# ── pause file ──────────────────────────────────────────────────────────────


class TestPauseFile:
    def test_creating_then_clearing(self, scraper):
        scraper._create_pause_file()
        assert os.path.exists(scraper.pause_file)
        scraper._clear_pause_file()
        assert not os.path.exists(scraper.pause_file)

    def test_clearing_an_absent_pause_file_is_harmless(self, scraper):
        scraper._clear_pause_file()

    def test_the_pause_check_is_cached_but_notices_the_file(self, scraper):
        """_check_pause caches for a moment so it can be called in a hot loop."""
        scraper._last_pause_check = 0.0
        assert scraper._check_pause() is False
        scraper._create_pause_file()
        scraper._last_pause_check = 0.0
        assert scraper._check_pause() is True


# ── resume filtering ────────────────────────────────────────────────────────


class TestFilterCompleted:
    def test_nothing_completed_returns_the_list_unchanged(self, scraper):
        series_list = [{"link": "/anime/stream/a"}, {"link": "/anime/stream/b"}]
        assert scraper._filter_completed(series_list) == series_list

    def test_completed_entries_are_dropped(self, scraper):
        scraper.completed_links = {"/anime/stream/a"}
        remaining = scraper._filter_completed([{"link": "/anime/stream/a"}, {"link": "/anime/stream/b"}])
        assert [entry["link"] for entry in remaining] == ["/anime/stream/b"]

    def test_everything_completed_returns_none_to_stop_the_run(self, scraper):
        """None is the signal that there is no work left, not an error."""
        scraper.completed_links = {"/anime/stream/a"}
        assert scraper._filter_completed([{"link": "/anime/stream/a"}]) is None


# ── Retry-After parsing ─────────────────────────────────────────────────────


class TestRetryAfter:
    def test_a_numeric_header_is_used(self):
        assert _retry_after_seconds(FakeResponse(429, headers={"Retry-After": "30"})) == 30.0

    def test_a_missing_header_yields_none(self):
        assert _retry_after_seconds(FakeResponse(429)) is None

    def test_an_http_date_yields_none_so_the_backoff_takes_over(self):
        """Not supported; None means "use our own doubling", which is safe."""
        response = FakeResponse(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        assert _retry_after_seconds(response) is None

    def test_a_nonsense_header_yields_none(self):
        assert _retry_after_seconds(FakeResponse(429, headers={"Retry-After": "soon"})) is None


# ── the periodic checkpoint and its journal ─────────────────────────────────
# Links here are opaque strings: the checkpoint never parses them, so one
# neutral shape serves all three sites.


def _items(count, prefix="s"):
    return [
        {"title": f"{prefix.upper()}{n}", "link": f"/series/{prefix}{n}", "url": f"https://x.test/series/{prefix}{n}"}
        for n in range(count)
    ]


def _fake_pool(scraper, fail=()):
    """Workers that "scrape" instantly; links in `fail` come back as errors."""

    async def scrape(_client, info):
        if info["link"] in fail:
            return scraper._error_result(info, "boom")
        return {
            "title": info["title"],
            "link": info["link"],
            "url": info["url"],
            "total_episodes": 3,
            "watched_episodes": 1,
            "seasons": [{"season": "1", "episodes": []}],
        }

    scraper._scrape_one_series = scrape
    scraper._acquire_client = lambda: asyncio.sleep(0, result=object())
    scraper._release_client = lambda: asyncio.sleep(0)


def _after_a_crash(scraper):
    """A new process reading what the old one left on disk."""
    fresh = Scraper()
    fresh.checkpoint_file = scraper.checkpoint_file
    fresh.load_checkpoint()
    return fresh


class TestPeriodicCheckpointKeepsItsResults:
    """The periodic checkpoint used to record links and none of their results.

    Only the final, pause and error paths wrote the data. A run that ended any
    other way -- the console window closed, a crash -- left a checkpoint that
    named hundreds of series as done, a resume skipped them all, and their
    results never reached the index.
    """

    @pytest.fixture(autouse=True)
    def _small_interval(self, monkeypatch):
        monkeypatch.setattr(sc, "CHECKPOINT_EVERY", 10)

    def test_a_crash_after_a_periodic_save_loses_no_scraped_series(self, scraper):
        _fake_pool(scraper)
        asyncio.run(scraper._scrape_list(_items(25), num_workers=3))
        # The process dies here: run()'s final save never happens.
        resumed = _after_a_crash(scraper)
        assert len(resumed.completed_links) == 20
        assert {entry["link"] for entry in resumed.series_data} == resumed.completed_links
        remaining = resumed._filter_completed(_items(25))
        assert remaining is not None and len(remaining) == 5

    def test_a_series_that_failed_is_tried_again_after_a_crash(self, scraper):
        """Its failure lived only in memory, so skipping it would leave no trace."""
        _fake_pool(scraper, fail={"/series/s3"})
        asyncio.run(scraper._scrape_list(_items(20), num_workers=1))
        resumed = _after_a_crash(scraper)
        assert "/series/s3" not in resumed.completed_links
        assert "/series/s4" in resumed.completed_links

    def test_a_resumed_run_keeps_the_results_its_paused_predecessor_saved(self, scraper):
        """A resumed run's periodic save used to drop the paused run's data from the file."""
        scraper.completed_links = {"/series/p0", "/series/p1"}
        scraper.series_data = [entry | {"total_episodes": 3} for entry in _items(2, prefix="p")]
        scraper._checkpoint_mode = "all_series"
        scraper.save_checkpoint(include_data=True)

        resumed = _after_a_crash(scraper)
        _fake_pool(resumed)
        asyncio.run(resumed._scrape_list(_items(10), num_workers=2))

        again = _after_a_crash(resumed)
        links = {entry["link"] for entry in again.series_data}
        assert {"/series/p0", "/series/p1"} <= links
        assert len(links) == 12

    def test_a_line_torn_by_a_crash_is_skipped_and_does_not_eat_the_next_batch(self, tmp_path):
        journal = str(tmp_path / "journal.jsonl")
        with open(journal, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"link": "/a"}) + "\n" + '{"link": "/b", "tit')
        sc._append_journal(journal, [{"link": "/c"}])
        assert [entry["link"] for entry in sc._read_journal(journal)] == ["/a", "/c"]

    def test_results_on_disk_but_never_recorded_as_done_are_not_loaded(self, scraper):
        """The run died between journaling a batch and recording its links."""
        sc._append_journal(scraper.checkpoint_journal, [{"link": "/series/x", "title": "X"}])
        scraper.completed_links = {"/series/y"}
        scraper.save_checkpoint()
        assert _after_a_crash(scraper).series_data == []

    def test_discarding_a_checkpoint_removes_its_journal(self, scraper, tmp_path):
        sc._append_journal(scraper.checkpoint_journal, [{"link": "/series/x"}])
        scraper.completed_links = {"/series/x"}
        scraper.save_checkpoint()
        Scraper.discard_checkpoint(str(tmp_path))
        assert not os.path.exists(scraper.checkpoint_file)
        assert not os.path.exists(scraper.checkpoint_journal)

    def test_clearing_a_checkpoint_removes_its_journal(self, scraper):
        sc._append_journal(scraper.checkpoint_journal, [{"link": "/series/x"}])
        scraper.clear_checkpoint()
        assert not os.path.exists(scraper.checkpoint_journal)


class TestRunsThatKeepNoCheckpoint:
    """A single-anime add or a nested rescrape used to wipe a paused run's checkpoint.

    The single-URL path cleared the checkpoint after its scrape, run() then
    wrote a "single" one in its place, and main.py deleted that afterwards;
    the rescrapes main.py offers after a save did the same as "batch" runs.
    The paused run the user meant to resume was gone either way.
    """

    @staticmethod
    def _paused_checkpoint(scraper):
        scraper.completed_links = {"/series/p0"}
        scraper.series_data = _items(1, prefix="p")
        scraper._checkpoint_mode = "all_series"
        scraper.save_checkpoint(include_data=True)
        with open(scraper.checkpoint_file, "rb") as fh:
            return fh.read()

    @staticmethod
    def _run(scraper, **kwargs):
        async def nothing(**_kwargs):
            return None

        scraper._async_run = nothing
        scraper.series_data = []
        scraper.completed_links = set()
        scraper.run(**kwargs)
        scraper.clear_checkpoint()  # what main.py does after a run that was not paused

    def test_a_single_url_run_leaves_a_paused_checkpoint_alone(self, scraper):
        before = self._paused_checkpoint(scraper)
        self._run(scraper, single_url="https://x.test/series/one")
        with open(scraper.checkpoint_file, "rb") as fh:
            assert fh.read() == before

    def test_a_run_started_without_a_checkpoint_leaves_it_alone(self, scraper):
        before = self._paused_checkpoint(scraper)
        self._run(scraper, url_list=["https://x.test/series/one"], checkpoint=False)
        with open(scraper.checkpoint_file, "rb") as fh:
            assert fh.read() == before

    def test_an_ordinary_run_still_owns_its_checkpoint(self, scraper):
        self._run(scraper, url_list=["https://x.test/series/one"])
        assert not os.path.exists(scraper.checkpoint_file)


class TestRunPutsCtrlCBack:
    """run() installed its Ctrl+C handler and never removed it.

    After the first scrape Ctrl+C never quit the program again: at the menu it
    printed "Pause requested" and left a pause file behind, and the genre
    scrape, which never reads that file, could not be interrupted at all.
    """

    def test_the_previous_handler_is_back_after_a_run(self, scraper):
        before = signal.getsignal(signal.SIGINT)

        async def nothing(**_kwargs):
            assert signal.getsignal(signal.SIGINT) is not before, "the pause handler was not installed"

        scraper._async_run = nothing
        scraper.run(checkpoint=False)
        assert signal.getsignal(signal.SIGINT) is before

    def test_the_previous_handler_is_back_after_a_run_that_failed(self, scraper):
        before = signal.getsignal(signal.SIGINT)

        async def boom(**_kwargs):
            raise RuntimeError("login failed")

        scraper._async_run = boom
        with pytest.raises(RuntimeError):
            scraper.run(checkpoint=False)
        assert signal.getsignal(signal.SIGINT) is before

    def test_a_pause_asked_for_after_the_workers_stopped_leaves_no_file(self, scraper):
        async def late_ctrl_c(**_kwargs):
            scraper._create_pause_file()

        scraper._async_run = late_ctrl_c
        scraper.run(checkpoint=False)
        assert not os.path.exists(scraper.pause_file)


class TestStoppingAfterTheIgnoredSeasonsIsAPause:
    """Answering n after the ignored-season phase used to end the run as if it had finished.

    The prompt saved a checkpoint and returned, run() carried on as normal,
    and main.py reported the scrape complete and deleted the checkpoint it had
    just been told to keep. Stopping there is a pause now, with a pause's
    checkpoint.
    """

    def test_declining_to_continue_pauses_the_run(self, scraper):
        entries = _items(2)

        async def nothing(*_args, **_kwargs):
            return None

        async def catalogue(_client):
            return entries

        ignored_slug = sc.slug_key(scraper.get_series_slug_from_url(entries[0]["link"]))
        scraper._revalidate_ignored_series = nothing
        scraper._get_all_series = catalogue
        scraper._confirm_catalogue_size = lambda _series: True
        scraper._check_ignored_vs_catalog = lambda *_args, **_kwargs: None
        scraper._check_index_vs_catalog = lambda *_args, **_kwargs: None
        scraper.load_existing_slugs = lambda: set()
        scraper._filter_new_entries = lambda new, _all: (list(new), set())
        scraper._get_ignored_seasons = lambda: {(ignored_slug, "1")}
        scraper._scrape_list = nothing
        scraper._ignored_seasons_continue = lambda: False

        tmp = mock.AsyncMock()
        with pytest.raises(sc.ScrapingPausedError):
            asyncio.run(scraper._async_run_inner(tmp))
