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

import json
import os

import pytest

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
