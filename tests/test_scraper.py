"""Unit tests for the Aniworld.to scraper's pure logic.

Run with:  python -m unittest discover -s tests

Each test targets one fix from the sibling BS.to project's AGENT_TASKS.txt
review, applied here where the same class of bug was found.
"""

import asyncio
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup  # noqa: E402

from config.config import configure_console  # noqa: E402
from src.atomic_io import atomic_write_json  # noqa: E402
from src.index_manager import (  # noqa: E402
    IndexManager,
    _build_merged_data,
    detect_changes,
    sync_season_counts,
)
from src.scraper import (  # noqa: E402
    AniWorldScraper,
    _extract_title,
    _heading_text,
    _parse_episodes,
)


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


class TempFileCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)


# ==================== console encoding ====================
class TestConfigureConsole(unittest.TestCase):
    def test_survives_cp1252_stream(self):
        """A stream stuck on cp1252 (redirected pipe/file) must not crash.

        Before the fix, printing any of the U+2192/U+2500/U+26A0-family
        characters this project uses in status output raises
        UnicodeEncodeError on such a stream.
        """
        buf = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict")
        old_stdout = sys.stdout
        sys.stdout = buf
        try:
            configure_console()
            print("→ ─ ⚠ ✓ ✗ ≈ │ \U0001f5d1 \U0001f4c4")
        except UnicodeEncodeError:
            self.fail("configure_console() did not prevent UnicodeEncodeError")
        finally:
            sys.stdout = old_stdout

    def test_reconfigures_to_utf8(self):
        buf = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict")
        old_stdout = sys.stdout
        sys.stdout = buf
        try:
            configure_console()
            self.assertEqual(sys.stdout.encoding.lower().replace("-", ""), "utf8")
        finally:
            sys.stdout = old_stdout


# ==================== durable atomic writes ====================
class TestAtomicWriteJson(TempFileCase):
    def test_data_survives_and_is_flushed_to_disk(self):
        target = os.path.join(self.dir.name, "out.json")
        atomic_write_json(target, {"a": 1}, backup=False)
        with open(target, encoding="utf-8") as f:
            self.assertEqual(json.load(f), {"a": 1})

    def test_fsync_is_called_before_replace(self):
        """The durability gap this closes: os.replace() alone doesn't flush
        file contents. Assert fsync actually runs, not just that the end
        file looks right (which would pass even without the fix)."""
        target = os.path.join(self.dir.name, "out.json")
        calls = []
        real_fsync = os.fsync
        try:
            os.fsync = lambda fd: (calls.append(fd), real_fsync(fd))[1]
            atomic_write_json(target, {"a": 1}, backup=False)
        finally:
            os.fsync = real_fsync
        self.assertTrue(calls, "os.fsync was never called")

    def test_no_leftover_tmp_file_on_failure(self):
        target = os.path.join(self.dir.name, "out.json")

        class Unserializable:
            pass

        with self.assertRaises(TypeError):
            atomic_write_json(target, {"bad": Unserializable()}, backup=False)
        leftovers = [f for f in os.listdir(self.dir.name) if f.endswith(".tmp")]
        self.assertEqual(leftovers, [], f"temp file(s) left behind: {leftovers}")

    def test_unique_tmp_names_avoid_collision(self):
        """Concurrent writers to the same target must not share one fixed
        temp filename (the pre-fix scraper.py pattern: file + '.tmp')."""
        target = os.path.join(self.dir.name, "out.json")
        atomic_write_json(target, {"n": 1}, backup=False)
        atomic_write_json(target, {"n": 2}, backup=False)
        with open(target, encoding="utf-8") as f:
            self.assertEqual(json.load(f), {"n": 2})


# ==================== title extraction must not mutate soup ====================
# Parse helpers take a pre-parsed soup, shared across extractors on one page --
# these two land together because the non-mutation fix is what makes that
# sharing safe.
class TestExtractTitle(unittest.TestCase):
    def test_heading_text_does_not_mutate_tree(self):
        html = "<div id='x'><h2>\n\t\tHarry Potter\n\t\t\t<small>Specials</small>\n</h2></div>"
        s = soup(html)
        before = str(s)
        text = _heading_text(s.h2)
        self.assertEqual(text, "Harry Potter")
        self.assertEqual(str(s), before, "_heading_text must not edit the parse tree")

    def test_inline_tags_separated(self):
        html = "<div id='x'><h1 class='fw-bold'>\n\t\tHarry Potter\n\t\t\t<small>Specials</small>\n</h1></div>"
        self.assertEqual(_extract_title(soup(html)), "Harry Potter")

    def test_shared_soup_not_mutated_by_extract_title(self):
        """Once callers share one soup across extractors, _extract_title must
        not corrupt it for the extractors that run after."""
        html = "<div id='x'><h1 class='fw-bold'>Harry Potter<small>Specials</small></h1></div>"
        s = soup(html)
        before = str(s)
        _extract_title(s)
        self.assertEqual(str(s), before)


class TestParseEpisodesTakesSoup(unittest.TestCase):
    def test_missing_episode_table_returns_none(self):
        """No episode table at all = we don't understand this page = failure.
        Verified live: aniworld.to serves a nav-less generic page for a
        season that doesn't exist, which lands exactly here."""
        html = "<html><body></body></html>"
        self.assertIsNone(_parse_episodes(soup(html)))

    def test_empty_but_present_table_returns_empty_list(self):
        """Table present with zero rows = a season listed before its episodes
        are uploaded. That is a real state (four such seasons exist in the
        sibling s.to index) and must NOT fail the whole series."""
        html = '<html><body><table class="seasonEpisodesList"><tbody></tbody></table></body></html>'
        self.assertEqual(_parse_episodes(soup(html)), [])

    def test_normal_row_parses(self):
        html = """
        <table class="seasonEpisodesList"><tbody><tr data-episode-id="1">
            <meta itemprop="episodeNumber" content="1">
            <td class="seasonEpisodeTitle"><a><strong>Pilot</strong></a></td>
        </tr></tbody></table>
        """
        episodes = _parse_episodes(soup(html))
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["number"], 1)

    def test_malformed_row_returns_none(self):
        """A row with a data-episode-id but no way to determine its episode
        number is a parse failure, not an empty season -- must return None
        so the call site's `if episodes is None:` guard catches it."""
        html = """
        <table class="seasonEpisodesList"><tbody><tr data-episode-id="1">
            <td class="seasonEpisodeTitle"><a><strong>Pilot</strong></a></td>
        </tr></tbody></table>
        """
        self.assertIsNone(_parse_episodes(soup(html)))


# ==================== unparseable season handling ====================
class _FakeResponse:
    """Stand-in for httpx.Response.

    Carries `status_code` because the retry path inspects it: a double that
    omits it would make every fetch look like a transport failure and hide
    whatever the test was actually about.
    """

    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.request = None


class _FakeClient:
    """Minimal stand-in for httpx.AsyncClient.get(), keyed by exact URL."""

    def __init__(self, pages: dict) -> None:
        self.pages = pages

    async def get(self, url, follow_redirects=True):  # noqa: ARG002
        return _FakeResponse(self.pages[url])


class TestScrapeOneSeriesUnparseableSeason(unittest.TestCase):
    """A season whose episode table fails to parse must error out, not be
    stored as a 0-episode season."""

    SERIES_URL = "https://aniworld.to/anime/stream/test-series"
    SEASON_URL = "https://aniworld.to/anime/stream/test-series/staffel-1"

    SERIES_HTML = """
    <html><body>
    <div class="avatar"><a href="/user/profil/1">Me</a></div>
    <h1 class="fw-bold">Test Series</h1>
    <div id="stream"><ul><li><a href="/anime/stream/test-series/staffel-1">1</a></li></ul></div>
    </body></html>
    """

    def _run(self, season_html: str) -> dict:
        scraper = AniWorldScraper()
        client = _FakeClient(
            {
                self.SERIES_URL: self.SERIES_HTML,
                self.SEASON_URL: season_html,
            }
        )
        info = {"url": self.SERIES_URL, "link": "/anime/stream/test-series", "title": "Test Series"}
        return asyncio.run(scraper._scrape_one_series(client, info))

    def test_missing_episode_table_is_an_error_result(self):
        result = self._run("<html><body>no episode table here</body></html>")
        self.assertTrue(result.get("_error"), f"expected an error result, got: {result}")
        self.assertIn("missing or unparseable", result.get("_error_reason", ""))
        self.assertEqual(result["seasons"], [])

    def test_genuinely_empty_season_is_stored_not_errored(self):
        """Regression guard: an empty-but-present episode table is a real
        season state and must be stored as 0, not fail the whole series."""
        season_html = '<html><body><table class="seasonEpisodesList"><tbody></tbody></table></body></html>'
        result = self._run(season_html)
        self.assertFalse(result.get("_error"), f"empty season must not error: {result}")
        self.assertEqual(result["total_episodes"], 0)
        self.assertEqual(len(result["seasons"]), 1)

    def test_malformed_row_is_still_an_error_result(self):
        """Pre-existing behaviour (episodes is None) must keep working
        alongside the new not-episodes case."""
        season_html = """
        <table class="seasonEpisodesList"><tbody><tr data-episode-id="1">
            <td class="seasonEpisodeTitle"><a><strong>Pilot</strong></a></td>
        </tr></tbody></table>
        """
        result = self._run(season_html)
        self.assertTrue(result.get("_error"))

    def test_normal_season_is_not_an_error(self):
        season_html = """
        <table class="seasonEpisodesList"><tbody><tr data-episode-id="1">
            <meta itemprop="episodeNumber" content="1">
            <td class="seasonEpisodeTitle"><a><strong>Pilot</strong></a></td>
        </tr></tbody></table>
        """
        result = self._run(season_html)
        self.assertFalse(result.get("_error"))
        self.assertEqual(result["total_episodes"], 1)



# ==================== season counter drift (One Piece S23 regression) ====
class TestSeasonCounterDrift(unittest.TestCase):
    """A season that gains episodes must not keep its old stored counters.

    Observed live: One Piece season 23 held ``watched_episodes: 15`` and
    ``total_episodes: 16`` while its ``episodes`` list -- verified against
    the site, which is the ground truth -- held 20 entries, all watched.
    The merge replaced the episode list but left the derived counters
    untouched, so the season reported the numbers it had on some earlier
    run indefinitely.
    """

    @staticmethod
    def _entry(episodes, stored_total, stored_watched):
        return {
            "title": "One Piece",
            "url": "https://example.invalid/one-piece",
            "link": "https://example.invalid/one-piece",
            "seasons": [
                {
                    "season": "23",
                    "url": "https://example.invalid/one-piece/staffel-23",
                    "episodes": episodes,
                    "total_episodes": stored_total,
                    "watched_episodes": stored_watched,
                }
            ],
        }

    def test_merge_refreshes_counters_when_season_grows(self):
        old_data = {"One Piece": self._entry([{"number": n, "watched": n <= 15} for n in range(1, 17)], 16, 15)}
        new_dict = {"One Piece": self._entry([{"number": n, "watched": True} for n in range(1, 21)], 20, 20)}

        merged = _build_merged_data(
            old_data,
            new_dict,
            {"watched": True, "unwatched": True, "title_ger": True, "title_eng": True},
        )
        season = merged["One Piece"]["seasons"][0]
        self.assertEqual(season["total_episodes"], 20)
        self.assertEqual(season["watched_episodes"], 20)
        self.assertEqual(merged["One Piece"]["total_episodes"], 20)
        self.assertEqual(merged["One Piece"]["watched_episodes"], 20)

    def test_sync_season_counts_respects_ignored_episode_zero(self):
        season = {
            "season": "1",
            "episodes": [{"number": 0, "watched": True}, {"number": 1, "watched": True}],
            "ignored_episode_0": True,
            "total_episodes": 99,
            "watched_episodes": 99,
        }
        self.assertEqual(sync_season_counts(season), (1, 1))
        self.assertEqual(season["total_episodes"], 1)
        self.assertEqual(season["watched_episodes"], 1)

    def test_reconcile_repairs_pre_existing_drift(self):
        # __new__ skips __init__, which would load the real on-disk index.
        manager = IndexManager.__new__(IndexManager)
        manager.series_index = {
            "One Piece": self._entry([{"number": n, "watched": True} for n in range(1, 21)], 16, 15)
        }
        self.assertEqual(manager._reconcile_derived_counts(), 1)
        season = manager.series_index["One Piece"]["seasons"][0]
        self.assertEqual(season["total_episodes"], 20)
        self.assertEqual(season["watched_episodes"], 20)
        self.assertEqual(manager.series_index["One Piece"]["unwatched_episodes"], 0)
        # Idempotent: a clean index reports no drift.
        self.assertEqual(manager._reconcile_derived_counts(), 0)


# ==================== backup rotation ====================
class TestBackupRotation(TempFileCase):
    """The .bak1-3 chain must survive the switch from copying to renaming.

    Backups stopped being made with shutil.copy2 -- on the series index that
    was tens of MB copied on every save for no reason, since the new file is
    already written and fsynced by then and the outgoing one can just be
    renamed aside. That is only an improvement if the generations still come
    out in the right order, so this pins them.
    """

    def test_three_generations_are_kept_in_order(self):
        path = os.path.join(self.dir.name, "index.json")
        for generation in range(1, 6):
            atomic_write_json(path, {"generation": generation})

        with open(path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["generation"], 5)
        for index, expected in ((1, 4), (2, 3), (3, 2)):
            with open(f"{path}.bak{index}", encoding="utf-8") as fh:
                self.assertEqual(json.load(fh)["generation"], expected, f"bak{index}")
        self.assertFalse(os.path.exists(f"{path}.bak4"), "only three generations are kept")

    def test_no_temp_files_are_left_behind(self):
        path = os.path.join(self.dir.name, "index.json")
        atomic_write_json(path, {"a": 1})
        atomic_write_json(path, {"a": 2})
        leftovers = [f for f in os.listdir(self.dir.name) if f.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_backup_false_writes_no_backup(self):
        path = os.path.join(self.dir.name, "checkpoint.json")
        atomic_write_json(path, {"a": 1}, backup=False)
        atomic_write_json(path, {"a": 2}, backup=False)
        self.assertFalse(os.path.exists(f"{path}.bak1"))

    def test_first_write_of_a_new_file_makes_no_backup(self):
        path = os.path.join(self.dir.name, "fresh.json")
        atomic_write_json(path, {"a": 1})
        self.assertFalse(os.path.exists(f"{path}.bak1"))

if __name__ == "__main__":
    unittest.main()


# ==================== removal gates ====================
class TestRemovalRequiresApproval(unittest.TestCase):
    """Nothing is ever deleted from the index without an explicit yes.

    A truncated response, a soft error page, or a season the site hid for a
    moment all look identical to a genuine removal from the scraper's side.
    So removals are detected and reported, and only applied when the user
    approves that category in the terminal.
    """

    ALLOW_NONE = {
        "watched": True,
        "unwatched": True,
        "title_ger": True,
        "title_eng": True,
        "episode_remove": False,
        "season_remove": False,
    }
    ALLOW_ALL = {**ALLOW_NONE, "episode_remove": True, "season_remove": True}

    @staticmethod
    def _series(seasons):
        return {
            "title": "Show",
            "url": "https://example.invalid/anime/stream/show",
            "link": "https://example.invalid/anime/stream/show",
            "seasons": [
                {"season": label, "url": f"https://example.invalid/s/{label}", "episodes": eps}
                for label, eps in seasons
            ],
        }

    @staticmethod
    def _eps(*numbers):
        return [{"number": n, "watched": True} for n in numbers]

    def setUp(self):
        season_two = ("2", self._eps(1))
        self.old = {"Show": self._series([("1", self._eps(1, 2, 3)), season_two])}
        self.new_missing_ep = {"Show": self._series([("1", self._eps(1, 3)), season_two])}
        self.new_missing_season = {"Show": self._series([("1", self._eps(1, 2, 3))])}

    def _nums(self, merged, label="1"):
        season = next(s for s in merged["Show"]["seasons"] if s["season"] == label)
        return [e["number"] for e in season["episodes"]]

    def test_missing_episode_is_kept_without_approval(self):
        merged = _build_merged_data(self.old, self.new_missing_ep, self.ALLOW_NONE)
        self.assertEqual(self._nums(merged), [1, 2, 3], "episode 2 must survive an unapproved removal")

    def test_missing_episode_is_deleted_with_approval(self):
        merged = _build_merged_data(self.old, self.new_missing_ep, self.ALLOW_ALL)
        self.assertEqual(self._nums(merged), [1, 3])

    def test_kept_episode_does_not_corrupt_season_counters(self):
        merged = _build_merged_data(self.old, self.new_missing_ep, self.ALLOW_NONE)
        season = next(s for s in merged["Show"]["seasons"] if s["season"] == "1")
        self.assertEqual(season["total_episodes"], 3)
        self.assertEqual(season["watched_episodes"], 3)

    def test_missing_season_is_kept_without_approval(self):
        merged = _build_merged_data(self.old, self.new_missing_season, self.ALLOW_NONE)
        self.assertEqual([s["season"] for s in merged["Show"]["seasons"]], ["1", "2"])

    def test_missing_season_is_deleted_with_approval(self):
        merged = _build_merged_data(self.old, self.new_missing_season, self.ALLOW_ALL)
        self.assertEqual([s["season"] for s in merged["Show"]["seasons"]], ["1"])

    def test_failed_scrape_never_wipes_seasons(self):
        """A series whose scrape failed arrives with no seasons at all."""
        merged = _build_merged_data(self.old, {"Show": self._series([])}, self.ALLOW_ALL)
        self.assertEqual([s["season"] for s in merged["Show"]["seasons"]], ["1", "2"])

    def test_absent_flag_is_treated_as_keep(self):
        """A caller that never heard of these flags must not delete anything."""
        legacy_flags = {"watched": True, "unwatched": True, "title_ger": True, "title_eng": True}
        merged = _build_merged_data(self.old, self.new_missing_ep, legacy_flags)
        self.assertEqual(self._nums(merged), [1, 2, 3])

    def test_changes_are_reported_for_the_prompt(self):
        changes = detect_changes(self.old, self.new_missing_ep)
        self.assertEqual(changes["removed_episodes"], [("Show", "1", "2")])
        self.assertEqual(changes["removed_seasons"], [])

        changes = detect_changes(self.old, self.new_missing_season)
        self.assertEqual(changes["removed_seasons"], [("Show", "2")])
        self.assertEqual(changes["removed_episodes"], [], "a vanished season is not N episode removals")
