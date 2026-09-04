"""Regression tests for the one question this program keeps having to answer:

    is the slug in the index the same series as the slug on the site?

The bug these guard against was not a wrong answer to that question, it was
two different answers. The catalogue side derived its slugs through the
scraper's URL parser and the index side derived its own with a separate
extractor, and the two normalised differently. aniworld.to prints
"/anime/stream/Dark%20Matter" in one list and "/anime/stream/dark%20matter"
in another, so one series ended up reported as vanished *and* new at the same
time, on every host, with the count difference cancelling out so the totals
looked almost right.

What that cost: the post-scrape cleanup deleted the series, the next
new-only scrape found it missing from the index and added it straight back
with the same mixed-case URL, and the same stale report flagged it again --
a loop that lost the entry's watch history on every pass and could not be
escaped by answering the prompt honestly.

So these tests assert the invariant rather than the symptom: every pair of
slug sets that gets compared anywhere in the program must be built with the
same key, and a series spelled two ways is one series.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config.config as cfg  # noqa: E402
import main  # noqa: E402
from src.index_manager import IndexManager, _extract_slug_from_field, show_vanished_series  # noqa: E402
from src.scraper import AniWorldScraper  # noqa: E402
from src.slug import slug_key, slug_keys  # noqa: E402
from tests._support import captured_output, scripted_input  # noqa: E402

# The exact pair from the incident: same series, two spellings, both real.
INDEX_SPELLING = "Dark%20Matter"
SITE_SPELLING = "dark%20matter"


def _index_entry(title: str, slug: str) -> dict:
    url = f"https://aniworld.to/anime/stream/{slug}"
    return {
        "title": title,
        "url": url,
        "link": f"/anime/stream/{slug}",
        "total_seasons": 1,
        "total_episodes": 8,
        "watched_episodes": 3,
        "unwatched_episodes": 5,
        "seasons": [],
    }


def _site_entry(title: str, slug: str) -> dict:
    """One catalogue row, shaped like _get_all_series returns it."""
    return {"title": title, "link": f"/anime/stream/{slug}", "url": f"https://aniworld.to/anime/stream/{slug}"}


class SlugKeyTests(unittest.TestCase):
    """The key itself: what it folds, and what it deliberately does not."""

    def test_case_and_encoding_are_one_key(self):
        self.assertEqual(slug_key(INDEX_SPELLING), slug_key(SITE_SPELLING))
        self.assertEqual(slug_key(INDEX_SPELLING), "dark matter")

    def test_decoded_and_encoded_spellings_agree(self):
        self.assertEqual(slug_key("Furia-Rasende%20Wut"), slug_key("furia-rasende wut"))

    def test_surrounding_slashes_and_spacing_ignored(self):
        self.assertEqual(slug_key("/Alpha/"), slug_key("alpha"))
        self.assertEqual(slug_key("  alpha  "), "alpha")

    def test_separators_are_not_folded(self):
        # "a-b" and "a b" may be two different series. Folding them would
        # trade a false vanished report for a false duplicate, which is worse:
        # duplicates get merged and lose history silently.
        self.assertNotEqual(slug_key("some-show"), slug_key("some show"))

    def test_nothing_to_compare_returns_none(self):
        # Typed as Any on purpose: the int and bytes are the point of the test.
        # slug_key's isinstance guard is what stops a malformed index entry --
        # a number, a byte string -- from taking a whole run down.
        values: list[Any] = ["", "   ", "/", None, 42, b"alpha"]
        for value in values:
            self.assertIsNone(slug_key(value), value)

    def test_slug_keys_drops_empties_and_the_unknown_sentinel(self):
        self.assertEqual(slug_keys(["Alpha", "unknown", "", None, "ALPHA"]), {"alpha"})


class BothSidesUseOneKeyTests(unittest.TestCase):
    """Every producer of a slug set must produce keys, not raw site text."""

    def test_index_extractor_returns_a_key(self):
        self.assertEqual(_extract_slug_from_field(f"/anime/stream/{INDEX_SPELLING}"), slug_key(SITE_SPELLING))

    def test_catalogue_side_returns_keys(self):
        scraper = AniWorldScraper()

        class _Client:
            async def aclose(self):
                return None

        async def fake_client(*_args, **_kwargs):
            return _Client()

        async def fake_series(*_args, **_kwargs):
            return [_site_entry("Dark Matter", SITE_SPELLING), _site_entry("Alpha", "Alpha")]

        with (
            patch.object(AniWorldScraper, "_create_logged_in_client", fake_client),
            patch.object(AniWorldScraper, "_get_all_series", fake_series),
        ):
            count, slugs = asyncio.run(scraper.get_catalogue_info_for_site("https://aniworld.to"))

        self.assertEqual(count, 2)
        self.assertEqual(slugs, {"dark matter", "alpha"})

    def test_index_slug_collection_returns_keys(self):
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path = os.path.join(tmp, "series_index.json")
            Path(index_path).write_text(json.dumps([_index_entry("Dark Matter", INDEX_SPELLING)]), encoding="utf-8")
            idx_mgr = IndexManager(index_path)
            slugs, _dups, _no_slug = main._collect_index_slugs(idx_mgr)
        self.assertEqual(slugs, {"dark matter"})

    def test_existing_slugs_match_what_the_catalogue_serves(self):
        """The half of the loop that re-added the series after it was deleted."""
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path = os.path.join(tmp, "series_index.json")
            Path(index_path).write_text(json.dumps([_index_entry("Dark Matter", INDEX_SPELLING)]), encoding="utf-8")
            scraper = AniWorldScraper()
            with patch("src.scraper.SERIES_INDEX_FILE", index_path):
                existing = scraper.load_existing_slugs()
            catalogue_slug = slug_key(scraper.get_series_slug_from_url(f"/anime/stream/{SITE_SPELLING}"))
        self.assertIn(catalogue_slug, existing)


class HostCrossCheckTests(unittest.TestCase):
    """The startup probe: the report that drove the deletions."""

    def _index(self, tmp: str, entries: list[dict]) -> tuple[str, IndexManager]:
        index_path = os.path.join(tmp, "series_index.json")
        Path(index_path).write_text(json.dumps(entries), encoding="utf-8")
        return index_path, IndexManager(index_path)

    def test_one_series_spelled_two_ways_is_not_a_mismatch(self):
        """The incident, reduced: two indexed series, three on site, one new.

        The count difference (-1) is real and stays. What must not appear is
        the mixed-case series on either mismatch list -- it is in both places.
        """
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            _index_path, idx_mgr = self._index(
                tmp,
                [_index_entry("Alpha", "alpha"), _index_entry("Dark Matter", INDEX_SPELLING)],
            )
            site_slugs = slug_keys(["alpha", SITE_SPELLING, "die-flodders-2026"])

            _idx_count, compare_txt, report = main._cross_check_index(
                AniWorldScraper(), "https://aniworld.to", 3, idx_mgr=idx_mgr, site_slugs=site_slugs
            )

        self.assertEqual(compare_txt, "mismatch (-1)")
        assert report is not None  # narrows for the type checker
        self.assertEqual(report["only_in_index"], [])
        self.assertEqual(report["only_on_site"], ["die-flodders-2026"])

    def test_a_genuinely_missing_series_is_still_reported(self):
        """The fix must not work by reporting nothing."""
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            _index_path, idx_mgr = self._index(
                tmp,
                [_index_entry("Alpha", "alpha"), _index_entry("Gone Forever", "gone-forever")],
            )
            _idx_count, _compare, report = main._cross_check_index(
                AniWorldScraper(), "https://aniworld.to", 1, idx_mgr=idx_mgr, site_slugs=slug_keys(["alpha"])
            )
        assert report is not None  # narrows for the type checker
        self.assertEqual(report["only_in_index"], ["gone-forever"])


class VanishedCleanupLoopTests(unittest.TestCase):
    """Report writing and report reading, end to end."""

    def _setup(self, tmp: str, entries: list[dict]):
        index_path = os.path.join(tmp, "series_index.json")
        Path(index_path).write_text(json.dumps(entries), encoding="utf-8")
        patches = [
            patch.object(cfg, "SERIES_INDEX_FILE", index_path),
            patch.object(cfg, "DATA_DIR", tmp),
            patch.object(main, "SERIES_INDEX_FILE", index_path),
            patch.object(main, "DATA_DIR", tmp),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in patches])
        return index_path, IndexManager(index_path)

    def _write_report(self, tmp: str, idx_mgr: IndexManager, site_slugs: set[str], site_count: int):
        """Write the report the way the startup probe writes it, for every host."""
        hosts = []
        for host in ("https://aniworld.to", "https://aniworld.cc"):
            _idx_count, _compare, entry = main._cross_check_index(
                AniWorldScraper(), host, site_count, idx_mgr=idx_mgr, site_slugs=site_slugs
            )
            hosts.append(entry)
        with captured_output():
            main._save_combined_mismatch_report(os.path.join(tmp, "mismatch_report.json"), idx_mgr, hosts)

    def test_series_present_under_another_spelling_is_never_offered_for_deletion(self):
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            _index_path, idx_mgr = self._setup(
                tmp,
                [_index_entry("Alpha", "alpha"), _index_entry("Dark Matter", INDEX_SPELLING)],
            )
            self._write_report(tmp, idx_mgr, slug_keys(["alpha", SITE_SPELLING, "die-flodders-2026"]), 3)

            self.assertEqual(main._find_vanished_to_clean(idx_mgr), {})

    def test_a_series_the_scrape_just_saw_is_not_vanished(self):
        """The report is written at startup; a later scrape outranks it."""
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            _index_path, idx_mgr = self._setup(tmp, [_index_entry("Alpha", "alpha")])
            self._write_report(tmp, idx_mgr, slug_keys(["beta", "gamma"]), 2)

            self.assertEqual(main._find_vanished_to_clean(idx_mgr), {"alpha": "Alpha"})
            self.assertEqual(main._find_vanished_to_clean(idx_mgr, seen_slugs={"alpha"}), {})
            self.assertEqual(main._find_vanished_to_clean(idx_mgr, seen_slugs={"ALPHA"}), {})

    def test_scrape_time_check_agrees_with_the_report(self):
        """show_vanished_series and the host probe must not disagree."""
        index = {
            "Alpha": _index_entry("Alpha", "alpha"),
            "Dark Matter": _index_entry("Dark Matter", INDEX_SPELLING),
        }
        discovered = {main._extract_slug(_site_entry("x", s)) for s in ("alpha", SITE_SPELLING)}
        with captured_output() as out:
            kept = show_vanished_series(index, discovered, "new_only")
        self.assertEqual(kept, [])
        self.assertNotIn("NOT found", out.getvalue())


class CleanupPromptTests(unittest.TestCase):
    """The prompt itself: it shows the decision table, and keeps by default."""

    def _setup(self, tmp: str):
        index_path = os.path.join(tmp, "series_index.json")
        Path(index_path).write_text(
            json.dumps([_index_entry("Alpha", "alpha"), _index_entry("Gone", "gone")]), encoding="utf-8"
        )
        patches = [
            patch.object(cfg, "SERIES_INDEX_FILE", index_path),
            patch.object(cfg, "DATA_DIR", tmp),
            patch.object(main, "SERIES_INDEX_FILE", index_path),
            patch.object(main, "DATA_DIR", tmp),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in patches])
        idx_mgr = IndexManager(index_path)
        hosts = []
        for host in ("https://aniworld.to", "https://aniworld.cc"):
            _c, _t, entry = main._cross_check_index(
                AniWorldScraper(), host, 1, idx_mgr=idx_mgr, site_slugs=slug_keys(["alpha"])
            )
            hosts.append(entry)
        with captured_output():
            main._save_combined_mismatch_report(os.path.join(tmp, "mismatch_report.json"), idx_mgr, hosts)
        return index_path, idx_mgr

    def _titles(self, index_path: str) -> set[str]:
        with open(index_path, encoding="utf-8") as f:
            return {entry["title"] for entry in json.load(f)}

    def test_the_decision_table_is_shown(self):
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path, idx_mgr = self._setup(tmp)
            with scripted_input("k", "n", default="n"), captured_output() as out:
                removed = main._prompt_clean_vanished(idx_mgr)
            printed = out.getvalue()

            self.assertFalse(removed)
            self.assertIn("Old (index)", printed)
            self.assertIn("New (site)", printed)
            self.assertEqual(self._titles(index_path), {"Alpha", "Gone"})

    def test_confirmed_deletion_removes_only_that_entry(self):
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path, idx_mgr = self._setup(tmp)
            with scripted_input("y", default="n"), captured_output():
                removed = main._prompt_clean_vanished(idx_mgr)

            self.assertTrue(removed)
            self.assertEqual(self._titles(index_path), {"Alpha"})

    def test_keeping_can_silence_future_reports(self):
        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path, idx_mgr = self._setup(tmp)
            with scripted_input("k", "y", default="n"), captured_output():
                main._prompt_clean_vanished(idx_mgr)

            with open(os.path.join(tmp, "ignored_vanished.json"), encoding="utf-8") as f:
                self.assertEqual(json.load(f), {"slugs": ["gone"]})
            self.assertEqual(self._titles(index_path), {"Alpha", "Gone"})
            self.assertEqual(main._find_vanished_to_clean(idx_mgr), {})

    def test_closed_stdin_keeps_everything(self):
        """An unattended run must not spin on the prompt, or delete on silence."""

        def eof(_prompt=""):
            raise EOFError

        with tempfile.TemporaryDirectory(prefix="aniworld_slug_") as tmp:
            index_path, idx_mgr = self._setup(tmp)
            with patch("builtins.input", eof), captured_output():
                removed = main._prompt_clean_vanished(idx_mgr)

            self.assertFalse(removed)
            self.assertEqual(self._titles(index_path), {"Alpha", "Gone"})


if __name__ == "__main__":
    unittest.main()
