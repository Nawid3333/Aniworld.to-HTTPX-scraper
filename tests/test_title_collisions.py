"""Two different series can share a title; the index must keep both.

Found on s.to, which lists "Wäldern" twice: /serie/waldern and /serie/wldern
are separate pages with the same name. This index code is shared with that
scraper. The index was a dict keyed by title, so saving one
replaced the other. A new-only scrape then found the missing slug, scraped it
and saved it over its namesake, which made *that* one missing on the next run
-- a loop that never settled and overwrote one entry's watch history with the
other's on every pass.

Identity is now title and slug together. The dict key stays the plain title,
since every prompt prints it, and only a title shared by two series gets the
slug appended ("Wäldern [wldern]").
"""

from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest import mock

import main
from src import index_manager as im
from tests._support import captured_output, series, write_index

ALLOW_EVERYTHING = dict.fromkeys(
    (
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
    ),
    True,
)


def _slugs(entries):
    return sorted(im._extract_slug(e) or "" for e in entries)


def _read(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class KeyingTests(unittest.TestCase):
    def test_a_unique_title_is_its_own_key(self):
        keyed = im._key_series([series("Dark", slug="dark"), series("Lost", slug="lost")])
        self.assertEqual(set(keyed), {"Dark", "Lost"})

    def test_a_shared_title_keeps_both_series_apart(self):
        keyed = im._key_series([series("Wäldern", slug="waldern"), series("Wäldern", slug="wldern")])
        self.assertEqual(set(keyed), {"Wäldern [waldern]", "Wäldern [wldern]"})
        self.assertEqual(_slugs(keyed.values()), ["waldern", "wldern"])

    def test_one_series_spelled_two_ways_is_still_one_key(self):
        # Case and percent-encoding are one slug (see src/slug.py), so they
        # must not count as a second series sharing the title.
        keyed = im._key_series([series("Show", slug="25%20Years"), series("Show", slug="25%20years")])
        self.assertEqual(list(keyed), ["Show"])


class LoadTests(unittest.TestCase):
    def test_both_namesakes_survive_a_load(self):
        path = write_index([series("Wäldern", slug="waldern"), series("Wäldern", slug="wldern")])
        manager = im.IndexManager(path)
        self.assertEqual(_slugs(manager.series_index.values()), ["waldern", "wldern"])


class SaveTests(unittest.TestCase):
    """The incident: the index has one Wäldern, the scrape brings the other."""

    def setUp(self):
        self.path = write_index([series("Wäldern", slug="waldern", watched=2, episodes_per_season=2)])
        self.newcomer = series("Wäldern", slug="wldern", episodes_per_season=2)

    def _save(self):
        manager = im.IndexManager(self.path)
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch("builtins.input", return_value="y"),
            captured_output(),
        ):
            result = im.confirm_and_save_changes([self.newcomer], "test", manager)
        return result, manager

    def test_the_newcomer_is_added_beside_its_namesake(self):
        result, manager = self._save()
        self.assertTrue(result)
        self.assertEqual(_slugs(_read(self.path)), ["waldern", "wldern"])
        self.assertEqual(_slugs(manager.series_index.values()), ["waldern", "wldern"])

    def test_the_existing_namesake_keeps_its_watch_history(self):
        self._save()
        kept = next(e for e in _read(self.path) if im._extract_slug(e) == "waldern")
        self.assertEqual(kept["watched_episodes"], 2)

    def test_the_newcomer_is_reported_as_new_not_as_a_change(self):
        old = [series("Wäldern", slug="waldern", watched=2, episodes_per_season=2)]
        changes = im.detect_changes(old, [self.newcomer])
        self.assertEqual(changes["new_series"], ["Wäldern [wldern]"])
        self.assertEqual(changes["newly_unwatched"], [])
        self.assertEqual(changes["removed_episodes"], [])


class RemovalTests(unittest.TestCase):
    def test_removing_one_namesake_leaves_the_other(self):
        waldern = series("Wäldern", slug="waldern")
        path = write_index([waldern, series("Wäldern", slug="wldern")])
        self.assertEqual(im.remove_series_from_index(path, [waldern]), 1)
        self.assertEqual(_slugs(_read(path)), ["wldern"])


class _RenamedOnSite:
    """A scraper whose live check finds the vanished URL alive under a new title."""

    def __init__(self, new_title):
        self.new_title = new_title

    async def verify_vanished_and_candidates(self, vanished, candidates):
        return [(self.new_title, url, True) for _title, url in vanished], []


class VanishedTableTests(unittest.TestCase):
    def test_a_live_rename_does_not_redirect_the_delete(self):
        """Delete after a re-scrape must still target the row's own entry.

        The re-scrape renamed the row to the title the site now shows. The
        delete then used that title, so it hit a different series already
        filed under it -- or nothing at all.
        """
        gone = series("Gone", slug="gone", watched=3)
        other = series("Other", slug="other", watched=5)
        old_data = {"Gone": gone, "Other": other}
        vanished = [("Gone", "not found", gone["url"])]
        with (
            mock.patch("builtins.input", side_effect=["r", "d", "y"]),
            captured_output(),
        ):
            to_delete = im._prompt_vanished_table(vanished, {}, old_data, scraper=_RenamedOnSite("Other"))
        self.assertEqual(to_delete, ["Gone"])


class DisappearedFromAccountTests(unittest.TestCase):
    def test_the_flag_is_cleared_on_the_series_that_left_not_its_namesake(self):
        left = series("Wäldern", slug="waldern", watchlist=True)
        still_listed = series("Wäldern", slug="wldern", watchlist=True)
        scraper = SimpleNamespace(
            all_discovered_series=[still_listed],
            failed_links=[],
            series_data=[dict(still_listed)],
        )
        pre_index = SimpleNamespace(series_index=im._key_series([left, still_listed]))
        with captured_output():
            main._inject_disappeared_series(scraper, pre_index, "watchlist")
        by_slug = {im._extract_slug(s): s for s in scraper.series_data}
        self.assertTrue(by_slug["wldern"]["watchlist"], "the series still on the watchlist stays on it")
        self.assertFalse(by_slug["waldern"]["watchlist"], "the one that left is the one flagged")


class OngoingExportTests(unittest.TestCase):
    def test_each_ongoing_namesake_exports_its_own_url(self):
        path = write_index(
            [
                series("Wäldern", slug="waldern", watched=1, episodes_per_season=2),
                series("Wäldern", slug="wldern", watched=1, episodes_per_season=2),
            ]
        )
        report = im.IndexManager(path).get_full_report()
        urls = report["categories"]["ongoing"]["urls"]
        self.assertEqual(sorted(u.rsplit("/", 1)[-1] for u in urls), ["waldern", "wldern"])


if __name__ == "__main__":
    unittest.main()
