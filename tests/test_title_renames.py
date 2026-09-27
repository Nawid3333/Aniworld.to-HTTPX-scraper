"""A series the site renames but keeps at its link must stay one index entry.

On the sibling s.to scraper, the site corrected "Die Minverva-Akademie" to
"Die Minerva-Akademie" and changed "Kein Frieden den Toten" to "Kein Friede
den Toten" without touching either link. Keyed by title, the next full scrape
read each as a brand-new series and added it beside the old entry, so the
index held three links twice and only the duplicate-slug check at startup
noticed. The save now asks about each rename before anything is diffed; this
scraper keys its index the same way, so it gets the same guard.
"""

from __future__ import annotations

import json
import unittest
from unittest import mock

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


def _read(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class _Answers:
    """Answer the rename prompt with *rename* and the final save with *save*."""

    def __init__(self, rename="y", save="y"):
        self.rename = rename
        self.save = save
        self.asked: list[str] = []

    def __call__(self, prompt=""):
        self.asked.append(prompt)
        return self.rename if "Rename" in prompt else self.save


class FindRenamesTests(unittest.TestCase):
    def test_a_new_title_on_an_indexed_link_is_a_rename(self):
        old = im._key_series([series("Die Minverva-Akademie", slug="die-minverva-akademie")])
        new = im._key_series([series("Die Minerva-Akademie", slug="die-minverva-akademie")])
        self.assertEqual(im._find_title_renames(old, new), [("Die Minverva-Akademie", "Die Minerva-Akademie")])

    def test_an_unchanged_title_is_not_a_rename(self):
        old = im._key_series([series("Dark", slug="dark")])
        new = im._key_series([series("Dark", slug="dark")])
        self.assertEqual(im._find_title_renames(old, new), [])

    def test_a_new_link_is_a_new_series_not_a_rename(self):
        old = im._key_series([series("Dark", slug="dark")])
        new = im._key_series([series("Lost", slug="lost")])
        self.assertEqual(im._find_title_renames(old, new), [])

    def test_two_series_sharing_a_title_are_not_renames_of_each_other(self):
        entries = [series("Wäldern", slug="waldern"), series("Wäldern", slug="wldern")]
        key_of = im._series_keyer(entries[:1], entries[1:])
        old = im._key_series(entries[:1], key_of)
        new = im._key_series(entries[1:], key_of)
        self.assertEqual(im._find_title_renames(old, new), [])

    def test_a_link_the_index_already_holds_twice_is_left_to_the_duplicate_prompt(self):
        old = im._key_series([series("A", slug="shared"), series("B", slug="shared")])
        new = im._key_series([series("C", slug="shared")])
        self.assertEqual(im._find_title_renames(old, new), [])


class SaveTests(unittest.TestCase):
    """The incident: the index has the old title, the scrape brings the new one."""

    def setUp(self):
        self.path = write_index(
            [
                series("Before", slug="before"),
                series("Kein Frieden den Toten", slug="kein-frieden-den-toten", watched=5, added_date="2026-07-27"),
                series("After", slug="after"),
            ]
        )
        self.manager = im.IndexManager(self.path)
        self.renamed = series("Kein Friede den Toten", slug="kein-frieden-den-toten", watched=5)

    def _save(self, answers, scraped=None):
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch("builtins.input", answers),
            captured_output(),
        ):
            return im.confirm_and_save_changes(scraped or [self.renamed], "test", self.manager)

    def test_an_approved_rename_leaves_one_entry_under_the_new_title(self):
        self.assertTrue(self._save(_Answers()))
        self.assertEqual(
            [e["title"] for e in _read(self.path)],
            ["Before", "Kein Friede den Toten", "After"],
        )

    def test_the_renamed_entry_keeps_its_history_and_its_old_name(self):
        self._save(_Answers())
        entry = next(e for e in _read(self.path) if e["title"] == "Kein Friede den Toten")
        self.assertEqual(entry["watched_episodes"], 5)
        self.assertEqual(entry["added_date"], "2026-07-27")
        self.assertIn("Kein Frieden den Toten", entry["alt_titles"])

    def test_a_rename_is_diffed_like_any_other_update(self):
        newer = series("Kein Friede den Toten", slug="kein-frieden-den-toten", watched=7)
        self._save(_Answers(), scraped=[newer])
        entry = next(e for e in _read(self.path) if e["title"] == "Kein Friede den Toten")
        self.assertEqual(entry["watched_episodes"], 7)

    def test_a_declined_rename_changes_nothing_and_adds_no_copy(self):
        before = _read(self.path)
        for answer in ("", "n", "j"):
            with self.subTest(answer=answer):
                self._save(_Answers(rename=answer))
                self.assertEqual(_read(self.path), before)

    def test_discarding_the_save_leaves_the_loaded_index_alone(self):
        self.assertFalse(self._save(_Answers(save="n")))
        self.assertIn("Kein Frieden den Toten", self.manager.series_index)
        self.assertEqual(self.manager.series_index["Kein Frieden den Toten"]["title"], "Kein Frieden den Toten")

    def test_each_rename_is_asked_about_on_its_own(self):
        answers = _Answers()
        self._save(answers)
        self.assertEqual(sum("Rename" in p for p in answers.asked), 1)


if __name__ == "__main__":
    unittest.main()
