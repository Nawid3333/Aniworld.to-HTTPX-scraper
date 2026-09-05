"""A series that failed to scrape must not be written into the index.

The scraper keeps an "_error" placeholder in series_data for every series it
could not read, so a checkpoint stays complete across a pause/resume. Those
placeholders carry no seasons and no episodes.

For a series the index already holds this is harmless -- _build_merged_data
walks the *new* entry's seasons, finds none, and leaves the stored entry
untouched. For a series the index has never seen it is not: the placeholder
takes the "new series" branch and is inserted as a genuine entry with zero
episodes. It then shows up in every report as a real series with nothing in
it, and the next successful scrape reads its entire episode list as newly
added and asks the user to approve all of it.

Both sibling scrapers (s.to, bs.to) have filtered these out since their first
release. This one did not, and its runs are the same shape.

Run with:  python -m unittest discover -s tests
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import index_manager as im  # noqa: E402

# Approve every category, so nothing this test asserts on can be explained by
# a prompt having said no.
ALLOW_EVERYTHING = {
    "new_series": True,
    "new_episodes": True,
    "watched": True,
    "unwatched": True,
    "subscribe": True,
    "unsubscribe": True,
    "watchlist_add": True,
    "watchlist_remove": True,
    "title_ger": True,
    "title_eng": True,
    "episode_remove": True,
    "season_remove": True,
}


def _series(title, season, watched_flags):
    """One index entry with a single season and the given watched flags."""
    episodes = [
        {"number": i + 1, "watched": w, "title_ger": f"E{i + 1}", "title_eng": ""} for i, w in enumerate(watched_flags)
    ]
    watched = sum(1 for w in watched_flags if w)
    slug = title.lower()
    return {
        "url": f"https://aniworld.to/anime/stream/{slug}",
        "link": f"/anime/stream/{slug}",
        "title": title,
        "title_ger": title,
        "title_eng": "",
        "subscribed": True,
        "watchlist": False,
        "total_seasons": 1,
        "total_episodes": len(episodes),
        "watched_episodes": watched,
        "unwatched_episodes": len(episodes) - watched,
        "seasons": [
            {
                "season": season,
                "url": f"https://aniworld.to/anime/stream/{slug}/staffel-{season}",
                "episodes": episodes,
                "watched_episodes": watched,
                "total_episodes": len(episodes),
            }
        ],
    }


class TestErrorEntriesAreNotSaved(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.index_file = str(Path(self.dir.name) / "series_index.json")
        with open(self.index_file, "w", encoding="utf-8") as f:
            json.dump([_series("Good", 1, [True] * 12)], f)

    def _saved(self):
        with open(self.index_file, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = list(data.values())
        return {s["title"]: s for s in data}

    @staticmethod
    def _failed(title):
        """What the scraper stores for a series it could not read."""
        slug = title.lower()
        return {
            "title": title,
            "url": f"https://aniworld.to/anime/stream/{slug}",
            "link": f"/anime/stream/{slug}",
            "_error": True,
            "_error_reason": "network unreachable",
            "total_episodes": 0,
            "watched_episodes": 0,
            "seasons": [],
        }

    def _save(self, new_data):
        manager = im.IndexManager(self.index_file)
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch.object(im, "_prompt_episode_mismatches", return_value=(True, None)),
            mock.patch("builtins.input", return_value="y"),
        ):
            return im.confirm_and_save_changes(new_data, "test run", manager)

    def test_a_failed_new_series_is_not_inserted(self):
        """The regression: "Broken" used to be added as a real 0-episode entry."""
        self._save([_series("Good", 1, [True] * 12), self._failed("Broken")])
        saved = self._saved()
        self.assertNotIn("Broken", saved)
        # Filtering the failure must not cost the series that did come back.
        self.assertIn("Good", saved)

    def test_the_dict_form_is_filtered_too(self):
        """confirm_and_save_changes is handed a list by main.py and a dict by
        the rescrape path, and both reach the same merge."""
        self._save({"Broken": self._failed("Broken")})
        self.assertNotIn("Broken", self._saved())

    def test_a_stored_series_keeps_its_episodes(self):
        """Not the regression -- the merge already ignores a season-less entry.

        Pinned anyway because it is the guarantee that actually matters, and
        it should hold whichever of the two defences is doing the work.
        """
        self._save({"Good": self._failed("Good")})
        stored = self._saved()["Good"]
        self.assertEqual(stored["total_episodes"], 12)
        self.assertEqual(stored["watched_episodes"], 12)
