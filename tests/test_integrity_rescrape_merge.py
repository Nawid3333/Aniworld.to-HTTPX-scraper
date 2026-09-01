"""Choosing "delete & rescrape" must not discard the confirmations already given.

The integrity check runs at the very end of confirm_and_save_changes, after
the user has answered every approval prompt for the whole run. When one
series trips a critical mismatch and the user picks "delete index & rescrape",
that decision concerns *that series only* -- every other approval in the run
(new episodes, newly watched, removed seasons) was still given and still
belongs in the index.

Observed live: the sibling s.to project's run read a series as 12/12 watched, the user
approved it, Futurama tripped the critical check, the user chose to rescrape
it -- and Siren's Kiss stayed at 7/12 in the index. The following two scrapes
re-detected and re-prompted for the same changes, because they had never been
saved. The three index backups on disk (7/12, 7/12, 7/12, then 12/12 only
after a separate single-link run) record exactly that.

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


def _series(title, season, watched_flags):
    """One index entry with a single season and the given watched flags."""
    episodes = [
        {"number": i + 1, "watched": w, "title_ger": f"E{i + 1}", "title_eng": ""}
        for i, w in enumerate(watched_flags)
    ]
    return {
        "url": f"https://aniworld.to/anime/stream/{title.lower()}",
        "link": f"/anime/stream/{title.lower()}",
        "title": title,
        "title_ger": title,
        "title_eng": "",
        "subscribed": True,
        "watchlist": False,
        "total_seasons": 1,
        "total_episodes": len(episodes),
        "watched_episodes": sum(1 for w in watched_flags if w),
        "unwatched_episodes": sum(1 for w in watched_flags if not w),
        "seasons": [
            {
                "season": season,
                "url": f"https://aniworld.to/anime/stream/{title.lower()}/staffel-{season}",
                "episodes": episodes,
                "watched_episodes": sum(1 for w in watched_flags if w),
                "total_episodes": len(episodes),
            }
        ],
    }


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


class TestApprovalsSurviveCriticalRescrape(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.index_file = str(Path(self.dir.name) / "series_index.json")

        # "Kept" is an ordinary series the user approves a watch change for.
        # "Critical" loses an episode, which is what trips the integrity check.
        old = [
            _series("Kept", 1, [True] * 7 + [False] * 5),
            _series("Critical", 1, [True, True]),
        ]
        with open(self.index_file, "w", encoding="utf-8") as f:
            json.dump(old, f)

        self.new_data = {
            "Kept": _series("Kept", 1, [True] * 12),
            "Critical": _series("Critical", 1, [True]),
        }

    def _saved_index(self):
        with open(self.index_file, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = list(data.values())
        return {s["title"]: s for s in data}

    def _run(self):
        """Approve everything, then choose 'delete & rescrape' for Critical."""
        manager = im.IndexManager(self.index_file)
        rescrape = {
            "urls": ["https://aniworld.to/anime/stream/critical"],
            "titles": ["Critical"],
        }
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch.object(im, "_prompt_episode_mismatches", return_value=(False, rescrape)),
            mock.patch("builtins.input", return_value="y"),
        ):
            return im.confirm_and_save_changes(self.new_data, "test run", manager)

    def test_rescrape_is_still_requested(self):
        result = self._run()
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("rescrape"))
        self.assertEqual(result["titles"], ["Critical"])

    def test_declining_the_save_also_cancels_the_rescrape(self):
        """Declining the final save must not still delete and rescrape.

        main.py acts on the returned dict by deleting those series from the
        index. Handing it back after the user answered "n" to "Save these
        changes?" would destroy data on the strength of a prompt they had
        just refused, so the refusal has to cancel both halves.
        """
        manager = im.IndexManager(self.index_file)
        rescrape = {
            "urls": ["https://aniworld.to/anime/stream/critical"],
            "titles": ["Critical"],
        }
        with (
            mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch.object(im, "_prompt_episode_mismatches", return_value=(False, rescrape)),
            mock.patch("builtins.input", return_value="n"),
        ):
            result = im.confirm_and_save_changes(self.new_data, "test run", manager)
        self.assertIs(result, False)
        self.assertEqual(self._saved_index()["Kept"]["watched_episodes"], 7)

    def test_approved_watch_change_is_saved(self):
        """The regression: approvals for every other series must persist."""
        self._run()
        kept = self._saved_index()["Kept"]
        self.assertEqual(
            kept["watched_episodes"],
            12,
            "approved watch change was discarded when a critical rescrape was chosen",
        )


if __name__ == "__main__":
    unittest.main()
