"""Tests for the "Suggest something to watch" menu option (option 8).

These tests exercise the filtering and sampling logic of
`_suggest_something_to_watch` without requiring real terminal input by
mocking the interactive genre picker.
"""

import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402


class FakeIndex:
    """Minimal index manager stand-in."""

    def __init__(self, entries):
        self.series_index = {e["title"]: e for e in entries}


def _series(title, total, watched=0, url=None):
    return {
        "title": title,
        "url": url or f"https://aniworld.to/anime/stream/{title.lower().replace(' ', '-')}",
        "total_episodes": total,
        "watched_episodes": watched,
    }


class TestSuggestPicker(unittest.TestCase):
    """Option 8: suggest unwatched anime."""

    def _capture(self, index, genre_key="all", genre_data=None):
        """Run `_suggest_something_to_watch` and return its printed output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            if genre_data is not None:
                path = os.path.join(tmpdir, "genre_index.json")
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(genre_data, f)

            captured = io.StringIO()
            with (
                mock.patch.object(main, "DATA_DIR", tmpdir),
                mock.patch.object(main, "_prompt_genre_choice", return_value=genre_key),
                mock.patch.object(main.random, "shuffle"),
                mock.patch("sys.stdout", new=captured),
            ):
                main._suggest_something_to_watch(FakeIndex(index))
            return captured.getvalue()

    def test_all_returns_at_most_ten_suggestions(self):
        """The sample must never exceed 10, even with many candidates."""
        index = [_series(f"Anime {i}", total=12) for i in range(15)]
        out = self._capture(index, genre_key="all")
        self.assertIn("10 suggestion(s) from 15 unwatched anime", out)
        self.assertEqual(out.count("Anime "), 10)

    def test_fewer_candidates_than_limit(self):
        """If there are fewer than 10 candidates, show all of them."""
        index = [_series("Solo", total=12), _series("Duo", total=24), _series("Trio", total=6)]
        out = self._capture(index, genre_key="all")
        self.assertIn("3 suggestion(s) from 3 unwatched anime", out)
        self.assertEqual(out.count("https://aniworld.to/"), 3)

    def test_genre_filter_excludes_unmatched_series(self):
        """Picking a genre only suggests series tagged with that genre."""
        index = [
            _series("Action Hero", total=12),
            _series("Comedy Hour", total=12),
            _series("Mixed Bag", total=12),
        ]
        genre_data = {
            "labels": {"action": "Action", "comedy": "Comedy"},
            "series": {
                "Action Hero": ["action"],
                "Comedy Hour": ["comedy"],
                "Mixed Bag": ["action", "comedy"],
            },
        }
        out = self._capture(index, genre_key="action", genre_data=genre_data)
        self.assertIn("Action", out)
        self.assertIn("Mixed Bag", out)
        self.assertNotIn("Comedy Hour", out)
        self.assertIn("2 suggestion(s) from 2 unwatched anime", out)

    def test_no_candidates_message(self):
        """When nothing is unwatched, a clear message is printed."""
        index = [_series("Watched One", total=12, watched=12)]
        out = self._capture(index, genre_key="all")
        self.assertIn("No unwatched anime found", out)

    def test_watched_episodes_zero_required(self):
        """Series with any watched episodes must not appear as candidates."""
        index = [
            _series("Unwatched", total=12, watched=0),
            _series("Started", total=12, watched=1),
            _series("Finished", total=12, watched=12),
        ]
        out = self._capture(index, genre_key="all")
        self.assertIn("1 suggestion(s) from 1 unwatched anime", out)
        self.assertIn("Unwatched", out)
        self.assertNotIn("Started", out)
        self.assertNotIn("Finished", out)

    def test_back_option_returns_without_suggestions(self):
        """Choosing 0/Back from the genre picker must return to the menu."""
        index = [_series("Anime A", total=12), _series("Anime B", total=12)]
        out = self._capture(index, genre_key="__back__")
        self.assertNotIn("suggestion", out)
        self.assertNotIn("Anime A", out)
        self.assertNotIn("Anime B", out)


if __name__ == "__main__":
    unittest.main()
