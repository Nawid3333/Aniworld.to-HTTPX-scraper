"""Tests for the genre statistics feature (menu option 7).

Every parser case here comes from a page that really exists, captured in
`tests/fixtures/pages/`. The headline case is hidden genres: aniworld truncates
long genre lists with CSS, and a parser that reads the visible ones only looks
perfectly correct on short lists while silently losing genres on long ones.
"""

import gzip
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import genre_stats  # noqa: E402
from src.genre_stats import (  # noqa: E402
    build_snapshot,
    diff_snapshots,
    extract_genres,
    load_genres,
    normalize_genre_key,
    save_genres,
)
from src.scraper import _extract_title, make_soup  # noqa: E402

PAGE_DIR = Path(__file__).resolve().parent / "fixtures" / "pages"


def load_page(name):
    path = PAGE_DIR / f"series__{name}.html.gz"
    if not path.exists():
        return None
    return make_soup(gzip.decompress(path.read_bytes()).decode("utf-8"))


def keys_of(soup):
    return [key for key, _ in extract_genres(soup)]


def series_entry(total, watched, slug):
    """Minimal index entry: only what get_episode_counts and the slug join read."""
    episodes = [{"number": n + 1, "watched": n < watched} for n in range(total)]
    return {
        "title": slug,
        "link": f"/anime/stream/{slug}",
        "url": f"https://aniworld.to/anime/stream/{slug}",
        "seasons": [{"season": "1", "episodes": episodes}],
    }


class FakeIndex:
    def __init__(self, entries):
        self.series_index = {e["title"]: e for e in entries}


@unittest.skipUnless(PAGE_DIR.exists(), "no fixtures captured yet")
class TestHiddenGenresAreCaptured(unittest.TestCase):
    """The single most likely way this feature ships subtly wrong."""

    def test_one_piece_yields_every_genre_including_the_hidden_four(self):
        soup = load_page("one-piece")
        if soup is None:
            self.skipTest("one-piece fixture not captured")
        keys = keys_of(soup)
        self.assertEqual(len(keys), 11, f"expected 11 genres, got {keys}")
        self.assertIn("komoedie", keys, "a genre after the '+ 4' button was dropped")

    def test_bleach_yields_every_genre_including_the_hidden_two(self):
        soup = load_page("bleach")
        if soup is None:
            self.skipTest("bleach fixture not captured")
        self.assertEqual(len(keys_of(soup)), 9)

    def test_the_show_more_button_is_never_returned_as_a_genre(self):
        for name in ("one-piece", "bleach", "naruto-shippuden", "a-whisker-away"):
            soup = load_page(name)
            if soup is None:
                continue
            with self.subTest(page=name):
                for key, label in extract_genres(soup):
                    self.assertTrue(key, "empty genre key")
                    self.assertNotIn("+", key)
                    self.assertFalse(label.strip().startswith("+"), f"button text leaked: {label!r}")

    def test_parsed_count_matches_what_the_page_says_it_hides(self):
        """The page announces the hidden count, so it can check our work."""
        for name in ("one-piece", "bleach", "detektiv-conan", "naruto-shippuden"):
            soup = load_page(name)
            if soup is None:
                continue
            hidden = genre_stats._hidden_genre_count(soup)
            if hidden is None:
                continue
            visible = 0
            for el in soup.select("div.genres li a[itemprop='genre'], div.genres li button.hiddenArea"):
                if el.name == "button":
                    break
                visible += 1
            with self.subTest(page=name):
                self.assertEqual(len(extract_genres(soup)), visible + hidden)

    def test_no_upper_bound_is_assumed(self):
        """Fixtures sample the site; they do not specify it."""
        counts = {}
        for path in sorted(PAGE_DIR.glob("series__*.html.gz")):
            soup = make_soup(gzip.decompress(path.read_bytes()).decode("utf-8"))
            genres = extract_genres(soup)
            if genres:
                counts[path.name] = len(genres)
        self.assertTrue(counts, "no fixture produced genres")
        self.assertGreaterEqual(max(counts.values()), 11)


class TestSeriesTitles(unittest.TestCase):
    """Displayed names must be real titles ("One Piece"), never slugs ("one-piece")."""

    def test_title_is_read_from_the_h1_not_the_url_slug(self):
        for name, expected in (("one-piece", "One Piece"), ("bleach", "Bleach")):
            soup = load_page(name)
            if soup is None:
                self.skipTest(f"{name} fixture not captured")
            with self.subTest(page=name):
                self.assertEqual(_extract_title(soup), expected)

    def test_a_page_with_no_usable_heading_falls_back_to_the_slug(self):
        soup = make_soup("<html><body><p>no heading here</p></body></html>")
        self.assertIsNone(_extract_title(soup))


class TestGenreIdentity(unittest.TestCase):
    def test_href_and_display_text_normalize_to_the_same_key(self):
        self.assertEqual(
            normalize_genre_key("/genre/fighting-shounen"),
            normalize_genre_key("Fighting-Shounen"),
        )

    def test_full_urls_paths_and_bare_text_all_agree(self):
        for value in (
            "https://aniworld.to/genre/action",
            "/genre/action",
            "genre/action",
            "Action",
            "  action  ",
        ):
            with self.subTest(value=value):
                self.assertEqual(normalize_genre_key(value), "action")

    def test_multiword_genres_collapse_whitespace(self):
        self.assertEqual(normalize_genre_key("Science Fiction"), "science-fiction")
        self.assertEqual(normalize_genre_key("True  Crime"), "true-crime")

    def test_junk_is_empty_not_an_exception(self):
        for value in (None, "", "   ", 42, []):
            with self.subTest(value=value):
                self.assertEqual(normalize_genre_key(value), "")

    def test_a_trailing_comma_is_stripped(self):
        self.assertEqual(normalize_genre_key("Krimi,"), "krimi")


class TestSlugJoin(unittest.TestCase):
    """Trap: a slug mismatch zeroes every number without raising anything."""

    def test_index_entries_are_found_by_catalogue_slug(self):
        entries = [series_entry(12, 12, "bleach"), series_entry(10, 3, "one-piece")]
        by_slug = genre_stats._index_by_slug(FakeIndex(entries))
        self.assertEqual(set(by_slug), {"bleach", "one-piece"})

    def test_a_full_url_and_a_bare_path_resolve_to_the_same_slug(self):
        entry = series_entry(1, 1, "bleach")
        only_url = {k: v for k, v in entry.items() if k != "link"}
        by_slug = genre_stats._index_by_slug(FakeIndex([only_url]))
        self.assertIn("bleach", by_slug)

    def test_entries_without_a_usable_slug_are_dropped_not_crashed_on(self):
        broken = {"title": "x", "link": "", "url": "", "seasons": []}
        self.assertEqual(genre_stats._index_by_slug(FakeIndex([broken])), {})


class TestSnapshot(unittest.TestCase):
    def setUp(self):
        self.entries = [
            series_entry(10, 10, "done-one"),
            series_entry(10, 4, "partial"),
            series_entry(0, 0, "empty"),
        ]
        self.by_slug = genre_stats._index_by_slug(FakeIndex(self.entries))
        self.data = {
            "series": {
                "done-one": ["action", "drama"],
                "partial": ["action"],
                "empty": ["drama"],
                "not-in-index": ["action"],
            },
            "labels": {"action": "Action", "drama": "Drama"},
            "catalogue_total": 4,
            "scraped_count": 4,
        }

    def test_a_series_counts_in_every_one_of_its_genres(self):
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["action"], {"done": 1, "indexed": 2})
        self.assertEqual(cats["drama"], {"done": 1, "indexed": 2})

    def test_column_totals_exceed_the_series_count_by_design(self):
        snap = build_snapshot(self.data, self.by_slug)
        indexed_sum = sum(c["indexed"] for c in snap["categories"].values())
        self.assertGreater(indexed_sum, snap["indexed_series"])

    def test_a_zero_episode_series_is_not_counted_as_done(self):
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["drama"]["done"], 1)

    def test_series_not_in_the_index_do_not_inflate_category_counts(self):
        # "not-in-index" carries "action" too, but only indexed series are
        # counted -- the site-vs-index "Site" column was tried and dropped
        # (see build_snapshot's docstring), so a catalogue-only series must
        # not show up anywhere in the table.
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["action"]["indexed"], 2)

    def test_series_with_no_genres_are_reported_not_silently_dropped(self):
        self.data["series"]["genreless"] = []
        snap = build_snapshot(self.data, self.by_slug)
        self.assertEqual(snap["without_genres"], ["genreless"])
        self.assertNotIn("", snap["categories"])

    def test_indexed_series_missing_from_the_genre_data_are_reported(self):
        del self.data["series"]["partial"]
        snap = build_snapshot(self.data, self.by_slug)
        self.assertIn("partial", snap["indexed_without_genre_data"])


class TestDiff(unittest.TestCase):
    def test_a_new_series_is_reported_with_its_genres(self):
        d = diff_snapshots({}, {"bleach": ["action"]})
        self.assertEqual(d["new_series"], [("bleach", ["action"])])

    def test_gained_and_lost_genres_are_both_reported(self):
        d = diff_snapshots({"x": ["action", "drama"]}, {"x": ["action", "fantasy"]})
        self.assertEqual(d["changed"], [("x", ["fantasy"], ["drama"])])

    def test_an_unchanged_series_produces_no_change(self):
        d = diff_snapshots({"x": ["action"]}, {"x": ["action"]})
        self.assertEqual(d["changed"], [])
        self.assertEqual(d["new_series"], [])

    def test_genre_order_alone_is_not_a_change(self):
        d = diff_snapshots({"x": ["action", "drama"]}, {"x": ["drama", "action"]})
        self.assertEqual(d["changed"], [])

    def test_new_and_vanished_categories_are_reported(self):
        d = diff_snapshots({"x": ["old"]}, {"x": ["new"]})
        self.assertEqual(d["new_categories"], ["new"])
        self.assertEqual(d["gone_categories"], ["old"])


class TestChangeLines(unittest.TestCase):
    """Change-list lines show real titles, never bare slugs."""

    def test_a_new_series_is_shown_by_title(self):
        data = {"labels": {"action": "Action"}, "titles": {"bleach": "Bleach"}}
        changes = diff_snapshots({}, {"bleach": ["action"]})
        lines = genre_stats._change_lines(data, changes)
        self.assertIn("Bleach is new in Action", lines[0])
        self.assertNotIn("bleach ", lines[0])

    def test_a_slug_with_no_known_title_falls_back_to_the_slug(self):
        data = {"labels": {}, "titles": {}}
        changes = diff_snapshots({}, {"one-punch-man": ["action"]})
        lines = genre_stats._change_lines(data, changes)
        self.assertIn("one-punch-man", lines[0])


class TestRowOrdering(unittest.TestCase):
    def test_most_complete_first_then_largest_among_ties(self):
        cats = {
            "done": {"done": 1, "indexed": 1},
            "small": {"done": 0, "indexed": 3},
            "big": {"done": 0, "indexed": 40},
            "half": {"done": 5, "indexed": 10},
        }
        labels = {k: k for k in cats}
        order = [r[0] for r in genre_stats._sorted_rows(cats, labels)]
        self.assertEqual(order, ["done", "half", "big", "small"])

    def test_an_empty_category_does_not_divide_by_zero(self):
        cats = {"none": {"done": 0, "indexed": 0}}
        rows = genre_stats._sorted_rows(cats, {"none": "None"})
        self.assertEqual(rows, [("None", 0, 0)])
        self.assertTrue(genre_stats._table_lines(rows))


class TestBar(unittest.TestCase):
    def test_bar_endpoints_and_width(self):
        self.assertEqual(genre_stats._bar(0, 10), "░" * 10)
        self.assertEqual(genre_stats._bar(100, 10), "█" * 10)
        self.assertEqual(len(genre_stats._bar(37.4, 10)), 10)

    def test_out_of_range_percentages_are_clamped(self):
        self.assertEqual(len(genre_stats._bar(-5, 10)), 10)
        self.assertEqual(genre_stats._bar(150, 10), "█" * 10)


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_a_missing_file_is_an_empty_skeleton_not_an_error(self):
        data = load_genres()
        self.assertEqual(data["series"], {})
        self.assertEqual(data["version"], genre_stats.SCHEMA_VERSION)

    def test_round_trip_preserves_the_data(self):
        data = load_genres()
        data["series"] = {"bleach": ["action"]}
        data["labels"] = {"action": "Action"}
        data["titles"] = {"bleach": "Bleach"}
        data["host"] = "https://aniworld.to"
        save_genres(data)
        reloaded = load_genres()
        self.assertEqual(reloaded["series"], {"bleach": ["action"]})
        self.assertEqual(reloaded["titles"], {"bleach": "Bleach"})

    def test_keys_are_written_sorted_for_clean_diffs(self):
        data = load_genres()
        data["series"] = {"zzz": ["b"], "aaa": ["a"]}
        data["labels"] = {"b": "B", "a": "A"}
        save_genres(data)
        with open(self.path, encoding="utf-8") as fh:
            written = json.load(fh)
        self.assertEqual(list(written["series"]), ["aaa", "zzz"])

    def test_no_backup_file_is_left_behind(self):
        save_genres(load_genres())
        self.assertEqual(list(Path(self.tmp.name).glob("*.bak*")), [])

    def test_corrupt_json_yields_an_empty_skeleton(self):
        Path(self.path).write_text("{not json", encoding="utf-8")
        self.assertEqual(load_genres()["series"], {})

    def test_an_unknown_schema_version_is_discarded_not_guessed_at(self):
        Path(self.path).write_text(json.dumps({"version": 99, "series": {"x": ["y"]}}), encoding="utf-8")
        self.assertEqual(load_genres()["series"], {})

    def test_a_wrongly_typed_series_map_is_rejected(self):
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": [], "labels": {}}),
            encoding="utf-8",
        )
        self.assertEqual(load_genres()["series"], {})

    def test_a_wrongly_typed_titles_map_is_rejected(self):
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": {}, "labels": {}, "titles": []}),
            encoding="utf-8",
        )
        self.assertEqual(load_genres()["series"], {})

    def test_a_file_saved_before_titles_existed_still_loads(self):
        """Additive field: an older genre_index.json simply has no titles yet."""
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": {"bleach": ["action"]}, "labels": {}}),
            encoding="utf-8",
        )
        data = load_genres()
        self.assertEqual(data["series"], {"bleach": ["action"]})
        self.assertEqual(data["titles"], {})


class TestChangeListConsumption(unittest.TestCase):
    """Viewing the change list must mark it as seen, or it repeats forever."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()
        data = genre_stats.load_genres()
        data["catalogue_total"] = 1
        data["scraped_count"] = 1
        data["labels"] = {"action": "Action"}
        data["previous_series"] = {}
        data["series"] = {"bleach": ["action"]}
        save_genres(data)

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_a_second_view_with_no_new_scrape_shows_no_changes(self):
        entry = series_entry(1, 1, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            first = diff_snapshots(load_genres().get("previous_series") or {}, load_genres()["series"])
            self.assertTrue(first["new_series"])
            genre_stats.show_stats()
            second = diff_snapshots(load_genres().get("previous_series") or {}, load_genres()["series"])
        self.assertEqual(second["new_series"], [])
        self.assertEqual(second["changed"], [])

    def test_viewing_stats_does_not_touch_series_data_itself(self):
        entry = series_entry(1, 1, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.show_stats()
        self.assertEqual(load_genres()["series"], {"bleach": ["action"]})


class TestExportReport(unittest.TestCase):
    """Regression pin: title and genres must live together per series, not
    as a separate top-level "titles" map a consumer has to cross-reference."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.index_path = str(Path(self.tmp.name) / "genre_index.json")
        self.report_path = str(Path(self.tmp.name) / "genre_report.json")
        self.patches = [
            mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.index_path),
            mock.patch.object(genre_stats, "GENRE_REPORT_FILE", self.report_path),
        ]
        for p in self.patches:
            p.start()
        data = genre_stats.load_genres()
        data["catalogue_total"] = 1
        data["scraped_count"] = 1
        data["labels"] = {"action": "Action"}
        data["titles"] = {"bleach": "Bleach"}
        data["series"] = {"bleach": ["action"]}
        save_genres(data)

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def test_title_and_genres_live_together_per_series(self):
        entry = series_entry(1, 1, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.export_report()
        with open(self.report_path, encoding="utf-8") as fh:
            report = json.load(fh)
        self.assertNotIn("titles", report, "a separate top-level titles map defeats the point of merging")
        self.assertEqual(report["series"], {"bleach": {"title": "Bleach", "genres": ["action"]}})

    def test_a_slug_with_no_known_title_falls_back_to_the_slug(self):
        data = genre_stats.load_genres()
        data["series"]["one-punch-man"] = ["action"]
        save_genres(data)
        entry = series_entry(1, 1, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.export_report()
        with open(self.report_path, encoding="utf-8") as fh:
            report = json.load(fh)
        self.assertEqual(report["series"]["one-punch-man"]["title"], "one-punch-man")


class TestUnwatchedByGenre(unittest.TestCase):
    """Option 7 sub menu 4: list unwatched series by genre with a back option."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.index_path = str(Path(self.tmp.name) / "series_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def _write_data(self, labels=None, series=None, titles=None):
        data = genre_stats.load_genres()
        data["labels"] = labels or {"action": "Action", "comedy": "Comedy"}
        data["series"] = series or {"bleach": ["action"], "naruto": ["comedy"]}
        data["titles"] = titles or {"bleach": "Bleach", "naruto": "Naruto"}
        save_genres(data)

    @mock.patch.object(genre_stats, "_prompt_genre_choice", return_value="__back__")
    def test_back_option_returns_early(self, _mock_picker):
        """Choosing 0/Back from the picker must not print unwatched series."""
        self._write_data()
        entry = series_entry(12, 0, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            captured = io.StringIO()
            with mock.patch("sys.stdout", new=captured):
                genre_stats.list_unwatched_by_genre()
        out = captured.getvalue()
        self.assertNotIn("Unwatched anime", out)
        self.assertNotIn("Bleach", out)

    @mock.patch.object(genre_stats, "_prompt_genre_choice", return_value="action")
    def test_selected_genre_filters_unwatched(self, _mock_picker):
        """Picking a genre only lists unwatched series tagged with that genre."""
        self._write_data()
        entries = [
            series_entry(12, 0, "bleach"),
            series_entry(12, 0, "naruto"),
        ]
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex(entries)):
            captured = io.StringIO()
            with mock.patch("sys.stdout", new=captured):
                genre_stats.list_unwatched_by_genre()
        out = captured.getvalue()
        self.assertIn("Unwatched anime", out)
        self.assertIn("bleach", out)
        self.assertNotIn("naruto", out)

    def test_picker_resolves_back_input(self):
        """The picker returns __back__ when the user types 0."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with mock.patch("sys.stdout.isatty", return_value=False), mock.patch("builtins.input", return_value="0"):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "__back__")

    def test_picker_resolves_label_input(self):
        """The picker returns the matching key for a typed genre label."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with mock.patch("sys.stdout.isatty", return_value=False), mock.patch("builtins.input", return_value="Action"):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "action")

    def test_picker_rejects_unknown_input_and_retries(self):
        """Unknown input loops back to retry instead of returning a default."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with (
            mock.patch("sys.stdout.isatty", return_value=False),
            mock.patch("builtins.input", side_effect=["nonsense", "Action"]),
        ):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "action")


if __name__ == "__main__":
    unittest.main()
