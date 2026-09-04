"""Tests for the genre feature's presentation and menu layer.

test_genre_stats.py covers the parsing and the arithmetic. This file covers
what the user actually sees and drives: the status header, the stats screen,
the exported report, the unwatched filter, and the menu loop that reaches
them. These paths decide whether a partial scrape is announced or silently
presented as the whole picture, so they are worth pinning.

Run with:  python -m unittest discover -s tests
"""

import io
import json
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import genre_stats  # noqa: E402

SLUG_PREFIX = "/anime/stream"


def series_entry(total, watched, slug, title=None):
    """Minimal index entry: only what get_episode_counts and the slug join read."""
    episodes = [{"number": n + 1, "watched": n < watched} for n in range(total)]
    return {
        "title": title or slug,
        "link": f"{SLUG_PREFIX}/{slug}",
        "url": f"https://example.test{SLUG_PREFIX}/{slug}",
        "seasons": [{"season": "1", "episodes": episodes}],
    }


class FakeIndex:
    def __init__(self, entries):
        self.series_index = {e["title"]: e for e in entries}


def genre_data(**overrides):
    """A complete, valid genre-data dict, so tests only state what they vary."""
    data = {
        "version": genre_stats.SCHEMA_VERSION,
        "generated": "2026-01-02T03:04:05.678901",
        "host": "https://example.test",
        "catalogue_total": 3,
        "scraped_count": 3,
        "series": {"done-one": ["action"], "partial": ["action", "drama"]},
        "labels": {"action": "Action", "drama": "Drama"},
        "titles": {"done-one": "Done One", "partial": "Partial One"},
        "previous_series": {},
    }
    data.update(overrides)
    return data


class _FlowTest(unittest.TestCase):
    """Isolates genre data, the report path, and the series index."""

    entries = [series_entry(10, 10, "done-one", "Done One"), series_entry(10, 4, "partial", "Partial One")]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        tmpdir = Path(self.tmp.name)

        self.report_path = str(tmpdir / "genre_report.json")
        self.index_path = str(tmpdir / "genre_index.json")
        for attr, value in (
            ("GENRE_REPORT_FILE", self.report_path),
            ("GENRE_INDEX_FILE", self.index_path),
        ):
            patch = mock.patch.object(genre_stats, attr, value)
            patch.start()
            self.addCleanup(patch.stop)

        index_patch = mock.patch.object(genre_stats, "IndexManager", lambda _path: FakeIndex(self.entries))
        index_patch.start()
        self.addCleanup(index_patch.stop)

    def run_capturing(self, fn, *args, **kwargs):
        out = io.StringIO()
        with redirect_stdout(out):
            fn(*args, **kwargs)
        return out.getvalue()

    def with_data(self, data):
        patch = mock.patch.object(genre_stats, "load_genres", return_value=data)
        patch.start()
        self.addCleanup(patch.stop)
        return data


class PartialDetectionTests(unittest.TestCase):
    """One definition of "partial", used by every screen."""

    def test_a_scrape_that_stopped_early_is_partial(self):
        self.assertTrue(genre_stats._is_partial({"scraped_count": 5, "catalogue_total": 10}))

    def test_a_complete_scrape_is_not_partial(self):
        self.assertFalse(genre_stats._is_partial({"scraped_count": 10, "catalogue_total": 10}))

    def test_nothing_scraped_yet_is_not_partial(self):
        """Zero is "not started", which the empty-data screens handle instead."""
        self.assertFalse(genre_stats._is_partial({"scraped_count": 0, "catalogue_total": 10}))

    def test_more_scraped_than_the_catalogue_reported_is_not_partial(self):
        self.assertFalse(genre_stats._is_partial({"scraped_count": 12, "catalogue_total": 10}))


class FormatWhenTests(unittest.TestCase):
    def test_an_iso_timestamp_is_rendered_to_the_minute(self):
        self.assertEqual(genre_stats._format_when({"generated": "2026-01-02T03:04:05.678901"}), "2026-01-02 03:04")

    def test_an_unset_timestamp_says_unknown_rather_than_blank(self):
        self.assertEqual(genre_stats._format_when({"generated": ""}), "unknown")


class StatusLineTests(unittest.TestCase):
    """The header on the menu, so a stale file is visible before anything runs."""

    def test_no_data_yet_points_at_the_scrape_option(self):
        lines = genre_stats._status_lines(genre_data(series={}))
        self.assertEqual(len(lines), 1)
        self.assertIn("none yet", lines[0])

    def test_the_counts_and_scrape_time_are_reported(self):
        lines = genre_stats._status_lines(genre_data())
        self.assertIn("3", lines[0])
        self.assertIn("2 genres", lines[0])
        self.assertIn("2026-01-02 03:04", lines[1])

    def test_a_complete_scrape_carries_no_partial_warning(self):
        self.assertEqual(len(genre_stats._status_lines(genre_data())), 2)

    def test_a_partial_scrape_is_called_out(self):
        lines = genre_stats._status_lines(genre_data(scraped_count=1, catalogue_total=3))
        self.assertEqual(len(lines), 3)
        self.assertIn("PARTIAL", lines[2])


class ShowStatsTests(_FlowTest):
    def test_no_data_yet_points_at_the_scrape_option(self):
        self.with_data(genre_data(series={}))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("No genre data yet", out)

    def test_data_with_no_parsed_genres_reports_a_broken_parser(self):
        """Silently showing an empty table would look like "no anime match"."""
        self.with_data(genre_data(series={"done-one": []}, labels={}))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("parser may be broken", out)

    def test_the_header_names_the_host_and_the_scrape_time(self):
        self.with_data(genre_data())
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("example.test", out)
        self.assertIn("2026-01-02 03:04", out)

    def test_every_genre_appears_in_the_table(self):
        self.with_data(genre_data())
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("Action", out)
        self.assertIn("Drama", out)

    def test_the_totals_line_reports_fully_watched_over_indexed(self):
        self.with_data(genre_data())
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("1/2", out)

    def test_a_partial_scrape_is_flagged_above_the_table(self):
        self.with_data(genre_data(scraped_count=1, catalogue_total=3))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("PARTIAL", out)

    def test_changes_since_last_check_are_listed(self):
        self.with_data(genre_data(previous_series={"done-one": ["action"]}))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("change(s) since you last checked", out)
        self.assertIn("Partial One", out)

    def test_viewing_the_changes_marks_them_seen(self):
        """Otherwise the same list is reported on every run, forever."""
        data = self.with_data(genre_data(previous_series={"done-one": ["action"]}))
        saved = {}
        with mock.patch.object(genre_stats, "save_genres", side_effect=lambda d: saved.update(d)):
            self.run_capturing(genre_stats.show_stats)
        self.assertEqual(saved["previous_series"], data["series"])

    def test_nothing_is_saved_when_there_is_nothing_new(self):
        self.with_data(genre_data(previous_series=genre_data()["series"]))
        with mock.patch.object(genre_stats, "save_genres") as save:
            out = self.run_capturing(genre_stats.show_stats)
        save.assert_not_called()
        self.assertNotIn("since you last checked", out)

    def test_a_long_change_list_is_capped_with_a_count_of_the_rest(self):
        """A big diff must not push the table itself off the screen."""
        series = {f"s{n}": ["action"] for n in range(30)}
        self.with_data(genre_data(series={**genre_data()["series"], **series}))
        out = self.run_capturing(genre_stats.show_stats)

        # Matched rather than asserted inline: a missing header or footer means
        # the cap did not run at all, and saying so beats an AttributeError.
        header = re.search(r"(\d+) change\(s\) since you last checked", out)
        footer = re.search(r"\.\.\. and (\d+) more", out)
        self.assertIsNotNone(header, "no change-count header was printed")
        self.assertIsNotNone(footer, "the change list was not capped")
        assert header is not None and footer is not None  # narrows for the type checker

        total = int(header.group(1))
        shown = len([ln for ln in out.splitlines() if ln.startswith("    + ") or ln.startswith("    ~ ")])
        remainder = int(footer.group(1))

        self.assertGreater(total, 25)
        self.assertEqual(shown, 25, "the change list is capped at 25 lines")
        self.assertEqual(remainder, total - 25)
        self.assertIn("GENRES", out, "the table must still be reached")

    def test_series_with_no_genre_data_are_reported_not_hidden(self):
        self.with_data(genre_data(series={"done-one": ["action"], "partial": []}))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("returned no genre data", out)

    def test_indexed_series_missing_from_the_catalogue_are_reported(self):
        self.with_data(genre_data(series={"done-one": ["action"]}))
        out = self.run_capturing(genre_stats.show_stats)
        self.assertIn("not in the catalogue", out)


class ExportReportTests(_FlowTest):
    def test_no_data_yet_writes_nothing(self):
        self.with_data(genre_data(series={}))
        out = self.run_capturing(genre_stats.export_report)
        self.assertIn("No genre data yet", out)
        self.assertFalse(Path(self.report_path).exists())

    def _export(self, data=None):
        self.with_data(data or genre_data())
        self.run_capturing(genre_stats.export_report)
        return json.loads(Path(self.report_path).read_text(encoding="utf-8"))

    def test_the_report_is_written_as_readable_json(self):
        report = self._export()
        self.assertEqual(report["host"], "https://example.test")
        self.assertEqual(report["scraped_count"], 3)

    def test_each_genre_carries_its_counts_and_percentage(self):
        rows = {row["genre"]: row for row in self._export()["categories"]}
        self.assertEqual(rows["Action"]["indexed"], 2)
        self.assertEqual(rows["Action"]["done"], 1)
        self.assertEqual(rows["Action"]["percent"], 50.0)

    def test_a_genre_carried_only_by_unindexed_series_is_omitted(self):
        """The columns answer "how much of this have I finished", so a genre
        with nothing in the index has no question to answer and no row."""
        data = genre_data(series={"not-in-index": ["action"]}, labels={"action": "Action"})
        report = self._export(data)
        self.assertEqual(report["categories"], [])
        self.assertEqual(report["series_indexed"], 0)

    def test_a_genre_nobody_has_finished_scores_zero_percent(self):
        data = genre_data(series={"partial": ["drama"]}, labels={"drama": "Drama"})
        rows = {row["genre"]: row for row in self._export(data)["categories"]}
        self.assertEqual(rows["Drama"]["done"], 0)
        self.assertEqual(rows["Drama"]["indexed"], 1)
        self.assertEqual(rows["Drama"]["percent"], 0.0)

    def test_a_partial_scrape_is_recorded_in_the_report(self):
        report = self._export(genre_data(scraped_count=1, catalogue_total=3))
        self.assertTrue(report["partial"])

    def test_each_series_carries_its_title_alongside_its_genres(self):
        """A consumer must not have to cross-reference a separate title map."""
        entry = self._export()["series"]["done-one"]
        self.assertEqual(entry["title"], "Done One")
        self.assertEqual(entry["genres"], ["action"])

    def test_a_series_with_no_known_title_falls_back_to_its_slug(self):
        data = genre_data(titles={})
        self.assertEqual(self._export(data)["series"]["done-one"]["title"], "done-one")

    def test_no_backup_file_is_left_beside_the_report(self):
        self._export()
        self._export()
        self.assertFalse(Path(f"{self.report_path}.bak1").exists())


class UnwatchedByGenreTests(_FlowTest):
    def test_no_data_yet_points_at_the_scrape_option(self):
        self.with_data(genre_data(series={}))
        out = self.run_capturing(genre_stats.list_unwatched_by_genre)
        self.assertIn("No genre data yet", out)

    def _run(self, answer, data=None):
        self.with_data(data or genre_data())
        with mock.patch("builtins.input", return_value=answer):
            return self.run_capturing(genre_stats.list_unwatched_by_genre)

    def test_choosing_back_lists_nothing(self):
        out = self._run("0")
        self.assertNotIn("Partial One", out)

    def test_all_lists_every_series_with_episodes_left(self):
        out = self._run("All genres / no filter")
        self.assertIn("Partial One", out)
        self.assertIn("(4/10)", out)

    def test_a_fully_watched_series_is_not_listed(self):
        self.assertNotIn("Done One", self._run("All genres / no filter"))

    def test_a_genre_filter_excludes_series_outside_it(self):
        """Partial One carries drama; Done One does not, and is watched anyway."""
        out = self._run("Drama")
        self.assertIn("Partial One", out)
        self.assertIn("in Drama", out)

    def test_a_genre_with_nothing_left_to_watch_says_so(self):
        data = genre_data(series={"done-one": ["action"], "partial": ["drama"]})
        out = self._run("Action", data)
        self.assertIn("No unwatched", out)
        self.assertIn("Action", out)


class FakeScraper:
    """Only what _targets reads: the active host and the two lookups."""

    site_url = "https://mirror.test"

    def __init__(self, ignored=()):
        self._ignored = set(ignored)

    def get_ignored_slugs(self):
        return self._ignored

    def get_series_slug_from_url(self, url):
        if not url:
            return "unknown"
        return url.rstrip("/").split("/")[-1] or "unknown"


class TargetsTests(unittest.TestCase):
    """Which catalogue entries a genre scrape will actually fetch."""

    def _entry(self, slug):
        return {"link": f"{SLUG_PREFIX}/{slug}", "url": f"https://primary.test{SLUG_PREFIX}/{slug}"}

    def test_each_entry_becomes_one_slug_and_url(self):
        targets = genre_stats._targets(FakeScraper(), [self._entry("one-piece")])
        self.assertEqual(targets, [("one-piece", f"https://mirror.test{SLUG_PREFIX}/one-piece")])

    def test_the_url_is_rebuilt_on_the_active_host_not_the_catalogue_one(self):
        """The catalogue hard-codes the primary host; a probed mirror wins."""
        ((_slug, url),) = genre_stats._targets(FakeScraper(), [self._entry("one-piece")])
        self.assertTrue(url.startswith("https://mirror.test"))
        self.assertNotIn("primary.test", url)

    def test_an_ignored_series_is_not_fetched(self):
        targets = genre_stats._targets(FakeScraper(ignored={"one-piece"}), [self._entry("one-piece")])
        self.assertEqual(targets, [])

    def test_a_duplicate_entry_is_fetched_once(self):
        entries = [self._entry("one-piece"), self._entry("one-piece")]
        self.assertEqual(len(genre_stats._targets(FakeScraper(), entries)), 1)

    def test_an_entry_with_no_usable_slug_is_dropped(self):
        self.assertEqual(genre_stats._targets(FakeScraper(), [{"link": "", "url": ""}]), [])

    def test_an_entry_with_only_a_url_falls_back_to_it(self):
        entry = {"url": f"https://primary.test{SLUG_PREFIX}/solo"}
        self.assertEqual(genre_stats._targets(FakeScraper(), [entry]), [("solo", entry["url"])])

    def test_order_is_preserved(self):
        entries = [self._entry("b"), self._entry("a"), self._entry("c")]
        self.assertEqual([s for s, _ in genre_stats._targets(FakeScraper(), entries)], ["b", "a", "c"])


class CheckTruncationTests(unittest.TestCase):
    """The tripwire for the CSS-hidden genre list."""

    def _warned(self, visible, raw_anchors, hidden):
        with mock.patch.object(genre_stats.logger, "warning") as warn:
            genre_stats._check_truncation("slug", visible, raw_anchors, hidden)
        return warn.called

    def test_matching_counts_do_not_warn(self):
        self.assertFalse(self._warned(visible=4, raw_anchors=6, hidden=2))

    def test_a_mismatch_warns(self):
        self.assertTrue(self._warned(visible=4, raw_anchors=9, hidden=2))

    def test_a_page_that_states_no_hidden_count_is_not_checked(self):
        self.assertFalse(self._warned(visible=4, raw_anchors=99, hidden=None))

    def test_a_page_with_no_anchors_at_all_is_not_checked(self):
        self.assertFalse(self._warned(visible=0, raw_anchors=0, hidden=2))

    def test_a_repeated_genre_does_not_trip_the_tripwire(self):
        """astra-lost-in-space really does list one genre twice."""
        self.assertFalse(self._warned(visible=5, raw_anchors=7, hidden=2))


class TableLinesTests(unittest.TestCase):
    def test_no_rows_renders_no_lines(self):
        self.assertEqual(genre_stats._table_lines([]), [])

    def test_every_row_is_rendered(self):
        lines = "\n".join(genre_stats._table_lines([("Action", 1, 2), ("Drama", 0, 3)]))
        self.assertIn("Action", lines)
        self.assertIn("Drama", lines)
        self.assertIn("1/2", lines)


class ChangeLineTests(unittest.TestCase):
    """Every kind of change gets a line, using labels rather than raw keys."""

    data = {"labels": {"action": "Action", "drama": "Drama"}, "titles": {"slug-a": "Series A"}}

    def _lines(self, **changes):
        base = {"new_series": [], "changed": [], "new_categories": [], "gone_categories": []}
        base.update(changes)
        return genre_stats._change_lines(self.data, base)

    def test_a_gained_genre_is_shown_with_a_plus(self):
        (line,) = self._lines(changed=[("slug-a", ["drama"], [])])
        self.assertIn("Series A", line)
        self.assertIn("+ Drama", line)
        self.assertNotIn("- ", line.split("+ Drama")[1])

    def test_a_lost_genre_is_shown_with_a_minus(self):
        (line,) = self._lines(changed=[("slug-a", [], ["action"])])
        self.assertIn("- Action", line)

    def test_gained_and_lost_appear_on_one_line(self):
        (line,) = self._lines(changed=[("slug-a", ["drama"], ["action"])])
        self.assertIn("+ Drama", line)
        self.assertIn("- Action", line)

    def test_a_new_category_is_reported(self):
        (line,) = self._lines(new_categories=["drama"])
        self.assertIn("new category: Drama", line)

    def test_a_vanished_category_is_reported(self):
        (line,) = self._lines(gone_categories=["action"])
        self.assertIn("category gone: Action", line)

    def test_an_unknown_key_falls_back_to_the_key_itself(self):
        (line,) = self._lines(new_categories=["mystery"])
        self.assertIn("mystery", line)


class PromptGenreChoiceTests(unittest.TestCase):
    """The non-tty fallback path, which is what a piped run and CI both take."""

    choices = {"all": "All genres / no filter", "action": "Action", "sci_fi": "Science Fiction"}

    def _choose(self, *answers, allow_back=True):
        with mock.patch("builtins.input", side_effect=list(answers)), redirect_stdout(io.StringIO()):
            return genre_stats._prompt_genre_choice(self.choices, allow_back=allow_back)

    def test_an_exact_label_selects_that_genre(self):
        self.assertEqual(self._choose("Action"), "action")

    def test_matching_is_case_insensitive(self):
        self.assertEqual(self._choose("aCtIoN"), "action")

    def test_a_substring_selects_the_matching_genre(self):
        self.assertEqual(self._choose("science"), "sci_fi")

    def test_zero_goes_back(self):
        self.assertEqual(self._choose("0"), "__back__")

    def test_the_word_back_also_goes_back(self):
        self.assertEqual(self._choose("back"), "__back__")

    def test_back_is_not_offered_when_it_is_not_allowed(self):
        """With allow_back off, "0" is not a shortcut and must not resolve."""
        self.assertEqual(self._choose("0", "Action", allow_back=False), "action")

    def test_unmatched_input_retries_rather_than_guessing(self):
        with mock.patch("builtins.input", side_effect=["zzz", "Action"]):
            out = io.StringIO()
            with redirect_stdout(out):
                selected = genre_stats._prompt_genre_choice(self.choices)
        self.assertEqual(selected, "action")
        self.assertIn("No genre matched", out.getvalue())

    def test_blank_input_retries(self):
        self.assertEqual(self._choose("", "Action"), "action")


class MenuTests(_FlowTest):
    """The loop itself: every option reaches its handler, and 0 leaves."""

    def setUp(self):
        super().setUp()
        self.with_data(genre_data())

    def _run_menu(self, *answers):
        with mock.patch("builtins.input", side_effect=list(answers)):
            return self.run_capturing(genre_stats.menu)

    def test_zero_returns_immediately(self):
        out = self._run_menu("0")
        self.assertIn("Watch Stats of Categories", out)

    def test_the_status_header_is_printed_each_pass(self):
        out = self._run_menu("0")
        self.assertIn("Genre data:", out)

    def test_an_invalid_choice_is_rejected_and_the_menu_repeats(self):
        out = self._run_menu("9", "0")
        self.assertIn("Invalid choice", out)

    def test_option_one_scrapes(self):
        with mock.patch.object(genre_stats, "scrape_genres") as fn:
            self._run_menu("1", "0")
        fn.assert_called_once()

    def test_option_two_shows_stats(self):
        with mock.patch.object(genre_stats, "show_stats") as fn:
            self._run_menu("2", "0")
        fn.assert_called_once()

    def test_option_three_exports(self):
        with mock.patch.object(genre_stats, "export_report") as fn:
            self._run_menu("3", "0")
        fn.assert_called_once()

    def test_option_four_lists_unwatched(self):
        with mock.patch.object(genre_stats, "list_unwatched_by_genre") as fn:
            self._run_menu("4", "0")
        fn.assert_called_once()

    def test_the_active_host_is_passed_through_to_the_handler(self):
        """The menu is reached after domain probing; the choice must survive it."""
        with (
            mock.patch("builtins.input", side_effect=["2", "0"]),
            mock.patch.object(genre_stats, "show_stats") as fn,
        ):
            self.run_capturing(genre_stats.menu, "https://chosen.test")
        fn.assert_called_once_with("https://chosen.test")

    def test_several_options_can_be_used_before_leaving(self):
        with (
            mock.patch.object(genre_stats, "show_stats") as show,
            mock.patch.object(genre_stats, "export_report") as export,
        ):
            self._run_menu("2", "3", "2", "0")
        self.assertEqual(show.call_count, 2)
        self.assertEqual(export.call_count, 1)


if __name__ == "__main__":
    unittest.main()
