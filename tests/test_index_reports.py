"""What the change report and the mismatch report actually print.

These paths were the largest untested block in index_manager. They are worth
covering because they are the last thing standing between a scrape and a
destructive answer: the user decides what to approve from these lines, so a
line that reports the wrong count, silently drops a series, or renders an
empty section is a bug with real consequences even though nothing crashes.

Style note for future edits
---------------------------
Assert on *facts* in the output -- a title being present, a count being
right -- rather than on exact strings. Pinning whole formatted lines makes
every cosmetic change a test failure, which trains people to update tests
without reading them. Where the exact shape genuinely matters (the S1E2
notation, the Sub/WL flags) it is asserted directly and the test says why.
"""

from __future__ import annotations

import pytest

from src.index_manager import (
    IndexManager,
    _find_series,
    _format_vanished_new_table,
    _get_season_stats,
    _match_vanished_to_new,
    _score_match,
    detect_changes,
    format_season_ep,
    group_episodes_by_season,
    show_changes,
)
from tests._support import SUPPORTS_SUBSCRIPTIONS, captured_output, scripted_input, season, series, write_index


@pytest.fixture
def two_series():
    """A small before/after pair used by most of the report tests."""
    old = {"Alpha": series("Alpha", seasons=[season(1, episodes=12, watched=6)])}
    new = {
        "Alpha": series("Alpha", seasons=[season(1, episodes=12, watched=9)]),
        "Beta": series("Beta", seasons=[season(1, episodes=6, watched=0)], subscribed=True, watchlist=False),
    }
    return old, new


# ── small helpers ───────────────────────────────────────────────────────────


class TestFindSeries:
    def test_finds_by_title_in_a_dict(self):
        data = {"Alpha": series("Alpha")}
        found = _find_series(data, "Alpha")
        assert found is not None
        assert found["title"] == "Alpha"

    def test_finds_by_title_in_a_list(self):
        data = [series("Alpha"), series("Beta")]
        found = _find_series(data, "Beta")
        assert found is not None
        assert found["title"] == "Beta"

    def test_returns_none_for_an_unknown_title(self):
        assert _find_series({"Alpha": series("Alpha")}, "Missing") is None

    def test_returns_none_for_an_unusable_container(self):
        """Callers pass whatever the scrape produced; None beats an exception."""
        for data in (None, "not a container", 42):
            assert _find_series(data, "Alpha") is None


class TestSeasonStats:
    def test_counts_one_named_season(self):
        entry = series("Alpha", seasons=[season(1, episodes=10, watched=4)])
        assert _get_season_stats(entry, "Season 1") == (10, 4)

    def test_an_unknown_season_is_zero_not_an_error(self):
        entry = series("Alpha", seasons=[season(1)])
        assert _get_season_stats(entry, "Season 9") == (0, 0)

    def test_a_missing_series_is_zero(self):
        assert _get_season_stats(None, "Season 1") == (0, 0)


class TestFormatSeasonEp:
    """The S1E2 notation, which the change lines are read in bulk."""

    @pytest.mark.parametrize(
        ("label", "expected"),
        [
            ("Season 1", "S1E2"),
            ("Staffel 3", "S3E2"),
            ("7", "S7E2"),
            ("Filme", "[Filme] Ep 2"),
        ],
    )
    def test_labels_render_predictably(self, label, expected):
        assert format_season_ep(label, 2) == expected


class TestGroupEpisodesBySeason:
    def test_episodes_of_one_season_collapse_to_a_single_line(self):
        entry = {"Alpha": series("Alpha", seasons=[season(1, episodes=12, watched=3)])}
        lines = group_episodes_by_season([("Alpha", "Season 1", n) for n in (1, 2, 3)], entry)
        assert len(lines) == 1, "three episodes of one season must not print three lines"
        assert "Alpha" in lines[0]

    def test_two_seasons_stay_on_separate_lines(self):
        entry = {"Alpha": series("Alpha", seasons=[season(1), season(2)])}
        lines = group_episodes_by_season([("Alpha", "Season 1", 1), ("Alpha", "Season 2", 1)], entry)
        assert len(lines) == 2

    def test_it_survives_a_series_missing_from_new_data(self):
        """A rename can leave a change referring to a title no longer present."""
        lines = group_episodes_by_season([("Ghost", "Season 1", 1)], {})
        assert lines and "Ghost" in lines[0]


# ── the change report ───────────────────────────────────────────────────────


class TestShowChanges:
    def test_no_changes_prints_nothing_and_reports_zero(self, two_series):
        old, _new = two_series
        with captured_output() as out:
            total = show_changes(detect_changes(old, old), new_data=old)
        assert total == 0
        assert out.getvalue().strip() == "", "an empty report must stay silent, not print an empty header"

    def test_a_new_series_is_named_with_its_progress(self, two_series):
        old, new = two_series
        changes = detect_changes(old, new)
        with captured_output() as out, scripted_input(default=""):
            show_changes(changes, new_data=new)
        printed = out.getvalue()
        assert "Beta" in printed
        assert "0/6" in printed, "a new series must show how much of it is already watched"

    @pytest.mark.skipif(not SUPPORTS_SUBSCRIPTIONS, reason="this site has no subscribe/watchlist feature")
    def test_subscription_flags_are_shown_for_a_new_series(self, two_series):
        old, new = two_series
        with captured_output() as out, scripted_input(default=""):
            show_changes(detect_changes(old, new), new_data=new)
        printed = out.getvalue()
        assert "Sub:" in printed and "WL:" in printed

    def test_newly_watched_episodes_are_reported(self, two_series):
        old, new = two_series
        changes = detect_changes(old, new)
        assert changes["newly_watched"], "fixture must actually contain newly watched episodes"
        with captured_output() as out, scripted_input(default=""):
            total = show_changes(changes, new_data=new)
        assert total > 0
        assert "Alpha" in out.getvalue()

    def test_excluding_a_category_removes_it_from_the_total(self, two_series):
        """The include_* flags gate what the user is asked to approve."""
        old, new = two_series
        changes = detect_changes(old, new)
        with captured_output(), scripted_input(default=""):
            with_watched = show_changes(changes, new_data=new)
            without_watched = show_changes(changes, include_watched=False, new_data=new)
        assert without_watched < with_watched

    def test_it_works_without_new_data(self, two_series):
        """new_data is optional; the report degrades to bare titles."""
        old, new = two_series
        with captured_output() as out, scripted_input(default=""):
            show_changes(detect_changes(old, new), new_data=None)
        assert "Beta" in out.getvalue()

    def test_a_long_list_paginates_and_can_be_skipped(self):
        """paginate_list stops early on 'q' -- proving the user can escape a huge report."""
        old = {}
        new = {f"Series {n:03d}": series(f"Series {n:03d}") for n in range(120)}
        with captured_output() as out, scripted_input("q", default="q"):
            show_changes(detect_changes(old, new), new_data=new)
        printed = out.getvalue()
        assert "skipped" in printed, "answering q must stop the listing"
        assert "Series 119" not in printed, "skipping must actually suppress the tail"


class TestReportOrderIsStable:
    """The report must read the same way twice for the same data.

    Both loops in detect_changes iterated raw set differences. Python
    randomises string hashing per process, so every category came out in a
    different order on every run -- and because the report is paginated, the
    first page, which is all a user sees before pressing q on a long list,
    was a random sample rather than the start of a stable list. Comparing two
    runs by eye was impossible.
    """

    def test_new_series_come_back_sorted(self):
        new = {f"Series {n:03d}": series(f"Series {n:03d}") for n in range(30)}
        listed = detect_changes({}, new)["new_series"]
        assert listed == sorted(listed)

    def test_sorting_is_case_insensitive(self):
        new = {t: series(t) for t in ("beta", "Alpha", "Gamma")}
        assert detect_changes({}, new)["new_series"] == ["Alpha", "beta", "Gamma"]

    def test_per_episode_categories_are_ordered_too(self):
        """The second loop feeds newly_watched, new_episodes and the rest."""
        titles = [f"Show {n:02d}" for n in range(20)]
        old = {t: series(t, seasons=[season(1, episodes=6, watched=0)]) for t in titles}
        new = {t: series(t, seasons=[season(1, episodes=6, watched=3)]) for t in titles}
        watched = detect_changes(old, new)["newly_watched"]
        assert [entry[0] for entry in watched] == sorted(entry[0] for entry in watched)

    def test_a_non_string_key_sorts_instead_of_raising(self):
        """A hand-edited index can contain anything; a scrape must not die on it."""
        new = {"Alpha": series("Alpha"), 7: series("Seven")}
        assert len(detect_changes({}, new)["new_series"]) == 2


# ── rename scoring ──────────────────────────────────────────────────────────


class TestScoreMatch:
    """Feeds the "is this new series a rename of that vanished one" hint.

    The threshold that consumes this is 0.75, chosen because unrelated shows
    were scoring 0.40-0.55. These tests pin both ends of that so a change to
    the scoring cannot quietly start pairing unrelated series.
    """

    def test_an_identical_title_scores_one(self):
        assert _score_match("One Piece", "", "One Piece", "") == 1.0

    def test_case_and_punctuation_do_not_matter(self):
        assert _score_match("one piece!", "", "One Piece", "") == 1.0

    def test_a_year_suffix_does_not_break_the_match(self):
        assert _score_match("Bleach (2004)", "", "Bleach", "") == 1.0

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            ("One Piece", "One Punch Man"),
            ("Death Note", "Deadman Wonderland"),
            ("Bleach", "Beelzebub"),
        ],
    )
    def test_unrelated_shows_stay_below_the_pairing_threshold(self, left, right):
        assert _score_match(left, "", right, "") < 0.75, f"{left} must not be offered as a rename of {right}"

    def test_an_empty_side_scores_zero(self):
        assert _score_match("", "", "Bleach", "") == 0.0

    def test_the_slug_can_carry_the_match_when_titles_differ(self):
        score = _score_match(
            "Frieren",
            "https://x/anime/stream/sousou-no-frieren",
            "Sousou no Frieren",
            "https://x/anime/stream/sousou-no-frieren",
        )
        assert score == 1.0

    def test_shared_german_articles_do_not_manufacture_a_match(self):
        """A real false-positive: both titles only share "der", a filler word.

        Before German stopwords were filtered, "der" counted as a genuine
        shared token and pushed this pair to 0.38 -- past the old 0.35 floor
        -- so a "Der kleine Prinz" rename table offered "Surviving Amazonas"
        as its counterpart. Neither title has anything else in common.
        """
        score = _score_match(
            "Der kleine Prinz (Netflix)",
            "https://x/anime/stream/der-kleine-prinz-netflix",
            "Surviving Amazonas - Die Prüfung der Shanenawa",
            "https://x/anime/stream/surviving-amazonas-die-prufung-der-shanenawa",
        )
        assert score < 0.75

    def test_match_vanished_to_new_rejects_scores_below_the_floor(self):
        """The pairing step must actually enforce 0.75, not just compute it.

        _score_match returning a low number is only half the fix -- this
        pins the consumer (_match_vanished_to_new) so a future edit can't
        silently lower its own floor back to something noise clears.
        """
        vanished = [("One Piece", "not found", "https://x/anime/stream/one-piece")]
        new_dict = {"One Punch Man": {"url": "https://x/anime/stream/one-punch-man"}}
        matched = _match_vanished_to_new(vanished, new_dict)
        v_title, v_url, n_title, n_url, reason = matched[0]
        assert n_title is None and reason is None

    @pytest.mark.parametrize(
        ("v_title", "n_title", "expected_reason"),
        [
            ("Wednesday", "Wednesday Addams", None),  # 0.720 -- just under the 0.75 floor
            ("Money Heist", "Money Heist: Korea", "weak"),  # 0.786
            ("Cobra Kai", "Cobra Kai 2", "strong"),  # 0.900
            ("Attack on Titan", "Attack on Titans", "exact"),  # 0.960
        ],
    )
    def test_match_vanished_to_new_tags_each_tier_correctly(self, v_title, n_title, expected_reason):
        """Pins the reason boundaries (0.75/0.85/0.95) against real titles.

        Each score above was checked against _score_match before being pinned
        here, so a future change to the tier cutoffs or to the scoring itself
        shows up as a wrong tag -- what the user actually sees in the table --
        not just as a silently different raw number.
        """
        vanished = [(v_title, "not found", "")]
        new_dict = {n_title: {"url": ""}}
        matched = _match_vanished_to_new(vanished, new_dict)
        assert matched[0][4] == expected_reason


class TestFormatVanishedNewTable:
    """The compact summary table that sits above the interactive prompt.

    A vanished entry with no candidate above the pairing floor is common now
    that the floor is 0.75 -- it must be reported as unmatched, never as a
    silently "paired" row with blank New-counterpart columns claiming things
    are complete when they are not.
    """

    def test_an_unmatched_vanished_entry_is_not_counted_as_paired(self):
        matched = [("One Piece", "https://x/anime/stream/one-piece", None, None, None)]
        lines, extra_lines = _format_vanished_new_table(matched)
        assert any("0/1" in line and "unmatched" in line for line in lines)
        assert not any("One Piece" in line for line in lines)
        assert extra_lines == []

    def test_the_status_line_survives_even_with_zero_pairs(self):
        """Previously this returned an empty table when nothing paired,
        silently dropping the "X unmatched" line the user needs to see."""
        matched = [("One Piece", "https://x/anime/stream/one-piece", None, None, None)]
        lines, _extra_lines = _format_vanished_new_table(matched)
        assert lines != []

    def test_a_genuine_pair_still_renders_the_full_table(self):
        matched = [("Frieren", "https://x/anime/stream/frieren", "Frieren", "https://x/anime/stream/frieren", "exact")]
        lines, _extra_lines = _format_vanished_new_table(matched)
        assert any("Frieren" in line for line in lines)
        assert any("1/1" in line and "complete" in line for line in lines)


@pytest.mark.skipif(not SUPPORTS_SUBSCRIPTIONS, reason="this site has no subscribe/watchlist state")
class TestWatchedCategoryMeansSubscribedAndComplete:
    """ "watched" deliberately requires `subscribed`, and that is not a bug.

    In this index a series with progress is meant to be subscribed, so
    "finished but unsubscribed" is drift, not a category -- main.py's
    print_completed_series_alerts is what surfaces it, with an offer to
    rescrape. Dropping the `subscribed` filter here would turn a state the
    program is supposed to shout about into a quietly correct-looking row,
    which is why this test exists.
    """

    @staticmethod
    def _categories(tmp_path, entries):
        path = write_index(entries, tmp_path)
        with captured_output():
            manager = IndexManager(path)
            report = manager.get_full_report()
        return {name: set(block.get("titles", [])) for name, block in report["categories"].items()}

    def test_finished_and_subscribed_counts_as_watched(self, tmp_path):
        entry = series("Done", seasons=[season(1, episodes=12, watched=12)], subscribed=True, watchlist=False)
        assert "Done" in self._categories(tmp_path, [entry])["watched"]

    def test_finished_but_unsubscribed_is_not_listed_as_watched(self, tmp_path):
        """It is drift; the CLI alert reports it instead of the report burying it."""
        entry = series("Done", seasons=[season(1, episodes=12, watched=12)], subscribed=False, watchlist=False)
        assert "Done" not in self._categories(tmp_path, [entry])["watched"]

    def test_a_watchlisted_series_is_waiting_not_watched(self, tmp_path):
        """The watchlist means 'waiting for more episodes'."""
        entry = series("Waiting", seasons=[season(1, episodes=12, watched=12)], subscribed=False, watchlist=True)
        categories = self._categories(tmp_path, [entry])
        assert "Waiting" in categories["waiting_for_new_episodes"]
        assert "Waiting" not in categories["watched"]
