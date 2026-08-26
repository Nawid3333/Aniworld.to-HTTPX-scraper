"""New content is gated in layers: existence first, then state.

An episode or series the index has never seen has no prior state to diff
against, so the scraper used to adopt whatever the site reported for it. An
episode that aired and was watched between two scrapes appeared only under
[NEW EPISODES] and was silently marked watched, never passing the
unwatched->watched confirmation every other watch change must pass. The same
held for a series already subscribed or on the watchlist when first scraped.

The approval order is now:

    1. [NEW SERIES]    -- may this series enter the index at all?
    2. [NEW EPISODES]  -- may these episodes enter the index at all?
    3. [NEWLY WATCHED] -- may the watched flags they arrived with be kept?
    4. subscription / watchlist, then the final save

Refusing an existence gate cascades: there is nothing left to decide about
content that is not being stored, so it drops out of the later prompts.
Refusing a state gate keeps the content but resets that state to the
expected default.

Run with:  python -m unittest discover -s tests
"""

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.index_manager import (  # noqa: E402
    _build_merged_data,
    _cascade_declined_new_content,
    detect_changes,
)

DENY = {
    "new_series": False,
    "new_episodes": False,
    "watched": False,
    "unwatched": False,
    "subscribe": False,
    "unsubscribe": False,
    "watchlist_add": False,
    "watchlist_remove": False,
    "title_ger": False,
    "title_eng": False,
    "episode_remove": False,
    "season_remove": False,
}
ADD_ONLY = {**DENY, "new_series": True, "new_episodes": True}
ADD_AND_WATCH = {**ADD_ONLY, "watched": True}
ALLOW_ALL = {**ADD_AND_WATCH, "subscribe": True, "watchlist_add": True}


def merge(old, new, allowed):
    """Merge fresh copies.

    ``_build_merged_data`` writes resolved watch flags back into the entries
    it is handed, so a test that merges the same fixture twice would feed the
    second call data the first call already rewrote.
    """
    return _build_merged_data(copy.deepcopy(old), copy.deepcopy(new), allowed)


def series(episodes, *, subscribed=False, watchlist=False, title="Show"):
    """Build one series entry with a single season."""
    slug = title.lower().replace(" ", "")
    return {
        "title": title,
        "url": f"https://example.invalid/anime/stream/{slug}",
        "link": f"https://example.invalid/anime/stream/{slug}",
        "subscribed": subscribed,
        "watchlist": watchlist,
        "seasons": [
            {
                "season": "Staffel 1",
                "url": f"https://example.invalid/anime/stream/{slug}/staffel-1",
                "episodes": [{"number": n, "watched": w} for n, w in episodes],
            }
        ],
    }


def watched_flags(merged, title="Show"):
    """Return {episode_number: watched} for the merged entry's season."""
    return {ep["number"]: ep["watched"] for ep in merged[title]["seasons"][0]["episodes"]}


class TestDetectionReachesThePrompts(unittest.TestCase):
    """Whatever arrives in a non-default state must reach an approval gate."""

    def test_new_episode_arriving_watched_is_reported_twice(self):
        old = {"Show": series([(1, True), (2, True)])}
        new = {"Show": series([(1, True), (2, True), (3, True)])}
        changes = detect_changes(old, new)
        self.assertEqual(changes["new_episodes"], [("Show", "Staffel 1", 3)])
        self.assertEqual(
            changes["newly_watched"],
            [("Show", "Staffel 1", 3)],
            "must reach the watched gate, not just [NEW EPISODES]",
        )

    def test_new_unwatched_episode_raises_no_watch_prompt(self):
        old = {"Show": series([(1, True)])}
        new = {"Show": series([(1, True), (2, False)])}
        changes = detect_changes(old, new)
        self.assertEqual(changes["new_episodes"], [("Show", "Staffel 1", 2)])
        self.assertEqual(changes["newly_watched"], [])

    def test_new_series_watched_episodes_reach_the_watch_gate(self):
        new = {"Show": series([(1, True), (2, True), (3, False)])}
        changes = detect_changes({}, new)
        self.assertEqual(changes["new_series"], ["Show"])
        self.assertEqual(
            changes["newly_watched"],
            [("Show", "Staffel 1", 1), ("Show", "Staffel 1", 2)],
        )

    def test_new_series_flags_reach_their_gates(self):
        new = {"Show": series([(1, False)], subscribed=True, watchlist=True)}
        changes = detect_changes({}, new)
        self.assertEqual(changes["newly_subscribed"], ["Show"])
        self.assertEqual(changes["watchlist_added"], ["Show"])

    def test_a_plain_new_series_raises_no_state_prompts(self):
        changes = detect_changes({}, {"Show": series([(1, False), (2, False)])})
        self.assertEqual(changes["new_series"], ["Show"])
        self.assertEqual(changes["newly_watched"], [])
        self.assertEqual(changes["newly_subscribed"], [])
        self.assertEqual(changes["watchlist_added"], [])


class TestCascade(unittest.TestCase):
    """Refusing an existence gate empties the state prompts that follow."""

    def setUp(self):
        self.changes = detect_changes(
            {"Known": series([(1, True)], title="Known")},
            {
                "Known": series([(1, True), (2, True)], title="Known"),
                "Fresh": series([(1, True)], subscribed=True, watchlist=True, title="Fresh"),
            },
        )

    def test_declining_new_series_removes_its_state_changes(self):
        _cascade_declined_new_content(self.changes, {**ADD_ONLY, "new_series": False})
        self.assertNotIn("Fresh", [x[0] for x in self.changes["newly_watched"]])
        self.assertEqual(self.changes["newly_subscribed"], [])
        self.assertEqual(self.changes["watchlist_added"], [])

    def test_declining_new_series_leaves_known_series_alone(self):
        _cascade_declined_new_content(self.changes, {**ADD_ONLY, "new_series": False})
        self.assertIn(("Known", "Staffel 1", 2), self.changes["newly_watched"])

    def test_declining_new_episodes_removes_those_watch_changes(self):
        _cascade_declined_new_content(self.changes, {**ADD_ONLY, "new_episodes": False})
        self.assertNotIn(("Known", "Staffel 1", 2), self.changes["newly_watched"])

    def test_declining_both_empties_the_watch_prompt(self):
        _cascade_declined_new_content(self.changes, DENY)
        self.assertEqual(self.changes["newly_watched"], [])

    def test_approving_both_changes_nothing(self):
        before = copy.deepcopy(self.changes)
        _cascade_declined_new_content(self.changes, ALLOW_ALL)
        self.assertEqual(self.changes, before)


class TestNewSeriesExistenceGate(unittest.TestCase):
    """Gate 1: may this series enter the index at all?"""

    def setUp(self):
        self.new = {"Show": series([(1, True), (2, True), (3, False)])}

    def test_declined_series_is_not_written(self):
        self.assertEqual(merge({}, self.new, DENY), {})

    def test_declined_series_is_offered_again_next_scrape(self):
        index = merge({}, self.new, DENY)
        changes = detect_changes(index, copy.deepcopy(self.new))
        self.assertEqual(changes["new_series"], ["Show"], "still unknown, so still new")

    def test_approved_series_is_written_with_every_episode(self):
        merged = merge({}, self.new, ADD_ONLY)
        self.assertIn("Show", merged)
        self.assertEqual(sorted(watched_flags(merged)), [1, 2, 3], "all episodes are stored")

    def test_approved_series_with_declined_watch_state_starts_unwatched(self):
        merged = merge({}, self.new, ADD_ONLY)
        self.assertEqual(watched_flags(merged), {1: False, 2: False, 3: False})
        self.assertEqual(merged["Show"]["watched_episodes"], 0)
        self.assertEqual(merged["Show"]["total_episodes"], 3)

    def test_approved_series_keeps_watch_state_when_that_is_approved_too(self):
        merged = merge({}, self.new, ADD_AND_WATCH)
        self.assertEqual(watched_flags(merged), {1: True, 2: True, 3: False})
        self.assertEqual(merged["Show"]["watched_episodes"], 2)

    def test_season_counters_follow_the_declined_watch_state(self):
        season = merge({}, self.new, ADD_ONLY)["Show"]["seasons"][0]
        self.assertEqual(season["total_episodes"], 3)
        self.assertEqual(season["watched_episodes"], 0)

    def test_declined_watch_state_is_offered_again_next_scrape(self):
        index = merge({}, self.new, ADD_ONLY)
        changes = detect_changes(index, copy.deepcopy(self.new))
        self.assertEqual(changes["new_series"], [], "the series is known now")
        self.assertEqual(
            changes["newly_watched"],
            [("Show", "Staffel 1", 1), ("Show", "Staffel 1", 2)],
        )


class TestNewEpisodeExistenceGate(unittest.TestCase):
    """Gate 2: may these episodes enter an already-known series?"""

    def setUp(self):
        self.old = {"Show": series([(1, True), (2, True)])}
        self.new = {"Show": series([(1, True), (2, True), (3, True), (4, False)])}

    def test_declined_episodes_are_not_written(self):
        self.assertEqual(watched_flags(merge(self.old, self.new, DENY)), {1: True, 2: True})

    def test_declining_episodes_never_touches_existing_history(self):
        merged = merge(self.old, self.new, DENY)
        self.assertEqual(merged["Show"]["watched_episodes"], 2)

    def test_declined_episodes_are_offered_again_next_scrape(self):
        index = merge(self.old, self.new, DENY)
        changes = detect_changes(index, copy.deepcopy(self.new))
        self.assertEqual(
            sorted(changes["new_episodes"]),
            [("Show", "Staffel 1", 3), ("Show", "Staffel 1", 4)],
        )

    def test_approved_episodes_all_enter_the_index(self):
        merged = merge(self.old, self.new, ADD_ONLY)
        self.assertEqual(sorted(watched_flags(merged)), [1, 2, 3, 4], "both new episodes stored")

    def test_approved_episodes_with_declined_watch_state_enter_unwatched(self):
        merged = merge(self.old, self.new, ADD_ONLY)
        self.assertEqual(watched_flags(merged), {1: True, 2: True, 3: False, 4: False})

    def test_approved_episodes_keep_watch_state_when_approved(self):
        merged = merge(self.old, self.new, ADD_AND_WATCH)
        self.assertEqual(watched_flags(merged), {1: True, 2: True, 3: True, 4: False})

    def test_season_counters_follow_the_declined_watch_state(self):
        season = merge(self.old, self.new, ADD_ONLY)["Show"]["seasons"][0]
        self.assertEqual(season["total_episodes"], 4)
        self.assertEqual(season["watched_episodes"], 2)


class TestSubscriptionAndWatchlistOnNewSeries(unittest.TestCase):
    """A new series that is already subscribed / listed."""

    def setUp(self):
        self.new = {"Show": series([(1, False)], subscribed=True, watchlist=True)}

    def test_flags_reset_when_declined_but_series_is_kept(self):
        merged = merge({}, self.new, ADD_ONLY)
        self.assertIn("Show", merged, "approving the series gate still adds it")
        self.assertFalse(merged["Show"]["subscribed"])
        self.assertFalse(merged["Show"]["watchlist"])

    def test_flags_kept_when_approved(self):
        merged = merge({}, self.new, ALLOW_ALL)
        self.assertTrue(merged["Show"]["subscribed"])
        self.assertTrue(merged["Show"]["watchlist"])

    def test_the_two_flags_are_independent(self):
        merged = merge({}, self.new, {**ADD_ONLY, "subscribe": True})
        self.assertTrue(merged["Show"]["subscribed"])
        self.assertFalse(merged["Show"]["watchlist"])

    def test_declined_flags_are_offered_again_next_scrape(self):
        index = merge({}, self.new, ADD_ONLY)
        changes = detect_changes(index, copy.deepcopy(self.new))
        self.assertEqual(changes["newly_subscribed"], ["Show"])
        self.assertEqual(changes["watchlist_added"], ["Show"])


class TestNothingIsLost(unittest.TestCase):
    """Refusing anything costs one run and never corrupts the index."""

    def test_repeated_refusals_keep_offering_the_series(self):
        site = {"Show": series([(1, True), (2, False)], subscribed=True)}
        index = {}
        for _ in range(3):
            index = merge(index, site, DENY)
            self.assertEqual(index, {})
            self.assertEqual(detect_changes(index, copy.deepcopy(site))["new_series"], ["Show"])
        final = merge(index, site, ALLOW_ALL)
        self.assertEqual(watched_flags(final), {1: True, 2: False})
        self.assertTrue(final["Show"]["subscribed"])

    def test_existing_watch_history_survives_every_refusal(self):
        old = {"Show": series([(1, True), (2, True)])}
        site = {"Show": series([(1, True), (2, True), (3, True)])}
        index = old
        for _ in range(3):
            index = merge(index, site, DENY)
            self.assertEqual(watched_flags(index), {1: True, 2: True})

    def test_a_failed_scrape_never_wipes_a_known_series(self):
        old = {"Show": series([(1, True), (2, True)])}
        empty = {"Show": {**series([]), "seasons": []}}
        merged = merge(old, empty, ALLOW_ALL)
        self.assertEqual(watched_flags(merged), {1: True, 2: True})


class TestOrdinaryTransitionsUnchanged(unittest.TestCase):
    """The behaviour this work must not disturb."""

    def test_watched_flip_on_a_known_episode_still_gated(self):
        old = {"Show": series([(1, False)])}
        new = {"Show": series([(1, True)])}
        self.assertEqual(detect_changes(old, new)["newly_watched"], [("Show", "Staffel 1", 1)])
        self.assertEqual(watched_flags(merge(old, new, DENY)), {1: False})
        self.assertEqual(watched_flags(merge(old, new, ADD_AND_WATCH)), {1: True})

    def test_unwatched_flip_on_a_known_episode_still_gated(self):
        old = {"Show": series([(1, True)])}
        new = {"Show": series([(1, False)])}
        self.assertEqual(detect_changes(old, new)["newly_unwatched"], [("Show", "Staffel 1", 1)])
        self.assertEqual(watched_flags(merge(old, new, DENY)), {1: True})
        self.assertEqual(watched_flags(merge(old, new, {**DENY, "unwatched": True})), {1: False})

    def test_subscription_flip_on_a_known_series_still_gated(self):
        old = {"Show": series([(1, False)], subscribed=False)}
        new = {"Show": series([(1, False)], subscribed=True)}
        self.assertEqual(detect_changes(old, new)["newly_subscribed"], ["Show"])
        self.assertFalse(merge(old, new, DENY)["Show"]["subscribed"])
        self.assertTrue(merge(old, new, ALLOW_ALL)["Show"]["subscribed"])


if __name__ == "__main__":
    unittest.main()
