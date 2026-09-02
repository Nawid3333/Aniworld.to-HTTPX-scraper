"""The menu-level guards in main.py: checkpoints, credentials, disk, slugs.

These decide whether a run starts at all and whether it resumes, so a wrong
answer here either destroys a checkpoint the user wanted or silently resumes
one they did not. They are also pure decision logic once ``input()`` is
scripted, which makes them cheap to cover -- they were simply never reached
by a test before.

Style note for future edits
---------------------------
``scripted_input`` answers prompts in order and returns ``default`` once it
runs out. Set ``default`` to the *safe* answer ("n") so a prompt added later
cannot make an old test silently start approving something.
"""

from __future__ import annotations

import json
import os
import re

import pytest

import main
from tests._support import captured_output, scripted_input, series


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Point main.py and the scraper class at a throwaway data directory."""
    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path))
    return tmp_path


def _write_checkpoint(directory, mode: str) -> str:
    path = os.path.join(str(directory), ".scrape_checkpoint.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"completed_links": ["/anime/stream/a"], "mode": mode}, fh)
    return path


class TestCheckpointPrompt:
    def test_no_checkpoint_starts_a_fresh_run_without_asking(self, data_dir):
        with captured_output(), scripted_input(default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}

    def test_matching_mode_can_be_resumed(self, data_dir):
        _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": True}

    def test_declining_the_resume_then_discarding_starts_fresh(self, data_dir):
        path = _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("n", "y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}
        assert not os.path.exists(path), "discarding must actually remove the checkpoint"

    def test_declining_both_cancels_rather_than_guessing(self, data_dir):
        """Neither resuming nor discarding is a real answer -- so do nothing."""
        path = _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("n", "n", default="n"):
            assert main._check_checkpoint("all") == {"ok": False, "resume": False}
        assert os.path.exists(path), "cancelling must leave the checkpoint alone"

    def test_a_checkpoint_from_another_mode_is_never_silently_resumed(self, data_dir):
        """Resuming a 'new only' checkpoint into a full scrape would skip real work."""
        _write_checkpoint(data_dir, "new_only")
        with captured_output() as out, scripted_input("n", default="n"):
            result = main._check_checkpoint("all")
        assert result == {"ok": False, "resume": False}
        assert "different mode" in out.getvalue()

    def test_a_mismatched_checkpoint_can_be_discarded_to_continue(self, data_dir):
        path = _write_checkpoint(data_dir, "new_only")
        with captured_output(), scripted_input("y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}
        assert not os.path.exists(path)

    def test_an_unremovable_checkpoint_does_not_crash_the_run(self, data_dir, monkeypatch):
        _write_checkpoint(data_dir, "all")

        def refuse(_path):
            raise OSError("locked")

        monkeypatch.setattr(main.os, "remove", refuse)
        with captured_output(), scripted_input("n", "y", default="n"):
            assert main._check_checkpoint("all")["ok"] is True


class TestCredentialValidation:
    def test_missing_credentials_are_refused_with_instructions(self, monkeypatch):
        monkeypatch.setattr(main, "EMAIL", "")
        monkeypatch.setattr(main, "PASSWORD", "")
        with captured_output() as out:
            assert main.validate_credentials() is False
        printed = out.getvalue()
        assert ".env" in printed, "the error must say where to put the credentials"

    def test_a_password_without_an_email_is_still_refused(self, monkeypatch):
        monkeypatch.setattr(main, "EMAIL", "")
        monkeypatch.setattr(main, "PASSWORD", "secret")
        with captured_output():
            assert main.validate_credentials() is False

    def test_both_present_passes(self, monkeypatch):
        monkeypatch.setattr(main, "EMAIL", "user@example.com")
        monkeypatch.setattr(main, "PASSWORD", "secret")
        assert main.validate_credentials() is True


class TestDiskSpaceCheck:
    def test_plenty_of_space_passes(self, monkeypatch):
        monkeypatch.setattr(main.shutil, "disk_usage", lambda _p: _Usage(free=5 * 1024**3))
        assert main.check_disk_space(min_mb=100) is True

    def test_low_space_is_reported_and_refused(self, monkeypatch):
        monkeypatch.setattr(main.shutil, "disk_usage", lambda _p: _Usage(free=10 * 1024**2))
        with captured_output() as out:
            assert main.check_disk_space(min_mb=100) is False
        assert "Low disk space" in out.getvalue()

    def test_an_unreadable_filesystem_does_not_block_the_run(self, monkeypatch):
        """A failed check is not evidence of a full disk, so it must not stop work."""

        def boom(_path):
            raise OSError("no such device")

        monkeypatch.setattr(main.shutil, "disk_usage", boom)
        assert main.check_disk_space() is True


class _Usage:
    """Minimal stand-in for shutil.disk_usage's named tuple."""

    def __init__(self, free: int):
        self.total = free * 2
        self.used = free
        self.free = free


class TestSlugExtraction:
    def test_link_is_preferred_over_url(self):
        entry = {"link": "https://x/anime/stream/from-link", "url": "https://x/anime/stream/from-url"}
        assert main._extract_slug(entry) == "from-link"

    def test_url_is_used_when_link_is_empty(self):
        """Only the returned slug is asserted.

        Whether the fallback is announced on stdout or only logged differs
        between the three repos, and pinning it here would make one sibling's
        cosmetic choice a failure in another. What must hold everywhere is
        that the entry still resolves rather than being treated as slugless.
        """
        entry = {"link": "", "url": "https://x/anime/stream/from-url", "title": "T"}
        with captured_output():
            assert main._extract_slug(entry) == "from-url"

    def test_an_entry_with_neither_yields_none(self):
        assert main._extract_slug({"title": "T"}) is None

    def test_a_non_dict_yields_none(self):
        assert main._extract_slug("not an entry") is None


class TestShowMenu:
    """The menu is derived from the output, not hardcoded.

    The three repos offer different numbers of options -- bs.to has no
    subscribed/watchlist scrape, for instance -- and the list grows over time.
    Reading the numbers back out of the printed menu means these tests keep
    working when an option is added, and still catch the two things that are
    always wrong: a gap in the numbering, and a missing exit.
    """

    @staticmethod
    def _options() -> set[int]:
        with captured_output() as out:
            main.show_menu()
        return {int(match) for match in re.findall(r"^\s*(\d+)\.", out.getvalue(), re.MULTILINE)}

    def test_the_menu_offers_options(self):
        assert self._options(), "show_menu printed nothing that looks like an option"

    def test_there_is_always_a_way_out(self):
        assert 0 in self._options(), "option 0 (exit) must always be offered"

    def test_the_numbering_has_no_gaps(self):
        """A gap means an option was removed and the rest never renumbered."""
        options = self._options()
        assert options == set(range(max(options) + 1)), f"menu numbering is not contiguous: {sorted(options)}"


class _FakeIndex:
    """Just enough IndexManager for the alert pass: it only reads series_index."""

    def __init__(self, entries):
        self.series_index = {e["title"]: e for e in entries}


def _alert_output(entries, *, answers=("n",), allow_rescrape=False):
    with scripted_input(*answers, default="n"), captured_output() as out:
        main.print_completed_series_alerts(_FakeIndex(entries), allow_rescrape=allow_rescrape)
    return out.getvalue()


def _flagged_under(output: str, heading: str) -> set[str]:
    """Titles listed under one alert heading, up to the block's closing rule."""
    if heading not in output:
        return set()
    tail = output.split(heading, 1)[1]
    body = tail.split("─" * 70)[1] if ("─" * 70) in tail else ""
    return {line.strip().lstrip("• ").strip() for line in body.splitlines() if line.strip().startswith("•")}


class TestSubscriptionAlerts:
    """Progress and subscription are the same state in this index.

    Any series with a watched episode is meant to be subscribed, and an
    unfinished one is meant to be on the watchlist ("waiting for more
    episodes"). These tests pin that rule so the alerts cannot be quietly
    narrowed again -- the started-but-unsubscribed case in particular used to
    be invisible whenever the series was on the watchlist.
    """

    COMPLETED = "COMPLETED"
    STARTED = "STARTED"
    ONGOING = "ONGOING"

    def test_finished_but_unsubscribed_is_flagged(self):
        entry = series("Done", seasons=1, watched=12, subscribed=False, watchlist=False)
        assert "Done" in _flagged_under(_alert_output([entry]), self.COMPLETED)

    def test_started_but_unsubscribed_is_flagged(self):
        entry = series("Halfway", seasons=1, watched=6, subscribed=False, watchlist=False)
        assert "Halfway" in _flagged_under(_alert_output([entry]), self.STARTED)

    def test_started_but_unsubscribed_is_flagged_even_when_watchlisted(self):
        """The regression: the watchlist used to mask a missing subscription."""
        entry = series("Halfway", seasons=1, watched=6, subscribed=False, watchlist=True)
        assert "Halfway" in _flagged_under(_alert_output([entry]), self.STARTED)

    def test_a_subscribed_series_raises_no_subscription_alert(self):
        entries = [
            series("Done", seasons=1, watched=12, subscribed=True, watchlist=False),
            series("Halfway", seasons=1, watched=6, subscribed=True, watchlist=True),
        ]
        output = _alert_output(entries)
        assert not _flagged_under(output, self.COMPLETED)
        assert not _flagged_under(output, self.STARTED)

    def test_an_unstarted_series_is_never_flagged(self):
        """Nothing watched means nothing to subscribe to yet."""
        entry = series("Untouched", seasons=1, watched=0, subscribed=False, watchlist=False)
        output = _alert_output([entry])
        assert not _flagged_under(output, self.COMPLETED)
        assert not _flagged_under(output, self.STARTED)
        assert not _flagged_under(output, self.ONGOING)

    def test_unfinished_and_unwatchlisted_is_flagged(self):
        entry = series("Halfway", seasons=1, watched=6, subscribed=True, watchlist=False)
        assert "Halfway" in _flagged_under(_alert_output([entry]), self.ONGOING)

    def test_both_problems_are_reported_separately(self):
        """Missing subscription and missing watchlist are different fixes."""
        entry = series("Halfway", seasons=1, watched=6, subscribed=False, watchlist=False)
        output = _alert_output([entry])
        assert "Halfway" in _flagged_under(output, self.STARTED)
        assert "Halfway" in _flagged_under(output, self.ONGOING)

    def test_a_finished_series_is_not_asked_for_a_watchlist_entry(self):
        """The watchlist means 'waiting for more episodes', so a finished one is fine."""
        entry = series("Done", seasons=1, watched=12, subscribed=True, watchlist=False)
        assert not _flagged_under(_alert_output([entry]), self.ONGOING)


class TestSubscriptionAlertRescrape:
    """The offer to rescrape must cover every unsubscribed series, and only those."""

    @staticmethod
    def _capture_rescrape(monkeypatch, entries, answers):
        calls = []
        monkeypatch.setattr(main, "_run_scrape_and_save", lambda **kw: calls.append(kw))
        with scripted_input(*answers, default="n"), captured_output():
            main.print_completed_series_alerts(_FakeIndex(entries), allow_rescrape=True)
        return calls

    @staticmethod
    def _entries():
        return [
            series("Done", seasons=1, watched=12, subscribed=False, watchlist=False),
            series("Halfway", seasons=1, watched=6, subscribed=False, watchlist=False),
            series("Fine", seasons=1, watched=12, subscribed=True, watchlist=False),
        ]

    def test_yes_rescrapes_finished_and_started_together(self, monkeypatch):
        calls = self._capture_rescrape(monkeypatch, self._entries(), ["y"])
        assert len(calls) == 1, "both unsubscribed lists should share one rescrape"
        urls = calls[0]["run_kwargs"]["url_list"]
        assert len(urls) == 2
        assert any("done" in u for u in urls) and any("halfway" in u for u in urls)

    def test_a_correctly_subscribed_series_is_not_rescraped(self, monkeypatch):
        calls = self._capture_rescrape(monkeypatch, self._entries(), ["y"])
        assert not any("fine" in u for u in calls[0]["run_kwargs"]["url_list"])

    def test_declining_rescrapes_nothing(self, monkeypatch):
        assert self._capture_rescrape(monkeypatch, self._entries(), ["n"]) == []

    def test_the_rescrape_cannot_recurse(self, monkeypatch):
        """The nested run must not offer the same prompt again."""
        calls = self._capture_rescrape(monkeypatch, self._entries(), ["y"])
        assert calls[0]["post_scrape_allow_rescrape"] is False

    def test_suppressed_mode_asks_nothing(self, monkeypatch):
        calls = []
        monkeypatch.setattr(main, "_run_scrape_and_save", lambda **kw: calls.append(kw))
        with scripted_input(default="y") as asked, captured_output():
            main.print_completed_series_alerts(_FakeIndex(self._entries()), allow_rescrape=False)
        assert asked == [] and calls == []
