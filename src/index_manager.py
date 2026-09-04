"""
AniWorld.to Index Manager (HTTPX version)
Manages the persistent series index and handles data merging, change detection, and analytics.
Adapted from aniworld.to backup's index_manager3.py for the httpx scraper.
"""

import asyncio
import copy
import difflib
import json
import logging
import os
import re
import shutil
import webbrowser
from collections import defaultdict
from datetime import datetime
from typing import Any, TypeVar
from urllib.parse import urlparse

from config.config import SITE_URL, VALID_SERIES_HOSTS
from src.atomic_io import atomic_write_json, create_file_backup
from src.slug import slug_key

logger = logging.getLogger(__name__)


# Pre-compiled regex for season number extraction
_SEASON_NUMBER_RE = re.compile(r"(staffel|season|s)\s*(\d+)", re.IGNORECASE)

# Pre-compiled regex for valid aniworld.to series path
# Unanchored so URLs with trailing /staffel-N or /episode-N segments are accepted.
_VALID_SERIES_PATH_RE = re.compile(r"/anime/stream/[^/]+")


def _is_valid_series_url(url):
    """Check if a URL is a valid aniworld.to series URL or relative path.

    Rejects dangerous schemes (javascript:, data:, file://) and
    allows only configured aniworld.to-compatible hosts and /anime/stream/... paths.
    """
    if not url or not isinstance(url, str):
        return False

    if _VALID_SERIES_PATH_RE.match(url):
        return True

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    if parsed.scheme not in ("http", "https"):
        return False
    # If VALID_SERIES_HOSTS is empty (config not loaded yet), accept any host
    if VALID_SERIES_HOSTS and parsed.netloc not in VALID_SERIES_HOSTS:
        return False
    return bool(_VALID_SERIES_PATH_RE.match(parsed.path))


def _series_path_of(url):
    """Return the series path of *url*, or None if it is not one at all.

    Splits the two questions the old single check ran together: "is this a
    series URL we are willing to store" and "is it on a host we currently
    talk to". Dangerous schemes (javascript:, data:, file://) are still
    rejected here -- only the host is left for _rehost_series_url to decide,
    because a stale host is a fixable detail, not grounds to throw the entry
    away.
    """
    if not url or not isinstance(url, str):
        return None
    if _VALID_SERIES_PATH_RE.match(url):
        return url
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    return parsed.path if _VALID_SERIES_PATH_RE.match(parsed.path) else None


def _rehost_series_url(url):
    """Point a series URL at the configured host, keeping its path.

    Index entries store whatever mirror was live when they were scraped.
    Retiring that mirror from _SITE_URLS used to make every entry on it fail
    validation; load_index dropped those entries, and the very next save
    wrote the shortened index back to disk -- silently deleting the series
    and every watched episode recorded against it. Since the whole index is
    normally on one host, one config edit could take all of it.

    The path is what identifies the series, so only the host is replaced.
    Rewriting to the configured primary is enough: if the run is actually
    using a different mirror, the merge stores the URL it really scraped.

    Returns (url, changed).
    """
    path = _series_path_of(url)
    if path is None or _is_valid_series_url(url):
        return url, False
    return f"{SITE_URL}{path}", True


# Re-exported under their old private names so this module's call sites
# keep working unchanged. The real implementation lives in atomic_io so
# scraper.py can share it too.
_create_file_backup = create_file_backup
_atomic_write_json = atomic_write_json


def _validate_series_entry(series, title=""):
    """Validate that a series entry has the required structure. Returns True if valid."""
    if not isinstance(series, dict):
        logger.warning("Skipping invalid series entry (not dict): %s", title)
        return False
    url = series.get("url", "")
    if not url:
        logger.warning("Skipping series '%s' - missing 'url' field", title)
        return False
    if _series_path_of(url) is None:
        logger.warning("Skipping series '%s' - invalid URL scheme/format: %s", title, url[:80])
        return False
    seasons = series.get("seasons")
    if seasons is not None and not isinstance(seasons, list):
        logger.warning(
            "Skipping series '%s' - 'seasons' must be list, got %s",
            title,
            type(seasons),
        )
        return False
    for season in seasons or []:
        if not isinstance(season, dict):
            continue
        episodes = season.get("episodes")
        if episodes is not None and not isinstance(episodes, list):
            logger.error(
                "Rejecting series '%s' — season '%s' has CORRUPT episodes (type=%s, expected list)",
                title,
                season.get("season", "?"),
                type(episodes).__name__,
            )
            return False
    return True


def _find_series(new_data, title):
    """Look up a series by title in either a dict or list."""
    if isinstance(new_data, dict):
        return new_data.get(title)
    if isinstance(new_data, list):
        return next((s for s in new_data if s.get("title") == title), None)
    return None


def _get_season_stats(series, season_label):
    """Get (total_episodes, watched_episodes) for a specific season."""
    if not series:
        return 0, 0
    for s in series.get("seasons", []):
        if s.get("season") == season_label:
            eps = s.get("episodes", [])
            return len(eps), sum(1 for ep in eps if ep.get("watched", False))
    return 0, 0


def get_episode_counts(series):
    """Get (total_episodes, watched_episodes) across all seasons of a series."""
    total = 0
    watched = 0
    for season in series.get("seasons", []):
        eps = season.get("episodes", [])
        if season.get("ignored_episode_0"):
            eps = [ep for ep in eps if ep.get("number") != 0]
        total += len(eps)
        watched += sum(1 for ep in eps if ep.get("watched", False))
    return total, watched


def sync_season_counts(season):
    """Recompute one season's stored watched/total counters from its episodes.

    The ``episodes`` list is the authoritative record -- it is rebuilt from
    the site on every scrape. ``watched_episodes``/``total_episodes`` are
    derived from it, so they go stale the moment a merge adds or drops an
    episode without refreshing them; the season then reports the counts it
    had on some earlier run while its episode list shows the current truth.
    Every writer that touches a season's episodes must call this.

    Returns (total, watched).
    """
    eps = season.get("episodes", [])
    if season.get("ignored_episode_0"):
        eps = [ep for ep in eps if ep.get("number") != 0]
    total = len(eps)
    watched = sum(1 for ep in eps if ep.get("watched", False))
    season["total_episodes"] = total
    season["watched_episodes"] = watched
    return total, watched


def paginate_list(items, formatter, page_size=50):
    """Show items with pagination, Enter = next page, q = skip"""
    if not items:
        return
    total = len(items)
    idx = 0
    while idx < total:
        end = min(idx + page_size, total)
        for item in items[idx:end]:
            print(formatter(item))
        idx = end
        if idx < total:
            choice = input(f"  ({idx}/{total}) Enter = more, q = skip: ").strip().lower()
            if choice == "q":
                print(f"  ... skipped {total - idx} remaining")
                break


def format_season_ep(season_label, ep_num):
    """Format season/episode for display."""
    match = _SEASON_NUMBER_RE.search(str(season_label))
    if match:
        return f"S{match.group(2)}E{ep_num}"
    if str(season_label).strip().isdigit():
        return f"S{season_label}E{ep_num}"
    return f"[{season_label}] Ep {ep_num}"


def group_episodes_by_season(episode_list, new_data, prefix="[+]"):
    """Group episodes by series and season, showing count per season."""
    grouped = defaultdict(list)
    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)

    if isinstance(new_data, list):
        new_data_dict = {s.get("title"): s for s in new_data}
    elif isinstance(new_data, dict):
        new_data_dict = new_data
    else:
        new_data_dict = {}

    result = []
    for (title, season), ep_nums in sorted(grouped.items()):
        series = new_data_dict.get(title, {})
        total_in_season, _ = _get_season_stats(series, season)
        if total_in_season > 0:
            result.append(f"  {prefix} {title} [{season}]: {len(ep_nums)}/{total_in_season} episodes")
        else:
            for ep_num in sorted(ep_nums):
                result.append(f"  {prefix} {title} {format_season_ep(season, ep_num)}")
    return result


def _detect_housekeeping_changes(old_data, new_dict):
    """Predict ep0 removals and ignored flag changes that the merge will apply.

    Must be called BEFORE _build_merged_data because the merge mutates
    old_data season objects in place (shared references).

    Returns dict with 'added' and 'removed' lists of (title, [seasons]).
    'added'  = seasons where ep0 will be removed and ignored flag set.
    'removed' = seasons where the ignored flag will be cleared.
    """
    if isinstance(old_data, list):
        old_map = {s.get("title"): s for s in old_data if s and s.get("title")}
    else:
        old_map = dict(old_data) if old_data else {}

    added = {}  # title -> [season_labels]
    removed = {}  # title -> [season_labels]
    for title, new_entry in new_dict.items():
        o_entry = old_map.get(title)
        if not o_entry:
            continue
        old_seasons = {s.get("season"): s for s in o_entry.get("seasons", [])}
        for new_season in new_entry.get("seasons", []):
            label = new_season.get("season")
            o_season = old_seasons.get(label)
            if not o_season:
                continue
            old_has_ep0 = any(ep.get("number") == 0 for ep in o_season.get("episodes", []))
            old_flag = o_season.get("ignored_episode_0", False)
            new_flag = new_season.get("ignored_episode_0", False)
            if (old_has_ep0 and new_flag) or (not old_flag and new_flag):
                added.setdefault(title, []).append(str(label))
            elif old_flag and not new_flag:
                removed.setdefault(title, []).append(str(label))
    return {"added": added, "removed": removed}


def _flag_new_series_non_default_state(title, new_series, changes):
    """Report the non-default state a brand-new series arrives with.

    A series the index has never seen is expected to be unwatched,
    unsubscribed and off the watchlist. When the site already reports
    otherwise, that state is surfaced in the same categories an existing
    series would use, so it reaches the approval prompts instead of being
    adopted silently under [NEW SERIES].
    """
    if not new_series or not isinstance(new_series, dict):
        return
    for season in new_series.get("seasons", []):
        if not season or not isinstance(season, dict):
            continue
        s_label = season.get("season", "")
        for ep in season.get("episodes", []):
            if not ep or not isinstance(ep, dict):
                continue
            ep_num = ep.get("number")
            if ep_num is not None and ep.get("watched", False):
                changes["newly_watched"].append((title, s_label, ep_num))
    if new_series.get("subscribed", False):
        changes["newly_subscribed"].append(title)
    if new_series.get("watchlist", False):
        changes["watchlist_added"].append(title)


def _report_order(title):
    """Sort key for anything the change report lists, case-insensitively.

    Falls back to str() so a non-string key in a hand-edited index sorts
    rather than raising TypeError halfway through a scrape.
    """
    text = str(title)
    return (text.lower(), text)


def detect_changes(old_data, new_data):
    """Detect changes between old and new data."""
    changes = {
        "new_series": [],
        "new_episodes": [],
        "newly_watched": [],
        "newly_unwatched": [],
        "newly_subscribed": [],
        "newly_unsubscribed": [],
        "watchlist_added": [],
        "watchlist_removed": [],
        "title_ger_changed": [],
        "title_eng_changed": [],
        "removed_episodes": [],
        "removed_seasons": [],
    }

    old_titles = (
        set(old_data.keys()) if isinstance(old_data, dict) else {s.get("title") for s in old_data if s.get("title")}
    )
    new_titles = (
        set(new_data.keys()) if isinstance(new_data, dict) else {s.get("title") for s in new_data if s.get("title")}
    )

    if isinstance(old_data, list):
        old_data = {s.get("title"): s for s in old_data}
    if isinstance(new_data, list):
        new_data = {s.get("title"): s for s in new_data}

    # Sorted, not raw set order. Python randomises string hashing per
    # process, so the same scrape listed its changes in a different order
    # every run -- and with the report paginated, the first page the user
    # actually reads before pressing q was a random sample of the set
    # rather than a stable, comparable list.
    for title in sorted(new_titles - old_titles, key=_report_order):
        changes["new_series"].append(title)
        _flag_new_series_non_default_state(title, new_data.get(title, {}), changes)

    for title in sorted(old_titles & new_titles, key=_report_order):
        old_series = old_data[title]
        new_series = new_data[title]

        old_sub = old_series.get("subscribed", False)
        new_sub = new_series.get("subscribed", False)
        if old_sub != new_sub:
            if new_sub:
                changes["newly_subscribed"].append(title)
            else:
                changes["newly_unsubscribed"].append(title)

        old_wl = old_series.get("watchlist", False)
        new_wl = new_series.get("watchlist", False)
        if old_wl != new_wl:
            if new_wl:
                changes["watchlist_added"].append(title)
            else:
                changes["watchlist_removed"].append(title)

        old_ger = old_series.get("title_ger", "")
        new_ger = new_series.get("title_ger", "")
        if old_ger and new_ger and old_ger != new_ger:
            changes["title_ger_changed"].append((title, old_ger, new_ger))

        old_eng = old_series.get("title_eng", "")
        new_eng = new_series.get("title_eng", "")
        if old_eng and new_eng and old_eng != new_eng:
            changes["title_eng_changed"].append((title, old_eng, new_eng))

        old_eps = {}
        for season in old_series.get("seasons", []):
            s_label = season.get("season", "")
            for ep in season.get("episodes", []):
                old_eps[(s_label, str(ep.get("number")))] = ep.get("watched", False)

        new_eps: set[tuple[str, str]] = set()
        for season in new_series.get("seasons", []):
            s_label = season.get("season", "")
            for ep in season.get("episodes", []):
                ep_num = ep.get("number")
                ep_key = (s_label, str(ep_num))
                new_eps.add(ep_key)
                new_watched = ep.get("watched", False)

                if ep_key not in old_eps:
                    changes["new_episodes"].append((title, s_label, ep_num))
                    if new_watched:
                        # No prior state existed to diff against, but the site
                        # already reports this brand-new episode as watched.
                        # Surface it as a watch change too, so it reaches the
                        # watched prompt instead of being adopted silently.
                        changes["newly_watched"].append((title, s_label, ep_num))
                elif old_eps[ep_key] != new_watched:
                    if not old_eps[ep_key] and new_watched:
                        changes["newly_watched"].append((title, s_label, ep_num))
                    elif old_eps[ep_key] and not new_watched:
                        changes["newly_unwatched"].append((title, s_label, ep_num))

        # Things the index has that this scrape did not return. Deleting data
        # is never done on the scraper's say-so -- a truncated response or a
        # soft error page looks exactly like a removal here -- so these are
        # only reported, and only applied if the user approves them.
        old_labels = {season.get("season", "") for season in old_series.get("seasons", [])}
        new_labels = {season.get("season", "") for season in new_series.get("seasons", [])}
        for s_label in sorted(old_labels - new_labels):
            changes["removed_seasons"].append((title, s_label))
        for s_label, ep_num in sorted(set(old_eps) - new_eps):
            # A season that vanished entirely is reported as one season
            # removal, not as N separate episode removals.
            if s_label in new_labels:
                changes["removed_episodes"].append((title, s_label, ep_num))

    return changes


def show_changes(
    changes,
    include_unwatched=True,
    include_watched=True,
    include_subscribe=True,
    include_unsubscribe=True,
    include_watchlist_add=True,
    include_watchlist_remove=True,
    new_data=None,
):
    """Display changes with pagination and smart season grouping."""

    total = 0
    for k, v in changes.items():
        if k == "newly_unwatched" and not include_unwatched:
            continue
        if k == "newly_watched" and not include_watched:
            continue
        if k == "newly_subscribed" and not include_subscribe:
            continue
        if k == "newly_unsubscribed" and not include_unsubscribe:
            continue
        if k == "watchlist_added" and not include_watchlist_add:
            continue
        if k == "watchlist_removed" and not include_watchlist_remove:
            continue
        total += len(v)
    if total == 0:
        return 0

    print("\n" + "=" * 70)
    print("  CHANGES DETECTED")
    print("=" * 70)

    if changes["new_series"]:
        print(f"\n[NEW SERIES] ({len(changes['new_series'])})")

        def format_new_series(title):
            if not new_data:
                return f"  + {title}"
            series = _find_series(new_data, title)
            if not series:
                return f"  + {title}"
            watched = series.get("watched_episodes", 0)
            total_ep = series.get("total_episodes", 0)
            sub = series.get("subscribed")
            wl = series.get("watchlist")
            sub_info = ""
            if sub is not None or wl is not None:
                parts = []
                if sub is not None:
                    parts.append(f"Sub:{'✓' if sub else '✗'}")
                if wl is not None:
                    parts.append(f"WL:{'✓' if wl else '✗'}")
                sub_info = f" ({' '.join(parts)})"
            return f"  + {title}: {watched}/{total_ep} watched{sub_info}"

        paginate_list(changes["new_series"], format_new_series)

    if changes["new_episodes"]:
        if new_data:
            grouped_lines = group_episodes_by_season(changes["new_episodes"], new_data)
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(grouped_lines, lambda line: line)
        else:
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(changes["new_episodes"], lambda x: f"  + {x[0]} [{x[1]}] Ep {x[2]}")

    if changes["newly_watched"] and include_watched:
        print(f"\n[NEWLY WATCHED] ({len(changes['newly_watched'])} episodes)")
        watched_lines = group_episodes_by_season(changes["newly_watched"], new_data)
        paginate_list(watched_lines, lambda line: line)

    if changes.get("newly_unwatched") and include_unwatched:
        print(f"\n[SITE REPORTS UNWATCHED] ({len(changes['newly_unwatched'])} episodes)")
        unwatched_lines = group_episodes_by_season(changes["newly_unwatched"], new_data, prefix="[!]")
        paginate_list(unwatched_lines, lambda line: line)

    # Sub/WL display only (no input/allowed logic here)
    sub_wl_items = []
    if changes.get("newly_subscribed") and include_subscribe:
        sub_wl_items.extend([(title, "Sub", "✗", "✓") for title in changes["newly_subscribed"]])
    if changes.get("newly_unsubscribed") and include_unsubscribe:
        sub_wl_items.extend([(title, "Sub", "✓", "✗") for title in changes["newly_unsubscribed"]])
    if changes.get("watchlist_added") and include_watchlist_add:
        sub_wl_items.extend([(title, "WL", "✗", "✓") for title in changes["watchlist_added"]])
    if changes.get("watchlist_removed") and include_watchlist_remove:
        sub_wl_items.extend([(title, "WL", "✓", "✗") for title in changes["watchlist_removed"]])
    if sub_wl_items:
        print(f"\n[SUBSCRIPTION / WATCHLIST CHANGES] ({len(sub_wl_items)})")
        paginate_list(sub_wl_items, lambda x: f"  ~ {x[0]}: {x[1]}: {x[2]} → {x[3]}")

    if changes.get("title_ger_changed"):
        print(f"\n[GERMAN TITLE CHANGED] ({len(changes['title_ger_changed'])} series)")
        paginate_list(changes["title_ger_changed"], lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'")

    if changes.get("title_eng_changed"):
        print(f"\n[ENGLISH TITLE CHANGED] ({len(changes['title_eng_changed'])} series)")
        paginate_list(changes["title_eng_changed"], lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'")

    print("\n" + "=" * 70)
    return total


class IndexManager:
    """Manages persistent series index for aniworld.to with atomic writes."""

    def __init__(self, index_file):
        self.index_file = index_file
        self.series_index = {}
        self.load_index()

    def load_index(self):
        """Load existing series index from file with corruption detection."""
        if not os.path.exists(self.index_file):
            # A save that failed midway used to be able to leave no file here
            # at all while the data sat in .bak1. Recovering only from corrupt
            # JSON missed that case and silently started from an empty index,
            # which then reads as "every series is new".
            recovered = self._try_restore_backup()
            if recovered:
                self.series_index = recovered
                print("[INFO] Index file missing — restored from backup.")
                logger.warning("Index file missing at %s; restored from backup", self.index_file)
                return
            logger.info("No existing index found at %s", self.index_file)
            self.series_index = {}
            return
        try:
            with open(self.index_file, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.series_index = {s.get("title"): s for s in data if s.get("title") and isinstance(s, dict)}
            elif isinstance(data, dict):
                first_item = next(iter(data.values()), None)
                if first_item and isinstance(first_item, dict) and first_item.get("title"):
                    self.series_index = data
                else:
                    self.series_index = {
                        item.get("title"): item
                        for item in data.values()
                        if isinstance(item, dict) and item.get("title")
                    }
            else:
                self.series_index = {}

            validated_index = {}
            rehosted = 0
            for title, series in self.series_index.items():
                if not _validate_series_entry(series, str(title)):
                    continue
                # An entry stored against a mirror that has since left
                # _SITE_URLS keeps its data and gets its host rewritten.
                # It used to be dropped here and then written out of the
                # index by the next save.
                moved = False
                for field in ("url", "link"):
                    value = series.get(field)
                    if value:
                        new_value, changed = _rehost_series_url(value)
                        if changed:
                            series[field] = new_value
                            moved = True
                rehosted += bool(moved)
                validated_index[title] = series
            self.series_index = validated_index
            if rehosted:
                print(f"[INFO] Repointed {rehosted} index entry(s) to {SITE_URL} (stored host no longer configured).")
                logger.warning("Repointed %d index entry(s) to %s", rehosted, SITE_URL)
            if not self.series_index:
                logger.warning("Loaded index is empty or contains no valid series")

        except json.JSONDecodeError as e:
            print(f"[ERROR] Index file corrupted: {e}")
            logger.error("Index file corrupted: %s", e)
            recovered = self._try_restore_backup()
            if recovered is not None:
                self.series_index = recovered
                print("[INFO] Restored index from backup.")
                logger.info("Restored index from backup after JSONDecodeError")
            else:
                self.series_index = {}
        except OSError as e:
            print(f"[ERROR] Cannot read index file: {e}")
            logger.error("Cannot read index file: %s", e)
            self.series_index = {}
        except Exception as e:
            print(f"[WARN] Error loading index: {e}")
            logger.error("Error loading index: %s", e)
            self.series_index = {}

    def _try_restore_backup(self):
        """Attempt to restore a valid index from the most recent backup."""
        backup_dir = os.path.dirname(self.index_file)
        filename = os.path.basename(self.index_file)
        for i in range(1, 4):
            backup_path = os.path.join(backup_dir, f"{filename}.bak{i}")
            if not os.path.exists(backup_path):
                continue
            try:
                with open(backup_path, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return {s.get("title"): s for s in data if s.get("title") and isinstance(s, dict)}
                if isinstance(data, dict):
                    first_item = next(iter(data.values()), None)
                    if first_item and isinstance(first_item, dict) and first_item.get("title"):
                        return data
                    return {
                        item.get("title"): item
                        for item in data.values()
                        if isinstance(item, dict) and item.get("title")
                    }
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Backup %s also corrupted: %s", backup_path, e)
        return None

    def _reconcile_derived_counts(self):
        """Force every derived count in the index to agree with its episodes.

        ``episodes`` is the only field scraped from the site; the season and
        series counters are derived from it. Recomputing them here means a
        writer that forgets to refresh them can never persist a stale count,
        and the warning below surfaces the offending writer instead of
        letting the drift sit silently in the index for months.
        """
        drifted = 0
        for series in self.series_index.values():
            for season in series.get("seasons", []):
                before = (season.get("total_episodes"), season.get("watched_episodes"))
                total, watched = sync_season_counts(season)
                if before != (total, watched):
                    drifted += 1
            total_eps, watched_eps = get_episode_counts(series)
            series["total_seasons"] = len(series.get("seasons", []))
            series["total_episodes"] = total_eps
            series["watched_episodes"] = watched_eps
            series["unwatched_episodes"] = total_eps - watched_eps
        if drifted:
            logger.warning(
                "Reconciled %d season counter(s) that disagreed with their episode lists",
                drifted,
            )
        return drifted

    def save_index(self):
        """Save series index to file atomically."""
        try:
            self._reconcile_derived_counts()
            series_list = list(self.series_index.values())
            _atomic_write_json(self.index_file, series_list)
            logger.info("Saved index with %d series", len(self.series_index))
        except Exception as e:
            print(f"[ERROR] Failed to save index: {e}")
            logger.error("Error saving index: %s", e)
            raise

    def get_series_with_progress(self, sort_by="completion", reverse=False):
        """Get series with computed episode progress information."""
        series_list = []
        for s in self.series_index.values():
            total_eps, watched_eps = get_episode_counts(s)
            is_incomplete = (total_eps == 0) or (watched_eps < total_eps)
            completion = round((watched_eps / total_eps) * 100, 2) if total_eps > 0 else 0.0
            series_list.append(
                {
                    "title": s.get("title", ""),
                    "watched_episodes": watched_eps,
                    "total_episodes": total_eps,
                    "is_incomplete": is_incomplete,
                    "completion": completion,
                    "subscribed": s.get("subscribed", False),
                    "watchlist": s.get("watchlist", False),
                    "season_labels": [str(sn.get("season", "?")) for sn in s.get("seasons", [])],
                }
            )
        if sort_by:
            series_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        return series_list

    def get_statistics(self):
        """Enhanced statistics with detailed analytics."""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)

        if total == 0:
            return {
                "total_series": 0,
                "watched": 0,
                "unwatched": 0,
                "watched_percentage": 0.0,
            }

        watched = sum(1 for s in series_with_progress if not s["is_incomplete"])
        unwatched = total - watched
        completion_percentages = [s["completion"] for s in series_with_progress]
        avg_completion = round(sum(completion_percentages) / total, 2)

        total_episodes = sum(s["total_episodes"] for s in series_with_progress)
        watched_episodes = sum(s["watched_episodes"] for s in series_with_progress)
        avg_episodes_per_series = round(total_episodes / total, 1) if total > 0 else 0

        subscribed_count = sum(1 for s in series_with_progress if s["subscribed"])
        watchlist_count = sum(1 for s in series_with_progress if s["watchlist"])
        both_count = sum(1 for s in series_with_progress if s["subscribed"] and s["watchlist"])

        completion_distribution = {
            "0-25%": sum(1 for p in completion_percentages if 0 <= p < 25),
            "25-50%": sum(1 for p in completion_percentages if 25 <= p < 50),
            "50-75%": sum(1 for p in completion_percentages if 50 <= p < 75),
            "75-99%": sum(1 for p in completion_percentages if 75 <= p < 100),
            "100%": sum(1 for p in completion_percentages if p == 100),
        }

        ongoing_only = [s for s in series_with_progress if 0 < s["completion"] < 100]
        sorted_ongoing = sorted(ongoing_only, key=lambda x: x["completion"], reverse=True)
        most_completed = sorted_ongoing[:5]
        least_completed = sorted_ongoing[-5:] if sorted_ongoing else []

        completed_count = watched
        ongoing_count = len(ongoing_only)
        not_started_count = sum(1 for s in series_with_progress if s["watched_episodes"] == 0)

        return {
            "total_series": total,
            "watched": watched,
            "unwatched": unwatched,
            "watched_percentage": round((watched / total * 100), 2),
            "completed_count": completed_count,
            "ongoing_count": ongoing_count,
            "not_started_count": not_started_count,
            "average_completion": avg_completion,
            "total_episodes": total_episodes,
            "watched_episodes": watched_episodes,
            "unwatched_episodes": total_episodes - watched_episodes,
            "average_episodes_per_series": avg_episodes_per_series,
            "subscribed_count": subscribed_count,
            "watchlist_count": watchlist_count,
            "both_subscribed_and_watchlist": both_count,
            "completion_distribution": completion_distribution,
            "most_completed_series": [
                {
                    "title": s["title"],
                    "completion": s["completion"],
                    "progress": f"{s['watched_episodes']}/{s['total_episodes']}",
                }
                for s in most_completed
            ],
            "least_completed_series": [
                {
                    "title": s["title"],
                    "completion": s["completion"],
                    "progress": f"{s['watched_episodes']}/{s['total_episodes']}",
                }
                for s in least_completed
            ],
        }

    def get_full_report(self, filter_subscribed=None, filter_watchlist=None, filter_mode="and"):
        """Generate a comprehensive report with detailed analytics."""
        series_progress = self.get_series_with_progress()
        stats = self.get_statistics()

        if filter_mode == "or" and filter_subscribed is not None and filter_watchlist is not None:
            series_progress = [
                s for s in series_progress if s["subscribed"] == filter_subscribed or s["watchlist"] == filter_watchlist
            ]
        else:
            if filter_subscribed is not None:
                series_progress = [s for s in series_progress if s["subscribed"] == filter_subscribed]
            if filter_watchlist is not None:
                series_progress = [s for s in series_progress if s["watchlist"] == filter_watchlist]

        watched_series = [s for s in series_progress if not s["is_incomplete"] and s.get("subscribed")]
        waiting_for_new = [s for s in series_progress if not s["is_incomplete"] and s.get("watchlist")]
        ongoing_series = [s for s in series_progress if s["is_incomplete"] and s["watched_episodes"] > 0]
        not_started_series = [s for s in series_progress if s["watched_episodes"] == 0]
        not_started_sub_wl = [
            s
            for s in series_progress
            if s["watched_episodes"] == 0 and s["total_episodes"] > 0 and (s.get("subscribed") or s.get("watchlist"))
        ]
        not_started_sub_wl_sorted = sorted(not_started_sub_wl, key=lambda x: x["title"])
        surprise_series = [
            s
            for s in series_progress
            if s.get("subscribed") and not s.get("watchlist") and s["is_incomplete"] and s["watched_episodes"] > 0
        ]
        surprise_sorted = sorted(surprise_series, key=lambda x: x["completion"], reverse=True)
        waiting_sorted = sorted(waiting_for_new, key=lambda x: x["title"])

        ongoing_sorted = sorted(ongoing_series, key=lambda x: x["completion"], reverse=True)

        episode_ranges = {
            "short_series": [s["title"] for s in series_progress if s["total_episodes"] <= 5],
            "medium_series": [s["title"] for s in series_progress if 6 <= s["total_episodes"] <= 25],
            "long_series": [s["title"] for s in series_progress if s["total_episodes"] > 25],
        }

        near_completion = [s["title"] for s in ongoing_sorted if 80 <= s["completion"] < 100][:10]
        stalled = [s["title"] for s in ongoing_sorted if s["completion"] < 25][:10]

        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_series_in_index": len(self.series_index),
                "active_series": len(series_progress),
                "filter_subscribed": filter_subscribed,
                "filter_watchlist": filter_watchlist,
                "statistics": stats,
            },
            "categories": {
                "watched": {
                    "count": len(watched_series),
                    "titles": sorted([s["title"] for s in watched_series]),
                },
                "ongoing": {
                    "count": len(ongoing_series),
                    "titles": [s["title"] for s in ongoing_sorted],
                    "details": [
                        {
                            "title": s["title"],
                            "completion": s["completion"],
                            "progress": f"{s['watched_episodes']}/{s['total_episodes']}",
                            "seasons": s.get("season_labels", []),
                        }
                        for s in ongoing_sorted[:20]
                    ],
                },
                "waiting_for_new_episodes": {
                    "count": len(waiting_for_new),
                    "titles": [s["title"] for s in waiting_sorted],
                    "details": [
                        {
                            "title": s["title"],
                            "progress": f"{s['watched_episodes']}/{s['total_episodes']}",
                            "subscribed": s["subscribed"],
                            "watchlist": s["watchlist"],
                            "seasons": s.get("season_labels", []),
                        }
                        for s in waiting_sorted
                    ],
                },
                "not_started": {
                    "count": len(not_started_series),
                    "titles": sorted([s["title"] for s in not_started_series]),
                },
                "surprise_new_episodes": {
                    "count": len(surprise_series),
                    "titles": [s["title"] for s in surprise_sorted],
                    "details": [
                        {
                            "title": s["title"],
                            "completion": s["completion"],
                            "progress": f"{s['watched_episodes']}/{s['total_episodes']}",
                            "seasons": s.get("season_labels", []),
                        }
                        for s in surprise_sorted
                    ],
                },
                "not_started_subscribed_watchlist": {
                    "count": len(not_started_sub_wl),
                    "titles": [s["title"] for s in not_started_sub_wl_sorted],
                    "details": [
                        {
                            "title": s["title"],
                            "total_episodes": s["total_episodes"],
                            "subscribed": s["subscribed"],
                            "watchlist": s["watchlist"],
                            "seasons": s.get("season_labels", []),
                        }
                        for s in not_started_sub_wl_sorted
                    ],
                },
            },
            "insights": {
                "completion_distribution": stats.get("completion_distribution", {}),
                "episode_ranges": episode_ranges,
                "near_completion": near_completion,
                "stalled_series": stalled,
                "most_completed": stats.get("most_completed_series", [])[:10],
                "least_completed": stats.get("least_completed_series", [])[:10],
            },
            "raw_data": {
                "all_series": self.series_index,
                "series_progress": series_progress,
            },
        }
        return report


def _format_subscription_and_watchlist_changes(changes):
    """Format subscription and watchlist changes into an aligned Action/Title table.

    Returns a formatted string with two columns: Action and Title.
    """
    rows = []
    for title in sorted(changes.get("newly_subscribed", [])):
        rows.append(("+ Subscribed", title))
    for title in sorted(changes.get("newly_unsubscribed", [])):
        rows.append(("- Unsubscribed", title))
    for title in sorted(changes.get("watchlist_added", [])):
        rows.append(("+ Watchlist", title))
    for title in sorted(changes.get("watchlist_removed", [])):
        rows.append(("- Watchlist", title))

    if not rows:
        return "  (no subscription/watchlist changes)"

    term_w = max(shutil.get_terminal_size().columns, 80)
    action_w = max(
        len("+ Subscribed"),
        len("- Unsubscribed"),
        len("+ Watchlist"),
        len("- Watchlist"),
        len("Action"),
    )
    max_title = max((len(str(t)) for _, t in rows), default=len("Title"))
    # indent (2) + action + gap (2) + title, capped by terminal width
    title_w = min(max_title, max(term_w - action_w - 6, 20))

    def _trunc(text, width):
        return text if len(text) <= width else text[: width - 1] + "…"

    lines = [
        f"  {'Action':<{action_w}}  {'Title':<{title_w}}",
        f"  {'─' * action_w}  {'─' * title_w}",
    ]
    for action, title in rows:
        lines.append(f"  {action:<{action_w}}  {_trunc(title, title_w):<{title_w}}")
    return "\n".join(lines)


def _detect_episode_count_mismatches(old_data, new_dict):
    """Detect episode count mismatches with detailed per-season analysis.

    Matches series by a stable key combining normalized title and URL slug so
    similarly-named shows are not confused. Series whose slug changed are
    skipped here and handled by the vanished/rename logic instead.
    """
    if isinstance(old_data, list):
        old_map = {s.get("title"): s for s in old_data if s and s.get("title")}
    else:
        old_map = dict(old_data) if old_data else {}

    # Build old lookup by stable match key; also keep a title-only fallback.
    old_by_key: dict[str, dict] = {}
    for title, entry in old_map.items():
        key = _series_match_key(title, entry.get("url", entry.get("link", "")))
        old_by_key[key] = entry

    mismatches = []
    for title, new_entry in new_dict.items():
        new_key = _series_match_key(title, new_entry.get("url", new_entry.get("link", "")))
        old_entry = old_by_key.get(new_key)

        if old_entry is None:
            # Legacy fallback: try exact title match for old indexes without slug data.
            old_entry = old_map.get(title)

        if old_entry is None:
            continue

        # If the URL slug changed, this is likely a rename; let vanished/rename logic handle it.
        old_slug = _extract_slug(old_entry)
        new_slug = _extract_slug(new_entry)
        if old_slug and new_slug and old_slug != new_slug:
            continue
        old_total, old_watched = get_episode_counts(old_entry)
        new_total = new_entry.get("total_episodes", 0)
        new_watched = new_entry.get("watched_episodes", 0)

        if old_total == 0 and new_total == 0:
            continue

        mismatch_details = {"title": title, "severity": "info", "issues": []}

        # 1. Total episode count difference
        if old_total != new_total:
            diff = new_total - old_total
            # Scale first, then round: rounding the ratio to 3 places quantised
            # the result to 0.1% steps and left float noise like 33.30000000004.
            denominator = max(old_total, new_total)
            percent_diff = round(diff / denominator * 100, 1) if denominator > 0 else 0
            mismatch_details["issues"].append(
                {
                    "type": "total_episode_count",
                    "old": old_total,
                    "new": new_total,
                    "diff": diff,
                    "percent_diff": percent_diff,
                }
            )
            # Only flag EPISODE LOSS as critical (disappearance)
            # Episode additions are normal (weekly releases), not a warning
            if new_total < old_total:
                # Episodes disappeared
                mismatch_details["severity"] = "critical"

        # 2. Season structure changes
        old_seasons = {s.get("season"): s for s in old_entry.get("seasons", [])}
        new_seasons = {s.get("season"): s for s in new_entry.get("seasons", [])}
        old_labels = set(old_seasons.keys())
        new_labels = set(new_seasons.keys())
        if old_labels != new_labels:
            mismatch_details["issues"].append(
                {
                    "type": "season_structure_change",
                    "seasons_removed": sorted(old_labels - new_labels),
                    "seasons_added": sorted(new_labels - old_labels),
                }
            )
            if old_labels - new_labels:
                mismatch_details["severity"] = "critical"

        # 3. Per-season episode count differences
        season_issues = []
        for label in old_labels & new_labels:
            old_cnt = len(old_seasons[label].get("episodes", []))
            new_cnt = len(new_seasons[label].get("episodes", []))
            if old_cnt != new_cnt:
                season_issues.append(
                    {
                        "season": label,
                        "old_count": old_cnt,
                        "new_count": new_cnt,
                        "diff": new_cnt - old_cnt,
                    }
                )
        if season_issues:
            mismatch_details["issues"].append({"type": "per_season_episode_mismatch", "seasons": season_issues})
            if any(abs(s["diff"]) > 10 for s in season_issues):
                mismatch_details["severity"] = "critical"

        # 4. Check episode title changes (only if titles exist)
        title_changes = []
        for label in old_labels & new_labels:
            old_season = old_seasons[label]
            new_season = new_seasons[label]
            old_eps_by_num = {ep.get("number"): ep for ep in old_season.get("episodes", [])}
            new_eps_by_num = {ep.get("number"): ep for ep in new_season.get("episodes", [])}

            for ep_num in old_eps_by_num.keys() & new_eps_by_num.keys():
                old_ep = old_eps_by_num[ep_num]
                new_ep = new_eps_by_num[ep_num]
                old_title = old_ep.get("title_ger") or old_ep.get("title_eng") or old_ep.get("title", "")
                new_title = new_ep.get("title_ger") or new_ep.get("title_eng") or new_ep.get("title", "")

                if old_title and new_title and old_title != new_title:
                    title_changes.append(
                        {
                            "season": label,
                            "episode": ep_num,
                            "old_title": old_title[:50],
                            "new_title": new_title[:50],
                        }
                    )

        if title_changes:
            mismatch_details["issues"].append(
                {
                    "type": "episode_title_changes",
                    "count": len(title_changes),
                    "samples": title_changes[:3],
                }
            )

        # 5. Watched status inconsistency - ALWAYS flag if watched > total
        if new_total > 0 and new_watched > new_total:
            mismatch_details["issues"].append(
                {
                    "type": "watched_status_inconsistency",
                    "old_watched": old_watched,
                    "new_watched": new_watched,
                    "new_total": new_total,
                    "description": "More episodes marked watched than total episodes (data corruption?)",
                }
            )
            mismatch_details["severity"] = "critical"

        # 5. Unwatched calculation mismatch
        new_unwatched_stored = new_entry.get("unwatched_episodes", 0)
        new_unwatched_calc = max(0, new_total - new_watched)
        if new_unwatched_stored != new_unwatched_calc:
            mismatch_details["issues"].append(
                {
                    "type": "unwatched_calculation_mismatch",
                    "expected": new_unwatched_calc,
                    "stored": new_unwatched_stored,
                    "description": "Unwatched episodes field doesn't match (total - watched)",
                }
            )
            if mismatch_details["severity"] == "info":
                mismatch_details["severity"] = "warning"

        # 6. Watched count drop
        if new_watched < old_watched:
            mismatch_details["issues"].append(
                {
                    "type": "watched_count_drop",
                    "old_watched": old_watched,
                    "new_watched": new_watched,
                }
            )
            if mismatch_details["severity"] == "info":
                mismatch_details["severity"] = "warning"

        if mismatch_details["issues"]:
            mismatches.append(mismatch_details)
    return mismatches


def _extract_critical_series_for_rescrape(mismatches, old_data, active_site_url=None):
    """Extract critical series and their URLs for rescraping.

    Returns:
        dict with 'urls', 'titles', and 'series' keys for critical issues
    """
    critical = [m for m in mismatches if m["severity"] == "critical"]
    if not critical:
        return {"urls": [], "titles": [], "series": {}}

    if isinstance(old_data, list):
        old_map = {s.get("title"): s for s in old_data if s and s.get("title")}
    else:
        old_map = dict(old_data) if old_data else {}

    base_url = (active_site_url or "https://aniworld.to").rstrip("/")
    urls = []
    titles = []
    series_data = {}

    for mismatch in critical:
        title = mismatch["title"]
        titles.append(title)

        if title in old_map:
            entry = old_map[title]
            url = entry.get("url") or entry.get("link")
            if url:
                if not url.startswith("http"):
                    url = f"{base_url}{url}"
                urls.append(url)
                series_data[title] = entry

    return {
        "urls": urls,
        "titles": titles,
        "series": series_data,
    }


def _prompt_episode_mismatches(mismatches, old_data=None, active_site_url=None):
    """Prompt user for warning/critical issues with option to delete & rescrape critical ones.

    Returns:
        tuple: (proceed: bool, rescrape_data: dict or None)
        - proceed: Whether to merge despite issues
        - rescrape_data: Dict with 'urls', 'titles' if user chose to rescrape critical series
    """
    if not mismatches:
        return True, None

    critical = [m for m in mismatches if m["severity"] == "critical"]
    warning = [m for m in mismatches if m["severity"] == "warning"]
    info = [m for m in mismatches if m["severity"] == "info"]

    if not critical and not warning:
        if info:
            logger.debug("Auto-approved %d minor index updates", len(info))
        return True, None

    def _format_mismatch_issue(issue):
        """Format a single issue into readable text."""
        lines = []
        if issue["type"] == "season_structure_change":
            if issue.get("seasons_removed"):
                lines.append(f"   → Seasons removed: {', '.join(issue['seasons_removed'])}")
            if issue.get("seasons_added"):
                lines.append(f"   → Seasons added: {', '.join(issue['seasons_added'])}")
        elif issue["type"] == "total_episode_count":
            lines.append(
                f"   → Episodes: {issue['old']} → {issue['new']} ({issue['diff']:+d}, {issue['percent_diff']}%)"
            )
        elif issue["type"] == "per_season_episode_mismatch":
            for s in issue["seasons"]:
                lines.append(f"   → S{s['season']}: {s['old_count']} → {s['new_count']} eps ({s['diff']:+d})")
        elif issue["type"] == "watched_status_inconsistency":
            lines.append(f"   → CORRUPTION: Watched ({issue['old_watched']}) > Total ({issue['new_total']})")
        elif issue["type"] == "watched_count_drop":
            diff = issue["new_watched"] - issue["old_watched"]
            lines.append(f"   → Watched drop: {issue['old_watched']} → {issue['new_watched']} ({diff:+d})")
        elif issue["type"] == "episode_title_changes":
            lines.append(f"   → {issue['count']} episode title(s) changed")
            for s in issue.get("samples", []):
                lines.append(f'     [{s["season"]}] Ep {s["episode"]}: "{s["old_title"]}" → "{s["new_title"]}"')
        elif issue["type"] == "unwatched_calculation_mismatch":
            lines.append(f"   → Calculation error: Expected unwatched {issue['expected']}, stored {issue['stored']}")
        else:
            lines.append(f"   → {issue['type']}")
        return lines

    def _format_mismatch_entry(mismatch):
        """Format a complete mismatch entry (title + all issues)."""
        lines = [f" {mismatch['title']}"]
        for issue in mismatch["issues"]:
            lines.extend(_format_mismatch_issue(issue))
        return "\n".join(lines)

    term_w = max(shutil.get_terminal_size().columns - 2, 40)
    print("\n" + "━" * term_w)
    print("DATA INTEGRITY CHECK")
    print("━" * term_w)

    # Write integrity check issues to file only (no console logging)
    if critical + warning:
        try:
            log_file = os.path.join(os.path.dirname(__file__), "..", "logs", "integrity_check.log")
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"\n[{datetime.now().isoformat()}] Integrity Check\n")
                f.write(f"Critical: {len(critical)}, Warnings: {len(warning)}\n")
                for m in critical:
                    f.write(f"  CRITICAL - {m['title']}: {len(m['issues'])} issue(s)\n")
                for m in warning:
                    f.write(f"  WARNING - {m['title']}: {len(m['issues'])} issue(s)\n")
        except Exception:
            pass  # Silent fail for logging

    # Show CRITICAL issues with pagination
    if critical:
        print(f"\nCRITICAL ISSUES ({len(critical)})")
        print("─" * term_w)

        formatted_critical = [_format_mismatch_entry(m) for m in critical]
        paginate_list(formatted_critical, lambda x: x, page_size=3)

    # Show WARNING issues with pagination
    if warning:
        print(f"\nWARNINGS ({len(warning)})")
        print("─" * term_w)

        formatted_warnings = [_format_mismatch_entry(m) for m in warning]
        paginate_list(formatted_warnings, lambda x: x, page_size=5)

    print("\n" + "━" * term_w)

    # Offer options for critical issues
    if critical:
        print("\nOPTIONS")
        print("─" * term_w)
        print("1) Proceed with merge despite issues")
        print(f"2) Delete index & rescrape {len(critical)} critical series")
        print("3) Cancel (discard all changes)\n")
        choice = input("Choose option (1-3): ").strip()

        if choice == "2":
            # Extract URLs for rescraping
            rescrape_data = _extract_critical_series_for_rescrape(critical, old_data, active_site_url=active_site_url)
            if rescrape_data["urls"]:
                print(f"\nWill rescrape {len(rescrape_data['urls'])} critical series")
                return False, rescrape_data  # False = don't merge now, rescrape instead
            else:
                print("\nCould not extract URLs for critical series")
                return False, None
        elif choice == "3":
            return False, None
        # Default or choice '1': proceed
    else:
        choice = input("\nProceed with merge despite warnings? (y/n): ").strip().lower()
        return choice == "y", None

    return True, None


def _cascade_declined_new_content(changes, allowed):
    """Drop state changes that belong to new content the user just declined.

    The existence gates run first. Once a new series or a new episode is
    refused, there is nothing left to decide about its watch or
    subscription state, so it must not appear in the prompts that follow.
    """
    if not allowed.get("new_series", True):
        refused = set(changes.get("new_series") or [])
        if refused:
            changes["newly_watched"] = [x for x in changes["newly_watched"] if x[0] not in refused]
            for key in ("newly_subscribed", "watchlist_added"):
                if key in changes:
                    changes[key] = [t for t in changes[key] if t not in refused]
    if not allowed.get("new_episodes", True):
        refused_eps = {tuple(x) for x in (changes.get("new_episodes") or [])}
        if refused_eps:
            changes["newly_watched"] = [x for x in changes["newly_watched"] if tuple(x) not in refused_eps]


def _prompt_change_confirmations(changes, new_dict):
    """Prompt the user to confirm each category of detected changes."""

    allowed = {
        # Existence gates: they decide what enters the index at all, and are
        # asked before the state gates below. Declining one cascades -- there
        # is nothing left to ask about content that is not being added.
        # Readers default an ABSENT flag to True: only an explicit refusal
        # keeps content out, so a caller that predates these gates still
        # stores everything, exactly as it did before.
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
        # Deletions default to False: if the user never answers, the index
        # keeps what it has. Losing watch history to a bad scrape is far
        # worse than carrying a stale episode for one more run.
        "episode_remove": False,
        "season_remove": False,
    }

    def _build_episode_lines(episode_list, new_dict, prefix="[+]"):
        grouped = defaultdict(list)
        for title, season, ep_num in episode_list:
            grouped[(title, season)].append(ep_num)
        lines = []
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            sub = "✓" if series and series.get("subscribed") else "✗"
            wl = "✓" if series and series.get("watchlist") else "✗"
            sub_wl = f" (Sub:{sub} WL:{wl})"
            if total_in_season > 0:
                lines.append(f"  {prefix} {title} [{season}]: {watched_in_season}/{total_in_season} episodes{sub_wl}")
            else:
                lines.append(f"  {prefix} {title} [{season}]: {len(ep_nums)} episode(s){sub_wl}")
        return lines

    def _show_and_confirm(header, items, formatter, prompt_text):
        print(f"\n{header}")
        print("   (manual confirmation required)")
        print("\n" + "-" * 70)
        for item in items:
            print(formatter(item))
        print("-" * 70)
        resp = input(f"\n{prompt_text} (y/n): ").strip().lower()
        return resp == "y"

    if changes["new_series"]:

        def _fmt_new_series(title):
            series = new_dict.get(title) or {}
            total_ep, watched_ep = get_episode_counts(series)
            sub = "✓" if series.get("subscribed") else "✗"
            wl = "✓" if series.get("watchlist") else "✗"
            return f"  [+] {title}: {watched_ep}/{total_ep} watched (Sub:{sub} WL:{wl})"

        if _show_and_confirm(
            f"[NEW SERIES] {len(changes['new_series'])} series not yet in the index",
            changes["new_series"],
            _fmt_new_series,
            "Add these new series to the index?",
        ):
            allowed["new_series"] = True
        else:
            print("  -> New series will NOT be added (offered again next scrape)")

    if changes["new_episodes"]:
        lines = _build_episode_lines(changes["new_episodes"], new_dict, prefix="[+]")
        if _show_and_confirm(
            f"[NEW EPISODES] {len(changes['new_episodes'])} episode(s) not yet in the index",
            lines,
            lambda x: x,
            "Add these new episodes to the index?",
        ):
            allowed["new_episodes"] = True
        else:
            print("  -> New episodes will NOT be added (offered again next scrape)")

    # Anything refused above drops out of the state prompts below.
    _cascade_declined_new_content(changes, allowed)

    if changes["newly_watched"]:
        lines = _build_episode_lines(changes["newly_watched"], new_dict, prefix="[+]")
        if _show_and_confirm(
            f"[OK] {len(changes['newly_watched'])} episode(s) would change from UNWATCHED to WATCHED",
            lines,
            lambda x: x,
            "Allow these episodes to be marked as WATCHED?",
        ):
            allowed["watched"] = True
        else:
            print("  -> Watched changes will be ignored (episodes stay unwatched)")

    if changes["newly_unwatched"]:
        lines = _build_episode_lines(changes["newly_unwatched"], new_dict, prefix="[!]")
        if _show_and_confirm(
            f"[WARN] {len(changes['newly_unwatched'])} episode(s) would change from WATCHED to UNWATCHED",
            lines,
            lambda x: x,
            "Allow these episodes to be marked as UNWATCHED?",
        ):
            allowed["unwatched"] = True
        else:
            print("  -> Unwatched changes will be ignored (episodes stay watched)")

    sub_wl_items_exist = (
        changes.get("newly_subscribed")
        or changes.get("newly_unsubscribed")
        or changes.get("watchlist_added")
        or changes.get("watchlist_removed")
    )
    if sub_wl_items_exist:
        total = (
            len(changes.get("newly_subscribed", []))
            + len(changes.get("newly_unsubscribed", []))
            + len(changes.get("watchlist_added", []))
            + len(changes.get("watchlist_removed", []))
        )
        formatted_changes = _format_subscription_and_watchlist_changes(changes)
        print(f"\n[SUBSCRIPTION / WATCHLIST CHANGES] ({total})")
        print("   (manual confirmation required)")
        term_w = max(shutil.get_terminal_size().columns - 6, 40)
        print("\n" + "─" * term_w)
        print(formatted_changes)
        print("─" * term_w)
        resp = input("\nAllow subscription/watchlist changes? (y/n): ").strip().lower()
        if resp == "y":
            if changes.get("newly_subscribed"):
                allowed["subscribe"] = True
            if changes.get("newly_unsubscribed"):
                allowed["unsubscribe"] = True
            if changes.get("watchlist_added"):
                allowed["watchlist_add"] = True
            if changes.get("watchlist_removed"):
                allowed["watchlist_remove"] = True
        else:
            print("  -> Subscription/watchlist changes will be ignored")

    if changes["title_ger_changed"]:

        def _fmt_ger_title_change(x):
            return f"  [~] {x[0]}\n      Old: {x[1]}\n      New: {x[2]}"

        if _show_and_confirm(
            f"[~] {len(changes['title_ger_changed'])} German title(s) changed",
            changes["title_ger_changed"],
            _fmt_ger_title_change,
            "Allow German title changes?",
        ):
            allowed["title_ger"] = True
        else:
            print("  -> German title changes will be ignored")

    if changes.get("removed_episodes"):
        grouped_removed = defaultdict(list)
        for title, season, ep_num in changes["removed_episodes"]:
            grouped_removed[(title, season)].append(str(ep_num))
        removed_lines = [
            f"  [-] {title} [{season}]: episode(s) {', '.join(nums)}"
            for (title, season), nums in sorted(grouped_removed.items())
        ]
        if _show_and_confirm(
            f"[WARN] {len(changes['removed_episodes'])} episode(s) are in the index but NOT in this scrape",
            removed_lines,
            lambda x: x,
            "DELETE these episodes from the index?",
        ):
            allowed["episode_remove"] = True
        else:
            print("  -> Episodes will be KEPT in the index (nothing deleted)")

    if changes.get("removed_seasons"):
        season_lines = [f"  [-] {title}: season {season}" for title, season in sorted(changes["removed_seasons"])]
        if _show_and_confirm(
            f"[WARN] {len(changes['removed_seasons'])} season(s) are in the index but NOT in this scrape",
            season_lines,
            lambda x: x,
            "DELETE these whole seasons from the index?",
        ):
            allowed["season_remove"] = True
        else:
            print("  -> Seasons will be KEPT in the index (nothing deleted)")

    if changes["title_eng_changed"]:

        def _fmt_eng_title_change(x):
            return f"  [~] {x[0]}\n      Old: {x[1]}\n      New: {x[2]}"

        if _show_and_confirm(
            f"[~] {len(changes['title_eng_changed'])} English title(s) changed",
            changes["title_eng_changed"],
            _fmt_eng_title_change,
            "Allow English title changes?",
        ):
            allowed["title_eng"] = True
        else:
            print("  -> English title changes will be ignored")

    return allowed


_SCALARS = {str, int, float, bool, type(None)}

# Mirrors copy.deepcopy's own signature: the copy has the type of the original.
# Without this the return widens to dict | list | Any, and every later
# subscript of the merged index is flagged as a possible list index.
_Copyable = TypeVar("_Copyable")


def fast_copy(x: _Copyable) -> _Copyable:
    """Deep copy JSON-like data faster, fall back to copy.deepcopy otherwise.

    Dicts and lists are rebuilt recursively so the result is never aliased.
    JSON scalars are immutable and returned as-is. Anything else (tuples,
    sets, dates, custom objects) is handed to copy.deepcopy so behaviour is
    identical to the original implementation for every possible input.

    The body works through a deliberately untyped alias: the signature's
    promise -- the copy has the type of the original -- is one a checker
    cannot verify from a freshly built dict or list, and stating it this way
    keeps that promise without scattering casts through the recursion.
    """
    value: Any = x
    copied: Any
    if type(value) is dict:
        copied = {k: fast_copy(v) for k, v in value.items()}
    elif type(value) is list:
        copied = [fast_copy(v) for v in value]
    elif type(value) in _SCALARS:
        copied = value
    else:
        copied = copy.deepcopy(value)
    return copied


def _build_merged_data(old_data, new_dict, allowed):
    """Merge new scraped data into old data, respecting user-allowed change categories.

    Both inputs are copied first. The merge resolves each episode's watch flag
    by writing it back into the new entry, so without this the caller's own
    data came back rewritten -- merging the same scrape twice gave a different
    answer the second time, and the scraper's series_data was quietly altered.
    """
    old_data = fast_copy(old_data)
    new_dict = fast_copy(new_dict)
    merged = old_data
    for title, new_entry in new_dict.items():
        if title in merged:
            old_entry = merged[title]
            old_seasons = {s.get("season"): s for s in old_entry.get("seasons", [])}
            for new_season in new_entry.get("seasons", []):
                season_label = new_season.get("season")
                if season_label in old_seasons:
                    old_eps = {str(ep.get("number")): ep for ep in old_seasons[season_label].get("episodes", [])}
                    merged_episodes = []
                    seen_new: set[str] = set()
                    for new_ep in new_season.get("episodes", []):
                        ep_num = str(new_ep.get("number"))
                        seen_new.add(ep_num)
                        if ep_num in old_eps:
                            old_watched = old_eps[ep_num].get("watched", False)
                            new_watched = new_ep.get("watched", False)
                            if allowed["watched"] and (not old_watched and new_watched):
                                new_ep["watched"] = True
                            elif allowed["unwatched"] and (old_watched and not new_watched):
                                new_ep["watched"] = False
                            else:
                                new_ep["watched"] = old_watched
                        else:
                            # An episode the index has never seen. It enters
                            # only if the user approved new episodes, and it
                            # enters unwatched unless they also approved the
                            # watched change it arrived with.
                            if not allowed.get("new_episodes", True):
                                continue
                            if new_ep.get("watched", False) and not allowed["watched"]:
                                new_ep["watched"] = False
                        merged_episodes.append(new_ep)
                    if not allowed.get("episode_remove", False):
                        # An episode the scrape did not return is kept unless
                        # the user approved deleting it. Appended entries are
                        # re-sorted so the season stays in episode order.
                        merged_episodes.extend(old_ep for ep_num, old_ep in old_eps.items() if ep_num not in seen_new)
                        merged_episodes.sort(key=lambda e: (e.get("number") is None, e.get("number") or 0))
                    old_seasons[season_label]["episodes"] = merged_episodes
                    # Remove leftover episode 0 if this season is in the ignore list
                    if new_season.get("ignored_episode_0"):
                        old_seasons[season_label]["episodes"] = [
                            ep for ep in old_seasons[season_label]["episodes"] if ep.get("number") != 0
                        ]
                    # Sync ignored_episode_0: add or remove based on new data
                    if new_season.get("ignored_episode_0"):
                        old_seasons[season_label]["ignored_episode_0"] = True
                    else:
                        old_seasons[season_label].pop("ignored_episode_0", None)
                    # The episode list above was rebuilt from the new scrape;
                    # refresh this season's derived counters to match it.
                    sync_season_counts(old_seasons[season_label])
                else:
                    validated_eps = []
                    for ep in new_season.get("episodes", []):
                        if ep.get("watched") is None:
                            logger.error(
                                "Episode %s in new season '%s' for '%s' has None watched status — dropping episode",
                                ep.get("number"),
                                season_label,
                                title,
                            )
                            continue
                        validated_eps.append(ep)
                    new_season["episodes"] = validated_eps
                    # Dropping an invalid episode above invalidates the counts
                    # the scraper computed, so recompute them here too.
                    sync_season_counts(new_season)
                    old_seasons[season_label] = new_season
            new_labels = {season.get("season") for season in new_entry.get("seasons", [])}
            if allowed.get("season_remove", False) and new_labels:
                # `new_labels` must be non-empty: a series whose scrape failed
                # arrives here with no seasons at all, and that is a failed
                # request, not the site having deleted the whole show.
                old_seasons = {label: season for label, season in old_seasons.items() if label in new_labels}
            old_entry["seasons"] = list(old_seasons.values())
            old_entry["total_seasons"] = len(old_entry["seasons"])
            total_eps, watched_eps = get_episode_counts(old_entry)
            old_entry["watched_episodes"] = watched_eps
            old_entry["total_episodes"] = total_eps
            old_entry["unwatched_episodes"] = old_entry["total_episodes"] - old_entry["watched_episodes"]
            new_url = new_entry.get("url", "")
            new_link = new_entry.get("link", "")
            if new_url and _is_valid_series_url(new_url):
                old_entry["url"] = new_url
            elif new_url:
                logger.warning(
                    "Rejected invalid URL during merge for '%s': %s",
                    title,
                    new_url[:80],
                )
            if new_link and _is_valid_series_url(new_link):
                old_entry["link"] = new_link
            elif new_link:
                logger.warning(
                    "Rejected invalid link during merge for '%s': %s",
                    title,
                    new_link[:80],
                )
            if "subscribed" in new_entry:
                new_sub = new_entry["subscribed"]
                if new_sub is None:
                    logger.error(
                        "Ignoring None subscribed for existing entry '%s' — keeping old value",
                        title,
                    )
                else:
                    old_sub = old_entry.get("subscribed", False)
                    if old_sub != new_sub:
                        if new_sub and allowed["subscribe"]:
                            old_entry["subscribed"] = True
                        elif not new_sub and allowed["unsubscribe"]:
                            old_entry["subscribed"] = False
            if "watchlist" in new_entry:
                new_wl = new_entry["watchlist"]
                if new_wl is None:
                    logger.error(
                        "Ignoring None watchlist for existing entry '%s' — keeping old value",
                        title,
                    )
                else:
                    old_wl = old_entry.get("watchlist", False)
                    if old_wl != new_wl:
                        if new_wl and allowed["watchlist_add"]:
                            old_entry["watchlist"] = True
                        elif not new_wl and allowed["watchlist_remove"]:
                            old_entry["watchlist"] = False
            if allowed["title_ger"] and "title_ger" in new_entry:
                old_entry["title_ger"] = new_entry["title_ger"]
            if allowed["title_eng"] and "title_eng" in new_entry:
                old_entry["title_eng"] = new_entry["title_eng"]
            old_alts = old_entry.get("alt_titles", [])
            new_alts = new_entry.get("alt_titles", [])
            combined = list(dict.fromkeys(old_alts + new_alts))
            old_entry["alt_titles"] = combined
            old_entry["last_updated"] = datetime.now().isoformat()
            old_entry.setdefault("subscribed", False)
            old_entry.setdefault("watchlist", False)
            # EMA-update per-series scrape timing (70% old / 30% new)
            new_dur = new_entry.get("scrape_duration_seconds")
            if isinstance(new_dur, (int, float)) and new_dur > 0:
                old_avg = old_entry.get("avg_scrape_seconds")
                if isinstance(old_avg, (int, float)) and old_avg > 0:
                    old_entry["avg_scrape_seconds"] = round(old_avg * 0.7 + new_dur * 0.3, 3)
                    # Also preserve the actual scrape duration from this scrape
                    old_entry["scrape_duration_seconds"] = new_dur
                else:
                    old_entry["avg_scrape_seconds"] = round(new_dur, 3)
                    old_entry["scrape_duration_seconds"] = new_dur
            # If the site changed the series title, keep the new one. We still
            # key the dict by the original title so downstream change detection
            # (new/removed series) is stable; a full re-key would need extra care.
            new_title = new_entry.get("title")
            if new_title and new_title != title:
                old_entry["title"] = new_title
            merged[title] = {
                "url": old_entry.get("url", ""),
                "link": old_entry.get("link", ""),
                "subscribed": old_entry.get("subscribed", False),
                "watchlist": old_entry.get("watchlist", False),
                "title": old_entry.get("title", title),
                "title_ger": old_entry.get("title_ger", ""),
                "title_eng": old_entry.get("title_eng", ""),
                "alt_titles": old_entry.get("alt_titles", []),
                "total_seasons": len(old_entry.get("seasons", [])),
                "total_episodes": old_entry.get("total_episodes", 0),
                "watched_episodes": old_entry.get("watched_episodes", 0),
                "unwatched_episodes": old_entry.get("unwatched_episodes", 0),
                "avg_scrape_seconds": old_entry.get("avg_scrape_seconds"),
                "added_date": old_entry.get("added_date", ""),
                "last_updated": old_entry.get("last_updated", ""),
                "seasons": old_entry.get("seasons", []),
                "scrape_duration_seconds": old_entry.get("scrape_duration_seconds"),
            }
        else:
            if "subscribed" not in new_entry:
                logger.warning(
                    "New entry '%s' missing 'subscribed' field — setting to False",
                    title,
                )
                new_entry["subscribed"] = False
            elif new_entry["subscribed"] is None:
                logger.error(
                    "Rejecting new entry '%s': subscribed is None (scrape failed)",
                    title,
                )
                continue
            if "watchlist" not in new_entry:
                logger.warning("New entry '%s' missing 'watchlist' field — setting to False", title)
                new_entry["watchlist"] = False
            elif new_entry["watchlist"] is None:
                logger.error("Rejecting new entry '%s': watchlist is None (scrape failed)", title)
                continue
            if not allowed.get("new_series", True):
                # The user declined to add this series, so the index stays
                # unaware of it and the next scrape offers it again.
                continue
            new_entry.setdefault("alt_titles", [])
            new_entry["added_date"] = datetime.now().isoformat()
            new_entry["last_updated"] = datetime.now().isoformat()
            if not allowed["watched"]:
                # The series is being added, but the watch state it arrived
                # with was not approved, so it starts from the expected
                # default. The next scrape offers that state again.
                for season in new_entry.get("seasons", []):
                    for ep in season.get("episodes", []):
                        ep["watched"] = False
                    sync_season_counts(season)
            total_eps, watched_eps = get_episode_counts(new_entry)
            new_dur = new_entry.get("scrape_duration_seconds")
            merged[title] = {
                "url": new_entry.get("url", ""),
                "link": new_entry.get("link", ""),
                "subscribed": new_entry["subscribed"] if allowed["subscribe"] else False,
                "watchlist": new_entry["watchlist"] if allowed["watchlist_add"] else False,
                "title": new_entry.get("title", title),
                "title_ger": new_entry.get("title_ger", ""),
                "title_eng": new_entry.get("title_eng", ""),
                "alt_titles": new_entry.get("alt_titles", []),
                "total_seasons": len(new_entry.get("seasons", [])),
                "total_episodes": total_eps,
                "watched_episodes": watched_eps,
                "unwatched_episodes": total_eps - watched_eps,
                "avg_scrape_seconds": (
                    round(new_dur, 3) if isinstance(new_dur, (int, float)) and new_dur > 0 else None
                ),
                "added_date": new_entry.get("added_date", ""),
                "last_updated": new_entry.get("last_updated", ""),
                "seasons": new_entry.get("seasons", []),
                "scrape_duration_seconds": new_entry.get("scrape_duration_seconds"),
            }

    return merged


def _extract_slug_from_field(value):
    """Return the comparison key for the slug in a link or URL field.

    The result is normalised by :func:`slug_key`, because every caller of this
    function is asking "is this the same anime?" -- against the catalogue, the
    mismatch report, or another index entry. The site does not spell a slug the
    same way in every list it prints, so a raw string comparison here reports
    an entry as both vanished and new at once. Use the stored ``url``/``link``
    field, not this key, when building a request.
    """
    if not value or not isinstance(value, str):
        return None
    idx = value.find("/anime/stream/")
    if idx == -1:
        return None
    slug = value[idx + len("/anime/stream/") :].strip("/").split("/")[0]
    return slug_key(slug)


def _extract_slug(entry):
    """Extract series slug from an index entry's link or url.

    Delegates to AniWorldScraper.get_series_slug_from_url.
    Returns None if extraction fails.
    """
    if not isinstance(entry, dict):
        return None
    for field in ("link", "url"):
        value = entry.get(field, "")
        if not value or not isinstance(value, str):
            continue
        slug = _extract_slug_from_field(value)
        if slug:
            return slug
    return None


def _normalize_match_key(title: str) -> str:
    """Return a lowercase, stripped title with year and common words removed."""
    if not title:
        return ""
    lowered = title.lower()
    lowered = re.sub(r"\(\d{4}\)", " ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "of",
        "to",
        "in",
        "on",
        "at",
        "from",
        "with",
        "by",
        "no",
        "san",
        "chan",
        "kun",
        "sama",
    }
    tokens = [t for t in lowered.split() if t and t not in stopwords]
    return " ".join(sorted(set(tokens)))


def _series_match_key(title: str, url_or_slug: str = "") -> str:
    """Return a stable match key combining normalized title and URL slug.

    This prevents similarly-named series (e.g. "Der Unsichtbare" variants)
    from being confused during critical-episode mismatch detection.
    """
    title_key = _normalize_match_key(title)
    slug = ""
    if url_or_slug:
        raw = url_or_slug.lower()
        raw = re.sub(r"https?://[^/]+", "", raw)
        raw = re.sub(r"/anime/stream/", " ", raw)
        raw = re.sub(r"[^a-z0-9\-]", " ", raw)
        slug = " ".join(sorted({t for t in raw.split("-") if len(t) > 2}))
    parts = [p for p in (title_key, slug) if p]
    return " | ".join(parts)


def remove_series_from_index(index_file, titles_to_remove):
    """Remove series entries from the index file by title.

    Loads the index under a file lock, filters out entries whose title is in the
    removal set, and atomically writes back.
    Returns the number of entries actually removed.
    """
    if not titles_to_remove or not os.path.exists(index_file):
        return 0
    removal_set = set(titles_to_remove)
    try:
        with open(index_file, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            filtered = [entry for entry in data if entry.get("title") not in removal_set]
            removed = len(data) - len(filtered)
        elif isinstance(data, dict):
            filtered_dict = {k: v for k, v in data.items() if k not in removal_set}
            removed = len(data) - len(filtered_dict)
            filtered = list(filtered_dict.values())
        else:
            return 0

        if removed > 0:
            _atomic_write_json(index_file, filtered)
            logger.info(
                "Removed %d vanished series from index: %s",
                removed,
                list(removal_set)[:10],
            )
        return removed
    except (json.JSONDecodeError, OSError):
        return 0


def _match_keys(title: str, url_or_slug: str = "") -> set[str]:
    """Return normalized match keys for a title.

    Includes the full normalized title, each side of a '|' separator,
    and tokens from the URL/slug to catch slug-based renames.
    """
    keys: set[str] = set()
    full = _normalize_match_key(title)
    if full:
        keys.add(full)

    if "|" in title:
        for part in title.split("|"):
            part_key = _normalize_match_key(part)
            if part_key:
                keys.add(part_key)

    slug_tokens = ""
    if url_or_slug:
        slug = url_or_slug.lower()
        slug = re.sub(r"https?://[^/]+", "", slug)
        slug = re.sub(r"/anime/stream/", "", slug)
        slug = re.sub(r"[^a-z0-9\-]", " ", slug)
        slug_tokens = " ".join(sorted({t for t in slug.split("-") if len(t) > 2}))
    if slug_tokens:
        keys.add(slug_tokens)

    return keys - {""}


def _score_match(v_title: str, v_url: str, n_title: str, n_url: str) -> float:
    """Return a match score between a vanished and a new series entry."""
    v_keys = _match_keys(v_title, v_url)
    n_keys = _match_keys(n_title, n_url)
    if not v_keys or not n_keys:
        return 0.0

    best = 0.0
    for v_key in v_keys:
        for n_key in n_keys:
            if v_key == n_key:
                return 1.0

            v_tokens = set(v_key.split())
            n_tokens = set(n_key.split())
            if v_tokens and n_tokens:
                overlap = len(v_tokens & n_tokens) / max(len(v_tokens), len(n_tokens))
                best = max(best, overlap)

            seq = difflib.SequenceMatcher(None, v_key, n_key).ratio()
            best = max(best, seq)
    return best


def _match_vanished_to_new(vanished_entries, new_dict):
    """Pair each vanished series with the best matching new series, if any.

    Args:
        vanished_entries: list of (title, reason, url) tuples. A bare
            (title, url) pair is also accepted, so an older report or a
            caller that has no vanish reason cannot break the pairing.
        new_dict: dict title -> series data for newly scraped series.

    Returns:
        list of (vanished_title, vanished_url, new_title, new_url, reason)
        tuples. `reason` here is the match quality -- 'exact', 'strong',
        'weak', or None -- not the vanish reason that came in.
    """
    new_titles = list(new_dict.keys())
    used_new = set()
    matched = []

    for item in vanished_entries:
        if len(item) == 3:
            v_title, _vanish_reason, v_url = item
        else:
            v_title, v_url = item

        best = None
        best_score = 0.0
        best_idx = -1

        for idx, n_title in enumerate(new_titles):
            if idx in used_new:
                continue
            n_data = new_dict[n_title]
            n_url = n_data.get("url", n_data.get("link", ""))
            score = _score_match(v_title, v_url, n_title, n_url)
            if score > best_score:
                best = n_title
                best_score = score
                best_idx = idx

        if best is not None and best_score >= 0.35:
            used_new.add(best_idx)
            n_data = new_dict[best]
            n_url = n_data.get("url", n_data.get("link", ""))
            if best_score >= 0.95:
                reason = "exact"
            elif best_score >= 0.65:
                reason = "strong"
            else:
                reason = "weak"
            matched.append((v_title, v_url, best, n_url, reason))
        else:
            matched.append((v_title, v_url, None, None, None))

    for idx, n_title in enumerate(new_titles):
        if idx not in used_new:
            n_data = new_dict[n_title]
            n_url = n_data.get("url", n_data.get("link", ""))
            matched.append((None, None, n_title, n_url, "extra"))

    return matched


def _format_vanished_new_table(matched):
    """Return (table_lines, extra_lines) for the vanished/new comparison."""
    if not matched:
        return [], []

    paired_rows = []
    extra_rows = []
    for v_title, v_url, n_title, n_url, reason in matched:
        if reason == "extra" or not v_title:
            extra_rows.append((n_title or "", n_url or ""))
            continue
        paired_rows.append((v_title or "", v_url or "", n_title or "", n_url or "", reason or ""))

    if not paired_rows:
        return [], _format_extra_new_series_lines(extra_rows)

    gap = "  │  "
    term_w = max(shutil.get_terminal_size().columns, 80)
    usable = max(term_w - len(gap) - 2, 40)  # 2 leading spaces
    max_col = usable // 2
    left_w = min(
        max(
            max((max(len(t), len(u)) for t, u, _, _, _ in paired_rows), default=0),
            len("Vanished (old)"),
        ),
        max_col,
    )
    right_w = min(
        max(
            max((max(len(t), len(u)) for _, _, t, u, _ in paired_rows), default=0),
            len("New counterpart"),
        ),
        max_col,
    )

    def _trunc(text, width):
        if len(text) <= width:
            return text
        return text[: width - 1] + "…"

    lines = []
    header = f"  {'#':>3}  {'Vanished (old)':<{left_w}}{gap}{'New counterpart':<{right_w}}"
    lines.append(header)
    lines.append(f"  {'─' * 5}{'─' * left_w}{gap}{'─' * right_w}")
    for i, (lt, lu, rt, ru, reason) in enumerate(paired_rows, 1):
        reason_tag = f" [{reason}]" if reason and reason != "exact" else ""
        lines.append(f"  {i:>3}  {_trunc(lt, left_w):<{left_w}}{gap}{_trunc(rt, right_w):<{right_w}}{reason_tag}")
        if lu or ru:
            if lu:
                lines.append(f"       {_trunc(lu, left_w):<{left_w}}{gap}{_trunc(ru, right_w):<{right_w}}")
            else:
                lines.append(f"       {'':<{left_w}}{gap}{_trunc(ru, right_w):<{right_w}}")
        lines.append("")
    total_vanished = sum(1 for v_title, _, _, _, _ in matched if v_title)
    matched_count = len(paired_rows)
    unmatched_count = total_vanished - matched_count
    if total_vanished == 0:
        status = "  No vanished series to match."
    elif unmatched_count == 0:
        status = f"  Matched: {matched_count}/{total_vanished} vanished series (complete ✓)"
    else:
        status = f"  Matched: {matched_count}/{total_vanished} vanished series ({unmatched_count} unmatched ⚠)"
    lines.append(status)
    return lines, _format_extra_new_series_lines(extra_rows)


def _format_extra_new_series_lines(extra_rows):
    """Return printable lines for new series with no vanished counterpart."""
    if not extra_rows:
        return []
    lines = []
    lines.append(f"\n  + {len(extra_rows)} new series not linked to vanished entries:")
    for title, _url in extra_rows:
        lines.append(f"    • {title}")
    return lines


def _save_vanished_series_report(vanished_entries, index_file):
    """Save vanished-series entries to a JSON file for later review.

    Writes to data/vanished_series_report.json alongside the index file.
    """
    if not index_file or not vanished_entries:
        return
    try:
        report_path = os.path.join(os.path.dirname(index_file), "vanished_series_report.json")
        report = {
            "generated": datetime.now().isoformat(),
            "count": len(vanished_entries),
            "entries": [{"title": title, "reason": reason, "url": url} for title, reason, url in vanished_entries],
        }
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"  📄 Vanished series report saved: {report_path}")
        logger.info(
            "Vanished series report saved with %d entries to %s",
            len(vanished_entries),
            report_path,
        )
    except Exception as exc:
        logger.warning("Failed to save vanished series report: %s", exc)


# Tabs are released in confirmed batches of this size. Every URL opened here
# becomes a real window in the user's browser, and a bad catalogue fetch can
# put thousands of entries on the vanished list.
_BROWSER_TAB_BATCH = 20


def _open_urls_for_comparison(old_url: str, new_url: str) -> int:
    """Open old and new series URLs in the default browser.

    Opens the new URL first (it is the current one) and then the old URL
    so the user can compare them side-by-side. Returns the number opened.
    """
    urls = [u for u in (new_url, old_url) if u]
    if not urls:
        print("  ⚠ No URLs available to open.")
        return 0
    print("  Opening browser tabs...")
    opened = 0
    for url in urls:
        try:
            webbrowser.open(url, new=2)
            print(f"    → {url}")
            opened += 1
        except Exception as exc:
            logger.warning("Failed to open %s: %s", url, exc)
            print(f"    ✗ Could not open: {url}")
    return opened


def _open_rows_in_browser(rows: list) -> int:
    """Open every row's pair of URLs, in confirmed batches.

    These are real browser windows, not fetches, so the cost lands on the
    user's desktop rather than on the scrape: opening one tab per URL across a
    long vanished list is how you lock up a machine. The total is stated first
    and the tabs are released in batches that have to be confirmed.

    Returns the number of tabs opened.
    """
    tab_count = sum(1 for row in rows for url in (row["v_url"], row["n_url"]) if url)
    if not tab_count:
        print("  ⚠ No URLs available to open.")
        return 0

    print(f"\n  This opens {tab_count} browser tab(s) for {len(rows)} entry(s).")
    if (input("  Continue? (y/n) [n]: ").strip().lower() or "n") != "y":
        print("  → Cancelled; no tabs opened.")
        return 0

    opened = 0
    since_pause = 0
    for row in rows:
        if since_pause >= _BROWSER_TAB_BATCH:
            answer = input(f"  {opened} tab(s) open, {tab_count - opened} to go. Continue? (y/n) [n]: ")
            if (answer.strip().lower() or "n") != "y":
                print(f"  → Stopped after {opened} tab(s).")
                return opened
            since_pause = 0
        just_opened = _open_urls_for_comparison(row["v_url"], row["n_url"])
        opened += just_opened
        since_pause += just_opened
    return opened


def _rescrape_rows(rows: list, scraper, old_data: dict) -> int:
    """Re-verify rows' old and new URLs live, updating them in place.

    Every row goes out in a single verification call, so the whole batch costs
    one sign-in and one connection pool rather than one of each per row. The
    results come back in input order, which is what pairs them back to rows.

    Only trusts a fetched title when the page was actually reached:
    verify_vanished_and_candidates returns every entry it was handed, with a
    reachability flag, so a non-empty result says nothing on its own — the
    flag is what separates "still there under a new name" from "really gone".

    Returns how many rows were updated from a reachable page.
    """
    if not scraper:
        for row in rows:
            print(f"  ⚠ Cannot rescrape {row['v_title']}: no scraper available.")
        return 0

    actionable = []
    for row in rows:
        if row["v_url"]:
            actionable.append(row)
        else:
            print(f"  ⚠ Cannot rescrape {row['v_title']}: no old URL available.")
    if not actionable:
        return 0

    vanished = [(row["v_title"], row["v_url"]) for row in actionable]
    candidate_rows = [row for row in actionable if row["new_entry"]]
    candidates = [row["new_entry"] for row in candidate_rows]

    if len(vanished) == 1:
        print(f"\n  → Re-scraping: {vanished[0][1]}")
    else:
        print(f"\n  → Re-scraping {len(vanished)} old URLs in one pass (one sign-in)...")

    try:
        # asyncio.run is correct here rather than an await: this prompt is only
        # reached from the synchronous CLI path (main._run_scrape_and_save ->
        # show_vanished_series), so no event loop is running. If it ever moves
        # under an async caller, this and the prompt itself have to become async.
        verified_vanished, verified_candidates = asyncio.run(
            scraper.verify_vanished_and_candidates(vanished, candidates)
        )
    except Exception as exc:
        logger.warning("Live re-scrape of %d URL(s) failed: %s", len(vanished), exc)
        print(f"  ✗ Re-scrape failed: {exc}")
        return 0

    # One result per entry is the contract; a short list would silently pair a
    # row with another row's verdict, which is worse than reporting nothing.
    if len(verified_vanished) != len(actionable) or len(verified_candidates) != len(candidate_rows):
        logger.warning(
            "Verification returned %d/%d vanished and %d/%d candidates; skipping row updates",
            len(verified_vanished),
            len(actionable),
            len(verified_candidates),
            len(candidate_rows),
        )
        print("  ✗ Re-scrape returned an unexpected number of results; nothing was changed.")
        return 0

    updated = set()
    for row, (new_v_title, new_v_url, reachable) in zip(actionable, verified_vanished, strict=True):
        v_title = row["v_title"]
        if reachable:
            row["v_title"] = new_v_title
            row["v_url"] = new_v_url or row["v_url"]
            row["old_entry"] = old_data.get(new_v_title, row["old_entry"])
            print(f"  ✓ {v_title}: old URL still reachable. Title now: {new_v_title}")
            updated.add(id(row))
        else:
            print(f"  ✗ {v_title}: old URL not reachable — the series really is gone.")

    for row, verified_new in zip(candidate_rows, verified_candidates, strict=True):
        v_title = row["v_title"]
        if verified_new.get("_verified_reachable"):
            row["n_title"] = verified_new.get("title", row["n_title"])
            row["n_url"] = verified_new.get("url", verified_new.get("link", row["n_url"]))
            row["new_entry"] = verified_new
            print(f"  ✓ {v_title}: new candidate verified: {row['n_title']} @ {row['n_url']}")
            updated.add(id(row))
        else:
            error = verified_new.get("_verified_error")
            suffix = f" ({error})" if error else ""
            print(f"  ✗ {v_title}: new candidate could not be verified{suffix}.")

    return len(updated)


def _rescrape_row(row: dict, scraper, old_data: dict) -> bool:
    """Re-verify a single row. Returns True when it was updated."""
    return _rescrape_rows([row], scraper, old_data) > 0


def _series_progress_line(entry: dict) -> str:
    """Return the same progress summary the scraper uses per series.

    Template when sub/wl are available:
      [seasons]: watched/total watched (Sub:{✓|✗} WL:{✓|✗})
    Template when they are not (e.g. BS.to):
      [seasons]: watched/total watched
    """
    total_seasons = entry.get("total_seasons", 0) or len(entry.get("seasons", []))
    total_eps = entry.get("total_episodes", 0)
    watched_eps = entry.get("watched_episodes", 0)
    if not total_eps and entry.get("seasons"):
        total_eps, watched_eps = get_episode_counts(entry)
    base = f"[{total_seasons}]: {watched_eps}/{total_eps} watched"
    has_sub = "subscribed" in entry
    has_wl = "watchlist" in entry
    if has_sub or has_wl:
        sub = "✓" if entry.get("subscribed") else "✗"
        wl = "✓" if entry.get("watchlist") else "✗"
        base += f" (Sub:{sub} WL:{wl})"
    return base


def _status_diff_line(old_entry: dict, new_entry: dict) -> str | None:
    """Return a compact status-difference string, or None if identical.

    Empty/missing new entry returns None. Sites without sub/wl fields
    (e.g. BS.to) only compare watched/total episode counts.
    """
    if not new_entry:
        return None

    def _counts(entry):
        total = entry.get("total_episodes", 0)
        watched = entry.get("watched_episodes", 0)
        if not total and entry.get("seasons"):
            total, watched = get_episode_counts(entry)
        return total, watched

    old_total, old_watched = _counts(old_entry)
    new_total, new_watched = _counts(new_entry)

    old_sub = old_entry.get("subscribed")
    new_sub = new_entry.get("subscribed")
    old_wl = old_entry.get("watchlist")
    new_wl = new_entry.get("watchlist")

    has_sub = old_sub is not None or new_sub is not None
    has_wl = old_wl is not None or new_wl is not None

    identical = (
        old_watched == new_watched
        and old_total == new_total
        and (not has_sub or bool(old_sub) == bool(new_sub))
        and (not has_wl or bool(old_wl) == bool(new_wl))
    )
    if identical:
        return None

    parts = []
    if has_sub and bool(old_sub) != bool(new_sub):
        parts.append(f"Sub {'✓' if old_sub else '✗'}→{'✓' if new_sub else '✗'}")
    if has_wl and bool(old_wl) != bool(new_wl):
        parts.append(f"WL {'✓' if old_wl else '✗'}→{'✓' if new_wl else '✗'}")
    if old_watched != new_watched or old_total != new_total:
        parts.append(f"W:{old_watched}/{old_total}→{new_watched}/{new_total}")

    return "⚠ Status differs: " + "  ".join(parts)


def _prompt_vanished_table(vanished_entries, new_dict, old_data, scraper=None):
    """Show a side-by-side decision table for vanished vs. new series.

    For each vanished entry the user can choose:
      y = delete old entry (same as d)
      n = keep old entry (same as k)
      k = keep old entry
      d = delete old entry
      a <action> = apply the action to all remaining rows (e.g. "a d")
      r = re-scrape the old URL to verify it live (updates candidate info)
      o = open old + new URLs in browser to compare visually
      s = skip all remaining entries (keep them)

    Args:
        vanished_entries: list of (title, reason, url) tuples for vanished series.
        new_dict: dict title -> series data for newly scraped series.
        old_data: dict title -> series data for the current index.
        scraper: optional scraper instance for live re-scraping of old URLs.

    Returns:
        list of titles confirmed for deletion.
    """
    matched = _match_vanished_to_new(vanished_entries, new_dict)
    to_delete = []
    apply_to_all = None  # action to apply to all remaining rows
    skip_all = False

    print("\n  Compare each vanished series with its best matching new counterpart.")
    print(
        "  Actions per row: [y]es=delete  [n]o=keep  [k]eep  [d]elete  "
        "[a <action>]=all  [r]escrape  [o]pen URLs  [s]kip all"
    )
    print()

    # Compute column widths from actual content so every line aligns.
    term_w = shutil.get_terminal_size().columns
    max_content_w = max(35, term_w // 2 - 8)
    left_items = []
    right_items = []
    for v_title, v_url, n_title, n_url, _reason in matched:
        if v_title is None:
            continue
        left_entry = old_data.get(v_title, {})
        right_entry = new_dict.get(n_title, {}) if n_title else {}
        left_items.append(v_title)
        left_items.append(v_url or "")
        left_items.append(_series_progress_line(left_entry))
        diff = _status_diff_line(left_entry, right_entry)
        if diff:
            left_items.append(diff)
        right_items.append(n_title or "—")
        right_items.append(n_url or "")
        right_items.append(_series_progress_line(right_entry) if right_entry else "")
    content_w = min(
        max_content_w,
        max(
            max((len(t) for t in left_items), default=0),
            max((len(t) for t in right_items), default=0),
            len("Old (index)"),
            len("New (site)"),
        ),
    )
    sep = " │ "
    match_w = 7
    status_w = 12
    indent = "  "
    num_w = 3

    def _trunc(text: str, width: int) -> str:
        if len(text) <= width:
            return text
        return text[: width - 1] + "…"

    def _cell(text: str, width: int) -> str:
        return _trunc(text, width).ljust(width)

    def _line(idx: str, left: str, right: str, match: str = "", status: str = "", rule: str = "") -> str:
        if rule:
            cells = sep.join(("─" * content_w, "─" * content_w, "─" * match_w, "─" * status_w))
            return f"{indent}{rule}{'─' * (num_w + 1)}{cells}"
        match_field = _cell(match, match_w)
        status_field = _cell(status, status_w)
        cells = sep.join((_cell(left, content_w), _cell(right, content_w), match_field, status_field))
        return f"{indent}{idx:>{num_w}}  {cells}"

    def _print_row(
        i: int, v_title: str, v_url: str, old_entry: dict, n_title: str, n_url: str, new_entry: dict, reason: str
    ) -> None:
        right_title = n_title or "—"
        status = _status_diff_line(old_entry, new_entry)
        if not new_entry:
            status_tag = "no match"
        elif status:
            status_tag = "differs ⚠"
        else:
            status_tag = "identical ✓"
        print(_line(str(i), v_title, right_title, match=reason, status=status_tag))
        print(_line("", v_url or "", n_url or ""))
        print(_line("", _series_progress_line(old_entry), _series_progress_line(new_entry) if new_entry else ""))
        if status:
            print(_line("", status, ""))
        print()

    header = sep.join(
        (
            f"{'Old (index)':<{content_w}}",
            f"{'New (site)':<{content_w}}",
            f"{'Match':<{match_w}}",
            f"{'Status':<{status_w}}",
        )
    )
    print(f"{indent}  {'#':>{num_w}}  {header}")
    print(_line("", "", "", rule="─"))

    # Pre-render rows so indices are stable even after live re-scraping
    rows = []
    for v_title, v_url, n_title, n_url, reason in matched:
        if v_title is None:
            # Extra new series with no vanished counterpart — not actionable here
            continue
        old_entry = old_data.get(v_title, {})
        new_entry = new_dict.get(n_title, {}) if n_title else {}
        rows.append(
            {
                "v_title": v_title,
                "v_url": v_url,
                "old_entry": old_entry,
                "n_title": n_title,
                "n_url": n_url,
                "new_entry": new_entry,
                "reason": reason or "none",
            }
        )

    for i, row in enumerate(rows, 1):
        if skip_all or apply_to_all is not None:
            break

        current_idx = i - 1
        v_title = row["v_title"]
        v_url = row["v_url"]
        old_entry = row["old_entry"]
        n_title = row["n_title"]
        n_url = row["n_url"]
        new_entry = row["new_entry"]
        reason = row["reason"]

        # Print current row
        _print_row(i, v_title, v_url, old_entry, n_title, n_url, new_entry, reason)

        while True:
            prompt = (
                f'  [{i}/{len(rows)}] Action for "{v_title}"? '
                f"(y=delete n=keep k=keep d=delete r=rescrape o=open a <action>=all s=skip all) [n]: "
            )
            try:
                choice = input(prompt).strip().lower() or "n"
            except EOFError:
                # No one is there to answer -- a piped or redirected run. The
                # loop below re-prompts on anything it does not recognise, so
                # without this an unattended run spins forever on a closed
                # stdin. Keeping every entry is the reversible answer.
                print("  -> No input available; keeping all remaining entries.")
                skip_all = True
                break

            if choice == "s":
                skip_all = True
                print("  → Skipping all remaining vanished entries.")
                break

            if choice.startswith("a "):
                apply_to_all = choice[2:].strip()
                if apply_to_all not in {"y", "n", "k", "d", "r", "o"}:
                    print(f"  ⚠ Unknown apply-to-all action '{apply_to_all}'. Use y/n/k/d/r/o.")
                    apply_to_all = None
                    continue
                print(f"  → Apply '{apply_to_all}' to all {len(rows) - i + 1} remaining entries.")
                # Apply to the current row immediately; the rest are handled after the loop.
                if apply_to_all in ("y", "d"):
                    # Deleting "all" is the one irreversible keystroke in this
                    # loop: it drops every remaining entry without showing them.
                    # A bad catalogue fetch can put thousands of perfectly good
                    # series on this list, so the count has to be stated and
                    # confirmed before it runs.
                    remaining_count = len(rows) - i + 1
                    print(f"\n  [WARN] This deletes {remaining_count} series from the index, including this one.")
                    print("  Deleted entries lose their stored watch history.")
                    typed = input(f"  Type 'DELETE {remaining_count}' to confirm: ").strip()
                    if typed != f"DELETE {remaining_count}":
                        print("  → Not confirmed; nothing deleted. Back to this entry.")
                        apply_to_all = None
                        continue
                    total_eps = old_entry.get("total_episodes", 0)
                    watched_eps = old_entry.get("watched_episodes", 0)
                    if not total_eps and old_entry.get("seasons"):
                        total_eps, watched_eps = get_episode_counts(old_entry)
                    if watched_eps and total_eps:
                        print(f"  ⚠ {v_title} has watched progress: {watched_eps}/{total_eps} episodes.")
                    to_delete.append(v_title)
                    print("  → Marked for deletion.")
                elif apply_to_all == "o":
                    _open_urls_for_comparison(v_url, n_url)
                elif apply_to_all == "r":
                    _rescrape_row(row, scraper, old_data)
                else:
                    print("  → Kept in index.")
                break

            if choice in ("k", "n"):
                print("  → Kept in index.")
                break

            if choice in ("d", "y"):
                # If the old entry had progress, warn the user.
                total_eps = old_entry.get("total_episodes", 0)
                watched_eps = old_entry.get("watched_episodes", 0)
                if not total_eps and old_entry.get("seasons"):
                    total_eps, watched_eps = get_episode_counts(old_entry)
                if watched_eps and total_eps:
                    print(f"\n  ⚠ This entry has watched progress: {watched_eps}/{total_eps} episodes.")
                    print("    Make sure the new entry on the site reflects the same progress,")
                    print("    otherwise the next scrape may report those episodes as unwatched.")
                if choice == "d":
                    confirm = input(f'  Confirm delete "{v_title}"? (y/n) [n]: ').strip().lower() or "n"
                else:
                    confirm = "y"
                if confirm == "y":
                    to_delete.append(v_title)
                    print("  → Marked for deletion.")
                else:
                    print("  → Not deleted.")
                break

            if choice == "o":
                _open_urls_for_comparison(v_url, n_url)
                continue

            if choice == "r":
                _rescrape_row(row, scraper, old_data)

                # Re-print the row, which the rescrape may have updated
                v_title = row["v_title"]
                v_url = row["v_url"]
                old_entry = row["old_entry"]
                n_title = row["n_title"]
                n_url = row["n_url"]
                new_entry = row["new_entry"]
                print("\n  Updated row:")
                _print_row(i, v_title, v_url, old_entry, n_title, n_url, new_entry, reason)
                continue

            print("  ⚠ Unknown choice. Use y/n/k/d/r/o/a/s.")

    # Apply the chosen action to all remaining rows
    if apply_to_all is not None:
        remaining = rows[current_idx + 1 :] if "current_idx" in locals() else rows
        action = apply_to_all
        if action in ("y", "d"):
            print(f"\n  Applying '{action}' to {len(remaining)} remaining entries...")
            for row in remaining:
                v_title = row["v_title"]
                old_entry = row["old_entry"]
                total_eps = old_entry.get("total_episodes", 0)
                watched_eps = old_entry.get("watched_episodes", 0)
                if not total_eps and old_entry.get("seasons"):
                    total_eps, watched_eps = get_episode_counts(old_entry)
                if watched_eps and total_eps:
                    print(f"  ⚠ {v_title} has watched progress: {watched_eps}/{total_eps} episodes.")
                to_delete.append(v_title)
            print(f"  → Marked {len(remaining)} entries for deletion.")
        elif action == "o":
            _open_rows_in_browser(remaining)
        elif action == "r":
            _rescrape_rows(remaining, scraper, old_data)
        else:
            print(f"\n  Kept all {len(remaining)} remaining entries in the index.")

    return to_delete


def show_vanished_series(old_data, all_discovered_slugs, scrape_scope, index_file=None, new_data=None, scraper=None):
    """Detect indexed series not found in the current scrape.

    Shows vanished series and prompts the user to delete each one.
    If index_file is provided, confirmed deletions are removed from disk.

    Args:
        scraper: optional scraper instance used to re-verify vanished and
            candidate URLs before matching.

    Returns:
        list of vanished (title, reason) tuples that were kept
    """
    if scrape_scope not in ("all", "new_only", "watchlist", "subscribed", "both"):
        return []

    vanished = []
    corrupt_entries = []

    for title, entry in old_data.items():
        slug = _extract_slug_from_field(entry.get("link", ""))
        if slug is None:
            slug = _extract_slug_from_field(entry.get("url", ""))
            if slug is not None:
                logger.warning("Used URL fallback for slug extraction: %s", title)
            else:
                corrupt_entries.append(title)
                continue

        if slug in all_discovered_slugs:
            continue

        is_sub = entry.get("subscribed", False)
        is_wl = entry.get("watchlist", False)
        url = entry.get("url", entry.get("link", ""))

        if scrape_scope in ("all", "new_only"):
            vanished.append((title, "not found on aniworld.to", url))
        elif scrape_scope == "watchlist":
            if is_wl:
                vanished.append((title, "was on watchlist", url))
        elif scrape_scope == "subscribed":
            if is_sub:
                vanished.append((title, "was subscribed", url))
        elif scrape_scope == "both":
            if is_sub and is_wl:
                vanished.append(
                    (
                        title,
                        "was subscribed + on watchlist — possibly deleted from aniworld.to",
                        url,
                    )
                )
            elif is_sub:
                vanished.append((title, "was subscribed", url))
            elif is_wl:
                vanished.append((title, "was on watchlist", url))

    if corrupt_entries:
        print(f"\n⚠ {len(corrupt_entries)} index entry(s) have corrupt/missing URL data:")
        for t in corrupt_entries[:10]:
            print(f"  • {t}")
        if len(corrupt_entries) > 10:
            print(f"  ... and {len(corrupt_entries) - 10} more")
        print("  These entries were skipped during vanished-series detection.")
        logger.warning(
            "Corrupt URL data in %d index entries: %s",
            len(corrupt_entries),
            corrupt_entries[:5],
        )

    if vanished:
        separator = "─" * 70
        print(f"\n{separator}")
        print(f"  [INFO] {len(vanished)} previously indexed series NOT found in current scrape:")
        print(separator)

        # Save mismatched entries to JSON for later review
        _save_vanished_series_report(vanished, index_file)

        # Only offer deletion for full catalog scopes
        if scrape_scope in ("all", "new_only"):
            # Optional live verification of vanished/rename URLs for accuracy
            if new_data is not None and scraper is not None:
                old_titles = set(old_data.keys())
                if isinstance(new_data, list):
                    candidate_entries = [s for s in new_data if s.get("title") and s.get("title") not in old_titles]
                else:
                    candidate_entries = [
                        s for s in new_data.values() if s.get("title") and s.get("title") not in old_titles
                    ]
                if candidate_entries:
                    try:
                        ask = (
                            input(
                                f"\n{len(vanished)} vanished series found; "
                                f"{len(candidate_entries)} new series could be renames. "
                                "Re-scrape all candidate URLs for verification? (y/n): "
                            )
                            .strip()
                            .lower()
                        )
                    except EOFError:
                        # Piped or redirected run. Skipping the live re-verification costs
                        # accuracy, not data -- the decision table below has its own
                        # closed-stdin guard and keeps every entry. Letting this raise
                        # would instead kill the run just before the results are saved.
                        print("  -> No input available; skipping live re-verification.")
                        ask = "n"
                    if ask == "y":
                        _, verified_new_data = asyncio.run(
                            scraper.verify_vanished_and_candidates(vanished, candidate_entries)
                        )
                        new_data = verified_new_data

            # Show new series alongside so user can spot renames before deciding
            if new_data is not None:
                old_titles = set(old_data.keys())
                if isinstance(new_data, list):
                    new_dict = {s.get("title"): s for s in new_data if s.get("title")}
                else:
                    new_dict = dict(new_data)
                incoming_new = [t for t in new_dict if t and t not in old_titles]
                if incoming_new:
                    matched = _match_vanished_to_new(vanished, new_dict)
                    table_lines, extra_lines = _format_vanished_new_table(matched)
                    for line in table_lines:
                        print(line)
                    for line in extra_lines:
                        print(line)
                    print(
                        f"\n  Compare {len(vanished)} vanished series with "
                        "their possible new counterparts above. "
                        "Use the interactive prompts below to delete old entries."
                    )
                else:
                    for i, (title, reason, url) in enumerate(vanished, 1):
                        print(f"  {i}. {title}  ({reason})")
                        print(f"      old: {url}")
                    print(separator)
            else:
                for i, (title, reason, url) in enumerate(vanished, 1):
                    print(f"  {i}. {title}  ({reason})")
                    print(f"      old: {url}")
                print(separator)

            new_dict_for_prompt = new_dict if new_data is not None else {}
            to_delete = _prompt_vanished_table(vanished, new_dict_for_prompt, old_data, scraper=scraper)

            if to_delete and index_file:
                removed = remove_series_from_index(index_file, to_delete)
                print(f"  ✓ Removed {removed} series from index.")
            elif to_delete:
                print(f"  ⚠ {len(to_delete)} series marked for deletion but no index_file provided.")
            else:
                print("  ✓ No series removed — all vanished entries preserved.")

            logger.info(
                "Vanished series: %d not found in scope '%s', %d deleted by user",
                len(vanished),
                scrape_scope,
                len(to_delete),
            )

            delete_set = set(to_delete)
            return [(title, reason) for title, reason, _ in vanished if title not in delete_set]

        # For account scopes (subscribed/watchlist/both), informational only
        print("  These entries reflect account status changes, not site removals.")
        logger.info(
            "Vanished series notification: %d series not found in scrape scope '%s'",
            len(vanished),
            scrape_scope,
        )

    return [(title, reason) for title, reason, _ in vanished] if vanished else []


def confirm_and_save_changes(new_data, description, index_manager, active_site_url=None):
    """Show changes, ask for separate watched/unwatched confirmation, merge, and save.

    Returns:
        True: Changes saved successfully
        False: Merge cancelled
        dict: Rescrape workflow triggered (with keys: 'rescrape', 'urls', 'titles')
    """
    old_data = dict(index_manager.series_index)

    new_dict = {s.get("title"): s for s in new_data if s.get("title")} if isinstance(new_data, list) else dict(new_data)

    changes = detect_changes(old_data, new_dict)
    logger.info("Detected changes: %s", {k: len(v) for k, v in changes.items()})

    allowed = _prompt_change_confirmations(changes, new_dict)

    if not allowed.get("new_series", True):
        changes["new_series"] = []
    if not allowed.get("new_episodes", True):
        changes["new_episodes"] = []
    if not allowed["watched"]:
        changes["newly_watched"] = []
    if not allowed["unwatched"]:
        changes["newly_unwatched"] = []
    if not allowed["subscribe"]:
        changes["newly_subscribed"] = []
    if not allowed["unsubscribe"]:
        changes["newly_unsubscribed"] = []
    if not allowed["watchlist_add"]:
        changes["watchlist_added"] = []
    if not allowed["watchlist_remove"]:
        changes["watchlist_removed"] = []
    if not allowed["title_ger"]:
        changes["title_ger_changed"] = []
    if not allowed["title_eng"]:
        changes["title_eng_changed"] = []
    if not allowed.get("episode_remove", False):
        changes["removed_episodes"] = []
    if not allowed["season_remove"]:
        changes["removed_seasons"] = []

    # Check for episode count mismatches before merging
    pending_rescrape = None
    mismatches = _detect_episode_count_mismatches(old_data, new_dict)
    if mismatches:
        proceed, rescrape_data = _prompt_episode_mismatches(mismatches, old_data, active_site_url=active_site_url)
        if rescrape_data:
            # Held, not returned. Returning here used to skip the merge
            # entirely, so every approval the user had just given for every
            # *other* series in the run was discarded with it -- the sibling
            # s.to run that found this lost an approved 7->12 watch change and
            # re-prompted for it on the next two scrapes. The rescrape
            # concerns one series; the rest of the run still has to be saved,
            # so this is handed back at the end instead, after the merge.
            pending_rescrape = {
                "rescrape": True,
                "urls": rescrape_data["urls"],
                "titles": rescrape_data["titles"],
            }
        elif not proceed:
            print("✗ Merge cancelled due to episode count mismatches.")
            return False

    # Detect housekeeping changes BEFORE merge (merge mutates old_data in place)
    housekeeping = _detect_housekeeping_changes(old_data, new_dict)

    merged = _build_merged_data(old_data, new_dict, allowed)

    main_changes = sum(
        len(v)
        for k, v in changes.items()
        if k
        not in (
            "newly_unwatched",
            "newly_subscribed",
            "newly_unsubscribed",
            "watchlist_added",
            "watchlist_removed",
        )
    )
    if allowed["unwatched"]:
        main_changes += len(changes["newly_unwatched"])
    if allowed["subscribe"]:
        main_changes += len(changes["newly_subscribed"])
    if allowed["unsubscribe"]:
        main_changes += len(changes["newly_unsubscribed"])
    if allowed["watchlist_add"]:
        main_changes += len(changes["watchlist_added"])
    if allowed["watchlist_remove"]:
        main_changes += len(changes["watchlist_removed"])
    if main_changes == 0:
        has_housekeeping = housekeeping["added"] or housekeeping["removed"]
        if has_housekeeping:
            total = sum(len(v) for v in housekeeping["added"].values()) + sum(
                len(v) for v in housekeeping["removed"].values()
            )
            print(f"\n⚠ {total} ignored episode 0 change(s):")
            print(f"{'─' * 70}")
            if housekeeping["added"]:
                print("  Remove ep0 & mark ignored:")
                for title in sorted(housekeeping["added"]):
                    seasons = ", ".join(housekeeping["added"][title])
                    series = new_dict.get(title, {})
                    watched = series.get("watched_episodes", 0)
                    total_ep = series.get("total_episodes", 0)
                    sub = series.get("subscribed")
                    wl = series.get("watchlist")
                    parts = []
                    if sub is not None:
                        parts.append(f"Sub:{'✓' if sub else '✗'}")
                    if wl is not None:
                        parts.append(f"WL:{'✓' if wl else '✗'}")
                    sub_info = f" ({' '.join(parts)})" if parts else ""
                    print(f"    • {title}  [{seasons}]: {watched}/{total_ep} watched{sub_info}")
            if housekeeping["removed"]:
                print("  Unmark ignored (ep0 no longer present):")
                for title in sorted(housekeeping["removed"]):
                    seasons = ", ".join(housekeeping["removed"][title])
                    series = new_dict.get(title, {})
                    watched = series.get("watched_episodes", 0)
                    total_ep = series.get("total_episodes", 0)
                    sub = series.get("subscribed")
                    wl = series.get("watchlist")
                    parts = []
                    if sub is not None:
                        parts.append(f"Sub:{'✓' if sub else '✗'}")
                    if wl is not None:
                        parts.append(f"WL:{'✓' if wl else '✗'}")
                    sub_info = f" ({' '.join(parts)})" if parts else ""
                    print(f"    • {title}  [{seasons}]: {watched}/{total_ep} watched{sub_info}")
            print(f"{'─' * 70}")
            if input("Apply these changes? (y/n): ").strip().lower() != "y":
                print("✗ Changes discarded.")
                return False
            index_manager.series_index = merged
            index_manager.save_index()
            print(f"✓ Saved {len(merged)} series to index")
            return pending_rescrape or True

        print(f"\n✓ {description} already up to date.")
        logger.info("No changes to save for %s.", description)
        return pending_rescrape or True

    show_changes(
        changes,
        include_unwatched=allowed["unwatched"],
        include_watched=allowed["watched"],
        include_subscribe=allowed["subscribe"],
        include_unsubscribe=allowed["unsubscribe"],
        include_watchlist_add=allowed["watchlist_add"],
        include_watchlist_remove=allowed["watchlist_remove"],
        new_data=new_dict,
    )

    response = input("\nSave these changes? (y/n): ").strip().lower()
    if response != "y":
        print("✗ Changes discarded. Nothing saved.")
        logger.info("User discarded changes. Nothing saved.")
        return False

    index_manager.series_index = merged
    index_manager.save_index()
    print(f"✓ Saved {len(merged)} series to index")
    logger.info("Saved %d series to index", len(merged))
    return pending_rescrape or True
