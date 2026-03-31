"""
AniWorld.to Index Manager (HTTPX version)
Manages the persistent series index and handles data merging, change detection, and analytics.
Adapted from aniworld.to backup's index_manager3.py for the httpx scraper.
"""

import copy
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


def _is_pid_alive(pid):
    """Check if a process with the given PID is still running (cross-platform)."""
    try:
        if sys.platform == 'win32':
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}', '/NH'],
                capture_output=True, check=False, text=True,
                encoding='utf-8', errors='replace'
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (OSError, ValueError):
        return False


# Pre-compiled regex for season number extraction
_SEASON_NUMBER_RE = re.compile(r'(staffel|season|s)\s*(\d+)', re.IGNORECASE)

# Pre-compiled regex for valid aniworld.to series URL/path
_VALID_SERIES_URL_RE = re.compile(r'^https://aniworld\.to/anime/stream/[^/]+/?$')
_VALID_SERIES_PATH_RE = re.compile(r'^/anime/stream/[^/]+/?$')


def _is_valid_series_url(url):
    """Check if a URL is a valid aniworld.to series URL or relative path.

    Rejects dangerous schemes (javascript:, data:, file://) and
    only allows https://aniworld.to/anime/stream/... or /anime/stream/... paths.
    """
    if not url or not isinstance(url, str):
        return False
    return bool(_VALID_SERIES_URL_RE.match(url) or _VALID_SERIES_PATH_RE.match(url))


class FileLock:
    """Simple file-based lock for preventing concurrent access to critical files.

    Uses a .lock file to indicate exclusive access. Waits for lock with timeout.
    Works cross-platform (Windows, Linux, Mac).
    """
    def __init__(self, filepath, timeout=10, poll_interval=0.1):
        self.filepath = filepath
        self.lock_file = filepath + '.lock'
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.lock_acquired = False

    def acquire(self):
        """Acquire lock, waiting up to timeout seconds."""
        start = time.time()
        while time.time() - start < self.timeout:
            try:
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n".encode())
                os.close(fd)
                self.lock_acquired = True
                return True
            except (OSError, FileExistsError):
                time.sleep(self.poll_interval)

        if self._is_lock_stale():
            logger.warning(f"Removing stale lock on {self.filepath} (owner process dead)")
            try:
                os.remove(self.lock_file)
            except OSError:
                pass
            try:
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n".encode())
                os.close(fd)
                self.lock_acquired = True
                return True
            except (OSError, FileExistsError):
                pass

        logger.warning(f"Could not acquire lock on {self.filepath} after {self.timeout}s")
        return False

    def _is_lock_stale(self):
        """Check if the lock file was left by a process that is no longer running."""
        try:
            with open(self.lock_file, 'r') as f:
                pid_str = f.read().strip()
            pid = int(pid_str)
            return not _is_pid_alive(pid)
        except (OSError, ValueError):
            return True

    def release(self):
        """Release lock by removing lock file."""
        if self.lock_acquired:
            try:
                os.remove(self.lock_file)
                self.lock_acquired = False
            except OSError:
                pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()


def _create_file_backup(filepath):
    """Create a backup of a file (up to 3 generations kept)."""
    if not os.path.exists(filepath):
        return
    try:
        backup_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        for i in range(3, 10):
            old_backup = os.path.join(backup_dir, f"{filename}.bak{i}")
            if os.path.exists(old_backup):
                try:
                    os.remove(old_backup)
                except OSError:
                    pass

        for i in range(2, 0, -1):
            src = os.path.join(backup_dir, f"{filename}.bak{i}")
            dst = os.path.join(backup_dir, f"{filename}.bak{i+1}")
            if os.path.exists(src):
                try:
                    shutil.move(src, dst)
                except OSError:
                    pass

        backup_path = os.path.join(backup_dir, f"{filename}.bak1")
        shutil.copy2(filepath, backup_path)
        logger.debug(f"Created backup: {backup_path}")
    except Exception as e:
        logger.warning(f"Could not create backup of {filepath}: {e}")


def _atomic_write_json(filepath, data):
    """Write JSON to file atomically via temp file + os.replace."""
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)

    if os.path.exists(filepath):
        _create_file_backup(filepath)

    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _validate_series_entry(series, title=''):
    """Validate that a series entry has the required structure. Returns True if valid."""
    if not isinstance(series, dict):
        logger.warning(f"Skipping invalid series entry (not dict): {title}")
        return False
    url = series.get('url', '')
    if not url:
        logger.warning(f"Skipping series '{title}' - missing 'url' field")
        return False
    if not _is_valid_series_url(url):
        logger.warning(f"Skipping series '{title}' - invalid URL scheme/format: {url[:80]}")
        return False
    seasons = series.get('seasons')
    if seasons is not None and not isinstance(seasons, list):
        logger.warning(f"Skipping series '{title}' - 'seasons' must be list, got {type(seasons)}")
        return False
    for season in (seasons or []):
        if not isinstance(season, dict):
            continue
        episodes = season.get('episodes')
        if episodes is not None and not isinstance(episodes, list):
            logger.error(f"Rejecting series '{title}' — season '{season.get('season', '?')}' has CORRUPT episodes (type={type(episodes).__name__}, expected list)")
            return False
    return True


def _find_series(new_data, title):
    """Look up a series by title in either a dict or list."""
    if isinstance(new_data, dict):
        return new_data.get(title)
    if isinstance(new_data, list):
        return next((s for s in new_data if s.get('title') == title), None)
    return None


def _get_season_stats(series, season_label):
    """Get (total_episodes, watched_episodes) for a specific season."""
    if not series:
        return 0, 0
    for s in series.get('seasons', []):
        if s.get('season') == season_label:
            eps = s.get('episodes', [])
            return len(eps), sum(1 for ep in eps if ep.get('watched', False))
    return 0, 0


def get_episode_counts(series):
    """Get (total_episodes, watched_episodes) across all seasons of a series."""
    total = 0
    watched = 0
    for season in series.get('seasons', []):
        eps = season.get('episodes', [])
        if season.get('ignored_episode_0'):
            eps = [ep for ep in eps if ep.get('number') != 0]
        total += len(eps)
        watched += sum(1 for ep in eps if ep.get('watched', False))
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
            if choice == 'q':
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


def group_episodes_by_season(episode_list, new_data, prefix='[+]'):
    """Group episodes by series and season, showing count per season."""
    grouped = defaultdict(list)
    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)

    if isinstance(new_data, list):
        new_data_dict = {s.get('title'): s for s in new_data}
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
        old_map = {s.get('title'): s for s in old_data if s and s.get('title')}
    else:
        old_map = dict(old_data) if old_data else {}

    added = {}   # title -> [season_labels]
    removed = {} # title -> [season_labels]
    for title, new_entry in new_dict.items():
        o_entry = old_map.get(title)
        if not o_entry:
            continue
        old_seasons = {s.get('season'): s for s in o_entry.get('seasons', [])}
        for new_season in new_entry.get('seasons', []):
            label = new_season.get('season')
            o_season = old_seasons.get(label)
            if not o_season:
                continue
            old_has_ep0 = any(ep.get('number') == 0 for ep in o_season.get('episodes', []))
            old_flag = o_season.get('ignored_episode_0', False)
            new_flag = new_season.get('ignored_episode_0', False)
            if (old_has_ep0 and new_flag) or (not old_flag and new_flag):
                added.setdefault(title, []).append(str(label))
            elif old_flag and not new_flag:
                removed.setdefault(title, []).append(str(label))
    return {'added': added, 'removed': removed}


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
    }

    old_titles = set(old_data.keys()) if isinstance(old_data, dict) else {s.get('title') for s in old_data if s.get('title')}
    new_titles = set(new_data.keys()) if isinstance(new_data, dict) else {s.get('title') for s in new_data if s.get('title')}

    if isinstance(old_data, list):
        old_data = {s.get('title'): s for s in old_data}
    if isinstance(new_data, list):
        new_data = {s.get('title'): s for s in new_data}

    for title in new_titles - old_titles:
        changes["new_series"].append(title)

    for title in old_titles & new_titles:
        old_series = old_data[title]
        new_series = new_data[title]

        old_sub = old_series.get('subscribed', False)
        new_sub = new_series.get('subscribed', False)
        if old_sub != new_sub:
            if new_sub:
                changes["newly_subscribed"].append(title)
            else:
                changes["newly_unsubscribed"].append(title)

        old_wl = old_series.get('watchlist', False)
        new_wl = new_series.get('watchlist', False)
        if old_wl != new_wl:
            if new_wl:
                changes["watchlist_added"].append(title)
            else:
                changes["watchlist_removed"].append(title)

        old_ger = old_series.get('title_ger', '')
        new_ger = new_series.get('title_ger', '')
        if old_ger and new_ger and old_ger != new_ger:
            changes["title_ger_changed"].append((title, old_ger, new_ger))

        old_eng = old_series.get('title_eng', '')
        new_eng = new_series.get('title_eng', '')
        if old_eng and new_eng and old_eng != new_eng:
            changes["title_eng_changed"].append((title, old_eng, new_eng))

        old_eps = {}
        for season in old_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                old_eps[(s_label, str(ep.get('number')))] = ep.get('watched', False)

        for season in new_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                ep_num = ep.get('number')
                ep_key = (s_label, str(ep_num))
                new_watched = ep.get('watched', False)

                if ep_key not in old_eps:
                    changes["new_episodes"].append((title, s_label, ep_num))
                elif old_eps[ep_key] != new_watched:
                    if not old_eps[ep_key] and new_watched:
                        changes["newly_watched"].append((title, s_label, ep_num))
                    elif old_eps[ep_key] and not new_watched:
                        changes["newly_unwatched"].append((title, s_label, ep_num))

    return changes


def show_changes(changes, include_unwatched=True, include_watched=True,
                 include_subscribe=True, include_unsubscribe=True,
                 include_watchlist_add=True, include_watchlist_remove=True, new_data=None):
    """Display changes with pagination and smart season grouping."""

    total = 0
    for k, v in changes.items():
        if k == 'newly_unwatched' and not include_unwatched:
            continue
        if k == 'newly_watched' and not include_watched:
            continue
        if k == 'newly_subscribed' and not include_subscribe:
            continue
        if k == 'newly_unsubscribed' and not include_unsubscribe:
            continue
        if k == 'watchlist_added' and not include_watchlist_add:
            continue
        if k == 'watchlist_removed' and not include_watchlist_remove:
            continue
        total += len(v)
    if total == 0:
        return 0

    print("\n" + "="*70)
    print("  CHANGES DETECTED")
    print("="*70)

    if changes["new_series"]:
        print(f"\n[NEW SERIES] ({len(changes['new_series'])})")
        def format_new_series(title):
            if not new_data:
                return f"  + {title}"
            series = _find_series(new_data, title)
            if not series:
                return f"  + {title}"
            watched = series.get('watched_episodes', 0)
            total_ep = series.get('total_episodes', 0)
            sub = series.get('subscribed')
            wl = series.get('watchlist')
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
        unwatched_lines = group_episodes_by_season(changes["newly_unwatched"], new_data, prefix='[!]')
        paginate_list(unwatched_lines, lambda line: line)

    if changes.get("newly_subscribed") and include_subscribe:
        print(f"\n[NEWLY SUBSCRIBED] ({len(changes['newly_subscribed'])} series)")
        paginate_list(changes["newly_subscribed"], lambda x: f"  [+] {x}")

    if changes.get("newly_unsubscribed") and include_unsubscribe:
        print(f"\n[NEWLY UNSUBSCRIBED] ({len(changes['newly_unsubscribed'])} series)")
        paginate_list(changes["newly_unsubscribed"], lambda x: f"  [-] {x}")

    if changes.get("watchlist_added") and include_watchlist_add:
        print(f"\n[ADDED TO WATCHLIST] ({len(changes['watchlist_added'])} series)")
        paginate_list(changes["watchlist_added"], lambda x: f"  [+] {x}")

    if changes.get("watchlist_removed") and include_watchlist_remove:
        print(f"\n[REMOVED FROM WATCHLIST] ({len(changes['watchlist_removed'])} series)")
        paginate_list(changes["watchlist_removed"], lambda x: f"  [-] {x}")

    if changes.get("title_ger_changed"):
        print(f"\n[GERMAN TITLE CHANGED] ({len(changes['title_ger_changed'])} series)")
        paginate_list(changes["title_ger_changed"], lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'")

    if changes.get("title_eng_changed"):
        print(f"\n[ENGLISH TITLE CHANGED] ({len(changes['title_eng_changed'])} series)")
        paginate_list(changes["title_eng_changed"], lambda x: f"  [~] {x[0]}: '{x[1]}' → '{x[2]}'")

    print("\n" + "="*70)
    return total


class IndexManager:
    """Manages persistent series index for aniworld.to with file locking."""

    def __init__(self, index_file):
        self.index_file = index_file
        self.series_index = {}
        self.file_lock = FileLock(index_file, timeout=10)
        self.load_index()

    def load_index(self):
        """Load existing series index from file with corruption detection and file locking."""
        with self.file_lock:
            if not os.path.exists(self.index_file):
                logger.info(f"No existing index found at {self.index_file}")
                self.series_index = {}
                return
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.series_index = {s.get('title'): s for s in data if s.get('title') and isinstance(s, dict)}
                    elif isinstance(data, dict):
                        first_item = next(iter(data.values()), None)
                        if first_item and isinstance(first_item, dict) and first_item.get('title'):
                            self.series_index = data
                        else:
                            self.series_index = {item.get('title'): item for item in data.values()
                                                 if isinstance(item, dict) and item.get('title')}
                    else:
                        self.series_index = {}

                    validated_index = {}
                    for title, series in self.series_index.items():
                        if _validate_series_entry(series, title):
                            validated_index[title] = series
                    self.series_index = validated_index
                    if not self.series_index:
                        logger.warning("Loaded index is empty or contains no valid series")

                print(f"[OK] Loaded {len(self.series_index)} series from index")
                logger.info(f"Loaded index with {len(self.series_index)} series")
            except json.JSONDecodeError as e:
                print(f"[ERROR] Index file corrupted: {e}")
                logger.error(f"Index file corrupted: {e}")
                self.series_index = {}
            except OSError as e:
                print(f"[ERROR] Cannot read index file: {e}")
                logger.error(f"Cannot read index file: {e}")
                self.series_index = {}
            except Exception as e:
                print(f"[WARN] Error loading index: {e}")
                logger.error(f"Error loading index: {e}")
                self.series_index = {}

    def save_index(self):
        """Save series index to file atomically with file locking."""
        with self.file_lock:
            try:
                series_list = list(self.series_index.values())
                _atomic_write_json(self.index_file, series_list)
                logger.info(f"Saved index with {len(self.series_index)} series")
            except Exception as e:
                print(f"[ERROR] Failed to save index: {e}")
                logger.error(f"Error saving index: {e}")
                raise

    def get_series_with_progress(self, sort_by='completion', reverse=False):
        """Get series with computed episode progress information."""
        series_list = []
        for s in self.series_index.values():
            total_eps, watched_eps = get_episode_counts(s)
            is_incomplete = (total_eps == 0) or (watched_eps < total_eps)
            completion = round((watched_eps / total_eps) * 100, 2) if total_eps > 0 else 0.0
            series_list.append({
                'title': s.get('title', ''),
                'watched_episodes': watched_eps,
                'total_episodes': total_eps,
                'is_incomplete': is_incomplete,
                'completion': completion,
                'subscribed': s.get('subscribed', False),
                'watchlist': s.get('watchlist', False),
                'empty': s.get('empty', False),
                'season_labels': [str(sn.get('season', '?')) for sn in s.get('seasons', [])],
            })
        if sort_by:
            series_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        return series_list

    def get_statistics(self):
        """Enhanced statistics with detailed analytics."""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)

        if total == 0:
            return {
                'total_series': 0, 'watched': 0, 'unwatched': 0,
                'watched_percentage': 0.0, 'empty_series': 0,
            }

        watched = sum(1 for s in series_with_progress if not s['is_incomplete'])
        unwatched = total - watched
        empty_count = sum(1 for s in self.series_index.values() if s.get('empty', False))

        completion_percentages = [s['completion'] for s in series_with_progress]
        avg_completion = round(sum(completion_percentages) / total, 2)

        total_episodes = sum(s['total_episodes'] for s in series_with_progress)
        watched_episodes = sum(s['watched_episodes'] for s in series_with_progress)
        avg_episodes_per_series = round(total_episodes / total, 1) if total > 0 else 0

        subscribed_count = sum(1 for s in series_with_progress if s['subscribed'])
        watchlist_count = sum(1 for s in series_with_progress if s['watchlist'])
        both_count = sum(1 for s in series_with_progress if s['subscribed'] and s['watchlist'])

        completion_distribution = {
            '0-25%': sum(1 for p in completion_percentages if 0 <= p < 25),
            '25-50%': sum(1 for p in completion_percentages if 25 <= p < 50),
            '50-75%': sum(1 for p in completion_percentages if 50 <= p < 75),
            '75-99%': sum(1 for p in completion_percentages if 75 <= p < 100),
            '100%': sum(1 for p in completion_percentages if p == 100),
        }

        ongoing_only = [s for s in series_with_progress if 0 < s['completion'] < 100]
        sorted_ongoing = sorted(ongoing_only, key=lambda x: x['completion'], reverse=True)
        most_completed = sorted_ongoing[:5]
        least_completed = sorted_ongoing[-5:] if sorted_ongoing else []

        completed_count = watched
        ongoing_count = len(ongoing_only)
        not_started_count = sum(1 for s in series_with_progress if s['watched_episodes'] == 0)

        return {
            'total_series': total,
            'watched': watched,
            'unwatched': unwatched,
            'watched_percentage': round((watched / total * 100), 2),
            'empty_series': empty_count,
            'completed_count': completed_count,
            'ongoing_count': ongoing_count,
            'not_started_count': not_started_count,
            'average_completion': avg_completion,
            'total_episodes': total_episodes,
            'watched_episodes': watched_episodes,
            'unwatched_episodes': total_episodes - watched_episodes,
            'average_episodes_per_series': avg_episodes_per_series,
            'subscribed_count': subscribed_count,
            'watchlist_count': watchlist_count,
            'both_subscribed_and_watchlist': both_count,
            'completion_distribution': completion_distribution,
            'most_completed_series': [
                {'title': s['title'], 'completion': s['completion'],
                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in most_completed
            ],
            'least_completed_series': [
                {'title': s['title'], 'completion': s['completion'],
                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in least_completed
            ],
        }

    def get_full_report(self, filter_subscribed=None, filter_watchlist=None, filter_mode='and'):
        """Generate a comprehensive report with detailed analytics."""
        series_progress = self.get_series_with_progress()
        stats = self.get_statistics()

        if filter_mode == 'or' and filter_subscribed is not None and filter_watchlist is not None:
            series_progress = [
                s for s in series_progress
                if s['subscribed'] == filter_subscribed or s['watchlist'] == filter_watchlist
            ]
        else:
            if filter_subscribed is not None:
                series_progress = [s for s in series_progress if s['subscribed'] == filter_subscribed]
            if filter_watchlist is not None:
                series_progress = [s for s in series_progress if s['watchlist'] == filter_watchlist]

        watched_series = [
            s for s in series_progress
            if not s['is_incomplete'] and s.get('subscribed')
        ]
        waiting_for_new = [
            s for s in series_progress
            if not s['is_incomplete'] and s.get('watchlist')
        ]
        ongoing_series = [
            s for s in series_progress
            if s['is_incomplete'] and s['watched_episodes'] > 0
        ]
        not_started_series = [
            s for s in series_progress
            if s['watched_episodes'] == 0
        ]
        not_started_sub_wl = [
            s for s in series_progress
            if s['watched_episodes'] == 0 and s['total_episodes'] > 0
            and (s.get('subscribed') or s.get('watchlist'))
        ]
        not_started_sub_wl_sorted = sorted(not_started_sub_wl, key=lambda x: x['title'])
        surprise_series = [
            s for s in series_progress
            if s.get('subscribed') and not s.get('watchlist')
            and s['is_incomplete'] and s['watched_episodes'] > 0
        ]
        surprise_sorted = sorted(surprise_series, key=lambda x: x['completion'], reverse=True)
        waiting_sorted = sorted(waiting_for_new, key=lambda x: x['title'])

        ongoing_sorted = sorted(ongoing_series, key=lambda x: x['completion'], reverse=True)

        episode_ranges = {
            'short_series': [s['title'] for s in series_progress if s['total_episodes'] <= 5],
            'medium_series': [s['title'] for s in series_progress if 6 <= s['total_episodes'] <= 25],
            'long_series': [s['title'] for s in series_progress if s['total_episodes'] > 25],
        }

        near_completion = [s['title'] for s in ongoing_sorted if 80 <= s['completion'] < 100][:10]
        stalled = [s['title'] for s in ongoing_sorted if s['completion'] < 25][:10]

        report = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'total_series_in_index': len(self.series_index),
                'active_series': len(series_progress),
                'filter_subscribed': filter_subscribed,
                'filter_watchlist': filter_watchlist,
                'statistics': stats,
            },
            'categories': {
                'watched': {
                    'count': len(watched_series),
                    'titles': sorted([s['title'] for s in watched_series]),
                },
                'ongoing': {
                    'count': len(ongoing_series),
                    'titles': [s['title'] for s in ongoing_sorted],
                    'details': [{'title': s['title'], 'completion': s['completion'],
                                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}",
                                 'seasons': s.get('season_labels', [])}
                                for s in ongoing_sorted[:20]],
                },
                'waiting_for_new_episodes': {
                    'count': len(waiting_for_new),
                    'titles': [s['title'] for s in waiting_sorted],
                    'details': [{'title': s['title'],
                                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}",
                                 'subscribed': s['subscribed'],
                                 'watchlist': s['watchlist'],
                                 'seasons': s.get('season_labels', [])}
                                for s in waiting_sorted],
                },
                'not_started': {
                    'count': len(not_started_series),
                    'titles': sorted([s['title'] for s in not_started_series]),
                },
                'surprise_new_episodes': {
                    'count': len(surprise_series),
                    'titles': [s['title'] for s in surprise_sorted],
                    'details': [{'title': s['title'], 'completion': s['completion'],
                                 'progress': f"{s['watched_episodes']}/{s['total_episodes']}",
                                 'seasons': s.get('season_labels', [])}
                                for s in surprise_sorted],
                },
                'not_started_subscribed_watchlist': {
                    'count': len(not_started_sub_wl),
                    'titles': [s['title'] for s in not_started_sub_wl_sorted],
                    'details': [{'title': s['title'],
                                 'total_episodes': s['total_episodes'],
                                 'subscribed': s['subscribed'],
                                 'watchlist': s['watchlist'],
                                 'seasons': s.get('season_labels', [])}
                                for s in not_started_sub_wl_sorted],
                },
            },
            'insights': {
                'completion_distribution': stats.get('completion_distribution', {}),
                'episode_ranges': episode_ranges,
                'near_completion': near_completion,
                'stalled_series': stalled,
                'most_completed': stats.get('most_completed_series', [])[:10],
                'least_completed': stats.get('least_completed_series', [])[:10],
            },
            'raw_data': {
                'all_series': self.series_index,
                'series_progress': series_progress,
            },
        }
        return report


def _prompt_change_confirmations(changes, new_dict):
    """Prompt the user to confirm each category of detected changes."""

    allowed = {
        'watched': False,
        'unwatched': False,
        'subscribe': False,
        'unsubscribe': False,
        'watchlist_add': False,
        'watchlist_remove': False,
        'title_ger': False,
        'title_eng': False,
    }

    def _build_episode_lines(episode_list, new_dict, prefix='[+]'):
        grouped = defaultdict(list)
        for title, season, ep_num in episode_list:
            grouped[(title, season)].append(ep_num)
        lines = []
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            sub = '✓' if series and series.get('subscribed') else '✗'
            wl = '✓' if series and series.get('watchlist') else '✗'
            sub_wl = f" (Sub:{sub} WL:{wl})"
            if total_in_season > 0:
                lines.append(f"  {prefix} {title} [{season}]: {watched_in_season}/{total_in_season} episodes{sub_wl}")
            else:
                lines.append(f"  {prefix} {title} [{season}]: {len(ep_nums)} episode(s){sub_wl}")
        return lines

    def _show_and_confirm(header, items, formatter, prompt_text, show_full_fn=None):
        print(f"\n{header}")
        print("   (manual confirmation required)")
        print("\n" + "-"*70)
        for item in items:
            print(formatter(item))
        print("-"*70)
        resp = input(f"\n{prompt_text} (y/n): ").strip().lower()
        return resp == 'y'

    if changes['newly_watched']:
        lines = _build_episode_lines(changes['newly_watched'], new_dict, prefix='[+]')
        if _show_and_confirm(
            f"[OK] {len(changes['newly_watched'])} episode(s) would change from UNWATCHED to WATCHED",
            lines, lambda x: x,
            "Allow these episodes to be marked as WATCHED?"
        ):
            allowed['watched'] = True
        else:
            print("  -> Watched changes will be ignored (episodes stay unwatched)")

    if changes['newly_unwatched']:
        lines = _build_episode_lines(changes['newly_unwatched'], new_dict, prefix='[!]')
        if _show_and_confirm(
            f"[WARN] {len(changes['newly_unwatched'])} episode(s) would change from WATCHED to UNWATCHED",
            lines, lambda x: x,
            "Allow these episodes to be marked as UNWATCHED?"
        ):
            allowed['unwatched'] = True
        else:
            print("  -> Unwatched changes will be ignored (episodes stay watched)")

    if changes['newly_subscribed']:
        if _show_and_confirm(
            f"[+] {len(changes['newly_subscribed'])} series newly SUBSCRIBED",
            changes['newly_subscribed'], lambda x: f"  [+] {x}",
            "Allow these series to be marked as SUBSCRIBED?"
        ):
            allowed['subscribe'] = True
        else:
            print("  -> Subscribe changes will be ignored")

    if changes['newly_unsubscribed']:
        if _show_and_confirm(
            f"[-] {len(changes['newly_unsubscribed'])} series UNSUBSCRIBED",
            changes['newly_unsubscribed'], lambda x: f"  [-] {x}",
            "Allow these series to be marked as UNSUBSCRIBED?"
        ):
            allowed['unsubscribe'] = True
        else:
            print("  -> Unsubscribe changes will be ignored")

    if changes['watchlist_added']:
        if _show_and_confirm(
            f"[+] {len(changes['watchlist_added'])} series ADDED TO WATCHLIST",
            changes['watchlist_added'], lambda x: f"  [+] {x}",
            "Allow these series to be added to WATCHLIST?"
        ):
            allowed['watchlist_add'] = True
        else:
            print("  -> Watchlist add changes will be ignored")

    if changes['watchlist_removed']:
        if _show_and_confirm(
            f"[-] {len(changes['watchlist_removed'])} series REMOVED FROM WATCHLIST",
            changes['watchlist_removed'], lambda x: f"  [-] {x}",
            "Allow these series to be removed from WATCHLIST?"
        ):
            allowed['watchlist_remove'] = True
        else:
            print("  -> Watchlist remove changes will be ignored")

    if changes['title_ger_changed']:
        def _fmt_title_change(x):
            return f"  [~] {x[0]}\n      Old: {x[1]}\n      New: {x[2]}"
        if _show_and_confirm(
            f"[~] {len(changes['title_ger_changed'])} German title(s) changed",
            changes['title_ger_changed'], _fmt_title_change,
            "Allow German title changes?"
        ):
            allowed['title_ger'] = True
        else:
            print("  -> German title changes will be ignored")

    if changes['title_eng_changed']:
        def _fmt_title_change(x):
            return f"  [~] {x[0]}\n      Old: {x[1]}\n      New: {x[2]}"
        if _show_and_confirm(
            f"[~] {len(changes['title_eng_changed'])} English title(s) changed",
            changes['title_eng_changed'], _fmt_title_change,
            "Allow English title changes?"
        ):
            allowed['title_eng'] = True
        else:
            print("  -> English title changes will be ignored")

    return allowed


def _build_merged_data(old_data, new_dict, allowed):
    """Merge new scraped data into old data, respecting user-allowed change categories."""
    merged = copy.deepcopy(old_data)
    for title, new_entry in new_dict.items():
        if title in merged:
            old_entry = merged[title]
            old_seasons = {s.get('season'): s for s in old_entry.get('seasons', [])}
            for new_season in new_entry.get('seasons', []):
                season_label = new_season.get('season')
                if season_label in old_seasons:
                    old_eps = {str(ep.get('number')): ep for ep in old_seasons[season_label].get('episodes', [])}
                    merged_episodes = []
                    for new_ep in new_season.get('episodes', []):
                        ep_num = str(new_ep.get('number'))
                        if ep_num in old_eps:
                            old_watched = old_eps[ep_num].get('watched', False)
                            new_watched = new_ep.get('watched', False)
                            if allowed['watched'] and (not old_watched and new_watched):
                                new_ep['watched'] = True
                            elif allowed['unwatched'] and (old_watched and not new_watched):
                                new_ep['watched'] = False
                            else:
                                new_ep['watched'] = old_watched
                        merged_episodes.append(new_ep)
                    old_seasons[season_label]['episodes'] = merged_episodes
                    # Remove leftover episode 0 if this season is in the ignore list
                    if new_season.get('ignored_episode_0'):
                        old_seasons[season_label]['episodes'] = [
                            ep for ep in old_seasons[season_label]['episodes']
                            if ep.get('number') != 0
                        ]
                    # Sync ignored_episode_0: add or remove based on new data
                    if new_season.get('ignored_episode_0'):
                        old_seasons[season_label]['ignored_episode_0'] = True
                    else:
                        old_seasons[season_label].pop('ignored_episode_0', None)
                else:
                    validated_eps = []
                    for ep in new_season.get('episodes', []):
                        if ep.get('watched') is None:
                            logger.error(f"Episode {ep.get('number')} in new season '{season_label}' "
                                        f"for '{title}' has None watched status — dropping episode")
                            continue
                        validated_eps.append(ep)
                    new_season['episodes'] = validated_eps
                    old_seasons[season_label] = new_season
            old_entry['seasons'] = list(old_seasons.values())
            old_entry['total_seasons'] = len(old_entry['seasons'])
            total_eps, watched_eps = get_episode_counts(old_entry)
            old_entry['watched_episodes'] = watched_eps
            old_entry['total_episodes'] = total_eps
            old_entry['unwatched_episodes'] = old_entry['total_episodes'] - old_entry['watched_episodes']
            new_url = new_entry.get('url', '')
            new_link = new_entry.get('link', '')
            if new_url and _is_valid_series_url(new_url):
                old_entry['url'] = new_url
            elif new_url:
                logger.warning(f"Rejected invalid URL during merge for '{title}': {new_url[:80]}")
            if new_link and _is_valid_series_url(new_link):
                old_entry['link'] = new_link
            elif new_link:
                logger.warning(f"Rejected invalid link during merge for '{title}': {new_link[:80]}")
            if 'subscribed' in new_entry:
                new_sub = new_entry['subscribed']
                if new_sub is None:
                    logger.error(f"Ignoring None subscribed for existing entry '{title}' — keeping old value")
                else:
                    old_sub = old_entry.get('subscribed', False)
                    if old_sub != new_sub:
                        if new_sub and allowed['subscribe']:
                            old_entry['subscribed'] = True
                        elif not new_sub and allowed['unsubscribe']:
                            old_entry['subscribed'] = False
            if 'watchlist' in new_entry:
                new_wl = new_entry['watchlist']
                if new_wl is None:
                    logger.error(f"Ignoring None watchlist for existing entry '{title}' — keeping old value")
                else:
                    old_wl = old_entry.get('watchlist', False)
                    if old_wl != new_wl:
                        if new_wl and allowed['watchlist_add']:
                            old_entry['watchlist'] = True
                        elif not new_wl and allowed['watchlist_remove']:
                            old_entry['watchlist'] = False
            if allowed['title_ger'] and 'title_ger' in new_entry:
                old_entry['title_ger'] = new_entry['title_ger']
            if allowed['title_eng'] and 'title_eng' in new_entry:
                old_entry['title_eng'] = new_entry['title_eng']
            old_alts = old_entry.get('alt_titles', [])
            new_alts = new_entry.get('alt_titles', [])
            combined = list(dict.fromkeys(old_alts + new_alts))
            old_entry['alt_titles'] = combined
            old_entry['empty'] = old_entry['total_episodes'] == 0
            old_entry['last_updated'] = datetime.now().isoformat()
            old_entry.setdefault('subscribed', False)
            old_entry.setdefault('watchlist', False)
            merged[title] = {
                'url': old_entry.get('url', ''),
                'link': old_entry.get('link', ''),
                'subscribed': old_entry.get('subscribed', False),
                'watchlist': old_entry.get('watchlist', False),
                'title': old_entry.get('title', title),
                'title_ger': old_entry.get('title_ger', ''),
                'title_eng': old_entry.get('title_eng', ''),
                'alt_titles': old_entry.get('alt_titles', []),
                'total_seasons': len(old_entry.get('seasons', [])),
                'total_episodes': old_entry.get('total_episodes', 0),
                'watched_episodes': old_entry.get('watched_episodes', 0),
                'unwatched_episodes': old_entry.get('unwatched_episodes', 0),
                'empty': old_entry.get('empty', False),
                'added_date': old_entry.get('added_date', ''),
                'last_updated': old_entry.get('last_updated', ''),
                'seasons': old_entry.get('seasons', []),
            }
        else:
            if 'subscribed' not in new_entry:
                logger.warning(f"New entry '{title}' missing 'subscribed' field — setting to False")
                new_entry['subscribed'] = False
            elif new_entry['subscribed'] is None:
                logger.error(f"Rejecting new entry '{title}': subscribed is None (scrape failed)")
                continue
            if 'watchlist' not in new_entry:
                logger.warning(f"New entry '{title}' missing 'watchlist' field — setting to False")
                new_entry['watchlist'] = False
            elif new_entry['watchlist'] is None:
                logger.error(f"Rejecting new entry '{title}': watchlist is None (scrape failed)")
                continue
            new_entry.setdefault('alt_titles', [])
            new_entry['added_date'] = datetime.now().isoformat()
            new_entry['last_updated'] = datetime.now().isoformat()
            total_eps, watched_eps = get_episode_counts(new_entry)
            merged[title] = {
                'url': new_entry.get('url', ''),
                'link': new_entry.get('link', ''),
                'subscribed': new_entry['subscribed'],
                'watchlist': new_entry['watchlist'],
                'title': new_entry.get('title', title),
                'title_ger': new_entry.get('title_ger', ''),
                'title_eng': new_entry.get('title_eng', ''),
                'alt_titles': new_entry.get('alt_titles', []),
                'total_seasons': len(new_entry.get('seasons', [])),
                'total_episodes': total_eps,
                'watched_episodes': watched_eps,
                'unwatched_episodes': total_eps - watched_eps,
                'empty': total_eps == 0,
                'added_date': new_entry.get('added_date', ''),
                'last_updated': new_entry.get('last_updated', ''),
                'seasons': new_entry.get('seasons', []),
            }

    return merged


def _extract_slug_from_field(value):
    """Extract series slug from a link or URL field containing '/anime/stream/'."""
    if not value or not isinstance(value, str):
        return None
    idx = value.find('/anime/stream/')
    if idx == -1:
        return None
    slug = value[idx + len('/anime/stream/'):].strip('/').split('/')[0]
    return slug if slug else None


def show_vanished_series(old_data, all_discovered_slugs, scrape_scope):
    """Show informational notification about previously indexed series not found in the current scrape."""
    if scrape_scope not in ('all', 'new_only', 'watchlist', 'subscribed', 'both'):
        return []

    vanished = []
    corrupt_entries = []

    for title, entry in old_data.items():
        slug = _extract_slug_from_field(entry.get('link', ''))
        if slug is None:
            slug = _extract_slug_from_field(entry.get('url', ''))
            if slug is not None:
                logger.warning(f"Used URL fallback for slug extraction: {title}")
            else:
                corrupt_entries.append(title)
                continue

        if slug in all_discovered_slugs:
            continue

        is_sub = entry.get('subscribed', False)
        is_wl = entry.get('watchlist', False)

        if scrape_scope in ('all', 'new_only'):
            vanished.append((title, 'not found on aniworld.to'))
        elif scrape_scope == 'watchlist':
            if is_wl:
                vanished.append((title, 'was on watchlist'))
        elif scrape_scope == 'subscribed':
            if is_sub:
                vanished.append((title, 'was subscribed'))
        elif scrape_scope == 'both':
            if is_sub and is_wl:
                vanished.append((title, 'was subscribed + on watchlist — possibly deleted from aniworld.to'))
            elif is_sub:
                vanished.append((title, 'was subscribed'))
            elif is_wl:
                vanished.append((title, 'was on watchlist'))

    if corrupt_entries:
        print(f"\n⚠ {len(corrupt_entries)} index entry(s) have corrupt/missing URL data:")
        for t in corrupt_entries[:10]:
            print(f"  • {t}")
        if len(corrupt_entries) > 10:
            print(f"  ... and {len(corrupt_entries) - 10} more")
        print("  These entries were skipped during vanished-series detection.")
        logger.warning(f"Corrupt URL data in {len(corrupt_entries)} index entries: {corrupt_entries[:5]}")

    if vanished:
        print(f"\n{'─'*70}")
        print(f"  [INFO] {len(vanished)} previously indexed series NOT found in current scrape:")
        print(f"{'─'*70}")
        for title, reason in vanished[:20]:
            print(f"  • {title}  ({reason})")
        if len(vanished) > 20:
            print(f"  ... and {len(vanished) - 20} more")
        print(f"{'─'*70}")
        print("  These series are preserved unchanged in the index.")
        logger.info(f"Vanished series notification: {len(vanished)} series not found in scrape scope '{scrape_scope}'")

    return vanished


def confirm_and_save_changes(new_data, description, index_manager):
    """Show changes, ask for separate watched/unwatched confirmation, merge, and save."""
    old_data = dict(index_manager.series_index)

    if isinstance(new_data, list):
        new_dict = {s.get('title'): s for s in new_data if s.get('title')}
    else:
        new_dict = dict(new_data)

    changes = detect_changes(old_data, new_dict)
    logger.info(f"Detected changes: { {k: len(v) for k, v in changes.items()} }")

    allowed = _prompt_change_confirmations(changes, new_dict)

    if not allowed['watched']:
        changes['newly_watched'] = []
    if not allowed['unwatched']:
        changes['newly_unwatched'] = []
    if not allowed['subscribe']:
        changes['newly_subscribed'] = []
    if not allowed['unsubscribe']:
        changes['newly_unsubscribed'] = []
    if not allowed['watchlist_add']:
        changes['watchlist_added'] = []
    if not allowed['watchlist_remove']:
        changes['watchlist_removed'] = []
    if not allowed['title_ger']:
        changes['title_ger_changed'] = []
    if not allowed['title_eng']:
        changes['title_eng_changed'] = []

    # Detect housekeeping changes BEFORE merge (merge mutates old_data in place)
    housekeeping = _detect_housekeeping_changes(old_data, new_dict)

    merged = _build_merged_data(old_data, new_dict, allowed)

    main_changes = sum(len(v) for k, v in changes.items()
                       if k not in ('newly_unwatched', 'newly_subscribed', 'newly_unsubscribed',
                                    'watchlist_added', 'watchlist_removed'))
    if allowed['unwatched']:
        main_changes += len(changes['newly_unwatched'])
    if allowed['subscribe']:
        main_changes += len(changes['newly_subscribed'])
    if allowed['unsubscribe']:
        main_changes += len(changes['newly_unsubscribed'])
    if allowed['watchlist_add']:
        main_changes += len(changes['watchlist_added'])
    if allowed['watchlist_remove']:
        main_changes += len(changes['watchlist_removed'])
    if main_changes == 0:
        has_housekeeping = housekeeping['added'] or housekeeping['removed']
        if has_housekeeping:
            total = sum(len(v) for v in housekeeping['added'].values()) + \
                    sum(len(v) for v in housekeeping['removed'].values())
            print(f"\n⚠ {total} ignored episode 0 change(s):")
            print(f"{'─'*70}")
            if housekeeping['added']:
                print("  Remove ep0 & mark ignored:")
                for title in sorted(housekeeping['added']):
                    seasons = ', '.join(housekeeping['added'][title])
                    series = new_dict.get(title, {})
                    watched = series.get('watched_episodes', 0)
                    total_ep = series.get('total_episodes', 0)
                    sub = series.get('subscribed')
                    wl = series.get('watchlist')
                    parts = []
                    if sub is not None:
                        parts.append(f"Sub:{'✓' if sub else '✗'}")
                    if wl is not None:
                        parts.append(f"WL:{'✓' if wl else '✗'}")
                    sub_info = f" ({' '.join(parts)})" if parts else ""
                    print(f"    • {title}  [{seasons}]: {watched}/{total_ep} watched{sub_info}")
            if housekeeping['removed']:
                print("  Unmark ignored (ep0 no longer present):")
                for title in sorted(housekeeping['removed']):
                    seasons = ', '.join(housekeeping['removed'][title])
                    series = new_dict.get(title, {})
                    watched = series.get('watched_episodes', 0)
                    total_ep = series.get('total_episodes', 0)
                    sub = series.get('subscribed')
                    wl = series.get('watchlist')
                    parts = []
                    if sub is not None:
                        parts.append(f"Sub:{'✓' if sub else '✗'}")
                    if wl is not None:
                        parts.append(f"WL:{'✓' if wl else '✗'}")
                    sub_info = f" ({' '.join(parts)})" if parts else ""
                    print(f"    • {title}  [{seasons}]: {watched}/{total_ep} watched{sub_info}")
            print(f"{'─'*70}")
            if input("Apply these changes? (y/n): ").strip().lower() != 'y':
                print("✗ Changes discarded.")
                return False
            index_manager.series_index = merged
            index_manager.save_index()
            print(f"✓ Saved {len(merged)} series to index")
            return True
        else:
            print(f"\n✓ {description} already up to date.")
            logger.info(f"No changes to save for {description}.")
            return True

    show_changes(changes, include_unwatched=False, include_watched=False,
                 include_subscribe=False, include_unsubscribe=False,
                 include_watchlist_add=False, include_watchlist_remove=False,
                 new_data=new_dict)

    response = input(f"\nSave these changes? (y/n): ").strip().lower()
    if response != 'y':
        print("✗ Changes discarded. Nothing saved.")
        logger.info("User discarded changes. Nothing saved.")
        return False

    index_manager.series_index = merged
    index_manager.save_index()
    print(f"✓ Saved {len(merged)} series to index")
    logger.info(f"Saved {len(merged)} series to index")
    return True
