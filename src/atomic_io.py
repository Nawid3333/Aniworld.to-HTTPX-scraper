"""Durable atomic JSON writes, shared by every persisted file in this project."""

import contextlib
import json
import logging
import os
import shutil
import tempfile
import time

logger = logging.getLogger(__name__)


def create_file_backup(filepath):
    """Create a backup of a file (up to 3 generations kept)."""
    if not os.path.exists(filepath):
        return
    try:
        backup_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        # Remove oldest backup if 3 already exist
        for i in range(3, 10):
            old_backup = os.path.join(backup_dir, f"{filename}.bak{i}")
            if os.path.exists(old_backup):
                with contextlib.suppress(OSError):
                    os.remove(old_backup)

        # Shift existing backups
        for i in range(2, 0, -1):
            src = os.path.join(backup_dir, f"{filename}.bak{i}")
            dst = os.path.join(backup_dir, f"{filename}.bak{i + 1}")
            if os.path.exists(src):
                with contextlib.suppress(OSError):
                    shutil.move(src, dst)

        # Create new backup
        backup_path = os.path.join(backup_dir, f"{filename}.bak1")
        shutil.copy2(filepath, backup_path)
        logger.debug("Created backup: %s", backup_path)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Could not create backup of %s: %s", filepath, e)


def _rotate_backups(filepath):
    """Shift .bak1 -> .bak2 -> .bak3 and drop anything older.

    Pure renames, so this costs the same whether the file is 2 KB or 80 MB.
    Caller is responsible for putting the current file into .bak1.
    """
    backup_dir = os.path.dirname(filepath)
    filename = os.path.basename(filepath)

    for i in range(3, 10):
        stale = os.path.join(backup_dir, f"{filename}.bak{i}")
        if os.path.exists(stale):
            with contextlib.suppress(OSError):
                os.remove(stale)

    for i in range(2, 0, -1):
        src = os.path.join(backup_dir, f"{filename}.bak{i}")
        dst = os.path.join(backup_dir, f"{filename}.bak{i + 1}")
        if os.path.exists(src):
            with contextlib.suppress(OSError):
                os.replace(src, dst)


# How long one write keeps retrying a file operation that Windows refuses
# with "access denied" or "in use by another process". A virus scanner or a
# search indexer opens a freshly closed file for a moment, and a rename that
# lands in that moment fails although nothing is wrong. These waits add up to
# about 1.5 s: long enough for a scan, short enough that a real permission
# problem still fails promptly.
_LOCK_RETRY_DELAYS = (0.05, 0.1, 0.2, 0.4, 0.8)

# A temp file older than this is an orphan from an earlier write that could
# not delete it (see _discard). Anything younger may belong to a write still
# running in another process, so it is left alone.
_STALE_TEMP_SECONDS = 3600


def _pending_backup_path(filepath):
    """Where the outgoing file waits until the new one is safely in place."""
    return f"{filepath}.bak-pending"


def _retry_on_lock(action, *args):
    """Run *action*, retrying while Windows reports the file as locked.

    Only PermissionError is retried -- WinError 5 and 32 both arrive as one.
    Any other error is real and is raised at once.
    """
    for delay in _LOCK_RETRY_DELAYS:
        try:
            return action(*args)
        except PermissionError:
            time.sleep(delay)
    return action(*args)


def _discard(path):
    """Remove *path* if it is there; one that stays locked is left for the sweep."""
    try:
        _retry_on_lock(os.remove, path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("Could not remove %s: %s", path, exc)


def _sweep_stale_temps(dirpath, filename):
    """Delete this file's temp files that an earlier write left behind.

    A temp file a scanner still held when its write failed cannot be deleted
    at that moment, and nothing used to come back for it: each one is a full
    copy of the file, tens of MB for the series index. Only this file's own
    prefix is touched, and only once it is old enough that no write can still
    be using it.
    """
    prefix = f".{filename}."
    cutoff = time.time() - _STALE_TEMP_SECONDS
    try:
        names = os.listdir(dirpath)
    except OSError:
        return
    for name in names:
        if name.startswith(prefix) and name.endswith(".tmp"):
            path = os.path.join(dirpath, name)
            with contextlib.suppress(OSError):
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)


def _finish_pending_backup(filepath):
    """Settle a pending backup that an interrupted write left behind.

    Still the same file as *filepath*: the write never reached its swap, and
    the pending name is only a spare link, so it goes. A different file: the
    swap happened and only the rotation was cut short, so it is the previous
    generation and moves into .bak1 as it would have.
    """
    pending = _pending_backup_path(filepath)
    if not os.path.exists(pending):
        return
    try:
        same = os.path.exists(filepath) and os.path.samefile(pending, filepath)
    except OSError:
        same = False
    if same:
        _discard(pending)
        return
    _rotate_backups(filepath)
    with contextlib.suppress(OSError):
        os.replace(pending, f"{filepath}.bak1")


def _stage_backup(filepath):
    """Give the outgoing file a second name before the swap; return it, or None.

    A hard link moves no data, so the 80 MB index is not copied on every
    save, and it leaves *filepath* where it is: if the swap then fails,
    nothing has moved and no generation is lost. A file system without hard
    links gets a copy instead. If neither works the write goes ahead without
    a new backup, as it always has -- a backup is not worth failing a save.
    """
    pending = _pending_backup_path(filepath)
    _discard(pending)
    try:
        os.link(filepath, pending)
        return pending
    except OSError:
        pass
    try:
        shutil.copy2(filepath, pending)
        return pending
    except OSError as exc:
        logger.warning("Could not back up %s before writing: %s", filepath, exc)
        _discard(pending)
        return None


def atomic_write_json(filepath, data, *, indent: int | None = 2, backup: bool = True):
    """Write JSON to file atomically via temp file + fsync + os.replace.

    Creates a backup before writing to prevent data loss on corruption.
    `os.replace` makes the directory-entry swap atomic, but it does not
    flush the file's *contents* to disk -- on an unclean shutdown the
    rename can land while the data is still sitting in the page cache,
    leaving a file that atomically points at nothing useful. The
    flush()+fsync() below close that gap. Shared by every JSON writer in
    this project (index, checkpoint, failed list, ignore list, ignored
    seasons) so the durability behaviour can't drift between call sites.

    A unique mkstemp() name (rather than a fixed "<file>.tmp") avoids
    collisions if two runs ever write the same file concurrently, and the
    except-branch cleanup means a failed write never leaves an orphaned
    temp file behind.

    The backups only move once the new file is in place. They used to be
    rotated first, so a swap that failed -- a scanner holding the fresh temp
    file is enough on Windows -- had already pushed the oldest generation
    out, and three failed saves in a row left no backup at all. Now the
    outgoing file gets a second name first (_stage_backup), the swap is
    retried while Windows reports a lock, and only a swap that succeeded
    rotates .bak1-3 and turns that second name into .bak1.
    """
    dirpath = os.path.dirname(filepath) or "."
    os.makedirs(dirpath, exist_ok=True)
    filename = os.path.basename(filepath)
    _sweep_stale_temps(dirpath, filename)
    if backup:
        _finish_pending_backup(filepath)

    fd, tmp_path = tempfile.mkstemp(dir=dirpath, prefix=f".{filename}.", suffix=".tmp")
    pending = None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        if backup and os.path.exists(filepath):
            pending = _stage_backup(filepath)
        _retry_on_lock(os.replace, tmp_path, filepath)
    except BaseException:
        # BaseException, so a Ctrl+C mid-write cleans up too. Nothing has
        # moved yet: *filepath* still holds the previous version, and only
        # the temp file and the spare backup name need to go.
        _discard(tmp_path)
        if pending:
            _discard(pending)
        raise

    if pending:
        # The new file is in place. From here a failure only costs this
        # generation's backup, never the save: whatever is left pending is
        # finished by the next write (_finish_pending_backup).
        _rotate_backups(filepath)
        try:
            os.replace(pending, f"{filepath}.bak1")
        except OSError as exc:
            logger.warning("Could not move the previous %s into .bak1: %s", filename, exc)
