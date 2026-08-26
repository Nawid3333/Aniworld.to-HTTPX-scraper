"""Durable atomic JSON writes, shared by every persisted file in this project."""

import contextlib
import json
import logging
import os
import shutil
import tempfile

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
    """
    dirpath = os.path.dirname(filepath) or "."
    os.makedirs(dirpath, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        # The new file is already on disk and fsynced, so the outgoing file
        # can simply be *renamed* into .bak1 rather than copied there. The
        # old code copied it: on the 80 MB series index that was 80 MB of
        # pointless I/O on every single save. A rename moves no data at all.
        moved_to_backup = False
        if backup and os.path.exists(filepath):
            _rotate_backups(filepath)
            try:
                os.replace(filepath, f"{filepath}.bak1")
                moved_to_backup = True
            except OSError:
                moved_to_backup = False
        try:
            os.replace(tmp_path, filepath)
        except Exception:
            # The outgoing file has already been renamed away at this point,
            # so failing here used to leave no file at that path at all --
            # the data survived only in .bak1, and the loader does not look
            # there for a *missing* file. Put it back before propagating.
            if moved_to_backup:
                with contextlib.suppress(OSError):
                    os.replace(f"{filepath}.bak1", filepath)
            raise
    except Exception:
        with contextlib.suppress(OSError):
            os.remove(tmp_path)
        raise
