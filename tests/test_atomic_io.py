"""Tests for the durable-write layer every persisted file in this project uses.

Most of these cover paths that only run when something has already gone wrong
-- a backup rotation, a failed rename, a dump that raises midway -- because
that is the whole point of the module. The index is the only copy of the
user's watch history, and a half-finished save is how it gets lost.

Run with:  python -m unittest discover -s tests
"""

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import atomic_io  # noqa: E402
from src.atomic_io import _rotate_backups, atomic_write_json, create_file_backup  # noqa: E402


class _TmpDirTest(unittest.TestCase):
    """Every test gets its own directory; nothing touches the real data dir."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dirpath = self._tmp.name

    def path(self, name="index.json"):
        return os.path.join(self.dirpath, name)

    def write(self, path, text):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)

    def read(self, path):
        with open(path, encoding="utf-8") as fh:
            return fh.read()


class AtomicWriteJsonTests(_TmpDirTest):
    """The happy path, and the shapes the data can take."""

    def test_writes_readable_json(self):
        p = self.path()
        atomic_write_json(p, {"a": 1, "b": [1, 2]})
        self.assertEqual(json.loads(self.read(p)), {"a": 1, "b": [1, 2]})

    def test_non_ascii_survives_the_round_trip(self):
        """ensure_ascii=False is deliberate: the titles are German and Japanese."""
        title = "Die Verräter — 日本語"
        p = self.path()
        atomic_write_json(p, {"title": title})
        self.assertEqual(json.loads(self.read(p))["title"], title)

    def test_indent_none_writes_one_line(self):
        p = self.path()
        atomic_write_json(p, {"a": 1, "b": 2}, indent=None)
        self.assertNotIn("\n", self.read(p))

    def test_a_missing_parent_directory_is_created(self):
        p = os.path.join(self.dirpath, "nested", "deeper", "index.json")
        atomic_write_json(p, {"ok": True})
        self.assertTrue(os.path.exists(p))

    def test_no_temp_file_is_left_behind(self):
        p = self.path()
        atomic_write_json(p, {"a": 1})
        self.assertEqual([n for n in os.listdir(self.dirpath) if n.endswith(".tmp")], [])

    def test_overwriting_replaces_the_content_entirely(self):
        p = self.path()
        atomic_write_json(p, {"old": True, "extra": 1})
        atomic_write_json(p, {"new": True})
        self.assertEqual(json.loads(self.read(p)), {"new": True})


class BackupOnWriteTests(_TmpDirTest):
    """The outgoing file is kept, and only three generations of it."""

    def test_first_write_makes_no_backup(self):
        p = self.path()
        atomic_write_json(p, {"a": 1})
        self.assertFalse(os.path.exists(f"{p}.bak1"))

    def test_second_write_moves_the_old_file_into_bak1(self):
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        atomic_write_json(p, {"gen": 2})
        self.assertEqual(json.loads(self.read(f"{p}.bak1")), {"gen": 1})
        self.assertEqual(json.loads(self.read(p)), {"gen": 2})

    def test_generations_shift_and_stop_at_three(self):
        p = self.path()
        for gen in range(1, 6):
            atomic_write_json(p, {"gen": gen})
        self.assertEqual(json.loads(self.read(p)), {"gen": 5})
        self.assertEqual(json.loads(self.read(f"{p}.bak1")), {"gen": 4})
        self.assertEqual(json.loads(self.read(f"{p}.bak2")), {"gen": 3})
        self.assertEqual(json.loads(self.read(f"{p}.bak3")), {"gen": 2})
        self.assertFalse(os.path.exists(f"{p}.bak4"))

    def test_backup_false_leaves_the_old_file_unbacked(self):
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        atomic_write_json(p, {"gen": 2}, backup=False)
        self.assertFalse(os.path.exists(f"{p}.bak1"))
        self.assertEqual(json.loads(self.read(p)), {"gen": 2})

    def test_a_generation_beyond_three_is_removed(self):
        """An older layout kept more; a rotation must not leave them behind."""
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        for i in range(3, 10):
            self.write(f"{p}.bak{i}", "stale")
        atomic_write_json(p, {"gen": 2})
        for i in range(4, 10):
            self.assertFalse(os.path.exists(f"{p}.bak{i}"), f"bak{i} survived")


class RotateBackupsTests(_TmpDirTest):
    """_rotate_backups on its own, including the errors it deliberately eats."""

    def test_rotation_is_a_no_op_when_there_is_nothing_to_rotate(self):
        _rotate_backups(self.path())
        self.assertEqual(os.listdir(self.dirpath), [])

    def test_rotation_shifts_each_generation_up_by_one(self):
        p = self.path()
        self.write(f"{p}.bak1", "one")
        self.write(f"{p}.bak2", "two")
        _rotate_backups(p)
        self.assertEqual(self.read(f"{p}.bak2"), "one")
        self.assertEqual(self.read(f"{p}.bak3"), "two")

    def test_an_unremovable_stale_backup_does_not_stop_the_rotation(self):
        """The suppressed OSError: a locked file must not fail the save."""
        p = self.path()
        self.write(f"{p}.bak1", "one")
        self.write(f"{p}.bak5", "stale")
        with mock.patch("src.atomic_io.os.remove", side_effect=OSError("locked")):
            _rotate_backups(p)
        self.assertEqual(self.read(f"{p}.bak2"), "one")

    def test_an_unmovable_generation_does_not_stop_the_rotation(self):
        p = self.path()
        self.write(f"{p}.bak1", "one")
        with mock.patch("src.atomic_io.os.replace", side_effect=OSError("locked")):
            _rotate_backups(p)
        self.assertEqual(self.read(f"{p}.bak1"), "one")


class WriteFailureTests(_TmpDirTest):
    """What is on disk after a write that did not finish."""

    def test_a_dump_that_raises_leaves_the_previous_file_intact(self):
        p = self.path()
        atomic_write_json(p, {"good": True})
        with mock.patch("src.atomic_io.json.dump", side_effect=ValueError("boom")), self.assertRaises(ValueError):
            atomic_write_json(p, {"bad": True})
        self.assertEqual(json.loads(self.read(p)), {"good": True})

    def test_a_dump_that_raises_leaves_no_temp_file(self):
        p = self.path()
        with mock.patch("src.atomic_io.json.dump", side_effect=ValueError("boom")), self.assertRaises(ValueError):
            atomic_write_json(p, {"bad": True})
        self.assertEqual([n for n in os.listdir(self.dirpath) if n.endswith(".tmp")], [])

    def test_a_failed_final_rename_restores_the_file_from_bak1(self):
        """The bug this branch exists for: the path must never end up empty."""
        p = self.path()
        atomic_write_json(p, {"gen": 1})

        real_replace = os.replace
        failed = []

        def replace(src, dst):
            # The write does: rotate, file -> .bak1, then tmp -> file, and
            # on failure .bak1 -> file. Both of the last two target the same
            # destination, so the source is what tells them apart: fail the
            # incoming temp file only, and let the restore through.
            if str(dst) == p and str(src).endswith(".tmp"):
                failed.append(dst)
                raise OSError("rename failed")
            return real_replace(src, dst)

        with mock.patch("src.atomic_io.os.replace", side_effect=replace), self.assertRaises(OSError):
            atomic_write_json(p, {"gen": 2})

        self.assertTrue(failed, "the failing rename never ran")
        self.assertTrue(os.path.exists(p), "the file was left missing")
        self.assertEqual(json.loads(self.read(p)), {"gen": 1})

    def test_when_the_restore_also_fails_the_data_survives_in_bak1(self):
        """The worst case. Nothing can be put back, so nothing may be lost."""
        p = self.path()
        atomic_write_json(p, {"gen": 1})

        with mock.patch("src.atomic_io.os.replace", side_effect=OSError("read-only")), self.assertRaises(OSError):
            atomic_write_json(p, {"gen": 2})

        # The rotation moved nothing (os.replace is dead), so the live file is
        # still where it was. Either way the old generation must be readable.
        surviving = p if os.path.exists(p) else f"{p}.bak1"
        self.assertEqual(json.loads(self.read(surviving)), {"gen": 1})

    def test_a_backup_that_cannot_be_made_does_not_stop_the_write(self):
        """If the old file cannot be moved aside, the new data still lands."""
        p = self.path()
        atomic_write_json(p, {"gen": 1})

        real_replace = os.replace

        def replace(src, dst):
            if str(dst) == f"{p}.bak1":
                raise OSError("locked")
            return real_replace(src, dst)

        with mock.patch("src.atomic_io.os.replace", side_effect=replace):
            atomic_write_json(p, {"gen": 2})

        self.assertEqual(json.loads(self.read(p)), {"gen": 2})


class LockedSwapTests(_TmpDirTest):
    """A swap Windows refuses must cost neither the save's backups nor a stray file.

    A scanner or indexer briefly holding the freshly written temp file makes
    the final rename fail with "in use by another process". The backups used
    to be rotated before that rename, so each failed save pushed the oldest
    generation out: three failed saves in a row left no backup at all, and
    every attempt left a full-size temp file behind.
    """

    def setUp(self):
        super().setUp()
        sleeper = mock.patch("src.atomic_io.time.sleep")
        sleeper.start()
        self.addCleanup(sleeper.stop)

    def _generations(self, p):
        names = [p] + [f"{p}.bak{i}" for i in (1, 2, 3)]
        return [json.loads(self.read(name))["gen"] if os.path.exists(name) else None for name in names]

    def _four_generations(self):
        p = self.path()
        for gen in range(1, 5):
            atomic_write_json(p, {"gen": gen})
        self.assertEqual(self._generations(p), [4, 3, 2, 1])
        return p

    def _locked_swap(self, p, times=None):
        """os.replace that refuses the temp-file swap onto *p*, *times* times or always."""
        real = os.replace
        refused = []

        def replace(src, dst):
            if str(src).endswith(".tmp") and str(dst) == p and (times is None or len(refused) < times):
                refused.append(src)
                raise PermissionError(13, "The process cannot access the file because it is being used")
            return real(src, dst)

        return replace, refused

    def _leftovers(self):
        return [n for n in os.listdir(self.dirpath) if n.endswith(".tmp") or "pending" in n]

    def test_a_failed_swap_keeps_every_backup_generation(self):
        p = self._four_generations()
        replace, _ = self._locked_swap(p)
        with mock.patch("src.atomic_io.os.replace", side_effect=replace), self.assertRaises(PermissionError):
            atomic_write_json(p, {"gen": 5})
        self.assertEqual(self._generations(p), [4, 3, 2, 1])
        self.assertEqual(self._leftovers(), [])

    def test_three_failed_saves_in_a_row_still_leave_three_backups(self):
        """The repro: each of these used to drop a generation."""
        p = self._four_generations()
        replace, _ = self._locked_swap(p)
        for gen in (5, 6, 7):
            with mock.patch("src.atomic_io.os.replace", side_effect=replace), self.assertRaises(PermissionError):
                atomic_write_json(p, {"gen": gen})
        self.assertEqual(self._generations(p), [4, 3, 2, 1])

    def test_a_briefly_locked_swap_is_retried_and_lands(self):
        p = self._four_generations()
        replace, refused = self._locked_swap(p, times=2)
        with mock.patch("src.atomic_io.os.replace", side_effect=replace):
            atomic_write_json(p, {"gen": 5})
        self.assertEqual(len(refused), 2, "the lock was not hit twice")
        self.assertEqual(self._generations(p), [5, 4, 3, 2])
        self.assertEqual(self._leftovers(), [])

    def test_retrying_gives_up_after_a_bounded_number_of_tries(self):
        p = self._four_generations()
        replace, refused = self._locked_swap(p)
        with mock.patch("src.atomic_io.os.replace", side_effect=replace), self.assertRaises(PermissionError):
            atomic_write_json(p, {"gen": 5})
        self.assertEqual(len(refused), len(atomic_io._LOCK_RETRY_DELAYS) + 1)

    def test_an_error_that_is_not_a_lock_is_not_retried(self):
        p = self._four_generations()
        calls = []
        real = os.replace

        def replace(src, dst):
            if str(src).endswith(".tmp"):
                calls.append(src)
                raise OSError("read-only file system")
            return real(src, dst)

        with mock.patch("src.atomic_io.os.replace", side_effect=replace), self.assertRaises(OSError):
            atomic_write_json(p, {"gen": 5})
        self.assertEqual(len(calls), 1)

    @unittest.skipUnless(os.name == "nt", "only Windows refuses to rename a file another handle holds")
    def test_a_real_handle_on_the_temp_file(self):
        """The live failure: a second open handle, as a virus scanner holds one."""
        p = self._four_generations()
        real_mkstemp = atomic_io.tempfile.mkstemp
        held = []

        def mkstemp(*args, **kwargs):
            fd, path = real_mkstemp(*args, **kwargs)
            held.append(open(path, "rb"))  # noqa: SIM115 -- held open on purpose until the write has failed
            return fd, path

        try:
            with mock.patch("src.atomic_io.tempfile.mkstemp", side_effect=mkstemp), self.assertRaises(PermissionError):
                atomic_write_json(p, {"gen": 5})
        finally:
            for handle in held:
                handle.close()
        self.assertEqual(self._generations(p), [4, 3, 2, 1])


class PendingBackupTests(_TmpDirTest):
    """The second name the outgoing file carries until the new one is in place."""

    def test_a_rotation_cut_short_after_the_swap_is_finished_by_the_next_write(self):
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        atomic_write_json(p, {"gen": 2})
        # The swap to gen 3 happened, the rotation did not: gen 2 is still
        # waiting under the pending name.
        self.write(f"{p}.bak-pending", json.dumps({"gen": 2}))
        self.write(p, json.dumps({"gen": 3}))
        atomic_write_json(p, {"gen": 4})
        self.assertEqual(json.loads(self.read(p)), {"gen": 4})
        self.assertEqual(json.loads(self.read(f"{p}.bak1")), {"gen": 3})
        self.assertEqual(json.loads(self.read(f"{p}.bak2")), {"gen": 2})
        self.assertEqual(json.loads(self.read(f"{p}.bak3")), {"gen": 1})
        self.assertFalse(os.path.exists(f"{p}.bak-pending"))

    def test_a_spare_link_from_a_write_that_never_swapped_is_dropped(self):
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        atomic_write_json(p, {"gen": 2})
        try:
            os.link(p, f"{p}.bak-pending")
        except OSError:
            self.skipTest("no hard links on this file system")
        atomic_write_json(p, {"gen": 3})
        self.assertEqual(json.loads(self.read(f"{p}.bak1")), {"gen": 2})
        self.assertEqual(json.loads(self.read(f"{p}.bak2")), {"gen": 1})
        self.assertFalse(os.path.exists(f"{p}.bak3"), "the spare link was rotated in as a generation")
        self.assertFalse(os.path.exists(f"{p}.bak-pending"))

    def test_without_hard_links_the_backup_is_copied(self):
        p = self.path()
        atomic_write_json(p, {"gen": 1})
        with mock.patch("src.atomic_io.os.link", side_effect=OSError("not supported")):
            atomic_write_json(p, {"gen": 2})
        self.assertEqual(json.loads(self.read(f"{p}.bak1")), {"gen": 1})
        self.assertEqual(json.loads(self.read(p)), {"gen": 2})


class StaleTempSweepTests(_TmpDirTest):
    """Temp files a failed write could not delete are cleared up later, and only those."""

    def test_an_old_temp_file_of_this_file_is_removed(self):
        p = self.path("index.json")
        stale = self.path(".index.json.abc123.tmp")
        self.write(stale, "{}")
        old = time.time() - atomic_io._STALE_TEMP_SECONDS - 60
        os.utime(stale, (old, old))
        atomic_write_json(p, {"a": 1})
        self.assertFalse(os.path.exists(stale))

    def test_a_young_temp_file_is_left_for_the_write_that_owns_it(self):
        p = self.path("index.json")
        young = self.path(".index.json.def456.tmp")
        self.write(young, "{}")
        atomic_write_json(p, {"a": 1})
        self.assertTrue(os.path.exists(young))

    def test_another_files_temp_file_is_never_touched(self):
        p = self.path("index.json")
        other = self.path(".checkpoint.json.abc123.tmp")
        self.write(other, "{}")
        old = time.time() - atomic_io._STALE_TEMP_SECONDS - 60
        os.utime(other, (old, old))
        atomic_write_json(p, {"a": 1})
        self.assertTrue(os.path.exists(other))


class CreateFileBackupTests(_TmpDirTest):
    """The standalone copy-based backup taken before a destructive edit."""

    def test_a_missing_file_is_a_no_op(self):
        create_file_backup(self.path("nope.json"))
        self.assertEqual(os.listdir(self.dirpath), [])

    def test_a_backup_copy_is_made_and_the_source_stays(self):
        p = self.path()
        self.write(p, "original")
        create_file_backup(p)
        self.assertEqual(self.read(f"{p}.bak1"), "original")
        self.assertEqual(self.read(p), "original")

    def test_repeated_backups_shift_generations(self):
        p = self.path()
        self.write(p, "one")
        create_file_backup(p)
        self.write(p, "two")
        create_file_backup(p)
        self.assertEqual(self.read(f"{p}.bak1"), "two")
        self.assertEqual(self.read(f"{p}.bak2"), "one")

    def test_generations_stop_at_three(self):
        p = self.path()
        for gen in ("one", "two", "three", "four", "five"):
            self.write(p, gen)
            create_file_backup(p)
        self.assertEqual(self.read(f"{p}.bak1"), "five")
        self.assertEqual(self.read(f"{p}.bak3"), "three")
        self.assertFalse(os.path.exists(f"{p}.bak4"))

    def test_a_generation_beyond_three_is_removed(self):
        p = self.path()
        self.write(p, "current")
        for i in range(3, 10):
            self.write(f"{p}.bak{i}", "stale")
        create_file_backup(p)
        for i in range(4, 10):
            self.assertFalse(os.path.exists(f"{p}.bak{i}"), f"bak{i} survived")

    def test_a_copy_that_fails_is_swallowed_not_raised(self):
        """A backup is best effort; it must never abort the caller's work."""
        p = self.path()
        self.write(p, "original")
        with mock.patch("src.atomic_io.shutil.copy2", side_effect=OSError("disk full")):
            create_file_backup(p)
        self.assertEqual(self.read(p), "original")


if __name__ == "__main__":
    unittest.main()
