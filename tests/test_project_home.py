"""The project-home override that makes an installed copy usable.

Every path this program touches -- .env, and the directories it writes --
hangs off one root. Unset, that root is the repo checkout, which is how it has
always behaved and how the README tells people to run it. ``ANIWORLD_HOME`` points
it somewhere else, which is the only reason an installed copy is usable at
all: inside a venv the config module lives in site-packages, and telling a
user to edit a .env in there is not a real instruction.

Both halves matter enough to pin. If the override breaks, installing the
package silently goes back to writing into site-packages. If the *default*
breaks, every existing user's data appears to vanish, because the program
starts looking for it somewhere new.

Style note for future edits
---------------------------
config resolves its paths at import time, so these spawn a real subprocess
with a real environment. Reloading the module in-process would not re-run that
resolution the way a user's shell does, and would prove nothing. To cover a
new path, add its attribute name to ``PATH_ATTRS`` -- the subprocess prints
whatever it is asked for, so no other change is needed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

HOME_VAR = "ANIWORLD_HOME"
CONFIG_MODULE = "config.config"
ROOT_ATTR = "PROJECT_HOME"

# Paths that must follow the project home. Add to this when config grows one.
PATH_ATTRS = ["DATA_DIR", "LOGS_DIR", "ENV_FILE", "SERIES_INDEX_FILE", "DEFAULT_BATCH_FILE"]


def _resolve(env_value: str | None) -> dict[str, str]:
    """Import config in a clean subprocess and report where its paths landed."""
    env = dict(os.environ)
    env.pop(HOME_VAR, None)
    if env_value is not None:
        env[HOME_VAR] = env_value
    # Keep the child from inheriting a parent's idea of where the repo is.
    env["PYTHONPATH"] = str(REPO_ROOT)

    wanted = [ROOT_ATTR, *PATH_ATTRS]
    code = (
        "import json, sys;"
        f"import {CONFIG_MODULE} as c;"
        f"print('<<RESULT>>' + json.dumps({{n: str(getattr(c, n)) for n in {wanted!r}}}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"config failed to import:\n{proc.stdout}\n{proc.stderr}"
    for line in proc.stdout.splitlines():
        if line.startswith("<<RESULT>>"):
            return json.loads(line[len("<<RESULT>>") :])
    raise AssertionError(f"no result marker in output:\n{proc.stdout}")


@pytest.fixture(scope="module")
def unset() -> dict[str, str]:
    return _resolve(None)


class TestDefaultIsTheCheckout:
    """Unset, nothing moves. This is what every existing install relies on."""

    def test_the_root_is_the_repo_itself(self, unset):
        assert Path(unset[ROOT_ATTR]).resolve() == REPO_ROOT

    def test_every_path_stays_inside_the_repo(self, unset):
        for name in PATH_ATTRS:
            resolved = Path(unset[name]).resolve()
            assert REPO_ROOT in resolved.parents or resolved == REPO_ROOT, f"{name} escaped the checkout: {resolved}"


class TestOverrideRelocatesEverything:
    def test_the_root_follows_the_variable(self, tmp_path):
        result = _resolve(str(tmp_path))
        assert Path(result[ROOT_ATTR]).resolve() == tmp_path.resolve()

    def test_no_path_is_left_behind_in_the_checkout(self, tmp_path):
        """A path that forgets to use the root would write into site-packages."""
        result = _resolve(str(tmp_path))
        for name in PATH_ATTRS:
            resolved = Path(result[name]).resolve()
            assert tmp_path.resolve() in resolved.parents or resolved == tmp_path.resolve(), (
                f"{name} did not follow {HOME_VAR}: {resolved}"
            )

    def test_an_empty_value_is_ignored_rather_than_making_the_root_the_drive(self, tmp_path):
        """Empty means "unset" -- otherwise a blank var would relocate to os.sep."""
        result = _resolve("")
        assert Path(result[ROOT_ATTR]).resolve() == REPO_ROOT

    def test_a_relative_value_is_made_absolute(self, tmp_path):
        """Relative paths would otherwise move with the working directory."""
        result = _resolve(".")
        assert Path(result[ROOT_ATTR]).is_absolute()
