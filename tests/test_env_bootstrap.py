"""The .env template: where it lives, and the copy written on first run.

Two things are pinned here. ``.env.example`` in the repo root must match
``ENV_TEMPLATE`` in config, because they are two faces of one thing -- the file
a reader opens has to be the file a fresh install actually receives, or the
instructions drift away from the behaviour. And ``ensure_env_file`` must create
that file when none exists while never touching one that does: it runs on every
CLI start, so a mistake there would overwrite real credentials.

Style note for future edits
---------------------------
The bootstrap runs in a subprocess with ANIWORLD_HOME pointed at tmp_path,
because config resolves ENV_FILE at import time. Calling it in-process would
resolve against the checkout and write a .env into the repo instead.
Comparisons normalise line endings: git may check the example out with CRLF
while ENV_TEMPLATE is a Python string with LF, and that gap is not a defect.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

HOME_VAR = "ANIWORLD_HOME"
CONFIG_MODULE = "config.config"
SENTINEL = "SECRET=already-here\n"


def _norm(text: str) -> str:
    return text.replace("\r\n", "\n")


def _run(home: Path, snippet: str) -> str:
    """Import config with the project home pointed at ``home`` and run ``snippet``."""
    env = dict(os.environ)
    env[HOME_VAR] = str(home)
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", f"import {CONFIG_MODULE} as c;" + snippet],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
    )
    assert proc.returncode == 0, proc.stderr
    # config prints a banner on import in some of these repos, so the value is
    # tagged rather than assumed to be the whole of stdout.
    marker = "<<RESULT>>"
    line = next(x for x in proc.stdout.splitlines() if x.startswith(marker))
    return line[len(marker) :]


def _example() -> str:
    return _norm((REPO_ROOT / ".env.example").read_text(encoding="utf-8", newline=""))


class TestTheShippedExample:
    def test_it_matches_the_template_the_program_writes(self):
        """If these drift, the documented file stops describing the real one."""
        printed = _run(REPO_ROOT, "print('<<RESULT>>' + repr(c.ENV_TEMPLATE))").strip()
        assert _example() == _norm(ast.literal_eval(printed))

    def test_it_sits_at_the_repo_root(self):
        """Root is where dotenv, editors and every README convention look."""
        assert (REPO_ROOT / ".env.example").is_file()
        assert not (REPO_ROOT / "config" / ".env.example").exists()


class TestFirstRunBootstrap:
    def test_it_writes_the_template_when_no_env_exists(self, tmp_path):
        written = _run(tmp_path, "print('<<RESULT>>' + str(c.ensure_env_file()))").strip()
        assert Path(written) == tmp_path / ".env"
        assert _norm((tmp_path / ".env").read_text(encoding="utf-8")) == _example()

    def test_it_leaves_an_existing_env_untouched(self, tmp_path):
        """It runs on every start, so overwriting here would destroy credentials."""
        existing = tmp_path / ".env"
        existing.write_text(SENTINEL, encoding="utf-8")
        assert "None" in _run(tmp_path, "print('<<RESULT>>' + str(c.ensure_env_file()))")
        assert existing.read_text(encoding="utf-8") == SENTINEL

    def test_env_file_resolves_to_the_project_home_itself(self, tmp_path):
        """Not a config/ subfolder -- the home stays a flat folder the user owns."""
        resolved = Path(_run(tmp_path, "print('<<RESULT>>' + str(c.ENV_FILE))").strip())
        assert resolved == tmp_path / ".env"
