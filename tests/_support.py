"""Builders and fakes shared by this repo's tests.

Why this module exists
----------------------
Before it, every test that needed an index entry hand-wrote the same nested
dict, and every test that needed an HTTP response hand-wrote the same stub
class. That is fine once and a liability by the tenth time: when the shape of
a series entry changes, the change has to be found in a dozen literals spread
across several files, and the ones that are missed fail in ways that look like
product bugs.

Everything here is a *builder with defaults*, not a fixed fixture. Call it
with no arguments for a plausible object; pass only the field the test is
actually about. A test then reads as "a series with two seasons, the second
half-watched" rather than forty lines of dict.

Adding to this module
---------------------
Keep builders total (every field has a working default) and keep them free of
assertions -- a builder that validates makes failures surface in setup rather
than in the test. Fakes record what they were asked for so a test can assert
on the call, not just the result.
"""

from __future__ import annotations

import builtins
import contextlib
import io
import json
import os
import tempfile
from pathlib import Path

from config.config import SITE_URL

# ── series / season / episode builders ──────────────────────────────────────
# The index stores a list of series; each has seasons; each season has
# episodes. These three mirror that nesting so a test can build any depth of
# it without knowing the layout by heart.

SERIES_PATH = "/anime/stream/"

# Site capabilities the three sibling suites branch on. Stated here once so a
# test that does not apply to a site skips with a reason, rather than failing
# and being "fixed" by weakening the assertion for everyone. bs.to genuinely
# has no subscribe/watchlist feature, which is why its scraper never records
# one and its report never prints one.
SUPPORTS_SUBSCRIPTIONS = True


def episode(number: int = 1, *, watched: bool = False, title_ger: str = "", title_eng: str = "", **extra) -> dict:
    """One episode row as the scraper stores it."""
    ep: dict = {"number": number, "watched": watched}
    if title_ger:
        ep["title_ger"] = title_ger
    if title_eng:
        ep["title_eng"] = title_eng
    ep.update(extra)
    return ep


def season(label: str | int = 1, *, episodes: int = 12, watched: int = 0, **extra) -> dict:
    """One season, with ``episodes`` rows of which the first ``watched`` are seen.

    ``watched`` is a count rather than a list because that is how tests
    actually talk about it ("half-watched", "finished"). Pass ``eps=`` through
    ``extra`` only when a test needs irregular episode numbers.
    """
    eps = extra.pop("eps", None)
    if eps is None:
        eps = [episode(n, watched=n <= watched) for n in range(1, episodes + 1)]
    entry = {
        "season": label if isinstance(label, str) else f"Season {label}",
        "episodes": eps,
        "total_episodes": len(eps),
        "watched_episodes": sum(1 for e in eps if e.get("watched")),
    }
    entry.update(extra)
    return entry


def series(
    title: str = "Demo Series",
    *,
    slug: str = "",
    host: str = "",
    seasons: list[dict] | int = 1,
    watched: int = 0,
    episodes_per_season: int = 12,
    **extra,
) -> dict:
    """One index entry.

    ``seasons`` takes either a list of built seasons or a count, so the common
    case ("three seasons, all unwatched") stays a single argument.
    """
    slug = slug or title.lower().replace(" ", "-").replace("'", "")
    base = host or SITE_URL.rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}" if base else ""
    if isinstance(seasons, int):
        seasons = [
            season(n, episodes=episodes_per_season, watched=watched if n == 1 else 0) for n in range(1, seasons + 1)
        ]
    url = f"{base}{SERIES_PATH}{slug}"
    entry = {
        "title": title,
        "url": url,
        "link": url,
        "seasons": seasons,
        "total_seasons": len(seasons),
        "total_episodes": sum(s["total_episodes"] for s in seasons),
        "watched_episodes": sum(s["watched_episodes"] for s in seasons),
    }
    entry["unwatched_episodes"] = entry["total_episodes"] - entry["watched_episodes"]
    entry.update(extra)
    return entry


def write_index(entries: list[dict], directory: str | os.PathLike | None = None) -> str:
    """Write an index file and return its path. Creates a temp dir if needed."""
    directory = directory or tempfile.mkdtemp()
    path = Path(directory) / "series_index.json"
    path.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")
    return str(path)


# ── HTTP fakes ──────────────────────────────────────────────────────────────


class FakeResponse:
    """Enough of httpx.Response for the code under test.

    ``headers`` defaults to a real dict so ``.get("Retry-After")`` works
    without every caller supplying one.
    """

    def __init__(self, status_code: int = 200, text: str = "", *, headers: dict | None = None, url: str = ""):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.url = url
        self.request = None

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class RecordingClient:
    """An async client that records every URL and replays canned responses.

    ``responses`` may be a single response (reused for every request), a list
    consumed in order, or a dict keyed by a substring of the URL. The dict form
    is what most tests want: it says "when asked for a season page, answer
    this" without pinning the exact request order.
    """

    def __init__(self, responses=None, *, default: FakeResponse | None = None):
        self.requested: list[str] = []
        self._responses = responses
        self._default = default or FakeResponse(200, "")

    def _pick(self, url: str) -> FakeResponse:
        if isinstance(self._responses, dict):
            for fragment, response in self._responses.items():
                if fragment in url:
                    return response
            return self._default
        if isinstance(self._responses, list):
            return self._responses.pop(0) if self._responses else self._default
        return self._responses or self._default

    async def get(self, url, *args, **kwargs):
        self.requested.append(str(url))
        return self._pick(str(url))

    async def post(self, url, *args, **kwargs):
        self.requested.append(str(url))
        return self._pick(str(url))

    async def request(self, method, url, *args, **kwargs):
        self.requested.append(str(url))
        return self._pick(str(url))

    async def aclose(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


# ── driving the interactive menus ───────────────────────────────────────────


@contextlib.contextmanager
def scripted_input(*answers: str, default: str = ""):
    """Answer every ``input()`` call from ``answers``, then from ``default``.

    Menu handlers are where several real bugs lived, so they need to be
    testable. Running out of scripted answers yields ``default`` rather than
    raising, so a test only has to script the prompts it cares about; set
    ``default`` to something that cancels if a stray prompt would otherwise
    destroy data.
    """
    remaining = list(answers)
    asked: list[str] = []

    def fake_input(prompt: str = "") -> str:
        asked.append(prompt)
        return remaining.pop(0) if remaining else default

    real = builtins.input
    builtins.input = fake_input
    try:
        yield asked
    finally:
        builtins.input = real


@contextlib.contextmanager
def captured_output():
    """Capture stdout and yield the buffer, so a test can assert on what was shown."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        yield buffer
