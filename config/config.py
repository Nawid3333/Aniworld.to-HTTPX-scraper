"""
Configuration for the AniWorld.to HTTPX scraper.

Loads credentials from .env file, sets paths, and scraping options.
"""

import contextlib
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


def configure_console() -> None:
    """Make arrow/box-drawing output safe on any code page.

    A redirected pipe or a legacy Windows code page falls back to cp1252,
    which cannot encode "→" or "─" -- printing the very first status
    line would kill the run with a UnicodeEncodeError. ``errors="replace"``
    guarantees no crash even where UTF-8 itself is refused.

    Called at import time because this module is the earliest one every
    entry point (main.py, the test suite) pulls in, and it prints on import.
    """
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


configure_console()

# ==================== PROJECT HOME ====================
# Every path this program reads or writes -- .env, data/, logs/ and the default
# batch file -- hangs off one directory, so there is a single thing to point
# somewhere else.
#
# Unset, it resolves to the repo checkout exactly as it always has: this file
# lives in config/, so its parent is the project root. Running from a clone is
# therefore byte-for-byte unchanged.
#
# ANIWORLD_HOME overrides it, and that is what makes an *installed* copy usable.
# Installed into a venv, config/ sits inside site-packages, where no user can
# reasonably be expected to find a .env to edit; pointing ANIWORLD_HOME at a real
# folder gives the program a writable home it owns.
_DEFAULT_HOME = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
PROJECT_HOME = os.path.abspath(os.environ.get("ANIWORLD_HOME") or _DEFAULT_HOME)

# Load environment variables from .env at import time so every module that
# imports from this config sees the correct values immediately.
ENV_FILE = os.path.join(PROJECT_HOME, ".env")
load_dotenv(ENV_FILE)


# The credentials template, written on first run by ensure_env_file(). This is
# the single source of truth for it: tests/test_env_bootstrap.py asserts that
# .env.example matches, so the shipped example cannot drift from what someone
# installing the package actually receives.
ENV_TEMPLATE = """# AniWorld.to Credentials
# Fill in the values below. This file is never committed.
ANIWORLD_EMAIL=
ANIWORLD_PASSWORD=
"""


def ensure_env_file():
    """Write ENV_TEMPLATE to ENV_FILE if no .env exists there yet.

    Returns the path written, or None when a file was already present -- an
    existing .env is never read, altered or overwritten. Called from the CLI
    entry point rather than at import time, because importing this module must
    stay free of side effects: the test suite imports it constantly.
    """
    if os.path.exists(ENV_FILE):
        return None
    os.makedirs(os.path.dirname(ENV_FILE) or ".", exist_ok=True)
    with open(ENV_FILE, "w", encoding="utf-8") as handle:
        handle.write(ENV_TEMPLATE)
    return ENV_FILE


def _validate_and_normalize_url(url: str) -> str:
    """Validate and normalize a URL, raising ValueError for invalid URLs."""
    if not url:
        raise ValueError("URL cannot be empty")

    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Parse and validate
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Invalid URL: {url}")
        return url.rstrip("/")
    except Exception as e:
        raise ValueError(f"Invalid URL '{url}': {e}") from e


# Site configuration (primary + fallback mirrors)
# NOTE: The IP fallback only supports HTTP (no TLS). It is a last-resort
# fallback — credentials are sent unencrypted when this host is used.
_SITE_URLS = [
    "https://aniworld.to",
    "https://aniworld.cc",
    "http://186.2.175.111",
]

SITE_URLS = []
_seen = set()
for _url in _SITE_URLS:
    try:
        _normalized = _validate_and_normalize_url(_url)
        if _normalized not in _seen:
            _seen.add(_normalized)
            SITE_URLS.append(_normalized)
    except ValueError:
        print(f"⚠ Warning: Invalid site URL skipped: {_url}")

# Backwards-compatible alias: the first configured URL is the canonical primary.
SITE_URL = SITE_URLS[0] if SITE_URLS else ""

# Compute valid series hosts from SITE_URLS for URL validation
_VALID_HOSTS = set()
for _url in SITE_URLS:
    try:
        _parsed = urlparse(_url)
        if _parsed.netloc:
            _VALID_HOSTS.add(_parsed.netloc)
    except Exception:
        pass
VALID_SERIES_HOSTS = frozenset(_VALID_HOSTS)

# ==================== CREDENTIALS ====================
EMAIL = os.getenv("ANIWORLD_EMAIL", "")
PASSWORD = os.getenv("ANIWORLD_PASSWORD", "")

# ==================== DIRECTORIES ====================
DATA_DIR = os.path.join(PROJECT_HOME, "data")
LOGS_DIR = os.path.join(PROJECT_HOME, "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# ==================== FILE PATHS ====================
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Default batch file for single/batch URL import
# Edit DEFAULT_BATCH_FILE_PATH below to change the default batch file
DEFAULT_BATCH_FILE_PATH = os.path.join(PROJECT_HOME, "series_urls.txt")
DEFAULT_BATCH_FILE = os.path.abspath(DEFAULT_BATCH_FILE_PATH)

# ==================== SCRAPING SETTINGS ====================
# Measured, not guessed: a worker sweep over a representative sample of
# this catalogue (median 1 season, matching the real distribution) found
# 43.9 pages/s at 8, vs 35.9 at 6 and 31.3 at 12.
# Re-measured after workers began sharing one logged-in session: the old
# per-worker login both skewed the comparison and cost real throughput.
# Past the peak the season fan-out already keeps pool_workers *
# SEASON_CONCURRENCY requests in flight, so more workers only add load.
#
# Where the time actually goes, measured with the built-in PhaseProfiler
# over 300 series x2 shuffled passes at these settings:
#   network 98.8%   parse 1.1%   checkpoint <0.1%
# Parsing costs 9% of ONE core across the run, so the scrape is bound by
# the network and not by this process. Offloading parse off the event loop was
# already measured 2-2.7x SLOWER (see parse_season_html), and the lxml parser
# cut per-page parse time another 5.7x on top, so there is nothing left to
# win here. Do not reopen this without a fresh profile showing otherwise.
NUM_WORKERS = int(os.getenv("ANIWORLD_MAX_WORKERS", "8"))

# Season pages of one series are independent GETs. Fetching them one after
# another made a series' scrape time scale linearly with its season count,
# so they are fanned out this many at a time instead. Total requests in
# flight is NUM_WORKERS * SEASON_CONCURRENCY -- raise either with care, and
# only alongside the RateGuard that reacts to the site pushing back.
SEASON_CONCURRENCY = int(os.getenv("ANIWORLD_SEASON_CONCURRENCY", "4"))


# Checkpoint frequency: serialize resume state every N completed series.
# Smaller index (≈18 MB) → moderate interval.
CHECKPOINT_EVERY = int(os.getenv("ANIWORLD_CHECKPOINT_EVERY", "25"))

# ==================== TIMEOUTS ====================
HTTP_REQUEST_TIMEOUT = 20.0

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "aniworld_backup.log")

print(f"✓ Config loaded (DATA_DIR: {os.path.abspath(DATA_DIR)})")
