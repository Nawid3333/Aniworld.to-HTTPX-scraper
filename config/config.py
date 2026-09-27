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

from src.term import cprint as print


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
# Measured, not guessed -- on the owner's PC (~100 Mbit/s, ~20 ms to the
# site), 600 series x2 repeats, tests/throughput_sweep.py, September 2026:
#
#   HTTP/1.1, 4 seasons at once       HTTP/2 (one connection)
#   workers  pages/s  CPU  ttfb50     pages/s  CPU  ttfb50
#      8      105.7   57%    77ms       88.8   49%    93ms
#     16      134.6   80%   116ms       88.5   49%   210ms
#     24      134.3   91%   174ms       91.0   51%   306ms
#     32      122.2   94%   242ms       90.3   51%   424ms
#     48       94.7   96%   413ms       90.0   50%   630ms
#
# On HTTP/1.1 the limit is this process: one CPU core, saturated from 24
# workers, with throughput falling beyond. Not the site: zero 429/503 in any
# of the 20 settings. Not the line: 17 Mbit/s at most, 17% of it. 16 is the
# best clean setting; 24 only adds CPU. Every setting returned identical data.
#
# Since season pages are parsed once instead of twice (600 series x2,
# HTTP/1.1): 16 -> 182.1 pages/s at 75% CPU, 24 -> 178.0 at 86%, 32 -> 149.2
# at 94%. The same shape about 35% higher, still one core, so 16 stays.
#
# Parsing stays on the event loop even so: moving it to a thread was measured
# 2-2.7x SLOWER (see parse_season_html). Cheaper per page is the way forward.
NUM_WORKERS = int(os.getenv("ANIWORLD_MAX_WORKERS", "16"))

# Season pages of one series are independent GETs. Fetching them one after
# another made a series' scrape time scale linearly with its season count,
# so they are fanned out this many at a time instead. Total requests in
# flight is NUM_WORKERS * SEASON_CONCURRENCY -- raise either with care, and
# only alongside the RateGuard that reacts to the site pushing back.
SEASON_CONCURRENCY = int(os.getenv("ANIWORLD_SEASON_CONCURRENCY", "4"))

# HTTP/2 multiplexes every request over ONE connection per host; HTTP/1.1
# opens up to NUM_WORKERS * SEASON_CONCURRENCY parallel connections instead.
# This site serves one connection only so fast: HTTP/2 stays flat at ~90
# pages/s from 8 to 48 workers while time-to-first-byte grows 7x, and
# HTTP/1.1 reached ~135 (+46-51%, in both repeats, with no push-back). So
# HTTP/1.1 is the default. ANIWORLD_HTTP2=1 (or true/yes/on) switches HTTP/2
# back on; anything else, unset included, uses HTTP/1.1.
USE_HTTP2 = os.getenv("ANIWORLD_HTTP2", "").strip().lower() in ("1", "true", "yes", "on")

# If the site starts pushing back on a full run -- "Site pushed back" or
# "Session had expired; logged back in" in the log, or series failing --
# the sweep's samples did not cover that load. No code change is needed to
# back off; set these in .env instead:
#   ANIWORLD_MAX_WORKERS=8                     HTTP/1.1 with less load (105.7 pages/s above)
#   ANIWORLD_HTTP2=1 + ANIWORLD_MAX_WORKERS=8  the previous defaults: one connection


# Checkpoint frequency: serialize resume state every N completed series.
# Smaller index (≈18 MB) → moderate interval.
CHECKPOINT_EVERY = int(os.getenv("ANIWORLD_CHECKPOINT_EVERY", "25"))

# ==================== TIMEOUTS ====================
HTTP_REQUEST_TIMEOUT = 20.0

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "aniworld_backup.log")

print(f"✓ Config loaded (DATA_DIR: {os.path.abspath(DATA_DIR)})")
