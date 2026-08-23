# AniWorld.to Anime Scraper & Index Manager (httpx)

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](./LICENSE)

Scrapes watched anime from **aniworld.to** and maintains a local JSON index.
Uses **httpx** (no browser needed) with a multi-session architecture for fast, parallel scraping.

## Features

- **Multi-session parallel scraping** — 10 concurrent httpx sessions by default (configurable in `config/config.py` or via the `ANIWORLD_MAX_WORKERS` env var)
- **Smart per-series ETA estimation** — each series stores its own `avg_scrape_seconds` (exponential moving average for ETA prediction) and `scrape_duration_seconds` (actual duration of the most recent scrape) in the index. ETA is predicted by summing those per-series averages for the remaining work, then blended with the live session rate (historical 85%→45% as progress increases). Because the database is stable, per-series history is the best predictor.
- **Checkpoint & resume** — automatically saves progress every 25 anime; resume after interruptions (Ctrl+C safe)
- **New anime detection** — detects newly added anime on your account and lists them before scraping
- **Vanished anime detection** — alerts when anime disappear from your account
- **Subscription & watchlist tracking** — scrapes subscription/watchlist status and detects changes
- **Language detection** — tracks available languages per episode (German dub, German sub, English sub)
- **Bilingual episode titles** — stores both German and English titles per episode
- **Alternative titles** — extracts alternative titles from series pages
- **Series descriptions** — persists description text for each anime
- **Ignored seasons** — manually defined list of placeholder seasons (e.g. episode 0 only) skipped via `.ignored_seasons.json`
- **Ignored series** — skip specific anime via `.ignored_series.json`
- **Completed anime alerts** — warns about fully watched anime not subscribed, and ongoing anime not on watchlist
- **Batch URL import** — import anime from a text file (comments supported)
- **Failed anime retry** — automatically tracks failures for later bulk retry
- **Pause/resume** — create a `.pause_scraping` file to gracefully pause workers
- **Report generation** — full statistics with subscription/watchlist filtering and ongoing anime export
- **Data integrity checks** — detects episode count drops, season removals, watched-status corruption, and title changes before merging; offers to delete & rescrape critical series
- **Atomic file writes** — all JSON writes use temp file + replace to prevent corruption
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- Python 3.10+ — developed and tested on 3.14. The 3.10 floor comes from
  `zip(strict=True)` and PEP 604 `X | None` annotations evaluated at runtime;
  versions between 3.10 and 3.13 are expected to work but are not tested.
- Dependencies: `httpx`, `beautifulsoup4`, `lxml`, `h2`, `python-dotenv`

`lxml` and `h2` are what make the scraper fast: pages parse ~1.4x quicker than with
the stdlib parser, and HTTP/2 lets one connection carry many requests. Both fall
back gracefully if unavailable, at the old speed.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file inside the `config/` directory (see `config/.env.example`):

```
ANIWORLD_EMAIL=your_email@example.com
ANIWORLD_PASSWORD=your_password
```

The default batch file is `series_urls.txt` next to `main.py`. To change it, edit `DEFAULT_BATCH_FILE` in `config/config.py`.

Site URL and fallback domains are defined in `config/config.py`:

```python
SITE_URL = "https://aniworld.to"
```

Built-in fallback hosts: `aniworld.cc`, `186.2.175.111` (HTTP, no TLS).

Scraping parallelism can be adjusted via the `ANIWORLD_MAX_WORKERS` environment variable or in `config/config.py`:

```python
NUM_WORKERS = 10  # Number of parallel httpx sessions
```

## Tuning

All optional, with sensible defaults. Set them in `config/.env`.

| Variable | Default | What it does |
| --- | --- | --- |
| `ANIWORLD_MAX_WORKERS` | `8` | Concurrent scraping sessions. The default was measured on a representative sample of this catalogue, not guessed — higher is not faster, and past the peak it only adds load. |
| `ANIWORLD_SEASON_CONCURRENCY` | `4` | Season pages fetched at once per series. Total requests in flight is workers x this. |
| `ANIWORLD_CHECKPOINT_EVERY` | `25` | Save resume state every N anime. |
| `ANIWORLD_PROFILE` | unset | Set to `1` to print where a run's time actually went (network vs parse vs disk). |

## Usage

```bash
python main.py
```

### Menu Options

| #   | Option                          | Description                                                                      |
| --- | ------------------------------- | -------------------------------------------------------------------------------- |
| 1   | **Scrape all anime**            | Full scrape of all watched anime. Choose sequential or parallel mode.            |
| 2   | **Scrape only NEW anime**       | Scrapes only anime not yet in the index (faster).                                |
| 3   | **Scrape unwatched anime**      | Skips fully watched anime; focuses on ongoing/partial.                           |
| 4   | **Generate report**             | Statistics report saved to JSON, with optional subscription/watchlist filtering. |
| 5   | **Single link / batch add**     | Paste a URL for a single anime, or load URLs from a file.                        |
| 6   | **Scrape subscribed/watchlist** | Scrape anime from your subscribed list, watchlist, or both.                      |
| 7   | **Retry failed scrapes**        | Bulk retry all anime that failed in previous runs.                               |
| 8   | **Exit**                        | Clean exit.                                                                      |

> **Pausing scraping:** there is no dedicated menu option. To gracefully pause workers, create a `.pause_scraping` file in the `data/` directory (see [Pause/resume](#pauseresume) below).

### Scraping Modes (Option 1)

1. **Sequential** — one httpx client, slower but most reliable
2. **Parallel** — multiple concurrent workers (default, faster)

### Batch File Format (Option 5)

One URL per line. Lines starting with `#` are ignored:

```
https://aniworld.to/anime/stream/one-piece
https://aniworld.to/anime/stream/jujutsu-kaisen
# https://aniworld.to/anime/stream/some-paused-anime
```

### Reports (Option 4)

Reports include:

- Total anime, completed, ongoing, not started counts
- Episode counts and completion percentages
- Completion distribution and top/bottom completion rankings
- Subscription and watchlist statistics
- Language availability per series

Filter options:

- Full report (all anime)
- Subscribed only
- Watchlist only
- Both subscribed and watchlist

After report generation, you can export ongoing anime URLs to the default batch file (`series_urls.txt` by default, or `DEFAULT_BATCH_FILE` if set).

## Pause/resume

There is no menu option for pausing. To gracefully pause a running scrape, create an empty `.pause_scraping` file in the `data/` directory:

```bash
# from the project folder
touch data/.pause_scraping          # Linux / macOS
New-Item data\.pause_scraping -ItemType File   # PowerShell
```

Active workers check for this file periodically and finish their current anime before stopping. The checkpoint is saved so you can resume the run later. Delete the file to allow new scrapes to run.

## Episode 0 / Ignored Seasons

Some aniworld.to anime have an "episode 0" entry that is only a placeholder with no watch links. These cause the anime to appear incomplete.

The file `data/.ignored_seasons.json` lists known seasons with this issue. It is **not** created automatically; you must add an entry manually (or copy one from `.failed_series.json` after a scrape flags an episode 0 placeholder):

```json
[
  { "slug": "one-piece", "season": "0" },
  { "slug": "jujutsu-kaisen", "season": "0" }
]
```

Only the `slug` and `season` fields are used for matching; `url`/`link` are optional and informational. Slugs are normalized to lowercase internally, so `Jujutsu-Kaisen` and `jujutsu-kaisen` match the same anime.

Note: the scraper detects new episode 0 placeholders during a run, but it does **not** modify `.ignored_seasons.json` automatically. Add entries manually after reviewing `.failed_series.json`.

**Behavior during scraping:**

| Scenario                                            | Behavior                                                                      |
| --------------------------------------------------- | ----------------------------------------------------------------------------- |
| Season is in ignore list                            | Episode 0 silently filtered; season marked `ignored_episode_0: true` in index |
| New episode 0 detected (not in list)                | Warning printed; added to `.failed_series.json` for review                    |
| Season in ignore list but episode 0 is gone (stale) | Notification before rest of scrape; prompt to continue                        |

When ignored-season anime are found in a scrape, they are processed first (two-phase). If any new or stale entries are detected, the scraper prompts before continuing with remaining anime.

## Ignored Series

Some anime pages are empty, return a 404/502 error, or exist in the catalog with no real seasons. These make every scrape fail or show phantom unwatched entries.

The file `data/.ignored_series.json` lists anime to skip entirely during scraping. It is **not** created automatically; create it manually when needed:

```json
[
  {
    "url": "https://aniworld.to/anime/stream/empty-anime",
    "title": "Empty Anime"
  }
]
```

Only the `url` field is required for matching; `title` is optional and informational. The slug is extracted from the URL automatically.

**Behavior during scraping:**

| Scenario                                              | Behavior                                                                                 |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Anime is in ignore list                               | Skipped entirely; not fetched, not counted, not included in reports                      |
| Ignored anime page is still empty / 404 / unreachable | Printed as `✓ {title}: still empty` during re-validation                                 |
| Ignored anime now has real season content             | Warning printed: `⚠ {title}: now available! Consider removing from .ignored_series.json` |
| Ignored anime no longer appears in the catalog        | Warning printed: consider removing the stale entry                                       |

The scraper re-validates ignored anime at the start of every run and checks them against the fetched catalog. It **does not** auto-add or auto-remove entries — all changes to `.ignored_series.json` are manual.

## Project Structure

```
├── .gitignore
├── LICENSE                  # GNU GPL v3.0
├── README.md                # This file
├── main.py                  # Entry point & interactive menu
├── requirements.txt         # Python dependencies
├── ruff.toml                # Lint/format configuration
├── config/
│   ├── .env.example         # Template for your credentials
│   └── config.py            # Settings (credentials, workers, paths)
├── src/
│   ├── atomic_io.py         # Durable atomic JSON writes (shared by every writer)
│   ├── index_manager.py     # Merge, change detection, stats, reports
│   └── scraper.py           # httpx scraping engine
└── tests/
    ├── __init__.py
    ├── capture_fixtures.py  # Regenerates test fixtures from the live site
    ├── fixture_spec.py      # Which parser outputs the fixtures pin
    ├── test_golden_parse.py # Parser output pinned against real captured pages
    └── test_scraper.py      # Unit + regression tests
```

Directories created at runtime (`data/`, `logs/`) and your `.env` are not part of
the repository. Test fixtures live in `tests/fixtures/` and are generated locally
with `python tests/capture_fixtures.py`.

## Author

Nawid Salehie

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
