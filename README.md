# AniWorld.to Anime Scraper & Index Manager (httpx)

Scrapes watched anime from **aniworld.to** and maintains a local JSON index.
Uses **httpx** (no browser needed) with a multi-session architecture for fast, parallel scraping.

## Features

- **Multi-session parallel scraping** — 10 concurrent httpx sessions by default (configurable via `ANIWORLD_MAX_WORKERS` env var)
- **Checkpoint & resume** — automatically saves progress every 10 series; resume after interruptions (Ctrl+C safe)
- **New anime detection** — detects newly added anime on your account and lists them before scraping
- **Vanished anime detection** — alerts when anime disappear from your account
- **Subscription & watchlist tracking** — scrapes subscription/watchlist status and detects changes
- **Language detection** — tracks available languages per episode (German dub, German sub, English sub)
- **Bilingual episode titles** — stores both German and English titles per episode
- **Alternative titles** — extracts alternative titles from series pages
- **Series descriptions** — persists description text for each anime
- **Ignored seasons** — automatically skips placeholder seasons (e.g. episode 0 only) via `.ignored_seasons.json`
- **Ignored series** — skip specific anime via `.ignored_series.json`
- **Completed anime alerts** — warns about fully watched anime not subscribed, and ongoing anime not on watchlist
- **Batch URL import** — import anime from a text file (comments supported)
- **Failed anime retry** — automatically tracks failures for later bulk retry
- **Pause/resume** — create a `.pause_scraping` file to gracefully pause workers
- **Report generation** — full statistics with subscription/watchlist filtering and ongoing anime export
- **Atomic file writes** — all JSON writes use temp file + replace to prevent corruption
- **File locking** — prevents concurrent access corruption
- **Disk space check** — warns before scraping if free space is below 100 MB
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- Python 3.8+
- Dependencies: `httpx`, `beautifulsoup4`, `python-dotenv`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file inside the `config/` directory:

```
ANIWORLD_EMAIL=your_email@example.com
ANIWORLD_PASSWORD=your_password
```

Scraping parallelism can be adjusted via environment variable or in `config/config.py`:

```python
NUM_WORKERS = 10  # Number of parallel httpx sessions
```

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
| 8   | **Pause scraping**              | Creates `.pause_scraping` flag file for graceful worker pause.                   |
| 9   | **Exit**                        | Clean exit.                                                                      |

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

After report generation, you can export ongoing anime URLs to `series_urls.txt`.

## Project Structure

```
├── main.py                     # Entry point & interactive menu
├── requirements.txt
├── series_urls.txt             # Optional batch URL file
├── config/
│   ├── config.py               # Settings (credentials, workers, paths)
│   └── .env                    # Credentials (not committed)
├── data/
│   ├── series_index.json       # Main anime database
│   ├── series_index.json.bak*  # 3 backup generations (auto-managed)
│   ├── series_report.json      # Generated report
│   ├── .ignored_seasons.json   # Episode 0 ignore list
│   ├── .ignored_series.json    # Series to skip during scraping
│   ├── .scrape_checkpoint.json # Resume checkpoint (auto-managed)
│   ├── .failed_series.json     # Failed anime list (auto-managed)
│   └── .pause_scraping         # Pause flag file (auto-managed)
├── src/
│   ├── scraper.py              # AniWorldScraper — httpx scraping engine
│   └── index_manager.py        # IndexManager — merge, change detection, stats, reports
└── logs/
    └── aniworld_backup.log     # Rotating log file
```

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
