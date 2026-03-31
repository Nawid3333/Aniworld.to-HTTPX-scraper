#!/usr/bin/env python3
"""
AniWorld.to Anime Scraper and Index Manager (HTTPX version)
Automatically scrapes your watched anime from aniworld.to and maintains a local index.
No browser needed — uses httpx async HTTP client.
"""

import copy
import json
import logging
import logging.handlers
import os
import shutil
import sys
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, LOG_FILE
from src.index_manager import (
    IndexManager, confirm_and_save_changes, show_vanished_series,
    _extract_slug_from_field, get_episode_counts,
)
from src.scraper import AniWorldScraper

# Pre-compiled regex for URL validation
import re
_ANIME_URL_RE = re.compile(r'/anime/stream/[^/]+')


def _extract_slug(entry):
    """Extract series slug from an index entry using link (primary) or url (fallback)."""
    slug = _extract_slug_from_field(entry.get('link', ''))
    if slug:
        return slug
    slug = _extract_slug_from_field(entry.get('url', ''))
    if slug:
        title = entry.get('title', '?')
        print(f"  ⚠ Used URL fallback for slug extraction: {title}")
        logger.warning(f"Used URL fallback for slug extraction: {title}")
        return slug
    return None


_MODE_LABELS = {
    'all_series': 'Scrape all anime (option 1)',
    'new_only': 'Scrape NEW anime only (option 2)',
    'batch': 'Batch add (option 3)',
    'subscribed': 'Subscribed anime (option 5)',
    'watchlist': 'Watchlist anime (option 5)',
    'both': 'Subscribed+Watchlist anime (option 5)',
    'retry': 'Retry failed (option 6)',
}


def _check_checkpoint(expected_mode):
    """Check for an existing checkpoint and prompt the user to resume or discard."""
    checkpoint_mode = AniWorldScraper.get_checkpoint_mode(DATA_DIR)
    if checkpoint_mode is None:
        return {'ok': True, 'resume': False}

    checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
    saved_label = _MODE_LABELS.get(checkpoint_mode, checkpoint_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)

    if checkpoint_mode == expected_mode:
        print(f"\n⚠ Checkpoint found from a previous \"{saved_label}\" run!\n")
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == 'y':
            return {'ok': True, 'resume': True}
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}
    else:
        print(f"\n⚠ A checkpoint exists from a different mode: \"{saved_label}\"")
        print(f"   You are about to run: \"{expected_label}\"\n")
        discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}


# Configure logging with rotation
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def print_header():
    print("\n" + "="*60)
    print("  ANIWORLD.TO ANIME SCRAPER & INDEX MANAGER (httpx)")
    print("="*60 + "\n")


def print_completed_series_alerts(index_manager=None):
    """Alert user about series that need attention:
    1. Fully watched but not subscribed
    2. Ongoing (started but incomplete) but not on watchlist
    """
    try:
        if index_manager is None:
            index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        completed_not_sub = []
        ongoing_no_wl = []

        for s in index_manager.series_index.values():
            total, watched = get_episode_counts(s)
            subscribed = s.get('subscribed', False)
            watchlist = s.get('watchlist', False)

            if total > 0 and watched == total and not subscribed:
                completed_not_sub.append(s)
            elif total > 0 and 0 < watched < total and not watchlist:
                ongoing_no_wl.append(s)

        if completed_not_sub:
            completed_not_sub.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print(f"⚠ {len(completed_not_sub)} COMPLETED ANIME — NOT SUBSCRIBED:")
            print("─" * 70)
            for s in completed_not_sub:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider subscribing or leaving as-is.")
            print("⚠" * 35)

            rescrape = input("\nRescrape these anime to update Sub/WL status? (y/n): ").strip().lower()
            if rescrape == 'y':
                urls = [s.get('url') for s in completed_not_sub if s.get('url')]
                if not urls:
                    print("✗ No URLs found for these anime")
                else:
                    print(f"\n→ Rescraping {len(urls)} completed anime...")
                    _run_scrape_and_save(
                        run_kwargs=dict(url_list=urls, parallel=False),
                        description=f"Rescrape completed anime ({len(urls)})",
                        success_msg=f"Rescrape completed! {len(urls)} anime updated.",
                        no_data_msg="No data scraped",
                    )

        if ongoing_no_wl:
            ongoing_no_wl.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print(f"⚠ {len(ongoing_no_wl)} ONGOING ANIME — NOT ON WATCHLIST:")
            print("─" * 70)
            for s in ongoing_no_wl:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider adding them to your watchlist.")
            print("⚠" * 35)

    except Exception as e:
        logger.error(f"Error printing series alerts: {e}")


def check_disk_space(min_mb=100):
    """Check if enough disk space is available."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print(f"\n✗ WARNING: Low disk space!")
            print(f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            print(f"  Please free up disk space before scraping.\n")
            return False
        return True
    except Exception as e:
        logger.warning(f"Could not check disk space: {e}")
        return True


def validate_credentials():
    """Check that credentials are configured."""
    if not EMAIL or not PASSWORD:
        print("\n✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Create a '.env' file in the config/ directory")
        print("2. Add your aniworld.to email and password:")
        print("   ANIWORLD_EMAIL=your_email@example.com")
        print("   ANIWORLD_PASSWORD=your_password")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():
    print("\nOptions:")
    print("  1. Scrape all anime")
    print("  2. Scrape only NEW anime")
    print("  3. Single link / batch add")
    print("  4. Generate report")
    print("  5. Scrape subscribed/watchlist anime")
    print("  6. Retry failed scrapes")
    print("  7. Pause scraping")
    print("  8. Exit\n")


def _run_scrape_and_save(run_kwargs, description, success_msg, no_data_msg,
                        pre_save_hook=None, vanished_scope=None):
    """Common pattern: create scraper, run, confirm & save, handle errors.

    Args:
        pre_save_hook: Optional callable(scraper, pre_index) called after scraping
                       but before confirm_and_save. Can modify scraper.series_data.
        vanished_scope: Override scope for show_vanished_series (default: auto-detect).
    """
    pre_index = IndexManager(SERIES_INDEX_FILE) if pre_save_hook else None
    try:
        scraper = AniWorldScraper()
        scraper.run(**run_kwargs)

        if scraper.series_data:
            if pre_save_hook:
                pre_save_hook(scraper, pre_index)

            index_manager = IndexManager(SERIES_INDEX_FILE)

            if scraper.all_discovered_series is not None:
                all_slugs = {_extract_slug(s) for s in scraper.all_discovered_series} - {None}
                scope = vanished_scope or ('new_only' if run_kwargs.get('new_only') else 'all')
                show_vanished_series(index_manager.series_index, all_slugs, scope)

            if confirm_and_save_changes(scraper.series_data, description, index_manager):
                print(f"\n✓ {success_msg}")
                print_completed_series_alerts(index_manager)
                logger.info(success_msg)
        else:
            print(f"\n⚠ {no_data_msg}")
            logger.warning(no_data_msg)

        scraper.clear_checkpoint()

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} anime failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print(f"\n⚠ Scraping interrupted by Ctrl+C")
        if 'scraper' in locals() and scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            if confirm_and_save_changes(scraper.series_data, description, index_manager):
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info(f"{description} interrupted — partial data saved")
        if 'scraper' in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} anime failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper if 'scraper' in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in {description}: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error(f"Unexpected error in {description}: {e}")
    return None


def scrape_all_series():
    """Scrape all anime with sequential or parallel mode selection"""
    print("\n→ Starting AniWorld.to complete scraper (httpx)...\n")

    chk = _check_checkpoint('all_series')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Sequential (slower, but most reliable)")
    print("  2. Parallel (faster, uses multiple sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or '2'

    if mode_choice == '0':
        return
    if mode_choice not in ['1', '2']:
        print("⚠ Invalid choice, using default (parallel)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'

    _run_scrape_and_save(
        run_kwargs=dict(resume_only=resume, parallel=use_parallel),
        description="All anime scrape",
        success_msg="Scraping completed and saved!",
        no_data_msg="No anime data scraped",
    )


def scrape_new_series():
    """Scrape only new anime not yet in the index"""
    print("\n→ Starting AniWorld.to scraper (NEW anime only)...\n")

    chk = _check_checkpoint('new_only')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    _run_scrape_and_save(
        run_kwargs=dict(new_only=True, resume_only=resume),
        description="New anime data",
        success_msg="New anime scraping completed successfully!",
        no_data_msg="No new anime found",
    )


def single_or_batch_add():
    """Add single anime by URL or batch from file with auto-detect"""
    default_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single anime")
    print("  • Enter filename → uses that file for batch")
    print(f"  • Press Enter → uses default (series_urls.txt)")
    print("  • Type 0   → back to main menu\n")

    user_input = input(f"Enter [default: series_urls.txt]: ").strip()

    if user_input == '0':
        return
    if not user_input:
        user_input = default_file

    if user_input.startswith(('http://', 'https://')):
        add_single_series(user_input)
    else:
        if not os.path.exists(user_input):
            print(f"✗ File not found: {user_input}")
            return
        batch_add_from_file(user_input)


def add_single_series(url):
    """Add a single anime to the index by URL"""
    print(f"\n→ Scraping single anime: {url}\n")

    if not _ANIME_URL_RE.search(urlparse(url).path):
        print("✗ Invalid aniworld.to anime URL format")
        return

    _run_scrape_and_save(
        run_kwargs=dict(single_url=url, parallel=False),
        description="Single anime",
        success_msg="Anime added/updated successfully!",
        no_data_msg="No data scraped for this anime",
    )


def batch_add_from_file(file_path):
    """Add multiple anime from a text file containing URLs"""
    try:
        urls = []
        skipped = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                url = line.strip()
                if not url or url.startswith('#'):
                    continue
                parsed = urlparse(url)
                if parsed.scheme and parsed.scheme not in ('http', 'https'):
                    skipped.append((line_num, url))
                    continue
                if not _ANIME_URL_RE.search(parsed.path):
                    skipped.append((line_num, url))
                    continue
                urls.append(url)
        if skipped:
            print(f"⚠ Skipped {len(skipped)} invalid URL(s):")
            for line_num, bad_url in skipped[:5]:
                print(f"  Line {line_num}: {bad_url[:80]}")
            if len(skipped) > 5:
                print(f"  ... and {len(skipped) - 5} more")
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error(f"Failed to read file {file_path}: {e}")
        return

    if not urls:
        print("✗ No valid URLs found in file")
        return

    print(f"✓ Found {len(urls)} valid URL(s) in file\n")
    print("URLs to process:")
    for url in urls[:5]:
        print(f"  • {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")

    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != 'y':
        print("✗ Cancelled")
        return

    chk = _check_checkpoint('batch')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print(f"\n→ Starting batch scraper for {len(urls)} anime...\n")

    run_kwargs = dict(url_list=urls, resume_only=resume, parallel=True)

    _run_scrape_and_save(
        run_kwargs=run_kwargs,
        description=f"Batch add ({len(urls)} anime)",
        success_msg=f"Batch add completed! {len(urls)} anime processed.",
        no_data_msg="No data scraped",
    )


def _show_ongoing_and_export(report, index_manager):
    """Show ongoing anime and offer to export their URLs to series_urls.txt"""
    ongoing_count = report['categories']['ongoing']['count']
    if ongoing_count == 0:
        return

    print(f"ONGOING ANIME ({ongoing_count}):")
    ongoing_titles = report['categories']['ongoing']['titles']
    for title in ongoing_titles[:10]:
        print(f"  - {title}")
    if ongoing_count > 10:
        print(f"  ... and {ongoing_count - 10} more\n")

    export = input(f"\nExport {ongoing_count} ongoing anime URLs to series_urls.txt? (y/n): ").strip().lower()
    if export == 'y':
        try:
            urls = []
            for title in ongoing_titles:
                series_data = index_manager.series_index.get(title, {})
                url = series_data.get('url') or series_data.get('link')
                if url:
                    if not url.startswith('http'):
                        url = f"https://aniworld.to{url}"
                    urls.append(url)

            if urls:
                urls_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
                with open(urls_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(urls) + '\n')
                print(f"\n✓ Exported {len(urls)} URLs to series_urls.txt")
                print(f"  → Use option 3 (Batch add from file) to rescrape these anime")
                logger.info(f"Exported {len(urls)} URLs to series_urls.txt")
            else:
                print("\n⚠ Could not extract URLs from ongoing anime")
        except Exception as e:
            print(f"\n✗ Failed to export URLs: {str(e)}")
            logger.error(f"Failed to export URLs: {e}")


def _print_report_summary(report, report_file, filter_name=None):
    """Print enhanced report summary to console."""
    stats = report['metadata']['statistics']
    ongoing_count = report['categories']['ongoing']['count']
    not_started_count = report['categories']['not_started']['count']
    not_started_sub_wl_count = report['categories']['not_started_subscribed_watchlist']['count']
    waiting_count = report['categories']['waiting_for_new_episodes']['count']

    header = f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')}):" if filter_name else "REPORT SUMMARY:"
    print(f"\n" + "-"*70)
    print(header)
    print("-"*70)
    print(f"  Total anime:         {stats['total_series']}")
    print(f"  Completed (100%):    {stats.get('completed_count', stats['watched'])}")
    print(f"  Ongoing (started):   {stats.get('ongoing_count', ongoing_count)}")
    if waiting_count > 0:
        print(f"  Waiting for new eps: {waiting_count}")
    print(f"  Not started (0%):    {stats.get('not_started_count', not_started_count)}")
    if not_started_sub_wl_count > 0:
        print(f"  Not started (Sub/WL):{not_started_sub_wl_count}")
    print(f"  Total episodes:      {stats['total_episodes']}")
    print(f"  Watched episodes:    {stats['watched_episodes']}")
    print(f"  Unwatched episodes:  {stats.get('unwatched_episodes', 0)}")
    print(f"  Avg episodes/anime:  {stats.get('average_episodes_per_series', 0)}")
    print(f"  Average completion:  {stats['average_completion']:.1f}%")
    print(f"  Subscribed:          {stats.get('subscribed_count', 0)}")
    print(f"  Watchlist:           {stats.get('watchlist_count', 0)}")
    print(f"  Both (Sub+WL):       {stats.get('both_subscribed_and_watchlist', 0)}")

    dist = stats.get('completion_distribution', {})
    if dist:
        parts = [f"{k}: {v}" for k, v in dist.items()]
        print(f"\n  Completion Distribution:")
        print(f"    {'  |  '.join(parts)}")

    most = stats.get('most_completed_series', [])
    if most:
        print(f"\n  Most Completed Ongoing (top {len(most)}):")
        for i, s in enumerate(most, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    least = stats.get('least_completed_series', [])
    if least:
        print(f"  Least Completed Ongoing (bottom {len(least)}):")
        for i, s in enumerate(least, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    print(f"\n  Saved to:            {report_file}")
    print("-"*70 + "\n")


def generate_report():
    """Generate anime report with optional filtering by subscription status"""
    print("\n→ Generate report")
    print("  1. Full report (all anime)")
    print("  2. Subscription/watchlist filtered report")
    print("  0. Back\n")

    choice = input("Choose report type (0-2): ").strip()

    if choice == '0':
        return

    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)

        if choice == '1':
            print("\n→ Generating full report...")
            report = index_manager.get_full_report()
            report_file = os.path.join(DATA_DIR, 'series_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file)
            logger.info("Full report generated")
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        elif choice == '2':
            print("\n→ Subscription/watchlist report")
            print("  1. Only subscribed")
            print("  2. Only watchlist")
            print("  3. Both")
            print("  0. Back\n")

            sub_choice = input("Choose filter (0-3): ").strip()

            if sub_choice == '0':
                return

            if sub_choice == '1':
                print("\n→ Generating report for subscribed anime...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=False)
                filter_name = "subscribed_only"
            elif sub_choice == '2':
                print("\n→ Generating report for watchlist anime...")
                report = index_manager.get_full_report(filter_subscribed=False, filter_watchlist=True)
                filter_name = "watchlist_only"
            elif sub_choice == '3':
                print("\n→ Generating report for subscribed AND watchlist...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=True)
                filter_name = "both_subscribed_watchlist"
            else:
                print("⚠ Invalid choice")
                return

            report_file = os.path.join(DATA_DIR, f'series_report_{filter_name}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file, filter_name)
            logger.info(f"Filtered report generated: {filter_name}")
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        else:
            print("⚠ Invalid choice")

    except Exception as e:
        print(f"\n✗ Error generating report: {str(e)}")
        logger.error(f"Error generating report: {e}")


def _inject_disappeared_series(scraper, pre_index, source):
    """Inject stubs for series no longer on account pages so merge can prompt."""
    discovered_slugs = {_extract_slug(s) for s in (scraper.all_discovered_series or [])} - {None}
    failed_slugs = {_extract_slug(fl) for fl in scraper.failed_links if isinstance(fl, dict)} - {None}
    scraped_titles = {s.get('title') for s in scraper.series_data if s.get('title')}

    for field, sources in [('watchlist', ('watchlist', 'both')), ('subscribed', ('subscribed', 'both'))]:
        if source not in sources:
            continue
        injected = []
        for title, entry in pre_index.series_index.items():
            if not entry.get(field, False):
                continue
            slug = _extract_slug(entry)
            if not slug or slug in discovered_slugs or slug in failed_slugs:
                continue
            if title in scraped_titles:
                # Already scraped — just flip the flag
                for item in scraper.series_data:
                    if item.get('title') == title:
                        item[field] = False
                        break
            else:
                stub = copy.deepcopy(entry)
                stub[field] = False
                scraper.series_data.append(stub)
                scraped_titles.add(title)
            injected.append(title)
        if injected:
            print(f"\n  ⚠ {len(injected)} anime no longer {field} (will prompt for confirmation):")
            for name in injected:
                print(f"    • {name}")


def scrape_subscribed_watchlist():
    """Scrape subscribed/watchlist anime with disappeared-series detection."""
    print("\n→ Scrape subscribed/watchlist anime")
    print("  1. Only subscribed")
    print("  2. Only watchlist")
    print("  3. Both")
    print("  0. Back\n")

    sub_choice = input("Choose source (0-3) [default: 3]: ").strip() or '3'
    if sub_choice == '0':
        return
    source = {'1': 'subscribed', '2': 'watchlist'}.get(sub_choice, 'both')

    chk = _check_checkpoint(source)
    if not chk['ok']:
        print("✗ Cancelled")
        return

    def _hook(scraper, pre_index):
        _inject_disappeared_series(scraper, pre_index, source)

    _run_scrape_and_save(
        run_kwargs=dict(account_source=source, resume_only=chk['resume']),
        description="Account anime",
        success_msg="Account anime scraping completed!",
        no_data_msg="No anime found on account pages",
        pre_save_hook=_hook,
        vanished_scope=source,
    )


def retry_failed_series():
    """Retry previously failed anime in sequential mode"""
    print("\n→ Retry failed anime from last run\n")

    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    temp_scraper = AniWorldScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed anime found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed anime from last run")
    print("\n→ Starting retry in sequential mode (for reliability)...")

    _run_scrape_and_save(
        run_kwargs=dict(retry_failed=True, parallel=False, resume_only=resume),
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data from retry",
    )


def pause_scraping():
    """Create a pause file to signal workers to pause scraping"""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}")
        print("Workers will pause at next checkpoint.\n")
        logger.info(f"Pause file created: {pause_file}")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error(f"Failed to create pause file: {e}")


def main():
    """Main application loop"""
    print_header()

    if not validate_credentials():
        sys.exit(1)

    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            sys.exit(1)

    while True:
        show_menu()
        choice = input("Enter your choice (1-8): ").strip()

        if not choice.isdigit() or not (1 <= int(choice) <= 8):
            print("✗ Invalid choice. Please enter a number between 1 and 8.")
            continue

        if choice in ['1', '2', '3', '5', '6']:
            if not check_disk_space():
                print("⚠ Aborting due to low disk space.")
                continue

        if choice == '1':
            scrape_all_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            single_or_batch_add()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            scrape_subscribed_watchlist()
        elif choice == '6':
            retry_failed_series()
        elif choice == '7':
            pause_scraping()
        elif choice == '8':
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()
