"""Find the worker count where the site, not this program, sets the pace.

Read-only diagnostic, in the same spirit as probe_cache_headers.py. It logs in
once per transport, then scrapes the SAME random sample of series again and
again with different settings, using the scraper's own _scrape_one_series --
the exact code a real run uses -- and reports what each setting achieved.
Nothing is written to the index, the checkpoint or the failed list; only its
own report file.

Run from the project root:

    python tests/throughput_sweep.py
    python tests/throughput_sweep.py --workers 4,8,12,16,24 --transport h2,h1 --sample 150 --repeats 2
    python tests/throughput_sweep.py --season-concurrency 2,4,8 --workers 8
    python tests/throughput_sweep.py --line-mbit 100

What it measures, per setting
-----------------------------
  pages/s      every page the site served, series + season pages. A 429 or a
               5xx is a refusal or a failure, not a page.
  series/s     what a real run's progress bar would show
  Mbit/s       bytes actually received (compressed, as they crossed the wire),
               to compare with your line speed
  ttfb p50/p90 time until the site starts answering. When this grows in step
               with the number of requests in flight while pages/s stays flat,
               the requests are waiting in a queue ON THE SITE: more workers
               only make the queue longer.
  body p50/p90 time from the headers to the last byte of the page. When this is
               what grows instead, the bytes themselves are waiting: on a full
               home line, or on a site that streams the page as it builds it.
               --line-mbit (your measured download speed) tells the two apart:
               a plateau at 70% of it or more is the line, not the site.
  429/503      the site explicitly pushing back
  cpu          this process's share of one CPU core. Near 100% means your PC
               (Python's single event loop) is the limit, not the site.
  idle         the share of worker time spent with nothing left to do: the
               queue ran dry and the worker waited for the others to finish. A
               long-running show can outlast every other worker; when much of
               a setting is spent like that, its pages/s says little about the
               worker count, and the setting needs a larger --sample.
  mismatch     series whose episode/watched counts differed from the first
               setting that scraped them -- speed must never cost accuracy.

Safety
------
A setting stops early when the site pushes back (429/503) or too many series
fail. In the default careful mode, every setting with at least as many
requests in flight (workers x season concurrency) is then skipped for that
transport: load only goes up from there. A transport whose warm-up series all
fail -- a login that did not take, a host that is down -- is not swept at all.
Settings are separated by a cool-down so one setting's load does not bleed
into the next, and each transport logs in only once -- repeated logins are
what made this site refuse logins before (see _acquire_client).

Whatever finished is saved even when the sweep is interrupted (Ctrl+C) or
crashes, marked "complete": false, so the load it put on the site is never
wasted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

import src.scraper as scraper_mod  # noqa: E402
from config.config import SERIES_INDEX_FILE  # noqa: E402
from src.scraper import AniWorldScraper  # noqa: E402

SCRAPER_CLS = AniWorldScraper
REPORT_FILE = Path(__file__).resolve().parent / "throughput_report.json"

# A setting is abandoned once this share of its series have failed. A few
# failures are normal (a vanished series, a utility page); a wave of them
# under load is the site struggling, and continuing only adds to it.
MAX_FAIL_RATIO = 0.10
# Explicit push-back tolerated within one setting before it is stopped.
MAX_PUSHBACK = 3
# Series scraped, uncounted, before a transport is measured. If every one of
# them fails, the session or the host is broken, and sweeping would only
# measure failures -- at full load.
WARMUP_SERIES = 5
TRANSPORTS = ("h2", "h1")
# Share of the measured line speed at which the line, not the site, is the
# ceiling. Short of 100%: protocol overhead and TCP never let a busy line
# carry its full rated payload.
LINE_FULL = 0.70
# Share of one core (the event loop's) past which this process, not the site,
# is at least part of the ceiling.
BUSY_CPU = 85
# A setting whose workers spent this share of their time idle -- queue empty,
# waiting on a straggler such as a 50-season show fetched SEASON_CONCURRENCY
# pages at a time -- understates its pages/s by about that much, which is more
# than the 5% the worker-count choice is made on.
IDLE_SHARE = 10


def pct(values, q):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * q))]


class Meter:
    """Counts what one setting did. Fed by client event hooks and a _get wrapper."""

    def __init__(self) -> None:
        self.ttfb: list[float] = []
        self.body: list[float] = []
        self.status: dict[int, int] = {}
        self.bytes = 0
        self.transport_errors = 0
        self.pushback = 0
        # What the connection actually spoke. Asking for HTTP/2 is only an
        # offer: a host that declines it is served over HTTP/1.1 without a
        # word, and "h2" numbers would then be HTTP/1.1 numbers.
        self.protocols: Counter[str] = Counter()
        self._started: dict[int, float] = {}
        self._headers_at: dict[int, float] = {}

    async def on_request(self, request: httpx.Request) -> None:
        self._started[id(request)] = time.perf_counter()

    async def on_response(self, response: httpx.Response) -> None:
        # httpx runs response hooks as soon as the headers are in, and reads
        # the body only afterwards: this is the line between the two timings.
        now = time.perf_counter()
        self._headers_at[id(response)] = now
        start = self._started.pop(id(response.request), None)
        if start is not None:
            self.ttfb.append(now - start)
        self.protocols[response.http_version] += 1
        self.status[response.status_code] = self.status.get(response.status_code, 0) + 1
        if response.status_code in (429, 503):
            self.pushback += 1

    def body_read(self, response: httpx.Response) -> None:
        """Called once the page's body is fully read.

        Only the response a caller finally got has its body timed. A redirect
        or a retried 5xx was stamped too, but nobody waited on its body.
        """
        headers_at = self._headers_at.pop(id(response), None)
        if headers_at is not None:
            self.body.append(time.perf_counter() - headers_at)

    @property
    def pages(self) -> int:
        """Pages actually served -- a 429 or a 5xx is a refusal or a failure, not a page."""
        return sum(n for code, n in self.status.items() if code != 429 and code < 500)


def fingerprint(result: dict):
    """What must not change between settings: the data, not the timing.

    None for a failed series. The scraper marks one with "_error" (see
    _error_result), and its zeroed counts must never be taken for data: they
    would pass as a success, hide the failure from the early stop, and show
    up as a "mismatch" against the next setting that got the series through.
    """
    if result.get("_error"):
        return None
    return (
        result.get("total_seasons"),
        result.get("total_episodes"),
        result.get("watched_episodes"),
    )


def load_sample(size: int, seed: int) -> list[dict]:
    """A fixed random sample of real series from the local index. Read-only."""
    with open(SERIES_INDEX_FILE, encoding="utf-8") as fh:
        data = json.load(fh)
    items = data if isinstance(data, list) else list(data.values())
    infos = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        url, link = entry.get("url"), entry.get("link")
        if url and link:
            infos.append({"title": entry.get("title", ""), "link": link, "url": url})
    random.Random(seed).shuffle(infos)
    return infos[:size]


async def sample_from_catalogue(scraper, client, size: int, seed: int) -> list[dict]:
    """Same, from the live catalogue, for a machine without a local index."""
    series = await scraper._get_all_series(client)
    random.Random(seed).shuffle(series)
    return series[:size]


def on_host(info: dict, site_url: str) -> dict:
    """The same series, fetched from the host this session is logged in to.

    Index entries keep the absolute URL of whichever mirror was live when they
    were scraped, and a login only counts on the host it was made on: fetched
    from another mirror, every page would come back logged out. A real run
    never meets this, because it scrapes the live catalogue, which always
    carries the active host. "url" stays as it was, so results still line up
    by series across settings.
    """
    path = urlparse(info["url"]).path
    if not path:
        return info
    return {**info, "scrape_url": f"{site_url.rstrip('/')}{path}"}


async def scrape_contained(scraper, client, info: dict) -> dict:
    """_scrape_one_series, with an unexpected exception kept to that one series.

    The real worker does the same: one unparseable page costs that series, not
    the rest of the queue. Here an escaped exception would also end the whole
    sweep, every other worker with it, before anything was saved.
    """
    try:
        return await scraper._scrape_one_series(client, on_host(info, scraper.site_url))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return scraper._error_result(info, f"unexpected error: {exc}")


async def run_setting(scraper, client, sample, workers, season_conc, cooldown, reference):
    """Scrape `sample` once with `workers` workers; return the measurements."""
    scraper_mod.SEASON_CONCURRENCY = season_conc
    scraper._rate_guard = scraper_mod.RateGuard()
    meter = Meter()
    client.event_hooks = {"request": [meter.on_request], "response": [meter.on_response]}

    original_get = scraper._get

    async def counted_get(c, url, **kwargs):
        try:
            resp = await original_get(c, url, **kwargs)
        except httpx.TransportError:
            # Connection-level failures that outlived _get's retries. A status
            # that did (a 5xx on every attempt) is already in status_counts.
            meter.transport_errors += 1
            raise
        # client.get has read the whole body by the time _get returns it.
        meter.body_read(resp)
        meter.bytes += resp.num_bytes_downloaded
        return resp

    scraper._get = counted_get
    queue: asyncio.Queue = asyncio.Queue()
    order = list(sample)
    random.shuffle(order)
    for info in order:
        queue.put_nowait(info)
    done = {"ok": 0, "failed": 0, "mismatch": 0, "stopped": None}
    mismatches: list[str] = []
    reasons: Counter[str] = Counter()

    ran_dry: list[float] = []

    async def worker():
        while not queue.empty() and not done["stopped"]:
            info = queue.get_nowait()
            result = await scrape_contained(scraper, client, info)
            fp = fingerprint(result)
            if fp is None:
                done["failed"] += 1
                reasons[result.get("_error_reason") or "unknown"] += 1
            else:
                done["ok"] += 1
                seen = reference.setdefault(info["url"], fp)
                if seen != fp:
                    done["mismatch"] += 1
                    mismatches.append(f"{info['url']}: {seen} -> {fp}")
            finished = done["ok"] + done["failed"]
            if meter.pushback >= MAX_PUSHBACK:
                done["stopped"] = f"site pushed back {meter.pushback}x (429/503)"
            elif finished >= 20 and done["failed"] / finished > MAX_FAIL_RATIO:
                top = reasons.most_common(1)[0][0]
                done["stopped"] = f"{done['failed']}/{finished} series failed (mostly: {top})"
        ran_dry.append(time.perf_counter())

    cpu0, t0 = time.process_time(), time.perf_counter()
    try:
        await asyncio.gather(*(worker() for _ in range(workers)))
    finally:
        scraper._get = original_get
        client.event_hooks = {"request": [], "response": []}
    end = time.perf_counter()
    wall = end - t0
    cpu = time.process_time() - cpu0
    idle = sum(end - t for t in ran_dry) / (workers * wall) if wall else 0

    series_done = done["ok"] + done["failed"]
    row = {
        "workers": workers,
        "season_concurrency": season_conc,
        "series": series_done,
        "series_ok": done["ok"],
        "series_failed": done["failed"],
        "failure_reasons": dict(reasons.most_common(5)),
        "pages": meter.pages,
        "wall_s": round(wall, 2),
        "idle_pct": round(idle * 100, 1),
        "pages_per_s": round(meter.pages / wall, 2) if wall else 0,
        "series_per_s": round(series_done / wall, 2) if wall else 0,
        "mbit_per_s": round(meter.bytes * 8 / wall / 1e6, 2) if wall else 0,
        "ttfb_p50_ms": round(pct(meter.ttfb, 0.5) * 1000),
        "ttfb_p90_ms": round(pct(meter.ttfb, 0.9) * 1000),
        "body_p50_ms": round(pct(meter.body, 0.5) * 1000),
        "body_p90_ms": round(pct(meter.body, 0.9) * 1000),
        "pushback_429_503": meter.pushback,
        "transport_errors": meter.transport_errors,
        "status_counts": meter.status,
        "protocols": dict(meter.protocols),
        "cpu_pct_one_core": round(cpu / wall * 100, 1) if wall else 0,
        "mismatches": done["mismatch"],
        "mismatch_detail": mismatches[:10],
        "stopped": done["stopped"],
    }
    if cooldown:
        await asyncio.sleep(cooldown)
    return row


def summarise(rows):
    """Median the repeats of each (transport, season_conc, workers) setting."""
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault((r["transport"], r["season_concurrency"], r["workers"]), []).append(r)
    table = []
    for key in sorted(groups):
        rs = groups[key]

        def med(field, rs=rs):
            return statistics.median(r[field] for r in rs)

        spoken: Counter[str] = Counter()
        for r in rs:
            spoken.update(r.get("protocols", {}))
        table.append(
            {
                "transport": key[0],
                "season_concurrency": key[1],
                "workers": key[2],
                "runs": len(rs),
                "pages_per_s": round(med("pages_per_s"), 1),
                "spread": round(max(r["pages_per_s"] for r in rs) - min(r["pages_per_s"] for r in rs), 1),
                "series_per_s": round(med("series_per_s"), 2),
                "mbit_per_s": round(med("mbit_per_s"), 1),
                "ttfb_p50_ms": round(med("ttfb_p50_ms")),
                "ttfb_p90_ms": round(med("ttfb_p90_ms")),
                "body_p50_ms": round(med("body_p50_ms")),
                "body_p90_ms": round(med("body_p90_ms")),
                "pushback": sum(r["pushback_429_503"] for r in rs),
                "failed": sum(r["series_failed"] for r in rs),
                "mismatches": sum(r["mismatches"] for r in rs),
                "cpu_pct": round(med("cpu_pct_one_core")),
                # The worst repeat: one straggler is enough to spoil a setting.
                "idle_pct": round(max(r["idle_pct"] for r in rs)),
                "protocol": spoken.most_common(1)[0][0] if spoken else None,
                "stopped": next((r["stopped"] for r in rs if r["stopped"]), None),
            }
        )
    return table


def line_share(mbit: float, line_mbit: float | None) -> float | None:
    """What share of the owner's measured line `mbit` is, when they gave one."""
    return mbit / line_mbit if line_mbit else None


def diagnose(table, line_mbit: float | None = None) -> list[str]:
    """Turn the table into plain answers. `line_mbit` is the owner's measured download speed."""
    lines = []
    declined = sorted(
        {t["protocol"] for t in table if t["transport"] == "h2" and t["protocol"] not in (None, "HTTP/2")}
    )
    if declined:
        lines.append(
            f"WARNING: asked for HTTP/2, the site answered in {', '.join(declined)} -- the h2 rows are not "
            "HTTP/2, so they say nothing about it."
        )
    # Idle workers cost throughput only while the core had room to spare. A
    # setting that kept the core busy anyway lost nothing to its stragglers.
    paced = [t for t in table if t["idle_pct"] >= IDLE_SHARE and t["cpu_pct"] <= BUSY_CPU]
    if paced:
        worst = max(paced, key=lambda t: t["idle_pct"])
        lines.append(
            f"WARNING: in {len(paced)} setting(s) the workers sat idle {IDLE_SHARE}% of the time or more, waiting "
            f"on stragglers (worst: {worst['idle_pct']}% at {worst['transport']} workers={worst['workers']}). Their "
            "pages/s understates what that worker count can do -- rerun with a larger --sample before trusting them."
        )
    clean = [t for t in table if not t["stopped"] and not t["pushback"]]
    pushed = [t for t in table if t["stopped"] or t["pushback"]]
    if not clean:
        lines.append(
            "No setting finished cleanly -- the site pushed back or failed everywhere. Lower the worker counts."
        )
        return lines
    best = max(clean, key=lambda t: t["pages_per_s"])
    lines.append(
        f"Fastest clean setting: {best['transport']} workers={best['workers']} "
        f"season_concurrency={best['season_concurrency']} -> {best['pages_per_s']} pages/s, "
        f"{best['series_per_s']} series/s, {best['mbit_per_s']} Mbit/s"
    )
    if line_mbit:
        peak = max(t["mbit_per_s"] for t in clean)
        share = line_share(peak, line_mbit) or 0
        if share >= LINE_FULL:
            lines.append(
                f"HOME LINE: the scrape reached {peak} Mbit/s, {share:.0%} of your {line_mbit:g} Mbit/s line. "
                "The connection is the ceiling -- more workers cannot help; use the smallest count that fills it."
            )
        else:
            lines.append(
                f"Home line: the scrape peaked at {peak} Mbit/s, {share:.0%} of your {line_mbit:g} Mbit/s line "
                "-- the connection is NOT the limit."
            )
    for key in sorted({(t["transport"], t["season_concurrency"]) for t in clean}):
        curve = sorted(
            (t for t in clean if (t["transport"], t["season_concurrency"]) == key), key=lambda t: t["workers"]
        )
        header = f"[{key[0]}, season_concurrency={key[1]}]"
        top = max(t["pages_per_s"] for t in curve)
        knee = next(t for t in curve if t["pages_per_s"] >= 0.95 * top)
        if len(curve) > 1:
            lines.append(
                f"{header} within 5% of the best from workers={knee['workers']} "
                "-- the useful maximum for this transport."
            )
        # Past the knee is where the question lives: extra load that bought
        # no throughput went somewhere, and the time-to-first-byte says where.
        # Reported even when only one count finished cleanly -- "it stopped
        # above N" is then the whole answer, and the most important one.
        refused = [
            t for t in pushed if (t["transport"], t["season_concurrency"]) == key and t["workers"] > knee["workers"]
        ]
        if refused:
            first = min(refused, key=lambda t: t["workers"])
            why = "pushed back (429/503)" if first["pushback"] else "started failing series"
            lines.append(
                f"{'   ->' if len(curve) > 1 else header} The site {why} from workers={first['workers']}: "
                "that is its limit, and the clean maximum below it is the setting to use."
            )
        if len(curve) < 2:
            continue
        last = curve[-1]
        if last is knee:
            if not refused:
                lines.append("   -> Still climbing at the largest count tried; sweep higher to find the ceiling.")
            continue
        load_x = last["workers"] / knee["workers"]
        tput_x = last["pages_per_s"] / knee["pages_per_s"] if knee["pages_per_s"] else 0
        latency_knee = knee["ttfb_p50_ms"] + knee["body_p50_ms"]
        latency_x = (last["ttfb_p50_ms"] + last["body_p50_ms"]) / latency_knee if latency_knee else 0
        lines.append(
            f"   From {knee['workers']} to {last['workers']} workers ({load_x:.1f}x load): "
            f"pages/s x{tput_x:.2f}, time-to-first-byte {knee['ttfb_p50_ms']}->{last['ttfb_p50_ms']}ms, "
            f"body download {knee['body_p50_ms']}->{last['body_p50_ms']}ms."
        )
        if tput_x < 1.1 and latency_x > 0.5 * load_x:
            # The extra load bought no throughput, so it waited somewhere.
            # Before the first byte is the site; while the bytes arrive is the
            # line -- or a site that streams its pages -- and only the line
            # speed can say which.
            share = line_share(max(t["mbit_per_s"] for t in curve), line_mbit)
            waited_in_body = last["body_p50_ms"] - knee["body_p50_ms"] > last["ttfb_p50_ms"] - knee["ttfb_p50_ms"]
            if share is not None and share >= LINE_FULL:
                lines.append(
                    f"   -> Requests queue ON YOUR LINE: throughput is flat at {share:.0%} of your "
                    f"{line_mbit:g} Mbit/s connection. More workers cannot help."
                )
            elif last["cpu_pct"] > BUSY_CPU:
                # Both timings are taken on the event loop. A loop that is
                # nearly always busy gets to an answer late, so they grow with
                # the load just as a site queue would -- they cannot tell the
                # two apart here.
                lines.append(
                    f"   -> Requests queue IN THIS PROCESS: it used {last['cpu_pct']}% of a core at "
                    f"{last['workers']} workers. A busy event loop reads answers late, which inflates both "
                    "timings above, so they cannot be pinned on the site. Less CPU per page is the lever here."
                )
            elif waited_in_body:
                where = (
                    f"your line is only at {share:.0%}, so the site is sending its pages slowly"
                    if share is not None
                    else "on your line, or at a site that streams its pages; --line-mbit tells which"
                )
                lines.append(
                    f"   -> The extra wait is in downloading the pages, not in the site starting to answer: {where}."
                )
            else:
                lines.append(
                    "   -> Requests queue ON THE SITE: its response time grows with the load while "
                    "throughput stays flat. The site's capacity is the ceiling; more workers only lengthen its queue."
                )
        elif max(t["cpu_pct"] for t in curve) > BUSY_CPU:
            lines.append("   -> This process used >85% of a core: your PC/Python is at least part of the limit.")
    transports = {t["transport"] for t in clean}
    if {"h1", "h2"} <= transports and not declined:
        b1 = max(t["pages_per_s"] for t in clean if t["transport"] == "h1")
        b2 = max(t["pages_per_s"] for t in clean if t["transport"] == "h2")
        lines.append(
            f"Best HTTP/1.1 {b1} vs best HTTP/2 {b2} pages/s. A clear HTTP/1.1 win means the site limits "
            "work PER CONNECTION (HTTP/2 puts everything on one); a tie means the limit is the site as a whole."
        )
    mism = sum(t["mismatches"] for t in table)
    lines.append(
        "Accuracy: every setting returned identical data."
        if not mism
        else f"Accuracy: {mism} series returned different counts between settings -- see mismatch_detail. "
        "(A new episode airing mid-sweep also shows up here.)"
    )
    return lines


def print_table(table) -> None:
    head = (
        f"{'proto':5} {'sc':>2} {'wrk':>3} {'pages/s':>8} {'±':>5} {'series/s':>8} {'Mbit/s':>7} "
        f"{'ttfb50':>7} {'ttfb90':>7} {'body50':>7} {'body90':>7} {'429/503':>7} {'fail':>4} {'mism':>4} "
        f"{'cpu%':>4} {'idle%':>5}  note"
    )
    print(head)
    print("-" * len(head))
    for t in table:
        print(
            f"{t['transport']:5} {t['season_concurrency']:>2} {t['workers']:>3} {t['pages_per_s']:>8} "
            f"{t['spread']:>5} {t['series_per_s']:>8} {t['mbit_per_s']:>7} {t['ttfb_p50_ms']:>6}ms "
            f"{t['ttfb_p90_ms']:>6}ms {t['body_p50_ms']:>6}ms {t['body_p90_ms']:>6}ms {t['pushback']:>7} "
            f"{t['failed']:>4} {t['mismatches']:>4} {t['cpu_pct']:>4} {t['idle_pct']:>5}  {t['stopped'] or ''}"
        )


async def sweep_transport(transport: str, args, sample: list[dict] | None, rows: list[dict], reference: dict):
    """Log in once over `transport` and run every setting on it; returns the sample used.

    Rows are appended to `rows` as each setting finishes, not returned at the
    end, so an interrupted sweep still has everything that completed.
    """
    scraper_mod.USE_HTTP2 = transport == "h2"
    scraper = SCRAPER_CLS()
    # Size the pool for the largest setting, as a real run of that size would.
    scraper.pool_workers = max(args.workers)
    scraper_mod.SEASON_CONCURRENCY = max(args.season_concurrency)
    print(f"\n→ [{transport}] logging in once...")
    client = await scraper._create_logged_in_client()
    try:
        if sample is None:
            sample = await sample_from_catalogue(scraper, client, args.sample, args.seed)
        if not sample:
            print(f"   [{transport}] no series to sample -- nothing to measure.")
            return sample
        warm = sample[:WARMUP_SERIES]
        print(f"→ [{transport}] warming up on {len(warm)} series (not counted)...")
        warm_results = [await scrape_contained(scraper, client, info) for info in warm]
        if all(fingerprint(r) is None for r in warm_results):
            print(
                f"   [{transport}] every warm-up series failed "
                f"({warm_results[0].get('_error_reason')}) -- not sweeping this transport."
            )
            return sample
        # Smallest load (requests in flight) at which a setting had to stop.
        ceiling: int | None = None
        for rep in range(args.repeats):
            settings = [(w, sc) for w in args.workers for sc in args.season_concurrency]
            random.shuffle(settings)
            # Larger loads last within a repeat when protecting the site
            # matters more than order effects.
            if args.careful:
                settings.sort(key=lambda s: s[0] * s[1])
            for workers, sc in settings:
                load = workers * sc
                if args.careful and ceiling is not None and load >= ceiling:
                    print(
                        f"   rep {rep} workers={workers:>3} sc={sc}: skipped -- {load} in flight, "
                        f"and {ceiling} already had to stop"
                    )
                    continue
                row = await run_setting(scraper, client, sample, workers, sc, args.cooldown, reference)
                row.update(transport=transport, repeat=rep)
                rows.append(row)
                print(
                    f"   rep {rep} workers={workers:>3} sc={sc}: {row['pages_per_s']:>6} pages/s "
                    f"{row['series_per_s']:>5} series/s {row['mbit_per_s']:>5} Mbit/s  "
                    f"ttfb p50 {row['ttfb_p50_ms']}ms body p50 {row['body_p50_ms']}ms  "
                    f"429/503={row['pushback_429_503']} fail={row['series_failed']} "
                    f"cpu={row['cpu_pct_one_core']}%  {row['stopped'] or ''}"
                )
                if row["stopped"] and args.careful:
                    ceiling = load if ceiling is None else min(ceiling, load)
    finally:
        await client.aclose()
    return sample


def report(rows: list[dict], args, sample: list[dict] | None, complete: bool) -> None:
    """Print the summary and verdict, and save everything to REPORT_FILE."""
    table = summarise(rows)
    print("\n" + "=" * 100)
    print(
        f"  {SCRAPER_CLS.__name__}: {len(sample or [])} series x {args.repeats} repeat(s), medians shown "
        f"(± = spread between repeats)"
    )
    if args.line_mbit:
        print(f"  Your line: {args.line_mbit:g} Mbit/s download, as measured by you")
    if not complete:
        print("  INTERRUPTED -- partial results, from the settings that finished.")
    print("=" * 100)
    print_table(table)
    print()
    verdict = diagnose(table, args.line_mbit)
    for line in verdict:
        print("  " + line)

    REPORT_FILE.write_text(
        json.dumps(
            {
                "generated": datetime.now().isoformat(),
                "scraper": SCRAPER_CLS.__name__,
                "complete": complete,
                "args": vars(args),
                "cpu_count": os.cpu_count(),
                "summary": table,
                "verdict": verdict,
                "runs": rows,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"\n  Saved: {REPORT_FILE}  (series URLs and numbers only -- no credentials)")


async def main_async(args) -> None:
    rows: list[dict] = []
    reference: dict[str, tuple] = {}
    sample: list[dict] | None = None
    if not args.from_catalogue:
        try:
            sample = load_sample(args.sample, args.seed)
        except FileNotFoundError:
            print(f"No local index at {SERIES_INDEX_FILE} -- use --from-catalogue.")
            return
        if not sample:
            print("No usable series in the local index -- use --from-catalogue.")
            return

    complete = False
    try:
        for transport in args.transport:
            sample = await sweep_transport(transport, args, sample, rows, reference)
        complete = True
    finally:
        # Saved on the way out whatever happened: a sweep interrupted after
        # twenty minutes still measured something, and getting it back would
        # mean putting the same load on the site again.
        if rows:
            report(rows, args, sample, complete)


def _number_list(text: str) -> list[int]:
    """argparse type: comma-separated whole numbers, each 1 or more."""
    try:
        values = sorted({int(x) for x in text.split(",") if x.strip()})
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected comma-separated numbers, got {text!r}") from None
    if not values or values[0] < 1:
        raise argparse.ArgumentTypeError(f"every value must be 1 or more, got {text!r}")
    return values


def _transport_list(text: str) -> list[str]:
    """argparse type: h2 and/or h1, comma-separated, in the order given."""
    values = list(dict.fromkeys(x.strip() for x in text.split(",") if x.strip()))
    if not values or any(v not in TRANSPORTS for v in values):
        raise argparse.ArgumentTypeError(f"expected h2 and/or h1, got {text!r}")
    return values


def _at_least(minimum: float, kind: Callable[[str], float] = int):
    def parse(text: str):
        try:
            value = kind(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None
        if value < minimum:
            raise argparse.ArgumentTypeError(f"must be {minimum} or more, got {text!r}")
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workers", type=_number_list, default="4,8,12,16,24", help="comma-separated worker counts")
    parser.add_argument(
        "--season-concurrency", type=_number_list, default="4", help="comma-separated SEASON_CONCURRENCY values"
    )
    parser.add_argument(
        "--transport", type=_transport_list, default="h2,h1", help="h2 (one multiplexed connection), h1 (many), or both"
    )
    parser.add_argument("--sample", type=_at_least(1), default=150, help="series per setting (default 150)")
    parser.add_argument("--repeats", type=_at_least(1), default=2, help="passes over every setting (default 2)")
    parser.add_argument("--cooldown", type=_at_least(0, float), default=15.0, help="seconds of quiet between settings")
    parser.add_argument("--seed", type=int, default=1, help="sample seed; same seed = same series")
    parser.add_argument("--from-catalogue", action="store_true", help="sample the live catalogue, not the index")
    parser.add_argument(
        "--line-mbit",
        type=_at_least(1, float),
        default=None,
        help="your measured download speed in Mbit/s, so the verdict can tell a full line from a slow site",
    )
    parser.add_argument(
        "--no-careful",
        dest="careful",
        action="store_false",
        help="fully random order and no skipping after push-back (cleaner statistics, harder on the site)",
    )
    return parser


def main() -> None:
    asyncio.run(main_async(build_parser().parse_args()))


if __name__ == "__main__":
    main()
