"""A dependency-free timing harness for the benchmark tests.

Running
-------
    python -m pytest tests/test_benchmarks.py --benchmark
    python -m pytest tests/test_benchmarks.py --benchmark-update   # re-record

Benchmarks are skipped in a normal run (see ``tests/conftest.py``) so the
everyday suite stays fast and machine-independent. Only the explicit flag runs
them.

How it decides something regressed
----------------------------------
Each benchmark records the *best* of N repeats, not the mean. Best-of is the
right statistic here because the noise on a developer machine is one-sided --
another process can only ever make a run slower, never faster -- so the
minimum is the closest thing to the code's actual cost, and it is far steadier
between runs than an average that a single scheduling hiccup can drag out.

A result is compared against ``benchmark_baseline.json`` and fails only when
it exceeds the recorded time by more than ``TOLERANCE``. The tolerance is
deliberately loose: this is here to catch an algorithmic regression -- a loop
that became quadratic, a parse that started running twice -- not to police a
few percent of drift between machines. An unrecorded benchmark records itself
and passes, so adding one never fails a first run.

Because the baseline is machine-specific, treat a failure as "look at what
changed", not as a hard gate in CI on hardware that did not record it.

Adding a benchmark
------------------
Add one function to ``tests/test_benchmarks.py``:

    @pytest.mark.benchmark
    def test_something_is_fast(bench):
        bench("a short stable name", lambda: thing_to_measure(fixed_input))

Keep the input fixed and hold it in the closure so setup cost is not measured.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

BASELINE_PATH = Path(__file__).resolve().parent / "benchmark_baseline.json"

# How much slower than the recorded best a run may be before it fails.
# 1.60 == "60% slower is a regression worth stopping for".
TOLERANCE = 1.60

# Repeats per measurement. Best-of this many; raise it for very fast functions
# where a single run is dominated by timer resolution.
REPEATS = 5


def load_baseline() -> dict:
    try:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_baseline(data: dict) -> None:
    BASELINE_PATH.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def measure(func, *, repeats: int = REPEATS) -> float:
    """Return the best wall-clock seconds over ``repeats`` runs of ``func``."""
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        best = min(best, time.perf_counter() - start)
    return best


class Recorder:
    """Collects this session's timings and writes them out at the end.

    One instance per pytest session, handed to tests as the ``bench`` fixture.
    """

    def __init__(self, *, update: bool):
        self.update = update
        self.baseline = load_baseline()
        self.results: dict[str, float] = {}

    def __call__(self, name: str, func, *, repeats: int = REPEATS) -> float:
        seconds = measure(func, repeats=repeats)
        self.results[name] = seconds
        recorded = self.baseline.get(name)

        if recorded is None or self.update:
            return seconds

        limit = recorded * TOLERANCE
        assert seconds <= limit, (
            f"{name} regressed: {seconds * 1000:.2f}ms now vs {recorded * 1000:.2f}ms recorded "
            f"(limit {limit * 1000:.2f}ms, tolerance x{TOLERANCE}). "
            f"If this is an intended trade-off, re-record with --benchmark-update."
        )
        return seconds

    def flush(self) -> None:
        """Write the baseline when updating, and always print a readable table."""
        if not self.results:
            return
        if self.update:
            merged = {**self.baseline, **self.results}
            save_baseline(merged)

        width = max(len(name) for name in self.results)
        lines = ["", f"Benchmarks (best of {REPEATS})"]
        for name, seconds in sorted(self.results.items()):
            recorded = self.baseline.get(name)
            delta = ""
            if recorded:
                ratio = seconds / recorded
                delta = f"  ({ratio:.2f}x recorded)"
            lines.append(f"  {name:<{width}}  {seconds * 1000:8.3f}ms{delta}")
        if self.update:
            lines.append(f"  -> baseline written to {BASELINE_PATH.name}")
        print("\n".join(lines))
