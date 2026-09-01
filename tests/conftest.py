"""Shared pytest setup for this repo's tests.

Two jobs, both about keeping future test files cheap to write:

1. Put the repo root on ``sys.path`` once, here, so a new test module can
   simply ``from src.index_manager import ...`` without repeating the
   ``sys.path.insert(0, ...)`` prologue that every existing file carries.
   The old files keep their prologue and are unaffected -- a duplicate entry
   on the path is harmless -- so this can be adopted gradually.

2. Register the ``--benchmark`` flag that ``tests/bench.py`` reads. Timing
   tests are skipped by default so the everyday suite stays fast; see
   ``tests/bench.py`` for how to run and update them.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def pytest_addoption(parser):
    parser.addoption(
        "--benchmark",
        action="store_true",
        default=False,
        help="Run the timing benchmarks in tests/test_benchmarks.py.",
    )
    parser.addoption(
        "--benchmark-update",
        action="store_true",
        default=False,
        help="Run the benchmarks and rewrite tests/benchmark_baseline.json with the new timings.",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "benchmark: a timing test, skipped unless --benchmark is passed")


def pytest_collection_modifyitems(config, items):
    """Skip benchmarks unless asked for, so the normal run stays quick."""
    if config.getoption("--benchmark") or config.getoption("--benchmark-update"):
        return
    skip = pytest.mark.skip(reason="timing test; pass --benchmark to run")
    for item in items:
        if "benchmark" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def bench(request):
    """Session-wide timing recorder; see tests/bench.py for the contract.

    Session-scoped so every benchmark in a run lands in one table and one
    baseline write, rather than each test rewriting the file.
    """
    from tests.bench import Recorder

    recorder = Recorder(update=request.config.getoption("--benchmark-update"))
    yield recorder
    recorder.flush()
