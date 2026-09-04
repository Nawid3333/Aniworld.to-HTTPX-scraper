"""Terminal helpers: the colour gate, semantic styles, and a colourising print.

All six sibling repos share one colour vocabulary:

    ``✗`` / ``ERROR:``                 red      something failed
    ``CRITICAL:``                       magenta  something failed unrecoverably
    ``⚠`` / ``[WARN]`` / ``WARNING:``    yellow   something needs attention
    ``✓`` / ``✅``                        green    something succeeded
    ``→`` at the start of a line          cyan     a step or section heading
    supporting detail                     dim

`cprint` applies that vocabulary on its own, line by line, from the marker a
line opens with. Modules opt in at the import:

    from src.term import cprint as print

so every existing ``print`` call keeps its shape and only gains colour. Only a
*line-leading* arrow is treated as a heading -- an arrow used mid-sentence
("12/24 → 24/24", "wrote 1 URL → batch.txt") is left alone.

Anything that needs a colour the markers cannot express (a whole block of
danger text, a dimmed hint inside a prompt) calls `danger`, `warn`, `ok`,
`step` or `dim` directly.
"""

from __future__ import annotations

import logging
import os
import re
import sys


class _T:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"


def _enable_windows_vt() -> bool:
    """Turn on virtual-terminal processing so a legacy console renders ANSI.

    Windows Terminal and the VS Code terminal enable this themselves, but a
    bare ``conhost`` does not, and without it every escape code is printed as
    literal ``<-[31m`` text. False means the console refused, so colour stays
    off rather than corrupting the output.
    """
    if sys.platform != "win32":
        return True
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        # A HANDLE is pointer-sized. Without these declarations ctypes would
        # return and pass it as a 32-bit int, truncating it on 64-bit Windows.
        kernel32.GetStdHandle.restype = ctypes.c_void_p
        kernel32.GetStdHandle.argtypes = [ctypes.c_uint32]
        kernel32.GetConsoleMode.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
        kernel32.SetConsoleMode.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        handle = kernel32.GetStdHandle(-11 & 0xFFFFFFFF)  # STD_OUTPUT_HANDLE
        if not handle or handle == ctypes.c_void_p(-1).value:
            return False
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        # 0x0004 = ENABLE_VIRTUAL_TERMINAL_PROCESSING
        return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))
    except Exception:
        return False


def _color_enabled() -> bool:
    """Colour only when a real terminal is on the other end.

    ``isatty`` is the important half: without it the escape codes end up in
    whatever file or pipe stdout was redirected to.
    """
    if os.getenv("NO_COLOR"):
        return False
    if os.getenv("TERM") == "dumb":
        return False
    try:
        if not sys.stdout.isatty():
            return False
    except (AttributeError, ValueError):
        return False
    return _enable_windows_vt()


_COLOR = _color_enabled()


def color_enabled() -> bool:
    """Whether styling is active for this run."""
    return _COLOR


def style(text: str, *codes: str) -> str:
    """Wrap text in ANSI codes, or return it untouched when colour is off."""
    if not _COLOR or not text:
        return text
    return "".join(codes) + text + _T.RESET


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes, for width maths and for log files."""
    return _ANSI_RE.sub("", text)


def err(text: str) -> str:
    """A failure."""
    return style(text, _T.RED)


def danger(text: str) -> str:
    """A failure that stops the run, or a destructive confirmation."""
    return style(text, _T.BOLD, _T.RED)


def warn(text: str) -> str:
    """Something that needs attention but is not fatal."""
    return style(text, _T.YELLOW)


def alert(text: str) -> str:
    """A warning heading that has to be seen before anything below it."""
    return style(text, _T.BOLD, _T.YELLOW)


def ok(text: str) -> str:
    """Something succeeded."""
    return style(text, _T.GREEN)


def step(text: str) -> str:
    """A step or section heading."""
    return style(text, _T.BOLD, _T.CYAN)


def dim(text: str) -> str:
    """Supporting detail: input hints, rules, URLs under a title."""
    return style(text, _T.DIM)


# A prompt's trailing hint -- "(0-2)", "[default: 2]", "(y/n) [n]" -- plus the
# colon that follows it. Anchored to the end so a parenthetical inside the
# question itself ("Rescrape (Sub/WL) now?") is not mistaken for the hint.
_HINT_RE = re.compile(r"((?:\([^()]*\)|\[[^\[\]]*\])(?:\s*(?:\([^()]*\)|\[[^\[\]]*\]))*\s*:?\s*)$")


def prompt(text: str) -> str:
    """Dim the trailing hint of a prompt, leaving the question at full weight.

    "Choose mode (0-2) [default: 2]: " keeps the question readable and pushes
    the range and the default into the background. Trailing whitespace stays
    outside the escape codes so the cursor sits where it always did.
    """
    if not _COLOR or not text or "\033" in text:
        return text
    match = _HINT_RE.search(text)
    if not match or match.start() == 0:
        return text
    hint = match.group(1)
    body = hint.rstrip()
    return text[: match.start()] + dim(body) + hint[len(body) :]


def cinput(text: str = "") -> str:
    """`input` that dims the hint. A pre-styled prompt is passed through."""
    return input(prompt(text))


# Ordered longest-prefix-first so "✅" is tested before "✓" would ever matter
# and the spelled-out words cannot be shadowed by a shorter marker.
_LEAD_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("✗", (_T.RED,)),
    ("⚠", (_T.YELLOW,)),
    ("✅", (_T.GREEN,)),
    ("✓", (_T.GREEN,)),
    ("→", (_T.BOLD, _T.CYAN)),
    ("[WARN]", (_T.YELLOW,)),
    ("WARNING:", (_T.YELLOW,)),
    ("ERROR:", (_T.RED,)),
    ("CRITICAL:", (_T.BOLD, _T.MAGENTA)),
)


def paint(text: str) -> str:
    """Colour each line of `text` that opens with a known marker.

    Indentation is kept outside the escape codes so that a reset never lands
    mid-indent, and a string that already carries escape codes is returned as
    it came -- a caller that styled its own text wins.
    """
    if not _COLOR or not text or "\033" in text:
        return text
    painted: list[str] = []
    for line in text.split("\n"):
        body = line.lstrip(" \t")
        indent = line[: len(line) - len(body)]
        for marker, codes in _LEAD_RULES:
            if body.startswith(marker):
                painted.append(indent + style(body, *codes))
                break
        else:
            painted.append(line)
    return "\n".join(painted)


def cprint(*values: object, **kwargs: object) -> None:
    """`print` that colours known markers and leaves everything else alone."""
    painted = tuple(paint(value) if isinstance(value, str) else value for value in values)
    print(*painted, **kwargs)  # type: ignore[arg-type]


_LEVEL_STYLES: dict[int, tuple[str, ...]] = {
    logging.DEBUG: (_T.DIM,),
    logging.INFO: (),
    logging.WARNING: (_T.YELLOW,),
    logging.ERROR: (_T.RED,),
    logging.CRITICAL: (_T.BOLD, _T.MAGENTA),
}


class ColorFormatter(logging.Formatter):
    """Colour console log records by level.

    Attach this to the ``StreamHandler`` only. The rotating file handlers keep
    their plain formatter, so no escape code ever reaches ``logs/*.log`` -- a
    coloured log file is unreadable in an editor and breaks ``grep``.

    A record whose message already carries escape codes is left as it is: the
    call site styled it deliberately, and wrapping it again would end the
    outer colour at the inner reset.
    """

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        if "\033" in text:
            return text
        codes = _LEVEL_STYLES.get(record.levelno, ())
        return style(text, *codes) if codes else paint(text)


class PlainFormatter(logging.Formatter):
    """Strip ANSI codes on the way to the log file.

    Some call sites style a message before handing it to the logger -- that
    colour is meant for the console. Left in place it writes escape codes into
    logs/*.log, where they are unreadable in an editor and break grep.
    """

    def format(self, record: logging.LogRecord) -> str:
        return strip_ansi(super().format(record))
