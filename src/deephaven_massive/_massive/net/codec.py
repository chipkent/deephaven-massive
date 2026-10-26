"""The JSON decoder of every Massive wire client: orjson when installed, else the stdlib.

REST bodies and WebSocket frames are decoded exactly once, through `loads`. orjson is an
optional dependency (`pip install deephaven-massive[orjson]`) that decodes several times faster;
without it, the standard library's `json` is used. The choice is made once, when this module is
first imported, and logged once at INFO on the `deephaven_massive._massive.net.codec` logger.

Both paths are strict in the same ways, so a malformed body fails the same way whichever is
installed: input must be UTF-8 (no other encoding is guessed, and a byte-order mark is
rejected), and `NaN`, `Infinity`, and numbers too large for a double are rejected rather than
decoded as non-finite floats.

Deep nesting is bounded on both paths, so a hostile or broken body can never exhaust the
calling thread's C stack: orjson rejects depth beyond 1024 itself, and the standard-library
path counts the depth before decoding and rejects anything deeper than `MAX_DEPTH` (512).

Two other differences remain, neither reachable from a well-formed Massive payload. An
integer above the unsigned 64-bit range decodes as an exact `int` on the standard-library path
and as a `float` under orjson; either way it fails the `long` check of the decoder that reads
it. A string escape naming a lone UTF-16 surrogate is rejected by orjson
and decoded by the standard library.

Only decoding lives here. Encoding (the few small WebSocket control frames) uses the standard
library's `json.dumps`, which returns `str`; orjson's `dumps` returns `bytes`, which
`websockets` would send as a binary frame.

This module imports only the standard library and, when installed, `orjson`.
"""

from __future__ import annotations

import importlib
import json
import logging
import math
from array import array
from itertools import accumulate
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

__all__ = ["MAX_DEPTH", "loads"]

_log = logging.getLogger(__name__)


def _select_orjson() -> ModuleType | None:
    """Import orjson if it is installed, and log which decoder is in use.

    Returns:
        The `orjson` module, or `None` when it is not installed.
    """
    try:
        orjson = importlib.import_module("orjson")
    except ImportError:
        _log.info("JSON decoding uses the standard library; install orjson for faster decoding")
        return None
    _log.info("JSON decoding uses orjson")
    return orjson


_orjson = _select_orjson()


def _reject_constant(name: str) -> float:
    """Reject `NaN`, `Infinity`, and `-Infinity`, which strict JSON does not allow.

    Args:
        name: The literal the standard-library decoder found.

    Raises:
        ValueError: Always, naming the literal.
    """
    raise ValueError(f"JSON does not allow {name}")


def _finite_float(text: str) -> float:
    """Parse a JSON number with a fraction or exponent, rejecting one that overflows a double.

    Args:
        text: The number's text, such as `"1.5"` or `"1e400"`.

    Returns:
        The number as a finite `float`.

    Raises:
        ValueError: If the number is too large for a double, such as `1e400`.
    """
    value = float(text)
    if not math.isfinite(value):
        raise ValueError(f"JSON number {text} is too large for a double")
    return value


MAX_DEPTH = 512
"""The deepest nesting of arrays and objects the standard-library path decodes.

Decoding recurses on the C stack, about 160 bytes per level on Python 3.12, so depth 512 needs
roughly 80 KB: safe on any Python or JVM thread (a JVM thread has 1 MB). It also stays below
the 1,000-frame recursion limit of Python 3.11. orjson does not recurse and keeps its own limit
of 1024. Massive's payloads nest fewer than ten levels deep.
"""

# Every byte except the five that shape nesting outside strings: `[`, `]`, `{`, `}`, `"`.
_NOT_SKELETON = bytes(sorted(set(range(256)) - set(b'[]{}"')))
# An opener becomes 1 and a closer 255, which reads as -1 in a signed byte array.
_STEPS = bytes.maketrans(b"[]{}", b"\x01\xff\x01\xff")


def _too_deep(data: bytes) -> bool:
    """Return whether a document nests arrays and objects deeper than `MAX_DEPTH`.

    The scan reads only the brackets outside strings, without parsing, so it never recurses.
    Escaped backslashes and escaped quotes are removed first, so every remaining `"` opens or
    closes a string. Within a valid document this matches the parser exactly. After a syntax
    error it may count too deep, never too shallow, and the parser rejects such a document
    anyway.

    Args:
        data: The document as UTF-8 bytes.

    Returns:
        True when some point of the document lies inside more than `MAX_DEPTH` open arrays
        and objects.
    """
    if data.count(b"[") + data.count(b"{") <= MAX_DEPTH:
        return False
    if b"\\" in data:
        data = data.replace(b"\\\\", b"").replace(b'\\"', b"")
    skeleton = data.translate(None, _NOT_SKELETON)
    outside = b"".join(skeleton.split(b'"')[::2])
    return max(accumulate(array("b", outside.translate(_STEPS))), default=0) > MAX_DEPTH


def loads(data: bytes | str) -> object:
    """Decode one JSON document strictly.

    Call it once per REST body or WebSocket text frame; the caller then checks the shape of
    the result (an envelope object, a list of events) itself. Safe from any thread: neither
    path can exhaust the calling thread's stack.

    Malformed input raises `ValueError`: `data` that is not UTF-8, is not a single JSON
    document, holds `NaN` or `Infinity`, holds a number too large for a double, or nests
    deeper than the decoder allows (`MAX_DEPTH` on the standard-library path, 1024 under
    orjson). The concrete class is `ValueError`, `UnicodeDecodeError`, `json.JSONDecodeError`,
    or `orjson.JSONDecodeError` (a subclass of `json.JSONDecodeError`); callers catch
    `ValueError`.

    Args:
        data: The document, as UTF-8 bytes or as text.

    Returns:
        The decoded value: a `dict`, `list`, `str`, `int`, `float`, `bool`, or `None`.

    Raises:
        ValueError: On the standard-library path, if the document nests deeper than
            `MAX_DEPTH`. The other failures above come from the backend itself.
    """
    if _orjson is not None:
        value: object = _orjson.loads(data)
        return value
    if isinstance(data, bytes):
        text = data.decode("utf-8")
    else:
        text, data = data, data.encode("utf-8", "surrogatepass")
    if _too_deep(data):
        raise ValueError(f"JSON nests deeper than {MAX_DEPTH} arrays and objects")
    return json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float)
