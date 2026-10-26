"""The JSON decoder of every Massive wire client: orjson when installed, else the stdlib.

REST bodies and WebSocket frames are decoded exactly once, through `loads`. orjson is an
optional dependency (`pip install deephaven-massive[orjson]`) that decodes several times faster;
without it, the standard library's `json` is used. The choice is made once, when this module is
first imported, and logged once at INFO on the `deephaven_massive._massive.net.codec` logger.

Both paths are strict in the same ways, so a malformed body fails the same way whichever is
installed: input must be UTF-8 (no other encoding is guessed, and a byte-order mark is
rejected), and `NaN`, `Infinity`, and numbers too large for a double are rejected rather than
decoded as non-finite floats. Two differences remain, neither reachable from a well-formed
Massive payload. An integer above the unsigned 64-bit range decodes as an exact `int` on the
standard-library path and as a `float` under orjson; either way it fails the `long` check of
the decoder that reads it. A string escape naming a lone UTF-16 surrogate is rejected by orjson
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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

__all__ = ["loads"]

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


def loads(data: bytes | str) -> object:
    """Decode one JSON document strictly.

    Call it once per REST body or WebSocket text frame; the caller then checks the shape of
    the result (an envelope object, a list of events) itself. Safe from any thread.

    Malformed input raises `ValueError`: `data` that is not UTF-8, is not a single JSON
    document, holds `NaN` or `Infinity`, or holds a number too large for a double. The
    concrete class is `UnicodeDecodeError`, `json.JSONDecodeError`, or
    `orjson.JSONDecodeError` (a subclass of `json.JSONDecodeError`), raised by the backend
    itself; callers catch `ValueError`. Nesting depth is not limited here: orjson stops at its
    own fixed depth, and the standard library recurses as deep as the interpreter allows.

    Args:
        data: The document, as UTF-8 bytes or as text.

    Returns:
        The decoded value: a `dict`, `list`, `str`, `int`, `float`, `bool`, or `None`.
    """
    if _orjson is not None:
        value: object = _orjson.loads(data)
        return value
    text = data.decode("utf-8") if isinstance(data, bytes) else data
    return json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float)
