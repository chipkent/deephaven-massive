"""Keep a client's secrets out of everything it decodes, logs, or puts in an error.

The API key travels only in the REST client's `Authorization` header and in the WebSocket
authentication frame, and the S3 secret goes only to boto3, so no client ever formats a secret
itself. A server can still echo one back: a proxy's error page that quotes the request, an
error envelope that repeats the key it rejected, a status message, a field of a row. So each
client cleans what it receives, in one place, before anything reads it: the REST client every
response body and the WebSocket client every frame (`Redactor.received`), and the flat-file
client S3's error words (`redact`). Every row, status, excerpt, error message, and log line
built from them is then free of the secret by construction, wherever it is built.

A JSON text can spell the secret with escapes (a `u0050` escape for `P`), which no search of the raw
bytes finds but which decodes to the secret. `Redactor.received` therefore also replaces the
secret's own JSON-escaped form, and when a body or frame holds an escape that spells a character
of a secret it decodes it, replaces the secret in every decoded string, and re-encodes it if
anything changed. Any other body, which is nearly every one, costs one substring search per
spelling and one pattern search, and is passed on uncopied.

The replacement is exact and literal: no pattern that could miss a variant or blank out
unrelated text. An empty secret is skipped, since replacing the empty string would insert the
marker between every character.

This module imports only the standard library and `net.codec`. It is part of `net`, the layer
every client shares.
"""

from __future__ import annotations

import json
import re
from typing import Final, overload

from deephaven_massive._massive.net import codec

__all__ = ["REDACTED", "Redactor", "redact", "redact_value"]

REDACTED: Final = "***"
"""What each occurrence of a secret becomes."""


def redact(text: str, *secrets: str | None) -> str:
    """Return `text` with every occurrence of each secret replaced by `REDACTED`.

    Args:
        text: Decoded text to clean, such as S3's error message.
        *secrets: The client's secrets (the API key, the S3 secret access key). `None` and the
            empty string are skipped.

    Returns:
        The text with each secret replaced; `text` itself when it holds none.
    """
    for secret in secrets:
        if secret and secret in text:
            text = text.replace(secret, REDACTED)
    return text


def redact_value(value: object, *secrets: str | None) -> object:
    """Return a decoded JSON value with each secret replaced in every string it holds.

    Args:
        value: A decoded JSON value: an object, a list, a string, or a scalar.
        *secrets: The client's secrets, as `redact` takes them.

    Returns:
        `value` itself when no string in it holds a secret, otherwise a copy (objects and lists
        rebuilt, keys included) with `REDACTED` in place of each occurrence.
    """
    if isinstance(value, str):
        return redact(value, *secrets)
    if isinstance(value, list):
        items = [redact_value(item, *secrets) for item in value]
        changed = any(new is not old for new, old in zip(items, value, strict=True))
        return items if changed else value
    if isinstance(value, dict):
        pairs = [(redact(k, *secrets), redact_value(v, *secrets)) for k, v in value.items()]
        changed = any(
            new_key is not key or new is not old
            for (new_key, new), (key, old) in zip(pairs, value.items(), strict=True)
        )
        return dict(pairs) if changed else value
    return value


class Redactor:
    """One client's secrets, ready to clean whatever the client receives.

    A client builds one at construction and passes every response body or frame through
    `received` before anything decodes, quotes, or logs it. It precomputes, once, each secret's
    spellings (as it is, and as JSON escapes it) and a pattern that matches only the JSON
    escapes able to spell a character some secret contains, so a body whose escapes spell
    nothing of a secret (the quotes of a news headline) is never decoded. Immutable after
    construction, so one instance is shared safely by every thread of its client.

    Example:
        ::

            redactor = Redactor("my-key")
            assert redactor.received(b'{"error":"bad key my-key"}') == b'{"error":"bad key ***"}'
    """

    __slots__ = ("_bytes_escapes", "_needles", "_secrets", "_text_escapes")

    def __init__(self, *secrets: str | None) -> None:
        """Precompute the secrets' spellings and the escapes that could spell them.

        Args:
            *secrets: The client's secrets; `None` and the empty string are skipped.
        """
        self._secrets: tuple[str, ...] = tuple(secret for secret in secrets if secret)
        self._needles = tuple(n for secret in self._secrets for n in _spellings(secret))
        characters = {c for secret in self._secrets for c in secret}
        escapes = sorted({escape for c in characters for escape in _escapes(c)})
        pattern = "|".join(re.escape(escape) for escape in escapes)
        # Never matches when there is no secret: an empty alternation would match everywhere.
        self._text_escapes = re.compile(pattern or r"(?!)", re.IGNORECASE)
        self._bytes_escapes = re.compile((pattern or r"(?!)").encode(), re.IGNORECASE)

    @overload
    def received(self, data: bytes) -> bytes: ...

    @overload
    def received(self, data: str) -> str: ...

    @overload
    def received(self, data: str | bytes) -> str | bytes: ...

    def received(self, data: str | bytes) -> str | bytes:
        """Return a received body or frame with each secret replaced, however its JSON spells it.

        Args:
            data: The body or frame as received: `bytes`, or `str` for a WebSocket text frame.
                It is usually JSON, but need not be (a proxy's HTML page).

        Returns:
            `data` with each secret and its JSON-escaped form replaced by `REDACTED`, and, when
            it holds an escape that spells a character of a secret, decoded, cleaned string by
            string (`redact_value`), and re-encoded if anything changed; `data` itself, uncopied,
            when nothing was replaced. Text that is not JSON keeps its escapes as they are.
        """
        if isinstance(data, bytes):
            for needle in self._needles:
                raw = needle.encode()
                if raw in data:
                    data = data.replace(raw, REDACTED.encode())
            spelled = self._bytes_escapes.search(data) is not None
        else:
            for needle in self._needles:
                if needle in data:
                    data = data.replace(needle, REDACTED)
            spelled = self._text_escapes.search(data) is not None
        if not spelled:
            return data
        try:
            value = codec.loads(data)
        except ValueError:
            return data
        cleaned = redact_value(value, *self._secrets)
        if cleaned is value:
            return data
        text = json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))
        return text.encode() if isinstance(data, bytes) else text


def _spellings(secret: str) -> tuple[str, ...]:
    """Return how a secret appears in JSON text: as it is, and as JSON escapes it.

    Args:
        secret: A non-empty secret.

    Returns:
        The secret, and its JSON-escaped form when that differs (a quote or a backslash in it).
    """
    escaped = json.dumps(secret, ensure_ascii=False)[1:-1]
    return (secret,) if escaped == secret else (secret, escaped)


# The last code point a single four-digit escape spells, and the first high surrogate.
_LAST_BMP: Final = 0xFFFF
_HIGH_SURROGATE: Final = 0xD800

# The short JSON escapes, by the character each spells.
_SHORT_ESCAPES: Final = {
    '"': '\\"',
    "\\": "\\\\",
    "/": "\\/",
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def _escapes(character: str) -> tuple[str, ...]:
    """Return every JSON escape that spells a character (its hex digits match in any case).

    Args:
        character: One character.

    Returns:
        Its four-hex-digit `u` escape (the first half of a surrogate pair for a character beyond the
        Basic Multilingual Plane, which no other character's escape starts with), and its short
        escape when it has one.
    """
    code = ord(character)
    if code > _LAST_BMP:
        code = _HIGH_SURROGATE + ((code - _LAST_BMP - 1) >> 10)
    long = f"\\u{code:04x}"
    short = _SHORT_ESCAPES.get(character)
    return (long,) if short is None else (long, short)
