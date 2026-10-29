"""Keep a client's secrets out of everything it decodes, logs, or puts in an error.

The API key travels only in the REST client's `Authorization` header and in the WebSocket
authentication frame, and the S3 secret goes only to boto3, so no client ever formats a secret
itself. A server can still echo one back: a proxy's error page that quotes the request, an
error envelope that repeats the key it rejected, a status message, a field of a row. So each
client cleans what it receives, in one place, before anything reads it: the REST client every
response body and the WebSocket client every frame (`Redactor.received`), and the flat-file
client S3's error words (`redact`). Every row, status, excerpt, error message, and log line
built from them is then free of the secret by construction, wherever it is built.

A JSON text can spell the secret with escapes (a `u0050` escape for `P`), which no search for
the secret itself finds but which decodes to it. `Redactor` therefore matches, for each secret,
a pattern in which every character is itself or any JSON escape that spells it, and replaces
each match in the raw text. That works the same whether the text is valid JSON, cut short, or
not JSON at all (a proxy's HTML page that quotes an escaped key), and it never decodes or
re-encodes anything. A body that holds neither a secret nor an escape able to spell one of its
characters, which is nearly every body, costs one substring search per secret and one pattern
search, and is passed on uncopied.

An empty secret is skipped, since replacing the empty string would insert the marker between
every character. The match errs toward hiding: text that spells a secret with escapes is
replaced even where those escapes are themselves quoted text rather than JSON, and a
replacement that cuts an escape in two can leave a body that no longer decodes, which then
fails as a decoding error rather than show the secret.

This module imports only the standard library. It is part of `net`, the layer every client
shares.
"""

from __future__ import annotations

import re
from typing import Final, overload

__all__ = ["REDACTED", "Redactor", "redact"]

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


class Redactor:
    r"""One client's secrets, ready to clean whatever the client receives.

    A client builds one at construction and passes every response body or frame through
    `received` before anything decodes, quotes, or logs it. It compiles, once, a pattern that
    matches each secret however JSON spells it (every character as itself or as any of its
    escapes, their hex digits in either case), and a cheaper pattern that matches only the
    escapes able to spell a character some secret contains, so a body whose escapes spell
    nothing of a secret (the quotes of a news headline) is never searched with the full one.
    Immutable after construction, so one instance is shared safely by every thread of its
    client.

    Example:
        ::

            redactor = Redactor("my-key")
            assert redactor.received(b'{"error":"bad key my-key"}') == b'{"error":"bad key ***"}'
            assert redactor.received(b"<p>\\u006dy-key</p>") == b"<p>***</p>"
    """

    __slots__ = (
        "_bytes_escapes",
        "_bytes_secrets",
        "_needles",
        "_text_escapes",
        "_text_secrets",
    )

    def __init__(self, *secrets: str | None) -> None:
        """Compile the patterns of the secrets and of the escapes that could spell them.

        Args:
            *secrets: The client's secrets; `None` and the empty string are skipped.
        """
        kept = sorted({secret for secret in secrets if secret}, key=len, reverse=True)
        self._needles: tuple[str, ...] = tuple(kept)
        escapes = sorted({e for secret in kept for c in secret for e in _escapes(c)})
        # Never matches when there is no secret: an empty alternation would match everywhere.
        nothing = r"(?!)"
        prefilter = "|".join(_hex_insensitive(escape) for escape in escapes) or nothing
        self._text_escapes = re.compile(prefilter)
        self._bytes_escapes = re.compile(prefilter.encode())
        self._text_secrets = re.compile("|".join(_spelled(s) for s in kept) or nothing)
        self._bytes_secrets = re.compile(
            b"|".join(_spelled_bytes(s) for s in kept) or nothing.encode()
        )

    @overload
    def received(self, data: bytes) -> bytes: ...

    @overload
    def received(self, data: str) -> str: ...

    @overload
    def received(self, data: bytearray) -> bytes | bytearray: ...

    @overload
    def received(self, data: str | bytes | bytearray) -> str | bytes | bytearray: ...

    def received(self, data: str | bytes | bytearray) -> str | bytes | bytearray:
        """Return a received body or frame with each secret replaced, however JSON spells it.

        Args:
            data: The body or frame as received: `bytes` or a `bytearray` (a streamed REST
                body, never copied to `bytes` here: it can be hundreds of megabytes), or `str`
                for a WebSocket text frame. It is usually JSON, but need not be: a body cut
                short or a proxy's HTML page is cleaned the same way.

        Returns:
            `data` with every spelling of each secret (as it is, with any of its characters
            spelled by a JSON escape, or both) replaced by `REDACTED`; `data` itself, uncopied,
            when nothing was replaced. A cleaned `bytearray` comes back as `bytes` or a
            `bytearray`; both decode the same.
        """
        if isinstance(data, (bytes, bytearray)):
            cleaned: bytes | bytearray = data
            for needle in self._needles:  # the secret as it is, first: see `_spelled`
                raw = needle.encode()
                if raw in cleaned:
                    cleaned = cleaned.replace(raw, REDACTED.encode())
            if self._bytes_escapes.search(cleaned) is not None:
                cleaned = self._bytes_secrets.sub(REDACTED.encode(), cleaned)
            return data if cleaned == data else cleaned
        text = data
        for needle in self._needles:
            if needle in text:
                text = text.replace(needle, REDACTED)
        if self._text_escapes.search(text) is not None:
            text = self._text_secrets.sub(REDACTED, text)
        return data if text == data else text


# The last code point a single four-digit escape spells, and the first high and low surrogates.
_LAST_BMP: Final = 0xFFFF
_HIGH_SURROGATE: Final = 0xD800
_LOW_SURROGATE: Final = 0xDC00
_SURROGATE_BITS: Final = 10

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
    """Return every JSON escape that spells a character, with lower-case hex digits.

    Args:
        character: One character.

    Returns:
        Its `u` escape (for a character beyond the Basic Multilingual Plane, the surrogate
        pair of two), and its short escape when it has one.
    """
    code = ord(character)
    if code > _LAST_BMP:
        offset = code - _LAST_BMP - 1
        high = _HIGH_SURROGATE + (offset >> _SURROGATE_BITS)
        low = _LOW_SURROGATE + (offset & ((1 << _SURROGATE_BITS) - 1))
        long = f"\\u{high:04x}\\u{low:04x}"
    else:
        long = f"\\u{code:04x}"
    short = _SHORT_ESCAPES.get(character)
    return (long,) if short is None else (long, short)


def _hex_insensitive(escape: str) -> str:
    """Return a pattern that matches an escape with its hex digits in either case.

    Args:
        escape: A JSON escape, its hex digits in lower case.

    Returns:
        The escape's pattern: its letters a to f as a class of both cases, everything else
        literal. The `u` itself is lower case in JSON and stays so.
    """
    return "".join(f"[{c}{c.upper()}]" if c in "abcdef" else re.escape(c) for c in escape)


def _spelled(secret: str) -> str:
    """Return a pattern that matches a secret with any of its characters spelled by escapes.

    Args:
        secret: A non-empty secret.

    Each character's group is atomic, its longest spelling tried first: a backslash's three
    spellings all start with a backslash, so a group that could give back what it matched would
    let a secret of backslashes, against a run of them, backtrack exponentially. Atomic, the
    search is linear, at the cost of missing a secret written partly as itself where a character
    of it could also start a longer escape; `Redactor.received` therefore first replaces the
    secret as it is, which the pattern then never needs to find.

    Returns:
        The pattern: for each character, itself or any of its escapes, longest first.
    """
    alternatives = (
        sorted([re.escape(c), *map(_hex_insensitive, _escapes(c))], key=len, reverse=True)
        for c in secret
    )
    return "".join("(?>" + "|".join(group) + ")" for group in alternatives)


def _spelled_bytes(secret: str) -> bytes:
    """Return `_spelled`'s pattern for UTF-8 bytes: each character as its UTF-8 or its escapes.

    Args:
        secret: A non-empty secret.

    Returns:
        The pattern, as bytes.
    """
    alternatives = (
        sorted(
            [re.escape(c.encode()), *(_hex_insensitive(e).encode() for e in _escapes(c))],
            key=len,
            reverse=True,
        )
        for c in secret
    )
    return b"".join(b"(?>" + b"|".join(group) + b")" for group in alternatives)
