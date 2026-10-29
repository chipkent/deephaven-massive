"""Keep a client's secrets out of everything it decodes, logs, or puts in an error.

The API key travels only in the REST client's `Authorization` header and in the WebSocket
authentication frame, and the S3 secret goes only to boto3, so no client ever formats a secret
itself. A server can still echo one back: a proxy's error page that quotes the request, an
error envelope that repeats the key it rejected, a status message, a field of a row. So each
client cleans what it receives, in one place, before anything reads it: the REST client every
response body and the WebSocket client every frame (`clean_received`), and the flat-file client
S3's error words (`redact`). Every row, status, excerpt, error message, and log line built from
them is then free of the secret by construction, wherever it is built.

A JSON text can spell the secret with escapes (a `u0050` escape for `P`), which no search of the raw
bytes finds but which decodes to the secret. `clean_received` therefore also replaces the
secret's own JSON-escaped form, and when a body or frame holds any escape at all it decodes it,
replaces the secret in every decoded string, and re-encodes it if anything changed. A body
without a backslash, which is nearly every market-data body, costs one substring search per
secret and is passed on uncopied.

The replacement is exact and literal: no pattern that could miss a variant or blank out
unrelated text. An empty secret is skipped, since replacing the empty string would insert the
marker between every character.

This module imports only the standard library and `net.codec`. It is part of `net`, the layer
every client shares.
"""

from __future__ import annotations

import json
from typing import Final, overload

from deephaven_massive._massive.net import codec

__all__ = ["REDACTED", "clean_received", "redact", "redact_value"]

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


@overload
def clean_received(data: bytes, *secrets: str | None) -> bytes: ...


@overload
def clean_received(data: str, *secrets: str | None) -> str: ...


@overload
def clean_received(data: str | bytes, *secrets: str | None) -> str | bytes: ...


def clean_received(data: str | bytes, *secrets: str | None) -> str | bytes:
    """Return a received body or frame with each secret replaced, however its JSON spells it.

    Call it once on everything a server sends, before anything decodes, quotes, or logs it.

    Args:
        data: The body or frame as received: `bytes`, or `str` for a WebSocket text frame. It
            is usually JSON, but need not be (a proxy's HTML page).
        *secrets: The client's secrets, as `redact` takes them.

    Returns:
        `data` with each secret and its JSON-escaped form replaced by `REDACTED`, and, when it
        holds a JSON escape, re-encoded with the secret replaced in every decoded string; `data`
        itself, uncopied, when nothing was replaced. Text that is not JSON keeps its escapes
        as they are: they decode to nothing anyone reads.
    """
    needles = [needle for secret in secrets if secret for needle in _spellings(secret)]
    if not needles:
        return data
    if isinstance(data, bytes):
        for needle in needles:
            raw = needle.encode()
            if raw in data:
                data = data.replace(raw, REDACTED.encode())
        escaped = b"\\" in data
    else:
        for needle in needles:
            if needle in data:
                data = data.replace(needle, REDACTED)
        escaped = "\\" in data
    if not escaped:
        return data
    try:
        value = codec.loads(data)
    except ValueError:
        return data
    cleaned = redact_value(value, *secrets)
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
