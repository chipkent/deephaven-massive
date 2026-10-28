"""Keep a client's secrets out of the text it logs or puts in an error.

The API key travels only in the REST client's `Authorization` header and in the WebSocket
authentication frame, and the S3 secret goes only to boto3, so no client ever formats a secret
itself. A server can still echo one back: a proxy's error page that quotes the request, an
error envelope that repeats the key it rejected, a status message. Every excerpt of a server's
words a client logs or quotes in an error therefore passes through `redact` (or, for an error
already built, `redacted`), which replaces each of the client's secrets with `REDACTED`.

The replacement is exact and literal: one `str.replace` per secret, with no pattern that could
miss a variant or blank out unrelated text. An empty secret is skipped, since replacing the
empty string would insert the marker between every character.

This module imports only the standard library. It is part of `net`, the layer
every client shares.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from deephaven_massive._massive.net.errors import WireError

__all__ = ["REDACTED", "redact", "redacted"]

REDACTED: Final = "***"
"""What each occurrence of a secret becomes."""


def redact(text: str, *secrets: str | None) -> str:
    """Return `text` with every occurrence of each secret replaced by `REDACTED`.

    Call it on any text that came from a server, or that may hold a server's words, before
    logging it or putting it in an error message.

    Args:
        text: The text to clean, such as an excerpt of a response body or a status message.
        *secrets: The client's secrets (the API key, the S3 secret access key). `None` and the
            empty string are skipped.

    Returns:
        The text with each secret replaced; `text` itself when it holds none.
    """
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def redacted(error: WireError, *secrets: str | None) -> WireError:
    """Return `error`, or a copy of it with each secret replaced in its message.

    For an error built from a server's words before the client could clean them, such as one
    raised while decoding a body. The copy has the same class, `status`, `host`, `path`, and
    `request_id`, and its message passes through `redact`. Raise it `from error.__cause__`,
    never `from error`: chaining the original would put its message, secret and all, back in
    the traceback.

    Args:
        error: The error.
        *secrets: The client's secrets, as `redact` takes them.

    Returns:
        `error` itself when its message holds no secret, otherwise the cleaned copy.
    """
    message = redact(error.message, *secrets)
    if message == error.message:
        return error
    return type(error)(
        message,
        status=error.status,
        host=error.host,
        path=error.path,
        request_id=error.request_id,
    )
