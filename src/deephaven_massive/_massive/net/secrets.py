"""Keep a client's secrets out of everything it decodes, logs, or puts in an error.

The API key travels only in the REST client's `Authorization` header and in the WebSocket
authentication frame, and the S3 secret goes only to boto3, so no client ever formats a secret
itself. A server can still echo one back: a proxy's error page that quotes the request, an
error envelope that repeats the key it rejected, a status message, a field of a row. So each
client cleans what it receives before anything reads it: the REST client every response body
(`redact_bytes`), the WebSocket client every frame (`redact`), and the flat-file client S3's
error words (`redact`). Every excerpt, error message, decoded row, and log line built from them
is then free of the secret by construction, wherever it is built.

The replacement is exact and literal: one `replace` per secret, with no pattern that could miss
a variant or blank out unrelated text, and it runs only when the secret is present (`in`, a fast
substring search), so a body without it is passed on unchanged and uncopied. An empty secret is
skipped, since replacing the empty string would insert the marker between every byte.

This module imports only the standard library. It is part of `net`, the layer every client
shares.
"""

from __future__ import annotations

from typing import Final

__all__ = ["REDACTED", "redact", "redact_bytes"]

REDACTED: Final = "***"
"""What each occurrence of a secret becomes."""


def redact(text: str, *secrets: str | None) -> str:
    """Return `text` with every occurrence of each secret replaced by `REDACTED`.

    Args:
        text: The text to clean, such as a WebSocket frame or S3's error message.
        *secrets: The client's secrets (the API key, the S3 secret access key). `None` and the
            empty string are skipped.

    Returns:
        The text with each secret replaced; `text` itself when it holds none.
    """
    for secret in secrets:
        if secret and secret in text:
            text = text.replace(secret, REDACTED)
    return text


def redact_bytes(data: bytes, *secrets: str | None) -> bytes:
    """Return `data` with every occurrence of each secret's UTF-8 bytes replaced by `REDACTED`.

    For a body received as bytes, cleaned before it is decoded, quoted, or logged.

    Args:
        data: The bytes to clean, such as a REST response body.
        *secrets: The client's secrets, as `redact` takes them.

    Returns:
        The bytes with each secret replaced; `data` itself, uncopied, when it holds none.
    """
    for secret in secrets:
        if secret and (needle := secret.encode()) in data:
            data = data.replace(needle, REDACTED.encode())
    return data
