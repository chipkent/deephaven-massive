"""API keys: which keys the REST client can send in a header.

The REST client sends the API key as a bearer token in the `Authorization` header, so a key
must be non-empty visible ASCII: no space, tab, or line break, and no letter outside ASCII. A
key read from a file often ends in a newline; sent as is, `http.client` raises an error that
quotes the header, key included.

`check_api_key` is the one body of that rule. The REST client applies it when it is created,
and `MassiveConfig` applies it to `api_key` when the config is built, so a key the client would
refuse is refused at the config, naming the setting, and the two checks cannot drift apart.
Neither the message nor the log line ever shows the key, or any character of it.

The function is pure, so it is safe from any thread. This module imports only the standard
library, so naming it loads no client.
"""

from __future__ import annotations

import logging
import re
from typing import Final

__all__ = ["check_api_key"]

logger = logging.getLogger(__name__)

_VISIBLE_ASCII: Final = re.compile(r"[!-~]+")
"""A non-empty run of visible ASCII: what an API key, sent in a header, may contain."""


def _kind_of(character: str) -> str:
    """Name the kind of a character an API key may not hold, without showing the character.

    Args:
        character: A character outside visible ASCII.

    Returns:
        `"whitespace"`, `"non-ASCII"`, or `"control"`.
    """
    if character.isspace():
        return "whitespace"
    return "control" if character.isascii() else "non-ASCII"


def check_api_key(api_key: str, *, setting: str) -> None:
    """Refuse an API key that cannot be sent in a header, without ever showing it.

    Example:
        ::

            check_api_key("abc123", setting="api_key")  # accepted
            check_api_key("abc 123", setting="api_key")
            # ValueError: api_key must be non-empty visible ASCII, with no whitespace or
            # line break (check for a trailing newline); the key is not shown

    Args:
        api_key: The key.
        setting: The name the caller knows the key by, such as `"api_key"`, which starts the
            message.

    Raises:
        ValueError: If the key is empty or holds anything but visible ASCII (a space, a tab,
            a line break, a non-ASCII letter). The message names `setting` and never includes
            the key; the DEBUG log line gives only the key's length and the position and kind
            of its first refused character.
    """
    if _VISIBLE_ASCII.fullmatch(api_key):
        return
    length = len(api_key)
    first = next((i for i, c in enumerate(api_key) if not _VISIBLE_ASCII.fullmatch(c)), None)
    refused = "none (empty)" if first is None else f"{_kind_of(api_key[first])} at index {first}"
    logger.debug(
        "API key refused: %s has %d characters; first refused character: %s",
        setting,
        length,
        refused,
    )
    raise ValueError(
        f"{setting} must be non-empty visible ASCII, with no whitespace or line break "
        "(check for a trailing newline); the key is not shown"
    )
