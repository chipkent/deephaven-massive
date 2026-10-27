"""Turn a vendor's field description into the plain text recorded in `spec/descriptions.json`.

Massive writes its descriptions for a web page: some carry Markdown links, some HTML links,
some line breaks from the source they were written in. Every place the library shows a
description (a column tooltip, the console, a generated Markdown page) needs one readable line
of text instead, so the refresh applies one fixed set of rules here, with no hand input, and
records only the result.

`plain_text` is the one implementation. Every transport's generator calls it (the REST
generator on each OpenAPI `description`; the WebSocket and flat-file generators on theirs), so
it lives in a module of its own rather than in the OpenAPI reader or the file renderer, which
both serve one stage of one transport only.

It imports the standard library only.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["plain_text"]

# One HTML attribute's worth of characters: anything but `>` and quotes, or a whole quoted value,
# so a `>` inside a quoted value never ends the tag.
_ATTRIBUTES: Final = r"""(?:[^>"']|"[^"]*"|'[^']*')*?"""
# An HTML link: `<a ... href="url" ...>text</a>`. `href` must be a whole attribute name (not
# `data-href`); its value is double-quoted, single-quoted, or unquoted; the text may span lines.
_HTML_LINK: Final = re.compile(
    rf"""<a\s{_ATTRIBUTES}(?<![\w-])href\s*=\s*"""
    r"""(?:"(?P<double>[^"]*)"|'(?P<single>[^']*)'|(?P<bare>[^\s"'>]+))"""
    rf"""{_ATTRIBUTES}>(?P<text>.*?)</a\s*>""",
    re.IGNORECASE | re.DOTALL,
)
# A Markdown inline link: `[text](url)` or `[text](url "title")`. The text may hold one level
# of balanced brackets (`[a [b] c](url)`); the URL has no spaces and is not empty.
_MARKDOWN_LINK: Final = re.compile(
    r"""\[(?P<text>(?:[^\[\]]|\[[^\[\]]*\])*)\]"""
    r"""\((?P<url>[^)\s]+)(?:\s+(?:"[^"]*"|'[^']*'))?\)"""
)
# A run of whitespace, newlines included.
_WHITESPACE: Final = re.compile(r"\s+")


def plain_text(description: str, *, origin: str) -> str:
    """Return a vendor description as plain text, by exactly four rules.

    1. An HTML link `<a href="url" ...>text</a>`, with any other attributes, becomes
       `text (url)`.
    2. A Markdown link `[text](url)`, with or without a title, becomes `text (url)`. HTML links
       are rewritten first, so the result never depends on how the two forms nest.
    3. A link's URL that is root-relative (`/docs/...`, but not `//host/...`) is resolved
       against `origin`, so a reader outside the vendor's web site can follow it.
    4. Every run of whitespace, newlines included, becomes one space, and the ends are trimmed.

    Nothing else changes: entities are not unescaped, other markup is kept, and nothing is
    shortened. Forms the rules do not describe (an `<a>` without `href`, a Markdown reference
    link) are kept as written.

    Args:
        description: The vendor's text, such as an OpenAPI `description`.
        origin: The scheme and host of the vendor's documentation, such as
            `"https://massive.com"`, with no trailing slash; the refresh takes it from the
            documentation index URL the snapshot's manifest records.

    Returns:
        The plain text; `""` for a description that is empty or only whitespace.
    """

    def absolute(url: str) -> str:
        return origin + url if url.startswith("/") and not url.startswith("//") else url

    def rewrite(url_of: Callable[[re.Match[str]], str]) -> Callable[[re.Match[str]], str]:
        return lambda link: f"{link['text']} ({absolute(url_of(link))})"

    text = _HTML_LINK.sub(
        rewrite(lambda link: link["double"] or link["single"] or link["bare"] or ""), description
    )
    text = _MARKDOWN_LINK.sub(rewrite(lambda link: link["url"]), text)
    return _WHITESPACE.sub(" ", text).strip()
