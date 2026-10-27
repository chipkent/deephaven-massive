"""Read Massive's documentation index (`llms.txt`) and the REST pages it links.

Massive publishes `https://massive.com/docs/llms.txt`, a Markdown index of its documentation:
one `## <section>` heading per product area (`Flat Files`, `REST API`, `WebSocket`, ...), each
followed by a bullet list of `- [<title>](<page url>): <summary>` links. The index names pages,
not endpoints. Each REST page names its endpoint on one line, `**Endpoint:**` followed by
`GET <path>` in backticks, in the template syntax the OpenAPI specification uses, so the pages
are what connects the index to the specification's paths. Overview and quickstart pages have
no endpoint line.

This module turns those documents into plain values and nothing else:

- `links(index)` lists every link of the index with its section;
- `rest_page_urls(index)` lists the REST pages `fetch` downloads, after checking each URL;
- `page_file(url)` maps a REST page URL to its file under the snapshot's `rest/` directory;
- `endpoint(page)` reads a page's endpoint line.

`fetch` and `snapshot` use them to store and find the pages, and `rest` to decide which
OpenAPI paths are documented. Every input is untrusted vendor text: it is parsed with anchored
regular expressions, never evaluated, and a page URL outside `https://massive.com/docs/rest/`
is rejected before it can name a file.

This module imports only the standard library.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath

__all__ = [
    "REST_PAGE_PREFIX",
    "REST_SECTION",
    "DocLink",
    "DocsError",
    "endpoint",
    "links",
    "page_file",
    "rest_page_urls",
]

REST_SECTION = "REST API"
"""The `llms.txt` section whose links are the REST pages."""

REST_PAGE_PREFIX = "https://massive.com/docs/rest/"
"""The URL prefix every REST page link must have."""

# A section heading and a link item of the index.
_SECTION = re.compile(r"^## (?P<title>\S.*?)\s*$")
_LINK = re.compile(r"^- \[(?P<title>[^\]]+)\]\((?P<url>[^)\s]+)\)")
# A REST page's endpoint line.
_ENDPOINT = re.compile(r"^\*\*Endpoint:\*\* `GET (?P<path>/[^`\s]*)`\s*$", re.MULTILINE)
# A REST page URL, and its path below the REST prefix with and without the Markdown suffix.
_PAGE = re.compile(r"https://massive\.com/docs/rest/(?P<file>(?P<relative>[^?#]+)\.md)")
# One path segment of a page URL below the REST prefix: lower-case words joined by hyphens.
_SEGMENT = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


class DocsError(ValueError):
    """The documentation index or a page does not have the shape this module reads.

    The refresh stops on it: a malformed index or page is a change at Massive that a person
    must look at, never something to work around.
    """


@dataclass(frozen=True, kw_only=True)
class DocLink:
    """One link of the documentation index.

    Attributes:
        section: The `##` heading the link sits under, such as `"REST API"`.
        title: The link text, such as `"Trades"`.
        url: The page URL exactly as the index gives it.
    """

    section: str
    title: str
    url: str


def links(index: str) -> list[DocLink]:
    """Return every link of the documentation index, in index order.

    A link before the first `##` heading belongs to the section `""`. Lines that are neither
    headings nor links (prose, blank lines) are skipped.

    Args:
        index: The text of `llms.txt`.

    Returns:
        One `DocLink` per `- [title](url)` line.
    """
    section = ""
    found: list[DocLink] = []
    for line in index.splitlines():
        if heading := _SECTION.match(line):
            section = heading["title"]
        elif link := _LINK.match(line):
            found.append(DocLink(section=section, title=link["title"], url=link["url"]))
    return found


def page_file(url: str) -> PurePosixPath:
    """Return where a REST page is stored, relative to the snapshot's `rest/` directory.

    The URL's path below `https://massive.com/docs/rest/` is kept, so
    `https://massive.com/docs/rest/stocks/trades-quotes/trades.md` is stored as
    `stocks/trades-quotes/trades.md`. Every directory and the file stem must be lower-case
    words joined by hyphens, so no URL can name a file outside the directory.

    Args:
        url: A REST page URL from the index.

    Returns:
        The relative path of the page's file.

    Raises:
        DocsError: If the URL is not below the REST prefix, does not end in `.md`, or has a
            path segment that is not lower-case words joined by hyphens.
    """
    page = _PAGE.fullmatch(url)
    if page is None:
        raise DocsError(f"not a REST documentation page: {url!r}")
    if not all(_SEGMENT.fullmatch(segment) for segment in page["relative"].split("/")):
        raise DocsError(f"unexpected characters in REST page URL: {url!r}")
    return PurePosixPath(page["file"])


def rest_page_urls(index: str) -> list[str]:
    """Return the URLs of the REST pages the index links, in index order, without repeats.

    Args:
        index: The text of `llms.txt`.

    Returns:
        Each link URL of the `REST API` section once.

    Raises:
        DocsError: If the index has no `REST API` link, or a link of that section is not a
            well-formed REST page URL (see `page_file`).
    """
    urls = list(dict.fromkeys(link.url for link in links(index) if link.section == REST_SECTION))
    if not urls:
        raise DocsError(f"the documentation index has no {REST_SECTION!r} links")
    for url in urls:
        page_file(url)
    return urls


def endpoint(page: str) -> str | None:
    """Return the REST path template a documentation page documents.

    Args:
        page: The Markdown text of a REST page.

    Returns:
        The path on the page's `**Endpoint:**` line, such as
        `"/v3/trades/{stockTicker}"`, or `None` for a page without one (an overview or a
        quickstart).

    Raises:
        DocsError: If the page has more than one endpoint line, so which one it documents is
            ambiguous.
    """
    found = _ENDPOINT.findall(page)
    if len(found) > 1:
        raise DocsError(f"a page names {len(found)} endpoints: {found}")
    return found[0] if found else None
