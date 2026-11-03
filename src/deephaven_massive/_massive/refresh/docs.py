"""Read Massive's documentation index (`llms.txt`) and the pages it links.

Massive publishes `https://massive.com/docs/llms.txt`, a Markdown index of its documentation:
one `## <section>` heading per product area (`Flat Files`, `REST API`, `WebSocket`, ...), each
followed by a bullet list of `- [<title>](<page url>): <summary>` links. The index names pages,
not endpoints, channels, or files; the pages say what each one documents:

- a **REST page** names its endpoint on one line, `**Endpoint:**` followed by `GET <path>` in
  backticks, in the template syntax the OpenAPI specification uses;
- a **WebSocket page** names its feed: the connection URLs (`wss://socket.massive.com/stocks`,
  whose path is the cluster) and the sentence "The channel prefix for this feed is `T`";
- a **flat-file overview page** lists its class's datasets in a table, one row per dataset
  page with the dataset's bucket prefix (`S3 us_stocks_sip/trades_v1`), and a **flat-file
  dataset page** lists the file's columns in a "Columns" table (name, type, description) and
  shows sample rows under "Example Data", whose header row is the files' own column order.

Overview and quickstart pages document no endpoint, channel, or file.

This module turns those documents into plain values and nothing else:

- `links(index)` lists every link of the index with its section;
- `page_urls(index, area)` lists the pages of one area (`REST`, `WEBSOCKET`, `FLAT_FILES`)
  that `fetch` downloads, after checking each URL;
- `page_file(url, area)` maps a page URL to its file below the area's snapshot directory;
- `title(page)`, `endpoint(page)`, `feed(page)`, `datasets(page)`, `columns(page)`,
  `example_header(page)`, and `example_rows(page)` read what one page documents.

`fetch` and `snapshot` use them to store and find the pages; `rest_sources`, `ws_sources`, and
`flat_file_sources` to decide what is documented. Every input is untrusted vendor text: it is parsed
with anchored regular expressions, never evaluated, and a page URL outside its area is rejected
before it can name a file.

This module imports only the standard library.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Final

__all__ = [
    "FLAT_FILES",
    "REST",
    "WEBSOCKET",
    "Area",
    "DocColumn",
    "DocLink",
    "DocsError",
    "columns",
    "datasets",
    "endpoint",
    "example_header",
    "example_rows",
    "feed",
    "links",
    "page_file",
    "page_urls",
    "title",
]


@dataclass(frozen=True, kw_only=True)
class Area:
    """One area of the documentation: an index section and the URL prefix of its pages.

    Attributes:
        section: The `llms.txt` heading whose links are the area's pages, such as
            `"REST API"`.
        prefix: The URL prefix every page of the area has, ending in `/`.
    """

    section: str
    prefix: str


REST: Final = Area(section="REST API", prefix="https://massive.com/docs/rest/")
"""The REST documentation: one page per endpoint, plus overviews."""

WEBSOCKET: Final = Area(section="WebSocket", prefix="https://massive.com/docs/websocket/")
"""The WebSocket documentation: one page per feed, plus overviews and a quickstart."""

FLAT_FILES: Final = Area(section="Flat Files", prefix="https://massive.com/docs/flat-files/")
"""The flat-file documentation: one page per dataset, an overview per class, a quickstart."""

# A section heading and a link item of the index.
_SECTION = re.compile(r"^## (?P<title>\S.*?)\s*$")
_LINK = re.compile(r"^- \[(?P<title>[^\]]+)\]\((?P<url>[^)\s]+)\)")
# A page's title, its one `###` heading.
_TITLE = re.compile(r"^### (?P<title>\S.*?)\s*$", re.MULTILINE)
# A REST page's endpoint line.
_ENDPOINT = re.compile(r"^\*\*Endpoint:\*\* `GET (?P<path>/[^`\s]*)`\s*$", re.MULTILINE)
# A WebSocket page's channel sentence and connection URLs.
_CHANNEL = re.compile(r"The channel prefix for this feed is `(?P<channel>[A-Z0-9]+)`")
_FEED_URL = re.compile(r"`wss://[a-z0-9.-]+/(?P<cluster>[a-z]+)`")
# A page's path below its area's prefix, without and with the Markdown suffix.
_PAGE = re.compile(r"(?P<file>(?P<relative>[^?#]+)\.md)")
# One path segment of a page URL below the prefix: lower-case words joined by hyphens.
_SEGMENT = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
# A heading of a page, which ends the table under the heading before it.
_HEADING = re.compile(r"^#{1,6} ")
# A flat-file overview row: the dataset page's link, then its bucket prefix in backticks.
_DATASET_ROW = re.compile(
    r"^\| \[[^\]]+\]\((?P<url>[^)\s]+)\) \|.*\| `S3 (?P<prefix>[a-z0-9_]+/[a-z0-9_]+)` \|\s*$"
)
# A "Columns" table row: the column name in backticks, its type, and its description.
_COLUMN_ROW = re.compile(
    r"^\| `(?P<name>[A-Za-z0-9_]+)` \| (?P<type>[a-z -]+?) \| (?P<description>.*?) ?\|\s*$"
)
# The separator row under a table's header, and the lines a table header takes with it.
_SEPARATOR = re.compile(r"^\|(?: ?:?-+:? ?\|)+\s*$")
_HEADER_LINES = 2


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


@dataclass(frozen=True, kw_only=True)
class DocColumn:
    """One row of a flat-file page's "Columns" table.

    Attributes:
        name: The column's CSV header, such as `"sip_timestamp"`.
        type: The documented type: `"integer"`, `"number"`, `"string"`, or
            `"timestamp - integer"`.
        description: The description cell, as the page gives it.
    """

    name: str
    type: str
    description: str


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


def page_file(url: str, area: Area) -> PurePosixPath:
    """Return where a page of an area is stored, relative to the area's snapshot directory.

    The URL's path below the area's prefix is kept, so
    `https://massive.com/docs/rest/stocks/trades-quotes/trades.md` is stored as
    `stocks/trades-quotes/trades.md`. Every directory and the file stem must be lower-case
    words joined by hyphens, so no URL can name a file outside the directory.

    Args:
        url: A page URL from the index.
        area: The area the page belongs to.

    Returns:
        The relative path of the page's file.

    Raises:
        DocsError: If the URL is not below the area's prefix, does not end in `.md`, or has a
            path segment that is not lower-case words joined by hyphens.
    """
    page = _PAGE.fullmatch(url.removeprefix(area.prefix)) if url.startswith(area.prefix) else None
    if page is None:
        raise DocsError(f"not a {area.section} documentation page: {url!r}")
    if not all(_SEGMENT.fullmatch(segment) for segment in page["relative"].split("/")):
        raise DocsError(f"unexpected characters in {area.section} page URL: {url!r}")
    return PurePosixPath(page["file"])


def page_urls(index: str, area: Area) -> list[str]:
    """Return the URLs of an area's pages the index links, in index order, without repeats.

    Args:
        index: The text of `llms.txt`.
        area: The area whose section to read.

    Returns:
        Each link URL of the area's section once.

    Raises:
        DocsError: If the index has no link in the area's section, or a link of that section
            is not a well-formed page URL of the area (see `page_file`).
    """
    urls = list(dict.fromkeys(link.url for link in links(index) if link.section == area.section))
    if not urls:
        raise DocsError(f"the documentation index has no {area.section!r} links")
    for url in urls:
        page_file(url, area)
    return urls


def title(page: str) -> str:
    """Return the title of a documentation page: the text of its first `###` heading.

    A page starts with its area and asset class as `#` and `##` headings (`# REST`,
    `## Stocks`), then names itself in its one `###` heading (`### Custom Bars (OHLC)`). The
    dataset reference shows that name over the page's plan tables and as the text of its link.

    Args:
        page: The Markdown text of a page.

    Returns:
        The heading's text, without the `###` and surrounding whitespace, such as
        `"Custom Bars (OHLC)"`.

    Raises:
        DocsError: If the page has no `###` heading, so it names nothing a link could show.
    """
    found = _TITLE.search(page)
    if found is None:
        raise DocsError("a page has no ### title")
    return found["title"]


def endpoint(page: str) -> str | None:
    """Return the REST path template a REST documentation page documents.

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


def feed(page: str) -> tuple[str, str] | None:
    """Return the WebSocket cluster and channel a WebSocket documentation page documents.

    Args:
        page: The Markdown text of a WebSocket page.

    Returns:
        `(cluster, channel)`, such as `("stocks", "T")`: the path of the page's connection
        URLs and its channel prefix; `None` for a page that names no channel prefix (an
        overview or the quickstart).

    Raises:
        DocsError: If the page names more than one channel prefix, or its connection URLs do
            not name exactly one cluster.
    """
    channels = sorted(set(_CHANNEL.findall(page)))
    if not channels:
        return None
    clusters = sorted(set(_FEED_URL.findall(page)))
    if len(channels) > 1 or len(clusters) != 1:
        raise DocsError(f"a feed page must name one channel and one cluster: {channels} {clusters}")
    return clusters[0], channels[0]


def datasets(page: str) -> list[tuple[str, str]]:
    """Return the datasets a flat-file overview page lists.

    Args:
        page: The Markdown text of a flat-file page.

    Returns:
        `(page url, prefix)` per dataset row, in table order, such as
        `("https://massive.com/docs/flat-files/stocks/trades.md", "us_stocks_sip/trades_v1")`;
        empty for a page with no such table (a dataset page or the quickstart).
    """
    return [
        (row["url"], row["prefix"])
        for line in page.splitlines()
        if (row := _DATASET_ROW.match(line))
    ]


def _table(page: str, heading: str) -> list[str]:
    """Return the lines of the first Markdown table below a `##` heading.

    Args:
        page: The Markdown text of a page.
        heading: The heading's text, such as `"Columns"`.

    Returns:
        The table's lines, header first, up to the next heading; empty when the page has no
        such heading.
    """
    lines = page.splitlines()
    try:
        start = lines.index(f"## {heading}") + 1
    except ValueError:
        return []
    table: list[str] = []
    for line in lines[start:]:
        if _HEADING.match(line):
            break
        if line.startswith("|"):
            table.append(line)
    return table


def columns(page: str) -> list[DocColumn]:
    """Return the rows of a flat-file dataset page's "Columns" table, in table order.

    Args:
        page: The Markdown text of a flat-file page.

    Returns:
        One `DocColumn` per row; empty for a page with no "Columns" heading.

    Raises:
        DocsError: If the table's second line is not a header separator, or a row of the table
            is not a column row (`` | `name` | type | text | ``).
    """
    table = _table(page, "Columns")
    found: list[DocColumn] = []
    for line in _rows(table, "Columns") if table else []:
        row = _COLUMN_ROW.match(line)
        if row is None:
            raise DocsError(f"unreadable Columns row: {line[:120]!r}")
        found.append(
            DocColumn(name=row["name"], type=row["type"], description=row["description"].strip())
        )
    return found


def example_header(page: str) -> list[str]:
    """Return the header row of a flat-file dataset page's "Example Data" table.

    The header lists the columns in the order the vendor's files carry them; the "Columns"
    table above it lists them in an order of its own (alphabetical on most pages). A
    `DocsError` propagates when the table's second line is not a header separator.

    Args:
        page: The Markdown text of a flat-file page.

    Returns:
        The header cells, stripped, in order; empty for a page with no "Example Data" table.
    """
    table = _table(page, "Example Data")
    if not table:
        return []
    _rows(table, "Example Data")
    return _cells(table[0])


def example_rows(page: str) -> list[list[str]]:
    """Return the sample rows of a flat-file dataset page's "Example Data" table.

    Args:
        page: The Markdown text of a flat-file page.

    Returns:
        Each row's cells as the page writes them, stripped, in header order (`""` for an
        empty cell); empty for a page with no "Example Data" table.

    Raises:
        DocsError: If the table's second line is not a header separator, or a row has
            another number of cells than the header.
    """
    table = _table(page, "Example Data")
    if not table:
        return []
    width = len(_cells(table[0]))
    rows = [_cells(line) for line in _rows(table, "Example Data")]
    for row in rows:
        if len(row) != width:
            raise DocsError(f"an Example Data row of {len(row)} cells under {width}: {row}")
    return rows


def _cells(line: str) -> list[str]:
    """Return the cells of a Markdown table line, stripped.

    Args:
        line: The line, `| a | b |`.

    Returns:
        `["a", "b"]`.
    """
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _rows(table: list[str], heading: str) -> list[str]:
    """Return a table's rows below its header, checking the header's separator line.

    Args:
        table: The table's lines, header first (`_table`); not empty.
        heading: The heading the table sits under, for the message.

    Returns:
        The lines after the header and its separator.

    Raises:
        DocsError: If the second line is not a header separator.
    """
    if len(table) < _HEADER_LINES or not _SEPARATOR.match(table[1]):
        raise DocsError(f"unreadable {heading} header: {table[0][:120]!r}")
    return table[_HEADER_LINES:]
