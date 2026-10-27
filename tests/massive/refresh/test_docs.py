from pathlib import PurePosixPath

import pytest

from deephaven_massive._massive.refresh import docs

_INDEX = """# Massive

> Market data.

- [Before any section](https://massive.com/docs/index.md): stray

## Flat Files

- [Trades](https://massive.com/docs/flat-files/stocks/trades.md): Tick-level trades

## REST API

REST endpoints.

- [Trades](https://massive.com/docs/rest/stocks/trades-quotes/trades.md): Trades
- [Overview](https://massive.com/docs/rest/stocks/overview.md)
- [Trades again](https://massive.com/docs/rest/stocks/trades-quotes/trades.md): repeat

## WebSocket

- [Trades](https://massive.com/docs/websocket/stocks/trades.md): Stream trades
"""


def test_links_keep_their_sections_in_index_order() -> None:
    assert [(link.section, link.title) for link in docs.links(_INDEX)] == [
        ("", "Before any section"),
        ("Flat Files", "Trades"),
        ("REST API", "Trades"),
        ("REST API", "Overview"),
        ("REST API", "Trades again"),
        ("WebSocket", "Trades"),
    ]


def test_rest_page_urls_are_the_rest_section_once_each() -> None:
    assert docs.rest_page_urls(_INDEX) == [
        "https://massive.com/docs/rest/stocks/trades-quotes/trades.md",
        "https://massive.com/docs/rest/stocks/overview.md",
    ]


def test_an_index_without_rest_links_is_rejected() -> None:
    with pytest.raises(docs.DocsError, match="no 'REST API' links"):
        docs.rest_page_urls("## WebSocket\n- [T](https://massive.com/docs/websocket/t.md)\n")


def test_a_rest_link_outside_the_rest_documentation_is_rejected() -> None:
    index = "## REST API\n- [Evil](https://example.com/docs/rest/x.md)\n"
    with pytest.raises(docs.DocsError, match="not a REST documentation page"):
        docs.rest_page_urls(index)


def test_page_file_keeps_the_url_path() -> None:
    url = "https://massive.com/docs/rest/stocks/trades-quotes/trades.md"
    assert docs.page_file(url) == PurePosixPath("stocks/trades-quotes/trades.md")


@pytest.mark.parametrize(
    "url",
    [
        "https://massive.com/docs/rest/../../etc/passwd.md",
        "https://massive.com/docs/rest/stocks//trades.md",
        "https://massive.com/docs/rest/Stocks/trades.md",
        "https://massive.com/docs/rest/stocks/trades_v1.md",
        "https://massive.com/docs/rest/stocks/trades.md?x=1",
    ],
)
def test_page_file_rejects_unexpected_characters(url: str) -> None:
    with pytest.raises(docs.DocsError):
        docs.page_file(url)


@pytest.mark.parametrize(
    "url",
    ["https://massive.com/docs/rest/stocks/trades.txt", "http://massive.com/docs/rest/a.md"],
)
def test_page_file_rejects_other_pages(url: str) -> None:
    with pytest.raises(docs.DocsError, match="not a REST documentation page"):
        docs.page_file(url)


def test_endpoint_reads_the_endpoint_line() -> None:
    page = "### Trades\n\n**Endpoint:** `GET /v3/trades/{stockTicker}`\n\nText.\n"
    assert docs.endpoint(page) == "/v3/trades/{stockTicker}"


def test_a_page_without_an_endpoint_line_has_none() -> None:
    assert docs.endpoint("# Overview\n\nNo endpoint here; not even `GET /v3/x` in prose.\n") is None


def test_an_endpoint_must_stand_on_its_own_line() -> None:
    assert docs.endpoint("See **Endpoint:** `GET /v3/x` inline.\n") is None


def test_a_page_with_two_endpoints_is_rejected() -> None:
    page = "**Endpoint:** `GET /a`\n**Endpoint:** `GET /b`\n"
    with pytest.raises(docs.DocsError, match="2 endpoints"):
        docs.endpoint(page)
