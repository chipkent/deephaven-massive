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


def test_page_urls_are_an_area_s_section_once_each() -> None:
    assert docs.page_urls(_INDEX, docs.REST) == [
        "https://massive.com/docs/rest/stocks/trades-quotes/trades.md",
        "https://massive.com/docs/rest/stocks/overview.md",
    ]
    assert docs.page_urls(_INDEX, docs.WEBSOCKET) == [
        "https://massive.com/docs/websocket/stocks/trades.md"
    ]
    assert docs.page_urls(_INDEX, docs.FLAT_FILES) == [
        "https://massive.com/docs/flat-files/stocks/trades.md"
    ]


@pytest.mark.parametrize("area", [docs.REST, docs.WEBSOCKET, docs.FLAT_FILES])
def test_an_index_without_an_area_s_links_is_rejected(area: docs.Area) -> None:
    with pytest.raises(docs.DocsError, match=f"no '{area.section}' links"):
        docs.page_urls("## Other\n- [T](https://massive.com/docs/other/t.md)\n", area)


def test_a_link_outside_its_area_is_rejected() -> None:
    index = "## REST API\n- [Evil](https://example.com/docs/rest/x.md)\n"
    with pytest.raises(docs.DocsError, match="not a REST API documentation page"):
        docs.page_urls(index, docs.REST)
    index = "## WebSocket\n- [Rest](https://massive.com/docs/rest/x.md)\n"
    with pytest.raises(docs.DocsError, match="not a WebSocket documentation page"):
        docs.page_urls(index, docs.WEBSOCKET)


@pytest.mark.parametrize(
    ("url", "area", "expected"),
    [
        (
            "https://massive.com/docs/rest/stocks/trades-quotes/trades.md",
            docs.REST,
            "stocks/trades-quotes/trades.md",
        ),
        ("https://massive.com/docs/websocket/stocks/trades.md", docs.WEBSOCKET, "stocks/trades.md"),
        (
            "https://massive.com/docs/flat-files/futures/trades/cme.md",
            docs.FLAT_FILES,
            "futures/trades/cme.md",
        ),
    ],
)
def test_page_file_keeps_the_url_path_below_the_area(
    url: str, area: docs.Area, expected: str
) -> None:
    assert docs.page_file(url, area) == PurePosixPath(expected)


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
        docs.page_file(url, docs.REST)


@pytest.mark.parametrize(
    "url",
    ["https://massive.com/docs/rest/stocks/trades.txt", "http://massive.com/docs/rest/a.md"],
)
def test_page_file_rejects_other_pages(url: str) -> None:
    with pytest.raises(docs.DocsError, match="not a REST API documentation page"):
        docs.page_file(url, docs.REST)


def test_title_reads_the_first_level_three_heading() -> None:
    page = "# REST\n## Stocks\n\n### Custom Bars (OHLC)  \n\n#### Not this\n### Nor this\n"
    assert docs.title(page) == "Custom Bars (OHLC)"


def test_a_page_without_a_title_is_rejected() -> None:
    with pytest.raises(docs.DocsError, match="no ### title"):
        docs.title("# REST\n## Stocks\n\n#### Too deep\n###\n")


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


# WebSocket pages.

_FEED = """### Trades

| Feed | URL |
| --- | --- |
| Delayed | `wss://delayed.massive.com/stocks` |
| Real-Time | `wss://socket.massive.com/stocks` |

The channel prefix for this feed is `T`. To subscribe, send `T.` followed by a ticker.
"""


def test_feed_reads_the_cluster_and_the_channel() -> None:
    assert docs.feed(_FEED) == ("stocks", "T")


def test_a_page_without_a_channel_prefix_has_no_feed() -> None:
    assert docs.feed("# Overview\n\n`wss://socket.massive.com/stocks`\n") is None


@pytest.mark.parametrize(
    "page",
    [
        _FEED + "The channel prefix for this feed is `Q`.\n",
        _FEED.replace("delayed.massive.com/stocks", "delayed.massive.com/options"),
        "The channel prefix for this feed is `T`.\n",
    ],
    ids=["two channels", "two clusters", "no cluster"],
)
def test_an_ambiguous_feed_is_rejected(page: str) -> None:
    with pytest.raises(docs.DocsError, match="one channel and one cluster"):
        docs.feed(page)


# Flat-file pages.

_FF = "https://massive.com/docs/flat-files"
_OVERVIEW = f"""## Available Datasets

| Datasets | Description | Path |
| --- | --- | --- |
| **Trades** | | |
| [CME]({_FF}/futures/trades/cme.md) | Trades. | `S3 us_futures_cme/trades_v1` |
| [Day Aggregates]({_FF}/stocks/day-aggregates.md) | Bars. | `S3 us_stocks_sip/day_aggs_v1` |

## Data Availability
"""

_DATASET = """### Day Aggregates

**Endpoint:** `S3 /stocks/day-aggregates`

## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price. |
| `ticker` | string | The symbol. See Exchanges (https://massive.com/x) for more. |
| `window_start` | timestamp - integer | The start. |
| `empty` | integer |  |

## Example Data

| Ticker | window_start | close | empty |
| --- | --- | --- | --- |
| BCC | 1680033600000000000 | 61.99 | |
"""


def test_datasets_reads_each_overview_row_s_page_and_prefix() -> None:
    assert docs.datasets(_OVERVIEW) == [
        ("https://massive.com/docs/flat-files/futures/trades/cme.md", "us_futures_cme/trades_v1"),
        (
            "https://massive.com/docs/flat-files/stocks/day-aggregates.md",
            "us_stocks_sip/day_aggs_v1",
        ),
    ]
    assert docs.datasets(_DATASET) == []


def test_columns_reads_the_columns_table_in_table_order() -> None:
    assert [(c.name, c.type, c.description) for c in docs.columns(_DATASET)] == [
        ("close", "number", "The close price."),
        ("ticker", "string", "The symbol. See Exchanges (https://massive.com/x) for more."),
        ("window_start", "timestamp - integer", "The start."),
        ("empty", "integer", ""),
    ]


def test_a_page_without_a_columns_table_has_no_columns() -> None:
    assert docs.columns(_OVERVIEW) == []
    assert docs.example_header(_OVERVIEW) == []


def test_an_unreadable_columns_row_is_rejected() -> None:
    page = _DATASET.replace("| `close` | number |", "| close | number |")
    with pytest.raises(docs.DocsError, match="unreadable Columns row"):
        docs.columns(page)


def test_a_columns_table_without_a_separator_is_rejected() -> None:
    # Read as a header, its first column row would be lost without a word.
    page = _DATASET.replace("|--------|------|-------------|\n", "")
    with pytest.raises(docs.DocsError, match="unreadable Columns header"):
        docs.columns(page)


def test_example_header_reads_the_files_column_order() -> None:
    assert docs.example_header(_DATASET) == ["Ticker", "window_start", "close", "empty"]


def test_example_rows_reads_each_row_s_cells_in_header_order() -> None:
    assert docs.example_rows(_DATASET) == [["BCC", "1680033600000000000", "61.99", ""]]
    assert docs.example_rows(_OVERVIEW) == []


def test_an_example_row_of_another_width_is_rejected() -> None:
    with pytest.raises(docs.DocsError, match="an Example Data row of 2 cells under 4"):
        docs.example_rows(_DATASET.replace("| BCC | 1680033600000000000 | 61.99 | |", "| a | b |"))


def test_an_example_table_without_a_separator_is_rejected() -> None:
    page = _DATASET.replace("| --- | --- | --- | --- |\n", "")
    with pytest.raises(docs.DocsError, match="unreadable Example Data header"):
        docs.example_header(page)
    with pytest.raises(docs.DocsError, match="unreadable Example Data header"):
        docs.example_header("## Example Data\n\n| a | b |\n")
