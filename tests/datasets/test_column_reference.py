"""Tests for the dataset reference: parity, docstring links, `render`, and `sync`.

The parity test renders the pages from the process's catalog, the dataset calls, and the
committed provenance, in the test JVM, and fails on any page under `docs/datasets/` that is
stale, missing, or extra, naming the command that fixes it. The docstring rule checks that
every dataset method's docstring names its own page. Both are functions over their inputs, so
the tests also run them on a copy of the directory and on synthetic namespaces that break them.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import os
import re
import shutil
import typing
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._dataset_list import dataset_calls
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.column_descriptions import TRANSPORT_ORDER
from deephaven_massive._datasets.column_reference import (
    COMMAND,
    INDEX,
    PAGES_URL,
    DatasetCall,
    SyncResult,
    call_path,
    column_changes,
    page_name,
    page_url,
    render,
    sync,
)
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._massive import (
    Field,
    ParamKind,
    Provenance,
    RestPath,
    Source,
    Transport,
    WireType,
    provenance,
    source,
)
from deephaven_massive._namespaces.query_building import Namespace
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._query import Query

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
DOCS = Path(__file__).resolve().parents[2] / "docs" / "datasets"
# The wire interface names `Source.memberships`, `RestPath.params`, and `Provenance.plans` but
# exports no constructor for their elements, so the tests copy real ones.
MEMBERSHIP = type(source(Transport.WS, "stocks/T").memberships[0])
PARAM = type(source(Transport.REST, "/v3/trades/{stockTicker}").params[0])
_REAL_PLANS = next(iter(provenance().plans.values()))


# --- The parity of docs/datasets ---------------------------------------------------------------


def parity_message(directory: Path, pages: Mapping[str, str]) -> str | None:
    """Say how `directory` differs from the rendered pages, or return None when it does not.

    Args:
        directory: The directory of the committed pages.
        pages: The rendered pages by file name.

    Returns:
        One message naming every stale, missing, and extra entry and the command that fixes
        them; None when the directory holds exactly the pages, byte for byte.
    """
    if not directory.is_dir() or directory.is_symlink():
        return f"docs/datasets/ is not a directory; run `{COMMAND}`"
    present = {entry.name: entry for entry in directory.iterdir()}
    problems = [f"extra: {name}" for name in sorted(present.keys() - pages.keys())]
    for name, text in pages.items():
        entry = present.get(name)
        if entry is None:
            problems.append(f"missing: {name}")
        elif not entry.is_file() or entry.is_symlink() or entry.read_bytes() != text.encode():
            problems.append(f"stale: {name}")
    if not problems:
        return None
    return f"docs/datasets/ is out of date ({'; '.join(problems)}); run `{COMMAND}`"


def rendered() -> dict[str, str]:
    return render(CATALOG, _massive.provenance(), dataset_calls())


def test_the_committed_pages_are_the_rendered_pages() -> None:
    message = parity_message(DOCS, rendered())
    assert message is None, message


def test_every_dataset_has_a_page_after_the_index() -> None:
    assert list(rendered()) == [INDEX, *(f"{name}.md" for name in CATALOG.by_name)]


def test_a_dataset_of_the_session_itself_has_a_page_of_its_own() -> None:
    page = rendered()["snapshots.md"]
    assert page.startswith("# `m.snapshots`\n")
    assert "\nm.snapshots(symbols, *, sort=None, order=None, limit=None, filters=None)\n" in page
    # `m.snapshots` is a method of the session, so the index lists it under no namespace.
    index = rendered()[INDEX]
    assert "\n## Session methods\n\n| Dataset | Summary | Verbs |\n" in index
    assert "[`m.snapshots`](snapshots.md)" in index.partition("## Session methods")[2]
    assert "## `m.snapshots`" not in index


def test_the_real_pages_state_each_poll_mode() -> None:
    pages = rendered()
    replace = "`dm.PollMode.REPLACE`: holds exactly the latest cycle's rows, keyed by `Symbol`"
    assert f"| `poll()` | {replace} |" in pages["options.contracts.md"]
    assert (
        "| `poll()` | `dm.PollMode.APPEND`: appends only rows it has not seen, keyed by `Id` and "
        "`Symbol` |" in pages["reference.news.md"]
    )
    assert (
        "| `poll()` | `dm.PollMode.UPSERT`: adds and updates rows, never deletes, keyed by "
        "`Symbol` and `IssuerName`" in pages["stocks.ipos.md"]
    )
    status = pages["reference.market_status.md"]
    assert (
        "| `poll()` | `dm.PollMode.REPLACE`: holds exactly the latest cycle's rows; "
        "`mode=dm.PollMode.APPEND` also keys by" in (status)
    )
    # A dataset without fetch() has no poll() row, and one without sort or filters none of theirs.
    trades = pages["stocks.trades.md"]
    assert "| `poll()` |" not in trades
    assert "| `sort` |" not in trades
    assert "| `filters` |" not in trades


def test_the_real_pages_list_sort_values_and_filters() -> None:
    pages = rendered()
    assert "| `sort` | `ticker`, `name`, `market`," in pages["reference.tickers.md"]
    assert "| `sort` | any REST field of the rows" in pages["stocks.dividends.md"]
    # The method's own arguments set `type` and `date`, so neither is a filter.
    tickers = next(
        line for line in pages["reference.tickers.md"].split("\n") if line.startswith("| `filters`")
    )
    assert "`type`" not in tickers
    assert "`date`" not in tickers
    assert "`ticker_gte`" in tickers
    assert (
        "| Time range | `start` and `end` are sent as `ex_dividend_date.gte` and"
        in (pages["stocks.dividends.md"])
    )


def test_the_real_pages_show_massives_plan_tables() -> None:
    bars = rendered()["stocks.bars.md"]
    assert "| Plan | Day Aggregates (flat files) | Minute Aggregates (flat files) |" in bars
    assert "| Stocks Basic | Not included | Not included | End-of-day, 2 years |" in bars
    assert "Custom Bars (OHLC) (REST)" in bars
    assert "Aggregates (Per Minute) (WebSocket)" in bars
    # One page per asset class, all of one title, share one column.
    snapshots = rendered()["snapshots.md"]
    assert "| Plan | Unified Snapshot (REST) |\n" in snapshots
    assert "| Stocks Starter | 15-minute delayed |\n" in snapshots


def test_the_wire_columns_follow_the_order_of_every_supplied_by() -> None:
    # The page orders the wire-field columns by transport; every column of the real catalog
    # lists its transports in that same order.
    rank = {transport: index for index, transport in enumerate(TRANSPORT_ORDER)}
    for spec in CATALOG.by_name.values():
        for column in CATALOG.dataset_schema(spec).columns:
            ranks = [rank[t] for t in column.supplied_by]
            assert ranks == sorted(ranks), (spec.name, column.name)


def _plant_stray(directory: Path) -> None:
    (directory / "stray.md").write_text("# Stray\n")


def _plant_directory(directory: Path) -> None:
    (directory / "old").mkdir()
    (directory / "old" / "trades.md").write_text("# Old\n")


def _delete_page(directory: Path) -> None:
    (directory / "stocks.trades.md").unlink()


def _edit_page(directory: Path) -> None:
    page = directory / "crypto.trades.md"
    page.write_text(page.read_text() + "An edit.\n")


@pytest.mark.parametrize(
    ("damage", "found"),
    [
        (_plant_stray, "extra: stray.md"),
        (_plant_directory, "extra: old"),
        (_delete_page, "missing: stocks.trades.md"),
        (_edit_page, "stale: crypto.trades.md"),
    ],
)
def test_the_parity_test_fails_on_damage_that_the_command_repairs(
    tmp_path: Path, damage: Callable[[Path], None], found: str
) -> None:
    copy = tmp_path / "datasets"
    shutil.copytree(DOCS, copy)
    pages = rendered()
    assert parity_message(copy, pages) is None
    damage(copy)
    message = parity_message(copy, pages)
    assert message is not None
    assert found in message
    assert f"run `{COMMAND}`" in message
    sync(copy, pages)
    assert parity_message(copy, pages) is None


def test_the_parity_test_fails_on_a_linked_page_or_a_missing_directory(tmp_path: Path) -> None:
    copy = tmp_path / "datasets"
    shutil.copytree(DOCS, copy)
    pages = rendered()
    # A link to the right bytes is still no committed page: sync would replace it.
    (tmp_path / "stocks.trades.md").write_bytes((copy / "stocks.trades.md").read_bytes())
    (copy / "stocks.trades.md").unlink()
    (copy / "stocks.trades.md").symlink_to(tmp_path / "stocks.trades.md")
    assert "stale: stocks.trades.md" in str(parity_message(copy, pages))
    gone = f"docs/datasets/ is not a directory; run `{COMMAND}`"
    assert parity_message(tmp_path / "absent", pages) == gone
    assert parity_message(tmp_path / "stocks.trades.md", pages) == gone


# --- The docstring rule: every dataset method names its own page -------------------------------


def factory(session: type, path: str) -> Callable[..., object] | None:
    """Return the method a dataset's call path names, walking the namespace properties.

    Args:
        session: The session class, the `m` of the path.
        path: The call path, such as `"m.stocks.financials.ratios"` or `"m.snapshots"`.

    Returns:
        The method, or None when some part of the path names nothing.
    """
    owner: type = session
    *namespaces, method = path.split(".")[1:]
    for name in namespaces:
        member = vars(owner).get(name)
        if not isinstance(member, property) or member.fget is None:
            return None
        owner = typing.get_type_hints(member.fget)["return"]
    found = vars(owner).get(method)
    return found if callable(found) else None


def link_problems(session: type, specs: Iterable[DatasetSpec]) -> list[str]:
    """Return every dataset whose method's docstring does not name its own page.

    Args:
        session: The session class whose namespaces hold the dataset methods.
        specs: The datasets.

    Returns:
        One message per dataset whose call path names no method, or whose method's docstring
        lacks `docs/datasets/<dataset>.md`; another dataset's page, even one whose name starts
        or ends like it, does not count, since the prefix and `.md` bound the name.
    """
    problems = []
    for spec in specs:
        method = factory(session, call_path(spec))
        link = f"docs/datasets/{page_name(spec)}"
        if method is None:
            problems.append(f"{spec.name}: no method at {call_path(spec)}")
        elif link not in (method.__doc__ or ""):
            problems.append(f"{spec.name}: the docstring must name {link}")
    return problems


def test_every_dataset_docstring_names_its_page() -> None:
    assert link_problems(dm.MassiveSession, CATALOG.by_name.values()) == []


class _Linked(Namespace):
    __slots__ = ()

    def dividends(self) -> Query:
        """Cash dividends. Columns: docs/datasets/stocks.dividends.md."""
        raise NotImplementedError

    def splits(self) -> Query:
        """Splits; the page named is another dataset's: docs/datasets/stocks.dividends.md."""
        raise NotImplementedError

    def ipos(self) -> Query:
        raise NotImplementedError

    def financials(self) -> Query:
        """Financials; the page named only starts like this one's.

        docs/datasets/stocks.financials.ratios.md
        """
        raise NotImplementedError

    def news(self) -> Query:
        """News; the page named only ends like this one's: docs/datasets/more.stocks.news.md."""
        raise NotImplementedError


class _LinkedSession:
    @property
    def stocks(self) -> _Linked:
        raise NotImplementedError

    def news(self) -> Query:
        """Any news: docs/datasets/reference.news.md."""
        raise NotImplementedError


def _linked(name: str) -> DatasetSpec:
    return DatasetSpec(name=name, summary="A dataset for the tests.", fetch_path="/x")


def test_the_docstring_rule_reports_every_missing_link() -> None:
    specs = [
        _linked(name)
        for name in (
            "stocks.dividends",
            "stocks.splits",
            "stocks.ipos",
            "stocks.financials",
            "stocks.news",
            "stocks.missing",
            "reference.news",
            "stocks.dividends.more",  # `dividends` is a method, not a namespace
        )
    ]
    assert link_problems(_LinkedSession, specs) == [
        "stocks.splits: the docstring must name docs/datasets/stocks.splits.md",
        "stocks.ipos: the docstring must name docs/datasets/stocks.ipos.md",
        "stocks.financials: the docstring must name docs/datasets/stocks.financials.md",
        "stocks.news: the docstring must name docs/datasets/stocks.news.md",
        "stocks.missing: no method at m.stocks.missing",
        "reference.news: no method at m.reference.news",
        "stocks.dividends.more: no method at m.stocks.dividends.more",
    ]


def test_a_session_method_is_reached_directly() -> None:
    # The path a dataset of the session itself has, such as `m.snapshots`.
    assert factory(_LinkedSession, "m.news") is _LinkedSession.news
    assert factory(_LinkedSession, "m.stocks.dividends") is _Linked.dividends


# --- render on a synthetic catalog -------------------------------------------------------------


def _field(wire_key: str, column: str, wire_type: WireType, *, text: str = "") -> Field:
    return Field(wire_key=wire_key, column=column, wire_type=wire_type, description=text)


_SOURCES: dict[tuple[Transport, str], Source] = {
    (R, "/last/{ticker}"): Source(
        key="/last/{ticker}",
        transport=R,
        fields=(
            _field("ticker", "Ticker", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT, text="When the trade printed."),
            _field("price", "Price", WireType.DOUBLE, text="The price, bid | ask."),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    # The kind's projection renames `p` and drops `x`; the page names the wire key `p`.
    (R, "/trades/{ticker}"): Source(
        key="/trades/{ticker}",
        transport=R,
        fields=(
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "TradePrice", WireType.DOUBLE),
            _field("x", "Extra", WireType.LONG),
        ),
        memberships=(
            MEMBERSHIP(
                kind="trades",
                group="stocks",
                position=1,
                projection={"t": "SipTimestamp", "p": "Price"},
            ),
        ),
    ),
    (F, "us_stocks/trades"): Source(
        key="us_stocks/trades",
        transport=F,
        fields=(
            _field("ticker", "Ticker", WireType.STRING, text="The ticker."),
            _field("sip_timestamp", "SipTimestamp", WireType.ISO_INSTANT),
            _field("price", "Price", WireType.DOUBLE),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    (W, "stocks/T"): Source(
        key="stocks/T",
        transport=W,
        fields=(
            _field("sym", "Ticker", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "Price", WireType.DOUBLE),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    (W, "crypto/XT"): Source(
        key="crypto/XT",
        transport=W,
        fields=(
            _field("pair", "Pair", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "Price", WireType.DOUBLE),
            _field("r", "Received", WireType.ISO_INSTANT, text="When Massive received it."),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="crypto", position=0),),
    ),
    # A list path whose `sort` is free text and whose one other parameter is a filter.
    (R, "/ratios"): RestPath(
        key="/ratios",
        fields=(_field("pe", "PriceEarnings", WireType.DOUBLE, text="Price | earnings."),),
        params=(
            PARAM(name="sort", kind=ParamKind.STRING),
            PARAM(name="pe.gte", kind=ParamKind.NUMBER),
        ),
    ),
}

_STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="Trades of stocks, tick by tick.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    extra_paths=("/last/{ticker}",),
    history_path="/trades/{ticker}",
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks/trades"},
    keys={
        (R, "/last/{ticker}"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
        (R, "/trades/{ticker}"): SourceKeys(
            KeySpec("ticker", from_request=True), KeySpec("SipTimestamp")
        ),
        (F, "us_stocks/trades"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
        (W, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
    },
)
_RATIOS = DatasetSpec(
    name="stocks.financials.ratios", summary="Financial ratios.", fetch_path="/ratios"
)
_CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="Trades of crypto pairs.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    channels={NotBars.ANY: "XT"},
    keys={(W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("SipTimestamp"))},
)
_CATALOG = Catalog(
    (_STOCK_TRADES, _RATIOS, _CRYPTO_TRADES),
    source_tables=lambda transport: {k: s for (t, k), s in _SOURCES.items() if t is transport},
)


def _plan_page(rows: tuple[tuple[str, str, str | None, str | None], ...]) -> object:
    """Return a page's plan tables, built from a real one, as `Provenance.plans` holds them.

    Args:
        rows: Each plan's name, access, recency, and history.

    Returns:
        The page's plan access, with the summaries `"Access."`, `"Recency."`, and `""`.
    """
    terms = _REAL_PLANS.plans[0]
    return dataclasses.replace(
        _REAL_PLANS,
        access="Access.",
        recency="Recency.",
        history="",
        plans=tuple(
            dataclasses.replace(terms, plan=plan, access=access, recency=recency, history=history)
            for plan, access, recency, history in rows
        ),
    )


_PROVENANCE = Provenance(
    fetched=dt.date(2026, 10, 27),
    urls={
        # A source of no asset class: its dataset, which has none either, keeps every page.
        (R, "/ratios"): [
            "https://docs.example/rest/crypto/ratios.md",
            "https://docs.example/rest/stocks/ratios.md",
        ],
        # A shared source: `stocks.trades` keeps only the page whose path names `stocks`.
        (W, "stocks/T"): [
            "https://docs.example/ws/crypto/trades.md",
            "https://massive.com/docs/websocket/stocks/trades.md",
        ],
        # No page names the class, so the dataset keeps them all; the duplicate is listed once.
        (R, "/trades/{ticker}"): ["https://docs.example/a.md", "https://docs.example/a.md"],
        (W, "crypto/XT"): ["https://docs.example/ws/crypto.md"],
    },
    plans=cast(
        "Mapping[str, Any]",
        {
            "https://docs.example/a.md": _plan_page(
                (
                    ("Stocks Basic", "Included", "End-of-day", "2 years"),
                    ("Stocks Starter", "Included", None, None),
                    ("Stocks Business", "Not included", None, None),
                ),
            ),
            "https://massive.com/docs/websocket/stocks/trades.md": _plan_page(
                (
                    ("Stocks Basic", "Included", "Not included", None),
                    ("Stocks Advanced", "Included", "Real-time", None),
                ),
            ),
            # A page that lists no plan, only its summaries.
            "https://docs.example/rest/stocks/ratios.md": _plan_page(()),
        },
    ),
    titles={
        "https://docs.example/rest/crypto/ratios.md": "Ratios",
        "https://docs.example/rest/stocks/ratios.md": "Ratios",
        "https://docs.example/ws/crypto/trades.md": "Trades",
        "https://massive.com/docs/websocket/stocks/trades.md": "Trades",
        "https://docs.example/a.md": "Trades",
        "https://docs.example/ws/crypto.md": "Crypto Trades",
    },
)

_CALLS = {
    "stocks.trades": DatasetCall(
        signature="m.stocks.trades(symbols)", verbs=("blink", "history"), arguments=("symbols",)
    ),
    "stocks.financials.ratios": DatasetCall(
        signature="m.stocks.financials.ratios(*, sort=None, filters=None)",
        verbs=("fetch", "poll"),
        arguments=("sort", "filters"),
        typed=("sort", "filters"),
    ),
    "crypto.trades": DatasetCall(
        signature="m.crypto.trades(symbols)", verbs=("blink",), arguments=("symbols",)
    ),
}

_FOOTER = (
    "Generated by `uv run python scripts/render_reference.py` from Massive's documentation as "
    "fetched on 2026-10-27. The descriptions and plan tables are Massive's, descriptions "
    "corrected where they are clearly wrong; the library writes its own for the key columns "
    "and the columns it computes."
)


def _rendered() -> dict[str, str]:
    return render(_CATALOG, _PROVENANCE, _CALLS)


def test_render_gives_the_index_and_one_page_per_dataset_in_catalog_order() -> None:
    assert list(_rendered()) == [
        INDEX,
        "stocks.trades.md",
        "stocks.financials.ratios.md",
        "crypto.trades.md",
    ]
    assert list(render(Catalog(()), _PROVENANCE, {})) == [INDEX]


def test_render_needs_every_datasets_call() -> None:
    with pytest.raises(
        KeyError, match=re.escape("no call given for the datasets ['crypto.trades']")
    ):
        render(_CATALOG, _PROVENANCE, {k: v for k, v in _CALLS.items() if k != "crypto.trades"})


CATALOG_INSERT = next(
    c.description
    for c in _CATALOG.dataset_schema(_STOCK_TRADES).columns
    if c.name == "TimestampInsert"
)
"""Every dataset's arrival-time description, the same on every page."""


def test_the_rendered_index() -> None:
    index = _rendered()[INDEX]
    classes = (
        "| Dataset | `m.stocks` | `m.options` | `m.futures` | `m.indices` | `m.forex` "
        "| `m.crypto` |"
    )
    # `trades` is shared by two classes, so it has a grid row; the ratios are listed only.
    grid = "| `trades` | [✓](stocks.trades.md) |  |  |  |  | [✓](crypto.trades.md) |"
    assert f"\n{classes}\n| --- | --- | --- | --- | --- | --- | --- |\n{grid}\n\nTables of" in index
    assert "`financials.ratios` |" not in index
    stocks = (
        "## `m.stocks`\n\n| Dataset | Summary | Verbs |\n| --- | --- | --- |\n"
        "| [`m.stocks.trades`](stocks.trades.md) | Trades of stocks, tick by tick. "
        "| blink, history |\n"
        "| [`m.stocks.financials.ratios`](stocks.financials.ratios.md) | Financial ratios. | "
        "fetch, poll |\n"
    )
    assert stocks in index
    assert index.endswith(f"\n---\n\n{_FOOTER}\n")
    # A catalog in which no two classes share a dataset has no grid.
    alone = Catalog(
        (_RATIOS,),
        source_tables=lambda transport: {k: s for (t, k), s in _SOURCES.items() if t is transport},
    )
    assert "By asset class" not in render(alone, _PROVENANCE, _CALLS)[INDEX]


def test_the_rendered_stocks_trades_page() -> None:
    page = _rendered()["stocks.trades.md"]
    columns = _CATALOG.dataset_schema(_STOCK_TRADES, raw_symbol=True).columns
    schema = {c.name: c for c in columns}
    symbol = schema["Symbol"].description.replace("|", "\\|")
    stamp = schema["Timestamp"].description.replace("|", "\\|")
    # Only crypto trades fill `Received`, so the stocks dataset has no such column.
    assert "Received" not in schema
    day = schema["Date"].description
    insert = schema["TimestampInsert"].description
    raw = schema["RawSymbol"].description
    every = "REST, flat files, WebSocket"
    expected = f"""# `m.stocks.trades`

<!-- Generated by `uv run python scripts/render_reference.py`; do not edit by hand. -->

Trades of stocks, tick by tick.

```python
m.stocks.trades(symbols)
```

[All datasets](README.md)

|  |  |
| --- | --- |
| Verbs | `blink()`, `history()` |
| Served by | REST, flat files, WebSocket |

## Plan access

Which of Massive's plans include this dataset, how recent its data is on each, and how far \
back its history goes, as Massive's pages state them.

| Plan | Trades (Massive) | Trades (WebSocket) |
| --- | --- | --- |
| Stocks Basic | End-of-day, 2 years | Included |
| Stocks Starter | Included |  |
| Stocks Business | Not included |  |
| Stocks Advanced |  | Real-time |

## Columns

| Column | Deephaven type | Description | Supplied by |
| --- | --- | --- | --- |
| `Symbol` | String | {symbol} | {every} |
| `Date` | String | {day} | {every} |
| `Timestamp` | Instant | {stamp} | {every} |
| `RawSymbol` | String | {raw} | {every} |
| `Price` | double | The price, bid \\| ask. | {every} |
| `TimestampInsert` | Instant | {insert} | {every} |

<details><summary>Massive fields</summary>

The Massive field behind each column, per transport (`request` for a value the library fills \
from the request; empty for a column the library computes).

| Column | REST field | Flat-file field | WebSocket field |
| --- | --- | --- | --- |
| `Symbol` | request, `ticker` | `ticker` | `sym` |
| `Date` |  |  |  |
| `Timestamp` | `t` | `sip_timestamp` | `t` |
| `RawSymbol` | request, `ticker` | `ticker` | `sym` |
| `Price` | `p`, `price` | `price` | `p` |
| `TimestampInsert` |  |  |  |

</details>

Massive's documentation of this dataset:

- [Trades (Massive)](https://docs.example/a)
- [Trades (WebSocket)](https://massive.com/docs/websocket/stocks/trades)

---

{_FOOTER}
"""
    assert page == expected


def test_the_rendered_ratios_page() -> None:
    page = _rendered()["stocks.financials.ratios.md"]
    facts = (
        "| Verbs | `fetch()`, `poll()` |\n"
        "| Served by | REST |\n"
        "| `poll()` | `dm.PollMode.REPLACE`: holds exactly the latest cycle's rows, keyed by "
        "`Symbol` |\n"
        "| `sort` | any REST field of the rows (see Massive fields below), or several separated "
        "by commas, each with an optional `.asc` or `.desc` |\n"
        "| `filters` | `pe_gte` |\n"
    )
    assert facts in page
    # A page that lists no plan shows its summaries; a page of no plan tables adds nothing.
    assert "\n| Plan |" not in page
    assert "\n- Ratios (Massive): Access.; Recency.\n" in page
    # No RawSymbol column.
    assert "RawSymbol" not in page
    assert "| `PriceEarnings` | double | Price \\| earnings. | REST |\n" in page


def test_sort_and_filters_of_a_path_that_takes_neither() -> None:
    bare = dataclasses.replace(cast("RestPath", _SOURCES[R, "/ratios"]), params=())
    catalog = Catalog(
        (_RATIOS,),
        source_tables=lambda transport: (
            {"/ratios": bare}
            if transport is R
            else {k: s for (t, k), s in _SOURCES.items() if t is transport}
        ),
    )
    page = render(catalog, _PROVENANCE, _CALLS)["stocks.financials.ratios.md"]
    assert "| `sort` | not taken |\n| `filters` | none |\n" in page


def test_the_rendered_crypto_trades_page() -> None:
    page = _rendered()["crypto.trades.md"]
    # One wire-field column for the one transport the dataset reads, and its own URLs only.
    assert "| Column | WebSocket field |\n" in page
    received = "| `Received` | Instant | When Massive received it. | WebSocket |\n"
    assert received in page
    links = (
        "\nMassive's documentation of this dataset:\n\n"
        "- [Crypto Trades (Massive)](https://docs.example/ws/crypto)\n"
    )
    assert page.endswith(f"</details>\n{links}\n---\n\n{_FOOTER}\n")
    # Its page has no plan tables.
    assert "## Plan access" not in page


def test_a_dataset_whose_sources_have_no_urls_has_no_url_list() -> None:
    bare = Provenance(fetched=dt.date(2026, 10, 27), urls={})
    page = render(_CATALOG, bare, _CALLS)["crypto.trades.md"]
    assert page.endswith(f"</details>\n\n---\n\n{_FOOTER}\n")
    assert "documentation of this dataset" not in page


def test_the_real_pages_link_only_the_datasets_own_class() -> None:
    options = rendered()["options.snapshots.md"]
    snapshot = "https://massive.com/docs/rest/{}/snapshots/unified-snapshot"
    # A link opens the web page: the Markdown URL without its `.md`.
    assert f"- [Unified Snapshot (REST)]({snapshot.format('options')})\n" in options
    for other in ("stocks", "forex", "crypto", "indices"):
        assert snapshot.format(other) not in options
    # The session-wide snapshot spans classes, so it links every class's page, each of one
    # title, so each label names its class.
    snapshots = rendered()["snapshots.md"]
    for every in ("stocks", "options", "forex", "crypto", "indices"):
        assert f"- [Unified Snapshot (REST, {every})]({snapshot.format(every)})\n" in snapshots


def test_every_page_line_is_clean() -> None:
    for page in (*_rendered().values(), *rendered().values()):
        assert page.endswith("\n")
        assert not page.endswith("\n\n")
        assert all(line == line.rstrip() for line in page.split("\n"))


def test_page_names_urls_and_call_paths() -> None:
    assert page_name(_STOCK_TRADES) == "stocks.trades.md"
    assert page_name(_RATIOS) == "stocks.financials.ratios.md"
    assert page_url(_RATIOS) == f"{PAGES_URL}stocks.financials.ratios.md"
    assert call_path(_RATIOS) == "m.stocks.financials.ratios"
    snapshots = CATALOG.by_name["snapshots"]
    assert (page_name(snapshots), call_path(snapshots)) == ("snapshots.md", "m.snapshots")
    news = CATALOG.by_name["partners.benzinga.news"]
    assert (page_name(news), call_path(news)) == (
        "partners.benzinga.news.md",
        "m.partners.benzinga.news",
    )


# --- sync --------------------------------------------------------------------------------------


def test_sync_writes_the_pages_into_a_missing_directory(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "datasets"
    result = sync(target, {"b.md": "B\n", "a.md": "A\n"})
    assert result == SyncResult(written=("a.md", "b.md"), deleted=())
    assert sorted(p.name for p in target.iterdir()) == ["a.md", "b.md"]
    assert (target / "a.md").read_bytes() == b"A\n"


def test_sync_with_no_pages_on_a_missing_directory_creates_nothing(tmp_path: Path) -> None:
    target = tmp_path / "datasets"
    assert sync(target, {}) == SyncResult(written=(), deleted=())
    assert not target.exists()


def test_sync_leaves_an_unchanged_page_untouched(tmp_path: Path) -> None:
    page = tmp_path / "a.md"
    page.write_bytes(b"A\n")
    os.utime(page, ns=(1_000_000_000, 1_000_000_000))
    (tmp_path / "b.md").write_bytes(b"old\n")
    result = sync(tmp_path, {"a.md": "A\n", "b.md": "new\n"})
    assert result == SyncResult(written=("b.md",), deleted=())
    assert page.stat().st_mtime_ns == 1_000_000_000
    assert (tmp_path / "b.md").read_bytes() == b"new\n"


def test_sync_deletes_everything_it_does_not_render(tmp_path: Path) -> None:
    target = tmp_path / "datasets"
    target.mkdir()
    (target / "stray.md").write_text("x")
    (target / "nested" / "deeper").mkdir(parents=True)
    (target / "nested" / "deeper" / "page.md").write_text("x")
    # A page's name taken by a directory: the directory goes, the page is written.
    (target / "a.md").mkdir()
    (target / "a.md" / "inner.md").write_text("x")
    result = sync(target, {"a.md": "A\n"})
    assert result == SyncResult(written=("a.md",), deleted=("a.md", "nested", "stray.md"))
    assert [p.name for p in target.iterdir()] == ["a.md"]
    assert (target / "a.md").read_bytes() == b"A\n"


def test_sync_never_follows_a_symbolic_link_out_of_its_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_text("keep\n")
    (outside / "file.md").write_text("outside\n")
    target = tmp_path / "datasets"
    target.mkdir()
    (target / "linked").symlink_to(outside, target_is_directory=True)
    (target / "a.md").symlink_to(outside / "file.md")
    result = sync(target, {"a.md": "A\n"})
    assert result == SyncResult(written=("a.md",), deleted=("a.md", "linked"))
    # The links are gone, a real file replaces one, and nothing outside changed.
    assert not (target / "a.md").is_symlink()
    assert (target / "a.md").read_bytes() == b"A\n"
    assert sorted(p.name for p in target.iterdir()) == ["a.md"]
    assert (outside / "keep.md").read_text() == "keep\n"
    assert (outside / "file.md").read_text() == "outside\n"


def test_sync_refuses_a_directory_that_is_a_symbolic_link(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "important.txt").write_text("keep\n")
    link = tmp_path / "datasets"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="is a symbolic link"):
        sync(link, {"a.md": "A\n"})
    assert [p.name for p in target.iterdir()] == ["important.txt"]


@pytest.mark.parametrize("name", ["", ".", "..", "../escape.md", "sub/page.md", "/abs.md"])
def test_sync_refuses_a_page_name_that_is_not_a_plain_file_name(tmp_path: Path, name: str) -> None:
    (tmp_path / "stray.md").write_text("x")
    with pytest.raises(ValueError, match="is not a plain file name"):
        sync(tmp_path, {"a.md": "A\n", name: "x"})
    # Checked before anything is touched.
    assert [p.name for p in tmp_path.iterdir()] == ["stray.md"]


# --- column_changes ----------------------------------------------------------------------------


def test_the_column_report_reads_back_every_dataset_of_the_real_pages() -> None:
    pages = rendered()
    # Rendering the same catalog twice changes nothing.
    assert column_changes(pages, pages) == []
    # A first rendering names every dataset as new, with its whole column list.
    report = column_changes({}, pages)
    assert len(report) == len(CATALOG.by_name)
    snapshots = CATALOG.dataset_schema(CATALOG.by_name["snapshots"], raw_symbol=True)
    assert f"m.snapshots: new dataset with {len(snapshots.columns)} columns" in report


def test_the_column_report_names_added_removed_and_retyped_columns() -> None:
    def page(call: str, rows: str) -> str:
        # The facts and the folded wire fields have rows that start like column rows too.
        return (
            f"# `{call}`\n\nSummary.\n\n| `sort` | `ticker` |\n\n## Columns\n\n"
            f"| Column | Deephaven type |\n{rows}\n<details><summary>x</summary>\n\n"
            "| `Wire` | `w` |\n\n</details>\n\n## Later\n\n| `After` | long |\n"
        )

    before = {
        INDEX: "# Dataset reference\n\n| `trades` | [✓](a.x.md) |\n",
        "a.x.md": page("m.a.x", "| `Keep` | long |\n| `Gone` | double |\n| `Codes` | int[] |\n"),
        "b.y.md": page("m.b.y", "| `Only` | String |\n"),
    }
    after = {
        "a.x.md": page("m.a.x", "| `Keep` | long |\n| `Codes` | long[] |\n| `New` | String[] |\n"),
        "c.z.md": page("m.c.z", "| `One` | String |\n"),
    }
    assert column_changes(before, after) == [
        "m.a.x: added New; removed Gone; retyped Codes int[] to long[]",
        "m.c.z: new dataset with 1 columns",
        "m.b.y: dataset removed",
    ]


def test_a_table_row_before_a_page_s_first_heading_belongs_to_no_dataset() -> None:
    first = "# `m.a.x`\n\n## Columns\n\n| Column | Deephaven type |\n| `Keep` | long |\n"
    second = (
        "# B\n\n## Columns\n\n| `Stray` | String |\n\n# `m.b.y`\n\n## Columns\n\n"
        "| `Only` | String |\n"
    )
    report = column_changes({"a.md": first, "b.md": second}, {"a.md": first})
    assert report == ["m.b.y: dataset removed"]  # m.a.x did not gain Stray


def test_the_merge_recipe_and_the_grid_are_on_the_index() -> None:
    pages = rendered()
    assert "To merge them" in pages[INDEX]
    assert "| `bars` | [✓](stocks.bars.md) | [✓](options.bars.md) |" in pages[INDEX]
    assert "To merge them" not in pages["stocks.trades.md"]


def test_the_real_index_lists_each_shared_column_whose_type_differs() -> None:
    # Massive declares a snapshot's or a mover's trade size a whole number for stocks and a
    # decimal for crypto, so the index names the column and the cast that merges them.
    index = rendered()[INDEX]
    assert "\n- `movers`: `LastTradeSize` is long for stocks, double for crypto\n" in index
    assert "`LastTradeSize` is long for stocks, options and futures, double for crypto" in index
    assert '`a.update("LastTradeSize = (double) LastTradeSize")`' in index
    # The test catalog's stock and crypto trades agree on every shared column's type.
    assert "Cast one table's column" not in _rendered()[INDEX]


def test_the_index_says_what_every_type_holds() -> None:
    index = _rendered()[INDEX]
    legend = next(line for line in index.splitlines() if "Deephaven type is one of" in line)
    for column_type in ColumnType:
        assert f"`{column_type.value}`, " in legend
    assert "`long[]`, an array of `long`" in legend


def test_each_vendor_link_is_labeled_and_opens_the_web_page() -> None:
    # Pages of one title name the class their URL names; a `.md` inside the path stays.
    urls = [
        "https://docs.example/rest/crypto/ratios.md",
        "https://docs.example/rest/stocks/ratios.md",
        "https://docs.example/rest/ratios.md",
        "https://docs.example/rest/more/ratios.md",
        "https://massive.com/docs/rest/x.md/summary.md",
    ]
    titles = dict.fromkeys(urls[:4], "Ratios") | {urls[4]: "Summary"}
    linked = Provenance(fetched=dt.date(2026, 10, 27), urls={(R, "/ratios"): urls}, titles=titles)
    page = render(_CATALOG, linked, _CALLS)["stocks.financials.ratios.md"]
    assert (
        "Massive's documentation of this dataset:\n\n"
        "- [Ratios (Massive, crypto)](https://docs.example/rest/crypto/ratios)\n"
        "- [Ratios (Massive)](https://docs.example/rest/more/ratios)\n"
        "- [Ratios (Massive)](https://docs.example/rest/ratios)\n"
        "- [Ratios (Massive, stocks)](https://docs.example/rest/stocks/ratios)\n"
        "- [Summary (REST)](https://massive.com/docs/rest/x.md/summary)\n"
    ) in page


def test_a_real_key_column_names_the_path_of_each_differing_quote() -> None:
    # Custom bars stamp `t` at the window's start, the grouped daily bars at its end.
    bars = rendered()["stocks.bars.md"]
    start = next(line for line in bars.splitlines() if line.startswith("| `TimestampStart`"))
    custom = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
    grouped = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
    assert f'REST `{custom}` `t`: "The Unix millisecond timestamp for the start' in start
    assert f'REST `{grouped}` `t`: "The Unix millisecond timestamp for the end' in start
