"""Tests for the `m.stocks` namespace: its dataset specs, schemas, factories, and fixtures.

The factories are checked without the network: each builds a query whose class, request, and
synchronous errors are asserted. The payload fixtures under `tests/data/namespaces/stocks/` are
the vendor's documented response examples, verbatim but for their `next_url`, which the fake
server would follow to Massive; each is served by the fake REST server, decoded through its
generated path, and must carry the keys the dataset declares, with no drift.

End to end, every REST dataset sends its exact request to the fake REST server and returns the
fixture's rows; every streamed channel's documented message, sent by the fake WebSocket server,
lands as one row of `blink()`; and every flat-file prefix reads one day's file from the fake S3
and keeps only the requested symbols.
"""

from __future__ import annotations

import inspect
import json
import logging
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only
from deephaven.time import to_j_local_date

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.dates import NEW_YORK
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import ChannelFamily
from deephaven_massive._massive import (
    RequestGovernor,
    RestClient,
    RestPath,
    Transport,
    WireType,
    source,
    to_batch,
)
from deephaven_massive._namespaces import stock_filings, stock_financials, stocks
from deephaven_massive._namespaces.query_building import Namespace
from deephaven_massive._query import (
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryQuery,
    StreamQuery,
    query_class,
)
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from deephaven.table import Table

    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws_session import WsSession

R = Transport.REST
FIXTURES = "namespaces/stocks"
DOCS = "docs/datasets/stocks."
# Whole New York days in epoch milliseconds: 2020-01-02 00:00 through 2020-01-03 23:59:59.999.
RANGE_PATH = "/v2/aggs/ticker/AAPL/range/1/day/1577941200000/1578113999999"
SNAPSHOTS_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers"

Fixture = Callable[[str, str], bytes]

# The datasets of the namespace, by name below `stocks`.
TREE_DATASETS = (*stocks.DATASETS, *stock_financials.DATASETS, *stock_filings.DATASETS)
"""Every dataset of the `m.stocks` tree, in catalog order."""

ALL_NAMES = [spec.name.removeprefix("stocks.") for spec in TREE_DATASETS]

# The REST list datasets: optional `symbols`, `dm.ALL` refused, the `…F` block.
LIST_WITH_SYMBOLS = [
    "dividends",
    "splits",
    "ipos",
    "short_interest",
    "short_volume",
    "share_float",
    "financials.balance_sheets",
    "financials.income_statements",
    "financials.cash_flow_statements",
    "financials.ratios",
    "filings.index",
    "filings.sections_10k",
    "filings.text_8k",
    "filings.disclosures_8k",
    "filings.risk_factors",
    "filings.initial_insider_ownership",
    "filings.insider_transactions",
    "filings.insider_activity",
    "filings.insider_holdings",
]
LIST_WITHOUT_SYMBOLS = [
    "filings.institutional_holdings",
    "filings.risk_factor_taxonomy",
    "filings.disclosure_taxonomy",
]
# Rows that each list several tickers: no `Symbol`, a `Tickers` column instead.
LIST_VALUED = [
    "financials.balance_sheets",
    "financials.income_statements",
    "financials.cash_flow_statements",
    "filings.disclosures_8k",
    "filings.initial_insider_ownership",
    "filings.insider_transactions",
    "filings.insider_activity",
    "filings.insider_holdings",
]
STREAMED = ["trades", "quotes", "bars", "luld", "imbalances", "fmv"]
# The one-row REST datasets, each beside the stream and history dataset of its kind.
ONE_ROW = {"last_trade": "trades", "last_quote": "quotes", "previous_day_bar": "bars"}

# Each date-axis dataset: its `start`/`end` wire pair and the `LocalDate` column it bounds.
AXES = {
    "dividends": ("ex_dividend_date", "ExDividendDate"),
    "splits": ("execution_date", "ExecutionDate"),
    "ipos": ("listing_date", "ListingDate"),
    "short_interest": ("settlement_date", "SettlementDate"),
    "short_volume": ("date", "Date"),
    "financials.balance_sheets": ("period_end", "PeriodEnd"),
    "financials.income_statements": ("period_end", "PeriodEnd"),
    "financials.cash_flow_statements": ("period_end", "PeriodEnd"),
    "filings.index": ("filing_date", "FilingDate"),
    "filings.sections_10k": ("filing_date", "FilingDate"),
    "filings.text_8k": ("filing_date", "FilingDate"),
    "filings.disclosures_8k": ("filing_date", "FilingDate"),
    "filings.risk_factors": ("filing_date", "FilingDate"),
    "filings.institutional_holdings": ("filing_date", "FilingDate"),
    "filings.initial_insider_ownership": ("filing_date", "FilingDate"),
    "filings.insider_transactions": ("filing_date", "FilingDate"),
    "filings.insider_activity": ("transaction_date", "TransactionDate"),
    "filings.insider_holdings": ("last_filing_date", "LastFilingDate"),
}

# Each REST source of the namespace, its fixture, and a value for each path placeholder.
# Keys a vendor example sends that its response schema does not document; the library drops
# them and reports them as drift, which the test pins so a refresh that documents them shows.
EXTRA_IN_EXAMPLE = {"/vX/reference/ipos": frozenset({"issue_start_date", "issue_end_date"})}

SOURCES: dict[str, tuple[str, dict[str, object]]] = {
    "/v2/last/trade/{stocksTicker}": ("last_trade.json", {"stocksTicker": "AAPL"}),
    "/v3/trades/{stockTicker}": ("trades.json", {"stockTicker": "AAPL"}),
    "/v2/last/nbbo/{stocksTicker}": ("last_quote.json", {"stocksTicker": "AAPL"}),
    "/v3/quotes/{stockTicker}": ("quotes.json", {"stockTicker": "AAPL"}),
    "/v2/aggs/ticker/{stocksTicker}/prev": ("bars_prev.json", {"stocksTicker": "AAPL"}),
    "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}": (
        "bars_range.json",
        {
            "stocksTicker": "AAPL",
            "multiplier": 1,
            "timespan": "day",
            "from": date(2023, 1, 9),
            "to": date(2023, 1, 9),
        },
    ),
    "/v2/aggs/grouped/locale/us/market/stocks/{date}": (
        "bars_grouped_daily.json",
        {"date": date(2023, 1, 9)},
    ),
    "/v2/snapshot/locale/us/markets/stocks/tickers": ("snapshots.json", {}),
    "/v2/snapshot/locale/us/markets/stocks/{direction}": ("movers.json", {"direction": "gainers"}),
    "/v1/open-close/{stocksTicker}/{date}": (
        "daily_open_close.json",
        {"stocksTicker": "AAPL", "date": date(2023, 1, 9)},
    ),
    "/stocks/v1/dividends": ("dividends.json", {}),
    "/stocks/v1/splits": ("splits.json", {}),
    "/vX/reference/ipos": ("ipos.json", {}),
    "/stocks/v1/short-interest": ("short_interest.json", {}),
    "/stocks/v1/short-volume": ("short_volume.json", {}),
    "/stocks/vX/float": ("share_float.json", {}),
    "/stocks/financials/v1/balance-sheets": ("financials_balance_sheets.json", {}),
    "/stocks/financials/v1/income-statements": ("financials_income_statements.json", {}),
    "/stocks/financials/v1/cash-flow-statements": ("financials_cash_flow_statements.json", {}),
    "/stocks/financials/v1/ratios": ("financials_ratios.json", {}),
    "/stocks/filings/vX/index": ("filings_index.json", {}),
    "/stocks/filings/10-K/vX/sections": ("filings_sections_10k.json", {}),
    "/stocks/filings/8-K/vX/text": ("filings_text_8k.json", {}),
    "/stocks/filings/8-K/vX/disclosures": ("filings_disclosures_8k.json", {}),
    "/stocks/filings/vX/risk-factors": ("filings_risk_factors.json", {}),
    "/stocks/filings/vX/13-F": ("filings_institutional_holdings.json", {}),
    "/stocks/filings/vX/form-3": ("filings_initial_insider_ownership.json", {}),
    "/stocks/filings/vX/form-4": ("filings_insider_transactions.json", {}),
    "/stocks/insiders/vX/activity": ("filings_insider_activity.json", {}),
    "/stocks/insiders/vX/holdings": ("filings_insider_holdings.json", {}),
    "/stocks/taxonomies/vX/risk-factors": ("filings_risk_factor_taxonomy.json", {}),
    "/stocks/taxonomies/vX/disclosures": ("filings_disclosure_taxonomy.json", {}),
}


def spec_of(name: str) -> DatasetSpec:
    return CATALOG.by_name[f"stocks.{name}"]


def factory(session: dm.MassiveSession, name: str) -> Callable[..., QueryBase]:
    target: object = session.stocks
    for part in name.split("."):
        target = getattr(target, part)
    return cast("Callable[..., QueryBase]", target)


def columns(name: str) -> dict[str, ColumnType]:
    schema = CATALOG.dataset_schema(spec_of(name))
    return {column.name: column.type for column in schema.columns}


# --- Namespaces ----------------------------------------------------------------------------------


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.stocks
    assert type(namespace) is stocks._Stocks
    assert isinstance(namespace, Namespace)
    assert type(namespace.financials) is stock_financials._StockFinancials
    assert type(namespace.filings) is stock_filings._StockFilings
    assert namespace.financials is namespace.financials
    label = massive_session._core.label
    assert repr(namespace.financials) == f"<{label} stocks.financials>"
    assert repr(namespace.filings) == f"<{label} stocks.filings>"


def test_every_dataset_is_in_the_process_catalog() -> None:
    assert all(CATALOG.by_name[spec.name] is spec for spec in TREE_DATASETS)
    assert sorted(ALL_NAMES) == sorted(
        [
            *STREAMED,
            *ONE_ROW,
            "snapshots",
            "movers",
            "daily_open_close",
            *LIST_WITH_SYMBOLS,
            *LIST_WITHOUT_SYMBOLS,
        ]
    )


def test_the_namespace_alone_passes_every_catalog_gate() -> None:
    assert Catalog(TREE_DATASETS).problems() == ()


@pytest.mark.parametrize("name", ALL_NAMES)
def test_every_dataset_docstring_names_its_own_page(
    massive_session: dm.MassiveSession, name: str
) -> None:
    doc = inspect.getdoc(factory(massive_session, name)) or ""
    assert f"{DOCS}{name}.md" in doc


# --- Schemas -------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", STREAMED)
def test_streamed_datasets_lead_with_symbol_date_and_their_instants(name: str) -> None:
    found = columns(name)
    times = ["TimestampStart", "TimestampEnd"] if name == "bars" else ["Timestamp"]
    assert list(found)[: 2 + len(times)] == ["Symbol", "Date", *times]
    assert list(found)[-1] == "TimestampInsert"
    assert found["Symbol"] is ColumnType.STRING
    assert found["Date"] is ColumnType.STRING
    assert all(found[time] is ColumnType.INSTANT for time in [*times, "TimestampInsert"])


@pytest.mark.parametrize(
    "name",
    [n for n in ALL_NAMES if n not in STREAMED and n not in LIST_VALUED and n not in ONE_ROW],
)
def test_other_datasets_have_no_timestamp(name: str) -> None:
    assert "Timestamp" not in columns(name)


@pytest.mark.parametrize(("name", "kind"), list(ONE_ROW.items()))
def test_one_row_datasets_share_the_columns_of_their_kind(name: str, kind: str) -> None:
    # The same kind gives the columns the two datasets share one type and one order, so the
    # tables merge on them; each has only the columns its own endpoints fill.
    assert spec_of(name).kind == spec_of(kind).kind
    mine, theirs = columns(name), columns(kind)
    shared = [(n, t) for n, t in mine.items() if n in theirs]
    assert shared == [(n, t) for n, t in theirs.items() if n in mine]
    assert len(shared) > 5
    assert type(spec_of(name).fetch_path) is str
    assert spec_of(kind).fetch_path is None


@pytest.mark.parametrize(
    "name",
    [
        "snapshots",
        "movers",
        "daily_open_close",
        *(n for n in LIST_WITH_SYMBOLS if n not in LIST_VALUED),
    ],
)
def test_single_ticker_rows_lead_with_symbol(name: str) -> None:
    found = columns(name)
    assert next(iter(found)) == "Symbol"
    assert "Ticker" not in found


@pytest.mark.parametrize("name", LIST_VALUED)
def test_rows_that_list_several_tickers_keep_tickers_and_have_no_symbol(name: str) -> None:
    found = columns(name)
    assert "Symbol" not in found
    assert found["Tickers"] is ColumnType.STRING_ARRAY
    assert not CATALOG.takes_symbols(spec_of(name))


@pytest.mark.parametrize("name", ALL_NAMES)
def test_no_dataset_has_an_underlying(name: str) -> None:
    assert "Underlying" not in columns(name)


@pytest.mark.parametrize(("name", "axis"), list(AXES.items()))
def test_date_axes_bound_a_date_column(name: str, axis: tuple[str, str]) -> None:
    wire, column = axis
    assert spec_of(name).time_axis == (f"{wire}.gte", f"{wire}.lte")
    assert columns(name)[column] is ColumnType.STRING  # every date is ISO text


@pytest.mark.parametrize(
    "name", [n for n in ALL_NAMES if n not in AXES and n not in STREAMED and n != "bars"]
)
def test_datasets_without_a_date_axis_have_no_start_or_end(
    massive_session: dm.MassiveSession, name: str
) -> None:
    assert spec_of(name).time_axis is None
    assert "start" not in inspect.signature(factory(massive_session, name)).parameters


def test_share_float_keeps_its_effective_date_as_a_column() -> None:
    assert columns("share_float")["EffectiveDate"] is ColumnType.STRING


@pytest.mark.parametrize("name", ALL_NAMES)
def test_poll_appends_filings_upserts_ipos_and_replaces_the_rest(
    name: str,
) -> None:
    expected = dm.PollMode.REPLACE
    if name == "ipos":
        expected = dm.PollMode.UPSERT  # an offering's record is revised in place
    elif name.startswith("filings.") and name not in REPLACE_KEYS:
        expected = dm.PollMode.APPEND
    assert spec_of(name).poll_mode is expected


def test_ipos_are_upserted_by_symbol_and_issuer() -> None:
    # Both are required on every row and stay fixed as an offering changes status.
    assert spec_of("ipos").poll_keys == ("Symbol", "IssuerName")
    fields = cast("RestPath", source(R, "/vX/reference/ipos"))
    assert fields.field("Ticker").required
    assert fields.field("IssuerName").required


@pytest.mark.parametrize("name", ALL_NAMES)
def test_only_the_current_state_datasets_declare_a_revision(name: str) -> None:
    # An explicit APPEND poll of a snapshot appends each new update of a symbol.
    revision: tuple[str, ...] = ("Updated",) if name in {"snapshots", "movers"} else ()
    if name in {"last_trade", "last_quote"}:
        revision = ("Timestamp",)  # each new trade or quote
    elif name == "previous_day_bar":
        revision = ("TimestampStart",)  # each new day
    assert spec_of(name).revision_keys == revision
    assert set(revision) <= set(columns(name))


CATEGORIES = ("PrimaryCategory", "SecondaryCategory", "TertiaryCategory")

# The key of each REST dataset that `poll()` replaces by default: what identifies one row.
REPLACE_KEYS = {
    "snapshots": ("Symbol",),
    "movers": ("Symbol",),
    "daily_open_close": ("Symbol",),
    "dividends": ("Id",),
    "splits": ("Id",),
    "short_interest": ("Symbol", "SettlementDate"),
    "short_volume": ("Symbol", "Date"),
    "share_float": ("Symbol",),
    "financials.balance_sheets": ("Cik", "PeriodEnd", "Timeframe"),
    "financials.income_statements": ("Cik", "PeriodEnd", "Timeframe"),
    "financials.cash_flow_statements": ("Cik", "PeriodEnd", "Timeframe"),
    "financials.ratios": ("Symbol", "Date"),
    # A later filing for the same day revises that day's totals.
    "filings.insider_activity": ("IssuerCik", "TransactionDate"),
    # A new filing by an insider updates that insider's current position.
    "filings.insider_holdings": ("IssuerCik", "OwnerCik"),
    "filings.risk_factor_taxonomy": ("Taxonomy", *CATEGORIES),
    "filings.disclosure_taxonomy": ("Taxonomy", *CATEGORIES),
}


@pytest.mark.parametrize(("name", "keys"), list(REPLACE_KEYS.items()))
def test_replaced_polls_key_one_row_each(name: str, keys: tuple[str, ...]) -> None:
    assert spec_of(name).poll_mode is dm.PollMode.REPLACE
    assert spec_of(name).poll_keys == keys
    assert set(keys) <= set(columns(name))


# --- Market data factories ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "cls"),
    [
        ("trades", StreamHistoryQuery),
        ("quotes", StreamHistoryQuery),
        ("luld", StreamQuery),
        ("imbalances", StreamQuery),
        ("fmv", StreamQuery),
    ],
)
def test_streams_take_symbols_and_all(
    massive_session: dm.MassiveSession, name: str, cls: type
) -> None:
    make = factory(massive_session, name)
    query = make(["MSFT", "AAPL"])
    assert type(query) is cls
    assert query._request.symbols == ("AAPL", "MSFT")
    assert make(dm.ALL)._request.symbols is dm.ALL
    with pytest.raises(ValueError, match="no symbols"):
        make([])


@pytest.mark.parametrize("name", STREAMED)
def test_each_stream_names_its_channel_family(name: str) -> None:
    expected = ChannelFamily.FMV if name == "fmv" else ChannelFamily.TICK
    assert spec_of(name).family is expected


def test_a_symbol_table_needs_a_symbol_column(massive_session: dm.MassiveSession) -> None:
    table = new_table([string_col("Ticker", ["AAPL"])])
    with pytest.raises(ValueError, match="Symbol column"):
        massive_session.stocks.trades(table)


def test_a_lower_case_ticker_names_the_upper_case_one(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"^'spy' is not a Massive stock ticker: .*use 'SPY'"):
        massive_session.stocks.trades("spy")


@pytest.mark.parametrize(
    "case",
    [
        (dm.Timespan.SECOND, 1, StreamHistoryQuery),
        (dm.Timespan.MINUTE, 1, StreamHistoryQuery),
        (dm.Timespan.DAY, 1, HistoryQuery),
        (dm.Timespan.MINUTE, 5, HistoryQuery),
        (dm.Timespan.HOUR, 1, HistoryQuery),
        (dm.Timespan.WEEK, 1, HistoryQuery),
    ],
)
def test_bars_pick_their_class_from_the_timespan(
    massive_session: dm.MassiveSession, case: tuple[dm.Timespan, int, type]
) -> None:
    timespan, multiplier, cls = case
    query = massive_session.stocks.bars("AAPL", timespan, multiplier=multiplier)
    assert type(query) is cls
    assert query._request.timespan is timespan
    assert query._request.multiplier == multiplier
    assert query._request.adjusted is True


def test_bars_default_to_streamed_adjusted_minutes(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.bars(dm.ALL, dm.Timespan.MINUTE)
    assert type(query) is StreamHistoryQuery
    assert query._request.timespan is dm.Timespan.MINUTE
    unadjusted = massive_session.stocks.bars("AAPL", dm.Timespan.DAY, adjusted=False)
    assert unadjusted._request.adjusted is False


def test_bars_reject_bad_arguments(massive_session: dm.MassiveSession) -> None:
    bars = cast("Any", massive_session.stocks.bars)
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bars("AAPL", dm.Timespan.SESSION)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        bars("AAPL", "minute")
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars("AAPL", dm.Timespan.MINUTE, adjusted="yes")
    # None would otherwise mean a path without `adjusted`.
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars("AAPL", dm.Timespan.MINUTE, adjusted=None)
    with pytest.raises(ValueError, match="multiplier"):
        bars("AAPL", dm.Timespan.MINUTE, multiplier=0)


def test_bars_declare_each_transport_by_timespan() -> None:
    assert stocks.BARS.channels == {dm.Timespan.SECOND: "A", dm.Timespan.MINUTE: "AM"}
    assert stocks.BARS.flat_file_prefixes == {
        dm.Timespan.MINUTE: "us_stocks_sip/minute_aggs_v1",
        dm.Timespan.DAY: "us_stocks_sip/day_aggs_v1",
    }
    assert stocks.BARS.grouped_daily_path == "/v2/aggs/grouped/locale/us/market/stocks/{date}"


def test_snapshots_take_all_and_the_otc_flag(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.snapshots(dm.ALL)
    assert type(query) is Query
    assert dict(query._request.params) == {"include_otc": False}
    otc = massive_session.stocks.snapshots(["AAPL"], include_otc=True)
    assert dict(otc._request.params) == {"include_otc": True}
    with pytest.raises(TypeError, match="include_otc must be a bool"):
        massive_session.stocks.snapshots("AAPL", include_otc=cast("Any", 1))


def test_movers_take_a_direction(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.movers(dm.Direction.LOSERS, include_otc=True)
    assert type(query) is Query
    assert query._request.symbols is None
    assert dict(query._request.params) == {"direction": dm.Direction.LOSERS, "include_otc": True}
    with pytest.raises(TypeError, match=r"direction must be dm\.Direction"):
        massive_session.stocks.movers(cast("Any", "gainers"))


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        ("2025-06-06", date(2025, 6, 6)),
        (date(2025, 6, 6), date(2025, 6, 6)),
        # The day values a Deephaven console hands its user: a Java LocalDate, a numpy day.
        (to_j_local_date("2025-06-06"), date(2025, 6, 6)),
        (np.datetime64("2025-06-06"), date(2025, 6, 6)),
        ("2025-6-6", date(2025, 6, 6)),  # Deephaven's own date text
    ],
)
def test_daily_open_close_names_a_new_york_day(
    massive_session: dm.MassiveSession, day: object, expected: date
) -> None:
    query = massive_session.stocks.daily_open_close("AAPL", cast("Any", day), adjusted=False)
    assert type(query) is Query
    assert dict(query._request.params) == {"date": expected}
    assert query._request.adjusted is False
    assert stocks.DAILY_OPEN_CLOSE.per_symbol


def test_daily_open_close_rejects_all_and_bad_days(massive_session: dm.MassiveSession) -> None:
    open_close = massive_session.stocks.daily_open_close
    with pytest.raises(ValueError, match=r"use m\.stocks\.bars\(dm\.ALL, dm\.Timespan\.DAY\)"):
        open_close(dm.ALL, "2025-06-06")
    with pytest.raises(ValueError, match="needs a timezone"):
        open_close("AAPL", datetime(2025, 6, 6, 9, 30))  # noqa: DTZ001 - a naive time on purpose
    with pytest.raises(ValueError, match=r"date=.* is a calendar date; pass a date"):
        open_close("AAPL", datetime(2025, 6, 7, 1, tzinfo=UTC))
    # Deephaven's own text reaches the day too, and a time there is no day either; the message
    # shows the text as passed.
    with pytest.raises(ValueError, match=r"date='2025-06-06T09:30 America/New_York' is a calendar"):
        open_close("AAPL", "2025-06-06T09:30 America/New_York")
    with pytest.raises(ValueError, match=r"names a day and a zone but no time"):
        open_close("AAPL", "2025-06-06 ET")
    with pytest.raises(ValueError, match=r"is neither ISO-8601 .* nor Deephaven date or time"):
        open_close("AAPL", "June 6")
    with pytest.raises(TypeError, match="date must be a str, date, datetime, Java Instant"):
        open_close("AAPL", cast("Any", 20250606))
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        open_close("AAPL", "2025-06-06", adjusted=cast("Any", None))


# --- REST list factories -----------------------------------------------------------------------


@pytest.mark.parametrize("name", LIST_WITH_SYMBOLS)
def test_list_datasets_filter_by_optional_symbols(
    massive_session: dm.MassiveSession, name: str
) -> None:
    make = factory(massive_session, name)
    assert type(make()) is Query
    assert make()._request.symbols is None
    assert make("AAPL")._request.symbols == ("AAPL",)
    with pytest.raises(ValueError, match="leave symbols out, which returns every ticker"):
        make(dm.ALL)


def a_sort_field(spec: DatasetSpec) -> str:
    # A `sort` every REST path of the dataset takes: one of the listed fields where the spec
    # lists them, else a field of the rows of the first path.
    paths = [cast("RestPath", CATALOG.source(Transport.REST, p)) for p in spec.rest_paths]
    sorts = [p for path in paths for p in path.params if p.name == "sort"]
    listed = [p.values for p in sorts if p.values]
    if listed:
        return listed[0][0]
    return paths[0].fields[0].wire_key


@pytest.mark.parametrize("name", [*LIST_WITH_SYMBOLS, *LIST_WITHOUT_SYMBOLS])
def test_list_datasets_take_the_list_block(massive_session: dm.MassiveSession, name: str) -> None:
    sort = a_sort_field(spec_of(name))
    query = factory(massive_session, name)(sort=sort, order=dm.Order.DESC, limit=25)
    assert type(query) is query_class(spec_of(name))
    assert dict(query._request.params) == {"order": dm.Order.DESC, "sort": sort}
    assert query._request.limit == 25
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        factory(massive_session, name)(order="asc")
    with pytest.raises(ValueError, match="has no filter 'nonsense'"):
        factory(massive_session, name)(filters={"nonsense": 1})


@pytest.mark.parametrize(("name", "axis"), list(AXES.items()))
def test_start_and_end_bound_the_date_axis(
    massive_session: dm.MassiveSession, name: str, axis: tuple[str, str]
) -> None:
    make = factory(massive_session, name)
    query = make(start="2024-01-01", end=date(2024, 3, 31))
    assert (query._request.start, query._request.end) == (date(2024, 1, 1), date(2024, 3, 31))
    with pytest.raises(ValueError, match="must not be after end"):
        make(start="2024-03-31", end="2024-01-01")
    with pytest.raises(ValueError, match="through start= and end="):
        make(filters={f"{axis[0]}_gte": "2024-01-01"})


@pytest.mark.parametrize(
    ("name", "reserved"),
    [("dividends", "ticker_any_of"), ("financials.balance_sheets", "tickers_any_of")],
)
def test_the_symbol_parameter_is_set_through_symbols(
    massive_session: dm.MassiveSession, name: str, reserved: str
) -> None:
    with pytest.raises(ValueError, match=f"{reserved} is set through symbols="):
        factory(massive_session, name)(filters={reserved: ["AAPL"]})


def test_filters_reach_the_request(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.dividends(filters={"distribution_type": "special"})
    assert dict(query._request.filters) == {"distribution_type": "special"}
    with pytest.raises(ValueError, match="takes"):
        massive_session.stocks.dividends(filters={"distribution_type": "monthly"})


# --- Fixtures ------------------------------------------------------------------------------------


def test_every_rest_source_has_a_fixture() -> None:
    used = {key for spec in TREE_DATASETS for t, key in spec.sources if t is R}
    assert used == set(SOURCES)


@pytest.mark.parametrize("path", list(SOURCES))
def test_fixtures_decode_with_their_declared_keys_and_no_drift(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes], path: str
) -> None:
    name, placeholders = SOURCES[path]
    body = json.loads(load_fixture(FIXTURES, name))
    rest_path = cast("RestPath", source(R, path))
    target = path.format(
        **{k: v.isoformat() if isinstance(v, date) else v for k, v in placeholders.items()}
    )
    fake_rest.reply(target, Reply(body=body))
    governor = RequestGovernor(("stocks",))
    client = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    try:
        page = client.get(rest_path, placeholders, {}, bucket="stocks")
    finally:
        client.close()
    spec = next(s for s in TREE_DATASETS if (R, path) in s.sources)
    required = [key.column for _, key in spec.keys_for(R, path).declared() if not key.from_request]
    batch, drift = to_batch(page.rows, rest_path, required=required)
    assert batch.num_rows >= 1
    assert (drift.extra, drift.missing) == (EXTRA_IN_EXAMPLE.get(path, frozenset()), frozenset())


# --- End to end: REST ----------------------------------------------------------------------------


def serve(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes], **paths: str
) -> None:
    # Each keyword is a fixture name (without `.json`), each value the path that serves it.
    for name, path in paths.items():
        fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, f"{name}.json"))))


def table_rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def new_york_midnight_ns(day: date) -> int:
    moment = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
    return int(moment.timestamp()) * 10**9


@pytest.mark.parametrize(
    ("name", "alternative"),
    [
        ("last_trade", r"m\.stocks\.snapshots\(dm\.ALL\)\.fetch\(\)"),
        ("last_quote", r"m\.stocks\.snapshots\(dm\.ALL\)\.fetch\(\)"),
        (
            "previous_day_bar",
            r"m\.stocks\.bars\(dm\.ALL, dm\.Timespan\.DAY\)\.history\(day, day\)",
        ),
    ],
)
def test_one_row_datasets_are_queries_that_refuse_all(
    massive_session: dm.MassiveSession, name: str, alternative: str
) -> None:
    make = factory(massive_session, name)
    query = make(["MSFT", "AAPL"])
    assert type(query) is Query
    assert query._request.symbols == ("AAPL", "MSFT")
    with pytest.raises(
        ValueError, match=rf"stocks\.{name} does not accept dm\.ALL; use {alternative}"
    ):
        make(dm.ALL)


def test_the_streams_and_daily_bars_have_no_fetch(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.stocks
    for query in (
        namespace.trades("AAPL"),
        namespace.quotes("AAPL"),
        namespace.bars("AAPL", dm.Timespan.DAY),
    ):
        assert not hasattr(query, "fetch")
        assert not hasattr(query, "poll")
    with pytest.raises(AttributeError, match=r"use m\.stocks\.last_trade\(\.\.\.\)\.fetch\(\)"):
        cast("Any", namespace.trades("AAPL")).fetch()


def test_previous_day_bar_is_one_adjusted_day_by_default(
    massive_session: dm.MassiveSession,
) -> None:
    query = massive_session.stocks.previous_day_bar("AAPL")
    assert query._request.timespan is dm.Timespan.DAY
    assert query._request.multiplier == 1
    assert query._request.adjusted is True
    assert (
        massive_session.stocks.previous_day_bar("AAPL", adjusted=False)._request.adjusted is False
    )
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        massive_session.stocks.previous_day_bar("AAPL", adjusted=cast("Any", 1))


def test_last_trade_fetch_reads_each_symbols_last_trade(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, last_trade="/v2/last/trade/AAPL")
    out = table_rows(rest_session.stocks.last_trade("AAPL").fetch())
    assert fake_rest.get_queries(one_value=True) == [("/v2/last/trade/AAPL", {})]
    assert [(r["Symbol"], r["Timestamp"], r["Price"], r["Size"]) for r in out] == [
        ("AAPL", 1617901342969834000, 129.8473, 25.0)
    ]
    assert out[0]["ParticipantTimestamp"] == 1617901342968000000


def test_last_quote_fetch_reads_each_symbols_last_nbbo(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, last_quote="/v2/last/nbbo/AAPL")
    out = table_rows(rest_session.stocks.last_quote(["AAPL"]).fetch())
    assert fake_rest.get_queries(one_value=True) == [("/v2/last/nbbo/AAPL", {})]
    assert [(r["Symbol"], r["Timestamp"], r["BidPrice"], r["AskPrice"]) for r in out] == [
        ("AAPL", 1617827221349730300, 127.96, 127.98)
    ]


@pytest.mark.parametrize(
    ("name", "path", "first"),
    [
        ("trades", "/v3/trades/AAPL", 1517562000016036600),
        ("quotes", "/v3/quotes/AAPL", 1517562000065700400),
    ],
)
def test_tick_history_over_rest_bounds_new_york_days(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
    path: str,
    first: int,
) -> None:
    serve(fake_rest, load_fixture, **{name: path})
    query = cast("StreamHistoryQuery", factory(rest_session, name)("AAPL"))
    out = table_rows(query.history("2018-02-02", "2018-02-02", source=dm.HistorySource.REST))
    assert fake_rest.get_queries(one_value=True) == [
        (
            path,
            {
                "timestamp.gte": str(new_york_midnight_ns(date(2018, 2, 2))),
                "timestamp.lt": str(new_york_midnight_ns(date(2018, 2, 3))),
                "sort": "timestamp",
                "order": "asc",  # oldest first, as the flat files are
                "limit": "50000",
            },
        )
    ]
    assert {r["Symbol"] for r in out} == {"AAPL"}
    assert out[0]["Timestamp"] == first


def test_previous_day_bar_fetches_the_previous_session(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, bars_prev="/v2/aggs/ticker/AAPL/prev")
    out = table_rows(rest_session.stocks.previous_day_bar("AAPL").fetch())
    assert fake_rest.get_queries(one_value=True) == [
        ("/v2/aggs/ticker/AAPL/prev", {"adjusted": "true"})
    ]
    # `/prev` stamps the bar at the 16:00 New York close (1605042000000); its window is its
    # whole New York day, as the range stamps it, and its `Date` that day.
    assert [
        (r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"], r["Close"], r["Vwap"])
        for r in out
    ] == [
        (
            "AAPL",
            "2020-11-10",
            new_york_midnight_ns(date(2020, 11, 10)),
            new_york_midnight_ns(date(2020, 11, 11)),
            115.97,
            116.3058,
        )
    ]
    assert out[0]["TimestampInsert"] is not None


def test_previous_day_bar_sends_adjusted_false_when_asked(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    body = json.loads(load_fixture(FIXTURES, "bars_prev.json"))
    fake_rest.reply("/v2/aggs/ticker/AAPL/prev", Reply(body={**body, "adjusted": False}))
    out = table_rows(rest_session.stocks.previous_day_bar("AAPL", adjusted=False).fetch())
    assert fake_rest.get_queries(one_value=True) == [
        ("/v2/aggs/ticker/AAPL/prev", {"adjusted": "false"})
    ]
    assert [r["Symbol"] for r in out] == ["AAPL"]


def test_previous_day_bar_refuses_a_bar_on_another_split_basis(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # The default asks for an adjusted bar; an as-traded answer must not pass for one.
    body = json.loads(load_fixture(FIXTURES, "bars_prev.json"))
    fake_rest.reply("/v2/aggs/ticker/AAPL/prev", Reply(body={**body, "adjusted": False}))
    with pytest.raises(
        dm.MassiveError,
        match=r"^stocks\.previous_day_bar: Massive answered .* with adjusted=False although "
        r"adjusted=True was requested",
    ):
        rest_session.stocks.previous_day_bar("AAPL").fetch()


def stamp_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the WARNINGs of bars stamped where Massive is not known to stamp one."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "is not a stamp Massive is known to use" in r.message
    ]


def test_previous_day_bar_serves_an_unexpected_stamp_with_a_warning(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    # 14:30 New York time is no stamp Massive is known to give a stock 1-day bar; the bar is
    # still served, labelled by the New York day it falls on.
    day = new_york_midnight_ns(date(2020, 11, 10))
    stamp = day // 10**6 + 14 * 3_600_000 + 1_800_000
    bar = {"T": "AAPL", "t": stamp, "c": 115.97}
    fake_rest.reply(
        "/v2/aggs/ticker/AAPL/prev", Reply({"status": "OK", "adjusted": True, "results": [bar]})
    )
    out = table_rows(rest_session.stocks.previous_day_bar("AAPL").fetch())
    assert [(r["Date"], r["TimestampStart"], r["Close"]) for r in out] == [
        ("2020-11-10", day, 115.97)
    ]
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        "stocks.previous_day_bar: a 1-day bar from /v2/aggs/ticker/{stocksTicker}/prev is "
        "stamped 2020-11-10T19:30:00+00:00"
    )


@pytest.mark.parametrize(
    ("name", "fixture", "path"),
    [
        ("last_trade", "last_trade", "/v2/last/trade/AAPL"),
        ("last_quote", "last_quote", "/v2/last/nbbo/AAPL"),
        ("previous_day_bar", "bars_prev", "/v2/aggs/ticker/AAPL/prev"),
    ],
)
def test_one_row_datasets_poll_one_row_per_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
    fixture: str,
    path: str,
) -> None:
    assert spec_of(name).poll_mode is dm.PollMode.REPLACE
    assert spec_of(name).poll_keys == ("Symbol",)
    serve(fake_rest, load_fixture, **{fixture: path})
    table = cast("Query", factory(rest_session, name)("AAPL")).poll(60)
    assert [row["Symbol"] for row in _rows_when(table, 1)] == ["AAPL"]
    rest_session.release(table)


def test_bars_history_over_rest_fills_the_range_path(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # The sample echoes `adjusted: true`; unadjusted bars come back saying so.
    body = json.loads(load_fixture(FIXTURES, "bars_range.json"))
    fake_rest.reply(RANGE_PATH, Reply(body={**body, "adjusted": False}))
    query = rest_session.stocks.bars("AAPL", dm.Timespan.DAY, adjusted=False)
    out = table_rows(query.history("2020-01-02", "2020-01-03", source=dm.HistorySource.REST))
    assert fake_rest.get_queries(one_value=True) == [
        (RANGE_PATH, {"adjusted": "false", "sort": "asc", "limit": "50000"})
    ]
    assert [(r["Symbol"], r["Date"], r["TimestampStart"], r["Close"]) for r in out] == [
        ("AAPL", "2020-01-02", 1577941200000 * 10**6, 75.0875),
        ("AAPL", "2020-01-03", 1578027600000 * 10**6, 74.3575),
    ]
    assert [r["TimestampEnd"] for r in out] == [
        new_york_midnight_ns(date(2020, 1, 3)),
        new_york_midnight_ns(date(2020, 1, 4)),
    ]


def test_a_page_on_another_adjustment_basis_fails_the_call(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # One history() never mixes split bases: Massive's echo of the basis must match the request.
    serve(fake_rest, load_fixture, bars_range=RANGE_PATH)  # the sample says adjusted: true
    query = rest_session.stocks.bars("AAPL", dm.Timespan.DAY, adjusted=False)
    with pytest.raises(
        dm.MassiveError,
        match=r"^stocks\.bars: Massive answered .* with adjusted=True although adjusted=False was "
        r"requested, so the call would mix split-adjusted and as-traded prices",
    ):
        query.history("2020-01-02", "2020-01-03", source=dm.HistorySource.REST)


def test_a_symbol_without_data_returns_no_rows_but_a_cut_short_one_fails(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # A symbol Massive has no data for answers HTTP 404 on its first request: no rows.
    missing = RANGE_PATH.replace("AAPL", "NOPE")
    fake_rest.reply(missing, Reply(status=404, body={"status": "NOT_FOUND"}))
    serve(fake_rest, load_fixture, bars_range=RANGE_PATH)
    query = rest_session.stocks.bars(["AAPL", "NOPE"], dm.Timespan.DAY)
    out = table_rows(query.history("2020-01-02", "2020-01-03", source=dm.HistorySource.REST))
    assert {r["Symbol"] for r in out} == {"AAPL"}
    # A 404 on a later page means the data was cut short: an error, never a short table.
    first = json.loads(load_fixture(FIXTURES, "bars_range.json"))
    later = f"{fake_rest.url}/v2/aggs/ticker/AAPL/next-page"
    fake_rest.reply_first(RANGE_PATH, Reply(body={**first, "next_url": later}))
    fake_rest.reply("/v2/aggs/ticker/AAPL/next-page", Reply(status=404, body={"status": "x"}))
    with pytest.raises(dm.MassiveError, match=r"^stocks\.bars: AAPL's data was cut short"):
        rest_session.stocks.bars("AAPL", dm.Timespan.DAY).history(
            "2020-01-02", "2020-01-03", source=dm.HistorySource.REST
        )


def test_all_daily_bars_read_the_grouped_daily_path(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/aggs/grouped/locale/us/market/stocks/2020-10-14"
    serve(fake_rest, load_fixture, bars_grouped_daily=path)
    query = rest_session.stocks.bars(dm.ALL, dm.Timespan.DAY)
    out = table_rows(query.history("2020-10-14", "2020-10-14", source=dm.HistorySource.REST))
    assert fake_rest.get_queries(one_value=True) == [(path, {"adjusted": "true"})]
    # Grouped daily stamps its bars at the 16:00 close (1602705600000), moved to the day's start.
    assert {(r["Date"], r["TimestampStart"]) for r in out} == {
        ("2020-10-14", new_york_midnight_ns(date(2020, 10, 14)))
    }
    assert len(out) == 3


SNAPSHOT_INSTANTS = (
    "Symbol",
    "Updated",
    "LastTradeSipTimestamp",
    "LastQuoteSipTimestamp",
    "LastMinuteWindowStart",
    "LastTradePrice",
)


def test_snapshots_split_a_long_watchlist_into_requests_of_250(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, snapshots=SNAPSHOTS_PATH)
    symbols = [f"T{n:03d}" for n in range(251)]
    rest_session.stocks.snapshots(symbols).fetch()
    sizes = sorted(len(q["tickers"].split(",")) for _, q in fake_rest.get_queries(one_value=True))
    assert sizes == [1, 250]


def test_snapshots_send_the_tickers_and_the_otc_flag(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, snapshots=SNAPSHOTS_PATH)
    out = table_rows(rest_session.stocks.snapshots(["MSFT", "AAPL"]).fetch())
    assert fake_rest.get_queries(one_value=True) == [
        (SNAPSHOTS_PATH, {"tickers": "AAPL,MSFT", "include_otc": "false"})
    ]
    # The documented example answers with its own tickers; its times mix ns and ms.
    assert {k: out[0][k] for k in SNAPSHOT_INSTANTS} == {
        "Symbol": "BCAT",
        "Updated": 1605192894630916600,
        "LastTradeSipTimestamp": 1605192894630916600,
        "LastQuoteSipTimestamp": 1605192959994246100,
        "LastMinuteWindowStart": 1684428600000 * 10**6,
        "LastTradePrice": 20.506,
    }
    assert "Timestamp" not in out[0]
    full = table_rows(rest_session.stocks.snapshots(dm.ALL, include_otc=True).fetch())
    assert fake_rest.get_queries(one_value=True)[1] == (SNAPSHOTS_PATH, {"include_otc": "true"})
    assert len(full) == len(out)


def test_movers_fill_the_direction(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/snapshot/locale/us/markets/stocks/losers"
    serve(fake_rest, load_fixture, movers=path)
    out = table_rows(rest_session.stocks.movers(dm.Direction.LOSERS).fetch())
    assert fake_rest.get_queries(one_value=True) == [(path, {"include_otc": "false"})]
    assert {k: out[0][k] for k in SNAPSHOT_INSTANTS} == {
        "Symbol": "PDS",
        "Updated": 1605195848258266000,
        "LastTradeSipTimestamp": 1605195848258266000,
        "LastQuoteSipTimestamp": 1605195929997325600,
        "LastMinuteWindowStart": 1684428600000 * 10**6,
        "LastTradePrice": 14.2284,
    }


def test_daily_open_close_requests_each_symbol_on_its_day(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, daily_open_close="/v1/open-close/AAPL/2023-01-09")
    query = rest_session.stocks.daily_open_close("AAPL", date(2023, 1, 9), adjusted=False)
    out = table_rows(query.fetch())
    assert fake_rest.get_queries(one_value=True) == [
        ("/v1/open-close/AAPL/2023-01-09", {"adjusted": "false"})
    ]
    assert [(r["Symbol"], r["Date"], r["Close"], r["PreMarket"]) for r in out] == [
        ("AAPL", "2023-01-09", 325.12, 324.5)
    ]
    # Its window is the New York day of its date.
    assert [(r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (new_york_midnight_ns(date(2023, 1, 9)), new_york_midnight_ns(date(2023, 1, 10)))
    ]


# Each REST list call: the dataset, its arguments, the path it reads, the exact query it sends
# (the page size as `limit`, never the row cap), and one column of the first row it returns.
LIST_CALLS: list[tuple[str, dict[str, Any], str, dict[str, str], tuple[str, object]]] = [
    (
        "dividends",
        {
            "symbols": ["AAPL"],
            "start": "2025-01-01",
            "end": "2025-12-31",
            "sort": "ex_dividend_date",
            "order": dm.Order.DESC,
        },
        "/stocks/v1/dividends",
        {
            "ticker.any_of": "AAPL",
            "ex_dividend_date.gte": "2025-01-01",
            "ex_dividend_date.lte": "2025-12-31",
            "sort": "ex_dividend_date.desc",
            "limit": "5000",
        },
        ("ExDividendDate", "2025-08-11"),
    ),
    (
        "splits",
        {"filters": {"adjustment_type": "forward_split"}},
        "/stocks/v1/splits",
        {"adjustment_type": "forward_split", "limit": "5000"},
        ("SplitTo", 2.0),
    ),
    (
        "ipos",
        {"symbols": "RAPP", "start": "2024-06-01"},
        "/vX/reference/ipos",
        {"ticker": "RAPP", "listing_date.gte": "2024-06-01", "limit": "1000"},
        ("ListingDate", "2024-06-07"),
    ),
    (
        "short_interest",
        {"symbols": "A", "filters": {"days_to_cover_gte": 1.5}},
        "/stocks/v1/short-interest",
        {"ticker.any_of": "A", "days_to_cover.gte": "1.5", "limit": "50000"},
        ("ShortInterest", 3906231),
    ),
    (
        "short_volume",
        {"start": "2025-03-25", "end": "2025-03-25"},
        "/stocks/v1/short-volume",
        {"date.gte": "2025-03-25", "date.lte": "2025-03-25", "limit": "50000"},
        ("ShortVolumeRatio", 31.57),
    ),
    (
        "share_float",
        {"symbols": ["AAPL"]},
        "/stocks/vX/float",
        {"ticker.any_of": "AAPL", "limit": "5000"},
        ("EffectiveDate", "2025-11-01"),
    ),
    (
        "financials.balance_sheets",
        {"symbols": "AAPL", "timeframe": dm.Timeframe.QUARTERLY, "start": "2025-01-01"},
        "/stocks/financials/v1/balance-sheets",
        {
            "tickers.any_of": "AAPL",
            "timeframe": "quarterly",
            "period_end.gte": "2025-01-01",
            "limit": "10000",
        },
        ("PeriodEnd", "2025-06-28"),
    ),
    (
        "financials.income_statements",
        {"timeframe": dm.Timeframe.TRAILING_TWELVE_MONTHS, "end": "2025-06-30"},
        "/stocks/financials/v1/income-statements",
        {"timeframe": "trailing_twelve_months", "period_end.lte": "2025-06-30", "limit": "10000"},
        ("PeriodEnd", "2025-06-28"),
    ),
    (
        "financials.cash_flow_statements",
        {"symbols": ["MSFT", "AAPL"]},
        "/stocks/financials/v1/cash-flow-statements",
        {"tickers.any_of": "AAPL,MSFT", "limit": "10000"},
        ("PeriodEnd", "2025-06-28"),
    ),
    (
        "financials.ratios",
        {"symbols": "AAPL", "sort": "market_cap"},
        "/stocks/financials/v1/ratios",
        {"ticker.any_of": "AAPL", "sort": "market_cap", "limit": "50000"},
        ("Date", "2024-09-19"),
    ),
    (
        "filings.index",
        {"form_type": "10-K", "end": "2025-12-31"},
        "/stocks/filings/vX/index",
        {"form_type": "10-K", "filing_date.lte": "2025-12-31", "limit": "10000"},
        ("Symbol", "AAPL"),
    ),
    (
        "filings.sections_10k",
        {"symbols": "AAPL", "filters": {"section": "risk_factors"}},
        "/stocks/filings/10-K/vX/sections",
        {"ticker.any_of": "AAPL", "section": "risk_factors", "limit": "100"},
        ("Section", "risk_factors"),
    ),
    (
        "filings.text_8k",
        {"start": "2025-01-14"},
        "/stocks/filings/8-K/vX/text",
        {"filing_date.gte": "2025-01-14", "limit": "100"},
        ("Symbol", "AXP"),
    ),
    (
        "filings.disclosures_8k",
        {"symbols": ["AAPL"]},
        "/stocks/filings/8-K/vX/disclosures",
        {"tickers.any_of": "AAPL", "limit": "1000"},
        ("Tickers", ["AAPL"]),
    ),
    (
        "filings.risk_factors",
        {"symbols": "MGLD"},
        "/stocks/filings/vX/risk-factors",
        {"ticker.any_of": "MGLD", "limit": "5000"},
        ("Symbol", "MGLD"),
    ),
    (
        "filings.institutional_holdings",
        {"filer_cik": "0001067983"},
        "/stocks/filings/vX/13-F",
        {"filer_cik": "0001067983", "limit": "1000"},
        ("FilerCik", "0001067983"),
    ),
    (
        "filings.initial_insider_ownership",
        {"symbols": ["NVDA", "GOOGL"], "issuer_cik": "0000320193"},
        "/stocks/filings/vX/form-3",
        {"tickers.any_of": "GOOGL,NVDA", "issuer_cik": "0000320193", "limit": "10000"},
        ("FilingDate", "2026-03-30"),
    ),
    (
        "filings.insider_transactions",
        {"owner_cik": "0001214156", "filters": {"transaction_code": "S"}},
        "/stocks/filings/vX/form-4",
        {"owner_cik": "0001214156", "transaction_code": "S", "limit": "10000"},
        ("FilingDate", "2026-03-30"),
    ),
    (
        "filings.insider_activity",
        {
            "symbols": "NVDA",
            "issuer_cik": ["0001045810", "0000320193"],
            "start": date(2026, 9, 1),
            "end": date(2026, 9, 30),
        },
        "/stocks/insiders/vX/activity",
        {
            "tickers.any_of": "NVDA",
            "issuer_cik.any_of": "0001045810,0000320193",
            "transaction_date.gte": "2026-09-01",
            "transaction_date.lte": "2026-09-30",
            "limit": "10000",
        },
        ("TransactionDate", "2026-09-17"),
    ),
    (
        "filings.insider_holdings",
        {
            "symbols": ["NVDA", "GOOGL"],
            "owner_cik": "0001588670",
            "start": date(2026, 1, 1),
            "sort": "shares_owned",
        },
        "/stocks/insiders/vX/holdings",
        {
            "tickers.any_of": "GOOGL,NVDA",
            "owner_cik": "0001588670",
            "last_filing_date.gte": "2026-01-01",
            "sort": "shares_owned",
            "limit": "10000",
        },
        ("OwnerCik", "0001588670"),
    ),
    (
        "filings.risk_factor_taxonomy",
        {},
        "/stocks/taxonomies/vX/risk-factors",
        {"limit": "1000"},
        ("Taxonomy", "1.0"),
    ),
    (
        "filings.disclosure_taxonomy",
        {},
        "/stocks/taxonomies/vX/disclosures",
        {"limit": "1000"},
        ("Taxonomy", "1.0"),
    ),
]


def test_every_list_dataset_has_an_end_to_end_call() -> None:
    assert sorted(name for name, *_ in LIST_CALLS) == sorted(
        [*LIST_WITH_SYMBOLS, *LIST_WITHOUT_SYMBOLS]
    )


@pytest.mark.parametrize("call", LIST_CALLS, ids=[name for name, *_ in LIST_CALLS])
def test_list_datasets_send_their_exact_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    call: tuple[str, dict[str, Any], str, dict[str, str], tuple[str, object]],
) -> None:
    name, kwargs, path, query, (column, value) = call
    fixture, _ = SOURCES[path]
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, fixture))))
    out = table_rows(cast("Query", factory(rest_session, name)(**kwargs)).fetch())
    assert fake_rest.get_queries(one_value=True) == [(path, query)]
    assert out[0][column] == value
    assert ("Symbol" in out[0]) == CATALOG.takes_symbols(spec_of(name))


def test_a_fetch_keeps_the_ex_dividend_days_of_its_range_both_ends_included(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # Rows outside the days asked for are dropped as Massive should have, and the inclusive
    # bounds keep the first and the last day: a calendar-date axis compares days, not instants.
    path = "/stocks/v1/dividends"
    body = json.loads(load_fixture(FIXTURES, "dividends.json"))
    days = ["2025-07-31", "2025-08-01", "2025-08-11", "2025-08-31", "2025-09-01"]
    first = body["results"][0]
    body["results"] = [first | {"ex_dividend_date": day, "id": f"E{day}"} for day in days]
    fake_rest.reply(path, Reply(body=body))
    query = rest_session.stocks.dividends("AAPL", start="2025-08-01", end="2025-08-31")
    out = table_rows(query.fetch())
    assert sorted(row["ExDividendDate"] for row in out) == days[1:4]


def test_limit_caps_the_rows_and_never_raises_the_page_size(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/stocks/filings/vX/index"
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, "filings_index.json"))))
    out = table_rows(rest_session.stocks.filings.index(limit=1).fetch())
    # A cap below the page size asks for no more rows than it keeps.
    assert fake_rest.get_queries(one_value=True) == [(path, {"limit": "1"})]
    assert [r["Symbol"] for r in out] == ["AAPL"]


def test_ipos_fan_out_one_request_per_ticker(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/vX/reference/ipos"
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, "ipos.json"))))
    table_rows(rest_session.stocks.ipos(["RAPP", "ACME"]).fetch())
    assert sorted(q["ticker"] for _, q in fake_rest.get_queries(one_value=True)) == ["ACME", "RAPP"]


@pytest.mark.parametrize(
    "name", [n for n in [*LIST_WITH_SYMBOLS, *LIST_WITHOUT_SYMBOLS] if n != "ipos"]
)
def test_order_needs_sort_where_the_path_folds_it_into_sort(
    massive_session: dm.MassiveSession, name: str
) -> None:
    with pytest.raises(ValueError, match="order needs sort"):
        factory(massive_session, name)(order=dm.Order.DESC)


def test_ipos_send_their_own_order_without_sort(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/vX/reference/ipos"
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, "ipos.json"))))
    rest_session.stocks.ipos(order=dm.Order.ASC).fetch()
    assert fake_rest.get_queries(one_value=True) == [(path, {"order": "asc", "limit": "1000"})]


# --- End to end: WebSocket -----------------------------------------------------------------------


def _rows_when(table: Table, count: int) -> list[dict[str, Any]]:
    # Bounded wait for an update-graph cycle to apply a published frame. It waits on
    # a consistent snapshot of the rows, never on `size`: `size` can show a row that the cycle
    # in progress is still adding, before a snapshot taken in that cycle contains it.
    pace = threading.Event()  # never set; only paces the loop
    for _ in range(500):
        rows = table_rows(table)
        if len(rows) == count:
            return rows
        pace.wait(0.02)
    rows = table_rows(table)
    assert len(rows) == count
    return rows


# Each streamed channel: the dataset, its call, the topic it subscribes, the vendor's documented
# message for the channel (FMV documents none; its frame follows the generated fields), and the
# values one row of the stream must carry, with `Timestamp` in epoch nanoseconds.
STREAM_CASES: list[
    tuple[str, Callable[[Any], StreamQuery], str, dict[str, object], dict[str, object]]
] = [
    (
        "trades",
        lambda s: s.trades("MSFT"),
        "T.MSFT",
        {
            "ev": "T",
            "sym": "MSFT",
            "x": 4,
            "i": "12345",
            "z": 3,
            "p": 114.125,
            "s": 100,
            "c": [0, 12],
            "t": 1536036818784,
            "pt": 1536036818763,
            "q": 3681328,
            "ds": "100",
        },
        {"Symbol": "MSFT", "Timestamp": 1536036818784 * 10**6, "Price": 114.125, "Size": 100.0},
    ),
    (
        "quotes",
        lambda s: s.quotes("MSFT"),
        "Q.MSFT",
        {
            "ev": "Q",
            "sym": "MSFT",
            "bx": 4,
            "bp": 114.125,
            "bs": 100,
            "ax": 7,
            "ap": 114.128,
            "as": 160,
            "c": 0,
            "i": [604],
            "t": 1536036818784,
            "pt": 1536036818783,
            "q": 50385480,
            "z": 3,
        },
        {
            "Symbol": "MSFT",
            "Timestamp": 1536036818784 * 10**6,
            "BidPrice": 114.125,
            "AskPrice": 114.128,
        },
    ),
    (
        "bars.second",
        lambda s: s.bars("MSFT", dm.Timespan.SECOND),
        "A.MSFT",
        {
            "ev": "A",
            "sym": "MSFT",
            "v": 200,
            "av": 8642007,
            "op": 25.66,
            "vw": 25.3981,
            "o": 25.39,
            "c": 25.39,
            "h": 25.39,
            "l": 25.39,
            "a": 25.3714,
            "z": 50,
            "s": 1610144868000,
            "e": 1610144869000,
        },
        {
            "Symbol": "MSFT",
            "Date": "2021-01-08",
            "TimestampStart": 1610144868000 * 10**6,
            "TimestampEnd": 1610144869000 * 10**6,
            "Close": 25.39,
            "Vwap": 25.3981,
        },
    ),
    (
        "bars.minute",
        lambda s: s.bars("MSFT", dm.Timespan.MINUTE),
        "AM.MSFT",
        {
            "ev": "AM",
            "sym": "MSFT",
            "v": 4110,
            "av": 9470157,
            "op": 0.4372,
            "vw": 0.4488,
            "o": 0.4488,
            "c": 0.4486,
            "h": 0.4489,
            "l": 0.4486,
            "a": 0.4352,
            "z": 685,
            "s": 1610144640000,
            "e": 1610144700000,
        },
        {
            "Symbol": "MSFT",
            "Date": "2021-01-08",  # 17:24 New York time
            "TimestampStart": 1610144640000 * 10**6,
            "TimestampEnd": 1610144700000 * 10**6,
            "Close": 0.4486,
            "Volume": 4110.0,
        },
    ),
    (
        "luld",
        lambda s: s.luld("MSFT"),
        "LULD.MSFT",
        {
            "ev": "LULD",
            "T": "MSFT",
            "h": 492.99,
            "l": 446.04,
            "i": [16],
            "z": 3,
            "t": 1764086430905642800,
            "q": 5925769,
        },
        {
            "Symbol": "MSFT",
            "Timestamp": 1764086430905642800,
            "HighPrice": 492.99,
            "LowPrice": 446.04,
        },
    ),
    (
        # Documented in milliseconds while the sample is nanoseconds: either is read.
        "luld.milliseconds",
        lambda s: s.luld("MSFT"),
        "LULD.MSFT",
        {"ev": "LULD", "T": "MSFT", "h": 492.99, "l": 446.04, "t": 1764086430905, "q": 1},
        {"Symbol": "MSFT", "Timestamp": 1764086430905 * 10**6, "HighPrice": 492.99},
    ),
    (
        "imbalances",
        lambda s: s.imbalances("MSFT"),
        "NOI.MSFT",
        {
            "ev": "NOI",
            "T": "MSFT",
            "t": 1601318039223013600,
            "at": 930,
            "a": "M",
            "i": 44,
            "x": 10,
            "o": 480,
            "p": 440,
            "b": 25.03,
        },
        {
            "Symbol": "MSFT",
            "Timestamp": 1601318039223013600,
            "ImbalanceQuantity": 480,
            "AuctionType": "M",
        },
    ),
    (
        "fmv",
        lambda s: s.fmv("MSFT"),
        "FMV.MSFT",
        {"ev": "FMV", "sym": "MSFT", "fmv": 189.22, "t": 1678220098130},
        {"Symbol": "MSFT", "Timestamp": 1678220098130 * 10**6, "Fmv": 189.22},
    ),
]


@pytest.mark.parametrize("case", STREAM_CASES, ids=[c[0] for c in STREAM_CASES])
def test_a_frame_lands_in_blink(
    *,
    ws_session: WsSession,
    case: tuple[str, Callable[[Any], StreamQuery], str, dict[str, object], dict[str, object]],
) -> None:
    _, make, topic, frame, expected = case
    result = make(ws_session.session.stocks).blink()
    seen = blink_to_append_only(result)
    ws_session.subscribed(topic)
    ws_session.publish(frame)
    (row,) = _rows_when(seen, 1)
    assert {k: row[k] for k in expected} == expected
    assert row["Date"] is not None
    assert row["TimestampInsert"] is not None  # when the frame arrived
    ws_session.session.release(result)


# --- End to end: flat files ----------------------------------------------------------------------


@pytest.fixture
def flat_session(fake_s3: FakeS3) -> Iterator[dm.MassiveSession]:
    # Test credentials the fake S3 accepts; passed as a mapping, as the flat-file tests do.
    credentials = {"s3_access_key_id": "AKIDFLATFILES", "s3_secret_access_key": "fake-secret"}
    m = dm.MassiveSession(
        dm.MassiveConfig(api_key="test-key", **cast("dict[str, Any]", credentials))
    )
    yield m
    m.close()


HOUR_NS = 3600 * 10**9


def test_a_daily_bar_has_one_stamp_whichever_source_serves_it(
    *, fake_rest: FakeRestServer, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> None:
    # The range stamps a day at its start; grouped daily, `/prev`, and the files at the close.
    day = date(2023, 3, 28)
    start = new_york_midnight_ns(day)
    close = start // 10**6 + 16 * 3_600_000
    credentials = {"s3_access_key_id": "AKIDFLATFILES", "s3_secret_access_key": "fake-secret"}
    config = dm.MassiveConfig(
        api_key="test-key", rest_base_url=fake_rest.url, **cast("dict[str, Any]", credentials)
    )
    after = new_york_midnight_ns(date(2023, 3, 29)) // 10**6 - 1
    fake_rest.reply(
        f"/v2/aggs/ticker/BCC/range/1/day/{start // 10**6}/{after}",
        Reply({"status": "OK", "results": [{"t": start // 10**6, "c": 61.99}]}),
    )
    bar = {"T": "BCC", "t": close, "c": 61.99}
    fake_rest.reply("/v2/aggs/ticker/BCC/prev", Reply({"status": "OK", "results": [bar]}))
    fake_rest.reply(
        "/v2/aggs/grouped/locale/us/market/stocks/2023-03-28",
        Reply({"status": "OK", "results": [bar]}),
    )
    fake_s3.put(
        "us_stocks_sip/day_aggs_v1/2023/03/2023-03-28.csv.gz",
        make_csv_gz(
            "ticker,volume,open,close,high,low,window_start,transactions\n"
            f"BCC,248274,61.68,61.99,62.565,61.41,{close * 10**6},4073\n"
        ),
    )
    with dm.MassiveSession(config) as m:
        bars = m.stocks.bars("BCC", dm.Timespan.DAY, adjusted=False)
        every = m.stocks.bars(dm.ALL, dm.Timespan.DAY, adjusted=False)
        tables = [
            bars.history(day, day, source=dm.HistorySource.REST),
            every.history(day, day, source=dm.HistorySource.REST),
            bars.history(day, day, source=dm.HistorySource.FLAT_FILES),
            m.stocks.previous_day_bar("BCC").fetch(),
        ]
        stamped = [
            [
                (r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"])
                for r in table_rows(t)
            ]
            for t in tables
        ]
    end = new_york_midnight_ns(date(2023, 3, 29))
    assert stamped == [[("BCC", "2023-03-28", start, end)]] * 4


def test_longer_bars_cover_their_whole_window_and_have_no_date(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # A week bar stamped on its Sunday (00:00 New York time, where Massive starts a week)
    # covers its Monday-to-Friday week; a month bar of June 2024 (whose 1st is a Saturday) its
    # weekdays, Monday 3rd to Friday 28th.
    sunday = new_york_midnight_ns(date(2025, 3, 2)) // 10**6
    week_end = new_york_midnight_ns(date(2025, 3, 8)) // 10**6 - 1
    fake_rest.reply(
        f"/v2/aggs/ticker/AAPL/range/1/week/{sunday}/{week_end}",
        Reply({"status": "OK", "adjusted": True, "results": [{"t": sunday, "c": 1.0}]}),
    )
    week = rest_session.stocks.bars("AAPL", dm.Timespan.WEEK)
    out = table_rows(week.history("2025-03-03", "2025-03-07", source=dm.HistorySource.REST))
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (None, new_york_midnight_ns(date(2025, 3, 3)), new_york_midnight_ns(date(2025, 3, 8)))
    ]
    first = new_york_midnight_ns(date(2024, 6, 1)) // 10**6
    month_end = new_york_midnight_ns(date(2024, 6, 29)) // 10**6 - 1
    fake_rest.reply(
        f"/v2/aggs/ticker/AAPL/range/1/month/{first}/{month_end}",
        Reply({"status": "OK", "adjusted": True, "results": [{"t": first, "c": 1.0}]}),
    )
    month = rest_session.stocks.bars("AAPL", dm.Timespan.MONTH)
    out = table_rows(month.history("2024-06-03", "2024-06-28", source=dm.HistorySource.REST))
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (None, new_york_midnight_ns(date(2024, 6, 3)), new_york_midnight_ns(date(2024, 6, 29)))
    ]


def test_bars_of_several_days_count_calendar_days_as_massive_does(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # Massive stamps 2-day stock bars on Friday, Sunday, and Tuesday: the Friday bar holds
    # Friday and Saturday, so Friday's trading alone, and the Sunday bar holds Monday's. Each
    # ends two calendar days after its stamp, so every bar of Friday to Wednesday is kept.
    days = [date(2026, 10, 16), date(2026, 10, 18), date(2026, 10, 20)]
    stamps = [new_york_midnight_ns(day) // 10**6 for day in days]
    last = new_york_midnight_ns(date(2026, 10, 22)) // 10**6 - 1
    results = [{"t": t, "c": 1.0, "v": v} for t, v in zip(stamps, [59.9, 34.6, 71.5], strict=True)]
    fake_rest.reply(
        f"/v2/aggs/ticker/AAPL/range/2/day/{stamps[0]}/{last}",
        Reply({"status": "OK", "adjusted": True, "results": results}),
    )
    query = rest_session.stocks.bars("AAPL", dm.Timespan.DAY, multiplier=2)
    out = table_rows(query.history("2026-10-16", "2026-10-21", source=dm.HistorySource.REST))
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        (None, new_york_midnight_ns(day), new_york_midnight_ns(day + timedelta(days=2)))
        for day in days
    ]


def test_a_day_file_moves_an_early_close_and_warns_on_an_unexpected_stamp(
    *,
    flat_session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    # The day after Thanksgiving closes at 13:00 New York time; that close means its day.
    day = new_york_midnight_ns(date(2024, 11, 29))
    key = "us_stocks_sip/day_aggs_v1/2024/11/2024-11-29.csv.gz"
    header = "ticker,volume,open,close,high,low,window_start,transactions\n"
    query = flat_session.stocks.bars("BCC", dm.Timespan.DAY, adjusted=False)
    fake_s3.put(
        key, make_csv_gz(f"{header}BCC,1,61.68,61.99,62.565,61.41,{day + 13 * HOUR_NS},1\n")
    )
    out = table_rows(query.history("2024-11-29", "2024-11-29", source=dm.HistorySource.FLAT_FILES))
    assert [(r["Symbol"], r["Date"], r["TimestampStart"]) for r in out] == [
        ("BCC", "2024-11-29", day)
    ]
    assert not stamp_warnings(caplog)
    # 14:30 is no stamp Massive is known to use: the bar may cover another day, but it is
    # served, labelled by the New York day it falls on, with one WARNING.
    afternoon = day + 29 * HOUR_NS // 2
    fake_s3.put(key, make_csv_gz(f"{header}BCC,1,61.68,61.99,62.565,61.41,{afternoon},1\n"))
    odd = table_rows(query.history("2024-11-29", "2024-11-29", source=dm.HistorySource.FLAT_FILES))
    assert [(r["Symbol"], r["Date"], r["TimestampStart"]) for r in odd] == [
        ("BCC", "2024-11-29", day)
    ]
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        "stocks.bars: a 1-day bar from the flat files us_stocks_sip/day_aggs_v1 is stamped "
        "2024-11-29T19:30:00+00:00 (2024-11-29T14:30:00-05:00 in America/New_York), which is "
        "not a stamp Massive is known to use for such a bar"
    )


# Each flat-file prefix: the call, the file's fixture (the vendor's documented example rows,
# with one AAPL row added to prove the symbol filter, and the minute bars' 17-digit sample
# times written as the nanoseconds the files carry), and the rows `history()` returns for one
# day, as (Symbol, Timestamp in epoch nanoseconds, one more column).
FLAT_CASES: list[tuple[str, Callable[[Any], HistoryQuery], str, str, list[tuple[object, ...]]]] = [
    (
        "trades_v1",
        lambda s: s.trades("MSFT"),
        "trades_v1_2023-03-28.csv",
        "Price",
        [("MSFT", 1679990400018381329, 276.16), ("MSFT", 1679990400034839796, 276.75)],
    ),
    (
        "quotes_v1",
        lambda s: s.quotes("MSFT"),
        "quotes_v1_2023-03-28.csv",
        "AskPrice",
        [("MSFT", 1679990400004468151, 0.0), ("MSFT", 1679990400018726518, 277.01)],
    ),
    (
        "minute_aggs_v1",
        lambda s: s.bars("MSFT", dm.Timespan.MINUTE, adjusted=False),
        "minute_aggs_v1_2023-03-28.csv",
        "Close",
        [("MSFT", 1679990400000000000, 275.52), ("MSFT", 1679990460000000000, 274.46)],
    ),
    (
        "day_aggs_v1",
        lambda s: s.bars(["BCC"], dm.Timespan.DAY, adjusted=False),
        "day_aggs_v1_2023-03-28.csv",
        "Close",
        # Stamped at the 16:00 New York close in the file, at the start of its day here.
        [("BCC", 1679976000000000000, 61.99)],
    ),
]


def test_every_flat_file_prefix_has_a_round_trip() -> None:
    prefixes = {
        prefix.removeprefix("us_stocks_sip/")
        for spec in TREE_DATASETS
        for prefix in spec.flat_file_prefixes.values()
    }
    assert prefixes == {name for name, *_ in FLAT_CASES}


@pytest.mark.parametrize("case", FLAT_CASES, ids=[c[0] for c in FLAT_CASES])
def test_a_flat_file_block_round_trips(
    *,
    flat_session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Fixture,
    make_csv_gz: Callable[[str], bytes],
    case: tuple[str, Callable[[Any], HistoryQuery], str, str, list[tuple[object, ...]]],
) -> None:
    prefix, make, fixture, column, expected = case
    key = f"us_stocks_sip/{prefix}/2023/03/2023-03-28.csv.gz"
    fake_s3.put(key, make_csv_gz(load_fixture(FIXTURES, fixture).decode()))
    query = make(flat_session.stocks)
    out = table_rows(query.history("2023-03-28", "2023-03-28", source=dm.HistorySource.FLAT_FILES))
    time = "TimestampStart" if "aggs" in prefix else "Timestamp"
    assert [(r["Symbol"], r[time], r[column]) for r in out] == expected
    assert {r["Date"] for r in out} == {"2023-03-28"}
    assert fake_s3.requests == [("flatfiles", key)]


# --- Poll keys identify rows -------------------------------------------------------------------


# The keyed REST datasets whose response is a list of rows, with the call and concrete path
# that serve them. Daily open/close answers one row per request, keyed by its symbol.
def _with_defaults(name: str) -> Callable[[dm.MassiveSession], QueryBase]:
    # A list dataset's call with every argument left at its default.
    def call(m: dm.MassiveSession) -> QueryBase:
        return factory(m, name)()

    return call


KEYED_CALLS: dict[str, tuple[Callable[[dm.MassiveSession], QueryBase], str]] = {
    "snapshots": (lambda m: m.stocks.snapshots(dm.ALL), SNAPSHOTS_PATH),
    "movers": (
        lambda m: m.stocks.movers(dm.Direction.GAINERS),
        "/v2/snapshot/locale/us/markets/stocks/gainers",
    ),
    **{
        name: (_with_defaults(name), cast("str", spec_of(name).fetch_path))
        for name in [*LIST_WITH_SYMBOLS, *LIST_WITHOUT_SYMBOLS]
    },
}


def _other(value: object, wire_type: WireType) -> object:
    # A wire value that differs from `value` and still converts to the field's type.
    if wire_type in {WireType.JSON_TEXT, WireType.STRING_ARRAY}:
        return [*cast("list[object]", value or []), "other"]
    if wire_type is WireType.DATE:
        base = date(2020, 1, 1) if value is None else date.fromisoformat(cast("str", value))
        return (base + timedelta(days=1)).isoformat()
    if wire_type is WireType.BOOL:
        return not value
    if wire_type in {WireType.LONG, WireType.DOUBLE}:
        return cast("float", value or 0) + 1
    return f"{value or ''}Z"


@pytest.mark.parametrize("name", list(KEYED_CALLS))
def test_poll_keys_identify_one_row_and_none_is_spare(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
) -> None:
    # The fixture's first row, then one copy per key column that differs only in that column:
    # the keys tell every row apart, and leaving out any one key column merges two rows.
    spec = spec_of(name)
    make, path = KEYED_CALLS[name]
    template = cast("str", spec.fetch_path)
    rest_path = cast("RestPath", source(R, template))
    body = json.loads(load_fixture(FIXTURES, SOURCES[template][0]))
    result_key = cast("str", rest_path.result_key)
    first = body[result_key][0]
    symbol = spec.keys_for(R, template).symbol
    rows = [first]
    for column in spec.poll_keys:
        field = rest_path.field(symbol.column if column == "Symbol" and symbol else column)
        rows.append({**first, field.wire_key: _other(first.get(field.wire_key), field.wire_type)})
    body[result_key] = rows
    fake_rest.reply(path, Reply(body=body))
    out = table_rows(cast("Query", make(rest_session)).fetch())
    # A list key (`OtherManagers`) is compared as a tuple, so a set can hold it.
    keyed = [
        tuple(tuple(v) if isinstance(v := row[k], list) else v for k in spec.poll_keys)
        for row in out
    ]
    assert len(set(keyed)) == len(rows)
    for index in range(len(spec.poll_keys)):
        fewer = {key[:index] + key[index + 1 :] for key in keyed}
        assert len(fewer) < len(rows), spec.poll_keys[index]


NULLABLE_KEYS = {
    "filings.institutional_holdings": {
        "PutCall",
        "OtherManagers",
        "SharesOrPrincipalAmount",
        "SharesOrPrincipalType",
        "VotingAuthoritySole",
        "VotingAuthorityShared",
        "VotingAuthorityNone",
    },
    "filings.initial_insider_ownership": {"NatureOfOwnership", "ExercisePrice", "ExerciseDate"},
    "filings.insider_transactions": {
        "NatureOfOwnership",
        "ExercisePrice",
        "ExerciseDate",
        "ExpirationDate",
        "TransactionDate",
        "TransactionCode",
        "TransactionShares",
        "TransactionPricePerShare",
        "TransactionAcquiredDisposed",
        "SharesOwnedFollowingTransaction",
    },
}


@pytest.mark.parametrize("name", ALL_NAMES)
def test_only_the_ownership_filings_have_nullable_poll_keys(name: str) -> None:
    nullable = spec_of(name).nullable_poll_keys
    assert set(nullable) == NULLABLE_KEYS.get(name, set())
    assert all(nullable.values())  # each with its reason


# --- Rows that share a ticker or an accession number -----------------------------------------


def test_two_issuers_of_one_ticker_poll_as_two_offerings(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    # A recycled ticker: an earlier issuer's withdrawn offering and a later issuer's listing.
    path = "/vX/reference/ipos"
    body = json.loads(load_fixture(FIXTURES, "ipos.json"))
    first = body["results"][0]
    earlier = first | {"issuer_name": "Rapport Acquisition Corp.", "ipo_status": "withdrawn"}
    body["results"] = [earlier, first]
    fake_rest.reply(path, Reply(body=body))
    table = rest_session.stocks.ipos().poll(60)
    rows = _rows_when(table, 2)
    assert sorted((row["Symbol"], row["IssuerName"], row["IpoStatus"]) for row in rows) == [
        ("RAPP", "Rapport Acquisition Corp.", "withdrawn"),
        ("RAPP", "Rapport Therapeutics Inc.", "history"),
    ]
    rest_session.release(table)


# --- Filings without a ticker --------------------------------------------------------------------

OPTIONAL_TICKER = [
    "filings.index",
    "filings.sections_10k",
    "filings.text_8k",
    "filings.risk_factors",
]


@pytest.mark.parametrize(
    "name", [n for n in ALL_NAMES if n not in OPTIONAL_TICKER and n not in STREAMED]
)
def test_every_other_symbol_key_is_required(name: str) -> None:
    spec = spec_of(name)
    symbols = [keys.symbol for keys in spec.keys.values() if keys.symbol is not None]
    assert all(key.required for key in symbols)


@pytest.mark.jvm_held  # its formula hands the session in its frame to the JVM
def test_the_split_factors_adjust_bars_as_traded(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # NVDA split 10 for 1, executed 2024-06-10. As traded, its 06-07 close is about 1208.88 and
    # its 06-10 close 121.79; the documented recipe restates 06-07 on today's basis.
    friday = new_york_midnight_ns(date(2024, 6, 7)) // 10**6
    monday = new_york_midnight_ns(date(2024, 6, 10)) // 10**6
    after = new_york_midnight_ns(date(2024, 6, 11)) // 10**6 - 1
    fake_rest.reply(
        f"/v2/aggs/ticker/NVDA/range/1/day/{friday}/{after}",
        Reply(
            {
                "status": "OK",
                "adjusted": False,
                "results": [
                    {"t": friday, "c": 1208.88, "v": 41_238_580.0},
                    {"t": monday, "c": 121.79, "v": 314_162_700.0},
                ],
            }
        ),
    )
    split = {
        "adjustment_type": "forward_split",
        "execution_date": "2024-06-10",
        "historical_adjustment_factor": 0.1,
        "id": "nvda-2024",
        "split_from": 1,
        "split_to": 10,
        "ticker": "NVDA",
    }
    fake_rest.reply("/stocks/v1/splits", Reply({"status": "OK", "results": [split]}))
    bars = rest_session.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
        "2024-06-07", "2024-06-10", source=dm.HistorySource.REST
    )
    splits = rest_session.stocks.splits("NVDA").fetch()
    adjusted = bars.raj(
        splits, on=["Symbol", "Date<ExecutionDate"], joins=["F=HistoricalAdjustmentFactor"]
    ).update(["F = isNull(F) ? 1.0 : F", "AdjClose = Close * F", "AdjVolume = Volume / F"])
    out = [
        (r["Date"], round(r["AdjClose"], 3), round(r["AdjVolume"])) for r in table_rows(adjusted)
    ]
    assert out == [("2024-06-07", 120.888, 412_385_800), ("2024-06-10", 121.79, 314_162_700)]
