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

import inspect
import json
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven import new_table
from deephaven.column import string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import NEW_YORK
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._enums import ChannelFamily, Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import (
    RestClient,
    RestPath,
    Source,
    Transport,
    WireType,
    WsClient,
    source,
    to_batch,
)
from deephaven_massive._namespaces import stocks
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply
from tests._fakes.s3 import FakeS3
from tests._fakes.ws import FakeMassiveServer

R = Transport.REST
FIXTURES = "namespaces/stocks"
DOCS = "docs/datasets/stocks.md#"
# Whole New York days in epoch milliseconds: 2020-01-02 00:00 through 2020-01-03 23:59:59.999.
RANGE_PATH = "/v2/aggs/ticker/AAPL/range/1/day/1577941200000/1578113999999"
SNAPSHOTS_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers"

Fixture = Callable[[str, str], bytes]

# The datasets of the namespace, by name below `stocks`.
ALL_NAMES = [spec.name.removeprefix("stocks.") for spec in stocks.DATASETS]

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
    "filings.insider_holdings",
    "filings.insider_transactions",
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
    "filings.insider_holdings",
    "filings.insider_transactions",
]
STREAMED = ["trades", "quotes", "bars", "luld", "imbalances", "fmv"]

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
    "filings.insider_holdings": ("filing_date", "FilingDate"),
    "filings.insider_transactions": ("filing_date", "FilingDate"),
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
    "/stocks/filings/vX/form-3": ("filings_insider_holdings.json", {}),
    "/stocks/filings/vX/form-4": ("filings_insider_transactions.json", {}),
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
    assert type(namespace.financials) is stocks._StockFinancials
    assert type(namespace.filings) is stocks._StockFilings
    assert namespace.financials is namespace.financials
    label = massive_session._core.label
    assert repr(namespace.financials) == f"<{label} stocks.financials>"
    assert repr(namespace.filings) == f"<{label} stocks.filings>"


def test_every_dataset_is_in_the_process_catalog() -> None:
    assert all(CATALOG.by_name[spec.name] is spec for spec in stocks.DATASETS)
    assert sorted(ALL_NAMES) == sorted(
        [
            *STREAMED,
            "snapshots",
            "movers",
            "daily_open_close",
            *LIST_WITH_SYMBOLS,
            *LIST_WITHOUT_SYMBOLS,
        ]
    )


def test_the_namespace_alone_passes_every_catalog_gate() -> None:
    assert Catalog(stocks.DATASETS).problems() == ()


@pytest.mark.parametrize("name", ALL_NAMES)
def test_every_dataset_docstring_links_its_column_reference(
    massive_session: dm.MassiveSession, name: str
) -> None:
    doc = inspect.getdoc(factory(massive_session, name)) or ""
    assert f"{DOCS}{name.replace('.', '-')}," in doc
    assert "limit" in doc
    assert "dm.ALL" in doc
    assert "columns()" in doc


# --- Schemas -------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", STREAMED)
def test_streamed_datasets_lead_with_symbol_and_timestamp(name: str) -> None:
    found = columns(name)
    assert list(found)[:2] == ["Symbol", "Timestamp"]
    assert found["Symbol"] is ColumnType.STRING
    assert found["Timestamp"] is ColumnType.INSTANT


@pytest.mark.parametrize(
    "name", [n for n in ALL_NAMES if n not in STREAMED and n not in LIST_VALUED]
)
def test_other_datasets_have_no_timestamp(name: str) -> None:
    assert "Timestamp" not in columns(name)


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
    assert found["Tickers"] is ColumnType.STRING
    assert not CATALOG.takes_symbols(spec_of(name))


def test_institutional_holdings_keep_the_filer_cik() -> None:
    found = columns("filings.institutional_holdings")
    assert "Symbol" not in found
    assert found["FilerCik"] is ColumnType.STRING
    declared = stocks.INSTITUTIONAL_HOLDINGS.keys_for(R, "/stocks/filings/vX/13-F")
    assert declared == SourceKeys(fanout=KeySpec("FilerCik", instrument=False))


@pytest.mark.parametrize("name", ALL_NAMES)
def test_no_dataset_has_an_underlying(name: str) -> None:
    assert "Underlying" not in columns(name)


@pytest.mark.parametrize(("name", "axis"), list(AXES.items()))
def test_date_axes_bound_a_local_date_column(name: str, axis: tuple[str, str]) -> None:
    wire, column = axis
    assert spec_of(name).time_axis == (f"{wire}.gte", f"{wire}.lte")
    assert columns(name)[column] is ColumnType.LOCAL_DATE


@pytest.mark.parametrize(
    "name", [n for n in ALL_NAMES if n not in AXES and n not in STREAMED and n != "bars"]
)
def test_datasets_without_a_date_axis_have_no_start_or_end(
    massive_session: dm.MassiveSession, name: str
) -> None:
    assert spec_of(name).time_axis is None
    assert "start" not in inspect.signature(factory(massive_session, name)).parameters


def test_share_float_keeps_its_effective_date_as_a_column() -> None:
    assert columns("share_float")["EffectiveDate"] is ColumnType.LOCAL_DATE


@pytest.mark.parametrize("name", ALL_NAMES)
def test_poll_appends_for_ipos_and_filings_and_replaces_taxonomies_and_the_rest(name: str) -> None:
    appends = name == "ipos" or (name.startswith("filings.") and "taxonomy" not in name)
    expected = dm.PollMode.APPEND if appends else dm.PollMode.REPLACE
    assert spec_of(name).poll_mode is expected


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
        ("trades", StreamHistoryLatestQuery),
        ("quotes", StreamHistoryLatestQuery),
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


@pytest.mark.parametrize(
    "case",
    [
        (dm.Timespan.SECOND, 1, StreamHistoryQuery),
        (dm.Timespan.MINUTE, 1, StreamHistoryQuery),
        (dm.Timespan.DAY, 1, HistoryLatestQuery),
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
    query = massive_session.stocks.bars(dm.ALL)
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
        bars("AAPL", adjusted="yes")
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        bars("AAPL", adjusted=None)  # None would otherwise mean a path without `adjusted`
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
        # 01:00 UTC on June 7 is still June 6 in New York.
        (datetime(2025, 6, 7, 1, tzinfo=UTC), date(2025, 6, 6)),
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
    with pytest.raises(TypeError, match="date must be a str, date, or datetime"):
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
    assert make("aapl")._request.symbols == ("AAPL",)
    with pytest.raises(ValueError, match="use symbols=None, which returns every ticker"):
        make(dm.ALL)


@pytest.mark.parametrize("name", [*LIST_WITH_SYMBOLS, *LIST_WITHOUT_SYMBOLS])
def test_list_datasets_take_the_list_block(massive_session: dm.MassiveSession, name: str) -> None:
    query = factory(massive_session, name)(sort="x", order=dm.Order.DESC, limit=25)
    assert type(query) is query_class(spec_of(name))
    assert dict(query._request.params) == {"order": dm.Order.DESC, "sort": "x"}
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
    with pytest.raises(ValueError, match="before"):
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


@pytest.mark.parametrize(
    "name",
    [
        "financials.balance_sheets",
        "financials.income_statements",
        "financials.cash_flow_statements",
    ],
)
def test_statements_take_a_timeframe(massive_session: dm.MassiveSession, name: str) -> None:
    make = factory(massive_session, name)
    query = make("AAPL", timeframe=dm.Timeframe.QUARTERLY)
    assert dict(query._request.params) == {"timeframe": dm.Timeframe.QUARTERLY}
    assert make()._request.params == ()
    with pytest.raises(TypeError, match=r"timeframe must be dm\.Timeframe"):
        make(timeframe="annual")


@pytest.mark.parametrize(
    "name", ["financials.income_statements", "financials.cash_flow_statements"]
)
def test_flow_statements_offer_the_trailing_twelve_months(
    massive_session: dm.MassiveSession, name: str
) -> None:
    trailing = factory(massive_session, name)(timeframe=dm.Timeframe.TRAILING_TWELVE_MONTHS)
    assert dict(trailing._request.params) == {"timeframe": dm.Timeframe.TRAILING_TWELVE_MONTHS}


def test_balance_sheets_refuse_the_trailing_twelve_months(
    massive_session: dm.MassiveSession,
) -> None:
    with pytest.raises(ValueError, match="balance sheets have no trailing twelve months"):
        massive_session.stocks.financials.balance_sheets(
            timeframe=dm.Timeframe.TRAILING_TWELVE_MONTHS
        )


def test_filings_index_takes_a_form_type(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.filings.index(form_type="10-K")
    assert dict(query._request.params) == {"form_type": "10-K"}
    with pytest.raises(TypeError, match="form_type must be a str"):
        massive_session.stocks.filings.index(form_type=cast("Any", 10))


def test_institutional_holdings_take_a_filer(massive_session: dm.MassiveSession) -> None:
    filings = massive_session.stocks.filings
    query = filings.institutional_holdings(filer_cik="0001067983")
    assert dict(query._request.params) == {"filer_cik": "0001067983"}
    assert query._request.symbols is None
    with pytest.raises(TypeError, match="filer_cik must be a str"):
        filings.institutional_holdings(filer_cik=cast("Any", 1067983))


@pytest.mark.parametrize("name", ["filings.insider_holdings", "filings.insider_transactions"])
def test_insider_forms_take_issuer_and_owner(massive_session: dm.MassiveSession, name: str) -> None:
    make = factory(massive_session, name)
    query = make("AAPL", issuer_cik="0000320193", owner_cik="0001214156")
    assert dict(query._request.params) == {"issuer_cik": "0000320193", "owner_cik": "0001214156"}
    with pytest.raises(TypeError, match="issuer_cik must be a str"):
        make(issuer_cik=320193)
    with pytest.raises(TypeError, match="owner_cik must be a str"):
        make(owner_cik=1214156)


# --- Fixtures ------------------------------------------------------------------------------------


def test_every_rest_source_has_a_fixture() -> None:
    used = {key for spec in stocks.DATASETS for t, key in spec.sources if t is R}
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
    client = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1)
    try:
        page = client.get(rest_path, placeholders, {})
    finally:
        client.close()
    spec = next(s for s in stocks.DATASETS if (R, path) in s.sources)
    required = [key.column for _, key in spec.keys_for(R, path).declared() if not key.from_request]
    batch, drift = to_batch(page.rows, rest_path, required=required)
    assert batch.num_rows >= 1
    assert (drift.extra, drift.missing) == (EXTRA_IN_EXAMPLE.get(path, frozenset()), frozenset())


# --- End to end: REST ----------------------------------------------------------------------------


@pytest.fixture
def rest_session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def serve(
    fake_rest: FakeRestServer, load_fixture: Callable[[str, str], bytes], **paths: str
) -> None:
    # Each keyword is a fixture name (without `.json`), each value the path that serves it.
    for name, path in paths.items():
        fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, f"{name}.json"))))


def sent(fake_rest: FakeRestServer) -> list[tuple[str, dict[str, str]]]:
    # Every GET the server received: its path and its query, one value per parameter.
    out = []
    for received in fake_rest.requests:
        parts = urlsplit(received.target)
        query = {k: v[0] for k, v in parse_qs(parts.query, keep_blank_values=True).items()}
        out.append((unquote(parts.path), query))
    return out


def table_rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


def new_york_midnight_ns(day: date) -> int:
    moment = datetime(day.year, day.month, day.day, tzinfo=NEW_YORK)
    return int(moment.timestamp()) * 10**9


def test_trades_fetch_reads_each_symbols_last_trade(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, last_trade="/v2/last/trade/AAPL")
    out = table_rows(rest_session.stocks.trades("aapl").fetch())
    assert sent(fake_rest) == [("/v2/last/trade/AAPL", {})]
    assert [(r["Symbol"], r["Timestamp"], r["Price"], r["Size"]) for r in out] == [
        ("AAPL", 1617901342969834000, 129.8473, 25.0)
    ]
    assert out[0]["ParticipantTimestamp"] == 1617901342968000000


def test_quotes_fetch_reads_each_symbols_last_nbbo(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, last_quote="/v2/last/nbbo/AAPL")
    out = table_rows(rest_session.stocks.quotes(["AAPL"]).fetch())
    assert sent(fake_rest) == [("/v2/last/nbbo/AAPL", {})]
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
    query = cast("StreamHistoryLatestQuery", factory(rest_session, name)("AAPL"))
    out = table_rows(query.history("2018-02-02", "2018-02-02", source=dm.HistorySource.REST))
    assert sent(fake_rest) == [
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


def test_daily_bars_fetch_the_previous_session(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, bars_prev="/v2/aggs/ticker/AAPL/prev")
    out = table_rows(rest_session.stocks.bars("AAPL", dm.Timespan.DAY).fetch())
    assert sent(fake_rest) == [("/v2/aggs/ticker/AAPL/prev", {"adjusted": "true"})]
    assert [(r["Symbol"], r["Timestamp"], r["Close"], r["Vwap"]) for r in out] == [
        ("AAPL", 1605042000000 * 10**6, 115.97, 116.3058)
    ]


def test_bars_history_over_rest_fills_the_range_path(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, bars_range=RANGE_PATH)
    query = rest_session.stocks.bars("AAPL", dm.Timespan.DAY, adjusted=False)
    out = table_rows(query.history("2020-01-02", "2020-01-03", source=dm.HistorySource.REST))
    assert sent(fake_rest) == [(RANGE_PATH, {"adjusted": "false", "sort": "asc", "limit": "50000"})]
    assert [(r["Symbol"], r["Timestamp"], r["Close"]) for r in out] == [
        ("AAPL", 1577941200000 * 10**6, 75.0875),
        ("AAPL", 1578027600000 * 10**6, 74.3575),
    ]


def test_all_daily_bars_read_the_grouped_daily_path(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/aggs/grouped/locale/us/market/stocks/2020-10-14"
    serve(fake_rest, load_fixture, bars_grouped_daily=path)
    query = rest_session.stocks.bars(dm.ALL, dm.Timespan.DAY)
    out = table_rows(query.history("2020-10-14", "2020-10-14", source=dm.HistorySource.REST))
    assert sent(fake_rest) == [(path, {"adjusted": "true"})]
    assert {r["Timestamp"] for r in out} == {1602705600000 * 10**6}
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
    sizes = sorted(len(q["tickers"].split(",")) for _, q in sent(fake_rest))
    assert sizes == [1, 250]


def test_snapshots_send_the_tickers_and_the_otc_flag(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    serve(fake_rest, load_fixture, snapshots=SNAPSHOTS_PATH)
    out = table_rows(rest_session.stocks.snapshots(["MSFT", "AAPL"]).fetch())
    assert sent(fake_rest) == [(SNAPSHOTS_PATH, {"tickers": "AAPL,MSFT", "include_otc": "false"})]
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
    assert sent(fake_rest)[1] == (SNAPSHOTS_PATH, {"include_otc": "true"})
    assert len(full) == len(out)


def test_movers_fill_the_direction(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/v2/snapshot/locale/us/markets/stocks/losers"
    serve(fake_rest, load_fixture, movers=path)
    out = table_rows(rest_session.stocks.movers(dm.Direction.LOSERS).fetch())
    assert sent(fake_rest) == [(path, {"include_otc": "false"})]
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
    assert sent(fake_rest) == [("/v1/open-close/AAPL/2023-01-09", {"adjusted": "false"})]
    assert [(r["Symbol"], r["From"], r["Close"], r["PreMarket"]) for r in out] == [
        ("AAPL", date(2023, 1, 9), 325.12, 324.5)
    ]


# Each REST list call: the dataset, its arguments, the path it reads, the exact query it sends
# (the page size as `limit`, never the row cap), and one column of the first row it returns.
LIST_CALLS: list[tuple[str, dict[str, Any], str, dict[str, str], tuple[str, object]]] = [
    (
        "dividends",
        {
            "symbols": ["AAPL"],
            "start": date(2025, 1, 1),
            "end": date(2025, 12, 31),
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
        ("ExDividendDate", date(2025, 8, 11)),
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
        {"symbols": "RAPP", "start": date(2024, 6, 1)},
        "/vX/reference/ipos",
        {"ticker": "RAPP", "listing_date.gte": "2024-06-01", "limit": "1000"},
        ("ListingDate", date(2024, 6, 7)),
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
        {"start": date(2025, 3, 25), "end": date(2025, 3, 25)},
        "/stocks/v1/short-volume",
        {"date.gte": "2025-03-25", "date.lte": "2025-03-25", "limit": "50000"},
        ("ShortVolumeRatio", 31.57),
    ),
    (
        "share_float",
        {"symbols": ["AAPL"]},
        "/stocks/vX/float",
        {"ticker.any_of": "AAPL", "limit": "5000"},
        ("EffectiveDate", date(2025, 11, 1)),
    ),
    (
        "financials.balance_sheets",
        {"symbols": "AAPL", "timeframe": dm.Timeframe.QUARTERLY, "start": date(2025, 1, 1)},
        "/stocks/financials/v1/balance-sheets",
        {
            "tickers.any_of": "AAPL",
            "timeframe": "quarterly",
            "period_end.gte": "2025-01-01",
            "limit": "50000",
        },
        ("PeriodEnd", date(2025, 6, 28)),
    ),
    (
        "financials.income_statements",
        {"timeframe": dm.Timeframe.TRAILING_TWELVE_MONTHS, "end": date(2025, 6, 30)},
        "/stocks/financials/v1/income-statements",
        {"timeframe": "trailing_twelve_months", "period_end.lte": "2025-06-30", "limit": "50000"},
        ("PeriodEnd", date(2025, 6, 28)),
    ),
    (
        "financials.cash_flow_statements",
        {"symbols": ["MSFT", "AAPL"]},
        "/stocks/financials/v1/cash-flow-statements",
        {"tickers.any_of": "AAPL,MSFT", "limit": "50000"},
        ("PeriodEnd", date(2025, 6, 28)),
    ),
    (
        "financials.ratios",
        {"symbols": "AAPL", "sort": "market_cap"},
        "/stocks/financials/v1/ratios",
        {"ticker.any_of": "AAPL", "sort": "market_cap", "limit": "50000"},
        ("Date", date(2024, 9, 19)),
    ),
    (
        "filings.index",
        {"form_type": "10-K", "end": date(2025, 12, 31)},
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
        {"start": date(2025, 1, 14)},
        "/stocks/filings/8-K/vX/text",
        {"filing_date.gte": "2025-01-14", "limit": "100"},
        ("Symbol", "AXP"),
    ),
    (
        "filings.disclosures_8k",
        {"symbols": ["AAPL"]},
        "/stocks/filings/8-K/vX/disclosures",
        {"tickers.any_of": "AAPL", "limit": "1000"},
        ("Tickers", '["AAPL"]'),
    ),
    (
        "filings.risk_factors",
        {"symbols": "MGLD"},
        "/stocks/filings/vX/risk-factors",
        {"ticker.any_of": "MGLD", "limit": "50000"},
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
        "filings.insider_holdings",
        {"issuer_cik": "0000320193"},
        "/stocks/filings/vX/form-3",
        {"issuer_cik": "0000320193", "limit": "10000"},
        ("FilingDate", date(2026, 3, 30)),
    ),
    (
        "filings.insider_transactions",
        {"owner_cik": "0001214156", "filters": {"transaction_code": "S"}},
        "/stocks/filings/vX/form-4",
        {"owner_cik": "0001214156", "transaction_code": "S", "limit": "10000"},
        ("FilingDate", date(2026, 3, 30)),
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
    assert sent(fake_rest) == [(path, query)]
    assert out[0][column] == value
    assert ("Symbol" in out[0]) == CATALOG.takes_symbols(spec_of(name))


def test_limit_caps_the_rows_and_never_raises_the_page_size(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/stocks/filings/vX/index"
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, "filings_index.json"))))
    out = table_rows(rest_session.stocks.filings.index(limit=1).fetch())
    # A cap below the page size asks for no more rows than it keeps.
    assert sent(fake_rest) == [(path, {"limit": "1"})]
    assert [r["Symbol"] for r in out] == ["AAPL"]


def test_ipos_fan_out_one_request_per_ticker(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/vX/reference/ipos"
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, "ipos.json"))))
    table_rows(rest_session.stocks.ipos(["RAPP", "ACME"]).fetch())
    assert sorted(q["ticker"] for _, q in sent(fake_rest)) == ["ACME", "RAPP"]


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
    assert sent(fake_rest) == [(path, {"order": "asc", "limit": "1000"})]


# --- End to end: WebSocket -----------------------------------------------------------------------


def _rows_when(table: Table, count: int) -> list[dict[str, Any]]:
    # Bounded wait for the flush thread and an update-graph cycle to apply a frame. It waits on
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


@pytest.fixture
def ws_session(fake_ws_server: FakeMassiveServer) -> Iterator[dm.MassiveSession]:
    # A session whose WebSocket connections reach the fake server instead of Massive.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", flush_interval=0.01))

    def client(
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[..., None],
        on_status: Callable[..., None],
        on_data: Callable[..., None],
        on_state: Callable[..., None],
    ) -> WsClient:
        return WsClient(
            fake_ws_server.url("/" + url.split("/", 3)[3]),
            api_key="test-key",
            sources=sources,
            on_open=on_open,
            on_status=on_status,
            on_data=on_data,
            on_state=on_state,
            reconnect_backoff=0.05,
            max_reconnect_backoff=0.1,
        )

    connections = ws.WsConnections(
        m._core,
        resolve_host=lambda *_: HostDecision("socket.test", Recency.REALTIME, "test"),
        record_verdict=lambda *_: None,
        client_factory=client,
    )
    cast("Any", m._core._ports)._streams = connections
    m._core.on_close(connections.close)
    yield m
    m.close()


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
        {"Symbol": "MSFT", "Timestamp": 1610144868000 * 10**6, "Close": 25.39, "Vwap": 25.3981},
    ),
    (
        "bars.minute",
        lambda s: s.bars("MSFT"),
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
        {"Symbol": "MSFT", "Timestamp": 1610144640000 * 10**6, "Close": 0.4486, "Volume": 4110.0},
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
    ws_session: dm.MassiveSession,
    fake_ws_server: FakeMassiveServer,
    case: tuple[str, Callable[[Any], StreamQuery], str, dict[str, object], dict[str, object]],
) -> None:
    _, make, topic, frame, expected = case
    result = make(ws_session.stocks).blink()
    seen = blink_to_append_only(result)
    fake_ws_server.wait_for(
        lambda s: any(
            json.loads(f)["action"] == "subscribe" and topic in json.loads(f)["params"].split(",")
            for f in (s.connections[-1].frames if s.connections else [])
        )
    )
    fake_ws_server.send(json.dumps([frame]))
    (row,) = _rows_when(seen, 1)
    assert {k: row[k] for k in expected} == expected
    ws_session.release(result)


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
        [("BCC", 1680033600000000000, 61.99)],
    ),
]


def test_every_flat_file_prefix_has_a_round_trip() -> None:
    prefixes = {
        prefix.removeprefix("us_stocks_sip/")
        for spec in stocks.DATASETS
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
    assert [(r["Symbol"], r["Timestamp"], r[column]) for r in out] == expected
    assert fake_s3.requests == [("flatfiles", key)]


def test_a_13f_row_without_its_filer_fails_the_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/stocks/filings/vX/13-F"
    body = json.loads(load_fixture(FIXTURES, "filings_institutional_holdings.json"))
    del body["results"][1]["filer_cik"]
    fake_rest.reply(path, Reply(body=body))
    with pytest.raises(dm.MassiveError, match="FilerCik"):
        rest_session.stocks.filings.institutional_holdings().fetch()


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
    if wire_type is WireType.JSON_TEXT:
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
    keyed = [tuple(row[k] for k in spec.poll_keys) for row in out]
    assert len(set(keyed)) == len(rows)
    for index in range(len(spec.poll_keys)):
        fewer = {key[:index] + key[index + 1 :] for key in keyed}
        assert len(fewer) < len(rows), spec.poll_keys[index]


# --- Filings without a ticker --------------------------------------------------------------------

OPTIONAL_TICKER = [
    "filings.index",
    "filings.sections_10k",
    "filings.text_8k",
    "filings.risk_factors",
]


@pytest.mark.parametrize("name", OPTIONAL_TICKER)
def test_a_filing_without_a_ticker_keeps_a_null_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
) -> None:
    # No filings schema marks `ticker` required, and the index documents it "if available".
    path = cast("str", spec_of(name).fetch_path)
    body = json.loads(load_fixture(FIXTURES, SOURCES[path][0]))
    first = body["results"][0]
    body["results"] = [first, {k: v for k, v in first.items() if k != "ticker"}]
    fake_rest.reply(path, Reply(body=body))
    out = table_rows(cast("Query", factory(rest_session, name)()).fetch())
    assert [row["Symbol"] for row in out] == [first["ticker"], None]
    key = spec_of(name).keys_for(R, path).symbol
    assert key is not None
    assert not key.required


@pytest.mark.parametrize(
    "name", [n for n in ALL_NAMES if n not in OPTIONAL_TICKER and n not in STREAMED]
)
def test_every_other_symbol_key_is_required(name: str) -> None:
    spec = spec_of(name)
    symbols = [keys.symbol for keys in spec.keys.values() if keys.symbol is not None]
    assert all(key.required for key in symbols)
