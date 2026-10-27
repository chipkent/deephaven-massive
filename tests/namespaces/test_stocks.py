"""Tests for the `m.stocks` namespace: its dataset specs, schemas, factories, and fixtures.

The factories are checked without the network: each builds a query whose class, request, and
synchronous errors are asserted. The payload fixtures under `tests/data/namespaces/stocks/` are
the vendor's documented response examples; each is served by the fake REST server, decoded
through its generated path, and must carry the keys the dataset declares, with no drift.
"""

import inspect
import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._massive import RestClient, RestPath, Transport, source, to_batch
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

R = Transport.REST
FIXTURES = "namespaces/stocks"
DOCS = "docs/datasets/stocks.md#"

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
def test_poll_appends_for_ipos_and_filings_and_replaces_otherwise(name: str) -> None:
    appends = name == "ipos" or name.startswith("filings.")
    expected = dm.PollMode.APPEND if appends else dm.PollMode.REPLACE
    assert spec_of(name).poll_mode is expected


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


def test_fmv_streams_from_the_fmv_family() -> None:
    assert stocks.FMV.family is not stocks.TRADES.family


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
def test_the_symbol_parameter_is_no_filter(
    massive_session: dm.MassiveSession, name: str, reserved: str
) -> None:
    with pytest.raises(ValueError, match=f"has no filter '{reserved}'"):
        factory(massive_session, name)(filters={reserved: ["AAPL"]})


def test_filters_reach_the_request(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.dividends(filters={"distribution_type": "special"})
    assert dict(query._request.filters) == {"distribution_type": "special"}
    with pytest.raises(ValueError, match="takes"):
        massive_session.stocks.dividends(filters={"distribution_type": "monthly"})


def test_ipos_fan_out_one_ticker_per_request() -> None:
    assert stocks.IPOS.symbol_param == "ticker"
    assert stocks.IPOS.per_symbol


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
    trailing = make(timeframe=dm.Timeframe.TRAILING_TWELVE_MONTHS)
    assert dict(trailing._request.params) == {"timeframe": dm.Timeframe.TRAILING_TWELVE_MONTHS}
    with pytest.raises(TypeError, match=r"timeframe must be dm\.Timeframe"):
        make(timeframe="annual")


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
