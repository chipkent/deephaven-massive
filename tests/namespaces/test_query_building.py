"""Tests for the shared dataset factory and the query class of every dataset.

Two rules over the factory methods live here too. The annotation rule: for every dataset of
the catalog, the return annotation of its factory method is the class `query_class` derives
from the spec (for `bars` that stream, `StreamHistoryQuery | HistoryQuery`, with `timespan`
required), and every public
factory method maps to exactly one dataset. The summary rule: every factory's docstring starts with
its `DatasetSpec.summary`. Each rule is a function over a session class and a catalog, so it
is checked against the real tree and against synthetic namespaces that break it.
"""

import collections.abc
import dataclasses
import datetime as dt
import importlib
import inspect
import logging
import re
import typing
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime
from typing import Any, ClassVar, Literal

import numpy as np
import pytest
from deephaven import new_table
from deephaven.column import int_col, string_col
from deephaven.time import to_j_instant, to_j_local_date

import deephaven_massive as dm
from deephaven_massive import _enums, queries
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, FuturesVenue, NotBars, Order, Timespan
from deephaven_massive._error_policy import DEFAULT_POLICY
from deephaven_massive._ingest.rest_requests import fetch_paths, plan_calls
from deephaven_massive._massive import Transport
from deephaven_massive._session import CATALOG
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    ADJUSTED_PRICE,
    ADJUSTED_VOLUME,
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    _check_bar_period,
    _time_value,
    accepted_values,
    adjustment_notes,
    build_query,
    list_params,
    one_or_any_of,
    optional_text,
    require_bool,
    value_table,
)
from deephaven_massive.queries import FetchQuery, HistoryQuery, Query, StreamHistoryQuery
from deephaven_massive.queries._choice import query_class


def whole(text: str) -> str:
    # A pattern that matches exactly `text`, so a test pins a message's whole text.
    return f"^{re.escape(text)}$"


FF = Transport.FLAT_FILES
SESSION_FILES = "us_futures_{venue}/session_aggs_v1"
STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.STOCKS,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
)
FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.FUTURES,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/futures/v1/aggs/{ticker}",
    flat_file_prefixes={Timespan.SESSION: SESSION_FILES},
    keys={
        (FF, SESSION_FILES): SourceKeys(
            KeySpec("Ticker"), KeySpec("WindowStart"), date=KeySpec("SessionEndDate")
        )
    },
)
TRADES_SPEC = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)
DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    fetch_path="/stocks/v1/dividends",
    keys={(Transport.REST, "/stocks/v1/dividends"): SourceKeys(KeySpec("Ticker"))},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
)
pd: Any = importlib.import_module("pandas")  # untyped: pandas ships no stubs

INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
)


def build(
    session: dm.MassiveSession,
    spec: DatasetSpec,
    *,
    symbols: dm.SymbolsLike | None = None,
    symbols_optional: bool = True,
    all_alternative: str | None = None,
    timespan: Timespan | None = None,
    venues: collections.abc.Set[FuturesVenue] | None = None,
    start: dm.DateLike | None = None,
    end: dm.DateLike | None = None,
    params: Mapping[str, object] | None = None,
    filters: Mapping[str, dm.FilterValue] | None = None,
    limit: int | None = None,
    on_error: dm.ErrorPolicy | None = None,
) -> Query:
    # Most tests build a list dataset without symbols, which only filter it; the refusal of
    # None for a dataset that needs symbols has its own test.
    return build_query(
        session._core,
        spec,
        query_type=Query,
        symbols=symbols,
        symbols_optional=symbols_optional,
        all_alternative=all_alternative,
        timespan=timespan,
        venues=venues,
        start=start,
        end=end,
        params=params,
        filters=filters,
        limit=limit,
        on_error=on_error,
    )


def test_list_params() -> None:
    assert list_params(sort=None, order=None) == {}
    assert list_params(sort="ticker", order=Order.DESC) == {"sort": "ticker", "order": Order.DESC}
    with pytest.raises(TypeError, match="sort must be a str"):
        list_params(sort=1, order=None)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        list_params(sort=None, order="desc")  # type: ignore[arg-type]  # the point: refused at run time
    # Whether `order` needs `sort` depends on the dataset's paths, which `build_query` reads.
    assert list_params(sort=None, order=Order.DESC) == {"order": Order.DESC}


def test_order_without_sort_follows_the_dataset_paths(massive_session: dm.MassiveSession) -> None:
    # The indicator path has its own `order` parameter; the dividends path only `sort`.
    assert build(massive_session, INDICATOR, params={"order": Order.DESC})._request.params == (
        ("order", Order.DESC),
    )
    needs_sort = (
        "stocks.dividends: order needs sort. Pass sort= with the field to order by, or leave "
        "order out."
    )
    with pytest.raises(ValueError, match=whole(needs_sort)):
        build(massive_session, DIVIDENDS, params={"order": Order.DESC, "sort": None})
    build(massive_session, DIVIDENDS, params={"order": Order.DESC, "sort": "pay_date"})


def test_limit_is_a_row_cap_not_a_wire_parameter(massive_session: dm.MassiveSession) -> None:
    # The total-row cap is kept apart from the wire parameters, whose `limit` is a page size.
    q = build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=5)
    assert q._request.limit == 5
    assert "limit" not in dict(q._request.params)
    assert "limit=5" in repr(massive_session.stocks.dividends("AAPL", limit=5))
    assert build(massive_session, DIVIDENDS, symbols=["AAPL"])._request.limit is None
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit="10")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=True)
    with pytest.raises(ValueError, match=whole("limit=0 is not positive.")):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=0)


def test_a_dataset_without_a_class_takes_any_class_by_prefix(
    massive_session: dm.MassiveSession,
) -> None:
    # Indicators, reference data, and the unified snapshot span asset classes.
    q = build(massive_session, INDICATOR, symbols=["X:BTCUSD", "I:SPX", "AAPL", "C:EURUSD"])
    assert q._request.symbols == ("AAPL", "C:EURUSD", "I:SPX", "X:BTCUSD")
    with pytest.raises(
        ValueError,
        match=r"^'i:spx' is an index ticker, not a stock one\. "
        r"Call dm\.canonical_symbol_index\('i:spx'\)\.$",
    ):
        build(massive_session, INDICATOR, symbols=["i:spx"])


def test_build_query_normalizes_the_request(massive_session: dm.MassiveSession) -> None:
    q = build(
        massive_session,
        DIVIDENDS,
        symbols=["MSFT", "AAPL"],
        start="2025-01-01",
        end="2025-06-30",
        params={"sort": "ex_dividend_date", "limit": None, "tickers": ["B", "A"]},
        filters={"frequency_lte": 12, "frequency": 4, "distribution_type": "recurring"},
    )
    assert isinstance(q, FetchQuery)
    request = q._request
    assert request.symbols == ("AAPL", "MSFT")
    assert request.start == date(2025, 1, 1)
    assert request.end == date(2025, 6, 30)
    assert request.params == (("sort", "ex_dividend_date"), ("tickers", ("B", "A")))
    assert request.filters == (
        ("distribution_type", "recurring"),
        ("frequency", 4),
        ("frequency_lte", 12),
    )
    assert hash(q)


def test_the_request_holds_the_call_s_policy_over_the_session_s() -> None:
    session_policy = dm.ErrorPolicy(dm.OnError.IGNORE, refused_venues=dm.OnError.LOG)
    m = dm.MassiveSession(dm.MassiveConfig(api_key="k", on_error=session_policy))
    try:
        plain = build(m, DIVIDENDS, symbols=["AAPL"])._request.on_error
        assert plain.malformed_rows is dm.OnError.IGNORE  # the session's
        assert plain.refused_venues is dm.OnError.LOG
        strict = dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE)
        called = build(m, DIVIDENDS, symbols=["AAPL"], on_error=strict)._request.on_error
        assert called.malformed_rows is dm.OnError.RAISE  # the call's
        assert called.unavailable_symbols is dm.OnError.IGNORE  # unset in the call: the session's
        q = m.stocks.dividends(["AAPL"], on_error=strict)
        assert q._request.on_error == called
        assert "on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE)" in repr(q)
    finally:
        m.close()


def test_a_request_built_without_a_session_policy_follows_the_defaults(
    massive_session: dm.MassiveSession,
) -> None:
    assert build(massive_session, DIVIDENDS, symbols=["AAPL"])._request.on_error == DEFAULT_POLICY


@pytest.mark.parametrize(
    "policy",
    [dm.OnError.RAISE, "raise", {"malformed_rows": "raise"}],
    ids=["member", "text", "map"],
)
def test_the_call_s_policy_must_be_an_error_policy(
    massive_session: dm.MassiveSession, policy: object
) -> None:
    if policy is dm.OnError.RAISE:
        refusal = (
            "on_error=dm.OnError.RAISE is not a dm.ErrorPolicy. To set every cause to RAISE, "
            "pass on_error=dm.ErrorPolicy(dm.OnError.RAISE)."
        )
        match = whole(refusal)
    else:
        match = r"^on_error must be dm\.ErrorPolicy"
    with pytest.raises(TypeError, match=match):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], on_error=policy)  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match=match):
        massive_session.stocks.trades("AAPL", on_error=policy)  # type: ignore[arg-type]  # the point: refused at run time


def test_build_query_without_symbols_or_axis(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, INDICATOR, timespan=Timespan.DAY)
    assert type(q) is FetchQuery  # not a bars dataset: the timespan does not pick the class
    assert q._request.symbols is None
    assert q._request.timespan is Timespan.DAY


def test_symbols_follow_the_asset_class(massive_session: dm.MassiveSession) -> None:
    fx = DatasetSpec(
        name="forex.quotes",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FOREX,
        channels={NotBars.ANY: "C"},
    )
    assert build(massive_session, fx, symbols="C:EURUSD")._request.symbols == ("C:EURUSD",)
    with pytest.raises(
        ValueError, match=r"^'EUR/USD' is not Massive's forex ticker\. Pass 'C:EURUSD'"
    ):
        build(massive_session, fx, symbols="EUR/USD")
    # A dataset without an asset class reads the class from each prefix; none is a stock.
    assert build(massive_session, INDICATOR, symbols="KIMpL")._request.symbols == ("KIMpL",)
    with pytest.raises(ValueError, match=r"tickers are case-sensitive"):
        build(massive_session, INDICATOR, symbols="aapl")


def test_none_is_refused_where_the_dataset_needs_symbols(
    massive_session: dm.MassiveSession, caplog: pytest.LogCaptureFixture
) -> None:
    with (
        caplog.at_level(logging.DEBUG, logger="deephaven_massive"),
        pytest.raises(ValueError, match="needs symbols"),
    ):
        build(massive_session, TRADES_SPEC, symbols_optional=False)
    assert caplog.messages == [
        "stocks.trades refused: symbols=None on a dataset that needs symbols"
    ]
    with pytest.raises(
        ValueError,
        match=whole(
            "stocks.trades needs symbols, and symbols is None. Pass a ticker, a list of tickers, "
            "a table with a Symbol column, or dm.ALL for every ticker."
        ),
    ):
        build(massive_session, TRADES_SPEC, symbols_optional=False)
    with pytest.raises(
        ValueError,
        match=whole(
            "stocks.trades needs symbols, and symbols is None. Pass a ticker, a list of tickers, "
            "or a table with a Symbol column."
        ),
    ):
        build(massive_session, TRADES_SPEC, symbols_optional=False, all_alternative="Use X.")
    # A dataset whose symbols only filter it takes None, and one without symbols takes none.
    assert build(massive_session, DIVIDENDS)._request.symbols is None
    with pytest.raises(ValueError, match=r"^stocks\.trades needs symbols"):
        massive_session.stocks.trades(None)  # type: ignore[arg-type]  # the refused value


def test_all_where_the_dataset_cannot_serve_it(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=whole("stocks.dividends does not take dm.ALL. Use X.")):
        build(massive_session, DIVIDENDS, symbols=dm.ALL, all_alternative="Use X.")
    assert build(massive_session, TRADES_SPEC, symbols=dm.ALL)._request.symbols is dm.ALL


def test_the_timespan_picks_the_class_of_a_bars_call(massive_session: dm.MassiveSession) -> None:
    # Only the bars Massive streams get the stream methods.
    q = build(massive_session, STOCK_BARS, symbols="AAPL", timespan=Timespan.MINUTE)
    assert type(q) is StreamHistoryQuery
    q = build(massive_session, FUTURES_BARS, symbols="ESZ5", timespan=Timespan.SESSION)
    assert type(q) is HistoryQuery


def test_venues_are_a_set_of_the_enum(massive_session: dm.MassiveSession) -> None:
    q = build(
        massive_session,
        FUTURES_BARS,
        symbols="ESZ5",
        timespan=Timespan.SESSION,
        venues={FuturesVenue.CME},
    )
    assert q._request.venues == (FuturesVenue.CME,)
    # Left out, every venue the plan allows, each refused one skipped with a warning.
    every = build(massive_session, FUTURES_BARS, symbols="ESZ5", timespan=Timespan.SESSION)._request
    assert (every.venues, every.refused_venues) == (tuple(FuturesVenue), dm.OnError.LOG)
    # A dataset whose flat files are not per venue has none.
    plain = build(massive_session, DIVIDENDS)._request
    assert (plain.venues, plain.refused_venues) == (None, None)
    with pytest.raises(TypeError, match=r"venues must be a set of dm\.FuturesVenue"):
        build(
            massive_session,
            FUTURES_BARS,
            symbols="ESZ5",
            timespan=Timespan.SESSION,
            venues="cme",  # type: ignore[arg-type]  # the point: refused at run time
        )
    with pytest.raises(TypeError, match=r"venues must hold only dm\.FuturesVenue members, not str"):
        build(
            massive_session,
            FUTURES_BARS,
            symbols="ESZ5",
            timespan=Timespan.SESSION,
            venues={"cme"},  # type: ignore[arg-type]  # the point: refused at run time
        )


def test_sort_is_checked_when_the_query_is_built(massive_session: dm.MassiveSession) -> None:
    m = massive_session
    assert dict(m.reference.tickers(sort="name")._request.params)["sort"] == "name"
    # A field the path does not list is sent: Massive answers it with HTTP 400.
    assert dict(m.reference.tickers(sort="nam")._request.params)["sort"] == "nam"
    statements = m.stocks.financials.balance_sheets
    assert dict(statements(sort="period_end.desc,fiscal_year")._request.params)["sort"] == (
        "period_end.desc,fiscal_year"
    )
    with pytest.raises(ValueError, match=r"sort part 'period_end\.desc' has its own direction"):
        statements(sort="period_end.desc", order=dm.Order.ASC)


def test_filter_validation(massive_session: dm.MassiveSession) -> None:
    # Every form of the axis, not only the start/end pair, is set through start= and end=.
    for key in ("ex_dividend_date_gte", "ex_dividend_date", "ex_dividend_date_gt"):
        refusal = (
            f"The filter {key!r} sets the time range of stocks.dividends. Pass start= and end= "
            "instead."
        )
        with pytest.raises(ValueError, match=whole(refusal)):
            build(massive_session, DIVIDENDS, filters={key: "2025-01-01"})
    with pytest.raises(TypeError, match="filters must be a mapping"):
        build(massive_session, DIVIDENDS, filters=[("a", 1)])  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(TypeError, match="filter names must be str"):
        build(massive_session, DIVIDENDS, filters={1: "a"})  # type: ignore[dict-item]  # the point: refused at run time
    with pytest.raises(
        TypeError,
        match=whole(
            "a must be a str, number, bool, date or time, or list of str or date, not dict"
        ),
    ):
        build(massive_session, DIVIDENDS, filters={"a": {"b": 1}})  # type: ignore[dict-item]  # the point: refused at run time
    with pytest.raises(TypeError, match=whole("a[1] must be a str or date, not int")):
        build(massive_session, DIVIDENDS, filters={"a": ["A", 1]})
    # A dataset without a time axis takes its timestamp filters through `filters`, and the
    # request keeps what the text means, never the text.
    assert build(
        massive_session, INDICATOR, filters={"timestamp_gte": "2025-01-02"}
    )._request.filters == (("timestamp_gte", date(2025, 1, 2)),)


def test_time_bounds_must_be_aware(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"^start='2025-01-01T09:30' has no timezone"):
        build(massive_session, DIVIDENDS, start="2025-01-01T09:30")


def test_naive_datetime_filters_and_params_are_rejected(
    massive_session: dm.MassiveSession,
) -> None:
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    message = (
        r"^timestamp_gte=datetime\.datetime\(2025, 1, 2, 9, 30\) has no timezone\. "
        r"Pass an aware datetime or a date\.$"
    )
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, filters={"timestamp_gte": naive})
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, params={"timestamp_gte": naive})
    aware = naive.replace(tzinfo=UTC)
    assert build(massive_session, INDICATOR, filters={"timestamp_gte": aware})._request.filters


def test_filters_take_the_date_and_time_objects_a_time_bound_takes(
    massive_session: dm.MassiveSession,
) -> None:
    # The indicator timestamps take whole milliseconds, as their ISO-8601 text does.
    instant = to_j_instant("2025-01-02T14:30:00.001Z")
    assert instant is not None
    filters = build(
        massive_session,
        INDICATOR,
        filters={
            "timestamp_gte": instant,
            "timestamp_lt": pd.Timestamp("2025-01-02T14:30:00.002Z"),
        },
    )._request.filters
    assert filters == (
        ("timestamp_gte", datetime(2025, 1, 2, 14, 30, 0, 1000, tzinfo=UTC)),
        ("timestamp_lt", datetime(2025, 1, 2, 14, 30, 0, 2000, tzinfo=UTC)),
    )
    assert all(type(value) is datetime for _, value in filters)
    java_day = to_j_local_date("2025-01-02")
    assert java_day is not None
    for day in (java_day, np.datetime64("2025-01-02")):
        assert build(
            massive_session,
            INDICATOR,
            filters={"timestamp_gte": day},
        )._request.filters == (("timestamp_gte", date(2025, 1, 2)),)
    # A Java day in a list is a day; the next element is the first the list refuses.
    with pytest.raises(TypeError, match=whole("a[1] must be a str or date, not int")):
        build(massive_session, DIVIDENDS, filters={"a": [java_day, 1]})
    # An instant between two microseconds rounds outward: an upper bound up, any other down,
    # whether the name is a filter key or a wire name.
    between = to_j_instant("2025-01-02T14:30:00.000001001Z")
    down = datetime(2025, 1, 2, 14, 30, 0, 1, tzinfo=UTC)
    up = datetime(2025, 1, 2, 14, 30, 0, 2, tzinfo=UTC)
    assert _time_value("timestamp_gte", between) == down
    assert _time_value("timestamp_gt", between) == down
    assert _time_value("timestamp_lt", between) == up
    assert _time_value("timestamp.lte", between) == up
    naive = pd.Timestamp("2025-01-02 09:30")
    with pytest.raises(ValueError, match=r"^timestamp_gte=.* has no timezone"):
        build(massive_session, INDICATOR, filters={"timestamp_gte": naive})


def test_accepted_values_read_the_generated_enum(massive_session: dm.MassiveSession) -> None:
    sma = CATALOG.by_name["indicators.sma"]
    spans = accepted_values(massive_session._core, sma, "timespan")
    assert "minute" in spans
    assert "second" not in spans
    conversion = CATALOG.by_name["forex.conversion"]
    assert accepted_values(massive_session._core, conversion, "precision") == tuple(
        str(digits) for digits in range(5)
    )


def test_every_snapshot_splits_its_tickers_alike() -> None:
    # The per-class and unified snapshots name one shared count of tickers per request.
    lists = [spec for spec in CATALOG.by_name.values() if spec.symbols_per_request is not None]
    assert {spec.name for spec in lists} >= {"snapshots", "stocks.snapshots", "crypto.snapshots"}
    assert {spec.symbols_per_request for spec in lists} == {SNAPSHOT_SYMBOLS_PER_REQUEST}


def test_filters_go_through_the_catalog_check(massive_session: dm.MassiveSession) -> None:
    build(massive_session, DIVIDENDS, filters={"frequency": 4})
    with pytest.raises(ValueError, match="has no filter 'pay_date_lte'"):
        build(massive_session, DIVIDENDS, filters={"pay_date_lte": "2025-12-31"})
    with pytest.raises(ValueError, match=r"filter 'frequency' takes integer values"):
        build(massive_session, DIVIDENDS, filters={"frequency": "4"})


def test_timespan_is_an_enum_on_every_dataset(massive_session: dm.MassiveSession) -> None:
    refusal = "timespan must be dm.Timespan, such as dm.Timespan.DAY, not 'day'"
    with pytest.raises(TypeError, match=whole(refusal)):
        build(massive_session, INDICATOR, timespan="day")  # type: ignore[arg-type]  # the point: refused at run time


NEWS = DatasetSpec(
    name="reference.news",
    summary="A dataset for the tests.",
    fetch_path="/v2/reference/news",
    keys={(Transport.REST, "/v2/reference/news"): SourceKeys()},
    time_axis=("published_utc.gte", "published_utc.lt"),
)


def test_a_time_axis_must_not_end_before_it_starts(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"^start=.* is after end="):
        build(massive_session, DIVIDENDS, start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError, match="is after end="):
        build(
            massive_session,
            NEWS,
            start=datetime(2025, 1, 2, 10, tzinfo=UTC),
            end=datetime(2025, 1, 2, 9, tzinfo=UTC),
        )
    # One whole day, an open-ended axis, and two equal instants (no rows) are all valid.
    assert build(massive_session, DIVIDENDS, start="2025-01-02", end="2025-01-02")._request.end
    assert build(massive_session, DIVIDENDS, start="2025-01-02")._request.end is None
    moment = datetime(2025, 1, 2, 9, tzinfo=UTC)
    assert build(massive_session, NEWS, start=moment, end=moment)._request.start == moment


def test_a_time_axis_refuses_mixed_bounds(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(
        ValueError,
        match=r"is a datetime\. Pass two dates for whole days, or two datetimes for exact "
        r"instants\.$",
    ):
        build(massive_session, NEWS, start="2025-01-02", end=datetime(2025, 1, 2, 9, tzinfo=UTC))
    with pytest.raises(
        ValueError,
        match=r"is a date\. Pass two dates for whole days, or two datetimes for exact instants\.$",
    ):
        build(massive_session, NEWS, start=datetime(2025, 1, 2, 9, tzinfo=UTC), end="2025-01-03")


def test_a_calendar_date_axis_takes_dates_only(massive_session: dm.MassiveSession) -> None:
    moment = datetime(2025, 1, 2, 9, tzinfo=UTC)
    message = whole(
        "stocks.dividends: ex_dividend_date is a calendar date, so start and end take dates, not "
        "times. Pass dates, such as start='2025-01-02'."
    )
    with pytest.raises(ValueError, match=message):
        build(massive_session, DIVIDENDS, start=moment)
    with pytest.raises(ValueError, match=message):
        build(massive_session, DIVIDENDS, end=moment)
    with pytest.raises(ValueError, match=message):
        build(massive_session, DIVIDENDS, start=moment, end=moment)
    # An instant axis takes either kind, and so does a dataset without a declared axis.
    assert build(massive_session, INDICATOR, start=moment)._request.start == moment
    assert build(massive_session, NEWS, start=moment)._request.start == moment
    assert build(massive_session, NEWS, start="2025-01-02")._request.start == date(2025, 1, 2)
    # The advice of the refusal: a date is taken.
    assert build(massive_session, DIVIDENDS, start="2025-01-02")._request.start == date(2025, 1, 2)


def test_adjusted_is_a_typed_bool(massive_session: dm.MassiveSession) -> None:
    spec = STOCK_BARS
    q = build_query(
        massive_session._core,
        spec,
        query_type=Query,
        symbols=["AAPL"],
        timespan=Timespan.DAY,
        adjusted=False,
    )
    assert q._request.adjusted is False
    assert (
        build_query(
            massive_session._core, spec, query_type=Query, symbols=["AAPL"], timespan=Timespan.DAY
        )._request.adjusted
        is None
    )
    with pytest.raises(TypeError, match="adjusted must be a bool, not str"):
        build_query(
            massive_session._core,
            spec,
            query_type=Query,
            symbols=["AAPL"],
            timespan=Timespan.DAY,
            adjusted="false",  # type: ignore[arg-type]  # the point: refused at run time
        )
    # One channel only: adjusted never travels in params, where a transport could drop it.
    with pytest.raises(
        ValueError,
        match=whole(
            "stocks.bars passes adjusted in params, not as build_query(adjusted=). This is a bug "
            "in deephaven-massive. Turn on dm.enable_debug_logging(), reproduce it, and report it "
            "with the log and m.streams() at https://github.com/chipkent/deephaven-massive/issues."
        ),
    ):
        build_query(
            massive_session._core,
            spec,
            query_type=Query,
            symbols=["AAPL"],
            timespan=Timespan.DAY,
            params={"adjusted": False},
        )


def test_adjusted_is_refused_in_filters(massive_session: dm.MassiveSession) -> None:
    # A filter would reach REST but not the source choice, which could then pick flat files,
    # whose bars are unadjusted.
    with pytest.raises(
        ValueError,
        match=whole("The filter 'adjusted' has its own argument. Pass adjusted= instead."),
    ):
        build_query(
            massive_session._core,
            STOCK_BARS,
            query_type=Query,
            symbols=["AAPL"],
            timespan=Timespan.DAY,
            filters={"adjusted": True},
        )


def test_a_symbol_table_is_boxed_by_identity(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    table = new_table([string_col("Symbol", ["AAPL"])])
    q = build(massive_session, TRADES_SPEC, symbols=table)
    boxed = q._request.symbols
    assert isinstance(boxed, SymbolTable)
    assert boxed.table is table
    assert build(massive_session, TRADES_SPEC, symbols=table) == q


def test_a_symbol_table_needs_a_symbol_column(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    table = new_table([string_col("Ticker", ["AAPL"]), string_col("Name", ["Apple"])])
    with pytest.raises(
        ValueError,
        match=whole(
            "The symbol table has no Symbol column. It has Ticker and Name. Name the column of "
            'tickers Symbol, as in t.rename_columns("Symbol = Ticker").'
        ),
    ):
        build(massive_session, TRADES_SPEC, symbols=table)
    # The advice: the renamed column is taken.
    renamed = table.rename_columns("Symbol = Ticker")
    assert build(massive_session, TRADES_SPEC, symbols=renamed)._request.symbols is not None


def test_a_symbol_table_without_text_is_told_to_add_the_column(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    # No column a rename could turn into Symbol, so the message names none.
    table = new_table([int_col("Id", [1])])
    with pytest.raises(
        ValueError,
        match=whole(
            "The symbol table has no Symbol column. It has Id. Pass a table with a Symbol column "
            "that holds the tickers as Strings."
        ),
    ):
        build(massive_session, TRADES_SPEC, symbols=table)
    # The advice: a table with such a column is taken.
    fixed = new_table([int_col("Id", [1]), string_col("Symbol", ["AAPL"])])
    assert build(massive_session, TRADES_SPEC, symbols=fixed)._request.symbols is not None


def test_a_column_of_tickers_or_codes_must_hold_text(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    # A column of numbers would otherwise fail inside the verb, naming no column.
    numbers = new_table([int_col("Symbol", [1]), int_col("ProductCode", [2])])
    with pytest.raises(
        ValueError,
        match=whole(
            "The Symbol column of the symbol table holds int, not text. Pass a table whose Symbol "
            "column holds each value as a String, such as 'AAPL'."
        ),
    ):
        build(massive_session, TRADES_SPEC, symbols=numbers)
    with pytest.raises(
        ValueError,
        match=whole(
            "The ProductCode column of the table passed as product_code holds int, not text. Pass "
            "a table whose ProductCode column holds each value as a String, such as 'ES'."
        ),
    ):
        value_table(numbers, argument="product_code", parameter="product_code", underlying=False)
    # A pandas Series of tickers is a sequence of str, so it reads as a list does.
    texts = new_table([string_col("Symbol", ["AAPL"])])
    assert build(massive_session, TRADES_SPEC, symbols=pd.Series(["AAPL"])) == build(
        massive_session, TRADES_SPEC, symbols=["AAPL"]
    )
    assert value_table(texts, argument="underlying", parameter="u", underlying=True).table is texts


# The query class of a dataset.)

_FETCH = {"fetch_path": "/v2/last/trade/{stocksTicker}"}
_HISTORY = {"history_path": "/v3/trades/{stockTicker}"}
_CHANNELS = {"channels": {NotBars.ANY: "T"}}


@pytest.mark.parametrize(
    "changes",
    [{**_CHANNELS, **_FETCH}, {**_HISTORY, **_FETCH}, {**_CHANNELS, **_HISTORY, **_FETCH}],
    ids=["stream", "history", "both"],
)
def test_a_fetch_path_is_a_dataset_of_its_own(changes: dict[str, Any]) -> None:
    # A one-row endpoint is never a verb of a stream or history dataset.
    with pytest.raises(ValueError, match=r"^stocks\.trades: a fetch path is a dataset of its own"):
        DatasetSpec(
            name="stocks.trades",
            summary="A dataset for the tests.",
            asset_class=AssetClass.STOCKS,
            **changes,
        )


# The annotation rule: factories return the class their spec derives.

# Datasets served by a method of the session itself rather than of a namespace; each dataset is
# named after its method.
_SESSION_FACTORIES = ("snapshots",)


def _hints(function: Callable[..., object]) -> dict[str, Any]:
    """Resolve a function's annotations, including names its module imports for typing only.

    Args:
        function: A factory, an overload of one, or a property getter.

    Returns:
        The annotations by parameter name, and `"return"`.
    """
    # A namespace module imports its typing-only names (`Mapping`, `Sequence`, `date`) under
    # `TYPE_CHECKING`, so they are not in its globals; supply them here.
    names = {
        **vars(collections.abc),
        **vars(dt),
        **{name: getattr(queries, name) for name in queries.__all__},
        **{
            name: getattr(dm, name)
            for name in ("SymbolsLike", "DateLike", "FilterValue", "ErrorPolicy")
        },
        **vars(_enums),
        "AbstractSet": collections.abc.Set,
        "Literal": Literal,
    }
    return typing.get_type_hints(function, localns=names)


def _factories(owner: type, path: str) -> Iterator[tuple[str, Callable[..., object]]]:
    """Yield every public factory method reachable from `owner`, by its dotted dataset path.

    A property whose return annotation is a `Namespace` subclass leads to a nested namespace
    (`m.stocks`, `m.partners.benzinga`, `m.stocks.financials`); every other public function
    defined on a namespace class is a factory.

    Args:
        owner: The session class or a namespace class.
        path: The dotted path of `owner` from the session, empty for the session.

    Yields:
        `(dataset path, factory)` pairs, such as `("stocks.trades", Stocks.trades)`.
    """
    for name, member in vars(owner).items():
        if name.startswith("_"):
            continue
        dotted = f"{path}.{name}" if path else name
        if isinstance(member, property) and member.fget is not None:
            target = _hints(member.fget)["return"]
            if isinstance(target, type) and issubclass(target, Namespace):
                yield from _factories(target, dotted)
        elif callable(member) and issubclass(owner, Namespace):
            yield dotted, member


def _bar_problems(name: str, factory: Callable[..., object]) -> list[str]:
    """Check that a `bars` factory requires its timespan: every call names its bar length.

    Args:
        name: The dataset name, for the messages.
        factory: Its factory method.

    Returns:
        One message when `timespan` has a default.
    """
    timespan = inspect.signature(factory).parameters.get("timespan")
    if timespan is not None and timespan.default is not inspect.Parameter.empty:
        return [f"{name}: timespan must be required, with no default"]
    return []


def _factory_map(session: type) -> dict[str, Callable[..., object]]:
    """Return every factory method reachable from a session class, by dataset name.

    Args:
        session: The session class whose namespaces hold the factories.

    Returns:
        The namespace factories (`_factories`) and the session's own dataset methods.
    """
    factories = dict(_factories(session, ""))
    for method in _SESSION_FACTORIES:
        if hasattr(session, method):
            factories[method] = getattr(session, method)
    return factories


def summary_problems(session: type, specs: Mapping[str, DatasetSpec]) -> list[str]:
    """Return every factory whose docstring does not start with its dataset's summary.

    The summary is the sentence a result's table description and the column reference show, so
    the dataset method's docstring, which a console user reads, opens with the same words. A
    factory without a spec, or a spec without a factory, is the annotation rule's concern and
    is skipped here.

    Args:
        session: The session class whose namespaces hold the factories.
        specs: The catalog's specs by name.

    Returns:
        One message per factory whose docstring is missing or starts otherwise.
    """
    factories = _factory_map(session)
    problems = []
    for name, spec in specs.items():
        factory = factories.get(name)
        if factory is None:
            continue
        if not (factory.__doc__ or "").lstrip().startswith(spec.summary):
            problems.append(f"{name}: the docstring must start with {spec.summary!r}")
    return problems


def _declared_classes(spec: DatasetSpec) -> tuple[type[Query], ...]:
    """Return the classes a dataset's factory must declare it returns, in their order.

    A bars dataset that streams declares both classes a call can give: `StreamHistoryQuery`
    for the bars Massive streams and `HistoryQuery` for the rest (`bar_query_class`).

    Args:
        spec: The dataset.

    Returns:
        The classes, one unless the dataset is bars that stream.
    """
    if spec.is_bars and spec.has_channels:
        return (StreamHistoryQuery, HistoryQuery)
    return (query_class(spec),)


def annotation_problems(session: type, specs: Mapping[str, DatasetSpec]) -> list[str]:
    """Return every break of the annotation rule.

    Args:
        session: The session class whose namespaces hold the factories.
        specs: The catalog's specs by name.

    Returns:
        One message per factory without a spec, spec without a factory, or factory whose return
        annotation is not the class its spec derives: for a bars dataset that streams, both
        classes a call can give (`bar_query_class`).
    """
    factories = _factory_map(session)
    problems = [f"{name}: a factory with no dataset" for name in sorted(factories - specs.keys())]
    for name, spec in specs.items():
        factory = factories.get(name)
        if factory is None:
            problems.append(f"{name}: a dataset with no factory")
        else:
            if spec.is_bars:
                problems.extend(_bar_problems(name, factory))
            declared = _hints(factory)["return"]
            expected = _declared_classes(spec)
            if (typing.get_args(declared) or (declared,)) != expected:
                shown = " | ".join(cls.__name__ for cls in expected)
                problems.append(f"{name}: the factory must return {shown}")
    return problems


def test_every_factory_returns_the_class_of_its_dataset() -> None:
    assert annotation_problems(dm.MassiveSession, CATALOG.by_name) == []


class _Good(Namespace):
    __slots__ = ()
    _path: ClassVar[str] = "stocks"

    def bars(self, timespan: Timespan, *, multiplier: int = 1) -> StreamHistoryQuery | HistoryQuery:
        raise NotImplementedError

    def dividends(self) -> FetchQuery:
        raise NotImplementedError


class _Bad(Namespace):
    __slots__ = ()
    _path: ClassVar[str] = "options"

    def bars(  # a default timespan, which every bars factory must not have
        self, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> Query:
        raise NotImplementedError

    def trades(
        self,
    ) -> FetchQuery:  # the trades dataset streams, so this must be a StreamQuery
        raise NotImplementedError

    def news(self) -> FetchQuery:  # no dataset is named options.news
        raise NotImplementedError

    def minute_bars(self) -> HistoryQuery:  # bars that stream must declare both classes
        raise NotImplementedError


class _GoodSession:
    @property
    def stocks(self) -> _Good:
        raise NotImplementedError

    @property
    def label(self) -> str:  # a property that leads to no namespace
        raise NotImplementedError

    def snapshots(self) -> FetchQuery:
        raise NotImplementedError


class _BadSession:
    @property
    def options(self) -> _Bad:
        raise NotImplementedError

    def snapshots(self) -> FetchQuery:  # no snapshots dataset
        raise NotImplementedError


def _renamed(spec: DatasetSpec, name: str) -> DatasetSpec:
    return DatasetSpec(
        name=name,
        summary="A dataset for the tests.",
        is_bars=spec.is_bars,
        asset_class=spec.asset_class,
        channels=spec.channels,
        fetch_path=spec.fetch_path,
        history_path=spec.history_path,
    )


def _catalog(*specs: tuple[DatasetSpec, str]) -> dict[str, DatasetSpec]:
    return {name: _renamed(spec, name) for spec, name in specs}


def test_the_annotation_rule_passes_matching_factories() -> None:
    specs = _catalog(
        (STOCK_BARS, "stocks.bars"),
        (DIVIDENDS, "stocks.dividends"),
        (DIVIDENDS, "snapshots"),  # served by the session's own `snapshots()`
    )
    assert annotation_problems(_GoodSession, specs) == []


def test_the_annotation_rule_reports_every_mismatch() -> None:
    specs = _catalog(
        (STOCK_BARS, "options.bars"),
        (TRADES_SPEC, "options.trades"),
        (STOCK_BARS, "options.minute_bars"),
        (DIVIDENDS, "options.splits"),
    )
    assert annotation_problems(_BadSession, specs) == [
        "options.news: a factory with no dataset",
        "snapshots: a factory with no dataset",
        "options.bars: timespan must be required, with no default",
        "options.bars: the factory must return StreamHistoryQuery | HistoryQuery",
        "options.trades: the factory must return StreamQuery",
        "options.minute_bars: the factory must return StreamHistoryQuery | HistoryQuery",
        "options.splits: a dataset with no factory",
    ]


# The summary rule: each dataset method's docstring starts with its dataset's summary.


def test_every_factory_docstring_starts_with_its_summary() -> None:
    assert summary_problems(dm.MassiveSession, CATALOG.by_name) == []


def test_symbols_is_the_first_positional_parameter_of_every_factory() -> None:
    # One form per parameter: `m.reference.news("AAPL")` reads like `m.stocks.trades("AAPL")`,
    # also where `symbols` may be left out.
    found = []
    for name, factory in _factory_map(dm.MassiveSession).items():
        params = list(inspect.signature(factory).parameters.values())[1:]  # after self
        if any(p.name == "symbols" for p in params):
            found.append(name)
            assert params[0].name == "symbols", name
            assert params[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD, name
    assert {"reference.tickers", "reference.news", "options.contract_details"} <= set(found)
    assert "partners.benzinga.news" in found


class _Summarized(Namespace):
    __slots__ = ()

    def dividends(self) -> FetchQuery:
        """Cash dividends of US-listed stocks. The rest of the summary line.

        The rest of the docstring.
        """
        raise NotImplementedError

    def splits(self) -> FetchQuery:
        """Something else entirely."""
        raise NotImplementedError

    def news(self) -> FetchQuery:
        raise NotImplementedError


class _SummarizedSession:
    @property
    def stocks(self) -> _Summarized:
        raise NotImplementedError


def _summarized(name: str, summary: str) -> DatasetSpec:
    return DatasetSpec(name=name, summary=summary, fetch_path="/v3/reference/dividends")


def test_the_summary_rule_reports_every_mismatch() -> None:
    specs = {
        spec.name: spec
        for spec in (
            _summarized("stocks.dividends", "Cash dividends of US-listed stocks."),
            _summarized("stocks.splits", "Stock splits of US-listed stocks."),
            _summarized("stocks.news", "News articles."),
            _summarized("stocks.ipos", "Initial public offerings."),  # no factory: skipped
        )
    }
    assert summary_problems(_SummarizedSession, specs) == [
        "stocks.splits: the docstring must start with 'Stock splits of US-listed stocks.'",
        "stocks.news: the docstring must start with 'News articles.'",
    ]


def test_a_typed_parameter_is_reserved_from_filters(massive_session: dm.MassiveSession) -> None:
    # Every typed parameter of the factory, set or not, and each of its suffixed forms.
    for key in ("frequency", "frequency_gte", "frequency_any_of"):
        own = f"The filter {key!r} has its own argument. Pass frequency= instead."
        with pytest.raises(ValueError, match=whole(own)):
            build(
                massive_session,
                DIVIDENDS,
                symbols=["AAPL"],
                params={"frequency": None},
                filters={key: 4},
            )
    # The valid filters an unknown key's message names leave the typed family out.
    with pytest.raises(ValueError, match="has no filter 'pay_date'") as caught:
        build(massive_session, DIVIDENDS, params={"frequency": None}, filters={"pay_date": "x"})
    assert "frequency" not in str(caught.value)
    assert "Pass one of distribution_type, " in str(caught.value)


def test_a_fixed_parameter_is_sent_and_reserved(massive_session: dm.MassiveSession) -> None:
    # The library sets `frequency` itself here: it is sent, and no filter form may name it.
    fixed = dataclasses.replace(DIVIDENDS, fixed_params={"frequency": 4})
    q = build(massive_session, fixed, symbols=["AAPL"])
    assert dict(q._request.params) == {"frequency": 4}
    for key in ("frequency", "frequency_gte"):
        library = (
            f"The library sets {key!r} itself, so it cannot be a filter. Remove it from filters."
        )
        with pytest.raises(ValueError, match=whole(library)):
            build(massive_session, fixed, filters={key: 4})
    # A refused filter's list of valid keys leaves the fixed family out.
    with pytest.raises(ValueError, match="has no filter 'nope'") as refused:
        build(massive_session, fixed, filters={"nope": 1})
    assert "frequency" not in str(refused.value)
    with pytest.raises(ValueError, match="has no filter 'nope'") as listed:
        build(massive_session, DIVIDENDS, filters={"nope": 1})
    assert "frequency" in str(listed.value)
    with pytest.raises(
        ValueError,
        match=whole(
            "stocks.dividends takes frequency both typed and fixed. This is a bug in "
            "deephaven-massive. Turn on dm.enable_debug_logging(), reproduce it, and report it "
            "with the log and m.streams() at https://github.com/chipkent/deephaven-massive/issues."
        ),
    ):
        build(massive_session, fixed, params={"frequency": 2})


def test_a_symbol_filter_names_symbols(massive_session: dm.MassiveSession) -> None:
    for key in ("ticker", "ticker_any_of", "ticker_all_of"):
        refusal = f"The filter {key!r} sets the symbols of stocks.dividends. Pass symbols= instead."
        with pytest.raises(ValueError, match=whole(refusal)):
            build(massive_session, DIVIDENDS, filters={key: "AAPL"})
    # The range forms select an alphabetical range, which `symbols` cannot express.
    assert build(massive_session, DIVIDENDS, filters={"ticker_gte": "A"})._request.filters


def test_require_bool() -> None:
    for good in (True, False):
        assert require_bool("flag", good) is good
    for bad in (0, 1, "true", None):
        with pytest.raises(TypeError, match=rf"^flag must be a bool, not {type(bad).__name__}$"):
            require_bool("flag", bad)


def test_one_or_any_of_sends_one_value_alone_and_a_sequence_as_any_of() -> None:
    assert one_or_any_of("code", None, every="every code") == {"code": None, "code.any_of": None}
    assert one_or_any_of("code", "ES", every="every code") == {"code": "ES", "code.any_of": None}
    assert one_or_any_of("code", ["ES", "NQ"], every="every code") == {
        "code": None,
        "code.any_of": ("ES", "NQ"),
    }
    with pytest.raises(
        ValueError,
        match=whole("code=() is empty, so it would select nothing. Leave code out for every code."),
    ):
        one_or_any_of("code", (), every="every code")
    with pytest.raises(
        TypeError, match=r"^code must be a str, a sequence of str, or None, not int$"
    ):
        one_or_any_of("code", 5, every="every code")


def test_optional_text() -> None:
    assert optional_text("search", None) is None
    assert optional_text("search", "apple") == "apple"
    with pytest.raises(TypeError, match=r"^search must be a str, not int$"):
        optional_text("search", 5)


# --- The library's time columns and the adjustment notes, on every declared dataset -----------


@pytest.mark.parametrize("spec", list(CATALOG.by_name.values()), ids=lambda spec: spec.name)
def test_every_dataset_ends_with_when_its_rows_arrived(spec: DatasetSpec) -> None:
    schema = CATALOG.dataset_schema(spec)
    last = schema.columns[-1]
    assert (last.name, last.type.value) == ("TimestampInsert", "Instant")
    # Every transport of the dataset fills it.
    assert set(last.supplied_by) == {transport for transport, _ in spec.sources}


@pytest.mark.parametrize("spec", list(CATALOG.by_name.values()), ids=lambda spec: spec.name)
def test_tick_bar_and_daily_rows_carry_their_trading_day_as_text(spec: DatasetSpec) -> None:
    types = {c.name: c.type.value for c in CATALOG.dataset_schema(spec).columns}
    times = spec.times
    if times is RowTimes.NONE:
        assert {"TimestampStart", "TimestampEnd"}.isdisjoint(types)
        return
    assert types["Date"] == "String"
    window = {"TimestampStart", "TimestampEnd"}
    if times is RowTimes.TICK:
        assert types["Timestamp"] == "Instant"
        assert window.isdisjoint(types)
    else:
        assert "Timestamp" not in types
        assert all(types[name] == "Instant" for name in window)


def test_the_row_times_of_the_datasets() -> None:
    def named(times: RowTimes) -> set[str]:
        return {name for name, spec in CATALOG.by_name.items() if spec.times is times}

    # The indicators are one value per bar, and the daily open-close summaries one day each.
    assert {"indicators.sma", "indicators.ema", "indicators.rsi", "indicators.macd"} <= named(
        RowTimes.BAR
    )
    assert named(RowTimes.DAY) == {
        "stocks.daily_open_close",
        "options.daily_open_close",
        "indices.daily_open_close",
        "crypto.daily_open_close",
    }
    assert "stocks.snapshots" in named(RowTimes.NONE)
    assert {"stocks.trades", "futures.quotes", "indices.values"} <= named(RowTimes.TICK)


def test_adjustment_notes_name_the_price_and_volume_columns() -> None:
    notes = adjustment_notes(("Close",), ("Volume",))
    assert notes == {"Close": ADJUSTED_PRICE, "Volume": ADJUSTED_VOLUME}
    assert "never adjusts for cash dividends" in ADJUSTED_PRICE
    # Only stocks and options have an adjustment choice; their bars, previous day's bar, and
    # open-close carry it. The options open interest, greeks, and unusual trades note their bare
    # `UnderlyingTicker`, and the admin messages what their archive's texts leave out.
    noted = sorted(name for name, spec in CATALOG.by_name.items() if spec.column_notes)
    assert noted == [
        "options.admin_messages",
        "options.bars",
        "options.daily_open_close",
        "options.greeks",
        "options.open_interest",
        "options.previous_day_bar",
        "options.unusual_trades",
        "stocks.bars",
        "stocks.daily_open_close",
        "stocks.previous_day_bar",
    ]
    for name in noted:
        spec = CATALOG.by_name[name]
        columns = set(CATALOG.dataset_schema(spec).column_names)
        assert set(spec.column_notes) <= columns, name


def _dataset_methods() -> Iterator[tuple[DatasetSpec, Callable[..., object]]]:
    # Every public dataset method that takes `limit`, with the dataset it reads: a namespace
    # method's name is its dataset's (`stocks.dividends`), and the session's own is `snapshots`.
    def subclasses(cls: type[Namespace]) -> Iterator[type[Namespace]]:
        for sub in cls.__subclasses__():
            yield sub
            yield from subclasses(sub)

    yield CATALOG.by_name["snapshots"], dm.MassiveSession.snapshots
    for cls in subclasses(Namespace):
        for name, method in vars(cls).items():
            public = callable(method) and not name.startswith("_")
            if public and "limit" in inspect.signature(method).parameters:
                yield CATALOG.by_name[f"{cls._path}.{name}"], method


def _caps_each_symbol(spec: DatasetSpec) -> bool:
    # Whether `limit` caps each symbol's rows when symbols are named: the reader caps each
    # request that names one symbol.
    named = ("AAA", "BBB")
    calls = [
        call
        for template, own in fetch_paths(spec, named)
        for call in plan_calls(
            spec,
            CATALOG.rest_path(template),
            Request(symbols=named),
            symbols=own,
            split=lambda symbol: (symbol, "USD"),
        )
    ]
    return any(call.symbol is not None for call in calls)


def test_every_dataset_says_whether_limit_caps_the_total_or_each_symbol() -> None:
    wrong = []
    for spec, method in _dataset_methods():
        parameters = inspect.signature(method).parameters
        found = re.search(
            r"^ {4}limit:(.*?)(?=^ {0,4}\S|\Z)",
            inspect.getdoc(method) or "",
            re.MULTILINE | re.DOTALL,
        )
        text = " ".join(found.group(1).split()) if found else ""
        symbols = parameters.get("symbols")
        each = symbols is not None and _caps_each_symbol(spec)
        # `limit` caps the total on a dataset that sends its symbols together, and on one that
        # caps each symbol when the symbols are optional and left out.
        total = not each or symbols is None or symbols.default is not inspect.Parameter.empty
        says_each = re.search(r"\b(each|per)\b", text, re.IGNORECASE) is not None
        says_total = re.search(r"\btotal\b", text) is not None
        if (says_each, says_total) != (each, total):
            wrong.append(f"{spec.name}: each={each} total={total}: {text!r}")
    assert wrong == []


def test_a_declared_class_the_dataset_cannot_give_is_a_library_defect(
    massive_session: dm.MassiveSession,
) -> None:
    # A streaming dataset declared as a REST one would lie to its caller.
    with pytest.raises(dm.MassiveError) as info:
        build_query(massive_session._core, TRADES_SPEC, query_type=FetchQuery, symbols=["AAPL"])
    assert info.value.source is dm.ErrorSource.LIBRARY
    assert str(info.value).startswith(
        "m.stocks.trades gives a StreamQuery, not the FetchQuery it declares. "
    )
    # A subclass of the declared class is what the declaration promises: bars declare
    # `HistoryQuery`, and minute bars, which stream, are a `StreamHistoryQuery`.
    minute = build_query(
        massive_session._core,
        STOCK_BARS,
        query_type=HistoryQuery,
        symbols=["AAPL"],
        timespan=Timespan.MINUTE,
    )
    assert type(minute) is StreamHistoryQuery
    hourly = build_query(
        massive_session._core,
        STOCK_BARS,
        query_type=HistoryQuery,
        symbols=["AAPL"],
        timespan=Timespan.HOUR,
    )
    assert type(hourly) is HistoryQuery


_BAR_SYMBOLS = {
    AssetClass.STOCKS: "AAPL",
    AssetClass.OPTIONS: "O:AAPL250117C00150000",
    AssetClass.FUTURES: "ESZ5",
    AssetClass.INDICES: "I:SPX",
    AssetClass.FOREX: "C:EURUSD",
    AssetClass.CRYPTO: "X:BTCUSD",
}
"""A valid symbol of each asset class that has bars."""


@pytest.mark.parametrize(
    "spec", [spec for spec in CATALOG.by_name.values() if spec.is_bars], ids=lambda spec: spec.name
)
def test_every_bars_call_builds_a_class_its_method_declares(
    massive_session: dm.MassiveSession, spec: DatasetSpec
) -> None:
    # Every class the method declares is built for some call, and no call builds another.
    declared = _declared_classes(spec)
    built = {
        type(
            build_query(
                massive_session._core,
                spec,
                query_type=HistoryQuery,
                symbols=[_BAR_SYMBOLS[spec.single_class]],
                timespan=timespan,
                multiplier=multiplier,
            )
        )
        for timespan in Timespan
        for multiplier in (1, 5)
    }
    assert built == set(declared)


def test_a_bars_call_without_a_timespan_is_refused(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match="timespan"):
        build_query(massive_session._core, STOCK_BARS, query_type=HistoryQuery, symbols=["AAPL"])


def test_bar_periods_are_left_to_massive() -> None:
    # Massive answers HTTP 400 to a multiplier below 1 and to session bars outside futures, and
    # serves futures day bars, so none is refused here.
    _check_bar_period(Timespan.DAY, 1)
    _check_bar_period(Timespan.SESSION, 1)
    _check_bar_period(Timespan.DAY, 0)
    _check_bar_period(Timespan.MINUTE, -5)
    refusal = "timespan must be dm.Timespan, such as dm.Timespan.DAY, not 'day'"
    with pytest.raises(TypeError, match=whole(refusal)):
        _check_bar_period("day", 1)


@pytest.mark.parametrize("multiplier", [True, 1.0])
def test_bar_multiplier_must_be_an_int(multiplier: object) -> None:
    with pytest.raises(TypeError, match="multiplier must be an int"):
        _check_bar_period(Timespan.DAY, multiplier)
