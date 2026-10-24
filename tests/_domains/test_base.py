"""Tests for the shared dataset factory (DESIGN § 7.1, § 9, § 10)."""

from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive._catalog import DatasetSpec, ParamKind
from deephaven_massive._domains.base import Namespace, bar_query_class, build_query, list_params
from deephaven_massive._enums import AssetClass, FuturesVenue, Order, Timespan
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._schema import BARS, TRADES

STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    query_class=HistoryQuery,
    is_bars=True,
    asset_class=AssetClass.STOCKS,
    schema=BARS.schema,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    fetch_method="get_previous_close_agg",
    history_method="list_aggs",
)
FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    query_class=HistoryQuery,
    is_bars=True,
    asset_class=AssetClass.FUTURES,
    schema=BARS.schema,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_method="list_futures_aggregates",
    flat_file_prefixes={Timespan.SESSION: "us_futures_{venue}/session_aggs_v1"},
)
INDEX_BARS = DatasetSpec(
    name="indices.bars",
    query_class=HistoryQuery,
    is_bars=True,
    asset_class=AssetClass.INDICES,
    schema=BARS.schema,
    flat_file_prefixes={Timespan.MINUTE: "us_indices/minute_aggs_v1"},
)
TRADES_SPEC = DatasetSpec(
    name="stocks.trades",
    query_class=StreamQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
)
DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
    query_class=Query,
    asset_class=AssetClass.STOCKS,
    fetch_method="list_stocks_dividends",
    time_axis=("ex_dividend_date_gte", "ex_dividend_date_lte"),
)
INDICATOR = DatasetSpec(name="indicators.sma", query_class=Query, fetch_method="get_sma")


def build(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> QueryBase:
    return build_query(session._core, spec, **cast("dict[str, Any]", kwargs))


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((STOCK_BARS, Timespan.SECOND, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 1), HistoryLatestQuery),
        ((STOCK_BARS, Timespan.MINUTE, 5), HistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 2), HistoryQuery),
        ((STOCK_BARS, Timespan.HOUR, 1), HistoryQuery),
        ((FUTURES_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((FUTURES_BARS, Timespan.SESSION, 1), HistoryQuery),
        ((INDEX_BARS, Timespan.DAY, 1), HistoryQuery),
    ],
)
def test_bar_query_class_selection(case: tuple[DatasetSpec, Timespan, int], expected: type) -> None:
    assert bar_query_class(*case) is expected


def test_bar_timespan_rules() -> None:
    with pytest.raises(ValueError, match="futures bars have no DAY timespan"):
        bar_query_class(FUTURES_BARS, Timespan.DAY, 1)
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bar_query_class(STOCK_BARS, Timespan.SESSION, 1)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        bar_query_class(STOCK_BARS, "day", 1)
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, True)
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 1.0)
    with pytest.raises(ValueError, match="multiplier must be positive"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 0)


def test_list_params() -> None:
    assert list_params(sort=None, order=None, limit=None) == {}
    assert list_params(sort="ticker", order=Order.DESC, limit=10) == {
        "sort": "ticker",
        "order": Order.DESC,
        "limit": 10,
    }
    with pytest.raises(TypeError, match="sort must be a str"):
        list_params(sort=cast("str", 1), order=None, limit=None)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        list_params(sort=None, order=cast("Order", "desc"), limit=None)
    with pytest.raises(TypeError, match="limit must be an int"):
        list_params(sort=None, order=None, limit=cast("int", "10"))
    with pytest.raises(TypeError, match="limit must be an int"):
        list_params(sort=None, order=None, limit=True)
    with pytest.raises(ValueError, match="limit must be positive"):
        list_params(sort=None, order=None, limit=0)


def test_build_query_normalizes_the_request(massive_session: dm.MassiveSession) -> None:
    q = build(
        massive_session,
        DIVIDENDS,
        symbols=["msft", "AAPL"],
        start="2025-01-01",
        end=datetime(2025, 6, 30, tzinfo=UTC),
        params={"sort": "ex_dividend_date", "limit": None, "tickers": ["B", "A"]},
        filters={"pay_date_lte": date(2025, 12, 31), "frequency": 4, "cash_amount_gt": 0.5},
    )
    assert isinstance(q, Query)
    request = q._request
    assert request.symbols == ("AAPL", "MSFT")
    assert request.start == date(2025, 1, 1)
    assert request.end == datetime(2025, 6, 30, tzinfo=UTC)
    assert request.params == (("sort", "ex_dividend_date"), ("tickers", ("B", "A")))
    assert request.filters == (
        ("cash_amount_gt", 0.5),
        ("frequency", 4),
        ("pay_date_lte", date(2025, 12, 31)),
    )
    assert hash(q)


def test_build_query_without_symbols_or_axis(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, INDICATOR, timespan=Timespan.DAY)
    assert type(q) is Query  # not a bars dataset: the timespan does not pick the class
    assert q._request.symbols is None
    assert q._request.timespan is Timespan.DAY


def test_symbols_follow_the_asset_class(massive_session: dm.MassiveSession) -> None:
    fx = DatasetSpec(name="forex.quotes", query_class=StreamQuery, asset_class=AssetClass.FOREX)
    assert build(massive_session, fx, symbols="EUR/USD")._request.symbols == ("C:EURUSD",)
    # Datasets without an asset class take stock-style tickers.
    assert build(massive_session, INDICATOR, symbols="aapl")._request.symbols == ("AAPL",)


def test_all_where_the_dataset_cannot_serve_it(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"stocks\.dividends does not accept dm\.ALL; use X"):
        build(massive_session, DIVIDENDS, symbols=dm.ALL, all_alternative="X")
    assert build(massive_session, TRADES_SPEC, symbols=dm.ALL)._request.symbols is dm.ALL


def test_bars_pick_their_class(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, STOCK_BARS, symbols="AAPL", timespan=Timespan.MINUTE)
    assert type(q) is StreamHistoryQuery
    q = build(massive_session, FUTURES_BARS, symbols="ESZ5", timespan=Timespan.SESSION)
    assert type(q) is HistoryQuery
    assert type(build(massive_session, STOCK_BARS, symbols="AAPL")) is HistoryQuery


def test_venue_is_an_enum(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, FUTURES_BARS, symbols="ESZ5", venue=FuturesVenue.CME)
    assert q._request.venue is FuturesVenue.CME
    with pytest.raises(TypeError, match=r"venue must be dm\.FuturesVenue"):
        build(massive_session, FUTURES_BARS, symbols="ESZ5", venue="cme")


def test_filter_validation(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="set through start= and end="):
        build(massive_session, DIVIDENDS, filters={"ex_dividend_date_gte": "2025-01-01"})
    with pytest.raises(TypeError, match="filters must be a mapping"):
        build(massive_session, DIVIDENDS, filters=[("a", 1)])
    with pytest.raises(TypeError, match="filter names must be str"):
        build(massive_session, DIVIDENDS, filters={1: "a"})
    with pytest.raises(TypeError, match="sequence of str; got dict"):
        build(massive_session, DIVIDENDS, filters={"a": {"b": 1}})
    with pytest.raises(TypeError, match="sequence of str; got list"):
        build(massive_session, DIVIDENDS, filters={"a": [1, 2]})
    # A dataset without a time axis accepts any filter name.
    assert build(massive_session, INDICATOR, filters={"timestamp_gte": "x"})._request.filters


def test_time_bounds_must_be_aware(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="timezone required"):
        build(massive_session, DIVIDENDS, start="2025-01-01T09:30")


def test_namespaces_hold_the_core(massive_session: dm.MassiveSession) -> None:
    namespace = Namespace(massive_session._core)
    assert namespace._core is massive_session._core
    assert not hasattr(namespace, "_query")


def test_filters_go_through_the_sdk_check(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "deephaven_massive._catalog.SDK_PARAMS",
        {"list_stocks_dividends": {"pay_date_lte": ParamKind.DATE}},
    )
    build(massive_session, DIVIDENDS, filters={"pay_date_lte": "2025-12-31"})
    with pytest.raises(ValueError, match="has no filter 'frequency'"):
        build(massive_session, DIVIDENDS, filters={"frequency": 4})
