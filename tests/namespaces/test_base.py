"""Tests for the shared dataset factory."""

from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive._catalog import DatasetSpec, ParamKind
from deephaven_massive._enums import AssetClass, FuturesVenue, Order, Timespan, Transport
from deephaven_massive._namespaces.base import Namespace, bar_query_class, build_query, list_params
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._schema import BARS, TRADES, Column, ColumnType, Schema
from deephaven_massive._timeunits import TimeUnit

S, LD = ColumnType.STRING, ColumnType.LOCAL_DATE

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
    key_maps={
        Transport.FLAT_FILES: {
            "ticker": "Symbol",
            "window_start": "Timestamp",
            "session_end_date": "SessionEndDate",
        }
    },
    time_units={(Transport.FLAT_FILES, "window_start"): TimeUnit.NANOSECONDS},
)
INDEX_BARS = DatasetSpec(
    name="indices.bars",
    query_class=HistoryQuery,
    is_bars=True,
    asset_class=AssetClass.INDICES,
    schema=BARS.schema,
    flat_file_prefixes={Timespan.MINUTE: "us_indices/minute_aggs_v1"},
    key_maps={Transport.FLAT_FILES: {"ticker": "Symbol", "window_start": "Timestamp"}},
    time_units={(Transport.FLAT_FILES, "window_start"): TimeUnit.NANOSECONDS},
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
    schema=Schema("stocks.dividends", (Column("Symbol", S), Column("ExDividendDate", LD))),
    fetch_method="list_stocks_dividends",
    time_axis=("ex_dividend_date_gte", "ex_dividend_date_lte"),
)
INDICATOR = DatasetSpec(
    name="indicators.sma",
    query_class=Query,
    schema=Schema("indicators.sma", (Column("Symbol", S), Column("Value", ColumnType.DOUBLE))),
    fetch_method="get_sma",
)


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
    with pytest.raises(ValueError, match="multiplier must be positive"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 0)


@pytest.mark.parametrize("multiplier", [True, 1.0])
def test_bar_multiplier_must_be_an_int(multiplier: object) -> None:
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, multiplier)


def test_list_params() -> None:
    assert list_params(sort=None, order=None) == {}
    assert list_params(sort="ticker", order=Order.DESC) == {"sort": "ticker", "order": Order.DESC}
    with pytest.raises(TypeError, match="sort must be a str"):
        list_params(sort=cast("str", 1), order=None)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        list_params(sort=None, order=cast("Order", "desc"))


def test_limit_is_a_row_cap_not_an_sdk_parameter(massive_session: dm.MassiveSession) -> None:
    # The total-row cap is kept apart from the SDK parameters, whose `limit` is a page size.
    q = build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=5)
    assert q._request.limit == 5
    assert "limit" not in dict(q._request.params)
    assert "limit=5" in repr(q)
    assert build(massive_session, DIVIDENDS, symbols=["AAPL"])._request.limit is None
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=cast("int", "10"))
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=True)
    with pytest.raises(ValueError, match="limit must be positive"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=0)


def test_a_dataset_without_a_class_takes_any_class_by_prefix(
    massive_session: dm.MassiveSession,
) -> None:
    # Indicators, reference data, and the unified snapshot span asset classes.
    q = build(massive_session, INDICATOR, symbols=["X:BTCUSD", "i:spx", "aapl", "C:EURUSD"])
    assert q._request.symbols == ("AAPL", "C:EURUSD", "I:SPX", "X:BTCUSD")


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
    fx = DatasetSpec(
        name="forex.quotes",
        query_class=StreamQuery,
        asset_class=AssetClass.FOREX,
        schema=TRADES.schema,
    )
    assert build(massive_session, fx, symbols="EUR/USD")._request.symbols == ("C:EURUSD",)
    # A dataset without an asset class reads the class from each prefix; none is a stock.
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
    with pytest.raises(TypeError, match=r"^a: element 1 of the sequence is int, not str$"):
        build(massive_session, DIVIDENDS, filters={"a": ["A", 1]})
    # A dataset without a time axis accepts any filter name.
    assert build(massive_session, INDICATOR, filters={"timestamp_gte": "x"})._request.filters


def test_time_bounds_must_be_aware(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"^start='2025-01-01T09:30' needs a timezone"):
        build(massive_session, DIVIDENDS, start="2025-01-01T09:30")


def test_naive_datetime_filters_and_params_are_rejected(
    massive_session: dm.MassiveSession,
) -> None:
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    message = (
        r"^updated_gte=datetime\.datetime\(2025, 1, 2, 9, 30\) needs a timezone: "
        r"pass an aware datetime or a date$"
    )
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, filters={"updated_gte": naive})
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, params={"updated_gte": naive})
    aware = naive.replace(tzinfo=UTC)
    assert build(massive_session, INDICATOR, filters={"updated_gte": aware})._request.filters


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


def test_timespan_is_an_enum_on_every_dataset(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        build(massive_session, INDICATOR, timespan="day")


def test_namespaces_repr_as_their_path(massive_session: dm.MassiveSession) -> None:
    label = repr(massive_session).removeprefix("<").split(" open")[0]
    m = massive_session
    namespaces = {
        "stocks": m.stocks,
        "options": m.options,
        "futures": m.futures,
        "indices": m.indices,
        "forex": m.forex,
        "crypto": m.crypto,
        "reference": m.reference,
        "indicators": m.indicators,
        "economy": m.economy,
        "partners": m.partners,
        "partners.benzinga": m.partners.benzinga,
        "partners.etf_global": m.partners.etf_global,
        "partners.tmx": m.partners.tmx,
        "partners.fable": m.partners.fable,
    }
    for path, namespace in namespaces.items():
        assert repr(namespace) == f"<{label} {path}>"


def test_a_time_axis_must_not_end_before_it_starts(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="must be before end"):
        build(massive_session, DIVIDENDS, start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError, match="must be before end"):
        build(
            massive_session,
            DIVIDENDS,
            start=datetime(2025, 1, 2, 10, tzinfo=UTC),
            end=datetime(2025, 1, 2, 9, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="must be before end"):
        build(massive_session, DIVIDENDS, start="2025-01-03", end=datetime(2025, 1, 2, tzinfo=UTC))
    # One whole day, an open-ended axis, and a datetime within the end day are all valid.
    assert build(massive_session, DIVIDENDS, start="2025-01-02", end="2025-01-02")._request.end
    assert build(massive_session, DIVIDENDS, start="2025-01-02")._request.end is None
    same_day = datetime(2025, 1, 2, 9, tzinfo=UTC)
    assert build(massive_session, DIVIDENDS, start="2025-01-02", end=same_day)._request.start


def test_adjusted_is_a_typed_bool(massive_session: dm.MassiveSession) -> None:
    spec = STOCK_BARS
    q = build_query(massive_session._core, spec, symbols=["AAPL"], adjusted=False)
    assert q._request.adjusted is False
    assert build_query(massive_session._core, spec, symbols=["AAPL"])._request.adjusted is None
    with pytest.raises(TypeError, match="adjusted must be a bool, not str"):
        build_query(massive_session._core, spec, symbols=["AAPL"], adjusted=cast("bool", "false"))
    # One channel only: adjusted never travels in params, where a transport could drop it.
    with pytest.raises(ValueError, match=r"adjusted is set through build_query\(adjusted=\.\.\.\)"):
        build_query(massive_session._core, spec, symbols=["AAPL"], params={"adjusted": False})


def test_adjusted_is_refused_in_filters(massive_session: dm.MassiveSession) -> None:
    # A filter would reach REST but not the source choice, which could then pick flat files,
    # whose bars are unadjusted.
    with pytest.raises(
        ValueError, match=r"adjusted is set through build_query\(adjusted=\.\.\.\), not filters"
    ):
        build_query(massive_session._core, STOCK_BARS, symbols=["AAPL"], filters={"adjusted": True})
