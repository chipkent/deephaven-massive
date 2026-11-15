"""Tests for planning the requests of a REST call, without a server."""

import dataclasses
from datetime import UTC, date, datetime

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import TimeRange
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Direction, Order, Timespan
from deephaven_massive._ingest import rest_requests
from deephaven_massive._massive import RestPath, Transport

R = Transport.REST

DIVIDENDS_PATH = "/stocks/v1/dividends"

CRYPTO_LAST_PATH = "/v1/last/crypto/{from}/{to}"

CRYPTO_LAST = DatasetSpec(
    summary="A dataset for the tests.",
    name="crypto.last",
    asset_class=AssetClass.CRYPTO,
    fetch_path=CRYPTO_LAST_PATH,
    keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
)

MOVERS_PATH = "/v2/snapshot/locale/us/markets/stocks/{direction}"

MOVERS = DatasetSpec(
    summary="A dataset for the tests.",
    name="stocks.movers",
    asset_class=AssetClass.STOCKS,
    fetch_path=MOVERS_PATH,
    keys={(R, MOVERS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)

SMA_STOCK = "/v1/indicators/sma/{stockTicker}"

SMA_CRYPTO = "/v1/indicators/sma/{cryptoTicker}"

SMA = DatasetSpec(
    summary="A dataset for the tests.",
    name="indicators.sma",
    row_times=RowTimes.BAR,
    fetch_path=SMA_STOCK,
    fetch_paths_by_class={AssetClass.CRYPTO: SMA_CRYPTO},
    time_axis=("timestamp.gte", "timestamp.lt"),
    keys={
        (R, SMA_STOCK): SourceKeys(
            symbol=KeySpec("stockTicker", from_request=True), time=KeySpec("Timestamp")
        ),
        (R, SMA_CRYPTO): SourceKeys(
            symbol=KeySpec("cryptoTicker", from_request=True), time=KeySpec("Timestamp")
        ),
    },
)

INDEX_SNAPSHOT_PATH = "/v3/snapshot/indices"

INDEX_SNAPSHOTS = DatasetSpec(
    summary="A dataset for the tests.",
    name="indices.snapshots",
    asset_class=AssetClass.INDICES,
    fetch_path=INDEX_SNAPSHOT_PATH,
    symbol_param="ticker.any_of",
    keys={(R, INDEX_SNAPSHOT_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
)

TRADES_PATH = "/v3/trades/{stockTicker}"

RANGE_PATH = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"

FUTURES_TRADES_PATH = "/futures/v1/trades/{ticker}"

FUTURES_AGGS_PATH = "/futures/v1/aggs/{ticker}"

FUTURES_BARS = DatasetSpec(
    summary="A dataset for the tests.",
    name="futures.bars",
    asset_class=AssetClass.FUTURES,
    is_bars=True,
    history_path=FUTURES_AGGS_PATH,
    time_axis=("window_start.gte", "window_start.lt"),
    keys={
        (R, FUTURES_AGGS_PATH): SourceKeys(
            KeySpec("ticker", from_request=True),
            KeySpec("WindowStart"),
            date=KeySpec("SessionEndDate"),
        )
    },
)

NEWS_PATH = "/v2/reference/news"


def _rest_path(template: str) -> RestPath:
    path = _massive.source(R, template)
    assert isinstance(path, RestPath)
    return path


def test_date_filters_on_instant_parameters_keep_their_calendar_meaning() -> None:
    path = _rest_path(SMA_CRYPTO)
    day = date(2025, 3, 3)
    instant = datetime(2025, 3, 3, 12, tzinfo=UTC)
    query = {
        "timestamp.gte": day,  # from the day's start
        "timestamp.lt": day,  # before the day's start
        "timestamp.gt": day,  # after the day's end
        "timestamp.lte": day,  # up to the day's end
        "timestamp": day,  # no range bound: kept
        "window": 50,
        "unknown.gte": day,
    }
    assert rest_requests._filter_days(
        path, {**query, "series_type": instant}, AssetClass.CRYPTO
    ) == {
        **query,
        "series_type": instant,
        "timestamp.gte": datetime(2025, 3, 3, tzinfo=UTC),
        "timestamp.lt": datetime(2025, 3, 3, tzinfo=UTC),
        "timestamp.gt": datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC),
        "timestamp.lte": datetime(2025, 3, 3, 23, 59, 59, 999000, tzinfo=UTC),
    }
    # An instant the caller gave is exact already; a calendar date parameter keeps its day.
    # An instant given by the caller is sent as given, even on an inclusive or exclusive
    # bound that a date would move by one unit.
    given = {"timestamp.gte": instant, "timestamp.lte": instant, "timestamp.gt": instant}
    assert rest_requests._filter_days(path, given, ClassFromPrefix.ANY) == given
    dividends = _rest_path(DIVIDENDS_PATH)
    assert rest_requests._filter_days(
        dividends, {"ex_dividend_date.gte": day}, ClassFromPrefix.ANY
    ) == {"ex_dividend_date.gte": day}


PAIRS = {
    "X:BTCUSD": ("BTC", "USD"),
    "X:ETHUSD": ("ETH", "USD"),
    "X:SOLUSD": ("SOL", "USD"),
    "X:XRPUSD": ("XRP", "USD"),
}
"""The crypto pairs these tests use, as Massive's crypto ticker list splits them."""


def split_pair(symbol: str) -> tuple[str, str]:
    # The split of a crypto pair, as `RestReader.crypto_pair` reads it from the list.
    return PAIRS[symbol]


def test_a_dataset_without_pairs_refuses_a_pair_path() -> None:

    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.pair",
        asset_class=AssetClass.STOCKS,
        fetch_path=CRYPTO_LAST_PATH,
        keys={(R, CRYPTO_LAST_PATH): SourceKeys(symbol=KeySpec("from", from_request=True))},
    )
    path = _rest_path(CRYPTO_LAST_PATH)
    with pytest.raises(ValueError, match="only forex and crypto symbols are currency pairs"):
        rest_requests.plan_calls(
            spec, path, Request(symbols=("AAPL",)), symbols=("AAPL",), split=split_pair
        )


def test_a_per_symbol_path_needs_explicit_symbols() -> None:
    path = _rest_path(CRYPTO_LAST_PATH)
    with pytest.raises(ValueError, match="needs explicit symbols"):
        rest_requests.plan_calls(
            CRYPTO_LAST, path, Request(symbols=dm.ALL), symbols=dm.ALL, split=split_pair
        )


def test_a_symbol_query_parameter_of_one_value_fans_out() -> None:
    news = DatasetSpec(
        summary="A dataset for the tests.",
        name="reference.news",
        fetch_path="/v2/reference/news",
        symbol_param="ticker",
        keys={(R, "/v2/reference/news"): SourceKeys()},
    )
    path = _rest_path("/v2/reference/news")
    calls = rest_requests.plan_calls(
        news, path, Request(), symbols=("AAPL", "MSFT"), split=split_pair
    )
    assert [(c.symbol, dict(c.query)) for c in calls] == [
        ("AAPL", {"limit": 1000, "ticker": "AAPL"}),
        ("MSFT", {"limit": 1000, "ticker": "MSFT"}),
    ]
    assert rest_requests.plan_calls(news, path, Request(), symbols=(), split=split_pair) == []
    # A path that takes no symbols ignores them: one request.
    movers = _rest_path(MOVERS_PATH)
    request = Request(params=(("direction", Direction.LOSERS),))
    (call,) = rest_requests.plan_calls(
        MOVERS, movers, request, symbols=("AAPL", "MSFT"), split=split_pair
    )
    assert (call.symbol, dict(call.path_params), dict(call.query)) == (
        None,
        {"direction": Direction.LOSERS},
        {},
    )
    assert [
        c.symbol
        for c in rest_requests.plan_calls(news, path, Request(), symbols=None, split=split_pair)
    ] == [None]


def test_a_per_symbol_path_with_a_list_parameter_sends_one_symbol_each() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.each",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        symbol_param="ticker.any_of",
        per_symbol=True,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    )
    path = _rest_path(DIVIDENDS_PATH)
    calls = rest_requests.plan_calls(spec, path, Request(), symbols=("A", "B"), split=split_pair)
    assert [dict(c.query) for c in calls] == [
        {"limit": 5000, "ticker.any_of": ("A",)},
        {"limit": 5000, "ticker.any_of": ("B",)},
    ]


def test_a_symbol_filled_into_a_query_parameter() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.by_query",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("ticker", from_request=True))},
    )
    path = _rest_path(DIVIDENDS_PATH)
    calls = rest_requests.plan_calls(spec, path, Request(), symbols=("A",), split=split_pair)
    assert [(c.symbol, dict(c.path_params), dict(c.query)) for c in calls] == [
        ("A", {}, {"limit": 5000, "ticker": "A"})
    ]


@pytest.mark.parametrize(
    ("path", "time_axis", "expected"),
    [
        (TRADES_PATH, ("timestamp.gte", "timestamp.lt"), {"order": "asc", "sort": "timestamp"}),
        ("/v1/indicators/sma/{stockTicker}", ("timestamp.gte", "timestamp.lt"), {"order": "asc"}),
        (RANGE_PATH, None, {"sort": "asc"}),
        (FUTURES_AGGS_PATH, ("window_start.gte", "window_start.lt"), {"sort": "window_start.asc"}),
        (FUTURES_TRADES_PATH, ("timestamp.gte", "timestamp.lt"), {"sort": "timestamp.asc"}),
        (FUTURES_TRADES_PATH, None, {}),
        # Several sort fields: the axis field when listed, else only the direction.
        (
            "/vX/reference/ipos",
            ("announced_date.gte", "announced_date.lte"),
            {"order": "asc", "sort": "announced_date"},
        ),
        ("/vX/reference/ipos", ("other.gte", "other.lt"), {"order": "asc"}),
        ("/v1/open-close/{stocksTicker}/{date}", None, {}),
    ],
)
def test_history_asks_for_ascending_order(
    path: str, time_axis: tuple[str, str] | None, expected: dict[str, object]
) -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="test.ascending",
        history_path=path,
        time_axis=time_axis,
    )
    rest_path = _rest_path(path)
    assert rest_requests._ascending(spec, rest_path, Request()) == expected
    # A direction the caller set is kept.
    assert rest_requests._ascending(spec, rest_path, Request(params=(("order", "desc"),))) == {}


def test_a_caller_direction_folded_into_sort_is_not_overridden() -> None:
    # The futures aggregates take no `order`: a caller's `order` folds into `sort`, and the
    # ascending default stays out of the way.
    rest_path = _rest_path(FUTURES_AGGS_PATH)
    mine = Request(
        symbols=("ESZ5",),
        timespan=Timespan.MINUTE,
        params=(("order", Order.DESC), ("sort", "window_start")),
    )
    order = rest_requests._ascending(FUTURES_BARS, rest_path, mine)
    (call,) = rest_requests.plan_calls(
        FUTURES_BARS, rest_path, mine, symbols=("ESZ5",), extra=order, split=split_pair
    )
    assert call.query["sort"] == "window_start.desc"
    assert "order" not in call.query
    # Without a caller direction, the ascending one is sent.
    plain = Request(symbols=("ESZ5",), timespan=Timespan.MINUTE)
    order = rest_requests._ascending(FUTURES_BARS, rest_path, plain)
    (call,) = rest_requests.plan_calls(
        FUTURES_BARS, rest_path, plain, symbols=("ESZ5",), extra=order, split=split_pair
    )
    assert call.query["sort"] == "window_start.asc"


def test_ascending_prefers_the_axis_field_among_several_ascending_sorts() -> None:
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="test.ascending",
        history_path=FUTURES_AGGS_PATH,
        time_axis=("window_start.gte", "window_start.lt"),
    )
    aggs = _rest_path(FUTURES_AGGS_PATH)
    values = ("ticker.asc", "window_start.asc", "window_start.desc")
    params = tuple(
        dataclasses.replace(p, values=values) if p.name == "sort" else p for p in aggs.params
    )
    several = dataclasses.replace(aggs, params=params)
    assert rest_requests._ascending(spec, several, Request()) == {"sort": "window_start.asc"}
    other = dataclasses.replace(spec, time_axis=("other.gte", "other.lt"))
    assert rest_requests._ascending(other, several, Request()) == {"sort": "ticker.asc"}


def axis_spec(lower: str, upper: str, path: str = TRADES_PATH) -> DatasetSpec:
    return DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.axis",
        asset_class=AssetClass.STOCKS,
        history_path=path,
        time_axis=(lower, upper),
        keys={
            (R, path): SourceKeys(
                KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
            )
        }
        if path == TRADES_PATH
        else {},
    )


def axis(spec: DatasetSpec, start: date | None, end: date | None) -> dict[str, object]:
    assert spec.history_path is not None
    path = _rest_path(spec.history_path)
    return rest_requests.axis_query(spec, path, dates=(start, end), asset_class=spec.asset_class)


def test_date_axes_are_whole_days_and_shift_for_exclusive_ends() -> None:
    spec = axis_spec("ex_dividend_date.gt", "ex_dividend_date.lt", DIVIDENDS_PATH)
    assert axis(spec, date(2025, 1, 10), date(2025, 1, 20)) == {
        "ex_dividend_date.gt": date(2025, 1, 9),
        "ex_dividend_date.lt": date(2025, 1, 21),
    }
    inclusive = axis_spec("ex_dividend_date.gte", "ex_dividend_date.lte", DIVIDENDS_PATH)
    assert axis(inclusive, date(2025, 1, 10), None) == {"ex_dividend_date.gte": date(2025, 1, 10)}


def test_timestamp_axes_take_midnights_in_the_day_zone() -> None:
    spec = axis_spec("timestamp.gte", "timestamp.lt")
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out["timestamp.gte"] == datetime(2025, 3, 3, 5, tzinfo=UTC)
    # A dataset without an asset class bounds its days in New York.
    path = _rest_path(SMA_STOCK)
    unclassed = rest_requests.axis_query(
        SMA, path, dates=(date(2025, 7, 1), None), asset_class=SMA.asset_class
    )
    assert unclassed == {"timestamp.gte": datetime(2025, 7, 1, 4, tzinfo=UTC)}
    assert out["timestamp.lt"] == datetime(2025, 3, 4, 5, tzinfo=UTC)


def test_inclusive_millisecond_bounds_move_back_one_unit() -> None:
    spec = axis_spec("timestamp.gt", "timestamp.lte", SMA_STOCK)
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out["timestamp.gt"] == datetime(2025, 3, 3, 4, 59, 59, 999000, tzinfo=UTC)
    assert out["timestamp.lte"] == datetime(2025, 3, 4, 4, 59, 59, 999000, tzinfo=UTC)


def test_an_instant_field_behind_a_date_parameter_makes_an_instant_axis() -> None:
    spec = axis_spec("published_utc.gte", "published_utc.lt", NEWS_PATH)
    out = axis(spec, date(2025, 3, 3), date(2025, 3, 3))
    assert out == {
        "published_utc.gte": datetime(2025, 3, 3, 5, tzinfo=UTC),
        "published_utc.lt": datetime(2025, 3, 4, 5, tzinfo=UTC),
    }
    assert all(type(value) is datetime for value in out.values())
    # An instant bound is already exact.
    instant = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    assert axis(spec, instant, None) == {"published_utc.gte": instant}
    # Massive does not document the precision of the ISO-8601 text, so no inclusive bound.
    inclusive = axis_spec("published_utc.gte", "published_utc.lte", NEWS_PATH)
    with pytest.raises(ValueError, match=r"published_utc\.lte takes ISO-8601 instants"):
        axis(inclusive, None, date(2025, 3, 3))


def test_a_date_parameter_without_a_field_of_its_name_stays_whole_days() -> None:
    # The options chain snapshot nests `expiration_date` under `details`.
    path = "/v3/snapshot/options/{underlyingAsset}"
    spec = axis_spec("expiration_date.gte", "expiration_date.lte", path)
    assert axis(spec, date(2025, 3, 3), date(2025, 3, 4)) == {
        "expiration_date.gte": date(2025, 3, 3),
        "expiration_date.lte": date(2025, 3, 4),
    }


def test_a_nanosecond_inclusive_bound_is_refused() -> None:
    spec = axis_spec("timestamp.gte", "timestamp.lte")
    with pytest.raises(ValueError, match=r"timestamp\.lte takes nanoseconds"):
        axis(spec, None, date(2025, 3, 3))


def test_an_axis_the_path_lacks_or_of_another_kind() -> None:
    spec = axis_spec("ticker.gte", "nothing.lt", DIVIDENDS_PATH)
    assert axis(spec, date(2025, 3, 3), date(2025, 3, 4)) == {"ticker.gte": "2025-03-03"}
    assert (
        rest_requests.axis_query(
            MOVERS,
            _rest_path(MOVERS_PATH),
            dates=(None, None),
            asset_class=MOVERS.asset_class,
        )
        == {}
    )


def test_an_empty_symbol_tuple_plans_no_request() -> None:
    path = _rest_path(INDEX_SNAPSHOT_PATH)
    assert (
        rest_requests.plan_calls(INDEX_SNAPSHOTS, path, Request(), symbols=(), split=split_pair)
        == []
    )
    assert rest_requests.fetch_paths(SMA, ()) == [(SMA_STOCK, ())]


def test_futures_date_bounds_are_whole_sessions() -> None:
    path = "/futures/v1/trades/{ticker}"
    spec = DatasetSpec(
        summary="A dataset for the tests.",
        name="futures.recent",
        asset_class=AssetClass.FUTURES,
        fetch_path=path,
        time_axis=("timestamp.gte", "timestamp.lt"),
        keys={(R, path): SourceKeys(symbol=KeySpec("ticker", from_request=True))},
    )
    rest_path = _rest_path(path)
    out = rest_requests.axis_query(
        spec, rest_path, dates=(date(2025, 3, 4), date(2025, 3, 4)), asset_class=spec.asset_class
    )
    # The session ending 2025-03-04 opens at 17:00 Chicago time on the 3rd.
    assert out == {
        "timestamp.gte": datetime(2025, 3, 3, 17, tzinfo=CHICAGO),
        "timestamp.lt": datetime(2025, 3, 4, 17, tzinfo=CHICAGO),
    }


def test_range_segments_floor_to_whole_milliseconds() -> None:
    start = datetime(2025, 3, 3, 14, 30, 0, 123456, tzinfo=UTC)
    end = datetime(2025, 3, 3, 15, 0, 0, 500, tzinfo=UTC)
    out = rest_requests._range_segments(TimeRange(start, end))
    assert out == {
        "from": datetime(2025, 3, 3, 14, 30, 0, 123000, tzinfo=UTC),
        "to": datetime(2025, 3, 3, 15, 0, 0, 0, tzinfo=UTC),
    }


def test_the_page_size_is_the_documented_maximum_unless_the_dataset_sets_one() -> None:
    dividends = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.dividends",
        asset_class=AssetClass.STOCKS,
        fetch_path=DIVIDENDS_PATH,
        keys={(R, DIVIDENDS_PATH): SourceKeys(symbol=KeySpec("Ticker"))},
    )
    path = _rest_path(DIVIDENDS_PATH)
    assert rest_requests.page_size(dividends, path) == 5000
    assert rest_requests.page_size(dataclasses.replace(dividends, page_size=100), path) == 100
    # A path without `limit` has no page size, whatever the dataset sets.
    movers = _rest_path(MOVERS_PATH)
    assert rest_requests.page_size(MOVERS, movers) is None


def test_a_path_without_a_documented_maximum_sends_only_the_row_cap() -> None:
    # The catalog requires a page size of such a path's datasets; a spec outside it that sets
    # none sends Massive's default page, or the caller's cap.
    bars = DatasetSpec(
        summary="A dataset for the tests.",
        name="stocks.bars",
        asset_class=AssetClass.STOCKS,
        is_bars=True,
        history_path=RANGE_PATH,
        keys={
            (R, RANGE_PATH): SourceKeys(
                KeySpec("stocksTicker", from_request=True), KeySpec("WindowStart")
            )
        },
    )
    path = _rest_path(RANGE_PATH)
    assert rest_requests.page_size(bars, path) is None
    _, query = rest_requests.shared_params(bars, path, Request(), fixed={})
    assert "limit" not in query
    _, query = rest_requests.shared_params(bars, path, Request(limit=7), fixed={})
    assert query["limit"] == 7
    _, query = rest_requests.shared_params(
        dataclasses.replace(bars, page_size=50_000), path, Request(), fixed={}
    )
    assert query["limit"] == 50_000


def test_symbol_request_fills_a_pair_with_its_two_currencies() -> None:
    path = _rest_path(CRYPTO_LAST_PATH)
    assert rest_requests._symbol_request(
        CRYPTO_LAST, "X:BTCUSD", path, targets=["from"], param=None, split=split_pair
    ) == ("X:BTCUSD", {"from": "BTC", "to": "USD"}, {}, None)


def test_symbol_request_sends_the_symbol_in_its_parameter() -> None:
    news = DatasetSpec(
        summary="A dataset for the tests.",
        name="reference.news",
        fetch_path="/v2/reference/news",
        symbol_param="ticker",
        keys={(R, "/v2/reference/news"): SourceKeys()},
    )
    path = _rest_path("/v2/reference/news")
    assert rest_requests._symbol_request(
        news, "AAPL", path, targets=[], param="ticker", split=split_pair
    ) == ("AAPL", {}, {"ticker": "AAPL"}, None)
