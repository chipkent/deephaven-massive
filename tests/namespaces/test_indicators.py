"""Tests for the `m.indicators` namespace."""

import json
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType, required_fields
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import indicators
from deephaven_massive._namespaces.base import Namespace
from deephaven_massive._query import Query
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply, envelope

OWNER = "namespaces/indicators"
R = Transport.REST
NAMES = ["sma", "ema", "rsi", "macd"]
WINDOWED = [("sma", indicators.SMA), ("ema", indicators.EMA), ("rsi", indicators.RSI)]


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.indicators
    assert type(namespace) is indicators._Indicators
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert [spec.name for spec in indicators.DATASETS] == [f"indicators.{n}" for n in NAMES]
    assert all(CATALOG.by_name[spec.name] is spec for spec in indicators.DATASETS)


@pytest.mark.parametrize("name", NAMES)
def test_each_indicator_reads_the_stock_and_crypto_paths(name: str) -> None:
    spec = CATALOG.by_name[f"indicators.{name}"]
    stock = f"/v1/indicators/{name}/{{stockTicker}}"
    crypto = f"/v1/indicators/{name}/{{cryptoTicker}}"
    assert spec.asset_class is None
    assert spec.sources == ((R, stock), (R, crypto))
    assert spec.fetch_path == stock
    assert spec.per_symbol
    assert spec.time_axis == ("timestamp.gte", "timestamp.lt")
    assert spec.poll_keys == ("Symbol", "TimestampStart")
    assert spec.page_size == 5000
    assert spec.keys_for(R, stock).symbol == KeySpec("stockTicker", from_request=True)
    assert spec.keys_for(R, crypto).symbol == KeySpec("cryptoTicker", from_request=True)
    schema = CATALOG.dataset_schema(spec)
    # Each value is one bar's: the bar's window and trading day, then the values.
    times = ("TimestampStart", "TimestampEnd")
    assert schema.column_names[:4] == ("Symbol", "Date", *times)
    assert schema.column_names[-1] == "TimestampInsert"
    types = {c.name: c.type for c in schema.columns}
    assert types["Symbol"] is ColumnType.STRING
    assert types["Date"] is ColumnType.STRING
    assert all(types[t] is ColumnType.INSTANT for t in (*times, "TimestampInsert"))
    extra = {"Histogram", "Signal"} if name == "macd" else set()
    library = {"Symbol", "Date", *times, "TimestampInsert"}
    assert set(schema.column_names) == {*library, "Value", *extra}


@pytest.mark.parametrize(("name", "spec"), WINDOWED)
def test_a_windowed_indicator_builds_its_query(
    massive_session: dm.MassiveSession, name: str, spec: DatasetSpec
) -> None:
    factory = getattr(massive_session.indicators, name)
    query = factory(["x:btcusd", "aapl"], 20)
    assert type(query) is Query
    assert query._spec is spec
    assert query._request == Request(
        symbols=("AAPL", "X:BTCUSD"),
        timespan=dm.Timespan.DAY,
        # `expand_underlying=false` is fixed by the library on every request.
        params=(
            ("expand_underlying", False),
            ("series_type", dm.SeriesType.CLOSE),
            ("window", 20),
        ),
    )
    later = factory(
        "AAPL",
        5,
        timespan=dm.Timespan.MINUTE,
        series_type=dm.SeriesType.HIGH,
        adjusted=False,
        start=datetime(2025, 1, 2, tzinfo=UTC),
        end=datetime(2025, 1, 3, 15, tzinfo=UTC),
        order=dm.Order.ASC,
        limit=100,
    )
    assert later._request == Request(
        symbols=("AAPL",),
        timespan=dm.Timespan.MINUTE,
        adjusted=False,
        start=datetime(2025, 1, 2, tzinfo=UTC),
        end=datetime(2025, 1, 3, 15, tzinfo=UTC),
        params=(
            ("expand_underlying", False),
            ("order", dm.Order.ASC),
            ("series_type", dm.SeriesType.HIGH),
            ("window", 5),
        ),
        limit=100,
    )


@pytest.mark.parametrize("name", NAMES)
def test_the_summary_opens_the_docstring(name: str) -> None:
    doc = getattr(indicators._Indicators, name).__doc__
    assert doc.splitlines()[0] == CATALOG.by_name[f"indicators.{name}"].summary


def test_macd_takes_three_windows(massive_session: dm.MassiveSession) -> None:
    query = massive_session.indicators.macd(
        "I:SPX", short_window=12, long_window=26, signal_window=9
    )
    assert query._spec is indicators.MACD
    assert dict(query._request.params) == {
        "expand_underlying": False,
        "long_window": 26,
        "series_type": dm.SeriesType.CLOSE,
        "short_window": 12,
        "signal_window": 9,
    }
    with pytest.raises(ValueError, match="signal_window must be positive"):
        massive_session.indicators.macd("AAPL", short_window=12, long_window=26, signal_window=0)


def test_arguments_are_validated_at_construction(massive_session: dm.MassiveSession) -> None:
    i = massive_session.indicators
    with pytest.raises(ValueError, match=r"indicators\.sma does not accept dm\.ALL"):
        i.sma(dm.ALL, 20)
    for span in (dm.Timespan.SECOND, dm.Timespan.SESSION):
        with pytest.raises(ValueError, match=f"Timespan.{span.name}"):
            i.sma("AAPL", 20, timespan=span)
    with pytest.raises(ValueError, match="window must be positive"):
        i.rsi("AAPL", 0)
    with pytest.raises(TypeError, match="adjusted must be a bool"):
        i.rsi("AAPL", 14, adjusted=cast("bool", 1))
    with pytest.raises(ValueError, match="the library sets expand_underlying itself"):
        i.sma("AAPL", 20, filters={"expand_underlying": True})
    with pytest.raises(ValueError, match=r"^adjusted is set through its own argument, adjusted="):
        i.sma("AAPL", 20, filters={"adjusted": True})
    with pytest.raises(ValueError, match="timestamp_gte is set through start= and end="):
        i.sma("AAPL", 20, filters={"timestamp_gte": 1})
    with pytest.raises(ValueError, match="carries the prefix"):
        i.sma("Q:NOPE", 20)


def test_enums_reject_text(massive_session: dm.MassiveSession) -> None:
    i = massive_session.indicators
    factories: list[Callable[..., Query]] = [
        lambda **kw: i.sma("AAPL", 20, **kw),
        lambda **kw: i.ema("AAPL", 20, **kw),
        lambda **kw: i.rsi("AAPL", 14, **kw),
        lambda **kw: i.macd("AAPL", short_window=12, long_window=26, signal_window=9, **kw),
    ]
    for factory in factories:
        with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
            factory(timespan="day")
        with pytest.raises(TypeError, match=r"series_type must be dm\.SeriesType"):
            factory(series_type="close")
        with pytest.raises(TypeError, match=r"order must be dm\.Order"):
            factory(order="asc")


@pytest.mark.parametrize(
    ("fixture", "values"),
    [
        ("sma_stock", [{"Value": 140.139}]),
        (
            "macd_crypto",
            [
                {"Value": 145.3613333333, "Signal": 106.9811666667, "Histogram": 38.3801666667},
                {"Value": 143.8374874833, "Signal": 102.7386283473, "Histogram": 41.098859136},
            ],
        ),
    ],
)
def test_the_vendor_samples_decode_without_drift(
    load_fixture: Callable[[str, str], bytes], fixture: str, values: list[dict[str, float]]
) -> None:
    # The rows are `results.values`; `results.underlying` holds the bars and is not decoded.
    name, kind = fixture.split("_")
    payload = json.loads(load_fixture(OWNER, f"{fixture}.json"))
    rest_path = cast("RestPath", source(R, f"/v1/indicators/{name}/{{{kind}Ticker}}"))
    spec = CATALOG.by_name[f"indicators.{name}"]
    required = [f.column for f in required_fields(spec, rest_path)]
    assert required == ["Timestamp"]
    batch, drift = to_batch(payload["results"]["values"], rest_path, required=required)
    assert drift.extra == drift.missing == frozenset()
    rows = batch.to_pylist()
    assert [{k: row[k] for k in value} for row, value in zip(rows, values, strict=True)] == values
    assert rows[0]["Timestamp"] == 1517562000016


# --- End to end through the fake REST server -------------------------------------------------


@pytest.fixture
def rest_session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def sent(server: FakeRestServer) -> dict[str, dict[str, list[str]]]:
    # Every GET the server received, by decoded path: its decoded query.
    return {
        unquote(urlsplit(r.target).path): parse_qs(urlsplit(r.target).query)
        for r in server.requests
    }


def sample(load_fixture: Callable[[str, str], bytes], name: str) -> Reply:
    # The vendor sample as the reply body, without its `next_url`, which names Massive's host.
    body = json.loads(load_fixture(OWNER, f"{name}.json"))
    body.pop("next_url")
    return Reply(body)


def test_each_symbol_class_is_served_by_its_own_path(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_rest.reply("/v1/indicators/sma/AAPL", sample(load_fixture, "sma_stock"))
    crypto = envelope({"values": [{"timestamp": 1517562001016, "value": 9001.5}]})
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", Reply(crypto))
    query = rest_session.indicators.sma(
        ["X:BTCUSD", "AAPL"],
        50,
        timespan=dm.Timespan.HOUR,
        series_type=dm.SeriesType.OPEN,
        start=datetime(2018, 2, 2, tzinfo=UTC),
        end=datetime(2018, 2, 3, tzinfo=UTC),
        order=dm.Order.ASC,
        limit=10,
    )
    out = dh_to_pylist(query.fetch())
    shared = {
        "timespan": ["hour"],
        "window": ["50"],
        "series_type": ["open"],
        "order": ["asc"],
        "expand_underlying": ["false"],
        "limit": ["10"],  # the page size, capped by the total-row limit
        "timestamp.gte": ["1517529600000"],
        "timestamp.lt": ["1517616000000"],
    }
    # `adjusted` is None, so neither path receives it.
    assert sent(fake_rest) == {
        "/v1/indicators/sma/AAPL": shared,
        "/v1/indicators/sma/X:BTCUSD": shared,
    }
    assert sorted((r["Symbol"], r["Value"]) for r in out) == [
        ("AAPL", 140.139),
        ("X:BTCUSD", 9001.5),
    ]
    aapl = next(r for r in out if r["Symbol"] == "AAPL")
    start = datetime(2018, 2, 2, 9, 0, 0, 16000, tzinfo=UTC)
    # An hourly value's bar ends an hour after its start, on the New York day it starts on.
    assert (aapl["Date"], aapl["TimestampStart"]) == ("2018-02-02", start)
    assert aapl["TimestampEnd"] == start + timedelta(hours=1)


def test_a_daily_value_takes_the_day_of_its_symbols_class(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # One instant, 03:00 UTC on 2025-01-02: still January 1st in New York, but the 2nd in UTC,
    # the day of a crypto pair.
    stamp = int(datetime(2025, 1, 2, 3, tzinfo=UTC).timestamp() * 1000)
    values = Reply(envelope({"values": [{"timestamp": stamp, "value": 1.0}]}))
    fake_rest.reply("/v1/indicators/sma/AAPL", values)
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", values)
    out = dh_to_pylist(rest_session.indicators.sma(["AAPL", "X:BTCUSD"], 20).fetch())
    windows = {r["Symbol"]: (r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out}
    new_york = timedelta(hours=5)  # UTC-5 in January
    assert windows == {
        "AAPL": (
            "2025-01-01",
            datetime(2025, 1, 1, tzinfo=UTC) + new_york,
            datetime(2025, 1, 2, tzinfo=UTC) + new_york,
        ),
        "X:BTCUSD": (
            "2025-01-02",
            datetime(2025, 1, 2, tzinfo=UTC),
            datetime(2025, 1, 3, tzinfo=UTC),
        ),
    }


def test_an_explicit_adjusted_reaches_only_a_path_that_takes_it(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v1/indicators/ema/AAPL", sample(load_fixture, "sma_stock"))
    rest_session.indicators.ema("AAPL", 20, adjusted=False).fetch()
    assert sent(fake_rest)["/v1/indicators/ema/AAPL"]["adjusted"] == ["false"]
    with pytest.raises(ValueError, match=r"\{cryptoTicker\} does not take \['adjusted'\]"):
        rest_session.indicators.ema("X:BTCUSD", 20, adjusted=True).fetch()


def test_macd_returns_its_three_lines(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    fake_rest.reply("/v1/indicators/macd/X%3ABTCUSD", sample(load_fixture, "macd_crypto"))
    query = rest_session.indicators.macd(
        "X:BTCUSD", short_window=12, long_window=26, signal_window=9
    )
    out = dh_to_pylist(query.fetch())
    params = sent(fake_rest)["/v1/indicators/macd/X:BTCUSD"]
    assert (params["short_window"], params["long_window"], params["signal_window"]) == (
        ["12"],
        ["26"],
        ["9"],
    )
    assert [(r["Symbol"], r["Value"], r["Signal"], r["Histogram"]) for r in out] == [
        ("X:BTCUSD", 145.3613333333, 106.9811666667, 38.3801666667),
        ("X:BTCUSD", 143.8374874833, 102.7386283473, 41.098859136),
    ]


def test_a_date_bound_is_a_day_in_the_symbols_zone(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    empty = Reply(envelope({"values": []}))
    fake_rest.reply("/v1/indicators/sma/AAPL", empty)
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", empty)
    fake_rest.reply("/v1/indicators/sma/C%3AEURUSD", empty)
    day = date(2025, 1, 2)
    symbols = ["AAPL", "X:BTCUSD", "C:EURUSD"]
    rest_session.indicators.sma(symbols, 20, start=day, end=day).fetch()
    requests = sent(fake_rest)
    # A crypto or forex day is a UTC day; a stock day is a New York day (UTC-5 in January).
    # Forex takes the stock template, so its zone comes from the symbol, not the path.
    for path in ("/v1/indicators/sma/X:BTCUSD", "/v1/indicators/sma/C:EURUSD"):
        assert (requests[path]["timestamp.gte"], requests[path]["timestamp.lt"]) == (
            [str(int(datetime(2025, 1, 2, tzinfo=UTC).timestamp() * 1000))],
            [str(int(datetime(2025, 1, 3, tzinfo=UTC).timestamp() * 1000))],
        )
    stock = requests["/v1/indicators/sma/AAPL"]
    assert (stock["timestamp.gte"], stock["timestamp.lt"]) == (
        [str(int(datetime(2025, 1, 2, 5, tzinfo=UTC).timestamp() * 1000))],
        [str(int(datetime(2025, 1, 3, 5, tzinfo=UTC).timestamp() * 1000))],
    )
