"""Tests for the `m.indicators` namespace."""

import inspect
import json
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Any, Final, cast

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.source_columns import required_fields
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._ingest.rest_requests import page_size
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import indicators
from deephaven_massive._namespaces.query_building import Namespace
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
    assert spec.asset_class is ClassFromPrefix.ANY
    assert spec.sources == ((R, stock), (R, crypto))
    assert spec.fetch_path == stock
    assert spec.per_symbol
    assert spec.time_axis == ("timestamp.gte", "timestamp.lt")
    assert spec.poll_keys == ("Symbol", "TimestampStart")
    assert {page_size(spec, CATALOG.rest_path(path)) for path in (stock, crypto)} == {5000}
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
    query = factory(["X:BTCUSD", "AAPL"], 20)
    assert type(query) is Query
    assert query._spec is spec
    assert query._request == Request(
        symbols=("AAPL", "X:BTCUSD"),
        timespan=dm.Timespan.DAY,
        adjusted=True,
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
    with pytest.raises(ValueError, match=r"^'Q:NOPE' is not a Massive stock ticker"):
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
    # The samples' value stamps are those of a day's aggregate: 00:00 New York time for a
    # stock (the sample's own underlying bar), 00:00 UTC for a crypto pair.
    first = {"sma_stock": 1577941200000, "macd_crypto": 1517616000000}
    assert rows[0]["Timestamp"] == first[fixture]


# --- End to end through the fake REST server -------------------------------------------------


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
    crypto = envelope({"values": [{"timestamp": 1517529600000, "value": 9001.5}]})
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
    # `adjusted=True`, the default, reaches only the stock path: the crypto path has no such
    # parameter, and a pair has no splits.
    assert dict(fake_rest.get_queries()) == {
        "/v1/indicators/sma/AAPL": {**shared, "adjusted": ["true"]},
        "/v1/indicators/sma/X:BTCUSD": shared,
    }
    assert sorted((r["Symbol"], r["Value"]) for r in out) == [
        ("AAPL", 140.139),
        ("X:BTCUSD", 9001.5),
    ]
    aapl = next(r for r in out if r["Symbol"] == "AAPL")
    start = datetime(2020, 1, 2, 5, tzinfo=UTC)
    # An hourly value's bar ends an hour after its start, on the New York day it starts on.
    assert (aapl["Date"], aapl["TimestampStart"]) == ("2020-01-02", start)
    assert aapl["TimestampEnd"] == start + timedelta(hours=1)


def test_a_daily_value_takes_the_day_of_its_symbols_class(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # A daily value carries its day's aggregate stamp: 00:00 New York time for a stock, 00:00
    # UTC for a crypto pair, whose day is the UTC day.
    def values(moment: datetime) -> Reply:
        stamp = int(moment.timestamp() * 1000)
        return Reply(envelope({"values": [{"timestamp": stamp, "value": 1.0}]}))

    fake_rest.reply("/v1/indicators/sma/AAPL", values(datetime(2025, 1, 1, 5, tzinfo=UTC)))
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", values(datetime(2025, 1, 2, tzinfo=UTC)))
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


def test_a_range_of_dates_follows_each_symbols_class_days(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> None:
    # A stock's days are New York days and a crypto pair's UTC days: each symbol's request
    # follows its own class's days, so a New York range never cuts the pair's first UTC day.
    def values(*moments: datetime) -> Reply:
        rows = [{"timestamp": int(m.timestamp() * 1000), "value": 1.0} for m in moments]
        return Reply(envelope({"values": rows}))

    def millis(moment: datetime) -> list[str]:
        return [str(int(moment.timestamp() * 1000))]

    new_york = timedelta(hours=5)  # UTC-5 in January
    first, after = datetime(2025, 1, 2, tzinfo=UTC), datetime(2025, 1, 4, tzinfo=UTC)
    fake_rest.reply("/v1/indicators/sma/AAPL", values(first + new_york))
    fake_rest.reply("/v1/indicators/sma/X%3ABTCUSD", values(first))
    query = rest_session.indicators.sma(
        ["AAPL", "X:BTCUSD"], 20, start=date(2025, 1, 2), end=date(2025, 1, 3)
    )
    out = dh_to_pylist(query.fetch())
    assert sorted((r["Symbol"], r["Date"]) for r in out) == [
        ("AAPL", "2025-01-02"),
        ("X:BTCUSD", "2025-01-02"),
    ]
    sent = {path: (q["timestamp.gte"], q["timestamp.lt"]) for path, q in fake_rest.get_queries()}
    assert sent == {
        "/v1/indicators/sma/AAPL": (millis(first + new_york), millis(after + new_york)),
        "/v1/indicators/sma/X:BTCUSD": (millis(first), millis(after)),
    }


def stamp_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the WARNINGs of bars stamped where Massive is not known to stamp one."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "is not a stamp Massive is known to use" in r.message
    ]


@pytest.mark.parametrize(
    ("timespan", "moment", "window"),
    [
        # 03:00 UTC is 22:00 the day before in New York: not the start of a stock's day. The
        # value is labelled by the New York day its stamp falls on.
        (
            dm.Timespan.DAY,
            datetime(2025, 1, 2, 3, tzinfo=UTC),
            (
                "2025-01-01",
                datetime(2025, 1, 1, 5, tzinfo=UTC),
                datetime(2025, 1, 2, 5, tzinfo=UTC),
            ),
        ),
        # Massive stamps a week value on its week's Sunday; a Wednesday is labelled by the
        # week it falls in, a stock week's Monday to Friday.
        (
            dm.Timespan.WEEK,
            datetime(2025, 1, 8, 5, tzinfo=UTC),
            (None, datetime(2025, 1, 6, 5, tzinfo=UTC), datetime(2025, 1, 11, 5, tzinfo=UTC)),
        ),
    ],
    ids=["day", "week"],
)
def test_a_value_of_a_day_or_longer_at_an_unexpected_stamp_is_served_with_a_warning(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    timespan: dm.Timespan,
    moment: datetime,
    window: tuple[str | None, datetime, datetime],
) -> None:
    caplog.set_level(logging.WARNING)
    stamp = int(moment.timestamp() * 1000)
    reply = Reply(envelope({"values": [{"timestamp": stamp, "value": 1.0}]}))
    fake_rest.reply("/v1/indicators/sma/AAPL", reply)
    out = dh_to_pylist(rest_session.indicators.sma("AAPL", 20, timespan=timespan).fetch())
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"], r["Value"]) for r in out] == [
        (*window, 1.0)
    ]
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        "indicators.sma: an indicator value from /v1/indicators/sma/{stockTicker} is stamped "
        f"{moment.isoformat()}"
    )


def test_adjusted_false_reaches_only_a_path_that_takes_it(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply("/v1/indicators/ema/AAPL", sample(load_fixture, "sma_stock"))
    rest_session.indicators.ema("AAPL", 20, adjusted=False).fetch()
    assert dict(fake_rest.get_queries())["/v1/indicators/ema/AAPL"]["adjusted"] == ["false"]
    # Unadjusted prices are never dropped silently on the crypto path, which has no basis.
    with pytest.raises(ValueError, match=r"\{cryptoTicker\} does not take \['adjusted'\]"):
        rest_session.indicators.ema("X:BTCUSD", 20, adjusted=False).fetch()


_SIGNATURE_PARAMS: Final = ("window", "short_window", "long_window", "signal_window")
"""The indicator arguments, beside `timespan`, `series_type`, and `adjusted`, named as on the
wire."""


@pytest.mark.parametrize("name", NAMES)
def test_every_signature_default_is_the_default_massive_documents(name: str) -> None:
    spec = CATALOG.by_name[f"indicators.{name}"]
    path = source(R, cast("str", spec.fetch_path))
    assert isinstance(path, RestPath)
    documented = {p.name: p.default for p in path.params}
    signature = inspect.signature(getattr(indicators._Indicators, name)).parameters
    compared = [
        n for n in (*_SIGNATURE_PARAMS, "timespan", "series_type", "adjusted") if n in signature
    ]
    assert len(compared) == (6 if name == "macd" else 4)
    for argument in compared:
        default = signature[argument].default
        shown = default.value if isinstance(default, Enum) else default
        assert shown == documented[argument], argument
        assert documented[argument] is not None


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
    params = dict(fake_rest.get_queries())["/v1/indicators/macd/X:BTCUSD"]
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
    requests = dict(fake_rest.get_queries())
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
