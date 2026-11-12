"""Tests for the `m.indices` namespace: its dataset declarations, schemas, and factory methods.

The fixtures under `tests/data/namespaces/indices/` are shaped from the vendor's examples for
each source the namespace reads; a test checks that every key they carry is a generated field
of that source, so no fixture can invent a wire key. The end-to-end tests send those fixtures
through the fake REST server, the fake WebSocket server, and the fake S3 bucket, and check the
exact requests and the rows every dataset returns.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven.stream import blink_to_append_only

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._enums import NotBars
from deephaven_massive._massive import Transport, sources
from deephaven_massive._session import CATALOG
from deephaven_massive._tables import stream
from deephaven_massive.namespaces import _indices as indices
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.queries import FetchQuery, HistoryQuery, StreamHistoryQuery
from deephaven_massive.queries._choice import query_class

# The reply and envelope builders are plain functions of the fake, which no fixture hands over.
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from deephaven.table import Table

    from tests._fakes.s3 import FakeS3
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

NOT_STREAMED = r"\.blink\(\): only 1-second and 1-minute bars at multiplier=1 stream"
"""The refusal of a stream of bars Massive doesn't stream."""

OWNER = "namespaces/indices"
SNAPSHOT = "/v3/snapshot/indices"
PREVIOUS = "/v2/aggs/ticker/{indicesTicker}/prev"
RANGE = "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}"
OPEN_CLOSE = "/v1/open-close/{indicesTicker}/{date}"


@pytest.fixture
def m(massive_session: dm.MassiveSession) -> dm.MassiveSession:
    return massive_session


def test_namespace_is_reached_from_the_session(m: dm.MassiveSession) -> None:
    namespace = m.indices
    assert type(namespace) is indices.IndicesNamespace
    assert isinstance(namespace, Namespace)


def test_every_dataset_is_declared_once_in_the_catalog() -> None:
    names = [spec.name for spec in indices.DATASETS]
    assert names == [
        "indices.values",
        "indices.bars",
        "indices.previous_day_bar",
        "indices.snapshots",
        "indices.daily_open_close",
    ]
    for spec in indices.DATASETS:
        assert CATALOG.by_name[spec.name] is spec


def test_the_namespace_passes_every_catalog_gate() -> None:
    assert Catalog(indices.DATASETS).problems() == ()
    assert [p for p in CATALOG.problems() if "indices." in p] == []


def test_the_datasets_read_the_documented_sources() -> None:
    rest, ws, files = Transport.REST, Transport.WS, Transport.FLAT_FILES
    assert indices.VALUES.sources == (
        (ws, "indices/V"),
        (files, "us_indices/values_v1"),
    )
    assert indices.BARS.sources == (
        (rest, RANGE),
        (ws, "indices/A"),
        (ws, "indices/AM"),
        (files, "us_indices/minute_aggs_v1"),
        (files, "us_indices/day_aggs_v1"),
    )
    assert indices.PREVIOUS_DAY_BAR.sources == ((rest, PREVIOUS),)
    assert indices.SNAPSHOTS.sources == ((rest, SNAPSHOT),)
    assert indices.DAILY_OPEN_CLOSE.sources == ((rest, OPEN_CLOSE),)


def test_every_dataset_is_an_instrument_dataset() -> None:
    # Each row names one index, so the declared-keys gate requires its symbol key; the open-close
    # rows carry it and the path takes one index a request.
    for spec in indices.DATASETS:
        assert CATALOG.takes_symbols(spec), spec.name


def test_values_history_is_flat_files_only() -> None:
    # The current level of an index is the snapshot's, a dataset of its own.
    assert not indices.VALUES.has_fetch
    assert not indices.VALUES.has_rest_history
    assert indices.VALUES.flat_file_prefixes == {NotBars.ANY: "us_indices/values_v1"}


def _schema(spec: DatasetSpec) -> list[tuple[str, str]]:
    # The columns the dataset's own sources fill. A kind's schema also holds the columns only
    # its other members supply, which this dataset always leaves null.
    return [
        (c.name, c.type.value)
        for c in CATALOG.dataset_schema(spec, raw_symbol=False).columns
        if c.supplied_by
    ]


def test_values_schema() -> None:
    assert _schema(indices.VALUES) == [
        ("Symbol", "String"),
        ("Date", "String"),
        ("Timestamp", "Instant"),
        ("Value", "double"),
        ("TimestampInsert", "Instant"),
    ]


def test_the_previous_day_bar_has_the_bars_columns_its_endpoint_fills() -> None:
    mine = CATALOG.dataset_schema(indices.PREVIOUS_DAY_BAR).column_names
    bars = CATALOG.dataset_schema(indices.BARS).column_names
    # The bars columns in the bars order, less the official open only the streams and files fill.
    assert tuple(name for name in bars if name in mine) == mine
    assert set(bars) - set(mine) == {"OfficialOpen"}


def test_bars_schema() -> None:
    # Index bars come from index values: no volume, no VWAP, no trade count.
    columns = {c.name: c for c in CATALOG.dataset_schema(indices.BARS).columns}
    for name in ("Volume", "Vwap", "Transactions"):
        assert name not in columns
    # The stream's window end `e` is the declared end key, so it is `TimestampEnd`.
    assert _schema(indices.BARS) == [
        ("Symbol", "String"),
        ("Date", "String"),
        ("TimestampStart", "Instant"),
        ("TimestampEnd", "Instant"),
        ("Open", "double"),
        ("High", "double"),
        ("Low", "double"),
        ("Close", "double"),
        ("OfficialOpen", "double"),
        ("TimestampInsert", "Instant"),
    ]


def test_snapshots_schema() -> None:
    # The snapshot in full; its update time keeps its own name, as on every snapshot.
    assert _schema(indices.SNAPSHOTS) == [
        ("Symbol", "String"),
        ("Value", "double"),
        ("Error", "String"),
        ("LastUpdated", "Instant"),
        ("MarketStatus", "String"),
        ("Message", "String"),
        ("SessionOpen", "double"),
        ("SessionHigh", "double"),
        ("SessionLow", "double"),
        ("SessionClose", "double"),
        ("SessionChange", "double"),
        ("SessionChangePercent", "double"),
        ("SessionPreviousClose", "double"),
        ("Name", "String"),
        ("Timeframe", "String"),
        ("Type", "String"),
        ("TimestampInsert", "Instant"),
    ]


def test_daily_open_close_schema() -> None:
    # The day, `from`, is the row's `Date`, and the library sets the day's window from it.
    assert _schema(indices.DAILY_OPEN_CLOSE) == [
        ("Symbol", "String"),
        ("Date", "String"),
        ("TimestampStart", "Instant"),
        ("TimestampEnd", "Instant"),
        ("Open", "double"),
        ("High", "double"),
        ("Low", "double"),
        ("Close", "double"),
        ("AfterHours", "double"),
        ("PreMarket", "double"),
        ("TimestampInsert", "Instant"),
    ]


def test_no_dataset_has_an_underlying() -> None:
    for spec in indices.DATASETS:
        assert "Underlying" not in CATALOG.dataset_schema(spec).column_names


def test_the_range_endpoint_fills_the_symbol_from_the_request() -> None:
    columns = {c.name: c for c in CATALOG.dataset_schema(indices.BARS).columns}
    assert columns["Symbol"].type.value == "String"
    keys = indices.BARS.keys_for(Transport.REST, RANGE)
    assert keys.symbol is not None
    assert keys.symbol.from_request
    assert keys.symbol.column == "indicesTicker"


# --- Fixtures are shaped like their sources ---------------------------------------------------


def _flatten(row: Mapping[str, Any], prefix: str = "") -> Iterator[str]:
    for key, value in row.items():
        if isinstance(value, Mapping):
            yield from _flatten(value, f"{prefix}{key}.")
        else:
            yield f"{prefix}{key}"


_ENVELOPE = frozenset({"status", "request_id"})
"""The keys of a response that has no result key that belong to the response, not the row."""


@pytest.mark.parametrize(
    ("name", "path"),
    [
        ("snapshot.json", SNAPSHOT),
        ("prev.json", PREVIOUS),
        ("range.json", RANGE),
        ("open_close.json", OPEN_CLOSE),
    ],
)
def test_rest_fixtures_carry_only_generated_fields(
    load_fixture: Callable[[str, str], bytes], name: str, path: str
) -> None:
    source = sources(Transport.REST)[path]
    body = json.loads(load_fixture(OWNER, name))
    rows = [body] if source.result_key is None else body[source.result_key]
    fields = {f.wire_key for f in source.fields}
    for row in rows:
        assert set(_flatten(row)) - _ENVELOPE <= fields
    required = {f.wire_key for f in source.fields if f.required}
    assert any(required <= set(_flatten(row)) for row in rows)


@pytest.mark.parametrize(
    ("name", "channel"),
    [
        ("ws_values.json", "indices/V"),
        ("ws_second_bars.json", "indices/A"),
        ("ws_minute_bars.json", "indices/AM"),
    ],
)
def test_websocket_fixtures_carry_every_generated_field(
    load_fixture: Callable[[str, str], bytes], name: str, channel: str
) -> None:
    fields = {f.wire_key for f in sources(Transport.WS)[channel].fields}
    for frame in json.loads(load_fixture(OWNER, name)):
        assert set(frame) - {"ev"} == fields


@pytest.mark.parametrize(
    ("name", "prefix"),
    [
        ("us_indices_values_v1_2025-06-02.csv", "us_indices/values_v1"),
        ("us_indices_minute_aggs_v1_2025-06-02.csv", "us_indices/minute_aggs_v1"),
        ("us_indices_day_aggs_v1_2025-06-02.csv", "us_indices/day_aggs_v1"),
    ],
)
def test_flat_file_fixtures_have_the_generated_header(
    load_fixture: Callable[[str, str], bytes], name: str, prefix: str
) -> None:
    header = next(csv.reader(io.StringIO(load_fixture(OWNER, name).decode())))
    # The reader matches headers by name; the generated order puts the prices first.
    generated = sources(Transport.FLAT_FILES)[prefix].fields
    assert sorted(header) == sorted(f.wire_key for f in generated)


# --- Factory methods --------------------------------------------------------------------------


def test_values_query(m: dm.MassiveSession) -> None:
    q = m.indices.values(["I:SPX", "I:NDX", "I:SPX"])
    assert type(q) is StreamHistoryQuery
    assert q._spec is indices.VALUES
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert repr(q) == "m.indices.values(['I:NDX', 'I:SPX'])"
    assert m.indices.values(dm.ALL)._request.symbols is dm.ALL


def test_values_have_no_fetch(m: dm.MassiveSession) -> None:
    assert not hasattr(m.indices.values("I:SPX"), "fetch")


def test_values_history_has_no_rest_source(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"indices\.values has no REST history"):
        m.indices.values("I:SPX").history(
            "2025-06-02", "2025-06-02", source=dm.HistorySource.REST, wait=True
        )


def test_values_history_from_flat_files_needs_credentials(
    monkeypatch: pytest.MonkeyPatch, no_aws_settings: None
) -> None:
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)
    with dm.MassiveSession(dm.MassiveConfig(api_key="test-key")) as session:
        q = session.indices.values("I:SPX")
        for source in (dm.HistorySource.FLAT_FILES, dm.HistorySource.AUTO):
            with pytest.raises(ValueError, match="no S3 credentials are configured"):
                q.history("2025-06-02", "2025-06-02", source=source, wait=True)


@pytest.mark.parametrize(
    "case",
    [
        (dm.Timespan.SECOND, 1, True),
        (dm.Timespan.MINUTE, 1, True),
        (dm.Timespan.DAY, 1, False),
        (dm.Timespan.MINUTE, 5, False),
        (dm.Timespan.HOUR, 1, False),
        (dm.Timespan.DAY, 2, False),
        (dm.Timespan.WEEK, 1, False),
    ],
)
def test_every_bars_call_is_one_class_and_only_some_stream(
    m: dm.MassiveSession, case: tuple[dm.Timespan, int, bool]
) -> None:
    timespan, multiplier, streams = case
    q = m.indices.bars("I:SPX", timespan, multiplier=multiplier)
    assert type(q) is StreamHistoryQuery  # every bars call, streaming or not
    if not streams:
        with pytest.raises(ValueError, match=NOT_STREAMED):
            q.blink()
    assert q._request.symbols == ("I:SPX",)
    assert q._request.timespan is timespan
    assert q._request.multiplier == multiplier
    assert q._request.adjusted is None


def test_bars_default_to_streamed_minutes(m: dm.MassiveSession) -> None:
    q = m.indices.bars(["I:SPX"], dm.Timespan.MINUTE)
    assert type(q) is StreamHistoryQuery
    assert repr(q) == ("m.indices.bars('I:SPX', dm.Timespan.MINUTE)")


def test_bars_reject_the_futures_session(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        m.indices.bars("I:SPX", dm.Timespan.SESSION)


def test_bars_have_no_adjusted_parameter(m: dm.MassiveSession) -> None:
    bars = m.indices.bars
    with pytest.raises(TypeError, match="adjusted"):
        bars("I:SPX", adjusted=False)  # type: ignore[call-arg]  # the point: refused at run time


def test_bars_check_their_argument_types(m: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        m.indices.bars("I:SPX", "minute")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(ValueError, match="multiplier must be positive"):
        m.indices.bars("I:SPX", dm.Timespan.MINUTE, multiplier=0)


def test_previous_day_bars_need_explicit_symbols(m: dm.MassiveSession) -> None:
    q = m.indices.previous_day_bar(["I:SPX", "I:NDX"])
    assert type(q) is FetchQuery
    assert repr(q) == ("m.indices.previous_day_bar(['I:NDX', 'I:SPX'])")
    with pytest.raises(
        ValueError,
        match=r"^indices\.previous_day_bar does not accept dm\.ALL; use "
        r"m\.indices\.bars\(dm\.ALL, dm\.Timespan\.DAY\)\.history\(day, day\), "
        r"every ticker's bar of that day$",
    ):
        m.indices.previous_day_bar(dm.ALL)


def test_bars_history_over_rest_needs_explicit_symbols(m: dm.MassiveSession) -> None:
    # Indices have no grouped-daily endpoint, so not even daily bars serve dm.ALL over REST.
    for timespan in (dm.Timespan.MINUTE, dm.Timespan.DAY):
        with pytest.raises(ValueError, match=r"indices\.bars history over REST needs explicit"):
            m.indices.bars(dm.ALL, timespan).history(
                "2025-06-02", "2025-06-02", source=dm.HistorySource.REST, wait=True
            )


def test_bar_flat_files_hold_only_minutes_and_days(m: dm.MassiveSession) -> None:
    q = m.indices.bars("I:SPX", dm.Timespan.HOUR)
    with pytest.raises(ValueError, match="flat files hold only 1-minute, 1-day bars"):
        q.history("2025-06-02", "2025-06-02", source=dm.HistorySource.FLAT_FILES, wait=True)


def test_snapshots_query(m: dm.MassiveSession) -> None:
    q = m.indices.snapshots(
        ["I:SPX", "I:NDX"],
        sort="ticker",
        order=dm.Order.DESC,
        limit=10,
        filters={"ticker_gte": "I:A"},
    )
    assert type(q) is FetchQuery
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert dict(q._request.params) == {"order": dm.Order.DESC, "sort": "ticker"}
    assert dict(q._request.filters) == {"ticker_gte": "I:A"}
    assert q._request.limit == 10
    assert m.indices.snapshots(dm.ALL)._request.symbols is dm.ALL


def test_snapshots_filters_are_the_snapshot_parameters(m: dm.MassiveSession) -> None:
    # The equality forms of the symbol parameter are set through symbols only.
    for key in ("ticker", "ticker_any_of", "ticker_all_of"):
        with pytest.raises(ValueError, match=f"{key} is set through symbols=, not filters"):
            m.indices.snapshots("I:SPX", filters={key: "I:A"})
    with pytest.raises(ValueError, match=r"has no filter 'timestamp_gte'; valid filters: \["):
        m.indices.snapshots("I:SPX", filters={"timestamp_gte": "I:A"})
    with pytest.raises(ValueError, match="'ticker_lt' takes text, not 5"):
        m.indices.snapshots("I:SPX", filters={"ticker_lt": 5})


def test_snapshots_check_their_list_parameters(m: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.indices.snapshots("I:SPX", order="asc")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(ValueError, match="limit must be positive"):
        m.indices.snapshots("I:SPX", limit=0)


@pytest.mark.parametrize(
    "day",
    [
        "2025-06-02",
        date(2025, 6, 2),
    ],
)
def test_daily_open_close_query(m: dm.MassiveSession, day: str | date) -> None:
    q = m.indices.daily_open_close(["I:SPX", "I:NDX"], day)
    assert type(q) is FetchQuery
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert dict(q._request.params) == {"date": date(2025, 6, 2)}


def test_daily_open_close_rejects_all(m: dm.MassiveSession) -> None:
    # Massive has no grouped daily endpoint for indices: the day's bars serve every index.
    with pytest.raises(
        ValueError,
        match=r"; use m\.indices\.bars\(dm\.ALL, dm\.Timespan\.DAY\)\.history\(day, day\), "
        r"every index's bar of that day$",
    ):
        m.indices.daily_open_close(dm.ALL, "2025-06-02")


def test_daily_open_close_needs_a_zoned_day(m: dm.MassiveSession) -> None:
    for moment in (datetime(2025, 6, 2, 15, 30, tzinfo=UTC), "2025-06-03T02:00:00+00:00"):
        with pytest.raises(ValueError, match=r"date=.* is a calendar date; pass a date"):
            m.indices.daily_open_close("I:SPX", moment)
    naive = datetime(2025, 6, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(ValueError, match="needs a timezone"):
        m.indices.daily_open_close("I:SPX", naive)
    with pytest.raises(ValueError, match=r"is neither ISO-8601 .* nor Deephaven date or time text"):
        m.indices.daily_open_close("I:SPX", "June 2")
    with pytest.raises(TypeError, match="date must be a str, date, datetime, Java Instant"):
        m.indices.daily_open_close("I:SPX", 20250602)  # type: ignore[arg-type]  # the point: refused at run time


@pytest.mark.parametrize(
    "name", ["values", "bars", "previous_day_bar", "snapshots", "daily_open_close"]
)
def test_symbols_must_be_index_symbols(m: dm.MassiveSession, name: str) -> None:
    method: object = getattr(m.indices, name)
    assert callable(method)
    extras: dict[str, tuple[object, ...]] = {
        "daily_open_close": ("2025-06-02",),
        "bars": (dm.Timespan.MINUTE,),
    }
    extra = extras.get(name, ())
    with pytest.raises(ValueError, match=r"'X:BTCUSD' is a crypto ticker; this dataset takes"):
        method("X:BTCUSD", *extra)
    with pytest.raises(ValueError, match=r"^'SPX' is not Massive's ticker; use 'I:SPX'"):
        method("SPX", *extra)
    with pytest.raises(ValueError, match="no symbols"):
        method([], *extra)


def test_factories_return_the_class_of_their_dataset() -> None:
    assert query_class(indices.VALUES) is StreamHistoryQuery
    assert query_class(indices.PREVIOUS_DAY_BAR) is FetchQuery
    assert query_class(indices.SNAPSHOTS) is FetchQuery
    assert query_class(indices.DAILY_OPEN_CLOSE) is FetchQuery
    assert query_class(indices.BARS) is StreamHistoryQuery


@pytest.mark.parametrize("spec", indices.DATASETS, ids=lambda spec: spec.name)
def test_each_docstring_names_its_dataset_page(spec: DatasetSpec) -> None:
    dataset = spec.name.split(".", 1)[1]
    doc = getattr(indices.IndicesNamespace, dataset).__doc__
    assert doc.splitlines()[0] == spec.summary
    assert f"docs/datasets/{spec.name}.md" in doc


# --- End to end: REST -------------------------------------------------------------------------

MS = 10**6
NS = 10**9
MIDNIGHT_MS = 1748836800000  # 2025-06-02 00:00 in New York


def rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit. Every dataset row
    # carries when it arrived, last, which the rows here leave out once it is checked.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    if "TimestampInsert" in data.column_names:
        assert data.column_names[-1] == "TimestampInsert"
        assert all(row.pop("TimestampInsert") is not None for row in out)
    return out


def bar_rows(table: Table) -> list[dict[str, Any]]:
    # The columns index bars fill, less the arrival time; any other column is null.
    filled = {name for name, _ in _schema(indices.BARS)} - {"TimestampInsert"}
    out = rows(table)
    for row in out:
        assert all(value is None for name, value in row.items() if name not in filled)
    return [{name: value for name, value in row.items() if name in filled} for row in out]


def test_the_snapshot_names_at_most_250_indices_a_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(SNAPSHOT, Reply(envelope([])))
    symbols = [f"I:X{n:03d}" for n in range(251)]
    rest_session.indices.snapshots(symbols).fetch(wait=True)
    batches = [query["ticker.any_of"][0].split(",") for _, query in fake_rest.get_queries()]
    # The batches are requested concurrently, so they arrive in either order.
    assert sorted(len(batch) for batch in batches) == [1, 250]
    assert sorted(batches[0] + batches[1]) == symbols


def test_snapshots_order_alone_is_sent_as_is(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The snapshot has its own order parameter, so no sort is needed beside it.
    fake_rest.reply(SNAPSHOT, Reply(envelope([])))
    rest_session.indices.snapshots("I:SPX", order=dm.Order.DESC).fetch(wait=True)
    assert fake_rest.get_queries() == [
        (SNAPSHOT, {"ticker.any_of": ["I:SPX"], "order": ["desc"], "limit": ["250"]})
    ]


def test_snapshots_fetch_keeps_every_field_and_the_error_rows(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(SNAPSHOT, Reply(load_fixture(OWNER, "snapshot.json")))
    q = rest_session.indices.snapshots(
        ["I:SPX", "I:NDX", "I:NOPE"], sort="ticker", order=dm.Order.DESC
    )
    out = rows(q.fetch(wait=True))
    assert fake_rest.get_queries() == [
        (
            SNAPSHOT,
            {
                "ticker.any_of": ["I:NDX,I:NOPE,I:SPX"],
                "sort": ["ticker"],
                "order": ["desc"],
                "limit": ["250"],
            },
        )
    ]
    assert out[0] == {
        "Symbol": "I:SPX",
        "RawSymbol": None,
        "Error": None,
        "LastUpdated": 1748894400 * NS,
        "MarketStatus": "closed",
        "Message": None,
        "Name": "S&P 500",
        "SessionChange": 24.25,
        "SessionChangePercent": 0.41,
        "SessionClose": 5935.94,
        "SessionHigh": 5940.35,
        "SessionLow": 5876.0,
        "SessionOpen": 5896.68,
        "SessionPreviousClose": 5911.69,
        "Timeframe": "REAL-TIME",
        "Type": "indices",
        "Value": 5935.94,
    }
    assert [(r["Symbol"], r["Error"], r["Message"], r["Value"]) for r in out[1:]] == [
        ("I:NDX", None, None, 21491.1),
        ("I:NOPE", "NOT_FOUND", "Ticker not found.", None),
    ]


def test_previous_day_bars_fetch_one_index_a_request(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    fake_rest.reply(quote("/v2/aggs/ticker/I:SPX/prev"), Reply(load_fixture(OWNER, "prev.json")))
    out = bar_rows(rest_session.indices.previous_day_bar("I:SPX").fetch(wait=True))
    assert fake_rest.get_queries() == [("/v2/aggs/ticker/I:SPX/prev", {})]
    # The New York day, ending an hour late at 01:00 New York time (midnight in Chicago),
    # since whether an index day bar covers the New York or the Chicago day is not verified.
    assert out == [
        {
            "Symbol": "I:SPX",
            "Date": "2025-06-02",
            "TimestampStart": MIDNIGHT_MS * MS,
            "TimestampEnd": (MIDNIGHT_MS + DAY_MS + HOUR_MS) * MS,
            "Open": 5896.68,
            "Close": 5935.94,
            "High": 5940.35,
            "Low": 5876.0,
        }
    ]


def test_a_previous_day_bar_poll_keeps_one_row_per_index(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    rows_when: Callable[[Table, Callable[[list[dict[str, Any]]], bool]], list[dict[str, Any]]],
) -> None:
    fake_rest.reply(quote("/v2/aggs/ticker/I:SPX/prev"), Reply(load_fixture(OWNER, "prev.json")))
    table = rest_session.indices.previous_day_bar("I:SPX").poll(60)
    found = rows_when(table, lambda got: len(got) == 1)
    assert [(r["Symbol"], r["Close"]) for r in found] == [("I:SPX", 5935.94)]
    assert fake_rest.get_queries()[0] == ("/v2/aggs/ticker/I:SPX/prev", {})


def test_bars_history_over_rest_fills_the_range_and_the_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    path = f"/v2/aggs/ticker/I:SPX/range/1/minute/{MIDNIGHT_MS}/{MIDNIGHT_MS + 86_400_000 - 1}"
    fake_rest.reply(quote(path), Reply(load_fixture(OWNER, "range.json")))
    q = rest_session.indices.bars(["I:SPX"], dm.Timespan.MINUTE)
    out = rows(
        q.history(date(2025, 6, 2), date(2025, 6, 2), source=dm.HistorySource.REST, wait=True)
    )
    assert fake_rest.get_queries() == [(path, {"limit": ["50000"], "sort": ["asc"]})]
    assert [(r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        ("I:SPX", "2025-06-02", 1748871000000 * MS, 1748871060000 * MS),
        ("I:SPX", "2025-06-02", 1748871060000 * MS, 1748871120000 * MS),
    ]
    assert [(r["Open"], r["Close"]) for r in out] == [(5896.68, 5899.02), (5899.02, 5898.4)]


HOUR_MS = 3_600_000
DAY_MS = 86_400_000


def bars_reply(*stamps: int) -> Reply:
    return Reply({"status": "OK", "results": [{"T": "I:SPX", "t": t, "c": 1.0} for t in stamps]})


def stamp_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the WARNINGs of bars stamped where Massive is not known to stamp one."""
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "is not a stamp Massive is known to use" in r.message
    ]


def test_index_bars_of_a_day_or_longer_start_at_new_york_midnight(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    # The range stamps a day at midnight in Chicago (01:00 New York time), `/prev` at the
    # 16:00 close; both mean the New York day, stamped at its start.
    day = date(2025, 6, 2)
    day_path = f"/v2/aggs/ticker/I:SPX/range/1/day/{MIDNIGHT_MS}/{MIDNIGHT_MS + DAY_MS - 1}"
    fake_rest.reply(quote(day_path), bars_reply(MIDNIGHT_MS + HOUR_MS))
    fake_rest.reply(
        quote(PREVIOUS.format(indicesTicker="I:SPX")), bars_reply(MIDNIGHT_MS + 16 * HOUR_MS)
    )
    q = rest_session.indices.bars("I:SPX", dm.Timespan.DAY)
    ranged = rows(q.history(day, day, source=dm.HistorySource.REST, wait=True))
    assert [r["TimestampStart"] for r in ranged] == [MIDNIGHT_MS * MS]
    prev = rest_session.indices.previous_day_bar("I:SPX")
    assert [r["TimestampStart"] for r in rows(prev.fetch(wait=True))] == [MIDNIGHT_MS * MS]
    # Massive starts a week on its Sunday, so a week bar is stamped 01:00 (midnight in
    # Chicago) on its Sunday; since an index can update around the clock, the week holds its
    # weekend: it starts at Sunday's midnight and ends an hour after the next Sunday's. A
    # range must reach the Saturday.
    sunday = MIDNIGHT_MS - DAY_MS
    week_path = f"/v2/aggs/ticker/I:SPX/range/1/week/{sunday}/{MIDNIGHT_MS + 6 * DAY_MS - 1}"
    fake_rest.reply(
        quote(week_path),
        bars_reply(sunday + HOUR_MS),
        bars_reply(sunday + 16 * HOUR_MS),
    )
    week = rest_session.indices.bars("I:SPX", dm.Timespan.WEEK)
    first, last = date(2025, 6, 1), date(2025, 6, 7)
    weeks = rows(week.history(first, last, source=dm.HistorySource.REST, wait=True))
    assert [(r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in weeks] == [
        (None, sunday * MS, (MIDNIGHT_MS + 6 * DAY_MS + HOUR_MS) * MS)
    ]
    # A range from the Monday holds no whole week, so it asks for none.
    assert rows(week.history(day, last, source=dm.HistorySource.REST, wait=True)) == []
    assert not stamp_warnings(caplog)
    # A week bar at the close, or a day bar at 02:00, may be another cut: each keeps its stamp
    # and ends a period later, past these ranges, so neither range holds it; each warns once.
    assert rows(week.history(first, last, source=dm.HistorySource.REST, wait=True)) == []
    fake_rest.reply_first(quote(day_path), bars_reply(MIDNIGHT_MS + 2 * HOUR_MS))
    assert rows(q.history(day, day, source=dm.HistorySource.REST, wait=True)) == []
    week_warning, day_warning = stamp_warnings(caplog)
    assert week_warning.startswith("indices.bars: a bar of I:SPX from ")
    assert " is stamped 2025-06-01T20:00:00+00:00 " in week_warning
    assert day_warning.startswith(
        "indices.bars: a 1-day bar of I:SPX from /v2/aggs/ticker/{indicesTicker}/range/"
    )
    assert (
        " is stamped 2025-06-02T06:00:00+00:00 (2025-06-02T02:00:00-04:00 in America/New_York), "
        "which is not a stamp Massive is known to use"
    ) in day_warning


@pytest.mark.parametrize(("minutes", "kept"), [(30, 0), (60, 1)], ids=["00:30", "01:00"])
def test_an_index_day_bar_is_kept_once_its_end_has_passed(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    minutes: int,
    kept: int,
) -> None:
    # An index day bar ends at 01:00 New York time the next day, so at 00:30 it is still
    # open and `history()` leaves it out, as a join on its end would.
    now = datetime.fromtimestamp((MIDNIGHT_MS + DAY_MS) / 1000 + minutes * 60, UTC)
    monkeypatch.setattr(history_source, "_now", lambda: now)
    day = date(2025, 6, 2)
    # An open end reads through the end of today.
    path = f"/v2/aggs/ticker/I:SPX/range/1/day/{MIDNIGHT_MS}/{MIDNIGHT_MS + 2 * DAY_MS - 1}"
    fake_rest.reply(quote(path), bars_reply(MIDNIGHT_MS + HOUR_MS))
    q = rest_session.indices.bars("I:SPX", dm.Timespan.DAY)
    out = rows(q.history(day, None, source=dm.HistorySource.REST, wait=True))
    assert [p for p, _ in fake_rest.get_queries()] == [path]
    assert [r["TimestampEnd"] for r in out] == [(MIDNIGHT_MS + DAY_MS + HOUR_MS) * MS] * kept


def test_daily_open_close_fetches_each_index_for_the_day(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    spx = json.loads(load_fixture(OWNER, "open_close.json"))
    ndx = {**spx, "symbol": "I:NDX", "close": 21491.1}
    del ndx["afterHours"]
    fake_rest.reply(quote("/v1/open-close/I:SPX/2025-06-02"), Reply(spx))
    fake_rest.reply(quote("/v1/open-close/I:NDX/2025-06-02"), Reply(ndx))
    out = rows(
        rest_session.indices.daily_open_close(["I:SPX", "I:NDX"], "2025-06-02").fetch(wait=True)
    )
    assert sorted(path for path, _ in fake_rest.get_queries()) == [
        "/v1/open-close/I:NDX/2025-06-02",
        "/v1/open-close/I:SPX/2025-06-02",
    ]
    # The day is the row's `Date`; its window is the New York day, ending an hour late.
    day = {
        "Date": "2025-06-02",
        "TimestampStart": MIDNIGHT_MS * MS,
        "TimestampEnd": (MIDNIGHT_MS + DAY_MS + HOUR_MS) * MS,
        "RawSymbol": None,
        "High": 5940.35,
        "Low": 5876.0,
        "Open": 5896.68,
    }
    assert sorted(out, key=lambda r: r["Symbol"]) == [
        {"Symbol": "I:NDX", "AfterHours": None, "Close": 21491.1, **day, "PreMarket": 5896.68},
        {"Symbol": "I:SPX", "AfterHours": 5937.1, "Close": 5935.94, **day, "PreMarket": 5896.68},
    ]


# --- End to end: WebSocket --------------------------------------------------------------------


def wait_for(condition: Callable[[], bool]) -> None:
    # Bounded wait for the flush thread and an update-graph cycle to apply a change.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(500):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


@pytest.mark.parametrize(
    ("fixture", "topic", "expected"),
    [
        (
            "ws_values.json",
            "V.I:SPX",
            {
                "Symbol": "I:SPX",
                "Date": "2025-06-02",
                "Timestamp": 1748894399000 * MS,
                "RawSymbol": None,
                "Value": 5935.94,
            },
        ),
        (
            "ws_minute_bars.json",
            "AM.I:SPX",
            {
                "Symbol": "I:SPX",
                "Date": "2025-06-02",
                "TimestampStart": 1748894340000 * MS,
                "TimestampEnd": 1748894400000 * MS,
                "Open": 5930.1,
                "Close": 5935.94,
                "High": 5936.2,
                "Low": 5929.8,
                "OfficialOpen": 5896.68,
            },
        ),
        (
            "ws_second_bars.json",
            "A.I:SPX",
            {
                "Symbol": "I:SPX",
                "Date": "2025-06-02",
                "TimestampStart": 1748894399000 * MS,
                # A one-second bar ends a second after its start, whatever its `e` says.
                "TimestampEnd": 1748894400000 * MS,
                "Open": 5935.5,
                "Close": 5935.94,
                "High": 5936.0,
                "Low": 5935.4,
                "OfficialOpen": 5896.68,
            },
        ),
    ],
    ids=["values", "minute-bars", "second-bars"],
)
def test_a_frame_lands_in_blink(
    *,
    ws_session: WsSession,
    load_fixture: Callable[[str, str], bytes],
    fixture: str,
    topic: str,
    expected: dict[str, object],
) -> None:
    indices = ws_session.session.indices
    if topic.startswith("V."):
        blink = indices.values("I:SPX").blink()
    elif topic.startswith("A."):
        blink = indices.bars("I:SPX", dm.Timespan.SECOND).blink()
    else:
        blink = indices.bars("I:SPX", dm.Timespan.MINUTE).blink()
    seen = blink_to_append_only(blink)
    ws_session.subscribed(topic)
    ws_session.publish(*json.loads(load_fixture(OWNER, fixture)))
    # Wait on a consistent snapshot of the rows, never on a size read in the middle of an
    # update cycle; the table is append-only, so the row stays for the read below.
    wait_for(lambda: len(rows(seen)) == 1)
    assert (rows(seen) if topic.startswith("V.") else bar_rows(seen)) == [expected]


def test_a_refused_values_topic_names_the_indices_host_setting(
    *,
    ws_session: WsSession,
    fake_ws_server: FakeMassiveServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failures: list[dm.MassiveError] = []
    fail = stream.Flusher.fail

    def recording(flusher: stream.Flusher, ids: Iterable[int], error: dm.MassiveError) -> None:
        failures.append(error)
        fail(flusher, ids, error)

    monkeypatch.setattr(stream.Flusher, "fail", recording)
    fake_ws_server.refuse["V.I:SPX"] = "not authorized"
    m = ws_session.session
    result = m.indices.values("I:SPX").blink()

    def error() -> str | None:
        ws_session.flush()  # a refused topic fails its streams at the next flush
        assert m._core.diagnostics.wait_until_shown(15.0)
        streams = rows(m.streams())
        failed = [s["Error"] for s in streams if s["State"] == "FAILED"]
        return failed[0] if failed else None

    wait_for(lambda: error() is not None)
    message = str(error())
    assert (
        "socket.test/indices refused the subscription to V.I:SPX; Massive said: not authorized"
    ) in message
    assert "MassiveConfig.websocket_hosts[AssetClass.INDICES]" in message
    assert "business.massive.com" in message
    assert [type(error) for error in failures] == [dm.EntitlementError]
    del result


# --- End to end: flat files -------------------------------------------------------------------


@pytest.fixture
def s3_session(fake_s3: FakeS3) -> Iterator[dm.MassiveSession]:
    config = dm.MassiveConfig(
        api_key="test-key",
        s3_access_key_id="AKIDFLATFILES",
        s3_secret_access_key="flat-file-secret",  # noqa: S106  # fake credential
    )
    session = dm.MassiveSession(config)
    yield session
    session.close()


@pytest.mark.parametrize(
    ("prefix", "fixture", "expected"),
    [
        (
            "us_indices/values_v1",
            "us_indices_values_v1_2025-06-02.csv",
            [
                ("I:SPX", "2025-06-02", 1748871000 * NS, None, 5896.68),
                ("I:SPX", "2025-06-02", 1748871001 * NS, None, 5897.12),
            ],
        ),
        (
            "us_indices/minute_aggs_v1",
            "us_indices_minute_aggs_v1_2025-06-02.csv",
            [("I:SPX", "2025-06-02", 1748871000 * NS, 1748871060 * NS, 5899.02)],
        ),
        (
            # The file stamps the day at midnight in Chicago, 01:00 New York time, as Massive's
            # sample does; the bar starts at the New York day's start and ends an hour late.
            "us_indices/day_aggs_v1",
            "us_indices_day_aggs_v1_2025-06-02.csv",
            [
                (
                    "I:SPX",
                    "2025-06-02",
                    MIDNIGHT_MS * MS,
                    (MIDNIGHT_MS + DAY_MS + HOUR_MS) * MS,
                    5935.94,
                )
            ],
        ),
    ],
    ids=["values", "minute-bars", "day-bars"],
)
def test_one_flat_file_round_trips(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    load_fixture: Callable[[str, str], bytes],
    make_csv_gz: Callable[[str], bytes],
    prefix: str,
    fixture: str,
    expected: list[tuple[object, ...]],
) -> None:
    text = load_fixture(OWNER, fixture).decode()
    fake_s3.put(f"{prefix}/2025/06/2025-06-02.csv.gz", make_csv_gz(text))
    q: HistoryQuery
    if prefix == "us_indices/values_v1":
        q, value = s3_session.indices.values(["I:SPX"]), "Value"
    elif prefix == "us_indices/day_aggs_v1":
        q, value = s3_session.indices.bars(["I:SPX"], dm.Timespan.DAY), "Close"
    else:
        q, value = s3_session.indices.bars(["I:SPX"], dm.Timespan.MINUTE), "Close"
    out = rows(q.history("2025-06-02", "2025-06-02", source=dm.HistorySource.FLAT_FILES, wait=True))
    time = "Timestamp" if value == "Value" else "TimestampStart"
    assert [
        (r["Symbol"], r["Date"], r[time], r.get("TimestampEnd"), r[value]) for r in out
    ] == expected


def test_every_index_from_a_day_file_at_an_unexpected_stamp_is_served_with_a_warning(
    *,
    s3_session: dm.MassiveSession,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    odd = (MIDNIGHT_MS + 2 * 3_600_000) * MS  # 02:00 New York time
    text = f"ticker,open,close,high,low,window_start\nI:SPX,1,2,3,0.5,{odd}\n"
    fake_s3.put("us_indices/day_aggs_v1/2025/06/2025-06-02.csv.gz", make_csv_gz(text))
    q = s3_session.indices.bars(dm.ALL, dm.Timespan.DAY)
    empty = "ticker,open,close,high,low,window_start\n"
    fake_s3.put("us_indices/day_aggs_v1/2025/06/2025-06-03.csv.gz", make_csv_gz(empty))
    out = rows(q.history("2025-06-02", "2025-06-03", source=dm.HistorySource.FLAT_FILES, wait=True))
    # The bar keeps its stamp, ends a day later, and has no Date; the range holds that window.
    assert [(r["Symbol"], r["Date"], r["TimestampStart"], r["TimestampEnd"]) for r in out] == [
        ("I:SPX", None, odd, odd + 86_400 * 10**9)
    ]
    [warning] = stamp_warnings(caplog)
    assert warning.startswith(
        "indices.bars: a 1-day bar of I:SPX from the flat files us_indices/day_aggs_v1 is "
        "stamped 2025-06-02T06:00:00+00:00"
    )


def test_the_current_state_datasets_revise_on_their_update_time() -> None:
    # An explicit APPEND poll appends each new state of an index, or each new day's bar.
    revised = {spec.name: spec.revision_keys for spec in indices.DATASETS if spec.revision_keys}
    assert revised == {
        "indices.previous_day_bar": ("TimestampStart",),
        "indices.snapshots": ("LastUpdated",),
    }
    assert "LastUpdated" in CATALOG.dataset_schema(indices.SNAPSHOTS).column_names
