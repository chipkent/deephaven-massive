"""Tests for the `m.indices` namespace: its dataset declarations, schemas, and factory methods.

The fixtures under `tests/data/namespaces/indices/` are shaped from the vendor's examples for
each source the namespace reads; a test checks that every key they carry is a generated field
of that source, so no fixture can invent a wire key.
"""

import csv
import io
import json
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._massive import Transport, sources
from deephaven_massive._namespaces import indices
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
)
from deephaven_massive._session import CATALOG

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
    assert type(namespace) is indices._Indices
    assert isinstance(namespace, Namespace)


def test_every_dataset_is_declared_once_in_the_catalog() -> None:
    names = [spec.name for spec in indices.DATASETS]
    assert names == [
        "indices.values",
        "indices.bars",
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
        (rest, SNAPSHOT),
        (ws, "indices/V"),
        (files, "us_indices/values_v1"),
    )
    assert indices.BARS.sources == (
        (rest, PREVIOUS),
        (rest, RANGE),
        (ws, "indices/A"),
        (ws, "indices/AM"),
        (files, "us_indices/minute_aggs_v1"),
        (files, "us_indices/day_aggs_v1"),
    )
    assert indices.SNAPSHOTS.sources == ((rest, SNAPSHOT),)
    assert indices.DAILY_OPEN_CLOSE.sources == ((rest, OPEN_CLOSE),)


def test_values_history_is_flat_files_only() -> None:
    assert indices.VALUES.has_fetch
    assert not indices.VALUES.has_rest_history
    assert indices.VALUES.flat_file_prefixes == {None: "us_indices/values_v1"}


def _schema(spec: DatasetSpec) -> list[tuple[str, str]]:
    return [(c.name, c.type.value) for c in CATALOG.dataset_schema(spec).columns]


def test_values_schema() -> None:
    # The snapshot joins the values kind with only its ticker, value, and update time.
    assert _schema(indices.VALUES) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("Value", "double"),
    ]


def test_bars_schema() -> None:
    # Index bars come from index values: no volume, no VWAP, no trade count.
    assert _schema(indices.BARS) == [
        ("Symbol", "String"),
        ("Timestamp", "Instant"),
        ("Open", "double"),
        ("Close", "double"),
        ("High", "double"),
        ("Low", "double"),
        ("OfficialOpen", "double"),
        ("EndTimestamp", "Instant"),
    ]


def test_snapshots_schema() -> None:
    # The snapshot in full; its update time keeps its own name, as on every snapshot.
    assert _schema(indices.SNAPSHOTS) == [
        ("Symbol", "String"),
        ("Error", "String"),
        ("LastUpdated", "Instant"),
        ("MarketStatus", "String"),
        ("Message", "String"),
        ("Name", "String"),
        ("SessionChange", "double"),
        ("SessionChangePercent", "double"),
        ("SessionClose", "double"),
        ("SessionHigh", "double"),
        ("SessionLow", "double"),
        ("SessionOpen", "double"),
        ("SessionPreviousClose", "double"),
        ("Timeframe", "String"),
        ("Type", "String"),
        ("Value", "double"),
    ]


def test_daily_open_close_schema() -> None:
    # The day is a calendar date, so the row has no Timestamp.
    assert _schema(indices.DAILY_OPEN_CLOSE) == [
        ("Symbol", "String"),
        ("AfterHours", "double"),
        ("Close", "double"),
        ("From", "LocalDate"),
        ("High", "double"),
        ("Low", "double"),
        ("Open", "double"),
        ("PreMarket", "double"),
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
    ("name", "channel"), [("ws_values.json", "indices/V"), ("ws_minute_bars.json", "indices/AM")]
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
    ],
)
def test_flat_file_fixtures_have_the_generated_header(
    load_fixture: Callable[[str, str], bytes], name: str, prefix: str
) -> None:
    header = next(csv.reader(io.StringIO(load_fixture(OWNER, name).decode())))
    assert header == [f.wire_key for f in sources(Transport.FLAT_FILES)[prefix].fields]


# --- Factory methods --------------------------------------------------------------------------


def test_values_query(m: dm.MassiveSession) -> None:
    q = m.indices.values(["spx", "I:NDX", "I:SPX"])
    assert type(q) is StreamHistoryLatestQuery
    assert q._spec is indices.VALUES
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert repr(q) == "<StreamHistoryLatestQuery indices.values symbols=('I:NDX', 'I:SPX')>"
    assert m.indices.values(dm.ALL)._request.symbols is dm.ALL


def test_values_fetch_needs_explicit_symbols(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"use m\.indices\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        m.indices.values(dm.ALL).fetch()


def test_values_history_has_no_rest_source(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"indices\.values has no REST history"):
        m.indices.values("I:SPX").history("2025-06-02", "2025-06-02", source=dm.HistorySource.REST)


def test_values_history_from_flat_files_needs_credentials(
    monkeypatch: pytest.MonkeyPatch, no_aws_settings: None
) -> None:
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)
    with dm.MassiveSession(dm.MassiveConfig(api_key="test-key")) as session:
        q = session.indices.values("I:SPX")
        for source in (dm.HistorySource.FLAT_FILES, dm.HistorySource.AUTO):
            with pytest.raises(ValueError, match="no S3 credentials are configured"):
                q.history("2025-06-02", "2025-06-02", source=source)


@pytest.mark.parametrize(
    "case",
    [
        (dm.Timespan.SECOND, 1, StreamHistoryQuery),
        (dm.Timespan.MINUTE, 1, StreamHistoryQuery),
        (dm.Timespan.DAY, 1, HistoryLatestQuery),
        (dm.Timespan.MINUTE, 5, HistoryQuery),
        (dm.Timespan.HOUR, 1, HistoryQuery),
        (dm.Timespan.DAY, 2, HistoryQuery),
        (dm.Timespan.WEEK, 1, HistoryQuery),
    ],
)
def test_bars_pick_their_class(m: dm.MassiveSession, case: tuple[dm.Timespan, int, type]) -> None:
    timespan, multiplier, expected = case
    q = m.indices.bars("SPX", timespan, multiplier=multiplier)
    assert type(q) is expected
    assert q._request.symbols == ("I:SPX",)
    assert q._request.timespan is timespan
    assert q._request.multiplier == multiplier
    assert q._request.adjusted is None


def test_bars_default_to_streamed_minutes(m: dm.MassiveSession) -> None:
    q = m.indices.bars(["I:SPX"])
    assert type(q) is StreamHistoryQuery
    assert repr(q) == (
        "<StreamHistoryQuery indices.bars symbols=('I:SPX',) timespan=Timespan.MINUTE>"
    )


def test_bars_reject_the_futures_session(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        m.indices.bars("I:SPX", dm.Timespan.SESSION)


def test_bars_have_no_adjusted_parameter(m: dm.MassiveSession) -> None:
    bars = cast("Callable[..., object]", m.indices.bars)
    with pytest.raises(TypeError, match="adjusted"):
        bars("I:SPX", adjusted=False)


def test_bars_check_their_argument_types(m: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan"):
        m.indices.bars("I:SPX", cast("dm.Timespan", "minute"))
    with pytest.raises(ValueError, match="multiplier must be positive"):
        m.indices.bars("I:SPX", dm.Timespan.MINUTE, multiplier=0)


def test_previous_day_bars_need_explicit_symbols(m: dm.MassiveSession) -> None:
    q = m.indices.bars(dm.ALL, dm.Timespan.DAY)
    assert type(q) is HistoryLatestQuery
    with pytest.raises(ValueError, match=r"use m\.indices\.snapshots\(dm\.ALL\)\.fetch\(\)"):
        q.fetch()


def test_bars_history_over_rest_needs_explicit_symbols(m: dm.MassiveSession) -> None:
    # Indices have no grouped-daily endpoint, so not even daily bars serve dm.ALL over REST.
    for timespan in (dm.Timespan.MINUTE, dm.Timespan.DAY):
        with pytest.raises(ValueError, match=r"indices\.bars history over REST needs explicit"):
            m.indices.bars(dm.ALL, timespan).history(
                "2025-06-02", "2025-06-02", source=dm.HistorySource.REST
            )


def test_bar_flat_files_hold_only_minutes_and_days(m: dm.MassiveSession) -> None:
    q = m.indices.bars("I:SPX", dm.Timespan.HOUR)
    with pytest.raises(ValueError, match="flat files hold only 1-minute, 1-day bars"):
        q.history("2025-06-02", "2025-06-02", source=dm.HistorySource.FLAT_FILES)


def test_snapshots_query(m: dm.MassiveSession) -> None:
    q = m.indices.snapshots(
        ["I:SPX", "ndx"],
        sort="ticker",
        order=dm.Order.DESC,
        limit=10,
        filters={"ticker_gte": "I:A"},
    )
    assert type(q) is Query
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert dict(q._request.params) == {"order": dm.Order.DESC, "sort": "ticker"}
    assert dict(q._request.filters) == {"ticker_gte": "I:A"}
    assert q._request.limit == 10
    assert m.indices.snapshots(dm.ALL)._request.symbols is dm.ALL


def test_snapshots_filters_are_the_snapshot_parameters(m: dm.MassiveSession) -> None:
    # The equality forms of the symbol parameter are set through symbols only.
    for key in ("ticker", "ticker_any_of", "timestamp_gte"):
        with pytest.raises(ValueError, match=f"has no filter '{key}'"):
            m.indices.snapshots("I:SPX", filters={key: "I:A"})
    with pytest.raises(ValueError, match="'ticker_lt' takes a string value"):
        m.indices.snapshots("I:SPX", filters={"ticker_lt": 5})


def test_snapshots_check_their_list_parameters(m: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        m.indices.snapshots("I:SPX", order=cast("dm.Order", "asc"))
    with pytest.raises(ValueError, match="limit must be positive"):
        m.indices.snapshots("I:SPX", limit=0)


@pytest.mark.parametrize(
    "day",
    [
        "2025-06-02",
        date(2025, 6, 2),
        datetime(2025, 6, 2, 15, 30, tzinfo=UTC),
        # 02:00 UTC on June 3 is still June 2 in New York.
        "2025-06-03T02:00:00+00:00",
    ],
)
def test_daily_open_close_query(m: dm.MassiveSession, day: str | date) -> None:
    q = m.indices.daily_open_close(["I:SPX", "NDX"], day)
    assert type(q) is Query
    assert q._request.symbols == ("I:NDX", "I:SPX")
    assert dict(q._request.params) == {"date": date(2025, 6, 2)}


def test_daily_open_close_rejects_all(m: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"use m\.indices\.snapshots\(dm\.ALL\)"):
        m.indices.daily_open_close(dm.ALL, "2025-06-02")


def test_daily_open_close_needs_a_zoned_day(m: dm.MassiveSession) -> None:
    naive = datetime(2025, 6, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(ValueError, match="needs a timezone"):
        m.indices.daily_open_close("I:SPX", naive)
    with pytest.raises(ValueError, match="is not an ISO-8601 date"):
        m.indices.daily_open_close("I:SPX", "June 2")
    with pytest.raises(TypeError, match="date must be a str, date, or datetime"):
        m.indices.daily_open_close("I:SPX", cast("str", 20250602))


@pytest.mark.parametrize("name", ["values", "bars", "snapshots", "daily_open_close"])
def test_symbols_must_be_index_symbols(m: dm.MassiveSession, name: str) -> None:
    method = cast("Callable[..., object]", getattr(m.indices, name))
    extra: tuple[object, ...] = ("2025-06-02",) if name == "daily_open_close" else ()
    with pytest.raises(ValueError, match="carries the prefix of another asset class"):
        method("X:BTCUSD", *extra)
    with pytest.raises(ValueError, match="no symbols"):
        method([], *extra)


def test_factories_return_the_class_of_their_dataset() -> None:
    assert query_class(indices.VALUES) is StreamHistoryLatestQuery
    assert query_class(indices.SNAPSHOTS) is Query
    assert query_class(indices.DAILY_OPEN_CLOSE) is Query
    assert query_class(indices.BARS) is HistoryQuery


@pytest.mark.parametrize("spec", indices.DATASETS, ids=lambda spec: spec.name)
def test_each_docstring_links_its_column_reference(spec: DatasetSpec) -> None:
    dataset = spec.name.split(".", 1)[1]
    doc = getattr(indices._Indices, dataset).__doc__
    assert doc.splitlines()[0] == spec.summary
    assert f"docs/datasets/indices.md#{dataset.replace('.', '-')}," in doc
    assert "`limit`" in doc
    assert "`dm.ALL`" in doc
