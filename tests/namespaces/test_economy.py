"""Tests for the `m.economy` namespace."""

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.request import Request
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._massive import Transport, source, to_batch
from deephaven_massive._namespaces import economy
from deephaven_massive._namespaces.base import Namespace
from deephaven_massive._query import Query
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply

OWNER = "namespaces/economy"


@dataclass(frozen=True)
class Series:
    # One dataset: its factory, spec, and path, and the first row of the vendor's sample.
    method: str
    spec: DatasetSpec
    path: str
    day: date
    column: str
    value: float


SERIES = [
    Series(
        "treasury_yields",
        economy.TREASURY_YIELDS,
        "/fed/v1/treasury-yields",
        date(1962, 1, 2),
        "Yield10Year",
        4.06,
    ),
    Series("inflation", economy.INFLATION, "/fed/v1/inflation", date(2025, 6, 1), "Cpi", 310.45),
    Series(
        "inflation_expectations",
        economy.INFLATION_EXPECTATIONS,
        "/fed/v1/inflation-expectations",
        date(2025, 6, 17),
        "ForwardYears5To10",
        2.6,
    ),
    Series(
        "labor_market",
        economy.LABOR_MARKET,
        "/fed/v1/labor-market",
        date(2024, 12, 1),
        "UnemploymentRate",
        4.2,
    ),
    Series(
        "funding_conditions",
        economy.FUNDING_CONDITIONS,
        "/fed/v1/funding-conditions",
        date(2025, 6, 2),
        "SecuredOvernightFinancingRate",
        4.31,
    ),
]
EACH = pytest.mark.parametrize("series", SERIES, ids=[s.method for s in SERIES])


def test_namespace_is_reached_from_the_session(massive_session: dm.MassiveSession) -> None:
    namespace = massive_session.economy
    assert type(namespace) is economy._Economy
    assert isinstance(namespace, Namespace)


def test_datasets_are_declared_by_the_namespace_package() -> None:
    assert tuple(s.spec for s in SERIES) == economy.DATASETS
    assert [spec.name for spec in economy.DATASETS] == [f"economy.{s.method}" for s in SERIES]
    assert all(CATALOG.by_name[spec.name] is spec for spec in economy.DATASETS)


@EACH
def test_each_series_is_a_date_axis_list_endpoint(series: Series) -> None:
    spec = series.spec
    assert spec.fetch_path == series.path
    assert spec.sources == ((Transport.REST, series.path),)
    assert spec.time_axis == ("date.gte", "date.lte")
    assert spec.keys == {}
    assert spec.poll_mode is dm.PollMode.REPLACE
    assert spec.poll_keys == ("Date",)
    assert spec.page_size == 50_000
    schema = CATALOG.dataset_schema(spec)
    assert "Symbol" not in schema.column_names
    assert "Timestamp" not in schema.column_names
    types = {c.name: c.type for c in schema.columns}
    assert types["Date"] is ColumnType.LOCAL_DATE
    assert types[series.column] is ColumnType.DOUBLE


@EACH
def test_each_factory_builds_its_query(massive_session: dm.MassiveSession, series: Series) -> None:
    factory = getattr(massive_session.economy, series.method)
    query = factory(start="2025-01-01", end=date(2025, 6, 30), limit=10)
    assert type(query) is Query
    assert query._spec is series.spec
    assert query._request == Request(start=date(2025, 1, 1), end=date(2025, 6, 30), limit=10)
    assert repr(query) == (
        f"<Query economy.{series.method} start=2025-01-01 end=2025-06-30 limit=10>"
    )
    assert factory()._request == Request()


@EACH
def test_the_vendor_sample_decodes_without_drift(
    load_fixture: Callable[[str, str], bytes], series: Series
) -> None:
    payload = json.loads(load_fixture(OWNER, f"{series.method}.json"))
    rest_path = source(Transport.REST, series.path)
    batch, drift = to_batch(payload["results"], rest_path, required=())
    assert drift.extra == drift.missing == frozenset()
    row = batch.to_pylist()[0]
    assert row["Date"] == series.day
    assert row[series.column] == series.value


def test_sort_and_order_are_kept_for_the_rest_layer(massive_session: dm.MassiveSession) -> None:
    # The endpoints have no `order`; the REST layer folds it into `sort` as its suffix.
    query = massive_session.economy.inflation(sort="date", order=dm.Order.DESC)
    assert query._request.params == (("order", dm.Order.DESC), ("sort", "date"))


@EACH
def test_the_summary_opens_the_docstring(series: Series) -> None:
    doc = getattr(economy._Economy, series.method).__doc__
    assert doc.splitlines()[0] == series.spec.summary


def test_enums_reject_text(massive_session: dm.MassiveSession) -> None:
    for series in SERIES:
        factory = getattr(massive_session.economy, series.method)
        with pytest.raises(TypeError, match=r"order must be dm\.Order"):
            factory(order=cast("dm.Order", "desc"))


def test_arguments_are_validated_at_construction(massive_session: dm.MassiveSession) -> None:
    e = massive_session.economy
    with pytest.raises(ValueError, match="date_gte is set through start= and end="):
        e.labor_market(filters={"date_gte": "2025-01-01"})
    with pytest.raises(ValueError, match="has no filter 'nope'"):
        e.labor_market(filters={"nope": 1})
    with pytest.raises(ValueError, match="end"):
        e.labor_market(start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError, match="limit must be positive"):
        e.labor_market(limit=0)
    with pytest.raises(TypeError, match="sort must be a str"):
        e.labor_market(sort=cast("str", 1))


# --- End to end through the fake REST server -------------------------------------------------


@pytest.fixture
def rest_session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


@EACH
def test_each_series_fetches_its_vendor_sample(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Callable[[str, str], bytes],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    series: Series,
) -> None:
    # The documentation samples write `request_id` as the number 1; the client needs text.
    body = json.loads(load_fixture(OWNER, f"{series.method}.json"))
    body["request_id"] = str(body["request_id"])
    fake_rest.reply(series.path, Reply(body))
    factory = getattr(rest_session.economy, series.method)
    out = dh_to_pylist(
        factory(
            start="2020-01-01", end=date(2025, 12, 31), sort="date", order=dm.Order.DESC
        ).fetch()
    )
    (request,) = fake_rest.requests
    assert urlsplit(request.target).path == series.path
    # The endpoints have no `order`: its direction becomes the suffix of `sort`.
    assert parse_qs(urlsplit(request.target).query) == {
        "date.gte": ["2020-01-01"],
        "date.lte": ["2025-12-31"],
        "sort": ["date.desc"],
        "limit": ["50000"],
    }
    assert len(out) == 1
    assert out[0]["Date"] == series.day
    assert out[0][series.column] == series.value
