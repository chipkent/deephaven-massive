"""Tests for `m.snapshots()`: the unified snapshot's dataset, which the session serves itself."""

import json
from collections.abc import Callable
from typing import Any, cast

from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.source_columns import required_fields
from deephaven_massive._enums import ClassFromPrefix
from deephaven_massive._massive import RestPath, Transport, source, to_batch
from deephaven_massive._namespaces import snapshots
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply, envelope

OWNER = "namespaces/snapshots"
R = Transport.REST
Fixture = Callable[[str, str], bytes]
ToRows = Callable[[Table], list[dict[str, Any]]]


def vendor_sample(load_fixture: Fixture) -> dict[str, Any]:
    # The vendor sample, without its `next_url`, which names Massive's host.
    body = cast("dict[str, Any]", json.loads(load_fixture(OWNER, "snapshots.json")))
    body.pop("next_url", None)
    return body


def test_the_module_declares_the_one_session_dataset() -> None:
    spec = snapshots.SNAPSHOTS
    assert (spec,) == snapshots.DATASETS
    assert CATALOG.by_name["snapshots"] is spec
    assert spec.asset_class is ClassFromPrefix.ANY
    assert spec.kind is None
    assert spec.option_rows
    assert spec.fetch_path == "/v3/snapshot"
    assert spec.symbol_param == "ticker.any_of"
    assert spec.page_size == spec.symbols_per_request == 250
    assert not spec.per_symbol
    # An explicit APPEND poll appends each new state of a ticker, of any class.
    assert spec.revision_keys == ("LastUpdated", "LastQuoteLastUpdated", "LastTradeLastUpdated")


def test_the_schema_has_the_options_columns_of_the_options_snapshot() -> None:
    schema = CATALOG.dataset_schema(snapshots.SNAPSHOTS)
    types = {c.name: c.type for c in schema.columns}
    assert schema.column_names[:2] == ("Symbol", "OptionRoot")
    # The underlying is `Underlying`, as on `m.options.snapshots`; no other name holds it.
    assert types["Underlying"] is ColumnType.STRING
    assert "UnderlyingAssetTicker" not in types
    root = next(c for c in schema.columns if c.name == "OptionRoot")
    assert root.description.endswith("Null on a row of any other class.")
    assert types["LastTradeConditions"] is ColumnType.LONG_ARRAY
    assert types["LastQuoteAskExchange"] is ColumnType.LONG
    assert types["LastUpdated"] is ColumnType.INSTANT
    assert {"Error", "Message"} <= set(types)
    assert set(snapshots.SNAPSHOTS.revision_keys) <= set(types)


def test_the_summary_opens_the_docstring() -> None:
    doc = cast("str", dm.MassiveSession.snapshots.__doc__)
    assert doc.splitlines()[0] == snapshots.SNAPSHOTS.summary


def test_the_vendor_sample_decodes(load_fixture: Fixture) -> None:
    rest_path = cast("RestPath", source(R, "/v3/snapshot"))
    rows = vendor_sample(load_fixture)["results"]
    required = [f.column for f in required_fields(snapshots.SNAPSHOTS, rest_path)]
    batch, report = to_batch(rows, rest_path, required=required)
    # The vendor's sample sends `details.underlying_ticker`, which its schema lacks.
    assert tuple(sorted(report.extra)) == ("details.underlying_ticker",)
    assert not report.missing
    first = batch.to_pylist()[0]
    assert first["Ticker"] == "O:NCLH221014C00005000"
    assert first["UnderlyingAssetTicker"] == "AAPL"
    assert first["LastTradeConditions"] == [209]


def test_fetch_fills_the_underlying_and_root_of_options_rows_only(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    dh_to_pylist: ToRows,
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(vendor_sample(load_fixture)))
    symbols = ["O:NCLH221014C00005000", "AAPL", "TSLAAPL"]
    out = dh_to_pylist(rest_session.snapshots(symbols).fetch())
    assert fake_rest.get_queries() == [
        (
            "/v3/snapshot",
            {"ticker.any_of": ["AAPL,O:NCLH221014C00005000,TSLAAPL"], "limit": ["250"]},
        )
    ]
    by_symbol = {r["Symbol"]: r for r in out}
    assert set(by_symbol) == set(symbols)
    option = by_symbol["O:NCLH221014C00005000"]
    assert (option["Underlying"], option["OptionRoot"]) == ("AAPL", "NCLH")
    assert option["LastTradeConditions"] == [209]
    assert option["LastTradeSipTimestamp"].value == 1675280958783136800  # every nanosecond
    # A stock row has neither, and a ticker Massive does not know keeps its row, with the error.
    assert (by_symbol["AAPL"]["Underlying"], by_symbol["AAPL"]["OptionRoot"]) == (None, None)
    assert by_symbol["TSLAAPL"]["Error"] == "NOT_FOUND"


def test_fetch_sends_250_tickers_a_request(
    rest_session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/snapshot", Reply(envelope([])))
    symbols = [f"T{i:03d}" for i in range(251)]
    rest_session.snapshots(symbols).fetch()
    batches = [q["ticker.any_of"][0].split(",") for _, q in fake_rest.get_queries()]
    assert sorted(len(batch) for batch in batches) == [1, 250]
    assert sorted(s for batch in batches for s in batch) == symbols
