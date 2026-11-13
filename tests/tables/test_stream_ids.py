"""Tests for the process's one allocator of stream ids."""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import SymbolStreamQuery

if TYPE_CHECKING:
    # Type only: the instance comes from the `fake_streams` fixture.

    import deephaven_massive as dm
    from tests._fakes.stream import FakeStreamTransport

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
)


def stream_query(
    session: dm.MassiveSession, symbols: dm.SymbolsLike = ("AAPL",)
) -> SymbolStreamQuery:
    return build_query(session._core, SPEC, query_type=SymbolStreamQuery, symbols=symbols)


def test_stream_ids_come_from_one_allocator(
    massive_session: dm.MassiveSession, fake_streams: FakeStreamTransport
) -> None:
    core = massive_session._core
    assert core.next_stream_id() == 1  # the id a poll() row takes
    result = stream_query(massive_session).blink()
    assert list(core.streams._streams) == [2]  # the WebSocket stream never reuses it
    assert core.next_stream_id() == 3
    massive_session.release(result)
