"""Tests that the Python examples of `README.md` run, against the fake REST and WebSocket servers.

Each example block of a README section runs, in order, in one namespace, as a reader would
paste them into the Deephaven console one after another. `dm.MassiveSession()` builds a session
whose REST client talks to the fake REST server and whose streams connect to the fake
WebSocket server, so nothing reaches Massive; the fakes answer each request the examples make
with Massive's documented shapes, holding NVDA's 10-for-1 split of 2024-06-10. The tests then
check that the tables hold what the README says they hold, and call the functions it defines.
"""

import json
import re
import runpy
import threading
import types
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._enums import Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Source, WsClient
from tests._fakes.rest import FakeRestServer, Reply, envelope
from tests._fakes.ws import FakeMassiveServer

README = Path(__file__).resolve().parent.parent / "README.md"

_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)


def _section_blocks(title: str) -> list[str]:
    """Return the Python blocks of one `## ` section of the README, in order.

    Args:
        title: The section's heading text, such as `"Split adjustment"`.

    Returns:
        The code of each block.
    """
    text = README.read_text(encoding="utf-8")
    start = text.index(f"## {title}\n")
    end = text.find("\n## ", start + 1)
    return _BLOCK.findall(text[start : end if end >= 0 else len(text)])


def _ms(text: str) -> int:
    """Return an ISO-8601 UTC instant as epoch milliseconds."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def _bar(day: str, close: float, volume: float) -> dict[str, object]:
    """Return one aggregates row of NVDA for a New York day."""
    return {
        "t": _ms(f"{day}T04:00:00"),  # 00:00 New York time, daylight saving
        "o": close,
        "h": close,
        "l": close,
        "c": close,
        "v": volume,
        "vw": close,
        "n": 1,
    }


def _no_crypto_pair(symbol: str) -> tuple[str, str]:
    """Refuse a crypto lookup: the README's examples stream no crypto pair.

    Raises:
        AssertionError: Always, naming the pair.
    """
    raise AssertionError(f"the README's examples stream no crypto pair, but asked for {symbol}")


@pytest.fixture
def readme_server(
    fake_rest: FakeRestServer, fake_ws_server: FakeMassiveServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[FakeRestServer]:
    """Make `dm.MassiveSession()` open a session on the fake servers, closed afterwards.

    Yields:
        The fake REST server the examples' session talks to, for scripting its replies; its
        streams connect to `fake_ws_server`.
    """
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url, flush_interval=0.01)
    session = dm.MassiveSession(config)

    def client(
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[..., None],
        on_status: Callable[..., None],
        on_data: Callable[..., None],
        on_state: Callable[..., None],
    ) -> WsClient:
        return WsClient(
            fake_ws_server.url("/" + url.split("/", 3)[3]),
            api_key="test-key",
            sources=sources,
            on_open=on_open,
            on_status=on_status,
            on_data=on_data,
            on_state=on_state,
            reconnect_backoff=0.05,
            max_reconnect_backoff=0.1,
        )

    connections = ws.WsConnections(
        session._core,
        resolve_host=lambda *_: HostDecision("socket.test", Recency.REALTIME, "test"),
        record_verdict=lambda *_: None,
        fetch_rows=lambda *_, **__: [],
        crypto_pair=_no_crypto_pair,
        client_factory=client,
    )
    cast("Any", session._core._ports)._streams = connections
    session._core.on_close(connections.close)

    def opened(*_args: object, **_kwargs: object) -> dm.MassiveSession:
        return session

    monkeypatch.setattr(dm, "MassiveSession", opened)
    yield fake_rest
    session.close()


@pytest.mark.jvm_held  # the scripts' formulas hand their globals, the session too, to the JVM
def test_the_split_adjustment_examples_run(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The whole New York days of 2024-06-03 to 2024-06-14, as the range path's `{from}/{to}`.
    path = (
        f"/v2/aggs/ticker/NVDA/range/1/day/{_ms('2024-06-03T04:00:00')}/"
        f"{_ms('2024-06-15T04:00:00') - 1}"
    )
    readme_server.reply(
        path,
        Reply(
            envelope(
                [_bar("2024-06-07", 120.888, 412_385_850), _bar("2024-06-10", 121.79, 314_162_700)],
                adjusted=True,
                ticker="NVDA",
            )
        ),
        Reply(
            envelope(
                [_bar("2024-06-07", 1208.88, 41_238_585), _bar("2024-06-10", 121.79, 314_162_700)],
                adjusted=False,
                ticker="NVDA",
            )
        ),
    )
    readme_server.reply(
        "/stocks/v1/splits",
        Reply(
            envelope(
                [
                    {
                        "adjustment_type": "forward_split",
                        "execution_date": "2024-06-10",
                        "historical_adjustment_factor": 0.1,
                        "id": "E1",
                        "split_from": 1,
                        "split_to": 10,
                        "ticker": "NVDA",
                    }
                ]
            )
        ),
    )
    blocks = _section_blocks("Split adjustment")
    assert len(blocks) == 3
    names: dict[str, Any] = {}
    for number, block in enumerate(blocks):
        # Run as a script, each block seeing what the blocks before it defined.
        script = tmp_path / f"split_adjustment_{number}.py"
        script.write_text(block, encoding="utf-8")
        names = runpy.run_path(str(script), init_globals=names)

    adjusted = {r["Date"]: r for r in dh_to_pylist(names["adjusted"])}
    traded = {r["Date"]: r for r in dh_to_pylist(names["traded"])}
    mine = {r["Date"]: r for r in dh_to_pylist(names["mine"])}
    assert adjusted["2024-06-07"]["Close"] == pytest.approx(120.888)
    assert traded["2024-06-07"]["Close"] == pytest.approx(1208.88)
    # The recipe turns the as-traded day before the split into the adjusted one.
    assert mine["2024-06-07"]["AdjustedClose"] == pytest.approx(120.888)
    assert mine["2024-06-07"]["AdjustedVolume"] == pytest.approx(412_385_850)
    # No split executes after the split's own day, so its factor is 1.
    assert mine["2024-06-10"]["Factor"] == 1.0
    assert mine["2024-06-10"]["AdjustedClose"] == pytest.approx(121.79)
    assert [r.target.partition("?")[2] for r in readme_server.requests][:2] == [
        "adjusted=true&limit=50000&sort=asc",
        "adjusted=false&limit=50000&sort=asc",
    ]
    _history_and_live_merges_history_with_live_bars(
        readme_server=readme_server,
        fake_ws_server=fake_ws_server,
        history_and_live=names["history_and_live"],
        dh_to_pylist=dh_to_pylist,
        monkeypatch=monkeypatch,
    )
    # The scripts' functions hold their module globals, which hold the functions and the
    # session: a cycle, which clearing those globals breaks, so the session goes with the test.
    for value in names.values():  # a function the scripts defined, not one they imported
        if isinstance(value, types.FunctionType) and value.__module__ == "<run_path>":
            value.__globals__.clear()
    names.clear()


def _history_and_live_merges_history_with_live_bars(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    history_and_live: Callable[[str, str], Table],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 2024-06-14 at 15:00 New York time: history reads that day's finished minutes over REST,
    # then the stream appends the live ones.
    now = datetime(2024, 6, 14, 19, tzinfo=UTC)
    monkeypatch.setattr(history_source, "_now", lambda: now)
    opened = _ms("2024-06-14T13:30:00")  # 09:30 New York time
    # An open end reads through the end of today; the bars not yet ended are dropped.
    path = (
        f"/v2/aggs/ticker/NVDA/range/1/minute/{_ms('2024-06-14T04:00:00')}/"
        f"{_ms('2024-06-15T04:00:00') - 1}"
    )
    minute = {**_bar("2024-06-14", 131.0, 1_000), "t": opened}
    readme_server.reply(path, Reply(envelope([minute], adjusted=True, ticker="NVDA")))
    table = history_and_live("NVDA", "2024-06-14")
    fake_ws_server.wait_for(
        lambda server: any(
            "AM.NVDA" in json.loads(frame).get("params", "")
            for connection in server.connections
            for frame in connection.frames
        )
    )
    live = {"ev": "AM", "sym": "NVDA", "v": 5, "o": 132.0, "c": 132.0, "h": 132.0, "l": 132.0}
    live |= {"s": _ms("2024-06-14T19:00:00"), "e": _ms("2024-06-14T19:01:00")}
    fake_ws_server.send(json.dumps([live]))
    # Bounded wait for the flush thread and an update-graph cycle to append the live bar.
    pace = threading.Event()  # never set; only paces the loop
    rows = dh_to_pylist(table)
    for _ in range(500):
        if len(rows) == 2:
            break
        pace.wait(0.02)
        rows = dh_to_pylist(table)
    assert [(r["Symbol"], r["Close"]) for r in rows] == [("NVDA", 131.0), ("NVDA", 132.0)]
    assert [r["TimestampStart"] for r in rows] == [
        datetime(2024, 6, 14, 13, 30, tzinfo=UTC),
        datetime(2024, 6, 14, 19, tzinfo=UTC),
    ]
