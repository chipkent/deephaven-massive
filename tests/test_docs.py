"""Tests of the user docs: the README's examples run, and every link a docstring cites exists.

`README.md` holds all hand-written user documentation, and its Python examples are real code:
every block runs here, in document order, in one namespace, as a reader would paste them into
the Deephaven console one after another, against the fake REST and WebSocket servers, so nothing
reaches Massive. `dm.MassiveSession()` builds a new session whose REST client talks to the fake
REST server and whose streams connect to the fake WebSocket server. Two sections, the first
tables and split adjustment, also run alone against scripted replies (NVDA's 10-for-1 split of
2024-06-10), and the tests check that their tables hold what the README says they hold.

The docstrings link instead of repeating: a shared argument or concept cites a README section as
`README.md#<anchor>`, and a dataset its generated page as `docs/datasets/<dataset>.md`. The
tests check that every such anchor and page exists, that every link inside the README resolves,
and that the README's namespace table lists exactly the catalog's namespaces.
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
from deephaven_massive._enums import Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Source, WsClient
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.transports import HostDecision
from tests._fakes.rest import FakeRestServer, Reply, envelope
from tests._fakes.ws import FakeMassiveServer

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
SOURCE = ROOT / "src" / "deephaven_massive"

_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)
_ANCHOR = re.compile(r'<a id="([a-z0-9-]+)"></a>')
_README_CITATION = re.compile(r"README\.md#([A-Za-z0-9_-]+)")
_PAGE_CITATION = re.compile(r"docs/datasets/([A-Za-z0-9_.]+?)\.md\b")
_LINK = re.compile(r"\]\(([^)\s]+)\)")


def _blocks(text: str) -> list[str]:
    """Return the code of each Python block of a Markdown text, in order."""
    return _BLOCK.findall(text)


def _section_blocks(anchor: str) -> list[str]:
    """Return the Python blocks of one README section, from its anchor to the next anchor.

    Args:
        anchor: The section's `<a id>`, such as `"split-adjustment"`.

    Returns:
        The code of each block, in order.
    """
    text = README.read_text(encoding="utf-8")
    start = text.index(f'<a id="{anchor}"></a>')
    end = text.find("<a id=", start + 1)
    return _blocks(text[start : end if end >= 0 else len(text)])


def _ms(text: str) -> int:
    """Return an ISO-8601 UTC instant as epoch milliseconds."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def _bar(day: str, close: float, volume: float) -> dict[str, object]:
    """Return one aggregates row of a stock for a New York day."""
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
    """Make `dm.MassiveSession()` open a new session on the fake servers, all closed afterwards.

    Each call opens a new session, as the real class does, so an example that closes one and
    opens another sees a fresh session. A configuration the example passes is ignored, since
    the session must talk to the fakes.

    Yields:
        The fake REST server the examples' sessions talk to, for scripting its replies; their
        streams connect to `fake_ws_server`.
    """
    real = dm.MassiveSession
    opened: list[dm.MassiveSession] = []

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

    def session(*_args: object, **_kwargs: object) -> dm.MassiveSession:
        config = dm.MassiveConfig(
            api_key="test-key", rest_base_url=fake_rest.url, flush_interval=0.01
        )
        made = real(config)
        connections = ws.WsConnections(
            made._core,
            resolve_host=lambda *_: HostDecision("socket.test", Recency.REALTIME, "test"),
            record_verdict=lambda *_: None,
            fetch_rows=lambda *_, **__: [],
            crypto_pair=_no_crypto_pair,
            client_factory=client,
        )
        cast("Any", made._core._ports)._streams = connections
        made._core.on_close(connections.close)
        opened.append(made)
        return made

    monkeypatch.setattr(dm, "MassiveSession", session)
    # An example that builds its own configuration reads the key from the environment.
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    yield fake_rest
    for made in opened:
        made.close()
    # The package resolves `MassiveSession` lazily, so the name is not bound in the module;
    # undoing the patch would bind it, so the binding is removed after the undo.
    monkeypatch.undo()
    vars(dm).pop("MassiveSession", None)


def _run_blocks(blocks: Sequence[str], tmp_path: Path, name: str) -> dict[str, Any]:
    """Run code blocks as scripts, in order, each seeing what the blocks before it defined.

    Args:
        blocks: The code of each block.
        tmp_path: Where to write the scripts.
        name: The scripts' file name prefix.

    Returns:
        The names the blocks defined.
    """
    names: dict[str, Any] = {}
    for number, block in enumerate(blocks):
        script = tmp_path / f"{name}_{number}.py"
        script.write_text(block, encoding="utf-8")
        names = runpy.run_path(str(script), init_globals=names)
    return names


def _forget(names: dict[str, Any]) -> None:
    """Break the cycle between the scripts' functions and their globals, then drop the names.

    A function a script defined holds its module globals, which hold the function and the
    session, so the session goes with the test only once those globals are cleared.

    Args:
        names: The names the scripts defined.
    """
    for value in names.values():
        if isinstance(value, types.FunctionType) and value.__module__ == "<run_path>":
            value.__globals__.clear()
    names.clear()


# --- Every example runs ------------------------------------------------------------------------


@pytest.mark.jvm_held  # the scripts' formulas hand their globals, the session too, to the JVM
def test_every_readme_example_runs_in_order(
    *, readme_server: FakeRestServer, tmp_path: Path
) -> None:
    # Every path the examples read answers an empty page, except the futures contracts, which
    # the next example streams, and must name at least one contract.
    readme_server.reply_default(Reply(envelope([])))
    contract = {"ticker": "ESZ6", "product_code": "ES", "date": "2026-10-07", "active": True}
    readme_server.reply("/futures/v1/contracts", Reply(envelope([contract])))
    blocks = _blocks(README.read_text(encoding="utf-8"))
    assert len(blocks) >= 15
    names = _run_blocks(blocks, tmp_path, "readme")
    # A few of the tables the README promises.
    assert isinstance(names["week"], Table)
    assert isinstance(names["kept"], Table)
    assert isinstance(names["watched"], Table)
    assert isinstance(names["es"], Table)
    _forget(names)


# --- Every cited link exists -------------------------------------------------------------------


def _readme_anchors() -> set[str]:
    """Return the ids of the README's explicit anchors."""
    return set(_ANCHOR.findall(README.read_text(encoding="utf-8")))


def _sources() -> dict[str, str]:
    """Return the text of every library module, by its path from the repository root."""
    return {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in sorted(SOURCE.rglob("*.py"))
    }


def test_every_readme_section_a_docstring_cites_exists() -> None:
    anchors = _readme_anchors()
    missing = [
        f"{path}: README.md#{anchor}"
        for path, text in _sources().items()
        for anchor in _README_CITATION.findall(text)
        if anchor not in anchors
    ]
    assert missing == []


def test_every_dataset_page_a_docstring_cites_exists() -> None:
    pages = ROOT / "docs" / "datasets"
    missing = [
        f"{path}: docs/datasets/{name}.md"
        for path, text in _sources().items()
        for name in _PAGE_CITATION.findall(text)
        if not (pages / f"{name}.md").is_file()
    ]
    assert missing == []


def test_the_citations_are_found() -> None:
    # A guard on the two patterns above: a typo in either would find nothing and pass.
    text = "See README.md#symbols. Columns: docs/datasets/stocks.financials.ratios.md."
    assert _README_CITATION.findall(text) == ["symbols"]
    assert _PAGE_CITATION.findall(text) == ["stocks.financials.ratios"]


def test_every_link_in_the_readme_resolves() -> None:
    text = README.read_text(encoding="utf-8")
    anchors = _readme_anchors()
    broken = []
    for target in _LINK.findall(text):
        if target.startswith(("http://", "https://")):
            continue
        path, _, anchor = target.partition("#")
        if (path and not (ROOT / path).exists()) or (not path and anchor not in anchors):
            broken.append(target)
    assert broken == []


def test_the_namespace_table_lists_the_catalogs_namespaces() -> None:
    text = README.read_text(encoding="utf-8")
    start = text.index('<a id="namespaces"></a>')
    table = text[start : text.index("<a id=", start + 1)]
    listed = re.findall(r"^\| `(m\.[a-z_.]+)` \|", table, re.MULTILINE)
    catalog = {
        f"m.{spec.name.rpartition('.')[0] or spec.name}" for spec in CATALOG.by_name.values()
    }
    assert len(listed) == len(set(listed))
    assert set(listed) == catalog


# --- Two sections, against scripted data -------------------------------------------------------


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
    blocks = _section_blocks("split-adjustment")
    assert len(blocks) == 3
    # The section continues the console session the README opened before it.
    names = _run_blocks(
        ["import deephaven_massive as dm\n\nm = dm.MassiveSession()\n", *blocks],
        tmp_path,
        "split_adjustment",
    )

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
    _forget(names)


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


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_first_tables_example_runs(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
) -> None:
    # The whole New York days of 2025-06-02 to 2025-06-06, as the range path's `{from}/{to}`.
    path = (
        f"/v2/aggs/ticker/AAPL/range/1/day/{_ms('2025-06-02T04:00:00')}/"
        f"{_ms('2025-06-07T04:00:00') - 1}"
    )
    days = [_bar(f"2025-06-0{day}", 190.0 + day, 1_000) for day in range(2, 7)]
    readme_server.reply(path, Reply(envelope(days, adjusted=True, ticker="AAPL")))
    # Every other path, today's headlines among them, answers an empty page.
    readme_server.reply_default(Reply(envelope([])))
    [block] = _section_blocks("first-tables")
    names = _run_blocks([block], tmp_path, "first_tables")

    assert [r["Date"] for r in dh_to_pylist(names["week"])] == [
        "2025-06-02",
        "2025-06-03",
        "2025-06-04",
        "2025-06-05",
        "2025-06-06",
    ]
    described = {r["Name"]: r["Description"] for r in dh_to_pylist(names["columns"])}
    assert {"Symbol", "Open", "Close", "TimestampEnd"} <= described.keys()
    fake_ws_server.wait_for(
        lambda server: any(
            "T.AAPL" in json.loads(frame).get("params", "")
            for connection in server.connections
            for frame in connection.frames
        )
    )
    trades = [
        {"ev": "T", "sym": symbol, "x": 4, "i": "1", "p": price, "s": 100, "t": 1_700_000_000_000}
        for symbol, price in (("AAPL", 190.0), ("MSFT", 410.0), ("AAPL", 191.0))
    ]
    fake_ws_server.send(json.dumps(trades))
    # Bounded wait for the flush thread and an update-graph cycle to publish the trades.
    pace = threading.Event()  # never set; only paces the loop
    latest: dict[str, float] = {}
    for _ in range(500):
        latest = {r["Symbol"]: r["Price"] for r in dh_to_pylist(names["latest"])}
        if latest == {"AAPL": 191.0, "MSFT": 410.0}:
            break
        pace.wait(0.02)
    assert latest == {"AAPL": 191.0, "MSFT": 410.0}
    names.clear()
