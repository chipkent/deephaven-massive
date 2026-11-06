"""Tests of the user docs: every page's examples run, and every link and citation resolves.

The hand-written user documentation is `README.md`, `docs/troubleshooting.md`, and
`docs/configuration.md`, whose settings reference is generated: the *user pages*. Their Python
examples are real code: every block of a page runs here on its own, in a namespace of its own,
as a reader would paste it into a fresh Deephaven console, against the
fake REST and WebSocket servers, so nothing reaches Massive. Every block therefore carries its
own imports and session. `dm.MassiveSession()` builds a new session whose REST client talks to
the fake REST server and whose streams connect to the fake WebSocket server.
A block fenced as `python continues` is the second step of an example, which the reader runs
in the same console after the block before it, so it runs in that block's namespace. A block
fenced as `python skip-test` does not run; only the blocks `SKIPPED_BLOCKS` names may be. The
README's settings file is written by running its shell commands, with `HOME` in a temporary
folder, and the settings page's example file loads as the page says. Five README sections also
run alone against scripted replies, and the tests check that
their tables hold what the README says they hold: the whole-market day, the watchlist that
adds and removes stocks, the settings file, the change since yesterday's close, and the
backfill of today's minutes.

The docstrings link instead of repeating. A docstring cites a page, or a section of a user page,
by its full GitHub URL, `DOCS_URL` followed by the page's path from the repository root and,
for a section, `#<anchor>`, so the link opens from an IDE or the console: a dataset's generated
page as `docs/datasets/<dataset>.md`, a README section as `README.md#<anchor>`. Every anchor is
an explicit `<a id>`, unique across the user pages. The tests check that every citation is a
URL, that every cited page and anchor exists, and that every relative link of a user page (and
of the tester guide) resolves.
"""

import dataclasses
import json
import os
import re
import runpy
import shutil
import subprocess
import threading
import types
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, cast

import deephaven
import pytest
from deephaven import update_graph
from deephaven.execution_context import ExecutionContext
from deephaven.table import Table, TableDefinitionLike
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._enums import Recency
from deephaven_massive._ingest import ws
from deephaven_massive._massive import Source, WsClient
from deephaven_massive._tables.transports import HostDecision
from tests._fakes.rest import FakeRestServer, Reply, envelope
from tests._fakes.ws import FakeMassiveServer

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
TROUBLESHOOTING = ROOT / "docs" / "troubleshooting.md"
CONFIGURATION = ROOT / "docs" / "configuration.md"
USER_PAGES = [README, TROUBLESHOOTING, CONFIGURATION]
TESTER_GUIDE = ROOT / "docs" / "tester-guide.md"
SOURCE = ROOT / "src" / "deephaven_massive"

SKIPPED_BLOCKS: frozenset[tuple[str, str]] = frozenset()
"""The blocks allowed a `python skip-test` fence, by page path and the block's first line.

A block belongs here only when it cannot run in the test process, such as one that starts its
own Deephaven server, or one that reads flat files, which needs S3 keys the fake session lacks.
"""

_FENCE = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.DOTALL | re.MULTILINE)
_BASH = re.compile(r"^```bash\n(.*?)^```", re.DOTALL | re.MULTILINE)
_YAML = re.compile(r"^```yaml\n(.*?)^```", re.DOTALL | re.MULTILINE)
_ANCHOR = re.compile(r'<a id="([a-z0-9-]+)"></a>')
DOCS_URL = "https://github.com/chipkent/deephaven-massive/blob/main/"
"""The prefix of every section citation in a docstring: the user pages on GitHub's main branch."""

_CITATION = re.compile(re.escape(DOCS_URL) + r"([A-Za-z0-9_./-]+?\.md)\b(?:#([A-Za-z0-9_-]+))?")
_BARE_CITATION = re.compile(
    r"(?<!/blob/main/)\b(?:(?:README|docs/troubleshooting|docs/configuration)\.md#[A-Za-z0-9_-]+"
    r"|docs/datasets/[A-Za-z0-9_.]+?\.md\b)"
)
_LINK = re.compile(r"\]\(([^)\s]+)\)")


def _name(page: Path) -> str:
    """Return a page's path from the repository root, such as `docs/troubleshooting.md`."""
    return page.relative_to(ROOT).as_posix()


class Block(NamedTuple):
    """One Python block of a user page that runs."""

    code: str
    """The block's code."""
    continues: bool
    """Whether it runs in the namespace of the block before it (a `python continues` fence)."""


def _fences(text: str) -> list[tuple[str, str]]:
    """Return the info string after `python` and the code of each Python block, in order."""
    return [(info.strip(), code) for info, code in _FENCE.findall(text)]


def _blocks(text: str) -> list[Block]:
    """Return each Python block of a Markdown text that runs, in order."""
    return [Block(code, info == "continues") for info, code in _fences(text) if info != "skip-test"]


def _section(page: Path, anchor: str) -> str:
    """Return the text of one section of a page, from its anchor to the next anchor.

    Args:
        page: The user page.
        anchor: The section's `<a id>`, such as `"whole-market-day"`.

    Returns:
        The section's Markdown.
    """
    text = page.read_text(encoding="utf-8")
    start = text.index(f'<a id="{anchor}"></a>')
    end = text.find("<a id=", start + 1)
    return text[start : end if end >= 0 else len(text)]


def _section_blocks(page: Path, anchor: str) -> list[Block]:
    """Return the Python blocks of one section of a page, in order (see `_section`)."""
    return _blocks(_section(page, anchor))


def _write_settings_file(home: Path) -> None:
    """Run the README's commands that write the delayed plans' settings file, with `HOME` at `home`.

    The section has one block of commands per kind of plan: first Starter and Developer's, which
    accept the delayed feed, then Advanced and Business's, which are real-time. The first runs,
    so the examples that load the file accept the delayed feed.

    Args:
        home: The folder that stands in for the reader's home folder.
    """
    commands, real_time = _BASH.findall(_section(README, "what-your-plan-allows"))
    assert "delayed_subscriptions: []" in real_time
    bash = shutil.which("bash")
    assert bash is not None, "the README's shell commands need bash"
    subprocess.run(  # noqa: S603 - bash running the README's own commands, no shell string
        [bash, "-c", commands],
        check=True,
        env={**os.environ, "HOME": str(home)},
    )


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


#: The crypto pairs the user pages' examples stream, with their base and quote currencies.
_CRYPTO_PAIRS: dict[str, tuple[str, str]] = {"X:BTCUSD": ("BTC", "USD"), "X:ETHUSD": ("ETH", "USD")}


def _crypto_pair(symbol: str) -> tuple[str, str]:
    """Answer a crypto lookup as Massive's crypto ticker list does, for the examples' pairs.

    Args:
        symbol: The crypto ticker, such as `"X:BTCUSD"`.

    Returns:
        The pair's base and quote currencies.

    Raises:
        AssertionError: For a pair no example streams, naming it.
    """
    if symbol not in _CRYPTO_PAIRS:
        raise AssertionError(f"the user pages' examples stream no crypto pair {symbol}")
    return _CRYPTO_PAIRS[symbol]


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
        reconnect_deadline: Callable[[], float | None],
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
            reconnect_deadline=reconnect_deadline,
        )

    def session(*_args: object, **_kwargs: object) -> dm.MassiveSession:
        config = dm.MassiveConfig(
            api_key="test-key", rest_base_url=fake_rest.url, flush_interval=0.01
        )
        made = real(config)
        connections = ws.WsConnections(
            made._core,
            resolve_host=lambda *_: HostDecision("socket.test", Recency.REALTIME, "test"),
            fetch_rows=lambda *_, **__: [],
            crypto_pair=_crypto_pair,
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


def _run_blocks(
    blocks: Sequence[Block],
    tmp_path: Path,
    name: str,
    *,
    console_command: Callable[[Callable[[], None]], None],
) -> list[dict[str, Any]]:
    """Run code blocks as scripts, in order, as a reader pastes them into the console.

    A block runs on its own, as in a fresh console, unless it continues the block before it:
    then it starts from that block's names, as in the same console. Each block runs as one
    console command (the `console_command` fixture): under the update graph's exclusive lock,
    as a reader pasting it into the Deephaven console runs it. A block that creates an input
    table and adds a row at once, as the watchlist examples do, is then never interrupted by an
    update cycle; without the lock, a cycle running while the table is created can refresh it
    in the step it was stamped with, and Deephaven exits the process.

    Args:
        blocks: The blocks, in order.
        tmp_path: Where to write the scripts.
        name: The scripts' file name prefix.
        console_command: Runs one block the way the console runs a command.

    Returns:
        The names each block defined, one namespace per block, in order; a continuing block's
        namespace holds the names it started from too.
    """
    namespaces: list[dict[str, Any]] = []
    for number, block in enumerate(blocks):
        script = tmp_path / f"{name}_{number}.py"
        script.write_text(block.code, encoding="utf-8")
        start = namespaces[-1] if block.continues else None

        def run(script: Path = script, start: dict[str, Any] | None = start) -> None:
            namespaces.append(runpy.run_path(str(script), init_globals=start))

        console_command(run)
    return namespaces


def _merged(namespaces: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Return the names of several blocks as one mapping, a later block's name winning.

    Args:
        namespaces: The names each block defined, as `_run_blocks` returns them.

    Returns:
        Every name, bound to the value its last defining block gave it.
    """
    merged: dict[str, Any] = {}
    for names in namespaces:
        merged.update(names)
    return merged


def _forget(namespaces: Sequence[dict[str, Any]]) -> None:
    """Break the cycle between the scripts' functions and their globals, then drop the names.

    A function a script defined holds its module globals, which hold the function and the
    session, so the session goes with the test only once those globals are cleared.

    Args:
        namespaces: The names each script defined.
    """
    for names in namespaces:
        for value in names.values():
            if isinstance(value, types.FunctionType) and value.__module__ == "<run_path>":
                value.__globals__.clear()
        names.clear()


# --- Every example runs ------------------------------------------------------------------------


#: Tables each page's examples promise, by page.
_PROMISED: dict[str, tuple[str, ...]] = {
    "README.md": (
        "day",
        "apple",
        "busiest",
        "returns",
        "headlines",
        "kept",
        "closes",
        "snapshot",
        "changes",
        "since_start",
        "spy",
    ),
    "docs/troubleshooting.md": ("streams",),
}


@pytest.mark.jvm_held  # the scripts' formulas hand their globals, the session too, to the JVM
@pytest.mark.parametrize("page", USER_PAGES, ids=_name)
def test_every_user_page_example_runs_alone(
    *,
    page: Path,
    readme_server: FakeRestServer,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
    dh_exec_ctx: ExecutionContext,
) -> None:
    # Each watchlist example's input table is created under the update graph's lock, so no
    # update cycle can refresh it in the step it was stamped with.
    locked: list[bool] = []
    real_input_table = deephaven.input_table

    def recording(
        col_defs: TableDefinitionLike | None = None,
        init_table: Table | None = None,
        key_cols: str | Sequence[str] | None = None,
    ) -> InputTable:
        locked.append(update_graph.has_exclusive_lock(dh_exec_ctx.update_graph))
        return real_input_table(col_defs, init_table, key_cols)

    monkeypatch.setattr(deephaven, "input_table", recording)
    # The README's settings file, written by its own commands in a stand-in home folder.
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_settings_file(tmp_path)
    # Every path the examples read answers an empty page.
    readme_server.reply_default(Reply(envelope([])))
    blocks = _blocks(page.read_text(encoding="utf-8"))
    namespaces = _run_blocks(blocks, tmp_path, page.stem, console_command=console_command)
    names = _merged(namespaces)
    for name in _PROMISED.get(_name(page), ()):
        assert isinstance(names[name], Table), name
    if any("input_table(" in block.code for block in blocks):
        assert locked  # the watchlist examples made input tables
    assert all(locked), locked  # and every one held the lock, whatever their count
    names.clear()
    _forget(namespaces)


def test_the_readme_has_its_examples() -> None:
    # A guard on the block pattern: a typo in it would find no block, and every page would pass.
    assert len(_blocks(README.read_text(encoding="utf-8"))) >= 10


def test_only_allowed_blocks_skip_the_test() -> None:
    skipped = set()
    for page in USER_PAGES:
        for info, code in _fences(page.read_text(encoding="utf-8")):
            assert info in {"", "continues", "skip-test"}, f"{_name(page)}: python {info}"
            if info == "skip-test":
                skipped.add((_name(page), code.split("\n", 1)[0]))
    assert skipped <= SKIPPED_BLOCKS


# --- Every cited link exists -------------------------------------------------------------------


def _anchors(page: Path) -> list[str]:
    """Return the ids of a page's explicit anchors, in order, repeats included."""
    return _ANCHOR.findall(page.read_text(encoding="utf-8"))


def _heading_slugs(page: Path) -> set[str]:
    """Return the anchors GitHub makes from a page's Markdown headings, such as `plan-access`."""
    headings = re.findall(r"^#+ (.+)$", page.read_text(encoding="utf-8"), re.MULTILINE)
    return {
        re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-") for heading in headings
    }


def _sources() -> dict[str, str]:
    """Return the text of every library module, by its path from the repository root."""
    return {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in sorted(SOURCE.rglob("*.py"))
    }


def test_every_anchor_is_unique_across_the_user_pages() -> None:
    seen: dict[str, list[str]] = {}
    for page in USER_PAGES:
        for anchor in _anchors(page):
            seen.setdefault(anchor, []).append(_name(page))
    assert {anchor: pages for anchor, pages in seen.items() if len(pages) > 1} == {}


def test_every_section_citation_is_a_url() -> None:
    # A bare path does not open from an IDE or the console, so a docstring cites a section, or a
    # dataset's page, by its full URL.
    bare = [
        f"{path}: {citation}"
        for path, text in _sources().items()
        for citation in _BARE_CITATION.findall(text)
    ]
    assert bare == []


def test_every_page_and_section_a_docstring_cites_exists() -> None:
    # A cited page must exist; a cited section must be one of its page's explicit anchors.
    missing = [
        f"{path}: {page}#{anchor}" if anchor else f"{path}: {page}"
        for path, text in _sources().items()
        for page, anchor in _CITATION.findall(text)
        if not (ROOT / page).is_file() or (anchor and anchor not in _anchors(ROOT / page))
    ]
    assert missing == []


def test_the_citations_are_found() -> None:
    # A guard on the patterns above: a typo in either would find nothing and pass.
    text = (
        f"See {DOCS_URL}README.md#install. See {DOCS_URL}docs/troubleshooting.md#the-log. "
        f"Columns: {DOCS_URL}docs/datasets/stocks.financials.ratios.md. "
        "Bare: README.md#install, docs/troubleshooting.md#the-log, docs/datasets/stocks.ipos.md."
    )
    assert _CITATION.findall(text) == [
        ("README.md", "install"),
        ("docs/troubleshooting.md", "the-log"),
        ("docs/datasets/stocks.financials.ratios.md", ""),
    ]
    assert _BARE_CITATION.findall(text) == [
        "README.md#install",
        "docs/troubleshooting.md#the-log",
        "docs/datasets/stocks.ipos.md",
    ]


@pytest.mark.parametrize("page", [*USER_PAGES, TESTER_GUIDE], ids=_name)
def test_every_relative_link_resolves(page: Path) -> None:
    # A link to a user page must name one of its explicit anchors, if any; a link into another
    # Markdown page, such as a generated dataset page, an explicit anchor or a heading's anchor;
    # a link to any other file must name a file that exists.
    user_pages = {path.resolve() for path in USER_PAGES}
    broken = []
    for target in _LINK.findall(page.read_text(encoding="utf-8")):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, anchor = target.partition("#")
        file = (page.parent / path).resolve() if path else page.resolve()
        if file in user_pages:
            known = set(_anchors(file))
        elif file.suffix == ".md" and file.exists():
            known = {*_anchors(file), *_heading_slugs(file)}
        else:
            known = set()
        if not file.exists() or (anchor and file.suffix == ".md" and anchor not in known):
            broken.append(target)
    assert broken == []


# --- Five sections, against scripted data ------------------------------------------------------


def _wait_for_rows(
    table: Table,
    done: Callable[[list[dict[str, Any]]], bool],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Return a live table's rows once `done` accepts them, or its last rows after ten seconds.

    Args:
        table: The live table.
        done: Whether the rows are the ones the test waits for.
        dh_to_pylist: The fixture that reads a table's rows.

    Returns:
        The rows `done` accepted, or the last rows read; the caller's assertion then fails.
    """
    pace = threading.Event()  # never set; only paces the loop
    rows = dh_to_pylist(table)
    for _ in range(500):
        if done(rows):
            break
        pace.wait(0.02)
        rows = dh_to_pylist(table)
    return rows


def _wait_for_subscription(fake_ws_server: FakeMassiveServer, topic: str) -> None:
    """Wait until a session subscribes to a topic on the fake WebSocket server, such as `AM.SPY`."""
    fake_ws_server.wait_for(
        lambda server: any(
            topic in json.loads(frame).get("params", "")
            for connection in server.connections
            for frame in connection.frames
        )
    )


@pytest.fixture
def settings_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Write the README's settings file into a stand-in home folder, and point `HOME` at it.

    Returns:
        The stand-in home folder.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    _write_settings_file(home)
    return home


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
@pytest.mark.usefixtures("settings_home")
def test_the_backfill_example_merges_history_with_live_bars(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    # 2024-06-14 at 15:00 New York time: history reads that day's finished minutes over REST,
    # then the stream appends the live ones.
    now = datetime(2024, 6, 14, 19, tzinfo=UTC)
    monkeypatch.setattr(history_source, "_now", lambda: now)
    monkeypatch.setattr("deephaven.time.dh_today", lambda *_: "2024-06-14")
    opened = _ms("2024-06-14T13:30:00")  # 09:30 New York time
    # An open end reads through the end of today; the bars not yet ended are dropped.
    path = (
        f"/v2/aggs/ticker/SPY/range/1/minute/{_ms('2024-06-14T04:00:00')}/"
        f"{_ms('2024-06-15T04:00:00') - 1}"
    )
    minute = {**_bar("2024-06-14", 541.0, 1_000), "t": opened}
    readme_server.reply(path, Reply(envelope([minute], adjusted=True, ticker="SPY")))
    [block] = _section_blocks(README, "backfill-then-live")
    [names] = _run_blocks([block], tmp_path, "backfill", console_command=console_command)
    _wait_for_subscription(fake_ws_server, "AM.SPY")
    live = {"ev": "AM", "sym": "SPY", "v": 5, "o": 542.0, "c": 542.0, "h": 542.0, "l": 542.0}
    live |= {"s": _ms("2024-06-14T19:00:00"), "e": _ms("2024-06-14T19:01:00")}
    fake_ws_server.send(json.dumps([live]))
    rows = _wait_for_rows(names["spy"], lambda rows: len(rows) == 2, dh_to_pylist)
    # Today's minute from history, then the live minute after it, oldest first.
    assert [(r["Symbol"], r["Close"]) for r in rows] == [("SPY", 541.0), ("SPY", 542.0)]
    assert [r["TimestampStart"] for r in rows] == [
        datetime(2024, 6, 14, 13, 30, tzinfo=UTC),
        datetime(2024, 6, 14, 19, tzinfo=UTC),
    ]
    names.clear()


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_whole_market_example_ranks_the_day(
    *,
    readme_server: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    # One grouped day of three stocks: BIG trades the most dollars, on the fewest shares.
    day = [
        _bar("2026-10-30", 11.0, 2_000_000) | {"T": "UP", "o": 10.0, "vw": 10.0},
        _bar("2026-10-30", 150.0, 500_000) | {"T": "BIG", "o": 100.0, "vw": 100.0},
        _bar("2026-10-30", 5.25, 3_000_000) | {"T": "FLAT", "o": 5.0, "vw": 5.0},
    ]
    readme_server.reply(
        "/v2/aggs/grouped/locale/us/market/stocks/2026-10-30",
        Reply(envelope(day, adjusted=True, resultsCount=len(day))),
    )
    [block] = _section_blocks(README, "whole-market-day")
    [names] = _run_blocks([block], tmp_path, "whole_market", console_command=console_command)

    # One row per stock, ranked by dollars traded, largest first.
    assert {r["Symbol"] for r in dh_to_pylist(names["day"])} == {"UP", "BIG", "FLAT"}
    busiest = dh_to_pylist(names["busiest"])
    assert [r["Symbol"] for r in busiest] == ["BIG", "UP", "FLAT"]
    assert [r["DollarVolume"] for r in busiest] == pytest.approx(
        [50_000_000, 20_000_000, 15_000_000]
    )
    names.clear()


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_watchlist_example_adds_and_removes_stocks(
    *,
    readme_server: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    for number, symbol in enumerate(("AAPL", "MSFT", "AMZN", "GOOGL", "NVDA")):
        previous = _bar("2026-10-29", 100.0 + number, 1_000) | {"T": symbol}
        readme_server.reply(
            f"/v2/aggs/ticker/{symbol}/prev", Reply(envelope([previous], ticker=symbol))
        )
    first, edit = _section_blocks(README, "drive-with-a-watchlist")
    assert edit.continues
    # The README polls once a minute; a one-second poll shows the same edit within seconds.
    assert "poll(60)" in first.code
    fast = first._replace(code=first.code.replace("poll(60)", "poll(1)"))
    [names] = _run_blocks([fast], tmp_path, "watchlist", console_command=console_command)

    def symbols(rows: list[dict[str, Any]]) -> set[str]:
        return {r["Symbol"] for r in rows}

    rows = _wait_for_rows(names["closes"], lambda rows: len(symbols(rows)) == 4, dh_to_pylist)
    assert symbols(rows) == {"AAPL", "MSFT", "AMZN", "GOOGL"}
    # The second block, in the same console: Nvidia's row arrives and Microsoft's leaves.
    script = tmp_path / "watchlist_edit.py"
    script.write_text(edit.code, encoding="utf-8")
    edited: dict[str, Any] = {}
    console_command(lambda: edited.update(runpy.run_path(str(script), init_globals=names)))
    after = {"AAPL", "NVDA", "AMZN", "GOOGL"}
    rows = _wait_for_rows(names["closes"], lambda rows: symbols(rows) == after, dh_to_pylist)
    assert symbols(rows) == after
    assert {r["Symbol"]: r["Close"] for r in rows}["NVDA"] == 104.0
    edited.clear()
    names.clear()


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_settings_example_reads_the_file_its_commands_write(
    *,
    readme_server: FakeRestServer,
    settings_home: Path,
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    assert (settings_home / "massive" / "settings.yaml").is_file()
    readme_server.reply_default(Reply(envelope([])))
    [block] = _section_blocks(README, "what-your-plan-allows")
    [names] = _run_blocks([block], tmp_path, "settings", console_command=console_command)
    # The file accepts the delayed Stocks feed, the setting Starter and Developer need.
    assert names["config"].delayed_subscriptions == {dm.Subscription.STOCKS}
    assert isinstance(names["snapshot"], Table)
    names.clear()


def test_the_settings_pages_shape_example_loads_each_form(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    [text, _] = _YAML.findall(CONFIGURATION.read_text(encoding="utf-8"))
    file = tmp_path / "settings.yaml"
    file.write_text(text, encoding="utf-8")
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    config = dm.MassiveConfig.from_file(file)
    # A list, a single value, and the one mapping.
    assert config.delayed_subscriptions == {dm.Subscription.STOCKS, dm.Subscription.OPTIONS}
    assert config.max_outage == 600
    assert config.websocket_hosts == {dm.AssetClass.INDICES: "business.massive.com"}


def test_the_settings_pages_complete_example_sets_every_setting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    [_, text] = _YAML.findall(CONFIGURATION.read_text(encoding="utf-8"))
    file = tmp_path / "settings.yaml"
    file.write_text(text, encoding="utf-8")
    monkeypatch.setenv("MASSIVE_API_KEY", "k")
    config = dm.MassiveConfig.from_file(file)
    credentials = {"api_key", "s3_access_key_id", "s3_secret_access_key"}
    settings = {f.name for f in dataclasses.fields(dm.MassiveConfig)} - credentials
    assert set(re.findall(r"^(\w+):", text, re.MULTILINE)) == settings
    assert config.delayed_subscriptions == {dm.Subscription.STOCKS, dm.Subscription.OPTIONS}
    assert config.websocket_hosts == {dm.AssetClass.INDICES: "business.massive.com"}
    assert config.noncanonical_symbols is dm.OnError.RAISE


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
@pytest.mark.usefixtures("settings_home")
def test_the_change_example_compares_each_minute_with_yesterday(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    for symbol, close in (("AAPL", 200.0), ("MSFT", 400.0)):
        previous = _bar("2026-10-29", close, 1_000) | {"T": symbol}
        readme_server.reply(
            f"/v2/aggs/ticker/{symbol}/prev", Reply(envelope([previous], ticker=symbol))
        )
    # The watchlist's other stock had no bar yesterday.
    readme_server.reply_default(Reply(envelope([])))
    [block] = _section_blocks(README, "change-since-close")
    [names] = _run_blocks([block], tmp_path, "change", console_command=console_command)
    # Each stock with a previous close has its row before its first minute bar, with no close.
    rows = _wait_for_rows(names["changes"], lambda rows: len(rows) == 2, dh_to_pylist)
    assert {r["Symbol"]: (r["Close"], r["PrevClose"]) for r in rows} == {
        "AAPL": (None, 200.0),
        "MSFT": (None, 400.0),
    }
    _wait_for_subscription(fake_ws_server, "AM.AAPL")
    minute = {"s": _ms("2026-10-30T19:00:00"), "e": _ms("2026-10-30T19:01:00"), "v": 100}
    bars = [
        {"ev": "AM", "sym": symbol, "o": price, "h": price, "l": price, "c": price} | minute
        for symbol, price in (("AAPL", 205.0), ("MSFT", 380.0), ("AAPL", 210.0))
    ]
    fake_ws_server.send(json.dumps(bars))
    rows = _wait_for_rows(
        names["changes"],
        lambda rows: {r["Symbol"]: r["Close"] for r in rows} == {"AAPL": 210.0, "MSFT": 380.0},
        dh_to_pylist,
    )
    # Each stock's newest minute, against yesterday's close.
    changes = {r["Symbol"]: r for r in rows}
    assert {s: (r["Close"], r["PrevClose"]) for s, r in changes.items()} == {
        "AAPL": (210.0, 200.0),
        "MSFT": (380.0, 400.0),
    }
    assert changes["AAPL"]["ChangePct"] == pytest.approx(5.0)
    assert changes["MSFT"]["ChangePct"] == pytest.approx(-5.0)
    names.clear()
