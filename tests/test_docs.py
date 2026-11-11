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
backfill of today's minutes. The helpers that read a page's blocks and run them are shared with
the live run of the README's examples, `tests/live/test_doc_examples_integration.py`, from
`tests/_live/doc_examples.py`.

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
import re
import runpy
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime
from datetime import time as dt_time
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import deephaven
import pytest
from deephaven import table_listener, update_graph
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
from tests._live.doc_examples import (
    Console,
    blocks,
    dashboard_panels,
    fences,
    forget,
    merged,
    run_blocks,
    section_blocks,
    write_settings_file,
)

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
_REPOSITORY_FILE = re.compile(
    r"https://(?:github\.com/chipkent/deephaven-massive/blob/main"
    r"|raw\.githubusercontent\.com/chipkent/deephaven-massive/main)/([^#]+)(?:#(.*))?"
)
"""A link to a file on the repository's main branch, as a page or as raw content."""
_REPOSITORY_FILE_PREFIXES = (
    "https://github.com/chipkent/deephaven-massive/blob/",
    "https://raw.githubusercontent.com/chipkent/deephaven-massive/",
)
"""The start of every link to a file in the repository, on any branch."""


def _name(page: Path) -> str:
    """Return a page's path from the repository root, such as `docs/troubleshooting.md`."""
    return page.relative_to(ROOT).as_posix()


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
    write_settings_file(README, tmp_path, delayed=True)
    # Every path the examples read answers an empty page.
    readme_server.reply_default(Reply(envelope([])))
    page_blocks = blocks(page.read_text(encoding="utf-8"))
    namespaces = run_blocks(page_blocks, tmp_path, page.stem, console_command=console_command)
    names = merged(namespaces)
    for name in _PROMISED.get(_name(page), ()):
        assert isinstance(names[name], Table), name
    if any("input_table(" in block.code for block in page_blocks):
        assert locked  # the watchlist examples made input tables
    assert all(locked), locked  # and every one held the lock, whatever their count
    names.clear()
    forget(namespaces)


def test_the_readme_has_its_examples() -> None:
    # A guard on the block pattern: a typo in it would find no block, and every page would pass.
    assert len(blocks(README.read_text(encoding="utf-8"))) >= 10


def test_only_allowed_blocks_skip_the_test() -> None:
    skipped = set()
    for page in USER_PAGES:
        for info, code in fences(page.read_text(encoding="utf-8")):
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


def _resolves(file: Path, anchor: str) -> bool:
    """Return whether a link's file exists and, for a Markdown page, has the link's anchor.

    A user page's anchors are its explicit ones only; another Markdown page, such as a
    generated dataset page, also has the anchors GitHub makes from its headings.

    Args:
        file: The linked file.
        anchor: The link's anchor, or `""` for none.

    Returns:
        Whether the link opens the file at its anchor.
    """
    if not file.exists():
        return False
    if not anchor or file.suffix != ".md":
        return True
    if file in {path.resolve() for path in USER_PAGES}:
        return anchor in _anchors(file)
    return anchor in {*_anchors(file), *_heading_slugs(file)}


@pytest.mark.parametrize("page", [*USER_PAGES, TESTER_GUIDE], ids=_name)
def test_every_relative_link_resolves(page: Path) -> None:
    broken = []
    for target in _LINK.findall(page.read_text(encoding="utf-8")):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, anchor = target.partition("#")
        file = (page.parent / path).resolve() if path else page.resolve()
        if not _resolves(file, anchor):
            broken.append(target)
    assert broken == []


def test_every_readme_link_opens_from_pypi() -> None:
    # PyPI shows the README but resolves no relative path, so the README links every other file,
    # its images included, by full URL, and each such link must name a file on main.
    broken = []
    for target in _LINK.findall(README.read_text(encoding="utf-8")):
        repository_file = _REPOSITORY_FILE.fullmatch(target)
        if repository_file:
            path, anchor = repository_file[1], repository_file[2] or ""
            if not _resolves((ROOT / path).resolve(), anchor):
                broken.append(target)
        else:
            # A relative path, or a repository file off main (another branch, or a typo).
            relative = not target.startswith(("#", "http://", "https://", "mailto:"))
            if relative or target.startswith(_REPOSITORY_FILE_PREFIXES):
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
    write_settings_file(README, home, delayed=True)
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
    [block] = section_blocks(README, "backfill-then-live")
    [names] = run_blocks([block], tmp_path, "backfill", console_command=console_command)
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
    [block] = section_blocks(README, "whole-market-day")
    [names] = run_blocks([block], tmp_path, "whole_market", console_command=console_command)

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
    first, edit = section_blocks(README, "drive-with-a-watchlist")
    assert edit.continues
    # The README polls once a minute; a one-second poll shows the same edit within seconds.
    assert "poll(60)" in first.code
    fast = first._replace(code=first.code.replace("poll(60)", "poll(1)"))
    [names] = run_blocks([fast], tmp_path, "watchlist", console_command=console_command)

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
    [block] = section_blocks(README, "what-your-plan-allows")
    [names] = run_blocks([block], tmp_path, "settings", console_command=console_command)
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
    [block] = section_blocks(README, "change-since-close")
    [names] = run_blocks([block], tmp_path, "change", console_command=console_command)
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


# --- What the library does with each example's data ------------------------------------------


def _new_york_ms(day: str) -> int:
    """Return midnight New York time of a day, as epoch milliseconds."""
    midnight = datetime.combine(date.fromisoformat(day), dt_time(0), ZoneInfo("America/New_York"))
    return int(midnight.timestamp() * 1000)


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_returns_example_gives_each_stock_its_return(
    *,
    readme_server: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    span = f"{_new_york_ms('2025-11-03')}/{_new_york_ms('2026-10-31') - 1}"
    for symbol, first, last in (
        ("AAPL", 200.0, 250.0),
        ("MSFT", 400.0, 380.0),
        ("NVDA", 100.0, 200.0),
    ):
        bars = [
            _bar("2025-11-03", first, 1_000) | {"t": _new_york_ms("2025-11-03")},
            _bar("2026-10-30", last, 1_000) | {"t": _new_york_ms("2026-10-30")},
        ]
        readme_server.reply(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{span}",
            Reply(envelope(bars, adjusted=True, ticker=symbol)),
        )
    [block] = section_blocks(README, "compare-stocks")
    [names] = run_blocks([block], tmp_path, "returns", console_command=console_command)
    # One row per stock, its first and last close, and the percent return between them.
    returns = {r["Symbol"]: r for r in dh_to_pylist(names["returns"])}
    assert {s: (r["Start"], r["End"]) for s, r in returns.items()} == {
        "AAPL": (200.0, 250.0),
        "MSFT": (400.0, 380.0),
        "NVDA": (100.0, 200.0),
    }
    assert {s: r["ReturnPct"] for s, r in returns.items()} == pytest.approx(
        {"AAPL": 25.0, "MSFT": -5.0, "NVDA": 100.0}
    )
    names.clear()


_NEWS_STOCKS = ["AAPL", "NVDA", "TSLA"]


def _articles() -> list[dict[str, Any]]:
    """Return three articles that list every stock of the news example, oldest in the middle."""
    fixture = Path(__file__).parent / "data" / "namespaces" / "reference" / "news.json"
    [template] = json.loads(fixture.read_text(encoding="utf-8"))["results"]
    stamps = ("2026-10-30T15:00:00Z", "2026-10-30T09:00:00Z", "2026-10-30T18:00:00Z")
    return [
        template
        | {"id": f"article-{n}", "published_utc": stamp, "tickers": ["AAPL", "NVDA", "TSLA"]}
        for n, stamp in enumerate(stamps)
    ]


class _Failure:
    """The text of the failure a table's listener heard, written on an update-graph thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._text: str | None = None

    def heard(self, error: Exception) -> None:
        """Keep the failure's text."""
        with self._lock:
            self._text = str(error)

    def text(self) -> str | None:
        """Return the failure's text, or None before the table fails."""
        with self._lock:
            return self._text


@pytest.fixture
def failure_text() -> Iterator[Callable[[Table], _Failure]]:
    """Return a function that listens for a live table's failure and keeps its text.

    Every listener is stopped when the test ends, before its leak check.

    Yields:
        A function mapping a table to the record of its failure.
    """
    handles = []

    def watch(table: Table) -> _Failure:
        failure = _Failure()
        # A wrapper of its own: the JVM holds the listener, and through it the wrapper.
        own = Table(j_table=table.j_table)
        handles.append(
            table_listener.listen(own, lambda _update, _replay: None, on_error=failure.heard)
        )
        return failure

    yield watch
    for handle in handles:
        handle.stop()


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
def test_the_news_example_lists_the_newest_first_and_stopping_keeps_its_rows(
    *,
    readme_server: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    failure_text: Callable[[Table], _Failure],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    readme_server.reply("/v2/reference/news", Reply(envelope(_articles())))
    console = Console(tmp_path, console_command)
    news = console.run(section_blocks(README, "follow-the-news"), "headlines", "latest")
    # Each stock's articles, newest first.
    rows = _wait_for_rows(news["latest"], lambda rows: len(rows) == 9, dh_to_pylist)
    published = [r["PublishedUtc"] for r in rows]
    assert published == sorted(published, reverse=True)
    assert {r["Symbol"] for r in rows} == {"AAPL", "NVDA", "TSLA"}
    failures = {name: failure_text(news[name]) for name in ("headlines", "latest")}
    # Example 6, in the same console: the session closes, and the tables keep their rows.
    stopped = console.run(section_blocks(README, "stop-live-tables"), "kept")
    pace = threading.Event()  # never set; only paces the wait for the failures
    for _ in range(500):
        if all(failure.text() is not None for failure in failures.values()):
            break
        pace.wait(0.02)
    for failure in failures.values():
        assert "was closed; this table no longer updates" in str(failure.text())
    assert dh_to_pylist(news["latest"]) == rows
    assert dh_to_pylist(stopped["kept"]) == rows
    news.clear()
    stopped.clear()
    console.close()


def _minute_bar(symbol: str, minute: int, close: float) -> dict[str, Any]:
    """Return a stock's minute bar of the fake stream, `minute` minutes after 15:00 UTC."""
    start = _ms("2026-10-30T15:00:00") + minute * 60_000
    bar = {"ev": "AM", "sym": symbol, "o": close, "h": close, "l": close, "c": close, "v": 100}
    return bar | {"s": start, "e": start + 60_000}


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
@pytest.mark.usefixtures("settings_home")
def test_the_table_types_example_keeps_the_newest_the_last_30_and_every_bar(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    readme_server.reply_default(Reply(envelope([])))
    console = Console(tmp_path, console_command)
    names = console.run(section_blocks(README, "table-shapes"), "newest", "last_30", "since_start")
    _wait_for_subscription(fake_ws_server, "AM.SPY")
    fake_ws_server.send(json.dumps([_minute_bar("SPY", n, 500.0 + n) for n in range(32)]))
    every = _wait_for_rows(names["since_start"], lambda rows: len(rows) == 32, dh_to_pylist)
    assert [r["Close"] for r in every] == [500.0 + n for n in range(32)]
    # The newest bar, and the newest 30.
    assert [r["Close"] for r in dh_to_pylist(names["newest"])] == [531.0]
    assert [r["Close"] for r in dh_to_pylist(names["last_30"])] == [502.0 + n for n in range(30)]
    names.clear()
    console.close()


@pytest.mark.jvm_held  # the script's formulas hand its globals, the session too, to the JVM
@pytest.mark.usefixtures("settings_home")
def test_the_dashboard_example_shows_its_chart_and_table_and_the_chart_grows(
    *,
    readme_server: FakeRestServer,
    fake_ws_server: FakeMassiveServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
    console_command: Callable[[Callable[[], None]], None],
) -> None:
    for symbol in ("AAPL", "MSFT", "NVDA"):
        previous = _bar("2026-10-29", 100.0, 1_000) | {"T": symbol}
        readme_server.reply(
            f"/v2/aggs/ticker/{symbol}/prev", Reply(envelope([previous], ticker=symbol))
        )
    console = Console(tmp_path, console_command)
    names = console.run(section_blocks(README, "dashboard"), "dashboard", "chart", "changes", "m")
    # Two panels side by side: the chart, then the change table.
    panels = dashboard_panels(names["dashboard"])
    assert [title for title, _ in panels] == ["Closes", "Change"]
    assert panels[0][1] is names["chart"]
    assert panels[1][1] is names["changes"]

    def published() -> int:
        rows = dh_to_pylist(names["m"].streams())
        return sum(int(r["RowsPublished"] or 0) for r in rows if r["Dataset"] == "stocks.bars")

    # The chart's stream publishes each minute's bars, so its line grows each minute.
    _wait_for_subscription(fake_ws_server, "AM.NVDA")
    counts = []
    for minute in range(2):
        fake_ws_server.send(
            json.dumps([_minute_bar(s, minute, 101.0) for s in ("AAPL", "MSFT", "NVDA")])
        )
        pace = threading.Event()  # never set; only paces the wait for the bars
        for _ in range(500):
            if published() == 3 * (minute + 1):
                break
            pace.wait(0.02)
        counts.append(published())
    assert counts == [3, 6]
    names.clear()
    console.close()
