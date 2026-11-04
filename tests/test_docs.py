"""Tests of the user docs: every page's examples run, and every link and citation resolves.

The hand-written user documentation is `README.md` and the reference pages of the user guide,
`docs/guide/*.md`: the *user pages*. Their Python examples are real code: every block of a page
runs here, in document order, in one namespace of its own, as a reader would paste them into
the Deephaven console one after another, against the fake REST and WebSocket servers, so nothing
reaches Massive. A guide page opens with a line naming what its examples assume, the import and
the session, and that code runs first. `dm.MassiveSession()` builds a new session whose REST
client talks to the fake REST server and whose streams connect to the fake WebSocket server.
A block fenced as `python skip-test` does not run; only the blocks `SKIPPED_BLOCKS` names may
be. Two sections, the first tables and split adjustment, also run alone against scripted
replies (NVDA's 10-for-1 split of 2024-06-10), and the tests check that their tables hold what
the pages say they hold.

The docstrings link instead of repeating. A shared argument or concept cites a section by its
full GitHub URL, `DOCS_URL` followed by `README.md#<anchor>` or `docs/guide/<page>.md#<anchor>`,
so the link opens from an IDE or the console, and a dataset cites its generated page the same
way, as `DOCS_URL` followed by `docs/datasets/<dataset>.md`. Every anchor is an explicit
`<a id>`, unique across the user pages. The tests check that every section and page citation
is a URL, that every cited anchor and page exists, that every relative link of a user page (and
of the tester guide) resolves, and that the README's namespace table lists exactly the
catalog's namespaces.
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
from deephaven_massive._session import CATALOG
from deephaven_massive._tables.transports import HostDecision
from tests._fakes.rest import FakeRestServer, Reply, envelope
from tests._fakes.ws import FakeMassiveServer

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
GUIDE = ROOT / "docs" / "guide"
GUIDE_PAGES = sorted(GUIDE.glob("*.md"))
USER_PAGES = [README, *GUIDE_PAGES]
TESTER_GUIDE = ROOT / "docs" / "tester-guide.md"
SOURCE = ROOT / "src" / "deephaven_massive"

PREAMBLE = (
    "Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`."
)
"""The line every guide page opens with, after its title: what its examples assume."""

PREAMBLE_CODE = "import deephaven_massive as dm\n\nm = dm.MassiveSession()\n"
"""The code the preamble names, which runs before a guide page's first block."""

SKIPPED_BLOCKS: frozenset[tuple[str, str]] = frozenset(
    {("docs/guide/install.md", "from deephaven_server import Server")}
)
"""The blocks allowed a `python skip-test` fence, by page path and the block's first line.

A block belongs here only when it cannot run in the test process, such as one that starts its
own Deephaven server.
"""

_FENCE = re.compile(r"^```python([^\n]*)\n(.*?)^```", re.DOTALL | re.MULTILINE)
_YAML_BLOCK = re.compile(r"```yaml\n(.*?)```", re.DOTALL)
_ANCHOR = re.compile(r'<a id="([a-z0-9-]+)"></a>')
DOCS_URL = "https://github.com/chipkent/deephaven-massive/blob/main/"
"""The prefix of every section citation in a docstring: the user pages on GitHub's main branch."""

_README_CITATION = re.compile(re.escape(DOCS_URL) + r"README\.md#([A-Za-z0-9_-]+)")
_GUIDE_CITATION = re.compile(re.escape(DOCS_URL) + r"docs/guide/([a-z-]+\.md)#([A-Za-z0-9_-]+)")
_BARE_CITATION = re.compile(
    r"(?<!/blob/main/)\b(?:(?:README\.md|docs/guide/[a-z-]+\.md)#[A-Za-z0-9_-]+"
    r"|docs/datasets/[A-Za-z0-9_.]+?\.md\b)"
)
_PAGE_CITATION = re.compile(re.escape(DOCS_URL) + r"docs/datasets/([A-Za-z0-9_.]+?)\.md\b")
_LINK = re.compile(r"\]\(([^)\s]+)\)")


def _name(page: Path) -> str:
    """Return a page's path from the repository root, such as `docs/guide/history.md`."""
    return page.relative_to(ROOT).as_posix()


def _fences(text: str) -> list[tuple[str, str]]:
    """Return the info string after `python` and the code of each Python block, in order."""
    return [(info.strip(), code) for info, code in _FENCE.findall(text)]


def _blocks(text: str) -> list[str]:
    """Return the code of each Python block of a Markdown text that runs, in order."""
    return [code for info, code in _fences(text) if info != "skip-test"]


def _section_blocks(page: Path, anchor: str) -> list[str]:
    """Return the Python blocks of one section of a page, from its anchor to the next anchor.

    Args:
        page: The user page.
        anchor: The section's `<a id>`, such as `"split-adjustment"`.

    Returns:
        The code of each block, in order.
    """
    text = page.read_text(encoding="utf-8")
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
    """Refuse a crypto lookup: the user pages' examples stream no crypto pair.

    Raises:
        AssertionError: Always, naming the pair.
    """
    raise AssertionError(f"the user pages' examples stream no crypto pair, asked for {symbol}")


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


def _run_blocks(
    blocks: Sequence[str],
    tmp_path: Path,
    name: str,
    *,
    console_command: Callable[[Callable[[], None]], None],
) -> dict[str, Any]:
    """Run code blocks as scripts, in order, each seeing what the blocks before it defined.

    Each block runs as one console command (the `console_command` fixture): under the update
    graph's exclusive lock, as a reader pasting it into the Deephaven console runs it. A block
    that creates an input table and adds a row at once, as the watchlist examples do, is then
    never interrupted by an update cycle; without the lock, a cycle running while the table is
    created can refresh it in the step it was stamped with, and Deephaven exits the process.

    Args:
        blocks: The code of each block.
        tmp_path: Where to write the scripts.
        name: The scripts' file name prefix.
        console_command: Runs one block the way the console runs a command.

    Returns:
        The names the blocks defined.
    """
    names: dict[str, Any] = {}
    for number, block in enumerate(blocks):
        script = tmp_path / f"{name}_{number}.py"
        script.write_text(block, encoding="utf-8")

        def run(script: Path = script) -> None:
            nonlocal names
            names = runpy.run_path(str(script), init_globals=names)

        console_command(run)
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


def _settings_file(home: Path) -> Path:
    """Write the guide's example settings file where its examples read it, under `home`.

    The file is the install page's own YAML example, so the test checks that the example is a
    file `MassiveConfig.from_file` accepts.

    Args:
        home: The folder that stands in for the home folder.

    Returns:
        The file's path, `<home>/massive/settings.yaml`.
    """
    text = (GUIDE / "install.md").read_text(encoding="utf-8")
    start = text.index('<a id="settings-file"></a>')
    end = text.find("<a id=", start + 1)
    [example] = _YAML_BLOCK.findall(text[start : end if end >= 0 else len(text)])
    file = home / "massive" / "settings.yaml"
    file.parent.mkdir()
    file.write_text(example, encoding="utf-8")
    return file


#: Tables each page's examples promise, by page.
_PROMISED: dict[str, tuple[str, ...]] = {
    "README.md": ("week", "near_quotes", "board"),
    "docs/guide/concepts.md": ("kept", "watched"),
    "docs/guide/watchlists.md": ("es", "live_board", "trades"),
    "docs/guide/history.md": ("adjusted", "mine"),
    "docs/guide/recipes.md": ("both",),
    "docs/guide/troubleshooting.md": ("streams",),
}


@pytest.mark.jvm_held  # the scripts' formulas hand their globals, the session too, to the JVM
@pytest.mark.parametrize("page", USER_PAGES, ids=_name)
def test_every_user_page_example_runs_in_order(
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
    # The settings-file examples read the guide's example file from the home folder, by path
    # and through the environment variable.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DEEPHAVEN_MASSIVE_CONFIG_FILE", str(_settings_file(tmp_path)))
    # Every path the examples read answers an empty page, except the futures contracts, which
    # the watchlist examples stream, and must name at least one contract.
    readme_server.reply_default(Reply(envelope([])))
    contract = {"ticker": "ESZ6", "product_code": "ES", "date": "2026-10-07", "active": True}
    readme_server.reply("/futures/v1/contracts", Reply(envelope([contract])))
    blocks = _blocks(page.read_text(encoding="utf-8"))
    if page != README:
        # A guide page's examples continue from the import and the session its preamble names.
        blocks = [PREAMBLE_CODE, *blocks]
    names = _run_blocks(blocks, tmp_path, page.stem, console_command=console_command)
    for name in _PROMISED.get(_name(page), ()):
        assert isinstance(names[name], Table), name
    if page.name == "install.md":
        assert names["config"].max_outage == 600  # read from the example file
    if any("input_table(" in block for block in blocks):
        assert locked  # the watchlist examples made input tables
    assert all(locked), locked  # and every one held the lock, whatever their count
    _forget(names)


def test_the_readme_has_its_examples() -> None:
    # A guard on the block pattern: a typo in it would find no block, and every page would pass.
    assert len(_blocks(README.read_text(encoding="utf-8"))) >= 10


def test_the_guide_has_its_pages() -> None:
    # The guide pages are found by a glob; a wrong folder would find none, and every page test
    # would pass with nothing to check.
    assert {page.name for page in GUIDE_PAGES} >= {
        "arguments.md",
        "concepts.md",
        "history.md",
        "install.md",
        "plans.md",
        "recipes.md",
        "troubleshooting.md",
        "watchlists.md",
    }


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=_name)
def test_every_guide_page_opens_with_the_preamble(page: Path) -> None:
    title, preamble, *_ = page.read_text(encoding="utf-8").split("\n\n")
    assert title.startswith("# ")
    assert preamble == PREAMBLE


def test_only_allowed_blocks_skip_the_test() -> None:
    skipped = set()
    for page in USER_PAGES:
        for info, code in _fences(page.read_text(encoding="utf-8")):
            assert info in {"", "skip-test"}, f"{_name(page)}: python {info}"
            if info == "skip-test":
                skipped.add((_name(page), code.split("\n", 1)[0]))
    assert skipped <= SKIPPED_BLOCKS


# --- Every cited link exists -------------------------------------------------------------------


def _anchors(page: Path) -> list[str]:
    """Return the ids of a page's explicit anchors, in order, repeats included."""
    return _ANCHOR.findall(page.read_text(encoding="utf-8"))


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


def test_every_readme_section_a_docstring_cites_exists() -> None:
    anchors = set(_anchors(README))
    missing = [
        f"{path}: README.md#{anchor}"
        for path, text in _sources().items()
        for anchor in _README_CITATION.findall(text)
        if anchor not in anchors
    ]
    assert missing == []


def test_every_guide_section_a_docstring_cites_exists() -> None:
    missing = [
        f"{path}: docs/guide/{page}#{anchor}"
        for path, text in _sources().items()
        for page, anchor in _GUIDE_CITATION.findall(text)
        if not (GUIDE / page).is_file() or anchor not in _anchors(GUIDE / page)
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
    # A guard on the patterns above: a typo in any would find nothing and pass.
    text = (
        f"See {DOCS_URL}README.md#install. See {DOCS_URL}docs/guide/arguments.md#symbols. "
        f"Columns: {DOCS_URL}docs/datasets/stocks.financials.ratios.md. "
        "Bare: README.md#install, docs/guide/arguments.md#symbols, docs/datasets/stocks.ipos.md."
    )
    assert _README_CITATION.findall(text) == ["install"]
    assert _GUIDE_CITATION.findall(text) == [("arguments.md", "symbols")]
    assert _PAGE_CITATION.findall(text) == ["stocks.financials.ratios"]
    assert _BARE_CITATION.findall(text) == [
        "README.md#install",
        "docs/guide/arguments.md#symbols",
        "docs/datasets/stocks.ipos.md",
    ]


@pytest.mark.parametrize("page", [*USER_PAGES, TESTER_GUIDE], ids=_name)
def test_every_relative_link_resolves(page: Path) -> None:
    # A link to a user page must name one of its explicit anchors, if any; a link to any other
    # file must name a file that exists.
    user_pages = {path.resolve() for path in USER_PAGES}
    broken = []
    for target in _LINK.findall(page.read_text(encoding="utf-8")):
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        path, _, anchor = target.partition("#")
        file = (page.parent / path).resolve() if path else page.resolve()
        if not file.exists() or (anchor and file in user_pages and anchor not in _anchors(file)):
            broken.append(target)
    assert broken == []


def test_the_namespace_table_lists_the_catalogs_namespaces() -> None:
    text = README.read_text(encoding="utf-8")
    start = text.index('<a id="namespaces"></a>')
    table = text[start : text.index("<a id=", start + 1)]
    listed = re.findall(r"^\| `(m\.[a-z_.]+)` \|", table, re.MULTILINE)
    # The unified snapshot, a dataset of no namespace, is the session's own method.
    catalog = {f"m.{spec.name.rpartition('.')[0]}" for spec in CATALOG.by_name.values()} - {"m."}
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
    console_command: Callable[[Callable[[], None]], None],
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
    blocks = _section_blocks(GUIDE / "history.md", "split-adjustment")
    assert len(blocks) == 2
    [recipe] = _section_blocks(README, "backfill-then-live")
    # The sections continue from the import and the session the guide's preamble names.
    names = _run_blocks(
        [PREAMBLE_CODE, *blocks, recipe],
        tmp_path,
        "split_adjustment",
        console_command=console_command,
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
    console_command: Callable[[Callable[[], None]], None],
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
    [block] = _section_blocks(README, "first-tables")
    names = _run_blocks([block], tmp_path, "first_tables", console_command=console_command)

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
            "AM.AAPL" in json.loads(frame).get("params", "")
            for connection in server.connections
            for frame in connection.frames
        )
    )
    minute = {"s": _ms("2025-06-06T19:00:00"), "e": _ms("2025-06-06T19:01:00"), "v": 100}
    bars = [
        {"ev": "AM", "sym": symbol, "o": price, "h": price, "l": price, "c": price} | minute
        for symbol, price in (("AAPL", 190.0), ("MSFT", 410.0), ("AAPL", 191.0))
    ]
    fake_ws_server.send(json.dumps(bars))
    # Bounded wait for the flush thread and an update-graph cycle to publish the bars.
    pace = threading.Event()  # never set; only paces the loop
    latest: dict[str, float] = {}
    for _ in range(500):
        latest = {r["Symbol"]: r["Close"] for r in dh_to_pylist(names["latest"])}
        if latest == {"AAPL": 191.0, "MSFT": 410.0}:
            break
        pace.wait(0.02)
    assert latest == {"AAPL": 191.0, "MSFT": 410.0}
    names.clear()
