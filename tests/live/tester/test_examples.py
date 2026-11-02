"""Tests of the tester examples' runner (`tests/_live/tester/examples.py`), and the examples.

Every example of `docs/tester-examples/` runs here, in order, against a fake Massive REST
server that answers like a free key (`MarketServer`), and the examples that need a paid plan's
streams run on a session whose WebSocket connections reach the fake WebSocket server. The
guide's code blocks are checked against the files, so the tester pastes exactly the code
these tests run.
"""

from __future__ import annotations

import contextlib
import logging
import re
import weakref
from datetime import date, datetime, timedelta
from datetime import time as day_time
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

import pytest
from deephaven import empty_table

import deephaven_massive as dm
from deephaven_massive import _debug_log, _massive
from deephaven_massive._config import MassiveConfig
from deephaven_massive._ingest import ws_connection
from deephaven_massive._types import override
from tests._fakes.rest import FakeRestServer, Received, Reply, envelope
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.tester import examples

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from deephaven.table import Table

    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError, WsState
    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "docs" / "tester-examples"
GUIDE = ROOT / "docs" / "tester-guide.md"
KEY = "PLANTED-tester-key-41c7"
NEW_YORK = ZoneInfo("America/New_York")
LIVE = ("18_live_minute_bars.py", "19_history_plus_live.py", "20_live_crypto.py")
"""The examples that need a paid plan's streams."""

AGGS = re.compile(r"^/v2/aggs/ticker/([^/]+)/range/(\d+)/(\w+)/([\d-]+)/([\d-]+)$")
DETAILS = re.compile(r"^/v3/reference/tickers/([A-Z.]+)$")
REFUSED = Reply(
    {"status": "NOT_AUTHORIZED", "request_id": "r", "message": "You are not entitled to this."},
    status=403,
)
TIMEFRAME = Reply(
    {
        "status": "NOT_AUTHORIZED",
        "request_id": "r",
        "message": "Your plan doesn't include this data timeframe. Please upgrade your plan.",
    },
    status=403,
)
PREVIOUS_DAY = Reply(
    envelope(
        [{"T": "AAPL", "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0, "t": 1793332800000}],
        ticker="AAPL",
        adjusted=True,
        resultsCount=1,
    )
)


def _day(text: str) -> date:
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, NEW_YORK).date()
    return date.fromisoformat(text)


def _bars(symbol: str, *, first: date, last: date) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    day = first
    price = 100.0 + len(symbol)
    while day <= last:
        if day.weekday() < 5:
            start = datetime.combine(day, day_time(), NEW_YORK)
            price *= 1.01 if day.toordinal() % 3 else 0.985
            rows.append(
                {
                    "o": price,
                    "h": price * 1.02,
                    "l": price * 0.98,
                    "c": price * 1.005,
                    "v": 1_000_000.0,
                    "vw": price,
                    "n": 1000,
                    "t": int(start.timestamp() * 1000),
                }
            )
        day += timedelta(days=1)
    return rows


class MarketServer(FakeRestServer):
    """A fake REST server that also invents daily bars for any range, and company facts."""

    @override
    def _next(self, received: Received) -> tuple[Reply, Callable[[Received], None] | None]:
        path = received.target.partition("?")[0]
        bars = AGGS.match(path)
        details = DETAILS.match(path)
        if bars is None and details is None:
            return super()._next(received)
        with self._lock:
            self._received.append(received)
        if details is not None:
            symbol = details.group(1)
            facts = {
                "ticker": symbol,
                "name": f"{symbol} Inc.",
                "primary_exchange": "XNAS",
                "list_date": "1980-12-12",
                "market_cap": 1e12,
                "active": True,
            }
            return Reply(envelope(facts)), None
        assert bars is not None
        symbol, _, _, first, last = bars.groups()
        if _day(first).year < 2020:
            return TIMEFRAME, None
        rows = _bars(symbol, first=_day(first), last=_day(last))
        body = envelope(rows, ticker=symbol, adjusted=True, resultsCount=len(rows), queryCount=1)
        return Reply(body), None


@pytest.fixture
def market(
    monkeypatch: pytest.MonkeyPatch, load_fixture: Callable[[str, str], bytes]
) -> Iterator[MarketServer]:
    """Start the fake market, and point every new `MassiveConfig` at it with a planted key.

    Yields:
        The server.
    """
    server = MarketServer()
    server.start()
    for path, name in (
        ("/v1/marketstatus/now", "reference/market_status.json"),
        ("/v1/marketstatus/upcoming", "reference/market_holidays.json"),
        ("/fed/v1/treasury-yields", "economy/treasury_yields.json"),
        ("/stocks/v1/dividends", "stocks/dividends.json"),
    ):
        server.reply(path, Reply(load_fixture("namespaces", name)))
    for symbol in ("AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"):
        server.reply(f"/v2/aggs/ticker/{symbol}/prev", PREVIOUS_DAY)
    for path in ("/v2/last/trade/AAPL", "/v3/snapshot"):
        server.reply(path, REFUSED)
    # The first example turns the debug log on: give it a switch of its own, and take its
    # handler off again afterwards, so no later test inherits the log.
    monkeypatch.setattr(_debug_log, "_switch", _debug_log._Switch())
    library = logging.getLogger("deephaven_massive")
    level, handlers = library.level, list(library.handlers)
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    defaults = MassiveConfig.__init__.__kwdefaults__
    assert defaults is not None
    monkeypatch.setitem(defaults, "rest_base_url", server.url)
    yield server
    server.close()
    for handler in [h for h in library.handlers if h not in handlers]:
        library.removeHandler(handler)
        handler.close()
    library.setLevel(level)


def _clients_reach(server: FakeMassiveServer, monkeypatch: pytest.MonkeyPatch) -> None:
    # Every WebSocket client a session makes connects to the fake server's endpoint of its
    # path, with a short backoff, so no stream reaches Massive.
    def fake(
        _factory: object,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[tuple[object, ...]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
        reconnect_deadline: Callable[[], float | None],
    ) -> _massive.WsClient:
        path = "/" + url.split("/", 3)[3]
        return _massive.WsClient(
            server.url(path),
            api_key=KEY,
            sources=sources,
            on_open=on_open,
            on_status=on_status,
            on_data=on_data,
            on_state=on_state,
            reconnect_backoff=0.05,
            max_reconnect_backoff=0.1,
            reconnect_deadline=reconnect_deadline,
            thread_prefix="tester examples",
        )

    monkeypatch.setattr(ws_connection._WsClientFactory, "__call__", fake)


@pytest.mark.jvm_held
@pytest.mark.timeout(300)  # 22 examples, Deephaven compiling each formula, on a busy worker
def test_every_example_runs_against_the_fake_market(
    market: MarketServer,
    tmp_path: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
    fake_ws_server: FakeMassiveServer,
) -> None:
    monkeypatch.setenv("MASSIVE_TEST_FOLDER", str(tmp_path))
    # The free plan's snapshot is refused, so the live minute bars open on the real-time host
    # and Massive's live connection refuses their topic.
    _clients_reach(fake_ws_server, monkeypatch)
    fake_ws_server.refuse["AM.AAPL"] = "You are not entitled to this data."
    files = [p for p in examples.example_files(EXAMPLES) if p.name not in LIVE]
    lines: list[str] = []
    outcomes = examples.run_examples(
        files, tmp_path / "out", secrets=SecretFilter((KEY,)), echo=lines.append
    )
    failures = [examples.summary_line(o) for o in outcomes if o.status != examples.PASSED]
    assert failures == []
    assert [o.name for o in outcomes] == [p.name for p in files]
    out = tmp_path / "out"
    printed = {p.stem: (out / f"{p.stem}.txt").read_text() for p in files}
    assert re.search(r"<MassiveSession #\d+ open>", printed["01_start_a_session"])
    query = "m.stocks.bars(['AAPL', 'MSFT', 'NVDA'], dm.Timespan.DAY)"
    assert query in printed["02_explore_datasets"]
    assert printed["11_plan_refusals"].count(": EntitlementError: ") == 2
    assert "live minute bars: opened;" in printed["11_plan_refusals"]
    assert printed["12_symbols"].splitlines()[:4] == ["C:EURUSD", "X:BTCUSD", "I:SPX", "BRK.B"]
    assert "0 copies of the key" in printed["13_check_the_log"]
    assert re.search(r"<MassiveSession #\d+ closed>", printed["22_finish"])
    assert re.search(r"MassiveError: MassiveSession #\d+ is closed", printed["22_finish"])
    assert (out / "tables" / "03_daily_bars" / "daily.csv").exists()
    assert (tmp_path / "growth.parquet").exists()
    assert sorted(p.name for p in (tmp_path / "diagnostics").iterdir()) == [
        "connections.csv",
        "rate_limits.csv",
        "streams.csv",
    ]
    assert KEY not in "".join(p.read_text() for p in out.rglob("*.*"))
    assert len((out / "summary.txt").read_text().splitlines()) == len(files)


@pytest.mark.jvm_held
def test_the_paid_examples_run_on_streams_from_the_fake_server(
    market: MarketServer,
    make_ws_session: Callable[..., WsSession],
    *,
    fake_ws_server: FakeMassiveServer,
) -> None:
    session = make_ws_session(rest_base_url=market.url)
    namespace: dict[str, Any] = {"m": session.session, "dm": dm}
    for name in LIVE:
        outcome, _ = examples.run_example(EXAMPLES / name, namespace)
        assert (outcome.name, outcome.status, outcome.error) == (name, examples.PASSED, "")
    made = examples.tables_of(namespace)
    for name in ("latest_bars", "aapl_today", "crypto_latest", "trades_per_minute"):
        assert name in made


def test_the_guide_shows_each_example_exactly_as_its_file() -> None:
    blocks = examples.guide_blocks(GUIDE.read_text(encoding="utf-8"))
    files = {p.name: p.read_text(encoding="utf-8") for p in examples.example_files(EXAMPLES)}
    assert blocks == files


def test_every_example_starts_with_a_comment() -> None:
    for path in examples.example_files(EXAMPLES):
        assert path.read_text(encoding="utf-8").startswith("# "), path.name


GUIDE_SAMPLE = """\
## 1. First (`01_first.py`)
Some prose.
```bash
not python
```
```python
x = 1

```
```python
not this one
```
### A heading without a file
```python
nor this
```
"""


def test_guide_blocks_reads_the_first_python_block_under_each_named_heading() -> None:
    assert examples.guide_blocks(GUIDE_SAMPLE) == {"01_first.py": "x = 1\n\n"}


def test_example_files_keep_only_numbered_python_files(tmp_path: Path) -> None:
    for name in ("02_b.py", "01_a.py", "notes.md", "helper.py", "1_short.py"):
        (tmp_path / name).write_text("", encoding="utf-8")
    assert [p.name for p in examples.example_files(tmp_path)] == ["01_a.py", "02_b.py"]


def test_the_first_example_opens_the_free_plans_session() -> None:
    assert examples.FREE_PLAN_SESSION in (EXAMPLES / "01_start_a_session.py").read_text()


def test_session_line_fits_the_keys_plans() -> None:
    free = dict.fromkeys(("stocks", "options", "indices", "currencies", "futures"), "basic")
    assert examples.session_line(free) == examples.FREE_PLAN_SESSION
    # Starter and Developer stream delayed data, except currencies, which have no delayed feed.
    paid = {
        "stocks": "developer",
        "options": "advanced",
        "indices": "basic",
        "currencies": "starter",
        "futures": "starter",
    }
    assert examples.session_line(paid) == (
        "m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions="
        "{dm.Subscription.INDICES}, delayed_subscriptions={dm.Subscription.FUTURES, "
        "dm.Subscription.STOCKS}))\n"
    )
    assert examples.session_line({"stocks": "advanced"}) == (
        "m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(), "
        "delayed_subscriptions=set()))\n"
    )


def test_fitted_runs_a_copy_only_when_the_session_line_changes(tmp_path: Path) -> None:
    first = _write(tmp_path, "01_a.py", f"x = 1\n{examples.FREE_PLAN_SESSION}")
    other = _write(tmp_path, "02_b.py", "y = 2\n")
    paid = "m = dm.MassiveSession()\n"
    assert examples.fitted(first, examples.FREE_PLAN_SESSION, tmp_path / "fitted") == first
    assert examples.fitted(other, paid, tmp_path / "fitted") == other
    copy = examples.fitted(first, paid, tmp_path / "fitted")
    assert copy == tmp_path / "fitted" / "01_a.py"
    assert copy.read_text(encoding="utf-8") == f"x = 1\n{paid}"


def _write(folder: Path, name: str, code: str) -> Path:
    path = folder / name
    path.write_text(code, encoding="utf-8")
    return path


def test_run_example_shares_variables_and_captures_prints(tmp_path: Path) -> None:
    namespace: dict[str, Any] = {}
    first = _write(tmp_path, "01_a.py", "x = 2\nprint('hello')\n")
    outcome, printed = examples.run_example(first, namespace)
    assert (outcome.status, printed, namespace["x"]) == (examples.PASSED, "hello\n", 2)
    second = _write(tmp_path, "02_b.py", "y = x * 3\n")
    examples.run_example(second, namespace)
    assert namespace["y"] == 6


def test_run_example_tells_a_plan_refusal_from_a_failure_under_the_console_lock(
    tmp_path: Path,
) -> None:
    code = "import deephaven_massive as dm\nraise dm.EntitlementError('no plan')\n"
    refused = _write(tmp_path, "01_a.py", code)
    failed = _write(tmp_path, "02_b.py", "print('before')\n1 / 0\n")
    namespace: dict[str, Any] = {"kept": 1}
    outcome, _ = examples.run_example(refused, namespace)
    assert (outcome.status, outcome.error) == (examples.REFUSED, "EntitlementError: no plan")
    outcome, printed = examples.run_example(failed, namespace)
    assert outcome.status == examples.FAILED
    assert outcome.error.startswith("ZeroDivisionError: division by zero\nTraceback")
    assert printed == "before\n"
    assert namespace == {"kept": 1}


def test_run_example_holds_the_lock_it_is_given(tmp_path: Path) -> None:
    held: list[str] = []

    @contextlib.contextmanager
    def lock() -> Iterator[None]:
        held.append("in")
        yield
        held.append("out")

    examples.run_example(_write(tmp_path, "01_a.py", "pass\n"), {}, lock=lock)
    assert held == ["in", "out"]


def test_run_example_holds_the_update_graphs_exclusive_lock_by_default(tmp_path: Path) -> None:
    code = (
        "from deephaven.execution_context import get_exec_ctx\n"
        "lock = get_exec_ctx().update_graph.j_update_graph.exclusiveLock()\n"
        "held = lock.isHeldByCurrentThread()\n"
    )
    namespace: dict[str, Any] = {}
    examples.run_example(_write(tmp_path, "01_a.py", code), namespace)
    assert namespace["held"] is True


def test_run_examples_writes_errors_tables_and_a_summary_without_the_secret(
    tmp_path: Path,
) -> None:
    folder = tmp_path / "examples"
    folder.mkdir()
    code = "from deephaven import empty_table\nt = empty_table(3)\nprint('K-1')\n"
    _write(folder, "01_a.py", code)
    _write(folder, "02_b.py", "raise RuntimeError('K-1 broke')\n")
    _write(folder, "03_c.py", "u = t\nv = t.update('X = 1')\n")
    lines: list[str] = []
    outcomes = examples.run_examples(
        examples.example_files(folder),
        tmp_path / "out",
        secrets=SecretFilter(("K-1",)),
        echo=lines.append,
        wait=lambda _namespace: False,
    )
    assert [o.status for o in outcomes] == [examples.PASSED, examples.FAILED, examples.PASSED]
    out = tmp_path / "out"
    assert (out / "01_a.txt").read_text() == "***\n"
    assert "RuntimeError: *** broke" in (out / "02_b.txt").read_text()
    assert sorted(p.name for p in (out / "tables" / "03_c").iterdir()) == ["u.csv", "v.csv"]
    assert (out / "tables" / "01_a" / "t.csv").read_text().count("\n") == 4
    summary = (out / "summary.txt").read_text()
    assert "02_b.py: failed" in summary
    assert "RuntimeError: *** broke" in summary
    assert "K-1" not in "".join(lines)
    assert lines[0] == "a background download is still running; going on without it"


def test_run_examples_closes_the_sessions_left_open(market: MarketServer, tmp_path: Path) -> None:
    folder = tmp_path / "examples"
    folder.mkdir()
    _write(folder, "01_a.py", "import deephaven_massive as dm\nm = dm.MassiveSession()\n")
    seen: list[weakref.ref[SessionCore]] = []

    def remember(namespace: dict[str, Any]) -> bool:
        seen.extend(weakref.ref(s._core) for s in examples.sessions(namespace))
        return True

    _write(folder, "02_b.py", "pass\n")
    files = examples.example_files(folder)
    examples.run_examples(
        files, tmp_path / "out", secrets=SecretFilter(()), echo=print, wait=remember
    )
    assert len(seen) == 1  # seen before the second example, after the first made it
    core = seen[0]()
    assert core is None or core.closed
    del core


def test_sessions_lists_each_session_once(market: MarketServer) -> None:
    with dm.MassiveSession() as session:
        assert examples.sessions({"a": session, "b": session, "c": 3}) == [session]


def test_loading_counts_background_downloads_and_none_once_closed(market: MarketServer) -> None:
    session = dm.MassiveSession()
    year = session.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-01-02", wait=False)
    assert examples.loading(session) in {0, 1}
    # The download's LOADING row reaches `streams()` on the diagnostics writer's thread: wait
    # for it, or the wait below can see no download yet and return before it starts.
    assert session._core.diagnostics.wait_until_shown(30.0)
    assert examples.wait_for_downloads({"m": session})
    assert examples.loading(session) == 0
    session.close()
    assert examples.loading(session) == 0
    del year


class Ticks:
    """A clock that moves only when something sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = 0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        self.sleeps += 1


def test_wait_for_downloads_returns_once_nothing_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    counts = iter([2, 1, 0])
    monkeypatch.setattr(examples, "sessions", lambda _namespace: ["one"])
    monkeypatch.setattr(examples, "loading", lambda _session: next(counts))
    ticks = Ticks()
    assert examples.wait_for_downloads({}, timeout=10, clock=ticks.clock, sleep=ticks.sleep)
    assert ticks.sleeps == 2


def test_wait_for_downloads_gives_up_at_the_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(examples, "sessions", lambda _namespace: ["one"])
    monkeypatch.setattr(examples, "loading", lambda _session: 1)
    ticks = Ticks()
    assert not examples.wait_for_downloads({}, timeout=3, clock=ticks.clock, sleep=ticks.sleep)
    assert ticks.now == 3


def test_summary_line_names_the_first_line_of_an_error() -> None:
    passed = examples.Outcome("01_a.py", examples.PASSED, 1.5, "")
    failed = examples.Outcome("02_b.py", examples.FAILED, 2.0, "ValueError: bad\nTraceback\n")
    assert examples.summary_line(passed) == "01_a.py: passed (1.5 s)"
    assert examples.summary_line(failed) == "02_b.py: failed (2 s) - ValueError: bad"


def test_tables_of_skips_private_names_and_other_values() -> None:
    table: Table = empty_table(1)
    assert examples.tables_of({"t": table, "_hidden": table, "n": 1}) == {"t": table}


class BrokenTable:
    """Stands in for a refreshing table that failed: reading it raises."""

    def head(self, _rows: int) -> Table:
        raise RuntimeError("the stream failed: K-2\nmore detail")


def test_save_tables_records_a_table_it_cannot_read_and_goes_on(tmp_path: Path) -> None:
    tables = {"broken": cast("Table", BrokenTable()), "fine": empty_table(1)}
    unsaved = examples.save_tables(tables, tmp_path, SecretFilter(("K-2",)))
    assert unsaved == ["broken: RuntimeError: the stream failed: K-2"]
    assert (tmp_path / "broken.error.txt").read_text() == (
        "broken: RuntimeError: the stream failed: ***\n"
    )
    assert (tmp_path / "fine.csv").exists()


def test_run_examples_names_a_table_it_could_not_save(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "examples"
    folder.mkdir()
    _write(folder, "01_a.py", "from deephaven import empty_table\nt = empty_table(1)\n")
    _write(folder, "02_b.py", "pass\n")
    monkeypatch.setattr(examples, "save_tables", lambda tables, _f, _s: [f"{n}: X" for n in tables])
    lines: list[str] = []
    examples.run_examples(
        examples.example_files(folder),
        tmp_path / "out",
        secrets=SecretFilter(()),
        echo=lines.append,
        wait=lambda _namespace: True,
    )
    assert "  table not saved: t: X" in lines
    assert (tmp_path / "out" / "01_a.txt").read_text() == "\ntable not saved: t: X"
    assert lines[-1].startswith("02_b.py: passed")
