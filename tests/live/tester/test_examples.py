"""Tests of the tester examples' runner (`tests/_live/tester/examples.py`), and the examples.

Every example of `docs/tester-examples/` runs here, in order, against a fake Massive REST
server (`MarketServer`) that refuses the snapshot and the last trade, and the examples that
stream run on a session whose WebSocket connections reach the fake WebSocket server. The
guide's code blocks are checked against the files, so the tester pastes exactly the code
these tests run.
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
import weakref
from datetime import UTC, date, datetime, timedelta
from datetime import time as day_time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pytest
from deephaven import empty_table
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _debug_log, _massive
from deephaven_massive._config import MassiveConfig
from deephaven_massive._datasets import history_source
from deephaven_massive._ingest import ws_connection
from deephaven_massive._ingest.recency_labels import delayed_refusal
from tests._fakes.rest import FakeRestServer, Received, Reply, envelope
from tests._fakes.ws import status_frame
from tests._live.live_run.secret_scrub import SecretFilter
from tests._live.tester import examples

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from deephaven.table import Table

    from deephaven_massive._massive import DriftReport, RowMarks, Source, Status, WireError, WsState
    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.ws import FakeMassiveServer
    from tests._fakes.ws_session import WsSession

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "docs" / "tester-examples"
GUIDE = ROOT / "docs" / "tester-guide.md"
KEY = "PLANTED-tester-key-41c7"
NEW_YORK = ZoneInfo("America/New_York")
LIVE = (
    "17_live_minute_bars.py",
    "18_history_plus_live.py",
    "19_live_crypto.py",
    "21_crypto_watchlist.py",
    "22_gainers_trades.py",
)
"""The examples that stream, which run on the fake WebSocket server."""

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
        ("/v2/snapshot/locale/us/markets/stocks/gainers", "stocks/movers.json"),
    ):
        server.reply(path, Reply(load_fixture("namespaces", name)))
    for symbol in ("AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM"):
        server.reply(f"/v2/aggs/ticker/{symbol}/prev", PREVIOUS_DAY)
    for path in ("/v2/last/trade/AAPL", "/v3/snapshot"):
        server.reply(path, REFUSED)
    # Example 20's contracts of a futures product: one ES contract.
    contract = {"ticker": "ESZ6", "product_code": "ES", "date": "2026-10-30"}
    server.reply("/futures/v1/contracts", Reply(envelope([contract])))
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
        on_data: Callable[[int, str, list[tuple[object, ...]], RowMarks, DriftReport], None],
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
@pytest.mark.timeout(300)  # 25 examples, Deephaven compiling each formula, on a busy worker
def test_every_example_runs_against_the_fake_market(
    market: MarketServer,
    tmp_path: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
    fake_ws_server: FakeMassiveServer,
) -> None:
    # The fake refuses the snapshot, so the live minute bars open on the real-time host
    # and Massive's live connection refuses their topic. The fake answers only once every
    # example has returned: a refusal that came while example 10 still built its table would
    # fail the open instead, so an immediate answer would race the example.
    _clients_reach(fake_ws_server, monkeypatch)
    fake_ws_server.silent.add("AM.AAPL")

    def refuse(opened: list[dm.MassiveSession]) -> None:
        # Example 10's connection is the run's only WebSocket connection. The refusal names
        # the topic, so it fails the stream whether or not its acknowledgement's deadline passed.
        fake_ws_server.send(status_frame("error", "You are not entitled to AM.AAPL."))
        pace = threading.Event()  # never set; only paces the loop
        for _ in range(1500):
            if any(examples.failed_streams(session) for session in opened):
                return
            pace.wait(0.02)

    paced = threading.Event()  # never set; only paces the looks at the downloads

    def look_again(_seconds: float) -> None:
        # The fake market serves a download in milliseconds: look every 20 ms, not every second.
        paced.wait(0.02)

    def downloaded(namespace: dict[str, Any]) -> bool:
        return examples.wait_for_downloads(namespace, sleep=look_again)

    files = [p for p in examples.example_files(EXAMPLES) if p.name not in LIVE]
    lines: list[str] = []
    outcomes = examples.run_examples(
        files,
        tmp_path / "out",
        tmp_path,
        secrets=SecretFilter((KEY,)),
        echo=lines.append,
        wait=downloaded,
        settle=refuse,
    )
    # Example 10's live minute bars return before Massive refuses them, and the run reads the
    # refusal from their stream.
    verdicts = {o.name: o.status for o in outcomes if o.status != examples.PASSED}
    assert verdicts == {"10_plan_refusals.py": examples.REFUSED}
    refusal = next(o for o in outcomes if o.status == examples.REFUSED)
    assert "AM.AAPL" in refusal.error
    assert [o.name for o in outcomes] == [p.name for p in files]
    out = tmp_path / "out"
    printed = {p.stem: (out / f"{p.stem}.txt").read_text() for p in files}
    assert re.search(r"<MassiveSession #\d+ open>", printed["01_start_a_session"])
    query = "m.stocks.bars(['AAPL', 'MSFT', 'NVDA'], dm.Timespan.DAY)"
    assert query in printed["02_explore_datasets"]
    assert printed["10_plan_refusals"].count(": EntitlementError: ") == 2
    assert "live minute bars: opened;" in printed["10_plan_refusals"]
    assert printed["11_symbols"].splitlines()[:4] == ["C:EURUSD", "X:BTCUSD", "I:SPX", "BRK.B"]
    assert "0 copies of the key" in printed["12_check_the_log"]
    assert re.search(r"<MassiveSession #\d+ closed>", printed["25_finish"])
    assert re.search(r"MassiveError: This session is closed\.", printed["25_finish"])
    assert (out / "tables" / "03_daily_bars" / "daily.csv").exists()
    assert (out / "tables" / "20_watchlist_poll" / "watched_bars.csv").exists()
    assert (out / "tables" / "20_watchlist_poll" / "watched_contracts.csv").exists()
    assert (out / "tables" / "23_followed_history" / "followed.csv").exists()
    assert sorted(p.name for p in (tmp_path / "diagnostics").iterdir()) == [
        "connections.csv",
        "problems.csv",
        "rate_limits.csv",
        "streams.csv",
        "symbol_reads.csv",
    ]
    assert KEY not in "".join(p.read_text() for p in out.rglob("*.*"))
    assert len((out / "summary.txt").read_text().splitlines()) == len(files)


def _on_the_given_session(path: Path, folder: Path) -> Path:
    # A copy of the example without its session line, so it runs on the `m` a test gives it.
    copy = folder / path.name
    copy.write_text(examples.SESSION_LINE.sub("", path.read_text(encoding="utf-8")))
    return copy


@pytest.mark.jvm_held
def test_the_live_examples_run_on_streams_from_the_fake_server(
    market: MarketServer,
    make_ws_session: Callable[..., WsSession],
    tmp_path: Path,
    *,
    fake_ws_server: FakeMassiveServer,
) -> None:
    session = make_ws_session(rest_base_url=market.url)
    namespace: dict[str, Any] = {"m": session.session, "dm": dm}
    for name in LIVE:
        runs = _on_the_given_session(EXAMPLES / name, tmp_path)
        outcome, _ = examples.run_example(runs, namespace)
        assert (outcome.name, outcome.status, outcome.error) == (name, examples.PASSED, "")
    made = examples.tables_of(namespace)
    for name in (
        "latest_bars",
        "aapl_today",
        "crypto_latest",
        "trades_per_minute",
        "latest_trades",
        "gainer_trades",
    ):
        assert name in made


@pytest.mark.jvm_held
def test_history_plus_live_keeps_one_row_per_minute(
    make_ws_session: Callable[..., WsSession],
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
    *,
    tmp_path: Path,
) -> None:
    # History holds 04:00 and 04:01 New York time; the stream then sends 04:01 again, as
    # Massive recomputed it, and 04:02. The table keeps one row per minute: the stream's
    # where both hold it.
    day = datetime.now(NEW_YORK).date()
    midnight = datetime.combine(day, day_time(), NEW_YORK)
    monkeypatch.setattr(history_source, "_now", lambda: midnight + timedelta(hours=23))

    def minute(index: int) -> int:
        return int((midnight + timedelta(hours=4, minutes=index)).timestamp() * 1000)

    rest = FakeRestServer()
    rest.start()
    try:
        first, last = minute(-240), minute(24 * 60 - 240) - 1
        rows = [
            {"o": c, "h": c, "l": c, "c": c, "v": 1.0, "t": minute(i)}
            for i, c in [(0, 1.0), (1, 2.0)]
        ]
        rest.reply(
            f"/v2/aggs/ticker/AAPL/range/1/minute/{first}/{last}",
            Reply(envelope(rows, ticker="AAPL", adjusted=True, resultsCount=2)),
        )
        session = make_ws_session(rest_base_url=rest.url)
        namespace: dict[str, Any] = {"m": session.session, "dm": dm}
        runs = _on_the_given_session(EXAMPLES / "18_history_plus_live.py", tmp_path)
        outcome, _ = examples.run_example(runs, namespace)
        assert (outcome.status, outcome.error) == (examples.PASSED, "")
        session.subscribed("AM.AAPL")
        live = [
            {"ev": "AM", "sym": "AAPL", "v": 1, "o": c, "c": c, "h": c, "l": c}
            | {"s": minute(i), "e": minute(i + 1)}
            for i, c in [(1, 3.0), (2, 4.0)]
        ]
        session.publish(*live)
        # History reads in the background, so it may land before or after the stream's
        # minutes: the rows are compared oldest first.
        table = namespace["aapl_today"].sort("TimestampStart")
        pace = threading.Event()  # never set; only paces the loop
        made = dh_to_pylist(table)
        for _ in range(500):
            if len(made) == 3 and made[1]["Close"] == 3.0:
                break
            pace.wait(0.02)
            made = dh_to_pylist(table)
    finally:
        rest.close()
    starts = [datetime.fromtimestamp(minute(i) / 1000, UTC) for i in range(3)]
    assert [(r["TimestampStart"], r["Close"]) for r in made] == list(
        zip(starts, [1.0, 3.0, 4.0], strict=True)
    )


def test_the_guide_shows_each_example_exactly_as_its_file() -> None:
    blocks = examples.guide_blocks(GUIDE.read_text(encoding="utf-8"))
    files = {
        int(p.name[:2]): p.read_text(encoding="utf-8") for p in examples.example_files(EXAMPLES)
    }
    # Two files with one number would collapse into one key, so count the files too.
    assert len(files) == len(examples.example_files(EXAMPLES))
    assert blocks == files


GUIDE_SAMPLE = """\
#### 1. First
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
### 2. A step, not an example
```python
nor this
```
#### 3. An example with no code
### A heading without a number
```python
nor this either
```
"""


def test_guide_blocks_reads_the_first_python_block_under_each_numbered_example() -> None:
    assert examples.guide_blocks(GUIDE_SAMPLE) == {1: "x = 1\n\n"}


def test_example_files_keep_only_numbered_python_files(tmp_path: Path) -> None:
    for name in ("02_b.py", "01_a.py", "notes.md", "helper.py", "1_short.py"):
        (tmp_path / name).write_text("", encoding="utf-8")
    assert [p.name for p in examples.example_files(tmp_path)] == ["01_a.py", "02_b.py"]


def test_every_example_but_the_diagnostics_opens_its_own_session() -> None:
    # Each runs as pasted into a fresh console; saving the diagnostics reads the session of
    # the example before it, and converting symbols and reading the log need none.
    without = {"11_symbols.py", "12_check_the_log.py", "24_save_diagnostics.py"}
    for path in examples.example_files(EXAMPLES):
        opens = examples.SESSION_LINE.search(path.read_text(encoding="utf-8")) is not None
        assert opens == (path.name not in without), path.name
    assert "m = dm.MassiveSession()\n" in (EXAMPLES / "01_start_a_session.py").read_text()


def test_settings_text_lists_the_delayed_subscriptions_sorted() -> None:
    assert examples.settings_text(("stocks", "futures")) == (
        "delayed_subscriptions: [futures, stocks]\n"
    )
    assert examples.settings_text(()) == "delayed_subscriptions: []\n"


def test_the_testers_own_settings_file_names_the_delayed_subscriptions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Which subscriptions Massive serves delayed is the tester's to say, in their own file."""
    monkeypatch.setenv("MASSIVE_API_KEY", "PLANTED-examples-key")
    settings = tmp_path / "settings.yaml"
    variable = {"DEEPHAVEN_MASSIVE_CONFIG_FILE": str(settings)}
    assert examples.listed_delayed(variable) is None
    settings.write_text("delayed_subscriptions: [stocks, futures]\n", encoding="utf-8")
    assert examples.listed_delayed(variable) == ("futures", "stocks")
    monkeypatch.setenv("HOME", str(tmp_path))  # SETTINGS_PATH is under the home folder
    assert examples.listed_delayed({}) is None


def test_fitted_runs_a_copy_only_when_the_example_names_a_path_of_the_run(tmp_path: Path) -> None:
    plain = _write(tmp_path, "01_a.py", "m = dm.MassiveSession()\n")
    reads = f'm = dm.MassiveSession(dm.MassiveConfig.from_file("{examples.SETTINGS_PATH}"))\n'
    settled = _write(tmp_path, "02_b.py", f"# live\n{reads}")
    logs = _write(tmp_path, "03_c.py", f'log = "{examples.TEST_FOLDER}/massive-debug.log"\n')
    settings, folder, copies = tmp_path / "settings.yaml", tmp_path / "t", tmp_path / "fitted"
    assert examples.fitted(plain, settings=settings, test_folder=folder, folder=copies) == plain
    copy = examples.fitted(settled, settings=settings, test_folder=folder, folder=copies)
    assert copy == copies / "02_b.py"
    assert copy.read_text(encoding="utf-8") == (
        f'# live\nm = dm.MassiveSession(dm.MassiveConfig.from_file("{settings}"))\n'
    )
    copy = examples.fitted(logs, settings=settings, test_folder=folder, folder=copies)
    assert copy.read_text(encoding="utf-8") == f'log = "{folder}/massive-debug.log"\n'


def test_stream_verdict_turns_a_pass_with_failed_streams_into_their_verdict() -> None:
    passed = examples.Outcome("17_a.py", examples.PASSED, 1.0, "")
    refusal = (
        'Massive answered the subscription to AM.AAPL on wss://socket.massive.com/stocks: "not '
        'authorized".'
    )
    assert examples.STREAM_REFUSAL in refusal
    assert examples.stream_verdict(passed, []) is passed
    refused = examples.stream_verdict(passed, [refusal])
    assert (refused.status, refused.error) == (examples.REFUSED, f"a stream failed: {refusal}")
    failed = examples.stream_verdict(passed, [refusal, "the connection closed"])
    assert failed.status == examples.FAILED
    already = examples.Outcome("10_b.py", examples.FAILED, 1.0, "RuntimeError: x")
    assert examples.stream_verdict(already, [refusal]) is already


def test_stream_verdict_tells_a_refused_read_from_an_unlisted_delay() -> None:
    passed = examples.Outcome("18_a.py", examples.PASSED, 1.0, "")
    answer = 'Massive answered GET /v2/aggs with HTTP 403: "You are not entitled to this data".'
    plan = f"stocks.bars: Massive refused this read for the Stocks subscription. {answer}"
    s3 = 'Massive answered the read of s3://flatfiles/x with HTTP 403: "AccessDenied".'
    files = [
        f"stocks.trades: Massive refused its flat files. {s3}",
        (
            f"futures.trades: Massive refused the flat files of NYMEX. {s3} Leave "
            "dm.FuturesVenue.NYMEX out of venues."
        ),
        (
            "futures.trades: Massive refused the flat files of every venue the read asked for, "
            f"CME and NYMEX. {s3}"
        ),
    ]
    delayed = str(
        delayed_refusal(
            dm.Subscription.STOCKS,
            "stocks.bars data",
            "timeframe=DELAYED",
            listed=set(),
            streams=False,
        )
    )
    for refusal in [plan, *files]:
        assert examples.stream_verdict(passed, [refusal]).status == examples.REFUSED, refusal
    for other in [
        f"stocks.trades history: Massive refused the flat files. {answer}",
        "Massive refused the flat files of NYMEX, so this table holds no rows of that exchange.",
    ]:
        assert examples.stream_verdict(passed, [other]).status == examples.FAILED, other
    assert examples.stream_verdict(passed, [delayed]).status == examples.NOT_LISTED
    assert examples.stream_verdict(passed, [plan, delayed]).status == examples.NOT_LISTED
    assert examples.stream_verdict(passed, [plan, "HTTP 500"]).status == examples.FAILED


def test_failed_streams_lists_none_once_the_session_is_closed(market: MarketServer) -> None:
    session = dm.MassiveSession()
    assert examples.failed_streams(session) == []
    session.close()
    assert examples.failed_streams(session) == []


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


def test_run_example_tells_a_refusal_from_a_failure_under_the_console_lock(
    tmp_path: Path,
) -> None:
    code = "import deephaven_massive as dm\nraise dm.EntitlementError('no plan', status=403)\n"
    unlisted = (
        "import deephaven_massive as dm\n"
        "raise dm.EntitlementError('delayed', source=dm.ErrorSource.USER)\n"
    )
    built = "import deephaven_massive as dm\nraise dm.EntitlementError('not entitled')\n"
    refused = _write(tmp_path, "01_a.py", code)
    failed = _write(tmp_path, "02_b.py", "print('before')\n1 / 0\n")
    delayed = _write(tmp_path, "03_c.py", unlisted)
    library = _write(tmp_path, "04_d.py", built)
    namespace: dict[str, Any] = {"kept": 1}
    outcome, _ = examples.run_example(refused, namespace)
    assert (outcome.status, outcome.error) == (examples.REFUSED, "EntitlementError: no plan")
    outcome, _ = examples.run_example(delayed, namespace)
    assert (outcome.status, outcome.error) == (examples.NOT_LISTED, "EntitlementError: delayed")
    # The library's own refusal, with no status of Massive's, is a finding, not Massive's.
    outcome, _ = examples.run_example(library, namespace)
    assert outcome.status == examples.FAILED
    assert outcome.error.startswith("EntitlementError: not entitled\nTraceback")
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
        tmp_path,
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
    settled: list[int] = []

    def remember(namespace: dict[str, Any]) -> bool:
        seen.extend(weakref.ref(s._core) for s in examples.sessions(namespace))
        return True

    _write(folder, "02_b.py", "pass\n")
    files = examples.example_files(folder)
    examples.run_examples(
        files,
        tmp_path / "out",
        tmp_path,
        secrets=SecretFilter(()),
        echo=print,
        wait=remember,
        settle=lambda opened: settled.append(len(opened)),
    )
    assert len(seen) == 1  # seen before the second example, after the first made it
    assert settled == [1]  # once, after the last example, with the session it opened
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
    broken: Table = BrokenTable()  # type: ignore[assignment]  # a table that fails to read
    tables = {"broken": broken, "fine": empty_table(1)}
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
        tmp_path,
        secrets=SecretFilter(()),
        echo=lines.append,
        wait=lambda _namespace: True,
    )
    assert "  table not saved: t: X" in lines
    assert (tmp_path / "out" / "01_a.txt").read_text() == "\ntable not saved: t: X"
    assert lines[-1].startswith("02_b.py: passed")
