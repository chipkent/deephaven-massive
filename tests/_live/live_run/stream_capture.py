"""The live run's streaming groups: a full trading day of live streams, and their throughput.

`stream-day` streams, for one trading day from 03:45 to 20:30 New York time, a watchlist of
every subscription's unit (`stream_units`), through the library's public verbs, so it
measures the library as users run it. The session uses the owner's settings, so a subscription
they list in `delayed_subscriptions` streams from the delayed feed.

- **Watchlists.** Stocks: minute and second bars of 20 stocks (with `KIMpL`, a preferred share,
  to check its spelling live), and their trades. Options: minute bars of the
  10 SPY contracts nearest the money at the nearest expiry, chosen from the chain at the start.
  Currencies: minute bars of two forex pairs and two crypto pairs, and the forex pairs' quotes.
  Indices: minute bars of `I:SPX` and `I:NDX`. Futures: minute bars of the front ES and NQ
  contracts (`contracts.front_contract`).
- **Shapes.** Each bars stream is also held all day as one `append_only()` and one `last_by()`
  table, so both shapes run. Every stream's rows reach the recorder through a `blink()` table
  and a listener that only copies each update's added rows onto a queue: a writer thread does
  the rest, so memory stays bounded and the update graph never waits.
- **Rows.** The writer appends each row to `rows/<stream>-<hour>.parquet` by the hour (UTC) of
  its `TimestampInsert`, counts rows per stream and minute into `minutes.csv`, and measures each
  row's arrival delay: `TimestampInsert` minus the event's own time (`Timestamp` for ticks,
  `TimestampEnd` for bars), a delayed feed's lag included, as Massive delivered it. Each hour's
  50th, 90th, and 99th percentiles and maximum go to `latency.csv`.
- **Samples.** Every 10 seconds the sampler appends one row per stream and per connection, from
  snapshots of `m.streams()` and `m.connections()`, to `samples-streams.csv` and
  `samples-connections.csv`, and one row of CPU seconds and JVM heap to `samples-process.csv`.
- **Gaps.** A child that starts again on the same day appends to the same files and records
  the time since the last sample in `gaps.csv`; more than 30 minutes of gaps fails the day.
- **Refusals.** Massive's refusal of a stream, whether its stream fails with it or it raises as
  the stream opens, and its refusal of the options chain, are the unit's alone: the other units
  stream on. So is the library's refusal of data Massive labels delayed while the owner's
  settings do not list its subscription in `delayed_subscriptions` (not listed).
- **Verdicts.** A unit none of whose streams could open, each refused or not listed, records no
  row: it is `refused`, quoting Massive's refusal, when Massive refused any of them, and
  otherwise skipped as not listed (`unopened_verdict`). One of whose streams could not open
  while others delivered passes, and its reason quotes why (`refusal_note`). A unit whose
  acknowledged streams recorded no row all day fails, once its class's session has opened
  (`SESSION_OPENS`), even when Massive refused its other streams: an acknowledged
  subscription that never delivers is a failure, not a quiet pass. A unit the
  owner lists as delayed skips instead, since the delayed feed's lag is not known in advance.

`throughput` holds, from 09:15, the busiest streams of every symbol (`dm.ALL`) as `last_by()`
tables: second bars of every stock, every crypto trade, and every forex quote. Its samples give
the rows a second of each stream over the measured hour (09:45 to 10:45 when the owner lists
stocks as delayed, 09:30 to 10:30 otherwise), the peak `BufferedRows`, heap, CPU, reconnects,
and any `SchemaDrift`, in `summary.json`; it passes when it sampled the whole hour without a gap,
and a unit none of whose streams could open is `refused` or skipped as not listed.

The overnight group (`stream_night`) reuses the recorder, the sampler, the gap records, and
the stream holding (`held_streams`) of the stream day.

Both run in a child process that started an embedded Deephaven server and opened the session;
the functions here take the session and run on the child's main thread, apart from the
listeners (the update graph's threads) and the writer thread.
"""

from __future__ import annotations

import contextlib
import csv
import importlib
import itertools
import json
import logging
import os
import queue
import threading
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Final, Protocol

import numpy as np

import deephaven_massive as dm
from tests._live.calendar import NEW_YORK
from tests._live.contracts import front_contract
from tests._live.summary import NOT_LISTED, REFUSED

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from pathlib import Path

    from deephaven.table import Table
    from deephaven.table_listener import TableListenerHandle

    from deephaven_massive import MassiveSession

STOCKS: Final = (
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "XOM", "SPY",
    "QQQ", "IWM", "BRK.B", "V", "UNH", "LLY", "AVGO", "COST", "WMT", "KIMpL",
)  # fmt: skip
"""The stream day's stocks: liquid names, two share classes, and a preferred share."""

FOREX: Final = ("C:EURUSD", "C:USDJPY")
CRYPTO: Final = ("X:BTCUSD", "X:ETHUSD")
INDICES: Final = ("I:SPX", "I:NDX")

SAMPLE_SECONDS: Final = 10.0
"""How often the sampler snapshots the diagnostics tables."""

MAX_GAP: Final = timedelta(minutes=30)
"""Gaps after which a stream day fails."""

GAP_THRESHOLD: Final = timedelta(seconds=60)
"""A pause between samples longer than this counts as a gap."""

DAY_START: Final = time(3, 45)
"""When a stream day starts, New York time (`timetable.STREAM_DAY`)."""

OPTION_CONTRACTS: Final = 10
"""How many SPY contracts the options unit streams."""

SESSION_OPENS: Final = {"stocks": time(4, 0), "options": time(9, 30), "indices": time(9, 30)}
"""When each class's session opens on a trading day, New York time: stock trades from the
extended hours, options and the S&P indices from the regular session. Stocks keep 04:00 though
the overnight session trades from 21:00 the evening before: stock trading has opened by 04:00
on every trading day, with or without that session, so a unit with no row by then has failed.
A class not listed, currencies and futures, trades through the night, so its session counts as
open from the stream day's start (`DAY_START`)."""

SESSION_GRACE: Final = timedelta(minutes=5)
"""How long past its session's open a unit may go without a row."""

REFUSAL_MARKS: Final = (
    "refused the subscription to",
    "HTTP 403",
    "NOT_AUTHORIZED",
    "not entitled",
    "may not include this data",
    "(FMV) data needs",
)
"""Text that marks a failed stream's error as a refusal by Massive: the library's wording of a
topic Massive's WebSocket server refused (`the WebSocket server <host> refused the subscription
to <topics>; Massive said: <its message>; <the feed's hint>`), an HTTP 403, a `NOT_AUTHORIZED`
envelope, and the hints of a refused topic, including a fair-market-value (FMV) topic's."""


def is_refusal(error: str) -> bool:
    """Tell whether a failed stream's error is Massive's refusal.

    Args:
        error: The stream's `Error`.

    Returns:
        True when it holds one of `REFUSAL_MARKS`.
    """
    return any(mark in error for mark in REFUSAL_MARKS)


NOT_LISTED_MARK: Final = "is not in MassiveConfig.delayed_subscriptions"
"""Text that marks a failed stream's error as the settings' refusal of delayed data: the
library's message names the setting that would accept it (`recency_labels.delayed_refusal`)."""


def is_not_listed(error: str) -> bool:
    """Tell whether a failed stream's error is the settings' refusal of delayed data.

    Massive labels the data delayed and the owner's settings do not list its subscription in
    `delayed_subscriptions`; the library refuses it, as the stream opens or later.

    Args:
        error: The stream's `Error`.

    Returns:
        True when it holds `NOT_LISTED_MARK`.
    """
    return NOT_LISTED_MARK in error


def split_failures(failed: list[str], opened: list[str]) -> tuple[list[str], list[str]]:
    """Split a unit's failures into refusals, Massive's and the settings', and the others.

    Args:
        failed: The errors of its failed streams (`failed_streams`).
        opened: The refusals of its streams, or of what they need, as they opened
            (`refusal_recorded`), Massive's and the settings' alike.

    Returns:
        The refusals, those at opening first, and the other failures. A failed stream's
        settings refusal (`is_not_listed`) is the error after `summary.NOT_LISTED`, as one at
        opening is. A stream refused as it opened may also leave a failed row with the same
        error; each refusal at opening absorbs at most one such row, so two streams refused
        alike count twice.
    """
    unmatched = list(opened)
    later: list[str] = []
    others: list[str] = []
    for error in failed:
        if not (is_refusal(error) or is_not_listed(error)):
            others.append(error)
            continue
        line = " ".join(error.split())
        match = next((o for o in unmatched if line in o), None)
        if match is not None:
            unmatched.remove(match)
        elif is_not_listed(error):
            later.append(NOT_LISTED + error)
        else:
            later.append(error)
    return opened + later, others


@contextlib.contextmanager
def refusal_recorded(name: str, unit: str, refused: dict[str, list[str]]) -> Iterator[None]:
    """Record a refusal of what a block opens as its unit's, and go on.

    A stream Massive refuses as it opens, or the options chain the options unit's contracts
    come from, raises at once instead of failing a stream later. So does one whose data Massive
    labels delayed while the owner's settings do not list its subscription as delayed: the
    library refuses it with an `EntitlementError` whose source is `ErrorSource.USER`.
    Recording either lets the group's other units stream on.

    Args:
        name: What the block opens, such as `"stocks-trades"` or `"options chain"`.
        unit: The unit it belongs to.
        refused: Receives the refusal under the unit: Massive's as `"<name>: <the error on one
            line>"`, the settings' as the same text after `summary.NOT_LISTED`.

    Yields:
        Control to the block.

    Raises:
        EntitlementError: When the block raises one whose source is neither Massive nor the
            user, such as a library defect.
    """
    try:
        yield
    except dm.EntitlementError as error:
        text = f"{name}: {' '.join(str(error).split())}"
        if error.source is dm.ErrorSource.MASSIVE:
            logger.info("Massive refused the %s unit's %s", unit, text)
        elif error.source is dm.ErrorSource.USER:
            logger.info("The settings do not list the %s unit's %s as delayed", unit, text)
            text = NOT_LISTED + text
        else:
            raise
        refused.setdefault(unit, []).append(text)


def unopened_verdict(refused: list[str]) -> tuple[str, str]:
    """Decide the result of a unit none of whose streams could open.

    Massive's refusal comes first: listing a subscription in the settings would not open a
    stream Massive refuses.

    Args:
        refused: The refusals of its streams (`split_failures`), at least one.

    Returns:
        `("refused", "refused: <error>")`, quoting Massive's first refusal when there is one;
        otherwise `("skipped", "not listed as delayed: <error>")`, the first not listed.
    """
    massive = [r for r in refused if not r.startswith(NOT_LISTED)]
    return ("refused", REFUSED + massive[0]) if massive else ("skipped", refused[0])


def refusal_note(refused: list[str]) -> str:
    """Describe a unit's first stream that could not open while others did.

    Args:
        refused: The refusals of its streams (`split_failures`), at least one.

    Returns:
        `"a stream refused: <error>"`, or `"a stream not listed as delayed: <error>"`.
    """
    first = refused[0]
    return f"a stream {first if first.startswith(NOT_LISTED) else REFUSED + first}"


_PERCENTILES: Final = (50, 90, 99)


@dataclass
class Stream:
    """One stream of a streaming group.

    Attributes:
        name: Its file name, such as `"stocks-minute-bars"`.
        unit: The streaming unit it belongs to, such as `"stocks"`.
        query: Builds its query on the session.
        event_column: The column of the event's own time: `Timestamp` or `TimestampEnd`.
        held: Whether its `append_only()` and `last_by()` tables are held too (bars).
    """

    name: str
    unit: str
    query: Callable[[MassiveSession], Any]
    event_column: str = "TimestampEnd"
    held: bool = True


def index_futures(day: date) -> list[str]:
    """Return the front E-mini S&P 500 and Nasdaq-100 contracts of a day.

    Args:
        day: The day; NQ rolls with ES (`contracts.front_contract`).

    Returns:
        Such as `["ESZ6", "NQZ6"]`.
    """
    month = front_contract("ES", day)[2:]
    return ["ES" + month, "NQ" + month]


def _futures_bars(today: date) -> Callable[[MassiveSession], Any]:
    """Return the query builder of the front ES and NQ contracts' minute bars on a day."""
    contracts = index_futures(today)
    return lambda m: m.futures.bars(contracts, dm.Timespan.MINUTE)


def option_contracts(rows: list[dict[str, Any]], today: date) -> list[str]:
    """Choose the SPY contracts nearest the money at the nearest expiry.

    Args:
        rows: The chain's rows: `Symbol`, `ExpirationDate`, `StrikePrice`, and
            `UnderlyingAssetPrice`.
        today: The New York date.

    Returns:
        Up to `OPTION_CONTRACTS` symbols, nearest strike first; empty for an empty chain.
    """
    live = [r for r in rows if (r["ExpirationDate"] or "") >= today.isoformat()]
    if not live:
        return []
    expiry = min(r["ExpirationDate"] for r in live)
    chosen = [r for r in live if r["ExpirationDate"] == expiry]
    prices = [r["UnderlyingAssetPrice"] for r in chosen if r["UnderlyingAssetPrice"] is not None]
    strikes = sorted(r["StrikePrice"] for r in chosen)
    middle = prices[0] if prices else strikes[len(strikes) // 2]
    chosen.sort(key=lambda r: (abs(r["StrikePrice"] - middle), r["Symbol"]))
    return [r["Symbol"] for r in chosen[:OPTION_CONTRACTS]]


def stream_day_streams(units: Iterable[str], *, today: date, options: list[str]) -> list[Stream]:
    """Return the stream day's streams for its due units.

    Args:
        units: The due units, such as `["stocks", "currencies"]`.
        today: The New York date, which picks the front futures contracts.
        options: The option contracts to stream (`option_contracts`).

    Returns:
        The streams, in the units' order.
    """
    streams: list[Stream] = []
    for unit in units:
        name = unit
        if name == "stocks":
            streams += [
                Stream(
                    "stocks-minute-bars", unit, lambda m: m.stocks.bars(STOCKS, dm.Timespan.MINUTE)
                ),
                Stream(
                    "stocks-second-bars", unit, lambda m: m.stocks.bars(STOCKS, dm.Timespan.SECOND)
                ),
                Stream(
                    "stocks-trades",
                    unit,
                    lambda m: m.stocks.trades(STOCKS),
                    event_column="Timestamp",
                    held=False,
                ),
            ]
        elif name == "options" and options:
            streams.append(
                Stream(
                    "options-minute-bars",
                    unit,
                    lambda m: m.options.bars(options, dm.Timespan.MINUTE),
                )
            )
        elif name == "currencies":
            streams.append(
                Stream("forex-minute-bars", unit, lambda m: m.forex.bars(FOREX, dm.Timespan.MINUTE))
            )
            streams.append(
                Stream(
                    "crypto-minute-bars", unit, lambda m: m.crypto.bars(CRYPTO, dm.Timespan.MINUTE)
                )
            )
            streams.append(
                Stream(
                    "forex-quotes",
                    unit,
                    lambda m: m.forex.quotes(FOREX),
                    event_column="Timestamp",
                    held=False,
                )
            )
        elif name == "indices":
            streams.append(
                Stream(
                    "indices-minute-bars",
                    unit,
                    lambda m: m.indices.bars(INDICES, dm.Timespan.MINUTE),
                )
            )
        elif name == "futures":
            streams.append(Stream("futures-minute-bars", unit, _futures_bars(today)))
    return streams


def throughput_streams(units: Iterable[str]) -> list[Stream]:
    """Return the throughput group's streams of every symbol, for its due units.

    Args:
        units: The due units: `stocks` and `currencies`.

    Returns:
        The streams: every stock's second bars, every crypto trade, every forex quote.
    """
    streams: list[Stream] = []
    for unit in units:
        if unit == "stocks":
            streams.append(
                Stream(
                    "stocks-all-second-bars",
                    unit,
                    lambda m: m.stocks.bars(dm.ALL, dm.Timespan.SECOND),
                )
            )
        else:
            streams.append(
                Stream("crypto-all-trades", unit, lambda m: m.crypto.trades(dm.ALL), "Timestamp")
            )
            streams.append(
                Stream("forex-all-quotes", unit, lambda m: m.forex.quotes(dm.ALL), "Timestamp")
            )
    return streams


def rows_of(table: Table) -> list[dict[str, Any]]:
    """Read a small table's rows under the update graph's shared lock.

    Args:
        table: The table, such as `m.streams()`.

    Returns:
        One dictionary per row.
    """
    arrow = importlib.import_module("deephaven.arrow")
    update_graph = importlib.import_module("deephaven.update_graph")
    with update_graph.shared_lock(table):
        rows: list[dict[str, Any]] = arrow.to_arrow(table).to_pylist()
    return rows


def diagnostics_rows(m: MassiveSession, view: Table) -> list[dict[str, Any]]:
    """Read a diagnostics view's rows, then release the view.

    Each `m.streams()`, `m.connections()`, or `m.rate_limits()` call makes a new view that
    the session tracks like a live result until it is released or collected, so a sampler
    that only dropped its views would keep them until the JVM collects them.

    Args:
        m: The session.
        view: A view the session just made.

    Returns:
        One dictionary per row.
    """
    try:
        return rows_of(view)
    finally:
        m.release(view)


DIAGNOSTICS_SHOWN: Final = 30.0
"""The most seconds `wait_for_diagnostics` waits for the queued diagnostics rows to show."""


def wait_for_diagnostics(m: MassiveSession) -> None:
    """Wait until every diagnostics row the session has queued shows in its views.

    The session's writer thread adds each diagnostics row, and the row reaches a view only on
    the next update cycle, so a read right after a stream fails, or right after a call changes
    a rate limit, can still show the row from before. Call it before a read whose rows decide
    a verdict. It waits at most `DIAGNOSTICS_SHOWN` seconds; past that the library logs a
    WARNING and the read that follows may miss the latest rows.

    Args:
        m: The session; not on an update-graph thread, and not holding the update graph's
            lock, under which no cycle runs.
    """
    m._core.diagnostics.wait_until_shown(DIAGNOSTICS_SHOWN)


def append_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Append rows to a CSV file, writing its header when the file is new.

    Args:
        path: The file.
        rows: The rows; the first one's keys name the columns.
    """
    if not rows:
        return
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]), extrasaction="ignore")
        if new:
            writer.writeheader()
        writer.writerows(rows)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write rows to a CSV file, replacing what it held, so writing them again duplicates none.

    Args:
        path: The file; removed when there are no rows, so no stale rows stay.
        rows: The rows; the first one's keys name the columns.
    """
    path.unlink(missing_ok=True)
    append_csv(path, rows)


@dataclass
class _Batch:
    """One update's added rows of one stream, as the listener copied them."""

    stream: Stream
    columns: dict[str, np.ndarray[Any, Any]]


@dataclass
class _StreamFiles:
    """What the writer keeps open for one stream: its hour's Parquet file and latencies."""

    hour: str = ""
    writer: Any = None
    delays: list[np.ndarray[Any, Any]] = field(default_factory=list)


class Recorder:
    """The writer thread behind every stream's listener.

    Created by `run_stream_day` with the attempt's folder; each listener calls `put` on an
    update-graph thread, which only queues the added rows. The writer thread appends them to
    the hour's Parquet file, counts them per minute, and keeps their arrival delays until the
    hour ends. `close` writes what is queued and closes every file. Thread-safe: only the
    writer thread touches the files, and the counts are behind a lock.
    """

    def __init__(self, folder: Path) -> None:
        """Start the writer thread.

        Args:
            folder: The attempt's folder; rows go to `rows/` inside it.
        """
        self._folder = folder
        (folder / "rows").mkdir(parents=True, exist_ok=True)
        self._queue: queue.SimpleQueue[_Batch | None] = queue.SimpleQueue()
        self._files: dict[str, _StreamFiles] = {}
        self._lock = threading.Lock()
        self._minutes: dict[tuple[str, str], int] = {}
        self.errors: list[str] = []
        self._thread = threading.Thread(target=self._drain, name="live run recorder")
        self._thread.start()

    def put(self, stream: Stream, columns: dict[str, np.ndarray[Any, Any]]) -> None:
        """Queue one update's added rows; called on an update-graph thread.

        Args:
            stream: The stream.
            columns: The added rows, by column.
        """
        self._queue.put(_Batch(stream, columns))

    def _drain(self) -> None:
        """Write batches until the end marker; a failure is kept, never raised."""
        while (batch := self._queue.get()) is not None:
            try:
                self._write(batch)
            except Exception as error:
                # One bad batch must not end the day: it is logged and fails the day's units.
                logger.exception("could not write a batch of %s", batch.stream.name)
                self.errors.append(f"{batch.stream.name}: {type(error).__name__}: {error}")

    def _write(self, batch: _Batch) -> None:
        """Write one batch: rows by hour, counts by minute, delays by hour.

        Args:
            batch: The batch.
        """
        pyarrow = importlib.import_module("pyarrow")
        parquet = importlib.import_module("pyarrow.parquet")
        inserted = batch.columns["TimestampInsert"].astype("datetime64[ns]")
        hours = inserted.astype("datetime64[h]")
        minutes = inserted.astype("datetime64[m]")
        with self._lock:
            for minute, count in zip(*np.unique(minutes, return_counts=True), strict=True):
                key = (batch.stream.name, str(minute))
                self._minutes[key] = self._minutes.get(key, 0) + int(count)
        event = batch.columns[batch.stream.event_column].astype("datetime64[ns]")
        delays = (inserted - event).astype("int64") / 1e6
        for hour in np.unique(hours):
            mask = hours == hour
            files = self._files.setdefault(batch.stream.name, _StreamFiles())
            label = str(hour).replace(":", "")
            if files.hour != label:
                self._end_hour(batch.stream.name, files)
                files.hour = label
            table = pyarrow.table({k: v[mask] for k, v in batch.columns.items()})
            if files.writer is None:
                path = self._folder / "rows" / f"{batch.stream.name}-{label}.parquet"
                if path.exists():  # a restarted child: keep the hour's earlier rows
                    path = path.with_name(f"{path.stem}-{os.getpid()}.parquet")
                files.writer = parquet.ParquetWriter(path, table.schema)
            files.writer.write_table(table)
            files.delays.append(delays[mask])

    def _end_hour(self, name: str, files: _StreamFiles) -> None:
        """Close a stream's hour: its Parquet file, and one line of its delays.

        Args:
            name: The stream.
            files: Its open files.
        """
        if files.writer is not None:
            files.writer.close()
            files.writer = None
        if files.delays:
            values = np.concatenate(files.delays)
            row = {"stream": name, "hour": files.hour, "rows": len(values)}
            for p in _PERCENTILES:
                row[f"p{p}_ms"] = round(float(np.percentile(values, p)), 1)
            row["max_ms"] = round(float(values.max()), 1)
            append_csv(self._folder / "latency.csv", [row])
            files.delays = []

    def flush_minutes(self) -> None:
        """Append the rows counted per stream and minute so far to `minutes.csv`."""
        with self._lock:
            counted, self._minutes = self._minutes, {}
        rows = [{"stream": s, "minute": m, "rows": n} for (s, m), n in sorted(counted.items())]
        append_csv(self._folder / "minutes.csv", rows)

    def close(self) -> None:
        """Write what is queued, close every hour, and stop the thread."""
        self._queue.put(None)
        self._thread.join()
        for name, files in self._files.items():
            self._end_hour(name, files)
        self.flush_minutes()


def _release(m: MassiveSession, tables: list[Table]) -> None:
    """Release a group's live tables, so their streams unsubscribe before the session closes.

    Args:
        m: The session that made them.
        tables: The tables.
    """
    for table in tables:
        m.release(table)


def _listen(table: Table, stream: Stream, recorder: Recorder) -> TableListenerHandle:
    """Hand every update's added rows of a blink table to the recorder.

    The listener runs on an update-graph thread holding its lock, so it only copies the added
    rows and queues them. Deephaven's wrapper of the listener holds the table, and the JVM holds
    that wrapper until it collects the stopped listener, so the table outlives `stop()` until
    the next JVM collection; the child's process ends soon after anyway.

    Args:
        table: The stream's `blink()` table.
        stream: The stream.
        recorder: The recorder.

    Returns:
        The listener's handle, kept while the listener must run and stopped when the group
        ends.
    """
    listener = importlib.import_module("deephaven.table_listener")
    handle: TableListenerHandle = listener.listen(table, RowCopier(stream, recorder).on_update)
    return handle


class RowQueue(Protocol):
    """Where a `RowCopier` queues the rows it copies: a `Recorder`."""

    def put(self, stream: Stream, columns: dict[str, np.ndarray[Any, Any]], /) -> None:
        """Queue one update's added rows (`Recorder.put`).

        Args:
            stream: The stream.
            columns: The added rows, by column.
        """
        ...


class AddedRows(Protocol):
    """A table update, as a `RowCopier` reads it: Deephaven's `TableUpdate`."""

    def added(self) -> dict[str, np.ndarray[Any, Any]]:
        """Return the update's added rows, by column.

        Returns:
            The rows.
        """
        ...


class RowCopier:
    """The listener of one stream's blink table: it hands each update's added rows on.

    Created by `_listen` for each stream. Deephaven calls `on_update` on an update-graph thread
    holding its lock, one update at a time, so it only copies the added rows and queues them on
    the recorder, whose own thread writes them.
    """

    def __init__(self, stream: Stream, recorder: RowQueue) -> None:
        """Remember where the rows go.

        Args:
            stream: The stream.
            recorder: The recorder.
        """
        self._stream = stream
        self._recorder = recorder

    def on_update(self, update: AddedRows, is_replay: bool) -> None:
        """Queue one update's added rows, when it has any.

        Args:
            update: The update; only its added rows are read (a blink table has no others).
            is_replay: Whether it replays the table's first rows; listeners here never ask for
                a replay, so it is ignored.
        """
        added = update.added()
        if added and len(next(iter(added.values()))):
            self._recorder.put(self._stream, added)


def process_sample(now: datetime) -> dict[str, Any]:
    """Return one row of the process's CPU time and JVM heap.

    Args:
        now: The sample's time.

    Returns:
        The time, CPU seconds (user and system), and the heap used and committed, in MB.
    """
    jpy = importlib.import_module("jpy")
    runtime = jpy.get_type("java.lang.Runtime").getRuntime()
    times = os.times()
    total = int(runtime.totalMemory())
    return {
        "time": now.isoformat(),
        "cpu_seconds": round(times.user + times.system, 2),
        "heap_used_mb": round((total - int(runtime.freeMemory())) / 2**20, 1),
        "heap_committed_mb": round(total / 2**20, 1),
    }


def _sample(m: MassiveSession, folder: Path, now: datetime) -> None:
    """Append one sample of the streams, the connections, and the process.

    Args:
        m: The session.
        folder: The attempt's folder.
        now: The sample's time.
    """
    stamp = {"time": now.isoformat()}
    streams = [stamp | _plain(r) for r in diagnostics_rows(m, m.streams())]
    connections = [stamp | _plain(r) for r in diagnostics_rows(m, m.connections())]
    append_csv(folder / "samples-streams.csv", streams)
    append_csv(folder / "samples-connections.csv", connections)
    append_csv(folder / "samples-process.csv", [process_sample(now)])


def _plain(row: dict[str, Any]) -> dict[str, Any]:
    """Return a diagnostics row with every value as CSV-ready text or number.

    Args:
        row: The row.

    Returns:
        The row, instants as ISO text.
    """
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}


def record_gap(folder: Path, now: datetime, *, day_start: datetime) -> timedelta:
    """Record the gap since a stream day's last sample, and return the day's gaps so far.

    The first child of a day that starts late counts the time since the day's start as a gap
    too, so a stream day begun in the evening cannot pass as a whole day.

    Args:
        folder: The attempt's folder, which may hold an earlier child's samples.
        now: When this child starts.
        day_start: When the stream day starts (03:45 New York time).

    Returns:
        The total of every gap recorded in `gaps.csv`.
    """
    samples = folder / "samples-process.csv"
    start, cause = day_start, "the day's first child started late"
    if samples.exists():
        with samples.open(encoding="utf-8") as file:
            last = list(csv.DictReader(file))[-1]["time"]
        start, cause = datetime.fromisoformat(last), "the child restarted"
    if now - start > GAP_THRESHOLD:
        gap = {"start": start.isoformat(), "end": now.isoformat(), "cause": cause}
        append_csv(folder / "gaps.csv", [gap])
    return total_gaps(folder)


def total_gaps(folder: Path) -> timedelta:
    """Add up the gaps recorded in `gaps.csv`.

    Args:
        folder: The attempt's folder.

    Returns:
        Their total; zero without the file.
    """
    path = folder / "gaps.csv"
    if not path.exists():
        return timedelta()
    with path.open(encoding="utf-8") as file:
        return sum(
            (
                datetime.fromisoformat(r["end"]) - datetime.fromisoformat(r["start"])
                for r in csv.DictReader(file)
            ),
            timedelta(),
        )


@dataclass
class Clock:
    """The time source and the wait between samples, so tests can drive a day quickly.

    Attributes:
        now: The current time, aware.
        wait: Waits a number of seconds, or until stopped; returns True when stopped.
    """

    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    wait: Callable[[float], bool] = threading.Event().wait


def run_stream_day(
    m: MassiveSession,
    *,
    units: list[str],
    delayed: frozenset[str],
    folder: Path,
    until: datetime,
    report: Callable[[str, str, str], None],
    clock: Clock | None = None,
    day_start: datetime | None = None,
) -> None:
    """Stream the due units until the day's end, recording rows, samples, and gaps.

    Args:
        m: The session.
        units: The due units.
        delayed: The subscriptions the owner's settings list as delayed.
        folder: The attempt's folder, shared by every child of the same day.
        until: When the day ends (20:30 New York time).
        report: Receives each unit's name, status, and reason at the end.
        clock: The time source and wait; the real clock by default.
        day_start: When the day began, for a late first child's gap; 03:45 New York time of
            the clock's day by default.
    """
    clock = clock or Clock()
    today = clock.now().astimezone(NEW_YORK).date()
    day_start = day_start or datetime.combine(today, DAY_START, NEW_YORK)
    record_gap(folder, clock.now(), day_start=day_start)
    options: list[str] = []
    chain_refused: dict[str, list[str]] = {}
    if "options" in units:
        with refusal_recorded("options chain", "options", chain_refused):
            chain = m.options.chain("SPY").fetch(wait=True)
            options = option_contracts(rows_of(chain), today)
    streams = stream_day_streams(units, today=today, options=options)
    recorder = Recorder(folder)
    with held_streams(m, streams, recorder) as opened:
        sample_until(m, folder, until=until, clock=clock, recorder=recorder)
        wait_for_diagnostics(m)
        failures = failed_streams(diagnostics_rows(m, m.streams()))
    rows = unit_rows(folder, streams)
    for unit in units:
        refused, broken = split_failures(
            failures.get(unit, []), chain_refused.get(unit, []) + opened.get(unit, [])
        )
        status, reason = _unit_verdict(
            refused=refused,
            broken=broken,
            streams=sum(s.unit == unit for s in streams),
            contracts=bool(options) or unit != "options",
            gaps=total_gaps(folder),
            errors=recorder.errors,
            rows=rows.get(unit, 0),
            late=clock.now() >= rows_due_from(unit, today),
            delayed=unit in delayed,
        )
        report(unit, status, reason)


@contextlib.contextmanager
def held_streams(
    m: MassiveSession, streams: list[Stream], recorder: Recorder
) -> Iterator[dict[str, list[str]]]:
    """Hold streams while the caller samples them, every added row reaching the recorder.

    Each stream's query is held as a `blink()` table with a listener (`_listen`), and a bars
    stream (`Stream.held`) also as an `append_only()` and a `last_by()` table. A stream Massive
    refuses as it opens, or the settings do not list as delayed, is recorded
    (`refusal_recorded`), and the others open. On the way out,
    even after an error, the listeners stop, the tables are released, and the recorder writes
    what is queued and closes.

    Args:
        m: The session.
        streams: The streams.
        recorder: Their rows' recorder; closed on the way out.

    Yields:
        The refusals of the streams refused as they opened, by unit (`refusal_recorded`).
    """
    tables: list[Table] = []
    listeners: list[TableListenerHandle] = []
    refused: dict[str, list[str]] = {}
    try:
        for stream in streams:
            with refusal_recorded(stream.name, stream.unit, refused):
                query = stream.query(m)
                blink = query.blink()
                tables.append(blink)
                listeners.append(_listen(blink, stream, recorder))
                if stream.held:
                    tables.extend([query.append_only(), query.last_by()])
        yield refused
    finally:
        for handle in listeners:
            handle.stop()
        _release(m, tables)
        recorder.close()


def _unit_verdict(
    *,
    refused: list[str],
    broken: list[str],
    streams: int,
    contracts: bool,
    gaps: timedelta,
    errors: list[str],
    rows: int,
    late: bool,
    delayed: bool,
) -> tuple[str, str]:
    """Decide one unit's result at the end of a stream day.

    Args:
        refused: The refusals of its streams, or of the chain its contracts come from
            (`split_failures`), Massive's and the settings' alike.
        broken: The errors of its streams that failed otherwise.
        streams: How many streams it opened or tried to open.
        contracts: Whether it had something to stream (an options unit needs contracts).
        gaps: The day's gaps.
        errors: The recorder's errors.
        rows: The rows its streams recorded all day.
        late: Whether its session had opened when the day ended.
        delayed: Whether the owner's settings list its subscription as delayed.

    Returns:
        `("passed", why)`, its reason the gaps and a refused stream, if any; when every one of
        its streams (or its chain) was refused and none recorded a row, `unopened_verdict`'s
        `"refused"`, or `"skipped"` as not listed when only the settings refused;
        `("skipped", why)` for a delayed unit without a row; or
        `("failed", why)`, which a unit whose acknowledged streams recorded no row during its
        session's hours gets even when Massive refused its other streams.
    """
    minutes = round(gaps.total_seconds() / 60)
    if broken or (refused and len(refused) >= streams and not rows):
        return ("failed", f"a stream failed: {broken[0]}") if broken else unopened_verdict(refused)
    if not contracts:
        return "failed", "the SPY chain gave no contract to stream"
    if gaps > MAX_GAP:
        return "failed", f"gaps {minutes}"
    if errors:
        return "failed", errors[0]
    if not rows and late:
        why = "no rows during its session's hours"
        return ("skipped", f"{why}, on the delayed feed") if delayed else ("failed", why)
    notes = [f"gaps {minutes}"] if minutes else []
    notes += [refusal_note(refused)] if refused else []
    return "passed", "; ".join(notes)


def unit_rows(folder: Path, streams: list[Stream]) -> dict[str, int]:
    """Add up the rows each unit's streams recorded today, by every child of the day.

    Args:
        folder: The attempt's folder, whose `minutes.csv` every child of the day appends to.
        streams: The streams of this child, which name each stream's unit.

    Returns:
        The rows of each unit with a stream; a unit none of whose streams recorded a row is
        missing.
    """
    path = folder / "minutes.csv"
    if not path.exists():
        return {}
    unit_of = {stream.name: stream.unit for stream in streams}
    found: dict[str, int] = {}
    with path.open(encoding="utf-8") as file:
        for row in csv.DictReader(file):
            unit = unit_of.get(row["stream"])
            if unit is not None:
                found[unit] = found.get(unit, 0) + int(row["rows"])
    return found


def rows_due_from(unit: str, day: date) -> datetime:
    """Return when a unit must have recorded a row on a stream day.

    Args:
        unit: The unit, such as `"stocks"`.
        day: The New York date of the stream day.

    Returns:
        Its class's session open that day (`SESSION_OPENS`), plus `SESSION_GRACE`, as an
        aware instant.
    """
    opens = SESSION_OPENS.get(unit, DAY_START)
    return datetime.combine(day, opens, NEW_YORK) + SESSION_GRACE


DATASET_CLASSES: Final = {"forex": "currencies", "crypto": "currencies"}
"""The class of the namespaces whose name is not their class's: forex and crypto streams are
the currencies unit's."""


def failed_streams(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Return each class whose streams ended the day failed, with each failure's error.

    A stream Massive refuses, or that fails on its own, fails asynchronously: its table stops
    growing while the sampler goes on, so the day's verdict must read the streams' states.

    Args:
        rows: The rows of `m.streams()` at the day's end.

    Returns:
        The errors of the `FAILED` streams of each class, in order, by class.
    """
    found: dict[str, list[str]] = {}
    for row in rows:
        if row["State"] == "FAILED":
            namespace = str(row["Dataset"]).split(".")[0]
            name = DATASET_CLASSES.get(namespace, namespace)
            found.setdefault(name, []).append(str(row["Error"]))
    return found


def sample_until(
    m: MassiveSession, folder: Path, *, until: datetime, clock: Clock, recorder: Recorder
) -> None:
    """Sample every `SAMPLE_SECONDS` until a time, writing the minute counts each minute.

    Two samples further apart than `GAP_THRESHOLD` (the machine slept, or the child stalled)
    record the interval between them in `gaps.csv`, as a restart does.

    Args:
        m: The session.
        folder: The attempt's folder.
        until: When to stop.
        clock: The time source and wait.
        recorder: Its minute counts are flushed once a minute.
    """
    last_minute = ""
    previous: datetime | None = None
    while (now := clock.now()) < until:
        if previous is not None and now - previous > GAP_THRESHOLD:
            gap = {"start": previous.isoformat(), "end": now.isoformat()}
            append_csv(folder / "gaps.csv", [gap | {"cause": "the machine slept or stalled"}])
        previous = now
        _sample(m, folder, now)
        if now.strftime("%H%M") != last_minute:
            recorder.flush_minutes()
            last_minute = now.strftime("%H%M")
        if clock.wait(min(SAMPLE_SECONDS, (until - now).total_seconds())):
            break


def throughput_window(delayed: frozenset[str], day: date) -> tuple[datetime, datetime]:
    """Return the hour the throughput is measured over.

    Args:
        delayed: The subscriptions the owner's settings list as delayed.
        day: The New York date.

    Returns:
        09:45 to 10:45 New York time when stocks are listed as delayed, so the hour holds
        regular-session rows, else 09:30 to 10:30.
    """
    start = time(9, 45) if "stocks" in delayed else time(9, 30)
    first = datetime.combine(day, start, NEW_YORK)
    return first, first + timedelta(hours=1)


def run_throughput(
    m: MassiveSession,
    *,
    units: list[str],
    delayed: frozenset[str],
    folder: Path,
    until: datetime,
    report: Callable[[str, str, str], None],
    clock: Clock | None = None,
    window: tuple[datetime, datetime] | None = None,
) -> None:
    """Hold every symbol's busiest streams, sampling them, and summarize the measured hour.

    Args:
        m: The session.
        units: The due units (stocks and currencies).
        delayed: The subscriptions the owner's settings list as delayed
            (`throughput_window`).
        folder: The attempt's folder.
        until: When to stop (11:00 New York time).
        report: Receives each unit's name, status, and reason at the end.
        clock: The time source and wait; the real clock by default.
        window: The measured hour; `throughput_window` by default.
    """
    clock = clock or Clock()
    window = window or throughput_window(delayed, clock.now().astimezone(NEW_YORK).date())
    streams = throughput_streams(units)
    tables: list[Table] = []
    opened: dict[str, list[str]] = {}
    recorder = Recorder(folder)
    try:
        for stream in streams:
            with refusal_recorded(stream.name, stream.unit, opened):
                tables.append(stream.query(m).last_by())
        sample_until(m, folder, until=until, clock=clock, recorder=recorder)
        wait_for_diagnostics(m)
        failures = failed_streams(diagnostics_rows(m, m.streams()))
    finally:
        _release(m, tables)
        recorder.close()
    summary = summarize_throughput(folder, window)
    (folder / "summary.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    for unit in units:
        refused, broken = split_failures(failures.get(unit, []), opened.get(unit, []))
        if broken:
            report(unit, "failed", f"a stream failed: {broken[0]}")
        elif refused and len(refused) >= sum(s.unit == unit for s in streams):
            report(unit, *unopened_verdict(refused))
        elif summary["covered"]:
            report(unit, "passed", refusal_note(refused) if refused else "")
        else:
            report(unit, "failed", "the measured hour was not sampled without a gap")


def summarize_throughput(folder: Path, window: tuple[datetime, datetime]) -> dict[str, Any]:
    """Summarize the throughput samples inside the measured hour.

    Args:
        folder: The attempt's folder, with its sample files.
        window: The measured hour.

    Returns:
        Per stream the mean rows a second and the peak over one sample interval, and for the
        process and connections the peaks of heap, CPU, `BufferedRows`, reconnects, and any
        `SchemaDrift`; `covered` tells whether samples span the hour with no gap.
    """
    first, last = window
    streams = _inside(folder / "samples-streams.csv", window)
    connections = _inside(folder / "samples-connections.csv", window)
    process = _inside(folder / "samples-process.csv", window)
    stamps = sorted({datetime.fromisoformat(r["time"]) for r in process})
    covered = (
        bool(stamps) and stamps[0] - first <= GAP_THRESHOLD and last - stamps[-1] <= GAP_THRESHOLD
    )
    covered = covered and all(b - a <= GAP_THRESHOLD for a, b in itertools.pairwise(stamps))
    rates: dict[str, dict[str, float]] = {}
    by_dataset: dict[str, list[tuple[datetime, int]]] = {}
    for row in streams:
        by_dataset.setdefault(row["Dataset"], []).append(
            (datetime.fromisoformat(row["time"]), int(row["RowsPublished"] or 0))
        )
    for dataset, points in by_dataset.items():
        points.sort()
        steps = [
            (b[1] - a[1]) / max((b[0] - a[0]).total_seconds(), 1e-9)
            for a, b in itertools.pairwise(points)
        ]
        span = (points[-1][0] - points[0][0]).total_seconds()
        mean = (points[-1][1] - points[0][1]) / span if span > 0 else 0.0
        rates[dataset] = {
            "mean_rows_per_second": round(mean, 1),
            "peak_rows_per_second": round(max(steps, default=0.0), 1),
        }
    return {
        "window": [first.isoformat(), last.isoformat()],
        "covered": covered,
        "streams": rates,
        "peak_buffered_rows": max((int(r["BufferedRows"] or 0) for r in connections), default=0),
        "reconnects": max((int(r["Reconnects"] or 0) for r in connections), default=0),
        "schema_drift": sorted(
            {r["SchemaDrift"] for r in connections + streams if r.get("SchemaDrift")}
        ),
        "peak_heap_used_mb": max((float(r["heap_used_mb"]) for r in process), default=0.0),
        "cpu_seconds": (
            round(float(process[-1]["cpu_seconds"]) - float(process[0]["cpu_seconds"]), 2)
            if process
            else 0.0
        ),
    }


def _inside(path: Path, window: tuple[datetime, datetime]) -> list[dict[str, str]]:
    """Read a sample file's rows whose time falls inside a window.

    Args:
        path: The file.
        window: The first and last time, inclusive.

    Returns:
        The rows; empty without the file.
    """
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as file:
        return [
            r
            for r in csv.DictReader(file)
            if window[0] <= datetime.fromisoformat(r["time"]) <= window[1]
        ]
