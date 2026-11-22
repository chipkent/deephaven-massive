"""Tests for history reads from the flat files, with REST serving what they cannot.

The flat-file read and the REST transport are fakes here: the files answer a scripted
`FlatFileRead` (or refusal), and REST records each call and returns no rows. The reads through
the real transports are tested in `tests/ingest/test_flatfiles.py`, and through `history()` in
`tests/queries/test_history.py`.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _massive, _session
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.history_source import history_window
from deephaven_massive._datasets.request import Request
from deephaven_massive._enums import Timespan
from deephaven_massive._errors import RowDataError
from deephaven_massive._ingest.flatfiles import FlatFileHistory
from deephaven_massive._ingest.history_reads import read_history, read_history_by_symbol
from deephaven_massive._log_limits import LoggedKeys
from deephaven_massive._massive import RowFault
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.transports import (
    FlatFileRead,
    SymbolFailure,
    SymbolOutcome,
    SymbolRows,
)
from deephaven_massive._tables.withheld import Withheld, WithheldGroup

if TYPE_CHECKING:
    from collections.abc import Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow

TRADES = _session.CATALOG.by_name["stocks.trades"]
"""A dataset REST serves too, one symbol at a time."""
BARS = _session.CATALOG.by_name["stocks.bars"]
"""Daily bars, which REST reads over the call's own window."""
VALUES = _session.CATALOG.by_name["indices.values"]
INDEX_BARS = _session.CATALOG.by_name["indices.bars"]
"""A dataset only the flat files serve."""
MONDAY, TUESDAY = date(2025, 3, 3), date(2025, 3, 4)


def _rows(
    session: dm.MassiveSession, symbols: list[str], *days: date, spec: DatasetSpec = TRADES
) -> Table:
    # Rows of the session's own columns, one a symbol and day.
    schema = session._core.dataset_schema(spec)
    data = [{"Symbol": symbol, "Date": day.isoformat()} for day in days for symbol in symbols]
    with entered(session._core.exec_ctx):
        return dataset_table(pa.Table.from_pylist(data, schema=schema.arrow_schema()), ["Date"])


def _empty(session: dm.MassiveSession, spec: DatasetSpec) -> Table:
    schema = session._core.catalog.dataset_schema(spec)
    with entered(session._core.exec_ctx):
        return dh_arrow.to_table(schema.arrow_schema().empty_table())


class _Files:
    # The flat-file read: answers `answer`, or raises it, and records `infer_holidays`.
    def __init__(self, answer: FlatFileRead | Exception) -> None:
        self.answer = answer
        self.inferred: list[bool] = []

    def history(self, *_: object, infer_holidays: bool, **__: object) -> FlatFileRead:
        self.inferred.append(infer_holidays)
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class _Rest:
    # The REST transport: records each history call's bounds and returns no rows.
    def __init__(self, session: dm.MassiveSession) -> None:
        self.session = session
        self.calls: list[tuple[object, object, tuple[date, ...] | None]] = []
        self.nows: list[datetime] = []
        self.by_symbol: list[tuple[object, object, tuple[str, ...]]] = []
        self.failing: dict[str, dm.MassiveError] = {}

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        tally: object,
        stop: object = None,
        days: tuple[date, ...] | None = None,
    ) -> Table:
        self.calls.append((request.start, request.end, days))
        self.nows.append(window.now)
        return _empty(self.session, spec)

    def history_by_symbol(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        symbols: Sequence[str],
        call: int,
        tally: object,
        stop: object = None,
    ) -> dict[str, SymbolOutcome]:
        # A row a symbol on the read's first and last day; a symbol in `failing` fails.
        self.by_symbol.append((request.start, request.end, tuple(symbols)))
        self.nows.append(window.now)
        start, end = request.start, request.end
        assert isinstance(start, date)
        assert isinstance(end, date)
        days = sorted({start, end})
        return {
            symbol: SymbolFailure(self.failing[symbol])
            if symbol in self.failing
            else SymbolRows(_rows(self.session, [symbol], *days, spec=spec))
            for symbol in symbols
        }


@pytest.fixture
def rest(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> _Rest:
    fake = _Rest(massive_session)
    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: fake)
    return fake


def _read(
    session: dm.MassiveSession,
    files: _Files,
    spec: DatasetSpec = TRADES,
    *,
    start: date | datetime = MONDAY,
    end: date | datetime = TUESDAY,
    forced: bool = False,
) -> tuple[Table, str | None, str | None]:
    symbols = ("I:SPX",) if spec is VALUES else ("AAPL",)
    request = Request(symbols=symbols, start=start, end=end)
    window = history_window(spec, request, start=start, end=end)
    return read_history(
        spec,
        request,
        window,
        core=session._core,
        files=files,
        dates=(MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=forced,
    )


def _files(
    session: dm.MassiveSession,
    spec: DatasetSpec = TRADES,
    *,
    unread: tuple[date, ...] = (),
    uncertain: tuple[date, ...] = (),
    absent: tuple[date, ...] = (),
    skipped: tuple[date, ...] = (),
) -> _Files:
    # Monday's file read; the days given were not.
    empty = _empty(session, spec)
    return _Files(FlatFileRead(((MONDAY, empty),), empty, unread, uncertain, absent, skipped))


def test_files_that_serve_the_whole_range_need_no_rest(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    files = _files(massive_session)
    _, served, lacks = _read(massive_session, files)
    assert (served, lacks) == (None, None)
    assert files.inferred == [False]  # REST could serve a missing day: nothing is inferred
    assert rest.calls == []


def test_the_days_the_files_lack_are_read_from_rest_one_by_one(
    massive_session: dm.MassiveSession, rest: _Rest, caplog: pytest.LogCaptureFixture
) -> None:
    files = _files(massive_session, unread=(TUESDAY,))
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.history_reads"):
        _, served, _ = _read(massive_session, files)
    assert rest.calls == [(TUESDAY, TUESDAY, None)]
    assert served == "Read from flat files, and from REST for 2025-03-04, whose files are missing."
    (message,) = caplog.messages
    assert message.startswith(
        "stocks.trades history: Massive has no flat files for 2025-03-04. The read took those "
        "rows from REST instead."
    )


def test_a_range_of_instants_reads_each_missing_day_clipped_to_it(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    start = datetime(2025, 3, 3, 15, tzinfo=UTC)
    end = datetime(2025, 3, 4, 15, tzinfo=UTC)
    files = _files(massive_session, unread=(TUESDAY,))
    _read(massive_session, files, start=start, end=end)
    # The missing day from its New York midnight to the range's end.
    assert rest.calls == [(datetime(2025, 3, 4, 5, tzinfo=UTC), end, None)]


def test_a_missing_day_read_later_keeps_the_call_s_instant(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    # The call began at 12:00 UTC on Tuesday; its missing day is read later, and its window is
    # still built for the call's instant, not the clock's.
    start = datetime(2025, 3, 3, 15, tzinfo=UTC)
    call = datetime(2025, 3, 4, 12, tzinfo=UTC)
    request = Request(symbols=("AAPL",), start=start, end=None)
    window = history_window(TRADES, request, start=start, end=None, now=call)
    files = _files(massive_session, unread=(TUESDAY,))
    read_history(
        TRADES,
        request,
        window,
        core=massive_session._core,
        files=files,
        dates=(MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=False,
    )
    assert rest.nows == [call]


@pytest.mark.parametrize(
    ("spec", "forced", "remedy"),
    [
        (
            TRADES,
            True,
            (
                "Call history() again later, or pass source=dm.HistorySource.REST to read the day "
                "from REST."
            ),
        ),
        (
            VALUES,
            False,
            (
                "REST cannot serve this read, since indices.values has no REST history. Call "
                "history() again later."
            ),
        ),
    ],
)
def test_a_file_not_published_yet_raises_where_rest_may_not_serve_it(
    *,
    massive_session: dm.MassiveSession,
    rest: _Rest,
    spec: DatasetSpec,
    forced: bool,
    remedy: str,
) -> None:
    files = _files(massive_session, spec, unread=(TUESDAY,))
    with pytest.raises(dm.MassiveError) as info:
        _read(massive_session, files, spec, forced=forced)
    assert files.inferred == [True]
    assert str(info.value) == (
        f"{spec.name}: Massive has not published the flat file for 2025-03-04 yet. Massive "
        "usually publishes a day's flat files by about 11:00 ET the next day, but the time "
        f"varies. {remedy}"
    )
    assert rest.calls == []


@pytest.mark.parametrize(
    ("spec", "remedy"),
    [
        (
            TRADES,
            (
                "Pass source=dm.HistorySource.REST to read them from REST, or end the range before "
                "them."
            ),
        ),
        (
            VALUES,
            (
                "REST cannot serve this read, since indices.values has no REST history. End the "
                "range before them."
            ),
        ),
    ],
)
def test_files_that_will_never_appear_raise(
    *, massive_session: dm.MassiveSession, rest: _Rest, spec: DatasetSpec, remedy: str
) -> None:
    files = _files(massive_session, spec, absent=(TUESDAY,))
    with pytest.raises(dm.MassiveError) as info:
        _read(massive_session, files, spec, forced=True)
    assert str(info.value) == (
        f"{spec.name}: Massive has no flat file for 2025-03-04, nor for any later day up to a "
        "week after the range. Massive publishes a day's file the next day, so these files will "
        f"not appear. {remedy}"
    )


def test_a_refusal_names_rest_when_rest_could_serve_the_read(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    refused = dm.EntitlementError("Massive refused the files.", status=403, request_id="r1")
    with pytest.raises(dm.EntitlementError) as info:
        _read(massive_session, _Files(refused), forced=True)
    assert str(info.value) == (
        "Massive refused the files. To read from REST instead, pass source=dm.HistorySource.REST "
        "to history()."
    )
    assert (info.value.status, info.value.request_id) == (403, "r1")
    with pytest.raises(dm.EntitlementError) as unchanged:
        _read(massive_session, _Files(refused), VALUES)
    assert unchanged.value is refused  # REST cannot serve it either
    rejected = dm.AuthenticationError("bad S3 key", status=403)
    with pytest.raises(dm.AuthenticationError):
        _read(massive_session, _Files(rejected), forced=True)
    assert rest.calls == []


def test_a_refusal_under_auto_reads_the_whole_call_from_rest(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    refused = dm.EntitlementError("no files", status=403)
    _, served, lacks = _read(massive_session, _Files(refused))
    assert (served, lacks) == ("Read from REST: AUTO chose flat files, which S3 refused.", None)
    assert rest.calls == [(MONDAY, TUESDAY, None)]


def test_sessions_a_futures_read_may_miss_are_named(
    massive_session: dm.MassiveSession, rest: _Rest, caplog: pytest.LogCaptureFixture
) -> None:
    files = _files(massive_session, uncertain=(MONDAY,))
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.history_reads"):
        _, served, lacks = _read(massive_session, files)
    assert served is None
    assert lacks is not None
    assert lacks.startswith("Session(s) 2025-03-03 may be incomplete")
    assert caplog.messages == [
        (
            "stocks.trades history: Massive's flat files hold rows of sessions other than their "
            "own day's, so rows of 2025-03-03 may sit in a file outside the range and be missing. "
            "To include them, read a wider range of dates, or by datetimes."
        )
    ]


def test_days_taken_for_holidays_are_named_on_the_table(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    # A missing day's file with a later file is taken for a holiday: the table says so, since
    # Massive's file may simply be missing and the library never hides a gap in its data.
    files = _files(massive_session, skipped=(TUESDAY,))
    _, served, lacks = _read(massive_session, files, forced=True)
    assert served is None
    assert lacks == (
        "No flat file exists for 2025-03-04, though a later day's does, so that day was taken "
        "for a day without trading (a holiday?) and has no rows here."
    )
    both = _files(massive_session, skipped=(MONDAY, TUESDAY), uncertain=(MONDAY,))
    _, _, lacks = _read(massive_session, both, forced=True)
    assert lacks is not None
    assert lacks.startswith(
        "No flat file exists for 2025-03-03, 2025-03-04, though a later day's does, so those "
        "days were taken for days without trading (a holiday?) and have no rows here. "
        "Session(s) 2025-03-03 may be incomplete"
    )
    assert rest.calls == []


# --- Reads by symbol ----------------------------------------------------------------------------


def _by_symbol(
    session: dm.MassiveSession,
    answer: FlatFileRead | Exception,
    *,
    forced: bool = False,
    end: date = TUESDAY,
    now: datetime | None = None,
) -> dict[str, SymbolOutcome]:
    request = Request(start=MONDAY, end=end)
    window = history_window(TRADES, request, start=MONDAY, end=end, now=now)
    return read_history_by_symbol(
        TRADES,
        request,
        window,
        core=session._core,
        files=_Files(answer),
        symbols=["MSFT", "AAPL", "MSFT"],
        dates=(MONDAY, TUESDAY),
        call=1,
        logged=LoggedKeys(),
        stop=None,
        forced=forced,
    )


def _monday_files(
    session: dm.MassiveSession,
    *,
    unread: tuple[date, ...] = (),
    uncertain: tuple[date, ...] = (),
) -> FlatFileRead:
    # Monday's file, with a row of each symbol; the days given were not read.
    empty = _rows(session, [], MONDAY)
    monday = _rows(session, ["AAPL", "MSFT"], MONDAY)
    return FlatFileRead(((MONDAY, monday),), empty, unread, uncertain)


def _dates(outcome: SymbolOutcome) -> list[str]:
    assert isinstance(outcome, SymbolRows)
    return [row["Date"] for row in dh_arrow.to_arrow(outcome.table).to_pylist()]


def test_one_symbol_s_rest_failure_for_a_missing_day_fails_only_that_symbol(
    massive_session: dm.MassiveSession, rest: _Rest, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The call began on Wednesday; its missing Tuesday is read after the clock moved on, with
    # day windows built for the call's instant.
    call = datetime(2025, 3, 5, 12, tzinfo=UTC)
    monkeypatch.setattr(history_source, "_now", lambda: call + timedelta(hours=1))
    rest.failing["MSFT"] = dm.MassiveError("HTTP 503", status=503)
    files = _monday_files(massive_session, unread=(TUESDAY,))
    out = _by_symbol(massive_session, files, now=call)
    assert list(out) == ["MSFT", "AAPL"]
    assert _dates(out["AAPL"]) == ["2025-03-03", "2025-03-04"]
    failure = out["MSFT"]
    assert isinstance(failure, SymbolFailure)
    assert failure.error is rest.failing["MSFT"]
    assert rest.by_symbol == [(TUESDAY, TUESDAY, ("MSFT", "AAPL"))]
    assert rest.nows == [call]


def test_a_symbol_that_failed_one_missing_day_is_not_read_for_the_next(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    rest.failing["MSFT"] = dm.MassiveError("HTTP 503", status=503)
    wednesday = date(2025, 3, 5)
    files = _monday_files(massive_session, unread=(TUESDAY, wednesday))
    out = _by_symbol(massive_session, files, end=wednesday)
    assert _dates(out["AAPL"]) == ["2025-03-03", "2025-03-04", "2025-03-05"]
    assert [symbols for _, _, symbols in rest.by_symbol] == [("MSFT", "AAPL"), ("AAPL",)]


def test_files_that_serve_every_day_split_by_symbol(
    massive_session: dm.MassiveSession, rest: _Rest, caplog: pytest.LogCaptureFixture
) -> None:
    files = _monday_files(massive_session, uncertain=(MONDAY,))
    with caplog.at_level(logging.DEBUG, "deephaven_massive._ingest.history_reads"):
        out = _by_symbol(massive_session, files)
    assert [_dates(o) for o in out.values()] == [["2025-03-03"], ["2025-03-03"]]
    assert rest.by_symbol == []
    assert "history by symbol: Session(s) 2025-03-03 may be incomplete" in caplog.text


def test_a_failed_scan_fails_every_symbol_with_one_error(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    broken = dm.MassiveError("cannot read s3://flatfiles/...")
    out = _by_symbol(massive_session, broken)
    errors = [o.error for o in out.values() if isinstance(o, SymbolFailure)]
    assert errors == [broken, broken]
    # A file not published yet, where REST may not serve the day, fails every symbol alike.
    late = _by_symbol(
        massive_session, _monday_files(massive_session, unread=(TUESDAY,)), forced=True
    )
    assert {str(o.error)[:40] for o in late.values() if isinstance(o, SymbolFailure)} == {
        "stocks.trades: Massive has not published"
    }
    assert rest.by_symbol == []


def test_refused_files_under_auto_read_every_symbol_from_rest(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    out = _by_symbol(massive_session, dm.EntitlementError("no files", status=403))
    assert [_dates(o) for o in out.values()] == [["2025-03-03", "2025-03-04"]] * 2
    assert rest.by_symbol == [(MONDAY, TUESDAY, ("MSFT", "AAPL"))]


def test_rejected_credentials_where_rest_may_not_serve_fail_the_whole_read(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    with pytest.raises(dm.AuthenticationError):
        _by_symbol(massive_session, dm.AuthenticationError("bad S3 key"), forced=True)


def test_the_flat_file_transport_reads_by_symbol_and_skips_an_empty_window(
    massive_session: dm.MassiveSession, rest: _Rest, monkeypatch: pytest.MonkeyPatch
) -> None:
    scans: list[object] = []

    def scan(
        self: FlatFileHistory, spec: DatasetSpec, request: Request, *_: object, **__: object
    ) -> FlatFileRead:
        scans.append(request.symbols)
        return _monday_files(massive_session)

    monkeypatch.setattr(FlatFileHistory, "history", scan)
    unread = _massive.FlatFileClient(  # never read: `history` is patched
        access_key_id="k",
        secret_access_key="s",  # noqa: S106  # fake credential
        endpoint_url="http://127.0.0.1:9",
        bucket="b",
        max_pool=1,
    )
    files = FlatFileHistory(massive_session._core, unread)
    request = Request(start=MONDAY, end=MONDAY)
    window = history_window(TRADES, request, start=MONDAY, end=MONDAY)
    out = files.history_by_symbol(
        TRADES,
        request,
        window,
        symbols=["MSFT", "AAPL"],
        dates=(MONDAY,),
        call=7,
        forced=False,
        logged=LoggedKeys(),
    )
    assert scans == [("AAPL", "MSFT")]
    assert [_dates(o) for o in out.values()] == [["2025-03-03"], ["2025-03-03"]]
    instant = datetime(2025, 3, 3, 5, tzinfo=UTC)
    empty = history_window(TRADES, request, start=instant, end=instant, now=instant)
    nothing = files.history_by_symbol(
        TRADES,
        request,
        empty,
        symbols=["AAPL"],
        dates=(),
        call=8,
        forced=False,
        logged=LoggedKeys(),
    )
    assert _dates(nothing["AAPL"]) == []
    assert len(scans) == 1


def test_a_missing_day_of_daily_bars_keeps_only_that_day_from_rest(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    # Daily bars are read with the call's own window, then kept by the missing days' `Date`.
    request = Request(start=MONDAY, end=TUESDAY, timespan=Timespan.DAY, multiplier=1)
    window = history_window(BARS, request, start=MONDAY, end=TUESDAY)
    empty = _rows(massive_session, [], spec=BARS)
    monday = _rows(massive_session, ["AAPL"], MONDAY, spec=BARS)
    files = _Files(FlatFileRead(((MONDAY, monday),), empty, (TUESDAY,)))
    out = read_history_by_symbol(
        BARS,
        request,
        window,
        core=massive_session._core,
        files=files,
        symbols=["AAPL"],
        dates=(MONDAY, TUESDAY),
        call=1,
        logged=LoggedKeys(),
        stop=None,
        forced=False,
    )
    assert _dates(out["AAPL"]) == ["2025-03-03", "2025-03-04"]
    assert rest.by_symbol == [(MONDAY, TUESDAY, ("AAPL",))]


def _lost(rows: int) -> Withheld:
    # `rows` rows withheld for a price that is no number.
    group = WithheldGroup(RowFault.UNCONVERTIBLE_VALUES, "AAPL", "AAPL", "Price", rows, "x")
    return Withheld(pa.table({"x": pa.array([], pa.int64())}), (group,), "x")


class _LostFiles(_Files):
    # The flat files of `answer`, whose every row of the read was withheld.
    @override
    def history(self, *_: object, infer_holidays: bool, **kwargs: object) -> FlatFileRead:
        tally = kwargs["tally"]
        assert isinstance(tally, ReadTally)
        tally.add(1, _lost(1), source="the flat files")
        return super().history(infer_holidays=infer_holidays)


def test_a_symbol_with_no_data_keeps_its_rows_when_the_read_fails(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The files hold no row of either symbol; REST withholds AAPL's only row of the missing
    # day and answers HTTP 404 for MSFT. The read fails AAPL, and MSFT keeps its empty rows.
    class Rest(_Rest):
        @override
        def history_by_symbol(
            self, spec: DatasetSpec, *_: object, symbols: Sequence[str], tally: object, **__: object
        ) -> dict[str, SymbolOutcome]:
            assert isinstance(tally, ReadTally)
            tally.add(1, _lost(1), source="GET /v3/trades/{stockTicker}")
            empty = _rows(self.session, [], spec=spec)
            return {s: SymbolRows(empty, no_data=s == "MSFT") for s in symbols}

    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: Rest(massive_session))
    empty = _rows(massive_session, [], MONDAY)
    out = _by_symbol(massive_session, FlatFileRead(((MONDAY, empty),), empty, (TUESDAY,)))
    aapl, msft = out["AAPL"], out["MSFT"]
    assert isinstance(aapl, SymbolFailure)
    assert isinstance(aapl.error, RowDataError)
    assert isinstance(msft, SymbolRows)
    assert msft.no_data


def test_a_rest_day_bar_of_a_files_day_counts_as_read(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every row of the files is withheld. REST reads day bars over the whole range: Monday's
    # bar, which the call drops for the files' day, reads whole, and the missing Tuesday's is
    # withheld. Massive sent a row of the call's window that read whole, so the call goes on.
    class Rest(_Rest):
        @override
        def history(self, spec: DatasetSpec, *_: object, tally: object, **__: object) -> Table:
            assert isinstance(tally, ReadTally)
            tally.add(2, _lost(1), source="GET /v2/aggs/...")
            return _rows(self.session, ["AAPL"], MONDAY, spec=spec)

    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: Rest(massive_session))
    request = Request(
        symbols=("AAPL",), start=MONDAY, end=TUESDAY, timespan=Timespan.DAY, multiplier=1
    )
    window = history_window(BARS, request, start=MONDAY, end=TUESDAY)
    empty = _rows(massive_session, [], spec=BARS)
    files = _LostFiles(FlatFileRead(((MONDAY, empty),), empty, (TUESDAY,)))
    table, _, _ = read_history(
        BARS,
        request,
        window,
        core=massive_session._core,
        files=files,
        dates=(MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=False,
    )
    assert table.size == 0


def test_a_symbol_whose_file_rows_were_all_withheld_fails_with_the_read(
    massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The files' rows were all withheld, so an empty file table is no sign of no data: AAPL,
    # whose REST days Massive answered HTTP 404 for, fails with the read.
    class Rest(_Rest):
        @override
        def history_by_symbol(
            self, spec: DatasetSpec, *_: object, symbols: Sequence[str], tally: object, **__: object
        ) -> dict[str, SymbolOutcome]:
            empty = _rows(self.session, [], spec=spec)
            return {s: SymbolRows(empty, no_data=True) for s in symbols}

    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: Rest(massive_session))
    empty = _rows(massive_session, [], MONDAY)
    request = Request(start=MONDAY, end=TUESDAY)
    window = history_window(TRADES, request, start=MONDAY, end=TUESDAY)
    out = read_history_by_symbol(
        TRADES,
        request,
        window,
        core=massive_session._core,
        files=_LostFiles(FlatFileRead(((MONDAY, empty),), empty, (TUESDAY,))),
        symbols=["AAPL"],
        dates=(MONDAY, TUESDAY),
        call=1,
        logged=LoggedKeys(),
        stop=None,
        forced=False,
    )
    failed = out["AAPL"]
    assert isinstance(failed, SymbolFailure)
    assert isinstance(failed.error, RowDataError)


def test_a_missing_index_minute_file_is_read_from_rest_as_the_chicago_day_it_holds(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    # An index minute file holds its date's Chicago day, 01:00 to 01:00 New York time, so
    # Tuesday's missing file is read from REST as those instants, clipped to the New York days
    # asked for: Monday's file, which was read, already holds Tuesday 00:00 to 01:00.
    request = Request(start=MONDAY, end=TUESDAY, timespan=Timespan.MINUTE, multiplier=1)
    window = history_window(INDEX_BARS, request, start=MONDAY, end=TUESDAY)
    empty = _rows(massive_session, [], spec=INDEX_BARS)
    files = _Files(FlatFileRead(((MONDAY, empty),), empty, (TUESDAY,)))
    read_history(
        INDEX_BARS,
        request,
        window,
        core=massive_session._core,
        files=files,
        dates=(date(2025, 3, 2), MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=False,
    )
    tuesday_chicago = datetime(2025, 3, 4, 6, tzinfo=UTC)  # 01:00 New York
    wednesday_new_york = datetime(2025, 3, 5, 5, tzinfo=UTC)  # the range's end
    assert rest.calls == [(tuesday_chicago, wednesday_new_york, None)]


def test_a_missing_first_index_minute_file_is_read_from_rest_for_the_first_hour_only(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    # The file before the range's first day holds only its hour from 00:00 to 01:00 New York.
    request = Request(start=TUESDAY, end=TUESDAY, timespan=Timespan.MINUTE, multiplier=1)
    window = history_window(INDEX_BARS, request, start=TUESDAY, end=TUESDAY)
    empty = _rows(massive_session, [], spec=INDEX_BARS)
    files = _Files(FlatFileRead(((TUESDAY, empty),), empty, (MONDAY,)))
    read_history(
        INDEX_BARS,
        request,
        window,
        core=massive_session._core,
        files=files,
        dates=(MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=False,
    )
    tuesday_new_york = datetime(2025, 3, 4, 5, tzinfo=UTC)
    tuesday_chicago = datetime(2025, 3, 4, 6, tzinfo=UTC)
    assert rest.calls == [(tuesday_new_york, tuesday_chicago, None)]
