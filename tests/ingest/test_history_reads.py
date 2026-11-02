"""Tests for history reads from the flat files, with REST serving what they cannot.

The flat-file read and the REST transport are fakes here: the files answer a scripted
`FlatFileRead` (or refusal), and REST records each call and returns no rows. The reads through
the real transports are tested in `tests/ingest/test_flatfiles.py`, and through `history()` in
`tests/test_query.py`.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, cast

import pytest
from deephaven import arrow as dh_arrow

import deephaven_massive as dm
from deephaven_massive import _session
from deephaven_massive._datasets.history_source import history_window
from deephaven_massive._datasets.request import Request
from deephaven_massive._ingest.history_reads import read_history
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.transports import FlatFileRead

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._ingest.flatfiles import FlatFileHistory

TRADES = _session.CATALOG.by_name["stocks.trades"]
"""A dataset REST serves too, one symbol at a time."""
VALUES = _session.CATALOG.by_name["indices.values"]
"""A dataset only the flat files serve."""
MONDAY, TUESDAY = date(2025, 3, 3), date(2025, 3, 4)


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

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        call: int,
        stop: object = None,
        days: tuple[date, ...] | None = None,
    ) -> Table:
        self.calls.append((request.start, request.end, days))
        self.nows.append(window.now)
        return _empty(self.session, spec)


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
) -> tuple[Table, str | None]:
    symbols = ("I:SPX",) if spec is VALUES else ("AAPL",)
    request = Request(symbols=symbols, start=start, end=end)
    window = history_window(spec, request, start=start, end=end)
    return read_history(
        spec,
        request,
        window,
        core=session._core,
        files=cast("FlatFileHistory", files),
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
) -> _Files:
    # Monday's file read; the days given were not.
    empty = _empty(session, spec)
    return _Files(FlatFileRead(((MONDAY, empty),), empty, unread, uncertain, absent))


def test_files_that_serve_the_whole_range_need_no_rest(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    files = _files(massive_session)
    _, served = _read(massive_session, files)
    assert served is None
    assert files.inferred == [False]  # REST could serve a missing day: nothing is inferred
    assert rest.calls == []


def test_the_days_the_files_lack_are_read_from_rest_one_by_one(
    massive_session: dm.MassiveSession, rest: _Rest, caplog: pytest.LogCaptureFixture
) -> None:
    files = _files(massive_session, unread=(TUESDAY,))
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.history_reads"):
        _, served = _read(massive_session, files)
    assert rest.calls == [(TUESDAY, TUESDAY, None)]
    assert served == "Read from flat files, and from REST for 2025-03-04, whose files are missing."
    (message,) = caplog.messages
    assert message.startswith(
        "stocks.trades history: AUTO read REST because the flat files are missing for these "
        "days: 1 [2025-03-04]"
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
        files=cast("FlatFileHistory", files),
        dates=(MONDAY, TUESDAY),
        call=1,
        stop=None,
        forced=False,
    )
    assert rest.nows == [call]


@pytest.mark.parametrize(("spec", "forced"), [(TRADES, True), (VALUES, False)])
def test_a_file_not_published_yet_raises_where_rest_may_not_serve_it(
    *, massive_session: dm.MassiveSession, rest: _Rest, spec: DatasetSpec, forced: bool
) -> None:
    files = _files(massive_session, spec, unread=(TUESDAY,))
    with pytest.raises(
        dm.MassiveError, match=r"the flat file for 2025-03-04 is not published"
    ) as info:
        _read(massive_session, files, spec, forced=forced)
    assert files.inferred == [True]
    remedy = "or read REST with" if forced else "REST cannot serve this read"
    assert remedy in str(info.value)
    assert rest.calls == []


@pytest.mark.parametrize(("spec", "remedy"), [(TRADES, "read REST"), (VALUES, "end the range")])
def test_files_that_will_never_appear_raise(
    *, massive_session: dm.MassiveSession, rest: _Rest, spec: DatasetSpec, remedy: str
) -> None:
    files = _files(massive_session, spec, absent=(TUESDAY,))
    with pytest.raises(dm.MassiveError, match=r"no flat file exists for 2025-03-04") as info:
        _read(massive_session, files, spec, forced=True)
    assert f"To read them, {remedy}" in str(info.value)


def test_a_refusal_names_rest_when_rest_could_serve_the_read(
    massive_session: dm.MassiveSession, rest: _Rest
) -> None:
    refused = dm.EntitlementError("no files", status=403, request_id="r1")
    with pytest.raises(dm.EntitlementError) as info:
        _read(massive_session, _Files(refused), forced=True)
    assert str(info.value).startswith("no files; REST serves this read too")
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
    _, served = _read(massive_session, _Files(refused))
    assert served == "Read from REST: AUTO chose flat files, which S3 refused."
    assert rest.calls == [(MONDAY, TUESDAY, None)]


def test_sessions_a_futures_read_may_miss_are_named(
    massive_session: dm.MassiveSession, rest: _Rest, caplog: pytest.LogCaptureFixture
) -> None:
    files = _files(massive_session, uncertain=(MONDAY,))
    with caplog.at_level(logging.WARNING, "deephaven_massive._ingest.history_reads"):
        _, served = _read(massive_session, files)
    assert served is not None
    assert served.startswith("Session(s) 2025-03-03 may be incomplete")
    assert "rows of session(s) 2025-03-03 may sit in a file outside the range" in caplog.text
