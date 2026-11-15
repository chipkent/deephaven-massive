"""Tests for `queries/_history.py`: `history()` from REST and flat files, and following a table."""

import logging
import threading
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest
from deephaven import new_table
from deephaven.column import string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import DaySpan, TimeRange
from deephaven_massive._enums import (
    AssetClass,
    HistorySource,
    NotBars,
    Timespan,
)
from deephaven_massive._tables.transports import (
    FailureKind,
    SymbolFailure,
)
from deephaven_massive.queries import HistoryQuery, StreamHistoryQuery
from tests._fakes.queries import (
    BARS_SPEC,
    FF,
    HISTORY_SPEC,
    TICKER_AND_SIP,
    TIMEOUT,
    TRADES_SPEC,
    VALUES_SPEC,
    FlatFiles,
    R,
    Recorder,
    follows,
    join_reads,
    make,
    released,
    strict,
    trades,
    until,
)
from tests._fakes.rest import FakeRestServer, Reply, envelope

FUTURES_SPEC = DatasetSpec(
    name="futures.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.FUTURES,
    channels={NotBars.ANY: "T"},
    history_path="/futures/v1/trades/{ticker}",
)


# Both history sources, with the keys of the real datasets, so the columns each fills are real.
TRADES_FILES_SPEC = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    history_path="/v3/trades/{stockTicker}",
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (R, "/v3/trades/{stockTicker}"): SourceKeys(
            KeySpec("stockTicker", from_request=True), KeySpec("SipTimestamp")
        ),
        (FF, "us_stocks_sip/trades_v1"): TICKER_AND_SIP,
    },
)


def test_a_history_call_through_flat_files_passes_its_id(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    values.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    assert flat.call_ids == [1]


def test_history_rest_passes_the_window(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    trades(massive_session).history(
        "2025-03-03", "2025-03-04", source=HistorySource.REST, wait=True
    )
    verb, _, request, extra = rest.calls[0]
    window = extra["window"]
    assert verb == "history"
    assert window.span == DaySpan(date(2025, 3, 3), date(2025, 3, 4))
    assert window.times == TimeRange(
        datetime(2025, 3, 3, 5, tzinfo=UTC), datetime(2025, 3, 5, 5, tzinfo=UTC)
    )
    # Transports receive the parsed bounds as well as the window.
    assert (request.start, request.end) == (date(2025, 3, 3), date(2025, 3, 4))


def test_history_reads_through_now_by_default(
    massive_session: dm.MassiveSession, rest: Recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2025, 3, 5, 15, tzinfo=UTC)
    monkeypatch.setattr(history_source, "_now", lambda: now)
    trades(massive_session).history("2025-03-03", source=HistorySource.REST, wait=True)
    _, _, request, extra = rest.calls[0]
    assert request.end is None
    assert extra["window"].span == DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    start = datetime(2025, 3, 5, 14, tzinfo=UTC)
    trades(massive_session).history(start, source=HistorySource.REST, wait=True)
    assert rest.calls[1][3]["window"].times == TimeRange(start, now)


def test_an_empty_history_range_calls_no_transport(
    massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    moment = datetime(2025, 3, 3, 15, tzinfo=UTC)
    for source in (HistorySource.AUTO, HistorySource.REST):  # the session has no S3 keys
        table = trades(massive_session).history(moment, moment, source=source, wait=True)
        assert table.size == 0
        assert "Timestamp" in table.column_names
    assert rest.calls == []
    assert flat.calls == []


def test_history_of_futures_dates_passes_sessions(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    q = make(massive_session, FUTURES_SPEC, symbols=["ESZ5"])
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-05", source=HistorySource.REST, wait=True)
    window = rest.calls[0][3]["window"]
    assert window.sessions == DaySpan(date(2025, 3, 3), date(2025, 3, 5))
    assert window.requested is None  # requested one session at a time


def test_history_day_zone_per_class(massive_session: dm.MassiveSession, rest: Recorder) -> None:
    q = make(massive_session, BARS_SPEC, symbols=["C:EURUSD"], timespan=Timespan.HOUR)
    assert isinstance(q, HistoryQuery)
    q.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    assert rest.calls[0][3]["window"].times.start == datetime(2025, 3, 3, tzinfo=UTC)
    yields = make(massive_session, HISTORY_SPEC)
    assert isinstance(yields, HistoryQuery)
    yields.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    assert rest.calls[1][3]["window"].times.start == datetime(2025, 3, 3, 5, tzinfo=UTC)


def test_history_rest_rejects_all_except_grouped_daily(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    with pytest.raises(ValueError, match="needs explicit symbols"):
        trades(massive_session, dm.ALL).history(
            "2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True
        )
    minute = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.MINUTE)
    assert isinstance(minute, HistoryQuery)
    with pytest.raises(ValueError, match="FLAT_FILES"):
        minute.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    five = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.DAY, multiplier=5)
    assert isinstance(five, HistoryQuery)
    with pytest.raises(ValueError, match="needs explicit symbols"):
        five.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    daily = make(massive_session, BARS_SPEC, symbols=dm.ALL, timespan=Timespan.DAY)
    assert type(daily) is StreamHistoryQuery  # every bars call, whatever its timespan
    daily.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    assert len(rest.calls) == 1


def test_history_rest_needs_a_rest_endpoint(
    massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    values = make(massive_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(ValueError, match=r"indices\.values has no REST history"):
        values.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)


def test_history_dispatches_by_the_source_choice(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with caplog.at_level(logging.INFO, logger="deephaven_massive.queries"):
        values.history(
            "2025-03-03", "2025-03-04", wait=True
        )  # AUTO: flat files are the only source
        values.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
        trades(s3_session).history("2025-03-03", "2025-03-03", wait=True)  # AUTO: no flat files
    assert [c[3]["dates"] for c in flat.calls] == [
        (date(2025, 3, 3), date(2025, 3, 4)),
        (date(2025, 3, 3),),
    ]
    assert len(rest.calls) == 1
    # Only AUTO calls log their choice; a forced source does not.
    assert caplog.messages == [
        (
            "indices.values history: AUTO chose FLAT_FILES (REST cannot serve it: indices.values "
            "has no REST history)"
        ),
        (
            "stocks.trades history: AUTO chose REST (flat files cannot serve it: stocks.trades has "
            "no flat files)"
        ),
    ]


def test_history_with_no_possible_source_raises_before_any_transport(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    every = trades(massive_session, dm.ALL)  # no flat files, and REST needs explicit symbols
    with pytest.raises(ValueError, match=r"stocks\.trades history has no possible source"):
        every.history("2025-03-03", "2025-03-03", wait=True)
    with pytest.raises(ValueError, match=r"stocks\.trades cannot be read from flat files"):
        trades(massive_session).history(
            "2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True
        )
    assert rest.calls == []
    assert flat.calls == []


def test_history_argument_errors(massive_session: dm.MassiveSession, flat: Recorder) -> None:
    q = trades(massive_session)
    with pytest.raises(TypeError, match=r"source must be dm\.HistorySource"):
        q.history(
            "2025-03-03",
            "2025-03-03",
            source="rest",  # type: ignore[arg-type]  # the point: refused at run time
            wait=True,
        )
    with pytest.raises(TypeError, match=r"reread must be dm\.Reread"):
        q.history(
            "2025-03-03",
            "2025-03-03",
            reread="never",  # type: ignore[arg-type]  # the point: refused at run time
            wait=True,
        )
    with pytest.raises(ValueError, match="needs a timezone: pass an aware datetime or a date"):
        q.history("2025-03-03T09:30", "2025-03-04", wait=True)
    with pytest.raises(ValueError, match="must not be after end"):
        q.history("2025-03-04", "2025-03-03", wait=True)
    with pytest.raises(ValueError, match="start and end must both be dates or both be datetimes"):
        q.history("2025-03-03", datetime(2025, 3, 4, tzinfo=UTC), wait=True)
    assert flat.calls == []


def test_history_reads_a_static_symbol_table_once_for_every_source(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A refreshing table is followed instead (tests/tables/test_followed_read.py).
    watchlist = new_table([string_col("Symbol", ["MSFT", "AAPL"])])
    reads: list[bool] = []
    iterate = Table.iter_tuple
    expected = massive_session._core.exec_ctx.j_exec_ctx

    def recorded(
        table: Table,
        cols: str | Sequence[str] | None = None,
        *,
        tuple_name: str = "Deephaven",
        chunk_size: int = 2048,
    ) -> Iterator[tuple[object, ...]]:
        reads.append(get_exec_ctx().j_exec_ctx.equals(expected))
        return iterate(table, cols, tuple_name=tuple_name, chunk_size=chunk_size)

    monkeypatch.setattr(Table, "iter_tuple", recorded)
    q = trades(massive_session, watchlist)

    def forced_rest() -> None:  # from a thread with no execution context of its own
        q.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)

    worker = threading.Thread(target=forced_rest)
    worker.start()
    worker.join(TIMEOUT)
    q.history("2025-03-03", "2025-03-03", wait=True)
    # Each verb read the table once, inside the session's context, and every source received
    # the same sorted tuple, never the table itself.
    assert reads == [True, True]
    assert rest.calls[0][2].symbols == ("AAPL", "MSFT")
    assert rest.calls[1][2].symbols == ("AAPL", "MSFT")  # AUTO: the dataset has no flat files


def test_history_refuses_a_symbol_table_row_that_is_not_canonical_under_raise(
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    flat: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strict(massive_session, monkeypatch)
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft", None])]))
    for source in HistorySource:
        with pytest.raises(
            ValueError,
            match=r"^stocks\.trades: a symbol table holds 'msft' .*dm\.canonicalize_symbols",
        ):
            q.history("2025-03-03", "2025-03-03", source=source, wait=True)
    assert rest.calls == []
    assert flat.calls == []


def test_history_skips_a_symbol_table_row_that_is_not_canonical(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    q = trades(massive_session, new_table([string_col("Symbol", ["AAPL", "msft", None])]))
    q.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)
    assert rest.calls[0][2].symbols == ("AAPL",)
    assert flat.calls == []
    only_bad = trades(massive_session, new_table([string_col("Symbol", ["msft"])]))
    with pytest.raises(
        ValueError,
        match=r"^m\.stocks\.trades\(\): no row of the symbol table is a ticker this dataset",
    ):
        only_bad.history("2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True)


def test_history_rejects_a_symbol_table_without_symbols(
    *, massive_session: dm.MassiveSession, rest: Recorder, flat: Recorder
) -> None:
    for content in ([], [None]):
        q = trades(massive_session, new_table([string_col("Symbol", content)]))
        for source in (HistorySource.AUTO, HistorySource.REST):  # the session has no S3 keys
            with pytest.raises(
                ValueError, match=r"^m\.stocks\.trades\(\): the symbol table holds no symbols$"
            ):
                q.history("2025-03-03", "2025-03-03", source=source, wait=True)
    assert rest.calls == []
    assert flat.calls == []


@pytest.mark.parametrize("unreadable", list(dm.OnError))
def test_an_empty_window_has_the_session_columns(unreadable: dm.OnError) -> None:
    # The empty result of a window no row fits in has the session's columns, with RawSymbol
    # exactly when the session keeps unreadable rows, as every other result of it does.
    config = dm.MassiveConfig(api_key="test-key", unreadable_symbols=unreadable)
    with dm.MassiveSession(config) as session:
        q = trades(session)
        empty = q._empty_rows()
        names = [c.name for c in empty.columns]
        assert ("RawSymbol" in names) is (unreadable is not dm.OnError.RAISE)
        assert names == [name for (name,) in q.columns().iter_tuple(cols="Name")]


def test_a_history_table_names_its_source(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    days = ("2025-03-03", "2025-03-04")
    ticks = trades(s3_session).history(*days, wait=True)
    forced = trades(s3_session).history(*days, source=HistorySource.REST, wait=True)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    files = values.history(*days, source=HistorySource.FLAT_FILES, wait=True)
    summary = "A dataset for the tests."
    assert [t.attributes()["TableDescription"] for t in (ticks, forced, files)] == [
        (
            f"stocks.trades: {summary} Read from REST: AUTO chose it (flat files cannot serve "
            "it: stocks.trades has no flat files)."
        ),
        f"stocks.trades: {summary} Read from REST: requested with source=dm.HistorySource.REST.",
        (
            f"indices.values: {summary} Read from flat files: requested with "
            "source=dm.HistorySource.FLAT_FILES."
        ),
    ]


def _symbols(count: int) -> list[str]:
    return [f"S{n:03d}" for n in range(count)]


def test_auto_warns_once_about_columns_the_other_source_fills(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(q, HistoryQuery)
    instant = datetime(2025, 3, 3, 15, tzinfo=UTC)
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        q.history(
            instant, instant, wait=True
        )  # no row can lie in the range: nothing is read, no warning
        assert caplog.messages == []
        q.history("2025-03-03", "2025-03-03", wait=True)
        q.history("2025-03-04", "2025-03-04", wait=True)  # once per session
    warning = (
        "stocks.trades history: AUTO read flat files (60 symbols >= flat_file_symbol_threshold "
        "50); the read leaves DecimalSize null, which the other source, REST, fills: pass "
        "source=dm.HistorySource.REST to get them (logged once per session)"
    )
    assert caplog.messages == [warning]


def test_auto_reads_the_days_not_published_yet_from_rest(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The flat files hold March 3 but not yet March 4: AUTO reads March 4 from REST and
    # returns the whole range; FLAT_FILES raises; so does AUTO where REST cannot serve.
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    files = FlatFiles(flat, s3_session._core, (date(2025, 3, 4),))
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: files)
    many = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(many, HistoryQuery)
    with caplog.at_level(logging.INFO, logger="deephaven_massive.queries"):
        many.history("2025-03-03", "2025-03-04", wait=True)
    [(verb, _, request, extra)] = rest.calls
    assert (verb, request.start, request.end) == ("history", date(2025, 3, 4), date(2025, 3, 4))
    assert extra["window"].span == DaySpan(date(2025, 3, 4), date(2025, 3, 4))
    assert caplog.messages[-1] == (
        "stocks.trades history: AUTO read REST because the flat files are missing for these "
        "days: 1 [2025-03-04]"
    )
    # Holidays are inferred only where no other source can serve the read.
    assert files.inferred == [False]
    note = (
        "stocks.trades: the flat file for 2025-03-04 is not published yet. Massive usually "
        "publishes a day's flat files by about 11:00 ET the next day, but the time varies. "
    )
    with pytest.raises(dm.MassiveError) as forced:
        many.history("2025-03-03", "2025-03-04", source=HistorySource.FLAT_FILES, wait=True)
    assert str(forced.value) == note + (
        "Retry later, or read REST with source=dm.HistorySource.REST"
    )
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(dm.MassiveError) as unserved:
        values.history("2025-03-03", "2025-03-04", wait=True)
    assert files.inferred == [False, True, True]
    assert str(unserved.value) == note.replace("stocks.trades", "indices.values") + (
        "Retry later; REST cannot serve this read: indices.values has no REST history"
    )
    assert len(rest.calls) == 1


def test_a_background_flat_file_read_checks_the_rest_it_may_read_at_the_call(
    *, s3_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Under AUTO, REST reads the days the files lack, so its parameter errors raise at the
    # call, as a read that waits raises them; FLAT_FILES never reads REST, and checks nothing.
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(q, HistoryQuery)
    auto = q.history("2025-03-03", "2025-03-03")
    forced = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES)
    join_reads(s3_session)
    assert [check[0] for check in rest.checks] == ["history"]
    assert rest.split_pairs == [False]  # a parameter check: the files read the pairs whole
    assert [call[0] for call in flat.calls] == ["history", "history"]

    def refuse(*_: object, **__: object) -> None:
        raise ValueError("the path does not take adjusted")

    monkeypatch.setattr(rest, "check_history", refuse)
    with pytest.raises(ValueError, match="does not take adjusted"):
        q.history("2025-03-03", "2025-03-03")
    released(s3_session, auto, forced)


def test_a_refused_flat_file_read_is_read_from_rest_under_auto(
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )

    def refused(*_: object, **__: object) -> Table:
        raise dm.EntitlementError("the plan does not include these flat files", status=403)

    monkeypatch.setattr(flat, "history", refused)
    q = make(s3_session, TRADES_FILES_SPEC, symbols=_symbols(60))
    assert isinstance(q, HistoryQuery)
    # FLAT_FILES raises, naming REST.
    with pytest.raises(dm.EntitlementError) as info:
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    assert str(info.value) == (
        "the plan does not include these flat files; REST serves this read too: pass "
        "source=dm.HistorySource.REST to history() (it can fill other columns; columns() "
        "shows which)"
    )
    assert info.value.status == 403
    assert rest.calls == []
    # AUTO chose the flat files (60 symbols), and serves the read from REST instead, warning.
    with caplog.at_level(logging.WARNING, logger="deephaven_massive"):
        q.history("2025-03-03", "2025-03-03", wait=True)
    assert [c[0] for c in rest.calls] == ["history"]
    assert caplog.messages[-1] == (
        "stocks.trades history: AUTO read REST because S3 refused the flat files (the plan does "
        "not include these flat files)"
    )

    def rejected(*_: object, **__: object) -> Table:
        raise dm.AuthenticationError("S3 rejected the flat-file credentials", status=403)

    # A rejection of the S3 credentials is served from REST the same way, and raised as it
    # came under FLAT_FILES.
    monkeypatch.setattr(flat, "history", rejected)
    with pytest.raises(dm.AuthenticationError, match=r"^S3 rejected the flat-file credentials$"):
        q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    described = q.history("2025-03-03", "2025-03-03", wait=True)
    assert [c[0] for c in rest.calls] == ["history", "history"]
    assert described.attributes()["TableDescription"].endswith(
        "Read from REST: AUTO chose flat files, which S3 refused."
    )
    # Where REST cannot serve the read, the refusal is raised as it came.
    monkeypatch.setattr(flat, "history", refused)
    values = make(s3_session, VALUES_SPEC, symbols=["I:SPX"])
    assert isinstance(values, HistoryQuery)
    with pytest.raises(dm.EntitlementError, match=r"^the plan does not include these flat files$"):
        values.history("2025-03-03", "2025-03-03", wait=True)


def test_history_follows_a_refreshing_table_from_rest_under_auto(
    live: Callable[..., InputTable],
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest, flat = Recorder(s3_session._core.catalog), Recorder(s3_session._core.catalog)
    monkeypatch.setattr(s3_session._core, "rest_transport", lambda: rest)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    many = [f"S{i:02d}" for i in range(60)]  # past the flat-file threshold of a static call
    q = make(s3_session, TRADES_FILES_SPEC, symbols=live(*many))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03", wait=True)
    assert result.is_refreshing
    assert flat.by_symbol == []
    assert [names for names, _, _ in rest.by_symbol] == [tuple(many)]  # the call's own read
    assert result.attributes()["TableDescription"].endswith(
        "Follows its symbol table over 2025-03-03 through 2025-03-03. Read from REST: AUTO "
        "reads a followed symbol table from REST, which can serve it."
    )
    s3_session.release(result)
    few = make(s3_session, TRADES_FILES_SPEC, symbols=live("AAPL", "MSFT", "NVDA"))
    assert isinstance(few, HistoryQuery)
    s3_session.release(few.history("2025-03-03", "2025-03-03", wait=True))
    assert [names for names, _, _ in rest.by_symbol][1] == ("AAPL", "MSFT", "NVDA")
    assert flat.by_symbol == []
    follows(s3_session)


def test_history_follows_from_flat_files_when_rest_cannot_serve(
    live: Callable[..., InputTable],
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, VALUES_SPEC, symbols=live("I:SPX"))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03", wait=False)
    until(lambda: [names for names, _, _ in flat.by_symbol] == [("I:SPX",)])
    assert result.attributes()["TableDescription"].endswith(
        "Read from flat files: AUTO reads a followed symbol table from flat files, since "
        "indices.values has no REST history."
    )
    s3_session.release(result)
    follows(s3_session)


def test_history_forced_to_flat_files_reads_the_waiting_symbols_in_one_scan(
    live: Callable[..., InputTable],
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flat = Recorder(s3_session._core.catalog)
    monkeypatch.setattr(
        s3_session._core, "flat_file_transport", lambda: FlatFiles(flat, s3_session._core)
    )
    q = make(s3_session, TRADES_FILES_SPEC, symbols=live("AAPL", "MSFT"))
    assert isinstance(q, HistoryQuery)
    result = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    assert [names for names, _, _ in flat.by_symbol] == [("AAPL", "MSFT")]
    assert (
        "Read from flat files: requested with source=dm.HistorySource.FLAT_FILES."
        in (result.attributes()["TableDescription"])
    )
    s3_session.release(result)
    follows(s3_session)


def test_a_followed_flat_file_history_keeps_one_record_of_what_it_logged_per_call(
    live: Callable[..., InputTable],
    *,
    s3_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every read of a call shares the call's record, so a refused venue is logged once per
    # call, and the next call starts its own; the result holds the record, not the process.
    flat = Recorder(s3_session._core.catalog)
    files = FlatFiles(flat, s3_session._core)
    monkeypatch.setattr(s3_session._core, "flat_file_transport", lambda: files)
    watchlist = live("AAPL")
    q = make(s3_session, TRADES_FILES_SPEC, symbols=watchlist)
    assert isinstance(q, HistoryQuery)
    first = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(files.logged) == 2)
    second = q.history("2025-03-03", "2025-03-03", source=HistorySource.FLAT_FILES, wait=True)
    one, again, other = files.logged
    assert one is again
    assert other is not one
    s3_session.release(first)
    s3_session.release(second)
    follows(s3_session)


def test_history_forced_to_rest_follows_from_rest(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live("AAPL")).history(
        "2025-03-03", "2025-03-03", source=HistorySource.REST, wait=True
    )
    assert (
        "Read from REST: requested with source=dm.HistorySource.REST."
        in (result.attributes()["TableDescription"])
    )
    assert rest.checks == [("history", TRADES_SPEC, rest.checks[0][2])]  # checked at the call
    massive_session.release(result)
    follows(massive_session)


def test_an_added_symbol_is_read_over_the_call_s_window(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    watchlist = live("AAPL")
    result = trades(massive_session, watchlist).history(
        "2025-03-03", wait=True
    )  # through the call instant
    watchlist.add(new_table([string_col("Symbol", ["MSFT"])]))
    until(lambda: len(rest.by_symbol) == 2)
    (first, window, caller), (added, later, reader) = rest.by_symbol
    assert (first, added) == (("AAPL",), ("MSFT",))
    assert later is window  # the same window, fixed at the call: the same end and cutoff
    assert caller == threading.current_thread().name
    assert " read_" in reader
    end = window.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert f"over 2025-03-03 through {end}." in result.attributes()["TableDescription"]
    massive_session.release(result)
    follows(massive_session)


def test_an_instant_range_prints_its_instants(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    start = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
    end = datetime(2025, 3, 3, 15, tzinfo=UTC)
    result = trades(massive_session, live("AAPL")).history(start, end, wait=False)
    assert (
        "over 2025-03-03T14:30:00Z through 2025-03-03T15:00:00Z."
        in (result.attributes()["TableDescription"])
    )
    massive_session.release(result)
    follows(massive_session)


def test_wait_false_returns_at_once_and_reads_in_the_background(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live("AAPL")).history("2025-03-03", "2025-03-03", wait=False)
    assert result.is_refreshing
    until(lambda: len(rest.by_symbol) == 1)
    assert " read_" in rest.by_symbol[0][2]
    massive_session.release(result)
    follows(massive_session)


def test_an_empty_refreshing_table_returns_an_empty_following_result(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    result = trades(massive_session, live()).history("2025-03-03", "2025-03-03", wait=True)
    assert result.is_refreshing
    assert result.size == 0
    assert rest.by_symbol == []
    assert [verb for verb, _, _ in rest.checks] == ["history"]  # checked with no symbol too
    massive_session.release(result)
    follows(massive_session)


def test_wait_true_raises_when_every_call_time_symbol_fails(
    live: Callable[..., InputTable], *, massive_session: dm.MassiveSession, rest: Recorder
) -> None:
    refusal = dm.EntitlementError("HTTP 403: not in the plan")
    rest.outcomes = {
        "AAPL": SymbolFailure(refusal, FailureKind.PERMANENT),
        "MSFT": SymbolFailure(dm.EntitlementError("HTTP 403: other"), FailureKind.PERMANENT),
    }
    with pytest.raises(dm.EntitlementError, match="not in the plan"):
        trades(massive_session, live("AAPL", "MSFT")).history("2025-03-03", "2025-03-03", wait=True)
    assert massive_session._core.workers._workers == {}
    rest.outcomes = {}
    rest.raises = dm.AuthenticationError("HTTP 401: wrong key")  # the read fails as a unit
    with pytest.raises(dm.AuthenticationError, match="wrong key"):
        trades(massive_session, live("AAPL")).history("2025-03-03", "2025-03-03", wait=True)
    follows(massive_session)


def test_wait_true_warns_once_when_some_call_time_symbols_fail(
    live: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest.outcomes = {
        "MSFT": SymbolFailure(
            dm.EntitlementError("HTTP 403: not in the plan"), FailureKind.PERMANENT
        )
    }
    with caplog.at_level(logging.WARNING, logger="deephaven_massive.queries"):
        result = trades(massive_session, live("AAPL", "MSFT")).history(
            "2025-03-03", "2025-03-03", wait=True
        )
    assert caplog.messages == [
        (
            "stocks.trades history: 1 of the 2 symbols of the call failed and are left out of the "
            "result for now; m.symbol_reads() shows each one (retrying, or failed): "
            "MSFT: HTTP 403: not in the plan"
        )
    ]
    massive_session.release(result)
    follows(massive_session)


def test_reread_is_ignored_for_a_static_symbol_table(
    *, massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    static = new_table([string_col("Symbol", ["AAPL"])])
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"):
        result = trades(massive_session, static).history(
            "2025-03-03", "2025-03-03", reread=dm.Reread.NEVER, wait=True
        )
    assert not result.is_refreshing
    assert rest.by_symbol == []
    assert any(
        "history('2025-03-03', '2025-03-03', source=<HistorySource.AUTO: 'auto'>, "
        "reread=<Reread.NEVER: 'never'>, wait=True)" in message
        for message in caplog.messages
    )


def test_a_call_that_waits_names_wait_true_in_its_start(
    *, massive_session: dm.MassiveSession, rest: Recorder, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="deephaven_massive.queries"):
        trades(massive_session).history("2025-03-03", "2025-03-03", wait=True)
    starts = [m for m in caplog.messages if " start: " in m]
    assert len(starts) == 1
    assert starts[0].endswith(
        ".history('2025-03-03', '2025-03-03', source=<HistorySource.AUTO: 'auto'>, wait=True)"
    )


def test_a_followed_table_refuses_a_bad_row_or_a_failed_table_at_the_call(
    live: Callable[..., InputTable],
    *,
    massive_session: dm.MassiveSession,
    rest: Recorder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    only_bad = live("msft")
    with pytest.raises(ValueError, match="no row of the symbol table is a ticker"):
        trades(massive_session, only_bad).history("2025-03-03", "2025-03-03", wait=True)
    strict(massive_session, monkeypatch)
    with pytest.raises(ValueError, match="a symbol table holds 'msft'"):
        trades(massive_session, live("AAPL", "msft")).history("2025-03-03", "2025-03-03", wait=True)
    assert rest.by_symbol == []


def test_a_followed_history_reads_each_symbol_from_rest_with_its_own_outcome(
    live: Callable[..., InputTable],
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    drain: Callable[[], None],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Through the real REST transport: Massive has no data for AAPL (404), the plan lacks MSFT
    # (403), and NVDA has one trade.
    nanos = int(datetime(2025, 3, 3, 15, tzinfo=UTC).timestamp() * 1e9)
    fake_rest.reply("/v3/trades/AAPL", Reply(envelope(status="NOT_FOUND"), status=404))
    fake_rest.reply(
        "/v3/trades/MSFT",
        Reply(envelope(status="NOT_AUTHORIZED", message="not in your plan"), status=403),
    )
    fake_rest.reply(
        "/v3/trades/NVDA",
        Reply(envelope([{"sip_timestamp": nanos, "price": 1.0, "size": 10}])),
    )
    with caplog.at_level(logging.WARNING):
        result = rest_session.stocks.trades(live("AAPL", "MSFT", "NVDA")).history(
            "2025-03-03", "2025-03-03", wait=False
        )
        until(lambda: [r["Symbol"] for r in dh_to_pylist(result)] == ["NVDA"])

        def states() -> dict[str, tuple[str, int, str | None]]:
            drain()
            return {
                r["Symbol"]: (r["State"], r["Rows"], r["Error"])
                for r in dh_to_pylist(rest_session.symbol_reads())
            }

        until(
            lambda: (
                {name: shown[0] for name, shown in states().items()}
                == {"AAPL": "DONE", "MSFT": "FAILED", "NVDA": "DONE"}
            )
        )
    found = states()
    assert found["AAPL"] == ("DONE", 0, None)
    assert found["NVDA"] == ("DONE", 1, None)
    assert found["MSFT"][:2] == ("FAILED", 0)
    assert "403" in (found["MSFT"][2] or "")
    assert any("AAPL" in m for m in caplog.messages)  # the 404's WARNING is in the log only
    rest_session.release(result)
    follows(rest_session)
