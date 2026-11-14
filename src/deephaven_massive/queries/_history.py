"""`HistoryQuery` and `StreamHistoryQuery`: the queries that read a past range with `history()`.

`history(start, end)` reads a dataset's rows over a range of days or instants, from Massive's
REST API or its S3 flat files. It picks the source itself (`choose_source` in
`_datasets/history_source.py`), reads from REST the days the flat files have not published yet,
follows a watchlist over the call's fixed window, and returns a static table or one that a
background read fills. A dataset that both streams and has history, such as trades or
one-minute bars, gets `StreamHistoryQuery`, which also has the stream methods: one query gives
the past and the live feed, with the same columns.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime
from typing import TYPE_CHECKING

from deephaven import arrow as dh_arrow

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.dates import bounds, parse_date_like
from deephaven_massive._datasets.history_source import (
    HISTORY_TRANSPORTS,
    choose_source,
    history_window,
    partition_dates,
    rest_history_problem,
    warn_unfilled,
)
from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable, symbol_class
from deephaven_massive._enums import (
    AssetClass,
    HistorySource,
    Reread,
    _require_enum,
)
from deephaven_massive._errors import (
    UnmatchedFormError,
    raises_through,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables.diagnostics import utc_text
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.time_text import deephaven_time_text
from deephaven_massive._tables.transports import (
    FlatFileTransport,
    RestTransport,
)
from deephaven_massive._tables.watchlist import (
    SymbolRowRule,
)
from deephaven_massive.queries._query import Query, _read_symbol_table, _verb, _wait
from deephaven_massive.queries._stream import SymbolStreamQuery

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request

__all__ = ["HistoryQuery", "StreamHistoryQuery"]

logger = logging.getLogger(__name__)


def _symbol_classes(symbols: tuple[str, ...] | AllSymbols | None) -> Iterable[AssetClass]:
    """Give the classes of the symbols a static `history()` call lists, which set its days.

    Args:
        symbols: The call's static symbols.

    Returns:
        The class of each listed symbol (`symbol_class`); none for `dm.ALL` or a dataset
        without instruments.

    Raises:
        UnmatchedFormError: For a form of symbols with no rule here.
    """
    match symbols:
        case tuple():
            return map(symbol_class, symbols)
        case AllSymbols() | None:
            return ()
        case _:
            raise UnmatchedFormError(symbols, "history()'s symbol classes")


def _window_text(first: date | datetime, last: date | datetime | None, now: datetime) -> str:
    """Write a followed `history()`'s fixed window for its table description.

    Args:
        first: The parsed `start`.
        last: The parsed `end`, or None for the call's instant.
        now: The call's instant (`HistoryWindow.now`).

    Returns:
        Such as `"2026-10-01 through 2026-11-02T14:05:12Z"`: a date as itself, an instant
        as UTC text to the second, and an open end as the call's instant.
    """

    def shown(value: date | datetime) -> str:
        return utc_text(value.timestamp()) if isinstance(value, datetime) else value.isoformat()

    end = utc_text(now.timestamp()) if last is None else shown(last)
    return f"{shown(first)} through {end}"


class HistoryQuery(Query):
    """Any query that reads a past range: `history(start, end)` reads the rows between two times.

    Annotate with `HistoryQuery` when your code only reads history, such as a helper that
    loads a week of any dataset's rows. Call `history(start, end)` for a table of the rows in
    that range. The library reads them from Massive's REST API or its S3 flat files, whichever
    serves the range, unless you choose with `source`. Each call reads its range again.

    Every dataset with history also streams today, so its method returns a
    `StreamHistoryQuery`, a subclass of this class that adds the stream methods; code annotated
    with `HistoryQuery` takes it.

    Import it as `from deephaven_massive.queries import HistoryQuery` to annotate your own code.
    You get one from a dataset method and never construct one.

    Example:
        ::

            from deephaven.table import Table

            from deephaven_massive.queries import HistoryQuery


            def week(query: HistoryQuery) -> Table:
                return query.history("2025-01-06", "2025-01-10")  # whole days, both included


            hourly = week(m.options.bars("O:AAPL250117C00150000", dm.Timespan.HOUR))
    """

    __slots__ = ()

    def _check_rest_history(self) -> None:
        """Reject `history(source=REST)` where REST cannot serve the request.

        Raises:
            ValueError: If the dataset has no REST history endpoint, or the symbols are
                `dm.ALL` and the request is not daily bars with a grouped-daily endpoint
                (`rest_history_problem`); the message names `HistorySource.FLAT_FILES`.
        """
        problem = rest_history_problem(self._spec, self._request)
        if problem is not None:
            raise ValueError(f"{problem}; use source=dm.HistorySource.FLAT_FILES")

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _require_enum,
        parse_date_like,
        _read_symbol_table,
        bounds,
        choose_source,
        _check_rest_history,
        RestTransport.history,
        RestTransport.check_history,
        FlatFileTransport.history,
        RestTransport.history_by_symbol,
        FlatFileTransport.history_by_symbol,
        open_followed_read,
        _wait,
    )
    def history(
        self,
        start: DateLike,
        end: DateLike | None = None,
        *,
        source: HistorySource = HistorySource.AUTO,
        reread: Reread = Reread.ALWAYS,
        wait: bool = False,
    ) -> Table:
        """Return the dataset's rows over a range of days or of exact times.

        By default the call returns a live table with no rows at once, and the rows arrive in the
        background. Deephaven updates the table, and every table built on it, as they land. For
        symbols passed in code or in a static table, they land all in one update, and
        `m.streams()` shows the read `LOADING`, then `DONE` or `FAILED`. Pass `wait=True` to wait
        for the rows; without a watchlist, you then get a static table.

        Dates, such as `"2025-03-03"`, read whole days in the market's time zone, both ends
        included. Times, such as a `datetime` with a time zone, read from `start` up to `end`, which
        is excluded. A bar is included only when its whole window is in the range and has ended.

        Rows come from REST or from Massive's flat files. By default, the library picks the source
        and reads from REST any day the flat files can't serve. The table's description says which
        source it read. Each flat file holds a whole day of data, and a flat-file read downloads
        all of it, however few symbols or short a range you ask for.

        A static table of symbols is read once, when you call. A watchlist is followed over the
        range fixed at the call: a symbol you add arrives with all its rows, and one you delete
        leaves
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#drive-with-a-watchlist).
        One symbol's failure leaves the watchlist's other symbols in place, and `m.symbol_reads()`
        shows each symbol's read.

        Args:
            start: The first day, or the first time included.
            end: The last day included, or the first time excluded. Leave it out to read through
                now.
            source: `dm.HistorySource.AUTO`, the default, picks the source; `REST` or `FLAT_FILES`
                forces one.
            reread: For a watchlist, whether a symbol you delete and add back is downloaded again
                (`dm.Reread.ALWAYS`, the default) or gets its earlier rows back without a request
                (`dm.Reread.NEVER`).
            wait: False, the default, returns at once and fills the table in the background. True
                waits for the rows; for a watchlist, until each symbol has been read once.

        Returns:
            A table with the dataset's columns, with no row when none is in the range: live, or
            static for `wait=True` without a watchlist.

        Raises:
            TypeError: If `source` isn't a `dm.HistorySource`, `reread` isn't a `dm.Reread`, `wait`
                isn't a `bool`, or a bound isn't a date or a time.
            ValueError: At the call, before any request, if:
                - it runs on a Deephaven update-graph thread;
                - a bound is a `datetime` without a time zone, unreadable text, or `NaT`;
                - one bound is a date and the other a time;
                - `start` is after `end`, or in the future when `end` is left out;
                - a static table of symbols is empty, or no row of a table of symbols can be sent;
                - no source can serve the request; the message gives each reason;
                - the endpoint doesn't take one of the query's parameters or filters.
            AuthenticationError: With `wait=True`, if Massive or its flat-file server rejects your
                keys.
            EntitlementError: With `wait=True`, if your plan doesn't include the data from any
                source the call can use.
            MassiveError: At the call, if the session is closed. With `wait=True`, also if:
                - a request fails;
                - adjusted bars come back unadjusted, or the reverse;
                - a trading day's flat file is missing and REST can't serve that day;
                - for a watchlist, every symbol's read failed.

        Without `wait=True`, an error from Massive fails the table instead of raising: it is
        logged and shows in `Error` of `m.streams()`. A watchlist's table fails when every symbol
        it held at the call fails for good, such as on a plan without the dataset.

        Example:
            ::

                day = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02")
                rows = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02", wait=True)
        """
        background = not _wait(wait)
        again = _require_enum(reread, Reread, "reread")
        rereads = "" if again is Reread.ALWAYS else f", reread={again!r}"
        return self._logged_call(
            f"history({start!r}, {end!r}, source={source!r}{rereads}"
            f"{'' if background else ', wait=True'})",
            lambda call: self._history(
                start, end, source=source, reread=again, call=call, wait=wait
            ),
            background=background,
        )

    def _history(
        self,
        start: DateLike,
        end: DateLike | None,
        *,
        source: HistorySource,
        reread: Reread,
        call: int,
        wait: bool,
    ) -> Table:
        """Run `history()` as call `call`; see `history` for everything else.

        Everything that needs no request is checked here, on the calling thread, before the
        transport call runs or a background read starts. A refreshing symbol table is followed
        (`_followed_history`).

        Args:
            start: As `history` takes it.
            end: As `history` takes it.
            source: As `history` takes it.
            reread: As `history` takes it.
            call: The call id.
            wait: As `history` takes it.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        chosen = _require_enum(source, HistorySource, "source")
        first = parse_date_like(deephaven_time_text(start, "start"), "start")
        last = (
            None
            if end is None
            else parse_date_like(deephaven_time_text(end, "end"), "end", upper=True)
        )
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            return self._followed_history(
                call, symbols, first=first, last=last, source=chosen, reread=reread, wait=wait
            )
        if isinstance(symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, symbols, query=repr(self))
        # A dataset with a path per symbol class takes its days from its symbols' class.
        request = replace(self._request, start=first, end=last, symbols=symbols)
        window = history_window(
            self._spec, request, start=first, end=last, classes=_symbol_classes(symbols)
        )
        spec = self._spec
        if chosen is HistorySource.REST:
            self._check_rest_history()
            note = "Read from REST: requested with source=dm.HistorySource.REST."
            return self._rest_history(call, request, window, wait=wait, note=note)
        dates = partition_dates(spec, window)
        choice = choose_source(spec, request, requested=chosen, config=self._core.config)
        label = TRANSPORT_LABELS[HISTORY_TRANSPORTS[choice.source]]
        if chosen is HistorySource.AUTO:
            logger.info(
                "%s history: AUTO chose %s (%s)", spec.name, choice.source.name, choice.reason
            )
            note = f"Read from {label}: AUTO chose it ({choice.reason})."
        else:
            note = f"Read from {label}: requested with source=dm.HistorySource.FLAT_FILES."
        if chosen is HistorySource.AUTO and not window.empty:
            warn_unfilled(spec, choice, catalog=self._core.catalog, warnings=self._core.warnings)
        if choice.source is HistorySource.REST:
            return self._rest_history(call, request, window, wait=wait, note=note)
        forced = chosen is HistorySource.FLAT_FILES
        return self._flat_history(
            call, request, window, dates=dates, wait=wait, note=note, forced=forced
        )

    def _followed_history(
        self,
        call: int,
        symbols: SymbolTable,
        *,
        first: date | datetime,
        last: date | datetime | None,
        source: HistorySource,
        reread: Reread,
        wait: bool,
    ) -> Table:
        """Run a `history()` call that follows a refreshing symbol table.

        The table's current rows are checked now, as a live verb checks them, and its call-time
        symbols fix the window's day zones; the window itself is fixed for every later read.
        Under `AUTO` the source is REST whenever REST can serve the request, whatever the
        number of symbols, so one added symbol never re-reads the flat files of the whole range;
        otherwise flat files. With `wait=True` the call-time symbols are read now, as one read;
        the call raises only when every one of them failed, and logs one WARNING when some did.

        Args:
            call: The call id.
            symbols: The boxed refreshing table.
            first: The parsed `start`.
            last: The parsed `end`, or None for the call's instant.
            source: As `history` takes it.
            reread: As `history` takes it.
            wait: As `history` takes it.

        Returns:
            The refreshing result (`open_followed_read`).

        Raises:
            ValueError: If no source can serve the request, as `choose_source` and
                `_check_rest_history` say.
        """
        spec, core = self._spec, self._core
        call_time = _read_symbol_table(core, spec, symbols, query=repr(self), live=True)
        request = replace(self._request, start=first, end=last, symbols=call_time)
        window = history_window(
            spec, request, start=first, end=last, classes=map(symbol_class, call_time)
        )
        span = _window_text(first, last, window.now)
        if source is HistorySource.REST:
            self._check_rest_history()
            plan = self._rest_plan(call, request, window=window)
            note = "Read from REST: requested with source=dm.HistorySource.REST."
        elif source is HistorySource.AUTO and rest_history_problem(spec, request) is None:
            plan = self._rest_plan(call, request, window=window)
            note = (
                "Read from REST: AUTO reads a followed symbol table from REST, which can serve it."
            )
        else:
            choose_source(spec, request, requested=source, config=core.config)
            files = core.flat_file_transport()
            dates = partition_dates(spec, window)
            forced = source is HistorySource.FLAT_FILES
            plan = FollowPlan(
                Transport.FLAT_FILES,
                one_scan=True,
                symbols_per_read=1,
                read=lambda names, stop: files.history_by_symbol(
                    spec,
                    request,
                    window,
                    symbols=names,
                    dates=dates,
                    call=call,
                    forced=forced,
                    stop=stop,
                ),
            )
            note = (
                "Read from flat files: requested with source=dm.HistorySource.FLAT_FILES."
                if forced
                else "Read from flat files: AUTO reads a followed symbol table from flat files, "
                f"since {rest_history_problem(spec, request)}."
            )
        logger.info("%s history: follows its symbol table; %s", spec.name, note)
        loaded = self._first_read(plan, call_time, verb="history") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=symbols.table,
            rule=SymbolRowRule.of(spec, core.config.unsendable_symbols),
            reread=reread,
            plan=plan,
            note=f"Follows its symbol table over {span}. {note}",
            first=loaded,
        )

    def _rest_plan(self, call: int, request: Request, *, window: HistoryWindow) -> FollowPlan:
        """Return the plan of a followed `history()` that reads REST, its parameters checked.

        Args:
            call: The call id.
            request: The request with the call-time symbols and the parsed bounds.
            window: The call's window.

        Returns:
            One read per symbol (`RestTransport.history_by_symbol`).
        """
        spec = self._spec
        rest = self._core.rest_transport()
        if not window.empty:  # a parameter error raises now, with symbols or without
            rest.check_history(spec, request, window, split_pairs=False)
        return FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=1,
            read=lambda names, stop: rest.history_by_symbol(
                spec, request, window, symbols=names, call=call, stop=stop
            ),
        )

    def _flat_history(
        self,
        call: int,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        wait: bool,
        note: str,
        forced: bool,
    ) -> Table:
        """Run a `history()` call that reads the flat files, now or in the background.

        A blocking read's description names the source that served it, REST included, after
        the read (`FlatFileTransport.read_history`), and what the files may lack: the days
        taken for holidays, and the futures sessions that may be incomplete. A background
        read's is written when the read starts, so under `AUTO`, when REST can serve the
        request, it says a day the flat files cannot serve is read from REST, and the read's
        WARNING names the days; the log names the days taken for holidays too.

        Args:
            call: The call id.
            request: The request with static symbols and the parsed bounds.
            window: The call's window.
            dates: The partition dates of the files the window needs.
            wait: As `history` takes it.
            note: The description's source sentence of the choice.
            forced: True for `source=FLAT_FILES`.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        spec = self._spec
        files = self._core.flat_file_transport()
        if window.empty:
            return self._result(
                call,
                request,
                wait=wait,
                transport=Transport.FLAT_FILES,
                note=note,
                run=lambda _stop: self._empty_rows(),
            )
        if wait:
            table, served, lacks = files.read_history(
                spec, request, window, dates=dates, call=call, forced=forced
            )
            source = note if served is None else served
            return self._described(table, note=source if lacks is None else f"{source} {lacks}")
        if not forced and rest_history_problem(spec, request) is None:
            # REST may read the days the files lack: its parameter errors raise now, as a read
            # that waits raises them before it reads those days.
            self._core.rest_transport().check_history(spec, request, window, split_pairs=False)
            # The description is written before the read: it can only say what may happen.
            note = f"{note} A day the flat files cannot serve is read from REST; the log names it."
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.FLAT_FILES,
            note=note,
            run=lambda stop: files.read_history(
                spec, request, window, dates=dates, call=call, forced=forced, stop=stop
            )[0],
        )

    def _rest_history(
        self, call: int, request: Request, window: HistoryWindow, *, wait: bool, note: str
    ) -> Table:
        """Read `history()` from REST, now or in the background.

        Args:
            call: The call id.
            request: The request with static symbols and the parsed bounds.
            window: The call's window; an empty one reads nothing.
            wait: As `history` takes it.
            note: The sentence that names the source in the table description.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        spec = self._spec
        rest = self._core.rest_transport()
        if not wait and not window.empty:  # a parameter error and a pair in code raise now
            rest.check_history(spec, request, window, split_pairs=self._splits_pairs_at_call)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=note,
            run=lambda stop: (
                self._empty_rows()
                if window.empty
                else rest.history(spec, request, window, call=call, stop=stop)
            ),
        )

    def _empty_rows(self) -> Table:
        """Return the static rows of a range no row can lie in, without any request.

        Returns:
            A static table with every column of the dataset and no row.
        """
        schema = self._core.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            return dh_arrow.to_table(schema.arrow_schema().empty_table())


class StreamHistoryQuery(SymbolStreamQuery, HistoryQuery):
    """A query of a stream that also has history: the live feed and the past, with one schema.

    Massive both streams some datasets live and keeps them for the past, such as stock trades
    and quotes, index values, and aggregate bars. Their dataset methods return a
    `StreamHistoryQuery`, which has every method of `SymbolStreamQuery` and of `HistoryQuery`:
    `history(start, end)` reads a past range, and the stream methods follow the live feed, with
    the same columns, so code written against one works on the other. `help(StreamQuery)`
    covers what the stream methods share, and `help(SymbolStreamQuery)` the symbols and
    watchlists they take.

    A `bars()` query streams only 1-second and 1-minute bars at `multiplier=1`, which Massive
    sends over its WebSocket; its stream methods refuse any other bars with `ValueError`, which
    names the bars that stream. `history()` reads every bar.

    Import it as `from deephaven_massive.queries import StreamHistoryQuery` to annotate your own
    code. You get one from a dataset method and never construct one.

    Example:
        ::

            q = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE)
            live = q.append_only()
            day = q.history("2025-03-03", "2025-03-03")
    """

    __slots__ = ()
