"""`FetchQuery`: the query of a REST dataset, read once with `fetch()` or again with `poll()`.

A REST dataset answers with what Massive has at the moment of the request: a stock's dividends,
each symbol's latest trade, a snapshot. `fetch()` reads it once: by default in the background,
into a live table that fills when the rows arrive, or with `wait=True` into a static table.
`poll(interval)` reads it again on a timer, into a live table. Both follow a live table of
symbols (a watchlist), and read a table passed where a dataset takes one value (an option
chain's underlying, a futures product code) value by value. The requests themselves are the
session's `RestTransport` (`_tables/transports.py`).
"""

from __future__ import annotations

import functools
import logging
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final

from deephaven_massive._config import require_positive_number
from deephaven_massive._datasets.filters import param_base
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import (
    PollMode,
    Reread,
    _require_enum,
)
from deephaven_massive._errors import (
    raises_through,
    required,
    user_defect,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import (
    rest_recency_note,
)
from deephaven_massive._tables.diagnostics import shown_query
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.refused_values import refused_values_of
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.transports import (
    RestTransport,
)
from deephaven_massive._tables.watchlist import (
    SKIPPED_ROWS_FIX,
    SymbolRowRule,
    read_symbols,
    skipped_rows_text,
)
from deephaven_massive.queries._query import Query, _read_symbol_table, _verb, _wait

if TYPE_CHECKING:
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import ValueTable
    from deephaven_massive._enums import OnError

__all__ = ["MINIMUM_POLL_INTERVAL", "FetchQuery"]

logger = logging.getLogger(__name__)

MINIMUM_POLL_INTERVAL: Final = timedelta(seconds=1)
"""The shortest interval `poll()` accepts.

A shorter one is almost always a typo, such as `poll(0.01)` for `poll(10)`, and on a plan that
Massive does not pace it would send requests back to back; a stream serves anything faster.
"""


@raises_through(require_positive_number)
def _interval(value: object) -> timedelta:
    """Validate a `poll()` interval.

    Args:
        value: Seconds as an `int` or `float`, or a `timedelta`.

    Returns:
        The interval as a `timedelta`.

    Raises:
        TypeError: If the value is neither a number nor a `timedelta`.
        ValueError: If the number is not finite and positive, if the interval overflows
            `timedelta`, or if it is shorter than `MINIMUM_POLL_INTERVAL`.
    """
    if isinstance(value, timedelta):
        period = value
    else:
        seconds = require_positive_number("interval", value)
        try:
            period = timedelta(seconds=seconds)
        except OverflowError:  # beyond timedelta's range
            raise ValueError(
                f"interval={value!r} is longer than a timedelta can hold. Pass a number of "
                "seconds, such as 60."
            ) from None
    if period < MINIMUM_POLL_INTERVAL:
        raise ValueError(
            f"interval={value!r} is below the least "
            f"{MINIMUM_POLL_INTERVAL.total_seconds():g} s. Pass 1 or more."
        )
    return period


def _newest_first(core: SessionCore, spec: DatasetSpec) -> str:
    """Return the arguments that order a feed's rows newest first, as a user types them.

    A feed whose paths take `order` takes it alone; any other sends the direction as a suffix
    of `sort`, so it names its time axis's field there too.

    Args:
        core: The session core, whose catalog says whether the feed takes `order`.
        spec: The feed, which declares a `time_axis`.

    Returns:
        Such as `"order=dm.Order.DESC"` or `"sort='published', order=dm.Order.DESC"`.
    """
    if core.catalog.takes_param(spec, "order"):
        return "order=dm.Order.DESC"
    axis = required(spec.time_axis, f"{spec.name}'s time axis")
    return f"sort={param_base(axis[0])!r}, order=dm.Order.DESC"


def _read_value_table(
    core: SessionCore,
    spec: DatasetSpec,
    values: ValueTable,
    *,
    query: str,
    noncanonical: OnError,
    live: bool = False,
    call: int | None = None,
) -> tuple[str, ...]:
    """Read a table passed where the dataset takes one value, once, as `_read_symbol_table` does.

    A row that is not a value the dataset takes (an underlying that is not a stock or index
    ticker) follows the call's `noncanonical_symbols` (`SymbolRowRule.for_values`).

    Args:
        core: The session core, for its execution context.
        spec: The dataset.
        values: The boxed table and the argument it stands for.
        query: The `repr` of the query whose verb reads it, which the messages name.
        noncanonical: The call's `noncanonical_symbols` (`Request.on_error`).
        live: True for a verb that follows the table or re-reads it, which may start on a
            table with no row yet.
        call: The id of the static verb's call whose values these are: each skipped value is
            listed in `m.problems()` as a problem of the call, with the call's query, before
            any error below; None
            for a live verb's read, whose table lists them.

    Returns:
        The table's values, sorted and de-duplicated; null and skipped rows are left out.

    Raises:
        ValueError: If the table has rows and none of them is a value the dataset takes, or,
            for a static read, if it holds no value at all; under `RAISE`, the `ValueError` of
            `read_symbols` for a bad row.
    """
    rule = SymbolRowRule.for_values(spec, values, noncanonical)
    with entered(core.exec_ctx):
        rows = read_symbols(rule, values.table, stream_id=None, reported=live or call is not None)
    if call is not None:
        where = f"the {values.argument} table of {query}"
        events = rule.skipped_events(rows.skipped, stream_id=call, where=where, field=rule.column)
        refused = refused_values_of(
            values.table, rule.column
        )  # a table `canonicalize_symbols` returned
        if refused is not None:
            events += refused.events(
                dataset=spec.name, stream_id=call, where=where, field=rule.column
            )
        if events:
            # Named first: a call that raises below never reaches the read that names it.
            core.diagnostics.name_read(call, query)
            core.problems(events)
    if rows.skipped and not rows.symbols:
        raise ValueError(
            user_defect(
                f"{query}: No row of the {values.argument} table is a value this dataset takes, "
                "so the result would read nothing. The table holds "
                f"{skipped_rows_text(rows.skipped)}",
                SKIPPED_ROWS_FIX,
            )
        )
    if not rows.symbols and not live:
        raise ValueError(
            user_defect(
                f"{query}: The {values.argument} table holds no value",
                f"Pass a table whose {rule.column} column holds at least one value",
            )
        )
    return rows.symbols


def _symbols_per_read(spec: DatasetSpec) -> int:
    """Return how many symbols one read of a followed `fetch()` takes.

    A dataset whose list parameter (`ticker.any_of`) names many symbols in one request reads
    up to `DatasetSpec.symbols_per_request` symbols at once, as a static `fetch()` groups
    them, and a `limit` is sent once on that request, as Massive's cap on its rows. Every
    other dataset reads one symbol a read.

    Args:
        spec: The dataset.

    Returns:
        The most symbols one read takes, at least 1.
    """
    return spec.symbols_per_request or 1


class FetchQuery(Query):
    """A query of a REST dataset: `fetch()` reads what Massive has now, `poll()` keeps reading it.

    Many of Massive's datasets answer with what it holds at the moment you ask: a stock's
    dividends, each symbol's latest trade, a market snapshot, a list of tickers. Their dataset
    methods return a `FetchQuery`. Call `fetch()` for a table of the rows Massive has now, or
    `poll(interval)` for a live table that reads them again every `interval` seconds. Each call
    reads again, so you can reuse the query.

    Import it as `from deephaven_massive.queries import FetchQuery` to annotate your own code.
    You get one from a dataset method, such as `m.stocks.dividends(...)`, and never construct
    one.

    Example:
        ::

            dividends = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
            latest = m.stocks.snapshots(["AAPL", "MSFT"]).poll(5)
    """

    __slots__ = ()

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        _read_value_table,
        RestTransport.fetch,
        RestTransport.check_fetch,
        RestTransport.fetch_by_symbol,
        RestTransport.fetch_by_value,
        RestTransport.fetch_values,
        open_followed_read,
        _wait,
    )
    def fetch(self, *, wait: bool = False) -> Table:
        """Return a table of every row Massive has now, up to the query's `limit`.

        By default the call returns a live table with no rows at once, and the rows arrive in the
        background. Deephaven updates the table, and every table built on it, as they land. For
        symbols passed in code or in a static table, they land all in one update, and
        `m.streams()` shows the read `LOADING`, then `DONE` or `FAILED`. Pass `wait=True` to wait
        for the rows; without a watchlist, you then get a static table.

        A static table of symbols is read once, when you call. A watchlist is followed: a symbol you
        add is read, and one you delete leaves. One symbol's failure leaves the watchlist's other
        symbols in place, unless its cause is set to `dm.OnError.RAISE`, which fails the table.
        `m.symbol_reads()` shows each symbol's read.

        Args:
            wait: False, the default, returns at once and fills the table in the background. True
                waits for the rows; for a watchlist, until each symbol has been read once.

        Returns:
            A table with the dataset's columns: live, or static for `wait=True` without a
            watchlist.

        Raises:
            TypeError: If `wait` isn't a `bool`.
            ValueError: At the call, before any request, if:
                - it runs on a Deephaven update-graph thread;
                - the endpoint doesn't take one of the query's parameters or filters, or, for a
                  watchlist, one of the endpoints its symbols could need doesn't;
                - a static `symbols`, `underlying=`, or `product_code=` table is empty;
                - no row of a table of symbols is Massive's exact ticker.
            AuthenticationError: With `wait=True`, if Massive rejects your API key.
            EntitlementError: With `wait=True`, if your plan doesn't include the data, or Massive
                delays current data whose subscription isn't in
                `MassiveConfig.delayed_subscriptions`. The message names the setting to change.
            SessionClosedError: If the session is closed.
            MassiveError: With `wait=True`, if:
                - a rate limit outlasts the retries;
                - a request fails in any other way;
                - Massive has no data for a symbol, under `unavailable_symbols=dm.OnError.RAISE`;
                - for a watchlist, every symbol's read failed.

        Without `wait=True`, an error from Massive fails the table instead of raising: it is
        logged and shows in `Error` of `m.streams()`. A watchlist's table fails when every symbol
        it held at the call fails for good, such as on a plan without the dataset.

        Example:
            ::

                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch()
                now = m.stocks.dividends(["AAPL", "MSFT"]).fetch(wait=True)
        """
        background = not _wait(wait)
        return self._logged_call(
            "fetch()" if background else "fetch(wait=True)",
            functools.partial(self._fetch, wait=wait),
            background=background,
        )

    def _fetch(self, call: int, *, wait: bool) -> Table:
        """Run `fetch()` as call `call`.

        A refreshing symbol table is followed (`_followed_fetch`). A static one is read once.

        Args:
            call: The call id.
            wait: As `fetch` takes it.

        Returns:
            The static table, or the refreshing one a background read fills, or the one that
            follows a refreshing symbol table.
        """
        spec = self._spec
        symbols = self._request.symbols
        if self._request.value_table is not None:
            return self._value_fetch(call, self._request.value_table, wait=wait)
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            return self._followed_fetch(call, symbols, wait=wait)
        request = self._static_request(call)
        rest = self._core.rest_transport()
        if not wait:  # a parameter error raises now
            rest.check_fetch(spec, request)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=rest_recency_note(spec, self._core.config.delayed_subscriptions),
            run=lambda stop: rest.fetch(spec, request, call=call, stop=stop),
        )

    def _followed_fetch(self, call: int, symbols: SymbolTable, *, wait: bool) -> Table:
        """Run a `fetch()` call that follows a refreshing symbol table.

        The table's current rows are checked now, as a live verb checks them, and the request's
        parameters against every path a symbol of the table could reach, so a parameter one of
        them lacks raises `ValueError` now, as for a list. Every symbol, at the call or added
        later, is read with the requests `fetch()` makes for it
        (`RestTransport.fetch_by_symbol`), as Massive answers at that read, and is read again
        whenever it returns to the table. With `wait=True` the call-time symbols are read now,
        as one read; the call raises when every one of them failed, or when one failed for a
        cause set to `RAISE`, and else logs one WARNING when some did.

        Args:
            call: The call id.
            symbols: The boxed refreshing table.
            wait: As `fetch` takes it.

        Returns:
            The refreshing result (`open_followed_read`).
        """
        spec, core = self._spec, self._core
        call_time = _read_symbol_table(
            core,
            spec,
            symbols,
            query=repr(self),
            noncanonical=self._request.on_error.noncanonical_symbols,
            live=True,
        )
        request = replace(self._request, symbols=call_time)
        rest = core.rest_transport()
        # A parameter any path of a symbol the table may hold lacks raises now, as for a list.
        rest.check_fetch(spec, self._request)
        plan = FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=_symbols_per_read(spec),
            read=lambda names, stop: rest.fetch_by_symbol(
                spec, request, symbols=names, call=call, stop=stop
            ),
            policy=request.on_error,
        )
        recency = rest_recency_note(spec, core.config.delayed_subscriptions)
        note = "Follows its symbol table: each symbol's rows are Massive's answer as of its read."
        logger.info("%s fetch: follows its symbol table", spec.name)
        loaded = self._first_read(plan, call_time, verb="fetch") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=symbols.table,
            rule=SymbolRowRule.of(spec, self._request.on_error.noncanonical_symbols),
            reread=Reread.ALWAYS,
            plan=plan,
            note=note if recency is None else f"{note} {recency}",
            first=loaded,
        )

    def _value_fetch(self, call: int, values: ValueTable, *, wait: bool) -> Table:
        """Run a `fetch()` call whose one-value argument is a table (`Request.value_table`).

        Each value is read with the requests `fetch()` makes when it is passed alone
        (`RestTransport.fetch_by_value`). A static table is read once, as a fixed list
        (`RestTransport.fetch_values`): a failed value is listed and left out, as a symbol in code
        is, and the call fails when every value fails, or when one fails for a cause set to
        `RAISE`. A refreshing one is
        followed (`_followed_value_fetch`). A symbol table passed beside it is read once.

        Args:
            call: The call id.
            values: The boxed table of values.
            wait: As `fetch` takes it.

        Returns:
            The static table, or the refreshing one a background read fills, or the one that
            follows a refreshing table of values.
        """
        if values.table.is_refreshing:
            return self._followed_value_fetch(call, values, wait=wait)
        spec, core = self._spec, self._core
        names = _read_value_table(
            core,
            spec,
            values,
            query=repr(self),
            noncanonical=self._request.on_error.noncanonical_symbols,
            call=call,
        )
        request = self._static_request(call)
        rest = core.rest_transport()
        if not wait:  # a parameter error raises now
            rest.check_fetch(spec, request.for_value(names[0]))
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=rest_recency_note(spec, core.config.delayed_subscriptions),
            run=lambda stop: rest.fetch_values(spec, request, values=names, call=call, stop=stop),
        )

    def _followed_value_fetch(self, call: int, values: ValueTable, *, wait: bool) -> Table:
        """Run a `fetch()` call that follows a refreshing table of values.

        As `_followed_fetch` follows a symbol table, keyed by each value: a value added to the
        table is read, as Massive answers then, and lands whole; a value removed leaves the
        result in the same update; a returning value is read again. Each value is one read.

        Args:
            call: The call id.
            values: The boxed refreshing table.
            wait: As `fetch` takes it.

        Returns:
            The refreshing result (`open_followed_read`).

        Raises:
            ValueError: If `symbols` is a refreshing table too: one result follows one table.
        """
        spec, core = self._spec, self._core
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            logger.debug("%r fetch() refused: two refreshing tables to follow", self)
            raise ValueError(
                user_defect(
                    f"{self!r}.fetch() follows one refreshing table, and both symbols and "
                    f"{values.argument} are refreshing tables",
                    "Call poll(60) instead, which reads both every 60 s, or pass one of them as "
                    f"a snapshot, such as {values.argument}=table.snapshot()",
                )
            )
        call_time = _read_value_table(
            core,
            spec,
            values,
            query=repr(self),
            noncanonical=self._request.on_error.noncanonical_symbols,
            live=True,
        )
        request = self._static_request(call)
        rest = core.rest_transport()
        if call_time:  # a parameter error raises now; with none, each read checks them
            rest.check_fetch(spec, request.for_value(call_time[0]))
        plan = FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=1,
            read=lambda names, stop: rest.fetch_by_value(
                spec, request, values=names, call=call, stop=stop
            ),
            policy=request.on_error,
        )
        recency = rest_recency_note(spec, core.config.delayed_subscriptions)
        note = (
            f"Follows its table of {values.argument} values: each value's rows are Massive's "
            "answer as of its read."
        )
        logger.info("%s fetch: follows its table of %s values", spec.name, values.argument)
        loaded = self._first_read(plan, call_time, verb="fetch") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=values.table,
            rule=SymbolRowRule.for_values(
                spec, values, self._request.on_error.noncanonical_symbols
            ),
            reread=Reread.ALWAYS,
            plan=plan,
            note=note if recency is None else f"{note} {recency}",
            first=loaded,
            values_column=values.column,
        )

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _interval,
        _require_enum,
        read_symbols,
        _read_value_table,
        RestTransport.poll,
    )
    def poll(self, interval: float | timedelta, *, mode: PollMode = PollMode.AUTO) -> Table:
        """Return a live table that fetches the query again every `interval`.

        Each poll reads the whole query again, so the table follows a watchlist. The table keeps
        polling while anything uses it
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#stop-live-tables).

        A feed, a dataset whose `poll()` appends new rows (news, filings, unusual trades), needs
        a `start` or a `limit`. Without either, each poll would read Massive's whole archive.

        A failed poll logs a WARNING and shows in `m.problems()`, and the next poll runs as
        usual. The table fails and stops polling if its polls keep failing for
        `MassiveConfig.max_outage`, 300 seconds by default. It also fails on a failure no later poll
        can fix, such as a rejected key.

        Each poll reads every symbol on its own, so one symbol's failure never costs another symbol
        its rows. A symbol whose read fails for now is read again at a later poll, and a
        `dm.PollMode.REPLACE` table keeps its last rows meanwhile. A symbol Massive refuses, or that
        keeps failing for `max_outage`, isn't read again until you delete its row from the table of
        symbols and add it back, or call `poll()` again. The table fails when every symbol of its
        first poll has failed that way before any of them answered, or when Massive refuses the feed
        or the API key. A poll skips a symbol it can't read or that Massive has no data for. Each
        such symbol is listed in `m.problems()`. To fail the table instead, set
        `noncanonical_symbols`, `unavailable_symbols`, or `refused_symbols` of `dm.ErrorPolicy` to
        `dm.OnError.RAISE`, through `MassiveConfig(on_error=...)` or the dataset method's
        `on_error=`.

        Args:
            interval: The time between polls, in seconds or as a `timedelta`, at least 1 second. For
                anything faster, use a stream.
            mode: How polls combine. `dm.PollMode.AUTO`, the default, uses the mode the dataset's
                page names. `REPLACE` keeps only the latest poll's rows. `UPSERT` keeps every key
                ever seen, at its latest row. `APPEND` adds only rows it hasn't seen.

        Returns:
            A live table with the dataset's columns: keyed for `REPLACE` and `UPSERT`, append-only
            for `APPEND`. Each row's `TimestampInsert` is the time of the poll that wrote it. The
            table remembers each key or row it has held, so its memory grows with them.

        Raises:
            TypeError: If `interval` isn't a number or a `timedelta`, or `mode` isn't a
                `dm.PollMode`.
            ValueError: At the call, before the table exists, if:
                - it runs on a Deephaven update-graph thread;
                - `interval` is shorter than 1 second or isn't finite;
                - a feed has neither `start` nor `limit`;
                - no row of a table of symbols is Massive's exact ticker;
                - an endpoint doesn't take one of the query's parameters or filters.
            SessionClosedError: If the session is closed.
            MassiveError: If the call meets a bug in the library.
        """
        period = _interval(interval)
        asked = _require_enum(mode, PollMode, "mode")
        chosen = self._spec.poll_mode if asked is PollMode.AUTO else asked
        request = self._request
        if self._spec.is_feed and request.start is None and request.limit is None:
            name = self._spec.name
            newest = _newest_first(self._core, self._spec)
            logger.debug("%r poll() refused: a feed's poll needs a start or a limit", self)
            raise ValueError(
                user_defect(
                    f"{self!r}.poll() needs a start or a limit. Without one, every cycle re-reads "
                    "Massive's whole archive. A start is fixed when the query is built, so the "
                    "window it bounds grows every day the poll runs",
                    f"For the latest rows, pass m.{name}() a limit with {newest}, then call poll()",
                )
            )
        if isinstance(self._request.symbols, SymbolTable):
            _read_symbol_table(
                self._core,
                self._spec,
                self._request.symbols,
                query=repr(self),
                noncanonical=self._request.on_error.noncanonical_symbols,
                live=True,
            )
        if self._request.value_table is not None:
            _read_value_table(
                self._core,
                self._spec,
                self._request.value_table,
                query=repr(self),
                noncanonical=self._request.on_error.noncanonical_symbols,
                live=True,
            )
        logger.debug(
            "%s poll(): every %.3f s, mode %s -> %s",
            shown_query(repr(self)),
            period.total_seconds(),
            asked.name,
            chosen.name,
        )
        return self._core.rest_transport().poll(
            self._spec, self._request, interval=period, mode=chosen, query=repr(self)
        )
