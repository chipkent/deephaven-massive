"""The `m.indices` namespace: index values, bars, the previous day's bar, snapshots, and more.

Each dataset is one factory method on `_Indices` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).

The five datasets and what serves them:

- `indices.values`: index levels, streamed tick by tick from the WebSocket `V` channel and
  read back from the `us_indices/values_v1` flat files. Massive has no REST history of index
  values, so `history()` reads flat files only; an index's current level is in
  `indices.snapshots`.
- `indices.bars`: OHLC bars from the `A` (1 second) and `AM` (1 minute) channels, the range
  aggregates endpoint, and the minute and day aggregate flat files. Index aggregates are
  computed from index values, so they have no volume columns and are never adjusted: they take
  no `adjusted`.
- `indices.previous_day_bar`: each index's 1-day bar of the previous session, from the
  previous-session aggregates endpoint, in the columns of the bars kind that its endpoint fills.
- `indices.snapshots`: every field of the indices snapshot, one row per index.
- `indices.daily_open_close`: one index's open, close, high, low, pre-market, and after-hours
  values for one day.

Index symbols are Massive's tickers, with the `I:` prefix (`"I:SPX"`); `"SPX"` is refused, and
`dm.canonical_symbol_index` adds the prefix. An index endpoint whose rows carry a ticker without
it gets the prefix from the endpoint's class, never from the name.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import AssetClass, NotBars, Timespan
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    Namespace,
    build_query,
    day_argument,
    list_params,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import (
        HistoryQuery,
        Query,
        StreamHistoryQuery,
    )
    from deephaven_massive._types import DateLike, FilterValue, StreamSpan, Symbols

__all__ = ["DATASETS", "_Indices"]

logger = logging.getLogger(__name__)

_REST, _WS, _FLAT_FILES = Transport.REST, Transport.WS, Transport.FLAT_FILES

_SNAPSHOT: Final = "/v3/snapshot/indices"
_PREVIOUS: Final = "/v2/aggs/ticker/{indicesTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_OPEN_CLOSE: Final = "/v1/open-close/{indicesTicker}/{date}"
_VALUE_FILES: Final = "us_indices/values_v1"
_MINUTE_FILES: Final = "us_indices/minute_aggs_v1"
_DAY_FILES: Final = "us_indices/day_aggs_v1"

_SNAPSHOT_PAGE: Final = 250
"""The indices snapshot's largest `limit`."""

_AGGREGATES_PAGE: Final = 50_000
"""The range endpoint's largest `limit`, which counts the base aggregates behind a page."""

VALUES: Final = DatasetSpec(
    name="indices.values",
    summary="Index levels, tick by tick, as computed by the index providers.",
    asset_class=AssetClass.INDICES,
    kind="values",
    channels={NotBars.ANY: "V"},
    flat_file_prefixes={NotBars.ANY: _VALUE_FILES},
    keys={
        (_WS, "indices/V"): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("Timestamp")),
        (_FLAT_FILES, _VALUE_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("Timestamp")
        ),
    },
)
"""`m.indices.values`: index levels, streamed and in flat files."""

_STREAM_BAR_KEYS: Final = SourceKeys(
    symbol=KeySpec("Ticker"), time=KeySpec("WindowStart"), end=KeySpec("EndTimestamp")
)
"""The keys of a WebSocket bars channel: the index, the window's start, and its end `e`."""

BARS: Final = DatasetSpec(
    name="indices.bars",
    summary="Open, high, low, and close index levels over fixed windows of time.",
    asset_class=AssetClass.INDICES,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        # The range endpoint's bars do not name their index; it is the path's ticker.
        (_REST, _RANGE): SourceKeys(
            symbol=KeySpec("indicesTicker", from_request=True), time=KeySpec("WindowStart")
        ),
        (_WS, "indices/A"): _STREAM_BAR_KEYS,
        (_WS, "indices/AM"): _STREAM_BAR_KEYS,
        (_FLAT_FILES, _MINUTE_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")
        ),
        (_FLAT_FILES, _DAY_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")
        ),
    },
    page_size=_AGGREGATES_PAGE,
)
"""`m.indices.bars`: OHLC bars of every timespan; volume columns null, no `adjusted`."""

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="indices.previous_day_bar",
    summary="The previous trading day's open, high, low, and close of each index.",
    asset_class=AssetClass.INDICES,
    kind="bars",
    fetch_path=_PREVIOUS,
    keys={(_REST, _PREVIOUS): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("WindowStart"))},
    per_symbol=True,
    revision_keys=("TimestampStart",),
)
"""`m.indices.previous_day_bar`: each index's 1-day bar of the previous session, from `/prev`."""

SNAPSHOTS: Final = DatasetSpec(
    name="indices.snapshots",
    summary="The current state of each index: its level, its session so far, and its status.",
    asset_class=AssetClass.INDICES,
    fetch_path=_SNAPSHOT,
    keys={(_REST, _SNAPSHOT): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    page_size=_SNAPSHOT_PAGE,
    revision_keys=("LastUpdated",),
)
"""`m.indices.snapshots`: the whole indices snapshot, one row per index."""

DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="indices.daily_open_close",
    summary="One day's opening, closing, high, and low levels of each index.",
    asset_class=AssetClass.INDICES,
    fetch_path=_OPEN_CLOSE,
    keys={(_REST, _OPEN_CLOSE): SourceKeys(symbol=KeySpec("Symbol"), date=KeySpec("From"))},
    per_symbol=True,
    row_times=RowTimes.DAY,
)
"""`m.indices.daily_open_close`: one day's open, close, high, low, and extended-hours values."""

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    VALUES,
    BARS,
    PREVIOUS_DAY_BAR,
    SNAPSHOTS,
    DAILY_OPEN_CLOSE,
)
"""Every dataset of `m.indices`, one `DatasetSpec` each, in the order the reference lists them."""


class _Indices(Namespace):
    """`m.indices`: the index datasets of a session.

    Created once by `MassiveSession` and reached as `m.indices`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            spx = m.indices.values(["I:SPX", "I:NDX"]).last_by()  # live index levels
            daily = m.indices.bars("I:SPX", dm.Timespan.DAY).history("2025-06-02", "2025-06-30")
    """

    __slots__ = ()
    _path: ClassVar[str] = "indices"

    @raises_through(build_query)
    def values(self, symbols: Symbols) -> StreamHistoryQuery:
        """Index levels, tick by tick, as computed by the index providers.

        Each row is one index level at one instant: `Symbol`, `Timestamp`, and `Value`, with its
        trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of `Timestamp`), and
        `TimestampInsert`, when the library received it. The stream verbs subscribe to the
        WebSocket `V` channel; each index's current level is in `m.indices.snapshots(symbols)`,
        and its previous session in `m.indices.previous_day_bar(symbols)`. `history()` reads the
        `us_indices/values_v1` flat files, the only history Massive has for index values: it
        needs S3 credentials, and `source=dm.HistorySource.REST` raises `ValueError`. There is
        no `limit`; every verb returns every row it is asked for.

        `dm.ALL` is accepted by the stream verbs (every index on the stream) and by
        `history()` (every index in the files). The values stream is
        served from the indices stream host the session resolved for your plan. If that host
        refuses the `V` topic, the stream fails with `EntitlementError`, whose message names
        the host, `MassiveConfig.ws_hosts[AssetClass.INDICES]`, and `business.massive.com`,
        the host Massive documents for index values.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#values,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `"I:SPX"` or `["I:SPX", "I:NDX"]` (`"SPX"` is refused); a
                Deephaven table with a `Symbol` column, which the stream follows as rows are added
                and removed; or `dm.ALL`.

        Returns:
            A query with the stream verbs and `history()`. Nothing is requested until a verb
            is called.

        Raises:
            TypeError: For `symbols` of any other type than a `str`, a sequence of `str`, a table,
                or `dm.ALL`.
            ValueError: For a symbol that is not a valid index symbol (one with another asset
                class's prefix, for example), an empty symbol list, or a table without a `Symbol`
                column. Also for `symbols` of None, with a message that names the forms it takes.

        Example:
            ::

                q = m.indices.values(["I:SPX", "I:VIX"])
                live = q.last_by()  # one row per index, updated as values arrive
                june = q.history("2025-06-02", "2025-06-30")  # from flat files
        """
        return cast("StreamHistoryQuery", build_query(self._core, VALUES, symbols=symbols))

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan = Timespan.MINUTE,
        *,
        multiplier: Literal[1] = 1,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self, symbols: Symbols, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery: ...
    @raises_through(build_query)
    def bars(
        self, symbols: Symbols, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery:
        """Open, high, low, and close index levels over fixed windows of time.

        Each row is one bar of one index: `Symbol`, its trading day `Date` (ISO `yyyy-MM-dd`
        text, New York time), its window from `TimestampStart` to the exclusive `TimestampEnd`,
        its levels, and `TimestampInsert`, when the library received it. Index bars are
        computed from index values, so they are never adjusted and have no `adjusted`
        parameter (Massive's endpoint has none), and they have none of the bar columns that
        only traded instruments fill, such as `Volume`, `Vwap`, and `Transactions`. A
        window in which the index did not change produces no bar.

        The timespan and multiplier decide the transports, and with them the query class:

        - 1-second and 1-minute bars (`multiplier=1`) stream from the WebSocket `A` and `AM`
          channels and have `history()`: a `StreamHistoryQuery`.
        - Every other timespan and multiplier has `history()` only: a `HistoryQuery`. Each
          index's bar of the previous completed session is `previous_day_bar()`.

        `history()` reads the range aggregates endpoint
        (`/v2/aggs/ticker/{indicesTicker}/range/...`, one request series per index) or, for
        1-minute and 1-day bars, the `us_indices/minute_aggs_v1` and `us_indices/day_aggs_v1`
        flat files. There is no `limit`; the library pages internally. `history()` returns
        only the bars whose whole window lies inside the range and has ended, judged by
        `TimestampEnd`: the current day's bar never appears, a day bar only from 01:00 New York
        time the next day, and a week bar only from a range that reaches its Saturday.

        `TimestampEnd` is the first instant at which the bar is complete, so join on it (`aj(t,
        bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only closed bars. An intraday
        bar ends its length after its start. A bar of a day or longer starts at 00:00 New York time
        of its window's first day, whatever the source: the range and the day files stamp a 1-day
        bar at 01:00 New York time (midnight in Chicago), which is moved to the day's start. It ends
        an hour after the midnight that follows its window's last day, at 01:00 New York time, since
        whether an index day bar covers the New York or the Chicago day is not verified. Since an
        index can update around the clock (`I:AAVE100`), a week or a longer period covers its whole
        calendar period, weekends included: it starts at 00:00 of its first calendar day (a week's
        Sunday) and ends an hour after the midnight that follows its last. A bar of several days
        counts trading days, ending as late as holidays could push it (one weekday more, and one
        more for every five further days), since there is no holiday calendar. A longer bar must be
        stamped at 00:00 or 01:00 on the day its period starts: a week on its Sunday, where Massive
        starts it, a month, quarter, or year on its 1st. Any other stamp fails the call with
        `MassiveError` naming the source, since the bar may cover other days than its label.
        `Date` is null for a bar that spans more than one date.

        `dm.ALL` is accepted by the stream verbs and by `history()` from flat files (1-minute
        and 1-day bars). `history()` over REST takes one index per request, so `dm.ALL`
        raises `ValueError` there; `m.indices.snapshots(dm.ALL).fetch()` gives every index's
        current session.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#bars,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `"I:SPX"` or `["I:SPX", "I:NDX"]` (`"SPX"` is refused); a
                Deephaven table with a `Symbol` column; or `dm.ALL`.
            timespan: The unit of the bar window, a `dm.Timespan`; `MINUTE` by default.
                `Timespan.SESSION` is for futures only.
            multiplier: The number of `timespan` units in one bar, a positive `int`; 1 by
                default. `multiplier=5` with `Timespan.MINUTE` is 5-minute bars.

        Returns:
            A `StreamHistoryQuery` or `HistoryQuery`, as above. Nothing is requested until a
            verb is called.

        Raises:
            TypeError: For a `timespan` that is not a `dm.Timespan` (`dm.Timespan("minute")`
                converts text), a `multiplier` that is not an `int`, or `symbols` of an unsupported
                type.
            ValueError: For `Timespan.SESSION`, a `multiplier` that is not positive, a symbol that
                is not a valid index symbol, an empty symbol list, or a table without a `Symbol`
                column. Also for `symbols` of None, with a message that names the forms it takes.

        Example:
            ::

                live = m.indices.bars(["I:SPX"]).append_only()  # 1-minute bars as they close
                hourly = m.indices.bars("I:SPX", dm.Timespan.HOUR).history(
                    "2025-06-02", "2025-06-06"
                )
        """
        return cast(
            "HistoryQuery",
            build_query(
                self._core, BARS, symbols=symbols, timespan=timespan, multiplier=multiplier
            ),
        )

    @raises_through(build_query)
    def previous_day_bar(self, symbols: Symbols) -> Query:
        """The previous trading day's open, high, low, and close of each index.

        One row per index: its 1-day bar of the previous completed session, which Massive documents
        as "the prior session's open, high, low, close", in the columns of its kind that its
        endpoint fills: `Symbol`, its trading day `Date` (ISO `yyyy-MM-dd` text, New York time), its
        window from `TimestampStart`, 00:00 New York time of the day, to the exclusive
        `TimestampEnd`, 01:00 New York time the next day, its levels, and `TimestampInsert`, when
        the library received it; the volume columns are always null, and there is no `adjusted`
        argument. Served by REST (`/v2/aggs/ticker/{indicesTicker}/prev`) through `fetch()` and
        `poll()`, one request per index; `poll()` replaces the table each cycle, keyed by `Symbol`,
        and an explicit `mode=dm.PollMode.APPEND` appends each new day's bar. Massive stamps the bar
        at the 16:00 session close (13:00 on a short trading day), which is moved to the day's
        start; any other stamp raises `MassiveError` naming `bars(symbols,
        dm.Timespan.DAY).history()` for that day. An index with no bar (HTTP 404) is left out, with
        a WARNING.

        `fetch()` and `poll()` take no `limit`: each index gives one row. `dm.ALL` is refused,
        since the endpoint takes one index per request: use
        `m.indices.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`, which reads every index's
        bar of a day from the flat files.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#previous_day_bar,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `"I:SPX"` or `["I:SPX", "I:NDX"]` (`"SPX"` is refused),
                or a Deephaven table with a `Symbol` column, re-read each `poll()` cycle.

        Returns:
            A query with `fetch()` and `poll()`. Nothing is requested until a verb is called.

        Raises:
            TypeError: For `symbols` of any other type than a `str`, a sequence of `str`, or a
                table.
            ValueError: For `dm.ALL`, a symbol that is not a valid index symbol, an empty symbol
                list, or a table without a `Symbol` column. Also for `symbols` of None, with a
                message that names the forms it takes.

        Example:
            ::

                prev = m.indices.previous_day_bar(["I:SPX", "I:NDX"]).fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "use m.indices.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
        )
        return cast("Query", query)

    @raises_through(build_query, list_params)
    def snapshots(
        self,
        symbols: Symbols,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The current state of each index: its level, its session so far, and its status.

        One row per index from `/v3/snapshot/indices`, with every field of the snapshot:
        `Value`, the session's open, high, low, close, change, and previous close,
        `LastUpdated`, the market status, and the index name. An index Massive does not know,
        or that the plan does not include, gives a row whose `Error` and `Message` say why.
        REST only: `fetch()` returns the snapshot once, and `poll(interval)` re-reads it every
        interval, into a table keyed by `Symbol`. Each request names at most 250 indices, so a
        list of 250 or fewer takes one request a cycle.
        The snapshot needs an Indices Starter plan or above; on Indices Basic it raises
        `EntitlementError`, and `m.indices.previous_day_bar(symbols)` (the
        previous session's bar) and `m.indices.daily_open_close()` are the REST alternatives.

        `dm.ALL` is every index Massive publishes, paged 250 at a time. `limit` caps the
        total number of rows across pages; the page size is internal.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#snapshots,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `["I:SPX", "I:NDX"]` (`"SPX"` is refused);
                a Deephaven table with a `Symbol` column, re-read on every `poll()` cycle; or
                `dm.ALL`.
            sort: The field to sort by; Massive accepts `"ticker"`. None keeps Massive's
                default.
            order: The sort direction, a `dm.Order`; None keeps Massive's default.
            limit: The most rows to return in total, a positive `int`; None returns every row.
            filters: Further parameters of the snapshot by key, with each dot of the wire
                name written as an underscore: `ticker_gte`, `ticker_gt`, `ticker_lte`, and
                `ticker_lt` select an alphabetical range of tickers. None sends none.

        Returns:
            A `Query` with `fetch()` and `poll()`. Nothing is requested until a verb is called.

        Raises:
            TypeError: For a `sort` that is not a `str`, an `order` that is not a `dm.Order`, a
                `limit` that is not an `int`, a `filters` that is not a mapping, or `symbols` of an
                unsupported type.
            ValueError: For a `limit` that is not positive, a filter key that is not a parameter of
                the snapshot or a value of the wrong kind (the message lists the valid keys), a
                symbol that is not a valid index symbol, an empty symbol list, or a table without a
                `Symbol` column. Also for `symbols` of None, with a message that names the forms it
                takes.

        Example:
            ::

                now = m.indices.snapshots(["I:SPX", "I:NDX", "I:VIX"]).fetch()
                board = m.indices.snapshots(["I:SPX", "I:NDX"]).poll(5)  # every 5 seconds
        """
        return cast(
            "Query",
            build_query(
                self._core,
                SNAPSHOTS,
                symbols=symbols,
                params=list_params(sort=sort, order=order),
                filters=filters,
                limit=limit,
            ),
        )

    @raises_through(build_query, day_argument)
    def daily_open_close(self, symbols: Symbols, date: DateLike) -> Query:
        """One day's opening, closing, high, and low levels of each index.

        One row per index for the day `date`, from `/v1/open-close/{indicesTicker}/{date}`:
        `Symbol`, `Date` (the day, ISO `yyyy-MM-dd` text), the day's window as a 1-day bar's
        (`TimestampStart` at 00:00 New York time, `TimestampEnd` at 01:00 the next day, as for
        index day bars), `Open`, `High`, `Low`, `Close`, and, where Massive has them,
        `PreMarket` and `AfterHours`. REST only, one request per index: `fetch()`
        returns the day once and `poll(interval)` re-reads it. There is no `limit`: each index
        gives one row.

        `dm.ALL` raises `ValueError`, because the endpoint takes one index per request; use
        `m.indices.snapshots(dm.ALL)` for every index's current session, or
        `m.indices.bars(dm.ALL, dm.Timespan.DAY).history(date, date)` for every index's daily
        bar from flat files.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#daily_open_close,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `["I:SPX", "I:NDX"]` (`"SPX"` is refused), or
                a Deephaven table with a `Symbol` column, re-read on every `poll()` cycle.
            date: The day, as an ISO-8601 date (`"2025-06-02"`), a `date`, Deephaven date text
                such as `"2025-3-3"`, a Java `LocalDate`, or a day-precision `numpy.datetime64`.
                It is a calendar date, so a `datetime` or an instant raises `ValueError`.

        Returns:
            A `Query` with `fetch()` and `poll()`. Nothing is requested until a verb is called.

        Raises:
            TypeError: For a `date` of none of the `DateLike` types, or `symbols` of an
                unsupported type.
            ValueError: For `dm.ALL`, a `datetime` or other time as `date`, text that is neither
                an ISO-8601 date nor Deephaven date text, a symbol that is not a valid index
                symbol, an empty symbol list, or a table without a `Symbol` column. Also for
                `symbols` of None, with a message that names the forms it takes.

        Example:
            ::

                day = m.indices.daily_open_close(["I:SPX", "I:NDX"], "2025-06-02").fetch()
        """
        return cast(
            "Query",
            build_query(
                self._core,
                DAILY_OPEN_CLOSE,
                symbols=symbols,
                all_alternative="use m.indices.snapshots(dm.ALL)",
                params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
