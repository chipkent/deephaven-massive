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

_AGGREGATES_PAGE: Final = 50_000
"""The aggregates range endpoint's largest `limit`, which counts the base aggregates
behind a page. Its specification states this maximum only in prose, so it is typed here."""

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

        Each row is one index level at one instant. Massive has no REST history of index values, so
        `history()` reads the flat files only (it needs S3 credentials;
        `source=dm.HistorySource.REST` raises `ValueError`); an index's current level is in
        `snapshots(symbols)`. If the session's indices stream host refuses the `V` topic, the stream
        fails with `EntitlementError` naming the host, `MassiveConfig.ws_hosts[AssetClass.INDICES]`,
        and `business.massive.com`, the host Massive documents for index values.

        Args:
            symbols: Indices with the `I:` prefix, such as `"I:SPX"` (`"SPX"` is refused), or
                `dm.ALL`. See README.md#symbols.

        Returns:
            A query with the stream verbs and `history()`. Columns: docs/datasets/indices.values.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If a symbol is not an index symbol, `symbols` is None or empty, or a table
                has no `Symbol` column.

        Example:
            ::

                live = m.indices.values(["I:SPX", "I:VIX"]).last_by()
        """
        return cast("StreamHistoryQuery", build_query(self._core, VALUES, symbols=symbols))

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan,
        *,
        multiplier: Literal[1] = 1,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self, symbols: Symbols, timespan: Timespan, *, multiplier: int = 1
    ) -> HistoryQuery: ...
    @raises_through(build_query)
    def bars(self, symbols: Symbols, timespan: Timespan, *, multiplier: int = 1) -> HistoryQuery:
        """Open, high, low, and close index levels over fixed windows of time.

        Each row is one bar of one index. Index bars are computed from index levels, so they have no
        volume columns and no `adjusted` argument, and a window in which the index did not change
        has no bar. One-second and one-minute bars also stream; every other length is history only.
        Bar rules follow README.md#bars, with two index differences: a bar of a day or longer ends
        an hour after the midnight that follows its last day (01:00 New York time), and a week or
        longer covers its whole calendar period, weekends included. REST `history()` takes explicit
        indices; `dm.ALL` reads the minute and day flat files.

        Args:
            symbols: Indices with the `I:` prefix, or `dm.ALL`. See README.md#symbols.
            timespan: The bar unit; required. See README.md#timespan-and-multiplier.
            multiplier: Units per bar; only 1 streams.

        Returns:
            A query of the class the timespan picks. Columns: docs/datasets/indices.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, a symbol is not an
                index symbol, `symbols` is None or empty, or a table has no `Symbol` column.

        Example:
            ::

                daily = m.indices.bars("I:SPX", dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
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

        One row per index: its 1-day bar of the previous completed session, one request per index.
        The volume columns are always null; the bar ends at 01:00 New York time the next day, as
        index day bars do (README.md#bars). An index with no bar (HTTP 404) is left out, with a
        WARNING. `dm.ALL` is refused: `bars(dm.ALL, dm.Timespan.DAY).history(day, day)` reads every
        index's bar of a day from the flat files.

        Args:
            symbols: Indices with the `I:` prefix. See README.md#symbols.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/indices.previous_day_bar.md.

        Raises:
            TypeError: If `symbols` is of another type.
            ValueError: If `symbols` is `dm.ALL`, None, or empty, a symbol is not an index symbol,
                or a table has no `Symbol` column.

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

        One row per index: its level, the session's open, high, low, close, and change, the market
        status, and the index name, at most 250 indices a request. An index Massive does not know,
        or that the plan lacks, gives a row whose `Error` and `Message` say why. It needs Indices
        Starter or above; on Indices Basic it raises `EntitlementError`, and `previous_day_bar()`
        and `daily_open_close()` are the alternatives.

        Args:
            symbols: Indices with the `I:` prefix, or `dm.ALL`. See README.md#symbols.
            sort: `"ticker"`. See README.md#sort-order-limit.
            order: See README.md#sort-order-limit.
            limit: See README.md#sort-order-limit.
            filters: The ticker range forms `ticker_gt`, `ticker_gte`, `ticker_lt`, and
                `ticker_lte`. See README.md#filters.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/indices.snapshots.md.

        Raises:
            TypeError: If `symbols` is of another type, `sort` is not a `str`, `order` is not a
                `dm.Order`, `limit` is not an `int`, or `filters` is not a mapping.
            ValueError: If `limit` is not positive, a filter key or value does not fit the endpoint,
                a symbol is not an index symbol, `symbols` is None or empty, or a table has no
                `Symbol` column.

        Example:
            ::

                board = m.indices.snapshots(["I:SPX", "I:NDX"]).poll(5)
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

        One row per index for the day, with the day's window as an index day bar's and, where
        Massive has them, the pre-market and after-hours levels, one request per index. `dm.ALL` is
        refused: `snapshots(dm.ALL)` gives every index's current session, and `bars(dm.ALL,
        dm.Timespan.DAY).history(date, date)` every index's day bar.

        Args:
            symbols: Indices with the `I:` prefix. See README.md#symbols.
            date: The day, a calendar date (a `datetime` raises). See README.md#start-and-end.

        Returns:
            A query with `fetch()` and `poll()`. Columns: docs/datasets/indices.daily_open_close.md.

        Raises:
            TypeError: If `date` is not a date type, or `symbols` is of another type.
            ValueError: If `symbols` is `dm.ALL`, None, or empty, `date` is a `datetime` or not a
                date, a symbol is not an index symbol, or a table has no `Symbol` column.

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
