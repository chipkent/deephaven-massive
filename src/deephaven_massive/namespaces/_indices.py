"""The `m.indices` namespace: index values, bars, the previous day's bar, snapshots, and more.

Each dataset is one factory method on `IndicesNamespace` that validates its arguments and returns a
query; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG`
in `_session.py`).

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

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import AssetClass, NotBars, Timespan
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    build_query,
    day_argument,
    list_params,
)
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    StreamHistoryQuery,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.symbols import SymbolsLike
    from deephaven_massive._enums import Order
    from deephaven_massive._error_policy import ErrorPolicy

__all__ = ["DATASETS", "IndicesNamespace"]


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
    serves_current_data=True,
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


class IndicesNamespace(Namespace):
    """The index datasets, reached as `m.indices`.

    Each method returns a query of one dataset, and together they cover index values, bars, each
    index's previous day's bar and daily open and close, and snapshots. Index symbols take the `I:`
    prefix, such as `I:SPX`. A method reads nothing, so it never blocks. You can call it
    from any thread.
    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import IndicesNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            levels = m.indices.values(["I:SPX", "I:NDX"]).last_by()
            daily = m.indices.bars("I:SPX", dm.Timespan.DAY).history("2026-10-01", "2026-10-30")
    """

    __slots__ = ()
    _path: ClassVar[str] = "indices"

    @raises_through(build_query)
    def values(
        self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None
    ) -> StreamHistoryQuery:
        """Index levels, tick by tick, as computed by the index providers.

        Each row holds one index's level at one instant. Massive has no REST history of index
        values, so `history()` reads only the flat files, which need S3 keys. For each index's
        current level, poll `snapshots()`.

        Args:
            symbols: Indices with the `I:` prefix, such as `"I:SPX"`, or `dm.ALL` for every index.
                `history()` downloads each day's whole flat file, with every index in it, however
                few indices you pass. The file for 2026-11-09 was 2.9 GB.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.values.md.

        Raises:
            TypeError: If `symbols` has another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an index;
                - a table has no `Symbol` column.

        Example:
            ::

                live = m.indices.values(["I:SPX", "I:VIX"]).last_by()
        """
        return build_query(
            self._core, VALUES, query_type=StreamHistoryQuery, symbols=symbols, on_error=on_error
        )

    @raises_through(build_query)
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        on_error: ErrorPolicy | None = None,
    ) -> StreamHistoryQuery | HistoryQuery:
        """Open, high, low, and close index levels over fixed windows of time.

        Each row is one bar of one index. Index bars come from index levels, so they have no volume
        and no `adjusted` argument. A window in which the index didn't change has no bar. A bar of a
        day or longer covers Chicago days, as Massive cuts them: it starts at Massive's stamp,
        midnight in Chicago (01:00 New York time), and ends at midnight in Chicago after its last
        day.

        Args:
            symbols: Indices with the `I:` prefix, or `dm.ALL` for every index. `history()` reads
                `dm.ALL` from the minute and day flat files.
            timespan: The bar's unit. Massive refuses `dm.Timespan.SESSION`, a futures unit.
            multiplier: Units per bar.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A `StreamHistoryQuery` for 1-second and 1-minute bars at `multiplier=1`, which
            Massive streams, with the stream methods and `history()`; a `HistoryQuery`, with
            `history()` only, for any other bars. A type checker allows a stream method on the
            result only after `isinstance(q, StreamHistoryQuery)`. Its columns, plans, and
            methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an index;
                - a table has no `Symbol` column.

        Example:
            ::

                daily = m.indices.bars("I:SPX", dm.Timespan.DAY).history("2026-10-26", "2026-10-30")
        """
        return build_query(
            self._core,
            BARS,
            query_type=HistoryQuery,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            on_error=on_error,
        )

    @raises_through(build_query)
    def previous_day_bar(
        self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None
    ) -> FetchQuery:
        """The previous trading day's open, high, low, and close of each index.

        Each row holds one index's bar of the previous complete session. Its volume columns are
        always null. An index with no bar gets no row, and the library logs a WARNING.

        Args:
            symbols: Indices with the `I:` prefix. `dm.ALL` is refused; for every index's bar of one
                day, use `m.indices.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.previous_day_bar.md.

        Raises:
            TypeError: If `symbols` has another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker of an index;
                - a table has no `Symbol` column.

        Example:
            ::

                prev = m.indices.previous_day_bar(["I:SPX", "I:NDX"]).fetch()
        """
        return build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "For every ticker's bar of one day, call m.indices.bars(dm.ALL, dm.Timespan.DAY)"
                ".history('2025-01-02', '2025-01-02')."
            ),
            timespan=Timespan.DAY,
            on_error=on_error,
        )

    @raises_through(build_query, list_params)
    def snapshots(
        self,
        symbols: SymbolsLike,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The current state of each index: its level, its session so far, and its status.

        Each row holds one index's level and its session's open, high, low, close, and change. It
        also holds the market status and the index's name. An index Massive doesn't know, or your
        plan lacks, keeps its row, and its `Error` and `Message` give the reason.

        Args:
            symbols: Indices with the `I:` prefix, or `dm.ALL` for every index.
            sort: `"ticker"`, the one field Massive sorts by.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"ticker_gte": "I:N"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.snapshots.md.

        Raises:
            TypeError: If `symbols` has another type, `sort` isn't a `str`, `order` isn't a
                `dm.Order`, `limit` isn't an `int`, or `filters` isn't a mapping, or `on_error`
                isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an index;
                - a table has no `Symbol` column;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                latest = m.indices.snapshots(["I:SPX", "I:NDX"]).poll(5)
        """
        return build_query(
            self._core,
            SNAPSHOTS,
            query_type=FetchQuery,
            symbols=symbols,
            params=list_params(sort=sort, order=order),
            filters=filters,
            limit=limit,
            on_error=on_error,
        )

    @raises_through(build_query, day_argument)
    def daily_open_close(
        self, symbols: SymbolsLike, date: DateLike, *, on_error: ErrorPolicy | None = None
    ) -> FetchQuery:
        """One day's opening, closing, high, and low levels of each index.

        Each row holds one index's levels for the day, with the window of an index day bar. It also
        holds the pre-market and after-hours levels where Massive has them.

        Args:
            symbols: Indices with the `I:` prefix. `dm.ALL` is refused; for every index's bar of one
                day, use `m.indices.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.
            date: The day, as a date. A `datetime` raises.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.daily_open_close.md.

        Raises:
            TypeError: If `date` isn't a date type, or `symbols` has another type, or `on_error`
                isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker of an index;
                - a table has no `Symbol` column;
                - `date` is a `datetime`, or isn't a date.

        Example:
            ::

                day = m.indices.daily_open_close(["I:SPX", "I:NDX"], "2026-10-30").fetch()
        """
        return build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "For every index's bar of one day, call m.indices.bars(dm.ALL, dm.Timespan.DAY)"
                ".history('2025-01-02', '2025-01-02')."
            ),
            params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            on_error=on_error,
        )
