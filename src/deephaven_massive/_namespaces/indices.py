"""The `m.indices` namespace: index values, bars, snapshots, and daily open/close.

Each dataset is one factory method on `_Indices` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).

The four datasets and what serves them:

- `indices.values`: index levels, streamed tick by tick from the WebSocket `V` channel,
  fetched from the indices snapshot (its `ticker`, `value`, and `last_updated` fields, the
  snapshot's membership of the values kind), and read back from the `us_indices/values_v1` flat
  files. Massive has no REST history of index values, so `history()` reads flat files only.
- `indices.bars`: OHLC bars from the `A` (1 second) and `AM` (1 minute) channels, the
  previous-session and range aggregates endpoints, and the minute and day aggregate flat files.
  Index aggregates are computed from index values, so they have no volume and no `adjusted`.
- `indices.snapshots`: every field of the indices snapshot, one row per index.
- `indices.daily_open_close`: one index's open, close, high, low, pre-market, and after-hours
  values for one day.

Index symbols carry the `I:` prefix; a symbol without it gets one (`"SPX"` is `"I:SPX"`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import calendar_day
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import Namespace, build_query, list_params

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import Order
    from deephaven_massive._query import (
        HistoryLatestQuery,
        HistoryQuery,
        Query,
        StreamHistoryLatestQuery,
        StreamHistoryQuery,
    )
    from deephaven_massive._types import DateLike, DaySpan, FilterValue, StreamSpan, Symbols

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
"""The indices snapshot's largest `limit`, so a long symbol list or `dm.ALL` takes few pages."""

_AGGREGATES_PAGE: Final = 50_000
"""The range endpoint's largest `limit`, which counts the base aggregates behind a page."""

_REFERENCE: Final = (
    "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md"
)

VALUES: Final = DatasetSpec(
    name="indices.values",
    summary="Index levels, tick by tick, as computed by the index providers.",
    asset_class=AssetClass.INDICES,
    kind="values",
    fetch_path=_SNAPSHOT,
    channels={None: "V"},
    flat_file_prefixes={None: _VALUE_FILES},
    keys={
        # The snapshot joins the values kind through a projection that keeps `ticker`,
        # `value`, and `last_updated`, the last under the name `Timestamp`.
        (_REST, _SNAPSHOT): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("Timestamp")),
        (_WS, "indices/V"): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("Timestamp")),
        (_FLAT_FILES, _VALUE_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("Timestamp")
        ),
    },
    symbol_param="ticker.any_of",
    page_size=_SNAPSHOT_PAGE,
)
"""`m.indices.values`: index levels, streamed, fetched from the snapshot, and in flat files."""

BARS: Final = DatasetSpec(
    name="indices.bars",
    summary="Open, high, low, and close index levels over fixed windows of time.",
    asset_class=AssetClass.INDICES,
    is_bars=True,
    kind="bars",
    fetch_path=_PREVIOUS,
    history_path=_RANGE,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_REST, _PREVIOUS): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")),
        # The range endpoint's bars do not name their index; it is the path's ticker.
        (_REST, _RANGE): SourceKeys(
            symbol=KeySpec("indicesTicker", from_request=True), time=KeySpec("WindowStart")
        ),
        (_WS, "indices/A"): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")),
        (_WS, "indices/AM"): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")),
        (_FLAT_FILES, _MINUTE_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")
        ),
        (_FLAT_FILES, _DAY_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("WindowStart")
        ),
    },
    per_symbol=True,
    page_size=_AGGREGATES_PAGE,
)
"""`m.indices.bars`: OHLC bars of every timespan; no volume and no `adjusted`."""

SNAPSHOTS: Final = DatasetSpec(
    name="indices.snapshots",
    summary="The current state of each index: its level, its session so far, and its status.",
    asset_class=AssetClass.INDICES,
    fetch_path=_SNAPSHOT,
    keys={(_REST, _SNAPSHOT): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="ticker.any_of",
    page_size=_SNAPSHOT_PAGE,
)
"""`m.indices.snapshots`: the whole indices snapshot, one row per index."""

DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="indices.daily_open_close",
    summary="One day's opening, closing, high, and low levels of each index.",
    asset_class=AssetClass.INDICES,
    fetch_path=_OPEN_CLOSE,
    keys={(_REST, _OPEN_CLOSE): SourceKeys(symbol=KeySpec("Symbol"))},
    per_symbol=True,
)
"""`m.indices.daily_open_close`: one day's open, close, high, low, and extended-hours values."""

DATASETS: Final[tuple[DatasetSpec, ...]] = (VALUES, BARS, SNAPSHOTS, DAILY_OPEN_CLOSE)
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

    def values(self, symbols: Symbols) -> StreamHistoryLatestQuery:
        """Index levels, tick by tick, as computed by the index providers.

        Each row is one index level at one instant: `Symbol`, `Timestamp`, and `Value`. The
        stream verbs subscribe to the WebSocket `V` channel. `fetch()` reads the current level
        of each index from the indices snapshot (`/v3/snapshot/indices`), whose `last_updated`
        becomes `Timestamp`, so it returns the same columns as the stream; the snapshot takes
        many indices per request, so a list needs few requests. An index
        Massive does not know, or that the plan does not include, is left out of the result
        with a WARNING that names it and Massive's reason. `history()`
        reads the `us_indices/values_v1` flat files, the only history Massive has for index
        values: it needs S3 credentials, and `source=dm.HistorySource.REST` raises
        `ValueError`. There is no `limit`; every verb returns every row it is asked for.

        `dm.ALL` is accepted by the stream verbs (every index on the stream) and by
        `history()` (every index in the files). `fetch()` with `dm.ALL` raises `ValueError`;
        `m.indices.snapshots(dm.ALL).fetch()` returns every index. The values stream is
        served from the indices TICK host (`MassiveConfig.ws_hosts`); a plan without index
        values is refused there, and the stream fails with a message that names that host.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#values,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `"I:SPX"` or `["I:SPX", "I:NDX"]` (`"SPX"` becomes
                `"I:SPX"`); a Deephaven table with a `Symbol` column, which the stream follows
                as rows are added and removed; or `dm.ALL`.

        Returns:
            A query with the stream verbs, `history()`, and `fetch()`. Nothing is requested
            until a verb is called.

        A symbol that is not a valid index symbol (one with another asset class's prefix, for
        example), an empty symbol list, or a table without a `Symbol` column raises
        `ValueError`; `symbols` of any other type than a `str`, a sequence of `str`, a table,
        or `dm.ALL` raises `TypeError`.

        Example:
            ::

                q = m.indices.values(["I:SPX", "I:VIX"])
                live = q.last_by()  # one row per index, updated as values arrive
                june = q.history("2025-06-02", "2025-06-30")  # from flat files
        """
        return cast("StreamHistoryLatestQuery", build_query(self._core, VALUES, symbols=symbols))

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
        self, symbols: Symbols, timespan: DaySpan, *, multiplier: Literal[1] = 1
    ) -> HistoryLatestQuery: ...
    @overload
    def bars(
        self, symbols: Symbols, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery: ...
    def bars(
        self, symbols: Symbols, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery:
        """Open, high, low, and close index levels over fixed windows of time.

        Each row is one bar of one index, keyed by `Symbol` and `Timestamp`, the start of the
        window. Index bars are computed from index values, so they have no volume, no trade
        count, and no `adjusted` parameter (Massive's endpoint has none). A window in which
        the index did not change produces no bar.

        The timespan and multiplier decide the transports, and with them the query class:

        - 1-second and 1-minute bars (`multiplier=1`) stream from the WebSocket `A` and `AM`
          channels and have `history()`: a `StreamHistoryQuery`.
        - 1-day bars have `history()` and `fetch()`, which returns the bar of the previous
          completed session from `/v2/aggs/ticker/{indicesTicker}/prev`: a
          `HistoryLatestQuery`.
        - Every other timespan and multiplier has `history()` only: a `HistoryQuery`.

        `history()` reads the range aggregates endpoint
        (`/v2/aggs/ticker/{indicesTicker}/range/...`, one request series per index) or, for
        1-minute and 1-day bars, the `us_indices/minute_aggs_v1` and `us_indices/day_aggs_v1`
        flat files. There is no `limit`; the library pages internally.

        `dm.ALL` is accepted by the stream verbs and by `history()` from flat files (1-minute
        and 1-day bars). `history()` over REST and `fetch()` take one index per request, so
        `dm.ALL` raises `ValueError` there; `m.indices.snapshots(dm.ALL).fetch()` gives every
        index's current session.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#bars,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `"I:SPX"` or `["I:SPX", "I:NDX"]` (`"SPX"` becomes
                `"I:SPX"`); a Deephaven table with a `Symbol` column; or `dm.ALL`.
            timespan: The unit of the bar window, a `dm.Timespan`; `MINUTE` by default.
                `Timespan.SESSION` is for futures only.
            multiplier: The number of `timespan` units in one bar, a positive `int`; 1 by
                default. `multiplier=5` with `Timespan.MINUTE` is 5-minute bars.

        Returns:
            A `StreamHistoryQuery`, `HistoryLatestQuery`, or `HistoryQuery`, as above. Nothing
            is requested until a verb is called.

        A `timespan` that is not a `dm.Timespan` (`dm.Timespan("minute")` converts text), a
        `multiplier` that is not an `int`, or `symbols` of an unsupported type raises
        `TypeError`. `Timespan.SESSION`, a `multiplier` that is not positive, a symbol that is
        not a valid index symbol, an empty symbol list, or a table without a `Symbol` column
        raises `ValueError`.

        Example:
            ::

                live = m.indices.bars(["I:SPX"]).append_only()  # 1-minute bars as they close
                prev = m.indices.bars("I:SPX", dm.Timespan.DAY).fetch()
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
        interval, one request per cycle for the whole list, into a table keyed by `Symbol`.
        This is the way to get current index levels on every plan.

        `dm.ALL` is every index Massive publishes, paged 250 at a time. `limit` caps the
        total number of rows across pages; the page size is internal.

        Each column is described in the column reference,
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/indices.md#snapshots,
        in the web UI's column tooltips, and by `columns()`.

        Args:
            symbols: The indices, such as `["I:SPX", "I:NDX"]` (`"SPX"` becomes `"I:SPX"`);
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

        A `sort` that is not a `str`, an `order` that is not a `dm.Order`, a `limit` that is
        not an `int`, a `filters` that is not a mapping, or `symbols` of an unsupported type
        raises `TypeError`. A `limit` that is not positive, a filter key that is not a
        parameter of the snapshot or a value of the wrong kind (the message lists the valid
        keys), a symbol that is not a valid index symbol, an empty symbol list, or a table
        without a `Symbol` column raises `ValueError`.

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

    def daily_open_close(self, symbols: Symbols, date: DateLike) -> Query:
        """One day's opening, closing, high, and low levels of each index.

        One row per index for the day `date`, from `/v1/open-close/{indicesTicker}/{date}`:
        `Symbol`, `From` (the day), `Open`, `High`, `Low`, `Close`, and, where Massive has
        them, `PreMarket` and `AfterHours`. REST only, one request per index: `fetch()`
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
            symbols: The indices, such as `["I:SPX", "I:NDX"]` (`"SPX"` becomes `"I:SPX"`), or
                a Deephaven table with a `Symbol` column, re-read on every `poll()` cycle.
            date: The day, as an ISO-8601 date (`"2025-06-02"`), a `date`, or an aware
                `datetime`, which names the New York calendar day it falls on.

        Returns:
            A `Query` with `fetch()` and `poll()`. Nothing is requested until a verb is called.

        A `date` that is not a `str`, `date`, or `datetime`, or `symbols` of an unsupported
        type, raises `TypeError`. `dm.ALL`, a naive `datetime` or text that is not ISO-8601 as
        `date`, a symbol that is not a valid index symbol, an empty symbol list, or a table
        without a `Symbol` column raises `ValueError`.

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
                all_alternative="m.indices.snapshots(dm.ALL)",
                params={"date": calendar_day(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
