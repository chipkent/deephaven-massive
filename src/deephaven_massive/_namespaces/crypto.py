"""The `m.crypto` namespace: crypto pairs such as `X:BTCUSD`.

Datasets: trades, each pair's last trade, quotes, bars, each pair's previous day's bar, fair
market value, snapshots, movers, and daily open/close.

Each dataset is one factory method on `_Crypto` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset
catalog (`CATALOG` in `_session.py`). A spec names the generated sources that serve its
dataset and the key columns of each; the columns, their types, and their timestamp units come
from the generated schemas.

A crypto pair is Massive's REST ticker, `X:BTCUSD`: every factory takes only that form
(`dm.canonical_symbol_crypto` converts `BTC-USD`), and every `Symbol` column holds it, whatever
form the transport sends (`BTC-USD` in WebSocket frames, `X:BTC-USD` in FMV frames). Crypto trades
around the clock, so a date-only bound is a whole UTC day.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import calendar_day
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    NotBars,
    Timespan,
    _require_enum,
)
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import SNAPSHOT_SYMBOLS_PER_REQUEST, Namespace, build_query

if TYPE_CHECKING:
    from deephaven_massive._query import (
        HistoryQuery,
        Query,
        StreamHistoryQuery,
        StreamQuery,
    )
    from deephaven_massive._types import DateLike, StreamSpan, Symbols

__all__ = ["DATASETS", "_Crypto"]

logger = logging.getLogger(__name__)


_R, _W, _F = Transport.REST, Transport.WS, Transport.FLAT_FILES

_LAST_TRADE: Final = "/v1/last/crypto/{from}/{to}"
_TRADES: Final = "/v3/trades/{cryptoTicker}"
_PREV: Final = "/v2/aggs/ticker/{cryptoTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{cryptoTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_GROUPED: Final = "/v2/aggs/grouped/locale/global/market/crypto/{date}"
_SNAPSHOTS: Final = "/v2/snapshot/locale/global/markets/crypto/tickers"
_MOVERS: Final = "/v2/snapshot/locale/global/markets/crypto/{direction}"
_OPEN_CLOSE: Final = "/v1/open-close/crypto/{from}/{to}/{date}"
_TRADE_FILES: Final = "global_crypto/trades_v1"
_MINUTE_FILES: Final = "global_crypto/minute_aggs_v1"
_DAY_FILES: Final = "global_crypto/day_aggs_v1"

_MAX_PAGE: Final = 50_000
"""The largest `limit` `/v3/trades` and the aggregates range endpoint accept.

On `/v3/trades` it is rows per page; on the range endpoint it counts the base aggregates
a page is built from, not the bars it returns."""


def _keys(symbol: KeySpec, time: str | None = None) -> SourceKeys:
    """Declare one source's `symbol` key and, where it has one, its `time` key column.

    Args:
        symbol: Where the source carries the pair.
        time: The source's column that holds the row's primary time, or None.

    Returns:
        The source's keys.
    """
    return SourceKeys(symbol=symbol, time=None if time is None else KeySpec(time))


def _request(parameter: str) -> KeySpec:
    """Declare a symbol the source takes as a request parameter rather than in its rows.

    Args:
        parameter: The path parameter the pair is sent in; `"from"` for a path that takes the
            pair as two currencies, `{from}/{to}`.

    Returns:
        The key, filled from the request.
    """
    return KeySpec(parameter, from_request=True)


TRADES: Final = DatasetSpec(
    name="crypto.trades",
    summary="Trades of crypto pairs, tick by tick, from the exchanges Massive aggregates.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    history_path=_TRADES,
    channels={NotBars.ANY: "XT"},
    flat_file_prefixes={NotBars.ANY: _TRADE_FILES},
    keys={
        (_R, _TRADES): _keys(_request("cryptoTicker"), "ParticipantTimestamp"),
        (_W, "crypto/XT"): _keys(KeySpec("Pair"), "ParticipantTimestamp"),
        (_F, _TRADE_FILES): _keys(KeySpec("Ticker"), "ParticipantTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=_MAX_PAGE,
)
"""`m.crypto.trades`: the `XT` stream, `/v3/trades`, and the trade flat files."""

LAST_TRADE: Final = DatasetSpec(
    name="crypto.last_trade",
    summary="The latest trade of each crypto pair.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    fetch_path=_LAST_TRADE,
    # The last trade holds the pair outside its `last` object and has no participant time.
    keys={(_R, _LAST_TRADE): _keys(_request("from"), "Timestamp")},
    per_symbol=True,
    revision_keys=("Timestamp",),
)
"""`m.crypto.last_trade`: each pair's last trade, in the trades columns its endpoint fills."""

QUOTES: Final = DatasetSpec(
    name="crypto.quotes",
    summary="Quotes of crypto pairs: each exchange's best bid and ask, as they change.",
    asset_class=AssetClass.CRYPTO,
    kind="quotes",
    channels={NotBars.ANY: "XQ"},
    keys={(_W, "crypto/XQ"): _keys(KeySpec("Pair"), "ParticipantTimestamp")},
)
"""`m.crypto.quotes`: the `XQ` stream only; Massive serves no crypto quote history."""

_STREAM_BAR_KEYS: Final = SourceKeys(
    symbol=KeySpec("Pair"), time=KeySpec("WindowStart"), end=KeySpec("EndTimestamp")
)
"""The keys of a WebSocket bars channel: the pair, the window's start, and its end `e`."""

BARS: Final = DatasetSpec(
    name="crypto.bars",
    summary="Open, high, low, close, and volume bars of crypto pairs, over any time window.",
    asset_class=AssetClass.CRYPTO,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    grouped_daily_path=_GROUPED,
    channels={Timespan.SECOND: "XAS", Timespan.MINUTE: "XA"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_R, _RANGE): _keys(_request("cryptoTicker"), "WindowStart"),
        (_R, _GROUPED): _keys(KeySpec("Ticker"), "WindowStart"),
        (_W, "crypto/XAS"): _STREAM_BAR_KEYS,
        (_W, "crypto/XA"): _STREAM_BAR_KEYS,
        (_F, _MINUTE_FILES): _keys(KeySpec("Ticker"), "WindowStart"),
        (_F, _DAY_FILES): _keys(KeySpec("Ticker"), "WindowStart"),
    },
    page_size=_MAX_PAGE,
)
"""`m.crypto.bars`: the `XAS` and `XA` streams, the range and grouped-daily endpoints, and the
minute and day aggregate flat files."""

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="crypto.previous_day_bar",
    summary="The previous UTC day's open, high, low, close, and volume of each crypto pair.",
    asset_class=AssetClass.CRYPTO,
    kind="bars",
    fetch_path=_PREV,
    keys={(_R, _PREV): _keys(KeySpec("Ticker"), "WindowStart")},
    per_symbol=True,
    revision_keys=("TimestampStart",),
)
"""`m.crypto.previous_day_bar`: each pair's 1-day bar of the previous UTC day, from `/prev`."""

FMV: Final = DatasetSpec(
    name="crypto.fmv",
    summary="Massive's fair market value of crypto pairs, streamed as it updates.",
    asset_class=AssetClass.CRYPTO,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(_W, "crypto/FMV"): _keys(KeySpec("Ticker"), "FmvLastUpdated")},
)
"""`m.crypto.fmv`: the Business-only `FMV` stream."""

SNAPSHOTS: Final = DatasetSpec(
    name="crypto.snapshots",
    summary="The current state of crypto pairs: today's, the last minute's, and yesterday's bars.",
    asset_class=AssetClass.CRYPTO,
    fetch_path=_SNAPSHOTS,
    keys={(_R, _SNAPSHOTS): _keys(KeySpec("Ticker"))},
    symbol_param="tickers",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    revision_keys=("Updated",),
)
"""`m.crypto.snapshots`: the full-market snapshot, narrowed by `tickers` unless `dm.ALL`."""

MOVERS: Final = DatasetSpec(
    name="crypto.movers",
    summary="The 20 crypto pairs that gained or lost the most since the previous day's close.",
    asset_class=AssetClass.CRYPTO,
    fetch_path=_MOVERS,
    keys={(_R, _MOVERS): _keys(KeySpec("Ticker"))},
    revision_keys=("Updated",),
)
"""`m.crypto.movers`: the gainers and losers snapshots."""

DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="crypto.daily_open_close",
    summary=(
        "The open and close prices of crypto pairs on one UTC day, with the trades behind them."
    ),
    asset_class=AssetClass.CRYPTO,
    fetch_path=_OPEN_CLOSE,
    # The body names the pair as `BTC-USD`; the transport reads it as `X:BTCUSD`.
    keys={(_R, _OPEN_CLOSE): SourceKeys(symbol=KeySpec("Symbol"), date=KeySpec("Day"))},
    per_symbol=True,
    row_times=RowTimes.DAY,
)
"""`m.crypto.daily_open_close`: one request per pair for one day's open and close."""

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    LAST_TRADE,
    QUOTES,
    BARS,
    PREVIOUS_DAY_BAR,
    FMV,
    SNAPSHOTS,
    MOVERS,
    DAILY_OPEN_CLOSE,
)
"""Every dataset of `m.crypto`, one `DatasetSpec` each, in the order of the namespace's methods."""


class _Crypto(Namespace):
    """`m.crypto`: the crypto pairs datasets of a session.

    Created once by `MassiveSession` and reached as `m.crypto`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            btc = m.crypto.trades("X:BTCUSD").last_by()  # the latest trade, live
            daily = m.crypto.bars(["X:BTCUSD", "X:ETHUSD"], dm.Timespan.DAY).history(
                "2026-01-01", "2026-01-31"
            )
    """

    __slots__ = ()
    _path: ClassVar[str] = "crypto"

    def trades(self, symbols: Symbols) -> StreamHistoryQuery:
        """Trades of crypto pairs, tick by tick, from the exchanges Massive aggregates.

        Each row is one trade on one exchange: its price, size, exchange ID, conditions, and the
        exchange's own (participant) time, which is the row's `Timestamp`. Each row also carries
        its trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of `Timestamp`), and
        `TimestampInsert`, when the library received it.

        Transports: the stream verbs (`blink()`, `append_only()`, `ring()`, `last_by()`)
        read the WebSocket `XT` channel; `history()` reads `/v3/trades/{cryptoTicker}` over
        REST or the `global_crypto/trades_v1` flat files. Each pair's latest trade is
        `last_trade()`. No verb takes a `limit`: `history()` pages internally through every
        row of the range.

        `dm.ALL` streams every pair and reads every pair's flat files;
        `history(source=dm.HistorySource.REST)` with `dm.ALL` raises `ValueError` (the REST
        endpoint takes one pair per request).

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#trades.

        Synchronous errors: a symbol that is not Massive's `X:` crypto ticker, or that carries
        another class's prefix, an empty sequence or exhausted generator, or a table without a
        `Symbol` column raises `ValueError`; `symbols` of another type than a string, a sequence of
        strings, a table, or `dm.ALL` raises `TypeError`. A pair missing from Massive's crypto
        ticker list (read once per session, listed and delisted pairs alike) is refused later,
        since the stream's topic spells the pair as two currencies: a stream verb raises
        `MassiveError` when it starts, and a symbol table that adds one fails the stream.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, a Deephaven table with a `Symbol` column (followed live by the
                stream verbs), or `dm.ALL`.

        Returns:
            The query. No request is made and no stream is opened until a verb is called.

        Example:
            ::

                q = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"])
                live = q.append_only()
                day = q.history("2026-01-02", "2026-01-02")
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each crypto pair.

        One row per pair: its most recent trade, in the columns of its kind that its endpoint
        fills: its price, size, exchange ID, conditions, and time (`Timestamp`), its trading day
        `Date` (ISO `yyyy-MM-dd` text, the UTC date of `Timestamp`), and `TimestampInsert`, when
        the library received it. The endpoint carries no trade ID and no received time, so the
        table has neither column. Served by REST (`/v1/last/crypto/{from}/{to}`) through
        `fetch()` and `poll()`, one request per pair; `poll()` replaces the table each cycle,
        keyed by `Symbol`, and an explicit `mode=dm.PollMode.APPEND` appends each cycle's last
        trade when it is new, so a trade between two cycles is never seen (`trades()` streams
        every one). Some plans lack the endpoint (HTTP 403 raises `EntitlementError`);
        `snapshots(symbols)` carries each pair's last trade on every plan. A pair with no trade
        (HTTP 404) is left out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each pair gives one row. `dm.ALL` is refused,
        since the endpoint takes one pair per request: use `m.crypto.snapshots(dm.ALL).fetch()`.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#last_trade.

        Synchronous errors: `dm.ALL`, a symbol that is not Massive's `X:` crypto ticker, or that
        carries another class's prefix, an empty sequence or exhausted generator, or a table
        without a `Symbol` column raises `ValueError`; `symbols` of another type than a string,
        a sequence of strings, or a table raises `TypeError`. The path takes the pair as two
        currencies, split by Massive's crypto ticker list (read once per session, listed and
        delisted pairs alike), so a pair missing from it makes `fetch()` raise `MassiveError`
        and fails a `poll()` table.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, or a Deephaven table with a `Symbol` column, re-read each `poll()`
                cycle.

        Returns:
            The query. No request is made until a verb is called.

        Example:
            ::

                last = m.crypto.last_trade(["X:BTCUSD", "X:ETHUSD"]).fetch()
        """
        query = build_query(
            self._core,
            LAST_TRADE,
            symbols=symbols,
            all_alternative="m.crypto.snapshots(dm.ALL).fetch()",
        )
        return cast("Query", query)

    def quotes(self, symbols: Symbols) -> StreamQuery:
        """Quotes of crypto pairs: each exchange's best bid and ask, as they change.

        Each row is one exchange's top of book for one pair, with its bid and ask prices and
        sizes and the exchange's (participant) time, which is the row's `Timestamp`. Each row
        also carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of
        `Timestamp`), and `TimestampInsert`, when the library received it.

        Transports: the WebSocket `XQ` channel only, through the stream verbs (`blink()`,
        `append_only()`, `ring()`, `last_by()`). Massive serves no crypto quote history and no
        last-quote endpoint, so the query has no `history()` and no `fetch()`; a stream has
        no `limit`. `m.crypto.snapshots(symbols).poll(n)` is the polled view of current
        state. `dm.ALL` subscribes to every pair.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#quotes.

        Synchronous errors: a symbol that is not Massive's `X:` crypto ticker, or that carries
        another class's prefix, an empty sequence or exhausted generator, or a table without a
        `Symbol` column raises `ValueError`; `symbols` of another type than a string, a sequence of
        strings, a table, or `dm.ALL` raises `TypeError`. A pair missing from Massive's crypto
        ticker list (read once per session, listed and delisted pairs alike) is refused later: a
        stream verb raises `MassiveError` when it starts, and a symbol table that adds one fails the
        stream.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, a Deephaven table with a `Symbol` column (followed live), or
                `dm.ALL`.

        Returns:
            The query. No stream is opened until a verb is called.

        Example:
            ::

                book = m.crypto.quotes("X:BTCUSD").last_by(["Symbol", "Exchange"])
        """
        return cast("StreamQuery", build_query(self._core, QUOTES, symbols=symbols))

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
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
    ) -> HistoryQuery: ...
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
    ) -> HistoryQuery:
        """Open, high, low, close, and volume bars of crypto pairs, over any time window.

        Each row is one bar of one pair: open, high, low, close, volume, and, where the
        transport supplies them, the volume-weighted average price and the trade count. It
        carries its window, from `TimestampStart` to the exclusive `TimestampEnd`, its trading
        day `Date` (ISO `yyyy-MM-dd` text, the UTC day), and `TimestampInsert`, when the library
        received it. `TimestampEnd` is the first instant at which the bar is complete, so join
        on it (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only closed
        bars; an intraday bar ends its length after its start, and a bar of a day or longer at
        00:00 UTC after its window's last day. `Date` is null for a bar that spans more than one
        UTC date.

        The timespan and multiplier decide the transports and so the query's class:

        - `Timespan.SECOND` or `Timespan.MINUTE` with `multiplier=1` (the default) streams the
          WebSocket `XAS` or `XA` channel (`blink()`, `append_only()`, `ring()`,
          `last_by()`) and has `history()`: a `StreamHistoryQuery`.
        - Anything else is history only: a `HistoryQuery`. Each pair's bar of the previous
          completed day is `previous_day_bar()`.

        `history()` reads the aggregates range endpoint over REST, one pair at a time, or the
        `global_crypto/minute_aggs_v1` and `global_crypto/day_aggs_v1` flat files for 1-minute
        and 1-day bars. Crypto has no splits or other corporate actions, so its bars are never
        adjusted: there is no `adjusted` choice and the library sends Massive's `adjusted`
        parameter on no request. There is no `limit`: `history()` pages internally (the
        endpoint's own `limit` counts base aggregates, not rows, so it cannot cap rows).

        A 1-day bar covers one UTC day and starts at 00:00 UTC of that day, whatever the source. The
        `global_crypto/day_aggs_v1` files stamp it at the day's last millisecond (23:59:59.999 UTC),
        which is moved to 00:00 UTC. Massive's documented REST samples do not sit on a UTC day
        boundary (the aggregates range at midnight New York time, grouped daily at times such as
        20:48 UTC), which would be another day cut, so a 1-day bar stamped anywhere in its UTC day
        but its first or last millisecond fails the call with `MassiveError` naming the source,
        rather than being relabelled; so does a REST bar of several days or longer not stamped at
        00:00 UTC, or a week not stamped on its Monday or a month, quarter, or year not on its 1st.
        The day cut of live REST data is not verified yet: if it follows those samples, crypto 1-day
        bars come only from the flat files (`source=dm.HistorySource.FLAT_FILES`).
        `HistorySource.AUTO` picks the source as for every other dataset; the check keeps the bars
        of both sources the same UTC days. `history()` keeps only bars whose whole window lies
        inside the requested range and has ended; weeks run Monday to Sunday and months are whole
        calendar months.

        `dm.ALL` streams every pair. Its `history()` reads the flat files for 1-minute and
        1-day bars, and the grouped-daily endpoint, one request per day, for 1-day bars over
        REST; every other timespan raises `ValueError` with `dm.ALL` (no flat file holds those
        bars, and the range endpoint takes one pair per request).

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#bars.

        Synchronous errors: `Timespan.SESSION` (futures only), a `multiplier` that is not positive,
        or a symbol that is not Massive's `X:` crypto ticker, or that carries another class's
        prefix, an empty sequence or exhausted generator, or a table without a `Symbol` column
        raises `ValueError`; a `timespan` that is not a `dm.Timespan` (a plain string such as
        `"minute"` included), a `multiplier` that is not an `int`, or `symbols` of another type than
        a string, a sequence of strings, a table, or `dm.ALL` raises `TypeError`. A pair missing
        from Massive's crypto ticker list (read once per session, listed and delisted pairs alike)
        is refused later: a stream verb raises `MassiveError` when it starts, and a symbol table
        that adds one fails the stream. `history()` needs no split.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, a Deephaven table with a `Symbol` column, or `dm.ALL`.
            timespan: The unit of the bar window, a `dm.Timespan`; `MINUTE` by default.
                `Timespan.SESSION` is for futures only.
            multiplier: How many `timespan` units one bar spans, a positive integer; 1 by
                default. Only 1 streams.

        Returns:
            The query; its class follows from `timespan` and `multiplier` as above. No request
            is made and no stream is opened until a verb is called.

        Example:
            ::

                live = m.crypto.bars("X:BTCUSD").last_by()  # 1-minute bars
                hourly = m.crypto.bars("X:BTCUSD", dm.Timespan.HOUR, multiplier=4).history(
                    "2026-01-01", "2026-01-07"
                )
        """
        return cast(
            "HistoryQuery",
            build_query(
                self._core,
                BARS,
                symbols=symbols,
                timespan=timespan,
                multiplier=multiplier,
            ),
        )

    def previous_day_bar(self, symbols: Symbols) -> Query:
        """The previous UTC day's open, high, low, close, and volume of each crypto pair.

        One row per pair: its 1-day bar of the previous completed UTC day, in the columns of its
        kind that its endpoint fills: open, high, low, close, volume, the volume-weighted average
        price, and the trade count, its window, from `TimestampStart`, 00:00 UTC, to the exclusive
        `TimestampEnd`, 00:00 UTC of the next day, its trading day `Date` (ISO `yyyy-MM-dd` text),
        and `TimestampInsert`, when the library received it. Served by REST
        (`/v2/aggs/ticker/{cryptoTicker}/prev`) through `fetch()` and `poll()`, one request per
        pair; `poll()` replaces the table each cycle, keyed by `Symbol`, and an explicit
        `mode=dm.PollMode.APPEND` appends each new day's bar. Crypto has no corporate actions, so
        there is no `adjusted` argument. A pair with no bar (HTTP 404) is left out, with a WARNING.

        A bar stamped at the first or last millisecond of its UTC day starts at 00:00 UTC of
        that day; Massive's documented sample of this endpoint sits at midnight New York time,
        another day cut, so a bar stamped anywhere else in its day fails the call with
        `MassiveError` naming `bars(symbols, dm.Timespan.DAY).history()` for that day, rather
        than being relabelled. The day cut of live data is not verified yet.

        `fetch()` and `poll()` take no `limit`: each pair gives one row. `dm.ALL` is refused,
        since the endpoint takes one pair per request: use
        `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)` for every pair's bar of a day.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#previous_day_bar.

        Synchronous errors: `dm.ALL`, a symbol that is not Massive's `X:` crypto ticker, or that
        carries another class's prefix, an empty sequence or exhausted generator, or a table
        without a `Symbol` column raises `ValueError`; `symbols` of another type than a string,
        a sequence of strings, or a table raises `TypeError`.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, or a Deephaven table with a `Symbol` column, re-read each `poll()`
                cycle.

        Returns:
            The query. No request is made until a verb is called.

        Example:
            ::

                yesterday = m.crypto.previous_day_bar("X:BTCUSD").fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative="m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)",
            timespan=Timespan.DAY,
        )
        return cast("Query", query)

    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value of crypto pairs, streamed as it updates.

        Each row is one fair-market-value update of one pair: Massive's proprietary estimate of
        its price and the time of the estimate, which is the row's `Timestamp`. Each row also
        carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of `Timestamp`),
        and `TimestampInsert`, when the library received it.

        Transports: the WebSocket `FMV` channel only, through the stream verbs (`blink()`,
        `append_only()`, `ring()`, `last_by()`), served from `business.massive.com/crypto`
        and available on Business plans. The verbs themselves never raise for a missing plan:
        when Massive refuses the topic, the stream's table fails with `EntitlementError`, and
        when it rejects the key on that host, with `AuthenticationError`. There is no history,
        no `fetch()`, and no `limit`. `dm.ALL` subscribes to every pair.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#fmv.

        Synchronous errors: a symbol that is not Massive's `X:` crypto ticker, or that carries
        another class's prefix, an empty sequence or exhausted generator, or a table without a
        `Symbol` column raises `ValueError`; `symbols` of another type than a string, a sequence of
        strings, a table, or `dm.ALL` raises `TypeError`. A pair missing from Massive's crypto
        ticker list (read once per session, listed and delisted pairs alike) is refused later: a
        stream verb raises `MassiveError` when it starts, and a symbol table that adds one fails the
        stream.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, a Deephaven table with a `Symbol` column (followed live), or
                `dm.ALL`.

        Returns:
            The query. No stream is opened until a verb is called.

        Example:
            ::

                fair = m.crypto.fmv(["X:BTCUSD", "X:ETHUSD"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    def snapshots(self, symbols: Symbols) -> Query:
        """The current state of crypto pairs: today's, the last minute's, and yesterday's bars.

        Each row is one pair: today's open, high, low, close, volume, and VWAP; the latest
        minute bar; the previous day's bar; the last trade; today's change and percentage
        change; and the time the snapshot was last updated. Massive clears the snapshot at
        midnight Eastern time and fills it as trades arrive.

        Transports: REST, `/v2/snapshot/locale/global/markets/crypto/tickers`, with the
        pairs in `tickers`: one request for up to 250 pairs, and one more for each further 250,
        so that no URL grows too long; `dm.ALL` is one request. `fetch()` returns every row of
        the responses and `poll(interval)` repeats them, keyed by `Symbol`
        (`PollMode.REPLACE` by default). The endpoint is not paginated and there is no
        `limit`. It needs a Currencies Starter plan or above, as the crypto
        streams do. The streams' `last_by()` never seeds from REST; poll this dataset for the
        current state of pairs instead.

        `dm.ALL` returns every traded pair.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#snapshots.

        Synchronous errors: a symbol that is not Massive's `X:` crypto ticker, or that carries
        another class's prefix, an empty sequence or exhausted generator, or a table without a
        `Symbol` column raises `ValueError`; `symbols` of another type than a string, a sequence of
        strings, a table, or `dm.ALL` raises `TypeError`.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, a Deephaven table with a `Symbol` column (read once per `poll()`
                cycle), or `dm.ALL` for the whole market.

        Returns:
            The query. No request is made until a verb is called.

        Example:
            ::

                board = m.crypto.snapshots(["X:BTCUSD", "X:ETHUSD", "X:SOLUSD"]).poll(5)
                market = m.crypto.snapshots(dm.ALL).fetch()
        """
        return cast("Query", build_query(self._core, SNAPSHOTS, symbols=symbols))

    def movers(self, direction: Direction) -> Query:
        """The 20 crypto pairs that gained or lost the most since the previous day's close.

        Each row is one pair, in the same columns as `snapshots()`: today's, the last
        minute's, and yesterday's bars, the last trade, and today's change. Massive ranks the
        pairs by percentage change and clears the list at midnight Eastern time.

        Transports: one REST request,
        `/v2/snapshot/locale/global/markets/crypto/{direction}`. `fetch()` returns the
        response's rows and `poll(interval)` repeats it, keyed by `Symbol`
        (`PollMode.REPLACE` by default, so pairs that leave the list are removed). There is
        no `limit` and no `symbols`: the endpoint chooses the pairs. A polled movers table
        can itself be the `symbols` of another dataset, which then follows the list.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#movers.

        Synchronous errors: a `direction` that is not a `dm.Direction` (a plain string such
        as `"gainers"` included) raises `TypeError`.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.

        Returns:
            The query. No request is made until a verb is called.

        Example:
            ::

                gainers = m.crypto.movers(dm.Direction.GAINERS).poll(60)
                trades = m.crypto.trades(gainers).append_only()  # follows the top 20
        """
        side = _require_enum(direction, Direction, "direction")
        return cast("Query", build_query(self._core, MOVERS, params={"direction": side}))

    def daily_open_close(self, symbols: Symbols, date: DateLike) -> Query:
        """The open and close prices of crypto pairs on one UTC day, with the trades behind them.

        Each row is one pair on one day: its open and close prices, and the first and last
        trades of the day (`OpenTrades` and `ClosingTrades`, JSON text). The crypto day runs
        from midnight to midnight UTC, which is the row's `TimestampStart` to `TimestampEnd`;
        a row Massive marks as not UTC (`IsUtc` false) fails the call with `MassiveError`,
        since its day would cover other hours.

        Transports: REST, `/v1/open-close/crypto/{from}/{to}/{date}`, one request per pair.
        `fetch()` returns one row per pair and `poll(interval)` repeats the requests, keyed
        by `Symbol`. There is no `limit`. Crypto has no splits or other corporate actions,
        so there is no `adjusted` choice and Massive's `adjusted` parameter is never sent.

        `dm.ALL` raises `ValueError`: the endpoint takes one pair per request. The daily bars
        of every pair are `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.

        Columns: `columns()` on the query, the web UI's column tooltips, or
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.md#daily_open_close.

        Synchronous errors: `dm.ALL`, a symbol that is not Massive's `X:` crypto ticker, or that
        carries another class's prefix, an empty sequence or exhausted generator, or a table without
        a `Symbol` column, or a `date` that is not ISO-8601 text or is a time without a timezone
        raises `ValueError`; `symbols` of another type than a string, a sequence of strings, a
        table, or `dm.ALL`, or a `date` that is not a `str`, `date`, or `datetime`, raises
        `TypeError`. A pair missing from Massive's crypto ticker list (read once per session, listed
        and delisted pairs alike) is refused later, since the path takes the pair as two currencies:
        `fetch()` raises `MassiveError`, and a `poll()` table fails with it. Since the list holds
        delisted pairs too, a past date of a pair no longer traded still resolves.

        Args:
            symbols: The pairs, as Massive's tickers such as `X:BTCUSD`; one string, a
                sequence, or a Deephaven table with a `Symbol` column (read once per
                `poll()` cycle).
            date: The UTC day, as `"YYYY-MM-DD"` or a `date`. It is a calendar date, so a
                `datetime` raises `ValueError`.

        Returns:
            The query. No request is made until a verb is called.

        Example:
            ::

                q = m.crypto.daily_open_close(["X:BTCUSD", "X:ETHUSD"], "2026-01-02")
                opens = q.fetch().view(["Symbol", "Open", "Close"])
        """
        return cast(
            "Query",
            build_query(
                self._core,
                DAILY_OPEN_CLOSE,
                symbols=symbols,
                all_alternative="m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)",
                params={"date": calendar_day(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
