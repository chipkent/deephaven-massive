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
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    NotBars,
    Timespan,
    _require_enum,
)
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    Namespace,
    build_query,
    day_argument,
    ticker_key,
)

if TYPE_CHECKING:
    from deephaven_massive._query import HistoryQuery, Query, StreamHistoryQuery, StreamQuery
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

_AGGREGATES_PAGE: Final = 50_000
"""The aggregates range endpoint's largest `limit`, which counts the base aggregates
behind a page. Its specification states this maximum only in prose, so it is typed here."""


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
        (_F, _TRADE_FILES): ticker_key("ParticipantTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
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
        (_R, _GROUPED): ticker_key("WindowStart"),
        (_W, "crypto/XAS"): _STREAM_BAR_KEYS,
        (_W, "crypto/XA"): _STREAM_BAR_KEYS,
        (_F, _MINUTE_FILES): ticker_key("WindowStart"),
        (_F, _DAY_FILES): ticker_key("WindowStart"),
    },
    page_size=_AGGREGATES_PAGE,
)
"""`m.crypto.bars`: the `XAS` and `XA` streams, the range and grouped-daily endpoints, and the
minute and day aggregate flat files."""

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="crypto.previous_day_bar",
    summary="The previous UTC day's open, high, low, close, and volume of each crypto pair.",
    asset_class=AssetClass.CRYPTO,
    kind="bars",
    fetch_path=_PREV,
    keys={(_R, _PREV): ticker_key("WindowStart")},
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
    keys={(_W, "crypto/FMV"): ticker_key("FmvLastUpdated")},
)
"""`m.crypto.fmv`: the Business-only `FMV` stream."""

SNAPSHOTS: Final = DatasetSpec(
    name="crypto.snapshots",
    summary="The current state of crypto pairs: today's, the last minute's, and yesterday's bars.",
    asset_class=AssetClass.CRYPTO,
    fetch_path=_SNAPSHOTS,
    keys={(_R, _SNAPSHOTS): ticker_key()},
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
    keys={(_R, _MOVERS): ticker_key()},
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

    @raises_through(build_query)
    def trades(self, symbols: Symbols) -> StreamHistoryQuery:
        """Trades of crypto pairs, tick by tick, from the exchanges Massive aggregates.

        Each row is one trade on one exchange; `Timestamp` is the exchange's own time and `Date` its
        UTC date. The stream verbs read the WebSocket; `history()` reads REST or the flat files.
        Each pair's latest trade alone is `last_trade()`. A pair missing from Massive's crypto
        ticker list (read once per session, delisted pairs included) is refused when the verb runs,
        since Massive spells it as two currencies.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`, or `dm.ALL`, which
                `history()` reads only from the flat files. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.trades.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a symbol is not an `X:` crypto ticker, the sequence is empty, a table has
                no `Symbol` column, or `symbols` is None.

        Example:
            ::

                live = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
        """
        return cast("StreamHistoryQuery", build_query(self._core, TRADES, symbols=symbols))

    @raises_through(build_query)
    def last_trade(self, symbols: Symbols) -> Query:
        """The latest trade of each crypto pair.

        One row per pair, from one REST request per pair; it carries no trade ID and no received
        time. An explicit `mode=dm.PollMode.APPEND` appends each cycle's trade when it is new, so a
        trade between two cycles is never seen (`trades()` streams every one). Some plans lack this
        endpoint (`EntitlementError`); `snapshots(symbols)` also carries each pair's last trade. A
        pair with no trade (HTTP 404) is left out, with a WARNING. A pair missing from Massive's
        crypto ticker list (read once per session, delisted pairs included) is refused when the verb
        runs, since Massive spells it as two currencies.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`. `dm.ALL` is refused: use
                `m.crypto.snapshots(dm.ALL)`. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.last_trade.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL` or None, a symbol is not an `X:` crypto ticker, the
                sequence is empty, or a table has no `Symbol` column.

        Example:
            ::

                last = m.crypto.last_trade(["X:BTCUSD", "X:ETHUSD"]).fetch()
        """
        query = build_query(
            self._core,
            LAST_TRADE,
            symbols=symbols,
            all_alternative="use m.crypto.snapshots(dm.ALL).fetch()",
        )
        return cast("Query", query)

    @raises_through(build_query)
    def quotes(self, symbols: Symbols) -> StreamQuery:
        """Quotes of crypto pairs: each exchange's best bid and ask, as they change.

        Each row is one exchange's top of book for one pair. Stream only: Massive serves no crypto
        quote history and no last quote, so poll `snapshots(symbols)` for current state. A pair
        missing from Massive's crypto ticker list (read once per session, delisted pairs included)
        is refused when the verb runs, since Massive spells it as two currencies.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`, or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.quotes.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a symbol is not an `X:` crypto ticker, the sequence is empty, a table has
                no `Symbol` column, or `symbols` is None.

        Example:
            ::

                book = m.crypto.quotes("X:BTCUSD").last_by(["Symbol", "Exchange"])
        """
        return cast("StreamQuery", build_query(self._core, QUOTES, symbols=symbols))

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
        self,
        symbols: Symbols,
        timespan: Timespan,
        *,
        multiplier: int = 1,
    ) -> HistoryQuery: ...
    @raises_through(build_query)
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan,
        *,
        multiplier: int = 1,
    ) -> HistoryQuery:
        """Open, high, low, close, and volume bars of crypto pairs, over any time window.

        Each row is one bar of `multiplier` times `timespan`. One-second and one-minute bars also
        stream from the WebSocket; every other length is history only. Days are UTC days and weeks
        run Sunday to Saturday. A 1-day bar that Massive stamps elsewhere than its UTC day's first
        or last millisecond is served on the day it falls on, with a WARNING. Crypto has no
        corporate actions, so there is no `adjusted`. Bar windows, stamps, and which bars
        `history()` returns follow README.md#bars. A pair missing from Massive's crypto ticker list
        (read once per session, delisted pairs included) is refused when the verb runs, since
        Massive spells it as two currencies.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`, or `dm.ALL` (`history()`
                takes it for one-minute and one-day bars). See README.md#symbols.
            timespan: The bar unit; required. See README.md#timespan-and-multiplier.
            multiplier: Units per bar; only 1 streams.

        Returns:
            A query of the class the timespan picks. Columns: docs/datasets/crypto.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, or `symbols` is empty,
                None, or holds a symbol that is not an `X:` crypto ticker.

        Example:
            ::

                hourly = m.crypto.bars("X:BTCUSD", dm.Timespan.HOUR).history(
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

    @raises_through(build_query)
    def previous_day_bar(self, symbols: Symbols) -> Query:
        """The previous UTC day's open, high, low, close, and volume of each crypto pair.

        One row per pair, from one REST request per pair: the 1-day bar of the previous completed
        UTC day. A bar stamped elsewhere than its UTC day's first or last millisecond is served on
        the day it falls on, with a WARNING (README.md#bars). An explicit `mode=dm.PollMode.APPEND`
        appends each new day's bar. A pair with no bar (HTTP 404) is left out, with a WARNING.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`. `dm.ALL` is refused: use
                `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.previous_day_bar.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL` or None, a symbol is not an `X:` crypto ticker, the
                sequence is empty, or a table has no `Symbol` column.

        Example:
            ::

                yesterday = m.crypto.previous_day_bar("X:BTCUSD").fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "use m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
        )
        return cast("Query", query)

    @raises_through(build_query)
    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value of crypto pairs, streamed as it updates.

        Each row is one update of Massive's proprietary price estimate, from its Business feed. A
        plan without it fails the stream's table with `EntitlementError`
        (README.md#plans-and-errors). Stream only: no `history()` or `fetch()`. A pair missing from
        Massive's crypto ticker list (read once per session, delisted pairs included) is refused
        when the verb runs, since Massive spells it as two currencies.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`, or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.fmv.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a symbol is not an `X:` crypto ticker, the sequence is empty, a table has
                no `Symbol` column, or `symbols` is None.

        Example:
            ::

                fair = m.crypto.fmv(["X:BTCUSD", "X:ETHUSD"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query)
    def snapshots(self, symbols: Symbols) -> Query:
        """The current state of crypto pairs: today's, the last minute's, and yesterday's bars.

        One row per pair: today's bar, the latest minute bar, the previous day's bar, the last
        trade, and today's change. Massive clears the snapshot at midnight Eastern time and refills
        it as trades arrive. Up to 250 pairs go in one request. The streams' `last_by()` never seeds
        from REST: poll this dataset for the current state instead.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`, or `dm.ALL` for every
                traded pair. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/crypto.snapshots.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a symbol is not an `X:` crypto ticker, the sequence is empty, a table has
                no `Symbol` column, or `symbols` is None.

        Example:
            ::

                board = m.crypto.snapshots(["X:BTCUSD", "X:ETHUSD"]).poll(5)
        """
        return cast("Query", build_query(self._core, SNAPSHOTS, symbols=symbols))

    @raises_through(_require_enum)
    def movers(self, direction: Direction) -> Query:
        """The 20 crypto pairs that gained or lost the most since the previous day's close.

        Rows have the columns of `snapshots()`, ranked by percentage change; Massive clears the list
        at midnight Eastern time. A `poll()` table keeps one row per pair, so pairs leave as the
        ranking changes, and it can be the `symbols` of another dataset, which then follows the
        list.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.

        Returns:
            A query. Columns: docs/datasets/crypto.movers.md.

        Raises:
            TypeError: If `direction` is not a `dm.Direction` (a plain string such as `"gainers"`
                included).

        Example:
            ::

                gainers = m.crypto.movers(dm.Direction.GAINERS).poll(60)
                trades = m.crypto.trades(gainers).append_only()  # follows the top 20
        """
        side = _require_enum(direction, Direction, "direction")
        return cast("Query", build_query(self._core, MOVERS, params={"direction": side}))

    @raises_through(build_query, day_argument)
    def daily_open_close(self, symbols: Symbols, date: DateLike) -> Query:
        """The open and close prices of crypto pairs on one UTC day, with the trades behind them.

        One row per pair, from one REST request per pair: the day's open and close, and its first
        and last trades (`OpenTrades` and `ClosingTrades`, JSON text). The window is the UTC day. A
        row Massive marks as not UTC (`IsUtc` false) is still served with the UTC day's window and
        logs one WARNING per session, since its dates may not line up with the other crypto tables'.
        A pair missing from Massive's crypto ticker list (read once per session, delisted pairs
        included) is refused when the verb runs, since Massive spells it as two currencies; delisted
        pairs still resolve for past dates.

        Args:
            symbols: Crypto pairs as Massive's tickers, such as `X:BTCUSD`. `dm.ALL` is refused: use
                `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`. See README.md#symbols.
            date: The UTC day, as a calendar date (`"2026-01-02"`, a `date`, or any `DateLike`
                date); a `datetime` or an instant is refused. See README.md#time.

        Returns:
            A query. Columns: docs/datasets/crypto.daily_open_close.md.

        Raises:
            TypeError: If `date` is not a `DateLike`, or `symbols` has an unsupported type.
            ValueError: If `symbols` is `dm.ALL` or None, `date` is a time rather than a date or is
                text that is not a date, a symbol is not an `X:` crypto ticker, the sequence is
                empty, or a table has no `Symbol` column.

        Example:
            ::

                q = m.crypto.daily_open_close(["X:BTCUSD", "X:ETHUSD"], "2026-01-02").fetch()
        """
        return cast(
            "Query",
            build_query(
                self._core,
                DAILY_OPEN_CLOSE,
                symbols=symbols,
                all_alternative=(
                    "use m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                    "every ticker's bar of that day"
                ),
                params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            ),
        )
