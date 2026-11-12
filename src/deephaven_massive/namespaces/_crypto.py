"""The `m.crypto` namespace: crypto pairs such as `X:BTCUSD`.

Datasets: trades, each pair's last trade, quotes, bars, each pair's previous day's bar, fair
market value, snapshots, movers, and daily open/close.

Each dataset is one factory method on `CryptoNamespace` that validates its arguments and returns a
query; nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG`
in `_session.py`). A spec names the generated sources that serve its dataset and the key columns of
each; the columns, their types, and their timestamp units come from the generated schemas.

A crypto pair is Massive's REST ticker, `X:BTCUSD`: every factory takes only that form
(`dm.canonical_symbol_crypto` converts `BTC-USD`), and every `Symbol` column holds it, whatever
form the transport sends (`BTC-USD` in WebSocket frames, `X:BTC-USD` in FMV frames). Crypto trades
around the clock, so a date-only bound is a whole UTC day.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

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
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    build_query,
    day_argument,
    ticker_key,
)
from deephaven_massive.queries import (
    FetchQuery,
    StreamHistoryQuery,
    SymbolStreamQuery,
)

if TYPE_CHECKING:
    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.symbols import SymbolsLike

__all__ = ["DATASETS", "CryptoNamespace"]


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
    serves_current_data=True,
    refused_alternative="crypto.snapshots",
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
    channel_family=ChannelFamily.FMV,
    keys={(_W, "crypto/FMV"): ticker_key("FmvLastUpdated")},
    real_time_only=True,
)
"""`m.crypto.fmv`: the Business-only `FMV` stream."""

SNAPSHOTS: Final = DatasetSpec(
    name="crypto.snapshots",
    summary="The current state of crypto pairs: today's, the last minute's, and yesterday's bars.",
    asset_class=AssetClass.CRYPTO,
    fetch_path=_SNAPSHOTS,
    serves_current_data=True,
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
    serves_current_data=True,
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


class CryptoNamespace(Namespace):
    """The crypto datasets, reached as `m.crypto`.

    Each method returns a query of one dataset, and together they cover trades, quotes, bars, each
    pair's last trade and previous day's bar, fair market value, snapshots, movers, and each pair's
    daily open and close. A method reads nothing, so it never blocks. You can call it from
    any thread.
    You reach it through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import CryptoNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            btc = m.crypto.trades("X:BTCUSD").last_by()
            daily = m.crypto.bars(["X:BTCUSD", "X:ETHUSD"], dm.Timespan.DAY).history(
                "2026-10-01", "2026-10-31"
            )
    """

    __slots__ = ()
    _path: ClassVar[str] = "crypto"

    @raises_through(build_query)
    def trades(self, symbols: SymbolsLike) -> StreamHistoryQuery:
        """Trades of crypto pairs, tick by tick, from the exchanges Massive aggregates.

        Each row holds one trade on one exchange. `Timestamp` is the exchange's own time, and `Date`
        is its UTC date. A stream skips a pair that isn't on Massive's crypto ticker list, or fails
        under `unlisted_symbols=dm.OnError.RAISE`; `history()` sends the pair as written, and
        Massive answers it. For each pair's latest trade alone, use `last_trade()`.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`, or `dm.ALL` for every pair. `history()` reads
                `dm.ALL` only from the flat files.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.trades.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                live = m.crypto.trades(["X:BTCUSD", "X:ETHUSD"]).append_only()
        """
        return build_query(self._core, TRADES, query_type=StreamHistoryQuery, symbols=symbols)

    @raises_through(build_query)
    def last_trade(self, symbols: SymbolsLike) -> FetchQuery:
        """The latest trade of each crypto pair.

        Each row holds one pair's latest trade. A pair with no trade gets no row, and the library
        logs a WARNING. Each poll sees only the trade that is latest at that moment; `trades()`
        streams every trade. Currencies Basic doesn't include this dataset. A pair that isn't on
        Massive's crypto ticker list gives no rows by default; under
        `unlisted_symbols=dm.OnError.RAISE` it fails.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`. `dm.ALL` is refused; for every pair, use
                `m.crypto.snapshots(dm.ALL)`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.last_trade.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, or a table.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                last = m.crypto.last_trade(["X:BTCUSD", "X:ETHUSD"]).fetch()
        """
        return build_query(
            self._core,
            LAST_TRADE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="use m.crypto.snapshots(dm.ALL).fetch()",
        )

    @raises_through(build_query)
    def quotes(self, symbols: SymbolsLike) -> SymbolStreamQuery:
        """Quotes of crypto pairs: each exchange's best bid and ask, as they change.

        Each row holds one exchange's best bid and ask for one pair. Massive serves no crypto quote
        history and no last quote. For each pair's current state, poll `snapshots()`. A pair
        that isn't on Massive's crypto ticker list gives no rows by default; under
        `unlisted_symbols=dm.OnError.RAISE` it fails.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`, or `dm.ALL` for every pair.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.quotes.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                book = m.crypto.quotes("X:BTCUSD").last_by(["Symbol", "Exchange"])
        """
        return build_query(self._core, QUOTES, query_type=SymbolStreamQuery, symbols=symbols)

    @raises_through(build_query)
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
    ) -> StreamHistoryQuery:
        """Open, high, low, close, and volume bars of crypto pairs, over any time window.

        Each row is one bar of `multiplier` times `timespan`. Days are UTC days, and weeks run
        Sunday to Saturday. Crypto has no splits, so there's no `adjusted`. A stream skips a pair
        that isn't on Massive's crypto ticker list, or fails under
        `unlisted_symbols=dm.OnError.RAISE`; `history()` sends the pair as written, and Massive
        answers it.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`, or `dm.ALL` for every pair. `history()` takes
                `dm.ALL` for one-minute and one-day bars.
            timespan: The bar's unit. Massive refuses `dm.Timespan.SESSION`, a futures unit.
            multiplier: Units per bar.

        Returns:
            A query that reads history, and streams 1-second and 1-minute bars at
            `multiplier=1`. Its stream methods refuse any other bars with `ValueError`. Its
            columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker is empty or holds `,` or `*`.

        Example:
            ::

                hourly = m.crypto.bars("X:BTCUSD", dm.Timespan.HOUR).history(
                    "2026-10-01", "2026-10-07"
                )
        """
        return build_query(
            self._core,
            BARS,
            query_type=StreamHistoryQuery,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
        )

    @raises_through(build_query)
    def previous_day_bar(self, symbols: SymbolsLike) -> FetchQuery:
        """The previous UTC day's open, high, low, close, and volume of each crypto pair.

        Each row holds one pair's bar of the previous complete UTC day. A pair with no bar gets no
        row, and the library logs a WARNING.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`. `dm.ALL` is refused; for every pair's bar of
                one day, use `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.previous_day_bar.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, or a table.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                yesterday = m.crypto.previous_day_bar("X:BTCUSD").fetch()
        """
        return build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "use m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
        )

    @raises_through(build_query)
    def fmv(self, symbols: SymbolsLike) -> SymbolStreamQuery:
        """Massive's fair market value of crypto pairs, streamed as it updates.

        Each row holds one update of Massive's own price estimate. Only Currencies Business includes
        it; on another plan, the table fails with `EntitlementError`. It streams only, with no
        history. A pair that isn't on Massive's crypto ticker list gives no rows by default; under
        `unlisted_symbols=dm.OnError.RAISE` it fails.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`, or `dm.ALL` for every pair.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.fmv.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                fair = m.crypto.fmv(["X:BTCUSD", "X:ETHUSD"]).last_by()
        """
        return build_query(self._core, FMV, query_type=SymbolStreamQuery, symbols=symbols)

    @raises_through(build_query)
    def snapshots(self, symbols: SymbolsLike) -> FetchQuery:
        """The current state of crypto pairs: today's, the last minute's, and yesterday's bars.

        Each row holds one pair's bar for today, its latest minute bar, and its previous day's bar.
        It also holds the pair's last trade and today's change. Massive clears the snapshots daily
        at midnight Eastern time and refills them as trades arrive. Poll this dataset for each
        pair's state at once; a stream's `last_by()` starts empty.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`, or `dm.ALL` for every traded pair.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.snapshots.md.

        Raises:
            TypeError: If `symbols` isn't a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column.

        Example:
            ::

                latest = m.crypto.snapshots(["X:BTCUSD", "X:ETHUSD"]).poll(5)
        """
        return build_query(self._core, SNAPSHOTS, query_type=FetchQuery, symbols=symbols)

    @raises_through(_require_enum)
    def movers(self, direction: Direction) -> FetchQuery:
        """The 20 crypto pairs that gained or lost the most since the previous day's close.

        Each row holds one pair's snapshot, with the columns of `snapshots()`, ranked by percentage
        change. Massive clears the list daily at midnight Eastern time. A `poll()` table keeps one
        row per pair, so pairs leave as the ranking changes. The table can be the `symbols` of
        another dataset, which then follows the list.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.movers.md.

        Raises:
            TypeError: If `direction` isn't a `dm.Direction`, such as the plain string `"gainers"`.

        Example:
            ::

                gainers = m.crypto.movers(dm.Direction.GAINERS).poll(60)
                trades = m.crypto.trades(gainers).append_only()  # follows the top 20
        """
        side = _require_enum(direction, Direction, "direction")
        return build_query(self._core, MOVERS, query_type=FetchQuery, params={"direction": side})

    @raises_through(build_query, day_argument)
    def daily_open_close(self, symbols: SymbolsLike, date: DateLike) -> FetchQuery:
        """The open and close prices of crypto pairs on one UTC day, with the trades behind them.

        Each row holds one pair's open and close for the UTC day. `OpenTrades` and `ClosingTrades`
        hold its first and last trades as JSON text. A pair that isn't on Massive's crypto
        ticker list gives no rows by default; under `unlisted_symbols=dm.OnError.RAISE` it fails.
        Delisted pairs are on the list, so past dates work for them.

        Args:
            symbols: Crypto pairs, such as `X:BTCUSD`. `dm.ALL` is refused; for every pair's bar of
                one day, use `m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`.
            date: The UTC day, as a date such as `"2026-01-02"`. A `datetime` or an instant is
                refused.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/crypto.daily_open_close.md.

        Raises:
            TypeError: If `date` isn't a date or a time, or `symbols` has another type.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker is empty or holds `,` or `*`;
                - a table has no `Symbol` column;
                - `date` is a time, or text that isn't a date.

        Example:
            ::

                days = m.crypto.daily_open_close(["X:BTCUSD", "X:ETHUSD"], "2026-01-02").fetch()
        """
        return build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "use m.crypto.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
        )
