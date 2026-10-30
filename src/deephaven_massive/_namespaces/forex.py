"""The `m.forex` namespace: foreign exchange.

Datasets: quotes, each pair's last quote, bars, each pair's previous day's bar, fair market
value, snapshots, movers, and currency conversion.

Each dataset is one factory method on `_Forex` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`).

A currency pair is Massive's REST ticker, `C:EURUSD`: every factory takes only that form
(`check_symbol` in `_datasets/symbols.py`; `dm.canonical_symbol_forex` converts `EUR/USD`), the
`Symbol` column always holds it, and the transports write the WebSocket form (`EUR-USD`) for
topics and read every spelling Massive sends back (`EUR/USD` in frames, `X:EUR-USD` in the
quotes flat-file sample). The last-quote
endpoint takes a pair as two path segments (`/v1/last_quote/currencies/EUR/USD`); its spec
declares the symbol as filled from the request, so the row's `Symbol` is the whole pair.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.symbols import is_forex_currency
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
    accepted_values,
    build_query,
)

if TYPE_CHECKING:
    from deephaven_massive._query import (
        HistoryQuery,
        Query,
        StreamHistoryQuery,
        StreamQuery,
    )
    from deephaven_massive._types import StreamSpan, Symbols

__all__ = ["DATASETS", "_Forex"]

logger = logging.getLogger(__name__)

_REST: Final = Transport.REST
_WS: Final = Transport.WS
_FILES: Final = Transport.FLAT_FILES

_PAGE_SIZE: Final = 50_000
"""The largest page the quotes and aggregate-range endpoints accept (their documented maximum)."""

_LAST_QUOTE: Final = "/v1/last_quote/currencies/{from}/{to}"
_QUOTES: Final = "/v3/quotes/{fxTicker}"
_QUOTE_FILES: Final = "global_forex/quotes_v1"

QUOTES: Final = DatasetSpec(
    name="forex.quotes",
    summary="Foreign exchange quotes: the best bid and ask of each currency pair, tick by tick.",
    asset_class=AssetClass.FOREX,
    kind="quotes",
    history_path=_QUOTES,
    channels={NotBars.ANY: "C"},
    flat_file_prefixes={NotBars.ANY: _QUOTE_FILES},
    keys={
        (_REST, _QUOTES): SourceKeys(
            symbol=KeySpec("fxTicker", from_request=True), time=KeySpec("ParticipantTimestamp")
        ),
        (_WS, "forex/C"): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("ParticipantTimestamp")
        ),
        (_FILES, _QUOTE_FILES): SourceKeys(
            symbol=KeySpec("Ticker"), time=KeySpec("ParticipantTimestamp")
        ),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=_PAGE_SIZE,
)
"""`m.forex.quotes`: the `C` stream, REST quote history, and quote files."""

LAST_QUOTE: Final = DatasetSpec(
    name="forex.last_quote",
    summary="The latest best bid and ask of each currency pair.",
    asset_class=AssetClass.FOREX,
    kind="quotes",
    fetch_path=_LAST_QUOTE,
    keys={
        # The last quote's `timestamp` is `Timestamp` after its projection onto the quotes kind.
        (_REST, _LAST_QUOTE): SourceKeys(
            symbol=KeySpec("from", from_request=True), time=KeySpec("Timestamp")
        ),
    },
    per_symbol=True,
    revision_keys=("Timestamp",),
)
"""`m.forex.last_quote`: each pair's last quote, in the quotes columns its endpoint fills."""

_PREV: Final = "/v2/aggs/ticker/{forexTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{forexTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_GROUPED: Final = "/v2/aggs/grouped/locale/global/market/fx/{date}"
_MINUTE_FILES: Final = "global_forex/minute_aggs_v1"
_DAY_FILES: Final = "global_forex/day_aggs_v1"
_BAR_KEYS: Final = SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("WindowStart"))
_STREAM_BAR_KEYS: Final = SourceKeys(
    symbol=KeySpec("Pair"), time=KeySpec("WindowStart"), end=KeySpec("EndTimestamp")
)

BARS: Final = DatasetSpec(
    name="forex.bars",
    summary="Aggregate bars of currency pairs: open, high, low, close, and volume per window.",
    asset_class=AssetClass.FOREX,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    grouped_daily_path=_GROUPED,
    channels={Timespan.SECOND: "CAS", Timespan.MINUTE: "CA"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_REST, _RANGE): SourceKeys(
            symbol=KeySpec("forexTicker", from_request=True), time=KeySpec("WindowStart")
        ),
        (_REST, _GROUPED): _BAR_KEYS,
        (_WS, "forex/CAS"): _STREAM_BAR_KEYS,
        (_WS, "forex/CA"): _STREAM_BAR_KEYS,
        (_FILES, _MINUTE_FILES): _BAR_KEYS,
        (_FILES, _DAY_FILES): _BAR_KEYS,
    },
    page_size=_PAGE_SIZE,
)
"""`m.forex.bars`: `CAS` and `CA` streams, ranges, grouped daily bars, and bar files."""

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="forex.previous_day_bar",
    summary="The previous UTC day's open, high, low, close, and volume of each currency pair.",
    asset_class=AssetClass.FOREX,
    kind="bars",
    fetch_path=_PREV,
    keys={(_REST, _PREV): _BAR_KEYS},
    per_symbol=True,
    revision_keys=("TimestampStart",),
)
"""`m.forex.previous_day_bar`: each pair's 1-day bar of the previous UTC day, from `/prev`."""

FMV: Final = DatasetSpec(
    name="forex.fmv",
    summary="Massive's fair market value of each currency pair, streamed as it updates.",
    asset_class=AssetClass.FOREX,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(_WS, "forex/FMV"): SourceKeys(symbol=KeySpec("Ticker"), time=KeySpec("FmvLastUpdated"))},
)
"""`m.forex.fmv`: the `FMV` stream of the Business feed."""

_SNAPSHOTS: Final = "/v2/snapshot/locale/global/markets/forex/tickers"

SNAPSHOTS: Final = DatasetSpec(
    name="forex.snapshots",
    summary=(
        "The current state of currency pairs: today's bar, the last minute, and the last quote."
    ),
    asset_class=AssetClass.FOREX,
    fetch_path=_SNAPSHOTS,
    keys={(_REST, _SNAPSHOTS): SourceKeys(symbol=KeySpec("Ticker"))},
    symbol_param="tickers",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    revision_keys=("Updated",),
)
"""`m.forex.snapshots`: the full-market snapshot, narrowed by its `tickers` parameter."""

_MOVERS: Final = "/v2/snapshot/locale/global/markets/forex/{direction}"

MOVERS: Final = DatasetSpec(
    name="forex.movers",
    summary="The twenty currency pairs that gained or lost the most since the previous UTC close.",
    asset_class=AssetClass.FOREX,
    fetch_path=_MOVERS,
    keys={(_REST, _MOVERS): SourceKeys(symbol=KeySpec("Ticker"))},
    revision_keys=("Updated",),
)
"""`m.forex.movers`: the top gainers or losers, the direction a path segment."""

_CONVERSION: Final = "/v1/conversion/{from}/{to}"

CONVERSION: Final = DatasetSpec(
    name="forex.conversion",
    summary="An amount converted between two currencies at the latest quote.",
    asset_class=AssetClass.FOREX,
    fetch_path=_CONVERSION,
    # The vendor's own `symbol` field is generated as `Symbol`, so the key uses it as is.
    keys={(_REST, _CONVERSION): SourceKeys(symbol=KeySpec("Symbol"))},
    # The quote the conversion used: an explicit APPEND poll appends each new conversion.
    revision_keys=("LastTimestamp",),
)
"""`m.forex.conversion`: one conversion between two currencies at the latest quote."""

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    QUOTES,
    LAST_QUOTE,
    BARS,
    PREVIOUS_DAY_BAR,
    FMV,
    SNAPSHOTS,
    MOVERS,
    CONVERSION,
)
"""Every dataset of `m.forex`, one `DatasetSpec` each, in the order of the namespace's methods."""


def _currency(name: str, value: object) -> str:
    """Return a currency code in the form the conversion endpoint takes.

    Args:
        name: The parameter's name, for messages.
        value: The code as passed; surrounding whitespace is ignored and letters are
            upper-cased.

    Returns:
        The three-letter code, upper case, such as `"EUR"`.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is not three letters.
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, not {type(value).__name__}")
    code = value.strip().upper()
    if not is_forex_currency(code):
        raise ValueError(f"{name}={value!r} is not a three-letter currency code such as 'USD'")
    return code


class _Forex(Namespace):
    """`m.forex`: the foreign exchange datasets of a session.

    Created once by `MassiveSession` and reached as `m.forex`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            eur = m.forex.quotes(["C:EURUSD", "C:GBPUSD"]).last_by()
    """

    __slots__ = ()
    _path: ClassVar[str] = "forex"

    @raises_through(build_query)
    def quotes(self, symbols: Symbols) -> StreamHistoryQuery:
        """Foreign exchange quotes: the best bid and ask of each currency pair, tick by tick.

        Massive's forex quotes are a consolidated feed of bank and venue prices. The stream verbs
        read the `C` WebSocket channel; `history()` reads the REST quote history
        (`/v3/quotes/{C:…}`) or the `global_forex/quotes_v1` flat files, in the same columns; each
        pair's latest quote is `last_quote()`. There is no `limit`: `history()` pages internally and
        returns every row of its range. `dm.ALL` streams every pair (the `C.*` topic) and is
        accepted by `history()` over flat files. Each row also carries its time, `Timestamp`, its
        trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of `Timestamp`), and
        `TimestampInsert`, when the library received it.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#quotes.

        Args:
            symbols: The currency pairs: one pair, a sequence of pairs, a Deephaven table with a
                `Symbol` column (a live watchlist), or `dm.ALL`. Each pair is Massive's ticker, such
                as `C:EURUSD` (`dm.canonical_symbol_forex` converts `EUR/USD`).

        Returns:
            A query; nothing is requested or subscribed until one of its verbs is called.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty
                (`"no symbols"`), or a table has no `Symbol` column. Also for `symbols` of None,
                with a message that names the forms it takes.

        Example:
            ::

                q = m.forex.quotes("C:EURUSD")
                live = q.last_by()
                day = q.history("2026-10-20", "2026-10-20")
        """
        query = build_query(self._core, QUOTES, symbols=symbols)
        return cast("StreamHistoryQuery", query)

    @raises_through(build_query)
    def last_quote(self, symbols: Symbols) -> Query:
        """The latest best bid and ask of each currency pair.

        One row per pair: its most recent quote, in the columns of its kind that its endpoint fills,
        with its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of
        `Timestamp`), and `TimestampInsert`, when the library received it. Served by REST
        (`/v1/last_quote/currencies/{from}/{to}`) through `fetch()` and `poll()`, one request per
        pair; `poll()` replaces the table each cycle, keyed by `Symbol`, and an explicit
        `mode=dm.PollMode.APPEND` appends each cycle's last quote when it is new, so a quote between
        two cycles is never seen (`quotes()` streams every one). Some plans lack the endpoint (HTTP
        403 raises `EntitlementError`); `snapshots(symbols)` also carries each pair's last quote,
        on a plan that includes snapshots (see
        https://github.com/chipkent/deephaven-massive/blob/main/docs/getting-started/plans.md).
        A pair with no quote (HTTP 404) is left out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each pair gives one row. `dm.ALL` is refused,
        since the endpoint takes one pair per request: use `m.forex.snapshots(dm.ALL).fetch()`.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#last_quote.

        Args:
            symbols: The currency pairs: one pair, a sequence of pairs, or a Deephaven table
                with a `Symbol` column, re-read each `poll()` cycle. Each pair is Massive's
                ticker, such as `C:EURUSD` (`dm.canonical_symbol_forex` converts `EUR/USD`).

        Returns:
            A query; nothing is requested until one of its verbs is called.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL`, a pair is not two three-letter currency codes, the
                sequence is empty, or a table has no `Symbol` column. Also for `symbols` of None,
                with a message that names the forms it takes.

        Example:
            ::

                top = m.forex.last_quote(["C:EURUSD", "C:USDJPY"]).poll(5)
        """
        query = build_query(
            self._core,
            LAST_QUOTE,
            symbols=symbols,
            all_alternative="use m.forex.snapshots(dm.ALL).fetch()",
        )
        return cast("Query", query)

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
        """Aggregate bars of currency pairs: open, high, low, close, and volume per window.

        Each row is one bar: its window, from `TimestampStart` to the exclusive `TimestampEnd`,
        its trading day `Date` (ISO `yyyy-MM-dd` text, the UTC day), and `TimestampInsert`,
        when the library received it. `TimestampEnd` is the first instant at which the bar is
        complete, so join on it (`t.aj(bar_table, on=["Symbol", "Timestamp>=TimestampEnd"])`,
        where `bar_table` is a table a verb such as `history()` returned) to attach only closed
        bars; an intraday bar ends its length after its start, and a bar of
        a day or longer at 00:00 UTC after its window's last day. `Date` is null for a bar that
        spans more than one UTC date.

        The timespan decides the transports, and so the query's class. One-second and one-minute
        bars (`multiplier=1`) stream from the `CAS` and `CA` WebSocket channels and have
        `history()`; every other timespan and multiplier is `history()` only, and each pair's bar of
        the previous UTC day is `previous_day_bar()`. `history()` reads the REST aggregate range
        (`/v2/aggs/ticker/{t}/range/…`) or the `global_forex/minute_aggs_v1` and
        `global_forex/day_aggs_v1` flat files. Currency pairs have no corporate actions, so their
        bars are never adjusted and there is no `adjusted` argument: no request sends one, and the
        flat files serve every bar timespan they hold. There is no `limit`: `history()` pages
        internally. `dm.ALL` streams every pair (the `CA.*` or `CAS.*` topic), and `history()`
        accepts it for one-day bars, through the grouped daily endpoint
        (`/v2/aggs/grouped/locale/global/market/fx/{date}`, one request per day) or the day files,
        and for one-minute bars through the minute files.

        A one-day bar is the UTC day and starts at 00:00 UTC on every source: the range already
        is, and the grouped daily and the day files (stamped 23:59:59.999 UTC) are
        moved to the start of their day. The interbank forex day rolls at 17:00 New York time,
        so Massive's UTC day is not the dealers' day. A bar of several days or longer must be
        stamped 00:00 UTC, a week on its Sunday and a month, quarter, or year on its 1st. Any
        other stamp fails the call with `MassiveError` naming the source, since the bar may
        cover other days than its label. `history()` returns only the bars whose whole window
        lies inside the range and has ended, so the current day's bar never appears. Forex
        trades from Sunday evening UTC, and Massive starts its weeks at 00:00 UTC on Sunday, so
        its weeks run Sunday to Saturday and its months, quarters, and years are whole calendar
        periods.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#bars.

        Args:
            symbols: The currency pairs: one pair, a sequence of pairs, a Deephaven table with
                a `Symbol` column, or `dm.ALL`. Each pair is Massive's ticker, such as `C:EURUSD`
                (`dm.canonical_symbol_forex` converts `EUR/USD`).
            timespan: The unit of one bar's window, a `dm.Timespan`; required, with no
                default. `dm.Timespan.SESSION` is futures only.
            multiplier: The number of `timespan` units in one bar's window, a positive
                integer. Defaults to 1.

        Returns:
            A `StreamHistoryQuery` for one-second or one-minute bars and a `HistoryQuery`
            otherwise. Nothing is requested or subscribed until a verb is called.

        Raises:
            TypeError: If `symbols` is not a pair, a sequence of pairs, a table, or `dm.ALL`,
                `timespan` is not a `dm.Timespan`, or `multiplier` is not an `int`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, a
                table has no `Symbol` column, `multiplier` is not positive, or `timespan` is
                `dm.Timespan.SESSION`. Also for `symbols` of None, with a message that names the
                forms it takes.

        Example:
            ::

                live = m.forex.bars("C:EURUSD", dm.Timespan.MINUTE).append_only()
                hourly = m.forex.bars(["C:EURUSD"], dm.Timespan.HOUR).history(
                    "2026-10-01", "2026-10-23"
                )
        """
        query = build_query(
            self._core,
            BARS,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
        )
        return cast("HistoryQuery", query)

    @raises_through(build_query)
    def previous_day_bar(self, symbols: Symbols) -> Query:
        """The previous UTC day's open, high, low, close, and volume of each currency pair.

        One row per pair: its 1-day bar of the previous completed UTC day, in the columns of its
        kind that its endpoint fills: its window, from `TimestampStart`, 00:00 UTC, to the exclusive
        `TimestampEnd`, 00:00 UTC of the next day, its trading day `Date` (ISO `yyyy-MM-dd` text),
        and `TimestampInsert`, when the library received it. Served by REST
        (`/v2/aggs/ticker/{t}/prev`) through `fetch()` and `poll()`, one request per pair; `poll()`
        replaces the table each cycle, keyed by `Symbol`, and an explicit `mode=dm.PollMode.APPEND`
        appends each new day's bar. Massive stamps the bar at the day's last millisecond,
        23:59:59.999 UTC, which is moved to the day's start; any other stamp raises `MassiveError`
        naming `bars(symbols, dm.Timespan.DAY).history()` for that day. Currency pairs have no
        corporate actions, so there is no `adjusted` argument. A pair with no bar (HTTP 404) is left
        out, with a WARNING.

        `fetch()` and `poll()` take no `limit`: each pair gives one row. `dm.ALL` is refused,
        since the endpoint takes one pair per request: use
        `m.forex.bars(dm.ALL, dm.Timespan.DAY).history(day, day)` for every pair's bar of a day.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#previous_day_bar.

        Args:
            symbols: The currency pairs: one pair, a sequence of pairs, or a Deephaven table
                with a `Symbol` column, re-read each `poll()` cycle. Each pair is Massive's
                ticker, such as `C:EURUSD` (`dm.canonical_symbol_forex` converts `EUR/USD`).

        Returns:
            A query; nothing is requested until one of its verbs is called.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL`, a pair is not two three-letter currency codes, the
                sequence is empty, or a table has no `Symbol` column. Also for `symbols` of None,
                with a message that names the forms it takes.

        Example:
            ::

                yesterday = m.forex.previous_day_bar("C:EURUSD").fetch()
        """
        query = build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            symbols=symbols,
            all_alternative=(
                "use m.forex.bars(dm.ALL, dm.Timespan.DAY).history(day, day), "
                "every ticker's bar of that day"
            ),
            timespan=Timespan.DAY,
        )
        return cast("Query", query)

    @raises_through(build_query)
    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value of each currency pair, streamed as it updates.

        The `FMV` channel of Massive's Business feed (`business.massive.com/forex`): a
        proprietary real-time price estimate. It is stream only: no `history()` or `fetch()`,
        and no `limit`. `dm.ALL` streams every pair (the `FMV.*` topic). Each row also carries
        its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the UTC date of
        `Timestamp`), and `TimestampInsert`, when the library received it.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#fmv.

        Args:
            symbols: The currency pairs: one pair, a sequence of pairs, a Deephaven table with
                a `Symbol` column, or `dm.ALL`. Each pair is Massive's ticker, such as `C:EURUSD`
                (`dm.canonical_symbol_forex` converts `EUR/USD`).

        Returns:
            A query; nothing is subscribed until one of its verbs is called.

        Raises:
            TypeError: If `symbols` is not a pair, a sequence of pairs, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, or
                a table has no `Symbol` column. Also for `symbols` of None, with a message that
                names the forms it takes.

        Example:
            ::

                fair = m.forex.fmv(["C:EURUSD", "C:USDJPY"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query)
    def snapshots(self, symbols: Symbols) -> Query:
        """The current state of currency pairs: today's bar, the last minute, and the last quote.

        One request to `/v2/snapshot/locale/global/markets/forex/tickers` returns, per pair,
        today's and the previous day's bars, the latest minute bar, the last quote, and the
        change since the previous close; the day bars follow the UTC day. It is REST only:
        `fetch()` once, or `poll(interval)` for a live table with one row per pair (keyed by
        `Symbol`). Every pair is returned in one response, so there is no `limit`. `dm.ALL`
        is every pair Massive tracks.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#snapshots.

        Args:
            symbols: The currency pairs, sent as the `tickers` parameter: one pair, a sequence
                of pairs, a Deephaven table with a `Symbol` column (re-read every poll
                cycle), or `dm.ALL`. Each pair is Massive's ticker, such as `C:EURUSD`
                (`dm.canonical_symbol_forex` converts `EUR/USD`).

        Returns:
            A query; nothing is requested until `fetch()` or `poll()` is called.

        Raises:
            TypeError: If `symbols` is not a pair, a sequence of pairs, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, or
                a table has no `Symbol` column. Also for `symbols` of None, with a message that
                names the forms it takes.

        Example:
            ::

                board = m.forex.snapshots(["C:EURUSD", "C:GBPUSD"]).poll(5)
                market = m.forex.snapshots(dm.ALL).fetch()
        """
        return cast("Query", build_query(self._core, SNAPSHOTS, symbols=symbols))

    @raises_through(_require_enum)
    def movers(self, direction: Direction) -> Query:
        """The twenty currency pairs that gained or lost the most since the previous UTC close.

        One request to `/v2/snapshot/locale/global/markets/forex/{direction}` returns the top
        gainers or losers by percentage change since the previous UTC day's close, in the
        same columns as `snapshots()`; the day bars follow the UTC day too. Massive clears the
        snapshot daily at 12:00 AM Eastern and repopulates it as quotes arrive, so it can be
        empty just after that reset. It is REST only: `fetch()`, or `poll(interval)` for a live
        table keyed by `Symbol` whose rows come and go as the ranking changes. It takes no
        symbols, so `dm.ALL` does not apply, and has no `limit`: Massive returns at most
        twenty pairs.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#movers.

        Args:
            direction: `dm.Direction.GAINERS` for the largest increases or
                `dm.Direction.LOSERS` for the largest decreases.

        Returns:
            A query; nothing is requested until `fetch()` or `poll()` is called.

        Raises:
            TypeError: If `direction` is not a `dm.Direction` (convert text with
                `dm.Direction("gainers")`).

        Example:
            ::

                losers = m.forex.movers(dm.Direction.LOSERS).poll(60)
        """
        side = _require_enum(direction, Direction, "direction")
        return cast("Query", build_query(self._core, MOVERS, params={"direction": side}))

    @raises_through(build_query, _currency)
    def conversion(
        self, from_currency: str, to_currency: str, *, amount: float = 1.0, precision: int = 2
    ) -> Query:
        """An amount converted between two currencies at the latest quote.

        One request to `/v1/conversion/{from}/{to}` returns one row: the converted amount,
        the amount converted, and the last bid and ask it used. It is REST only: `fetch()`,
        or `poll(interval)` for a live single-row table keyed by `Symbol`. It takes no
        symbols, so `dm.ALL` does not apply, and has no `limit`.

        Each column is described in the web UI's column tooltips, by `columns()`, and at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/forex.md#conversion.

        Args:
            from_currency: The currency to convert from, a three-letter code such as
                `"AUD"`; case and surrounding whitespace are ignored.
            to_currency: The currency to convert to, such as `"USD"`.
            amount: The amount of `from_currency` to convert, a positive, finite `int` or
                `float`; an `int` is sent as is. Defaults to 1.0.
            precision: The decimal places of the converted amount, one of the values the
                endpoint's specification lists (0 to 4). Defaults to 2.

        Returns:
            A query; nothing is requested until `fetch()` or `poll()` is called.

        Raises:
            TypeError: If a currency is not a `str`, `amount` is not a number, or `precision`
                is not an `int` (a `bool` is neither).
            ValueError: If a currency is not three letters, `amount` is not positive and
                finite, or `precision` is not one the endpoint accepts.

        Example:
            ::

                usd = m.forex.conversion("AUD", "USD", amount=100.0, precision=4).fetch()
        """
        base = _currency("from_currency", from_currency)
        target = _currency("to_currency", to_currency)
        if isinstance(amount, bool) or not isinstance(amount, int | float):
            raise TypeError(f"amount must be an int or float, not {type(amount).__name__}")
        if not amount > 0 or amount == math.inf:  # NaN fails the first test
            raise ValueError(f"amount must be positive and finite, got {amount!r}")
        if isinstance(precision, bool) or not isinstance(precision, int):
            raise TypeError(f"precision must be an int, not {type(precision).__name__}")
        # An enum of digit strings ("0" to "4" at the snapshot), followed through a refresh.
        accepted = frozenset(map(int, accepted_values(self._core, CONVERSION, "precision")))
        if precision not in accepted:
            raise ValueError(f"precision must be one of {sorted(accepted)}, got {precision}")
        # The endpoint's `precision` is an enum of digit strings, so it is sent as text.
        params = {"from": base, "to": target, "amount": amount, "precision": str(precision)}
        return cast("Query", build_query(self._core, CONVERSION, params=params))
