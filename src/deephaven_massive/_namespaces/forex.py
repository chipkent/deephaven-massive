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
        value: The code as passed, which must already be exact: nothing is stripped or
            re-cased.

    Returns:
        The code unchanged, three upper-case letters such as `"EUR"`.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is not exactly three upper-case letters; when trimming and
            upper-casing would give a code, the message names it (`write 'USD'`).
    """
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a str, not {type(value).__name__}")
    if is_forex_currency(value):
        return value
    meant = value.strip().upper()
    hint = f"; write {meant!r}" if is_forex_currency(meant) else ", such as 'USD'"
    raise ValueError(f"{name}={value!r} is not a three-letter upper-case currency code{hint}")


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

        Each row is one quote of Massive's consolidated feed of bank and venue prices; `Date` is the
        UTC date of `Timestamp`. The stream verbs read the WebSocket; `history()` reads REST or the
        flat files. Each pair's latest quote alone is `last_quote()`.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`
                (`dm.canonical_symbol_forex` converts `EUR/USD`), or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/forex.quotes.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, a
                table has no `Symbol` column, or `symbols` is None.

        Example:
            ::

                eur = m.forex.quotes("C:EURUSD").last_by()
        """
        query = build_query(self._core, QUOTES, symbols=symbols)
        return cast("StreamHistoryQuery", query)

    @raises_through(build_query)
    def last_quote(self, symbols: Symbols) -> Query:
        """The latest best bid and ask of each currency pair.

        One row per pair, from one REST request per pair. An explicit `mode=dm.PollMode.APPEND`
        appends each cycle's quote when it is new, so a quote between two cycles is never seen
        (`quotes()` streams every one). Some plans lack this endpoint (`EntitlementError`);
        `snapshots(symbols)` also carries each pair's last quote. A pair with no quote (HTTP 404) is
        left out, with a WARNING.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`. `dm.ALL` is refused:
                use `m.forex.snapshots(dm.ALL)`. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/forex.last_quote.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL` or None, a pair is not two three-letter currency
                codes, the sequence is empty, or a table has no `Symbol` column.

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

        Each row is one bar of `multiplier` times `timespan`. One-second and one-minute bars also
        stream from the WebSocket; every other length is history only. Days are UTC days, though the
        interbank day rolls at 17:00 New York time, and weeks run Sunday to Saturday. Pairs have no
        corporate actions, so there is no `adjusted`. Bar windows, stamps, and which bars
        `history()` returns follow README.md#bars; each pair's previous day is `previous_day_bar()`.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`, or `dm.ALL`
                (`history()` takes it for one-minute and one-day bars). See README.md#symbols.
            timespan: The bar unit; required. See README.md#timespan-and-multiplier.
            multiplier: Units per bar; only 1 streams.

        Returns:
            A query of the class the timespan picks. Columns: docs/datasets/forex.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `timespan` is `SESSION`, `multiplier` is below 1, or `symbols` is empty,
                None, or holds a pair that is not two three-letter currency codes.

        Example:
            ::

                hourly = m.forex.bars("C:EURUSD", dm.Timespan.HOUR).history(
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

        One row per pair, from one REST request per pair: the 1-day bar of the previous completed
        UTC day. Massive stamps it at 23:59:59.999 UTC, which is moved to the day's start; another
        stamp is served on the UTC day it falls on, with a WARNING (README.md#bars). An explicit
        `mode=dm.PollMode.APPEND` appends each new day's bar. A pair with no bar (HTTP 404) is left
        out, with a WARNING.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`. `dm.ALL` is refused:
                use `m.forex.bars(dm.ALL, dm.Timespan.DAY).history(day, day)`. See
                README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/forex.previous_day_bar.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, or a table.
            ValueError: If `symbols` is `dm.ALL` or None, a pair is not two three-letter currency
                codes, the sequence is empty, or a table has no `Symbol` column.

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

        Each row is one update of Massive's proprietary real-time price estimate, from the `FMV`
        channel of its Business feed. Stream only: no `history()` or `fetch()`.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`, or `dm.ALL`. See
                README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/forex.fmv.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, a
                table has no `Symbol` column, or `symbols` is None.

        Example:
            ::

                fair = m.forex.fmv(["C:EURUSD", "C:USDJPY"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    @raises_through(build_query)
    def snapshots(self, symbols: Symbols) -> Query:
        """The current state of currency pairs: today's bar, the last minute, and the last quote.

        One row per pair: today's and the previous UTC day's bars, the latest minute bar, the last
        quote, and the change since the previous close. Every pair comes in one response.

        Args:
            symbols: Currency pairs as Massive's tickers, such as `C:EURUSD`, or `dm.ALL` for every
                pair Massive tracks. See README.md#symbols.

        Returns:
            A query. Columns: docs/datasets/forex.snapshots.md.

        Raises:
            TypeError: If `symbols` is not a `str`, a sequence of `str`, a table, or `dm.ALL`.
            ValueError: If a pair is not two three-letter currency codes, the sequence is empty, a
                table has no `Symbol` column, or `symbols` is None.

        Example:
            ::

                board = m.forex.snapshots(["C:EURUSD", "C:GBPUSD"]).poll(5)
        """
        return cast("Query", build_query(self._core, SNAPSHOTS, symbols=symbols))

    @raises_through(_require_enum)
    def movers(self, direction: Direction) -> Query:
        """The twenty currency pairs that gained or lost the most since the previous UTC close.

        Rows have the columns of `snapshots()`, ranked by percentage change. Massive clears the list
        daily at midnight Eastern time and refills it as quotes arrive, so it can be empty just
        after. A `poll()` table keeps one row per pair, so pairs come and go as the ranking changes.

        Args:
            direction: `dm.Direction.GAINERS` for the largest increases, or `dm.Direction.LOSERS`.

        Returns:
            A query. Columns: docs/datasets/forex.movers.md.

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

        One row: the converted amount, the amount converted, and the last bid and ask it used.

        Args:
            from_currency: The currency to convert from, a three-letter code such as `"AUD"`,
                written exactly: upper case, nothing around it.
            to_currency: The currency to convert to, such as `"USD"`.
            amount: The amount to convert, a positive, finite `int` or `float`; 1.0 by default.
            precision: The decimal places of the result, 0 to 4; 2 by default.

        Returns:
            A query. Columns: docs/datasets/forex.conversion.md.

        Raises:
            TypeError: If a currency is not a `str`, `amount` is not a number, or `precision` is not
                an `int` (a `bool` is neither).
            ValueError: If a currency is not exactly three upper-case letters (`" usd"` and
                `"usd"` are refused, naming `'USD'`), `amount` is not positive and finite, or
                `precision` is not one the endpoint accepts.

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
