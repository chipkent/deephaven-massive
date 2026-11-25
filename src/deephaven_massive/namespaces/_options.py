"""The `m.options` namespace: US-listed options contracts, as reported by OPRA.

Datasets: `trades`, `last_trade` (each contract's latest trade), `quotes`, `bars`,
`previous_day_bar` (each contract's bar of the previous session), `fmv` (fair market value),
`admin_messages` (OPRA's administrative messages and FLEX trade notices), `chain` (one underlying's
option chain), `snapshots` (the current state of chosen contracts), `contracts` (the contracts
list), `contract_details` (named contracts' reference data), `daily_open_close`, `open_interest`
(each contract's open interest by day), `open_interest_by_underlying` (each underlying's open
interest by day, every contract summed), `greeks` (each contract's end-of-day greeks and implied
volatility), and `unusual_trades` (large trades and sweeps).

Each dataset is one factory method on `OptionsNamespace`. The method validates its arguments and
returns a query; nothing touches the network until a verb is called on the query. This module owns
the namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog
(`CATALOG` in `_session.py`). A spec names the dataset's sources and key columns, never its columns:
those come from the generated wire schemas.

Option symbols are Massive's exact tickers: `O:` and the OCC symbol (`O:AAPL250117C00150000`).
Every method that takes contracts takes only that form (`check_symbol` in
`_datasets/symbols.py`), and `dm.canonical_symbol_option` converts the bare or space-padded OCC
forms. Every options dataset whose rows name a contract also has `OptionRoot`, the OCC root read
from the symbol (`SPXW`, `AAPL1`), or Massive's own `root` where the rows carry one (the open
interest, the greeks, and the unusual trades). The admin messages name none: Massive sends them
per exchange,
so the stream subscribes every exchange (`AD.*`) and `history()` reads every exchange's archive,
the rows have no `Symbol`, and their text is kept as OPRA sends it. The rows of the
chain, the snapshots, and the contract reference data carry the instrument Massive links the
contract to as `Underlying`, a canonical ticker: it names the linked instrument, not the
deliverable, so an adjusted contract (root `AAPL1`) may deliver more or less than 100 shares
of it. Neither column is ever mapped to the other. `underlying=` takes the canonical ticker
too, or a table of them in a `Symbol` column (`value_table`), each read as if passed alone.
The chain takes and writes an index as `I:SPX`, but the contracts list takes and writes it bare
(`SPX`, and no contract for `I:SPX`), so the REST transport sends it bare
(`wire_symbols.contracts_underlying`) and the rows' index underlyings get their `I:` back by
their CFI code (`wire_symbols.index_underlyings`). The open interest, the greeks, and the
unusual trades write it bare with no code, so their rows' `Underlying` gets its `I:` by one
lookup per option root, and the open interest by underlying, whose `Symbol` is the underlying
itself, by one lookup per bare ticker (`DatasetSpec.index_prefix`, `_ingest/option_roots.py`).

One `/options` WebSocket connection allows 1,000 contracts. The stream verbs count topics
against that cap and raise `ValueError` before subscribing a static list that would cross it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, cast

from deephaven.table import Table

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._datasets.symbols import (
    ALL,
    UNDERLYING_CLASSES,
    check_symbol,
    is_canonical,
    symbol_class,
)
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    ClassFromPrefix,
    ContractType,
    IndexPrefix,
    NotBars,
    Order,
    PollMode,
    Timespan,
    _require_enum,
)
from deephaven_massive._error_policy import ErrorPolicy, check_policy
from deephaven_massive._errors import raises_through
from deephaven_massive._massive import Transport
from deephaven_massive.namespaces._namespace import Namespace
from deephaven_massive.namespaces._query_building import (
    SNAPSHOT_SYMBOLS_PER_REQUEST,
    adjustment_notes,
    build_query,
    day_argument,
    list_params,
    list_query,
    one_or_any_of,
    require_bool,
    value_table,
)
from deephaven_massive.queries import (
    FetchQuery,
    HistoryQuery,
    NoSymbolStreamHistoryQuery,
    StreamHistoryQuery,
    StreamQuery,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.filters import FilterValue
    from deephaven_massive._datasets.symbols import SymbolsLike

__all__ = ["DATASETS", "OptionsNamespace"]

logger = logging.getLogger(__name__)

_REST, _WS, _FLAT = Transport.REST, Transport.WS, Transport.FLAT_FILES

_SNAPSHOT_TYPE: Final = "options"
"""The `type` the unified snapshot is sent for `dm.ALL`, so it lists options contracts only.

Tickers never travel with it: Massive answers `type` beside `ticker.any_of` with HTTP 400
("Cannot specify tickers and type."), and each contract's `O:` prefix already names its class.
"""

_LAST_TRADE: Final = "/v2/last/trade/{optionsTicker}"
_TRADES: Final = "/v3/trades/{optionsTicker}"
_QUOTES: Final = "/v3/quotes/{optionsTicker}"
_PREV: Final = "/v2/aggs/ticker/{optionsTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_CHAIN: Final = "/v3/snapshot/options/{underlyingAsset}"
_SNAPSHOT: Final = "/v3/snapshot"
_CONTRACTS: Final = "/v3/reference/options/contracts"
_CONTRACT: Final = "/v3/reference/options/contracts/{options_ticker}"
_OPEN_CLOSE: Final = "/v1/open-close/{optionsTicker}/{date}"
_OPEN_INTEREST: Final = "/options/v1/open-interest"
_OPEN_INTEREST_BY_UNDERLYING: Final = "/options/v1/open-interest/underlying"
_GREEKS: Final = "/options/v1/analytics"
_UNUSUAL_TRADES: Final = "/options/v1/unusual-trades"
_ADMIN_MESSAGES: Final = "/options/v1/admin-messages"

_TRADE_FILES: Final = "us_options_opra/trades_v1"
_QUOTE_FILES: Final = "us_options_opra/quotes_v1"
_MINUTE_FILES: Final = "us_options_opra/minute_aggs_v1"
_DAY_FILES: Final = "us_options_opra/day_aggs_v1"

_AGGREGATES_PAGE: Final = 50_000
"""The aggregates range endpoint's largest `limit`, which counts the base aggregates
behind a page. Its specification states this maximum only in prose, so it is typed here."""


def _keys(symbol: str, time: str | None = None, *, fanout: str | None = None) -> SourceKeys:
    """Build the key columns of one source from the source's own column names.

    Args:
        symbol: The column holding the contract, renamed to `Symbol`.
        time: The column holding the row's time, renamed to `Timestamp`; None for none.
        fanout: The column holding the underlying, renamed to `Underlying`; None for none.

    Returns:
        The source's keys.
    """
    return SourceKeys(
        symbol=KeySpec(symbol),
        time=None if time is None else KeySpec(time),
        fanout=None if fanout is None else KeySpec(fanout),
    )


def _list_keys(time: str | None = None) -> SourceKeys:
    """Build the keys of the open interest or the unusual trades: contract, time, underlying.

    Massive declares `underlying_ticker` optional on both, so a row without one keeps a null
    `Underlying` rather than being withheld.

    Args:
        time: The column holding the row's time, renamed to `Timestamp`; None for none.

    Returns:
        The source's keys.
    """
    return SourceKeys(
        symbol=KeySpec("Ticker"),
        time=None if time is None else KeySpec(time),
        fanout=KeySpec("UnderlyingTicker", required=False),
    )


def _stream_bar_keys() -> SourceKeys:
    """Build the keys of a WebSocket bars channel: its contract, window start, and end `e`.

    Returns:
        The channel's keys.
    """
    return SourceKeys(
        symbol=KeySpec("Ticker"), time=KeySpec("WindowStart"), end=KeySpec("EndTimestamp")
    )


def _from_path(time: str) -> SourceKeys:
    """Build the keys of a source that takes the contract as its `{optionsTicker}` segment.

    Args:
        time: The column holding the row's time, renamed to `Timestamp`.

    Returns:
        The source's keys; `Symbol` is filled from the request.
    """
    return SourceKeys(symbol=KeySpec("optionsTicker", from_request=True), time=KeySpec(time))


TRADES: Final = DatasetSpec(
    name="options.trades",
    summary="Trades of US-listed options contracts (OPRA), tick by tick.",
    asset_class=AssetClass.OPTIONS,
    kind="trades",
    history_path=_TRADES,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: _TRADE_FILES},
    keys={
        (_REST, _TRADES): _from_path("SipTimestamp"),
        (_WS, "options/T"): _keys("Ticker", "SipTimestamp"),
        (_FLAT, _TRADE_FILES): _keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)

# Each one-row dataset joins the kind of its stream and history dataset, so its rows have the
# same columns; an explicit APPEND poll() records each new row by its time.
LAST_TRADE: Final = DatasetSpec(
    name="options.last_trade",
    summary="The latest trade of each US-listed options contract.",
    asset_class=AssetClass.OPTIONS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    serves_current_data=True,
    refused_alternative="options.snapshots",
    keys={(_REST, _LAST_TRADE): _keys("Ticker", "SipTimestamp")},
    per_symbol=True,
    revision_keys=("Timestamp",),
)

QUOTES: Final = DatasetSpec(
    name="options.quotes",
    summary="Quotes of US-listed options contracts (OPRA), tick by tick.",
    asset_class=AssetClass.OPTIONS,
    kind="quotes",
    history_path=_QUOTES,
    channels={NotBars.ANY: "Q"},
    flat_file_prefixes={NotBars.ANY: _QUOTE_FILES},
    keys={
        (_REST, _QUOTES): _from_path("SipTimestamp"),
        (_WS, "options/Q"): _keys("Ticker", "SipTimestamp"),
        (_FLAT, _QUOTE_FILES): _keys("Ticker", "SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
)

BARS: Final = DatasetSpec(
    name="options.bars",
    summary="Aggregate bars (open, high, low, close, volume) of US-listed options contracts.",
    asset_class=AssetClass.OPTIONS,
    is_bars=True,
    kind="bars",
    history_path=_RANGE,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_REST, _RANGE): _from_path("WindowStart"),
        (_WS, "options/A"): _stream_bar_keys(),
        (_WS, "options/AM"): _stream_bar_keys(),
        (_FLAT, _MINUTE_FILES): _keys("Ticker", "WindowStart"),
        (_FLAT, _DAY_FILES): _keys("Ticker", "WindowStart"),
    },
    page_size=_AGGREGATES_PAGE,
    column_notes=adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",)),
)

PREVIOUS_DAY_BAR: Final = DatasetSpec(
    name="options.previous_day_bar",
    summary="The previous trading day's open, high, low, close, and volume of options contracts.",
    asset_class=AssetClass.OPTIONS,
    kind="bars",
    fetch_path=_PREV,
    keys={(_REST, _PREV): _keys("Ticker", "WindowStart")},
    per_symbol=True,
    revision_keys=("TimestampStart",),
    column_notes=adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",)),
)

FMV: Final = DatasetSpec(
    name="options.fmv",
    summary="Massive's fair market value of US-listed options contracts, streamed in real time.",
    asset_class=AssetClass.OPTIONS,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    channel_family=ChannelFamily.FMV,
    keys={(_WS, "options/FMV"): _keys("Ticker", "FmvLastUpdated")},
)

ADMIN_MESSAGES: Final = DatasetSpec(
    name="options.admin_messages",
    summary="OPRA's administrative messages, including FLEX trade notices, live and past.",
    asset_class=AssetClass.OPTIONS,
    kind="admin_messages",
    history_path=_ADMIN_MESSAGES,
    channels={NotBars.ANY: "AD"},
    keys={
        (_REST, _ADMIN_MESSAGES): SourceKeys(time=KeySpec("SipTimestamp")),
        (_WS, "options/AD"): SourceKeys(time=KeySpec("SipTimestamp")),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    # Massive sends the messages per exchange (`AD.<exchange id>`), and no message names a
    # contract outside its free text, which the library never parses.
    names_symbols=False,
    # The archive's own texts, which describe these columns first, link nothing and describe
    # the wire, so each note says what the column holds.
    column_notes={
        "Exchange": "Massive lists the exchange IDs at "
        "https://massive.com/docs/rest/options/market-operations/exchanges.",
        "Retransmission": "Null on an original message.",
    },
)

_SNAPSHOT_REVISION: Final = ("LastQuoteLastUpdated", "LastTradeSipTimestamp")
"""The update times of a contract's snapshot: a new quote or trade is a new row of an explicit
`poll(mode=PollMode.APPEND)`. The snapshot carries no single update time of its own."""

CHAIN: Final = DatasetSpec(
    name="options.chain",
    summary="The snapshot of every contract in one underlying's option chain.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_CHAIN,
    serves_current_data=True,
    keys={(_REST, _CHAIN): _keys("Ticker", fanout="UnderlyingAssetTicker")},
    revision_keys=_SNAPSHOT_REVISION,
)

SNAPSHOTS: Final = DatasetSpec(
    name="options.snapshots",
    summary="The current snapshot of each requested options contract, with its greeks.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_SNAPSHOT,
    serves_current_data=True,
    keys={(_REST, _SNAPSHOT): _keys("Ticker", fanout="UnderlyingAssetTicker")},
    symbol_param="ticker.any_of",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    # Massive refuses `type` beside tickers, so the library sends it for `dm.ALL` only.
    library_params=frozenset({"type"}),
    revision_keys=_SNAPSHOT_REVISION,
)

CONTRACTS: Final = DatasetSpec(
    name="options.contracts",
    summary="Options contracts by underlying: call or put, exercise style, expiration, and strike.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_CONTRACTS,
    keys={(_REST, _CONTRACTS): _keys("Ticker", fanout="UnderlyingTicker")},
    index_prefix=IndexPrefix.FROM_CFI,
    # Massive's page marks `ticker` deprecated here and points to the per-contract endpoint.
    symbol_params_elsewhere={
        "ticker": "To read named contracts, call m.options.contract_details(contracts).fetch()."
    },
)
CONTRACT_DETAILS: Final = DatasetSpec(
    name="options.contract_details",
    summary="The options contracts you name: call or put, exercise style, expiration, and strike.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_CONTRACT,
    # The rows carry their ticker, although the request names it too.
    keys={(_REST, _CONTRACT): _keys("Ticker", fanout="UnderlyingTicker")},
    index_prefix=IndexPrefix.FROM_CFI,
    per_symbol=True,
)

DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="options.daily_open_close",
    summary="The open, high, low, close, and volume of each options contract on one day.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_OPEN_CLOSE,
    keys={(_REST, _OPEN_CLOSE): SourceKeys(symbol=KeySpec("Symbol"), date=KeySpec("From"))},
    # One contract a request, which makes this an instrument dataset: its `Symbol` is required.
    per_symbol=True,
    row_times=RowTimes.DAY,
    column_notes=adjustment_notes(
        ("Open", "High", "Low", "Close", "PreMarket", "AfterHours"), ("Volume",)
    ),
)

_ROOT_UNDERLYING_NOTE: Final = (
    "Massive's `root` is the `OptionRoot` column. An index has its `I:` prefix (`I:SPX`), as "
    "in `m.options.contracts()`. If Massive's "
    "[Contract Overview](https://massive.com/docs/rest/options/contracts/contract-overview) "
    "can't say whether a root is an index, the library leaves its rows out and lists them in "
    "`m.problems()` as `unreadable_symbols`."
)
"""The note on `Underlying` of the datasets whose rows name it bare, with no CFI code."""

OPEN_INTEREST: Final = DatasetSpec(
    name="options.open_interest",
    summary="The open interest of each US-listed options contract at the end of each trading day.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_OPEN_INTEREST,
    keys={(_REST, _OPEN_INTEREST): _list_keys()},
    time_axis=("date.gte", "date.lte"),
    symbol_param="ticker.any_of",
    # A chain's contracts can number thousands, so a table of them is sent 250 to a request,
    # as the snapshots are, which keeps each request's URL short.
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    poll_keys=("Symbol", "Date"),
    index_prefix=IndexPrefix.BY_ROOT,
    column_notes={"Underlying": _ROOT_UNDERLYING_NOTE},
)

_UNDERLYING_SYMBOL_NOTE: Final = (
    "The underlying, with its `I:` prefix for an index (`I:SPX`), as in `m.options.contracts()`. "
    "If Massive's "
    "[All Contracts](https://massive.com/docs/rest/options/contracts/all-contracts) list "
    "can't say whether a ticker is an index, the library leaves its rows out and lists them in "
    "`m.problems()` as `unreadable_symbols`. A ticker that names both a stock and an index "
    "(`DJT`) is classified by the first contract Massive lists for it, while Massive's row may "
    "total the options on both."
)
"""The note on `Symbol` of the dataset whose rows name the bare underlying alone."""

OPEN_INTEREST_BY_UNDERLYING: Final = DatasetSpec(
    name="options.open_interest_by_underlying",
    summary="Each underlying's open interest at the end of each trading day, calls and puts apart.",
    fetch_path=_OPEN_INTEREST_BY_UNDERLYING,
    # The row's symbol is the underlying itself: Massive totals every root on it.
    keys={(_REST, _OPEN_INTEREST_BY_UNDERLYING): SourceKeys(symbol=KeySpec("UnderlyingTicker"))},
    time_axis=("date.gte", "date.lte"),
    symbol_param="underlying_ticker.any_of",
    # A watchlist of underlyings is sent 250 to a request, as the contracts are.
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    poll_keys=("Symbol", "Date"),
    index_prefix=IndexPrefix.BY_UNDERLYING,
    column_notes={"Symbol": _UNDERLYING_SYMBOL_NOTE},
)

GREEKS: Final = DatasetSpec(
    name="options.greeks",
    summary="The end-of-day greeks and implied volatility of each US-listed options contract.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_GREEKS,
    keys={(_REST, _GREEKS): _list_keys()},
    time_axis=("date.gte", "date.lte"),
    symbol_param="ticker.any_of",
    # One row per contract per day, as the open interest: the same 250 contracts a request.
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    poll_keys=("Symbol", "Date"),
    index_prefix=IndexPrefix.BY_ROOT,
    column_notes={
        "Underlying": _ROOT_UNDERLYING_NOTE,
        "OpenInterest": "Massive publishes it the morning of `Date`, for the prior trading "
        "day's close, so it is the value `m.options.open_interest()` dates one trading day "
        "earlier.",
    },
)

UNUSUAL_TRADES: Final = DatasetSpec(
    name="options.unusual_trades",
    summary="Large trades and sweeps of US-listed options contracts, flagged by Massive.",
    asset_class=AssetClass.OPTIONS,
    fetch_path=_UNUSUAL_TRADES,
    keys={(_REST, _UNUSUAL_TRADES): _list_keys("SipTimestamp")},
    time_axis=("sip_timestamp.gte", "sip_timestamp.lt"),
    symbol_param="ticker.any_of",
    symbols_per_request=SNAPSHOT_SYMBOLS_PER_REQUEST,
    # A trade is an event at one instant, so its rows carry `Date` as `options.trades` does.
    row_times=RowTimes.TICK,
    poll_mode=PollMode.APPEND,
    poll_keys=("Symbol", "Timestamp", "SequenceNumber"),
    # Massive serves the latest days from its real-time data, so a poll, or a fetch that
    # reaches the present, reads current trades; a past range reads final ones.
    latest_is_current=True,
    index_prefix=IndexPrefix.BY_ROOT,
    column_notes={"Underlying": _ROOT_UNDERLYING_NOTE},
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    LAST_TRADE,
    QUOTES,
    BARS,
    PREVIOUS_DAY_BAR,
    FMV,
    ADMIN_MESSAGES,
    CHAIN,
    SNAPSHOTS,
    CONTRACTS,
    CONTRACT_DETAILS,
    DAILY_OPEN_CLOSE,
    OPEN_INTEREST,
    OPEN_INTEREST_BY_UNDERLYING,
    GREEKS,
    UNUSUAL_TRADES,
)
"""Every dataset of `m.options`, one `DatasetSpec` each, in the order of the namespace's methods."""


@raises_through(check_symbol)
def _chain_underlying(value: object) -> str:
    """Check the chain's `underlying` argument: Massive's exact ticker of an instrument.

    Massive judges the instrument: it answers HTTP 400 ("Invalid ticker.") to the chain of a
    currency or crypto pair, so any canonical ticker is sent, an option contract included.

    Args:
        value: The `underlying` argument as passed, such as `"AAPL"` or `"I:SPX"`.

    Returns:
        The ticker, unchanged.

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is not canonical (`check_symbol`).
    """
    if not isinstance(value, str):
        kind = "dm.ALL" if value is ALL else type(value).__name__
        raise TypeError(f"underlying must be one ticker as a str, not {kind}")
    return check_symbol(ClassFromPrefix.ANY, value)


_PAIR_CLASSES: Final = frozenset({AssetClass.FOREX, AssetClass.CRYPTO})
"""The classes whose tickers the contracts list answers with no error and no rows."""


@raises_through(_chain_underlying)
def _underlying(value: object) -> str:
    """Check the contracts list's `underlying` argument: any of Massive's tickers but a pair.

    A currency or crypto pair is refused, because Massive answers it with no error: sent
    `underlying_ticker=C:EURUSD` or `X:BTCUSD`, the contracts list answered HTTP 200 with no
    rows, which would read as a pair with no options listed. Every other canonical ticker is
    sent for Massive to judge, an option contract included.

    Args:
        value: The `underlying` argument as passed, such as `"AAPL"`, `"BRK.B"`, or `"I:SPX"`.

    Returns:
        The ticker, unchanged: case is kept (`KIMpL`), and nothing is converted. The REST
        transport sends an index bare to the contracts list (`contracts_underlying`).

    Raises:
        TypeError: If `value` is not a `str`.
        ValueError: If `value` is not canonical, or is a currency or crypto pair.
    """
    ticker = _chain_underlying(value)
    if symbol_class(ticker) in _PAIR_CLASSES:
        raise ValueError(
            f"underlying={value!r} is a currency or crypto pair, which has no listed options. "
            "Pass a stock or index ticker, such as 'AAPL' or 'I:SPX'."
        )
    return ticker


@raises_through(one_or_any_of, _underlying)
def _underlyings(value: object) -> dict[str, object]:
    """Check the `underlying` argument of a dataset that filters by one underlying or several.

    Each value passes `_underlying`'s check. One is sent as `underlying_ticker` and several as
    `underlying_ticker.any_of`; the REST transport sends an index bare in either form
    (`wire_symbols.contracts_underlying`).

    Args:
        value: The argument as passed: one ticker, a sequence of them, or None for every
            underlying.

    Returns:
        The two wire parameters, the unset one None (`one_or_any_of`).

    Raises:
        TypeError: If the value is neither None, a `str`, nor a sequence of `str`.
        ValueError: If the sequence is empty, or a ticker is not canonical or is a currency or
            crypto pair.
    """
    sent = one_or_any_of(
        "underlying_ticker", value, every="every underlying", argument="underlying"
    )
    one, several = sent["underlying_ticker"], sent["underlying_ticker.any_of"]
    if one is not None:
        _underlying(one)
    if several is not None:
        assert isinstance(several, tuple)  # one_or_any_of's form of a sequence
        sent["underlying_ticker.any_of"] = tuple(_underlying(ticker) for ticker in several)
    return sent


@raises_through(_require_enum)
def _contract_type(value: object) -> ContractType | None:
    """Check the optional `contract_type` argument of the chain and the contracts.

    Args:
        value: The argument as passed.

    Returns:
        The member, or None when the argument is None.

    Raises:
        TypeError: If the value is neither None nor a `dm.ContractType` (`_require_enum`).
    """
    return None if value is None else _require_enum(value, ContractType, "contract_type")


def _expired(value: object) -> bool | None:
    """Check the `expired` argument of the contracts and return what is sent.

    `False` is Massive's default, so it is not sent: the request is then the same as one that
    never mentions `expired`.

    Args:
        value: The argument as passed.

    Returns:
        True to send `expired=true`; None for False, which sends nothing.

    Raises:
        TypeError: If the value is not a `bool`.
    """
    if not isinstance(value, bool):
        raise TypeError(f"expired must be a bool, not {type(value).__name__}")
    return True if value else None


class OptionsNamespace(Namespace):
    """The US options datasets, reached as `m.options`.

    Each method returns a query of one dataset. The datasets cover:

    - market data: trades, quotes, bars, and fair market value;
    - each contract's last trade, previous day's bar, and daily open and close;
    - option chains, snapshots, and contract reference data;
    - each contract's open interest and end-of-day greeks by day, each underlying's open
      interest by day, and the unusual trades Massive flags;
    - OPRA's administrative messages.

    A contract's ticker is `O:` followed by its OCC symbol, such as `O:AAPL250117C00150000`. A
    method reads nothing, so it never blocks, and you can call it from any thread. You reach it
    through a session and never construct it. Import it as
    `from deephaven_massive.namespaces import OptionsNamespace` to annotate your own code.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            calls = m.options.chain("AAPL", contract_type=dm.ContractType.CALL).fetch()
            live = m.options.trades(calls.head(10)).append_only()  # ten current contracts
    """

    __slots__ = ()
    _path: ClassVar[str] = "options"

    @raises_through(build_query)
    def trades(
        self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None
    ) -> StreamHistoryQuery:
        """Trades of US-listed options contracts (OPRA), tick by tick.

        Each row holds a trade's price, size, exchange, condition codes, and SIP timestamp.
        `history()` of `dm.ALL` reads a day's flat file. One connection carries at most 1,000
        contracts.

        Args:
            symbols: Option contracts, or `dm.ALL` for every contract.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.trades.md.

        Raises:
            TypeError: If `symbols` is of another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                from datetime import UTC, datetime, timedelta

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                hour = m.options.trades(contract).history(datetime.now(UTC) - timedelta(hours=1))
        """
        return build_query(
            self._core, TRADES, query_type=StreamHistoryQuery, symbols=symbols, on_error=on_error
        )

    @raises_through(build_query)
    def last_trade(
        self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None
    ) -> FetchQuery:
        """The latest trade of each US-listed options contract.

        Each row holds one contract's most recent trade. A contract with no trade is left out, and
        the library logs a WARNING. `snapshots()` also gives each contract's last trade, on more
        plans than this dataset. `poll()` misses any trade between two polls; `trades()` streams
        every one.

        Args:
            symbols: Option contracts. `dm.ALL` is refused; use `chain(underlying)` for every
                contract of an underlying.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.last_trade.md.

        Raises:
            TypeError: If `symbols` is of another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                last = m.options.last_trade(contract).fetch()
        """
        return build_query(
            self._core,
            LAST_TRADE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="For every contract of one underlying, call "
            "m.options.chain('AAPL').fetch().",
            on_error=on_error,
        )

    @raises_through(build_query)
    def quotes(
        self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None
    ) -> StreamHistoryQuery:
        """Quotes of US-listed options contracts (OPRA), tick by tick.

        Each row holds a quote's bid and ask prices, sizes, and exchanges, with its SIP timestamp.
        `snapshots()` gives each contract's latest quote. `history()` of `dm.ALL` reads a day's flat
        file. One connection carries at most 1,000 contracts.

        Args:
            symbols: Option contracts, or `dm.ALL` for every contract.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.quotes.md.

        Raises:
            TypeError: If `symbols` is of another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                book = m.options.quotes(contract).last_by()
        """
        return build_query(
            self._core, QUOTES, query_type=StreamHistoryQuery, symbols=symbols, on_error=on_error
        )

    @raises_through(build_query)
    def bars(
        self,
        symbols: SymbolsLike,
        timespan: Timespan,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
        on_error: ErrorPolicy | None = None,
    ) -> StreamHistoryQuery | HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed options contracts.

        Each row is one contract's bar of `multiplier` times `timespan`. The dataset's page, linked
        below, says how each bar's window and `Date` are set.

        - History is adjusted for splits of the underlying unless you pass `adjusted=False`. Streams
          and flat files serve bars as traded.
        - `history()` of `dm.ALL` reads a day's flat file.

        Args:
            symbols: Option contracts, or `dm.ALL` for every contract.
            timespan: The bar's unit. Massive refuses `dm.Timespan.SESSION`, a futures unit.
            multiplier: Units per bar.
            adjusted: Adjusts for splits of the underlying.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A `StreamHistoryQuery` for 1-second and 1-minute bars at `multiplier=1`, which
            Massive streams, with the stream methods and `history()`; a `HistoryQuery`, with
            `history()` only, for any other bars. A type checker allows a stream method on the
            result only after `isinstance(q, StreamHistoryQuery)`. Its columns, plans, and
            methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.bars.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                from deephaven_massive.queries import StreamHistoryQuery

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                bars = m.options.bars(contract, dm.Timespan.MINUTE)
                assert isinstance(bars, StreamHistoryQuery)  # 1-minute bars stream
                minute = bars.append_only()
        """
        return build_query(
            self._core,
            BARS,
            query_type=HistoryQuery,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            adjusted=require_bool("adjusted", adjusted),
            on_error=on_error,
        )

    @raises_through(build_query)
    def previous_day_bar(
        self, symbols: SymbolsLike, *, adjusted: bool = True, on_error: ErrorPolicy | None = None
    ) -> FetchQuery:
        """The previous trading day's open, high, low, close, and volume of options contracts.

        Each row holds one contract's bar of the previous completed session. A contract with no bar
        is left out, and the library logs a WARNING.

        Args:
            symbols: Option contracts. `dm.ALL` is refused; for every contract's bar of one day, use
                `bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day)`, which reads the
                flat files.
            adjusted: Adjusts for splits of the underlying.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.previous_day_bar.md.

        Raises:
            TypeError: If `adjusted` isn't a `bool`, or `symbols` is of another type, or `on_error`
                isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                previous = m.options.previous_day_bar(contract).fetch()
        """
        return build_query(
            self._core,
            PREVIOUS_DAY_BAR,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "For every contract's bar of one day, call m.options.bars(dm.ALL, "
                "dm.Timespan.DAY, adjusted=False).history('2025-01-02', '2025-01-02')."
            ),
            timespan=Timespan.DAY,
            adjusted=require_bool("adjusted", adjusted),
            on_error=on_error,
        )

    @raises_through(build_query)
    def fmv(self, symbols: SymbolsLike, *, on_error: ErrorPolicy | None = None) -> StreamQuery:
        """Massive's fair market value of US-listed options contracts, streamed in real time.

        Each row holds a contract's fair market value and when Massive computed it. It streams
        only, with no history. One connection carries at most 1,000 contracts.

        Args:
            symbols: Option contracts, or `dm.ALL` for every contract.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.fmv.md.

        Raises:
            TypeError: If `symbols` is of another type, or `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                fair = m.options.fmv(contract).last_by()
        """
        return build_query(
            self._core, FMV, query_type=StreamQuery, symbols=symbols, on_error=on_error
        )

    @raises_through(check_policy)
    def admin_messages(self, *, on_error: ErrorPolicy | None = None) -> NoSymbolStreamHistoryQuery:
        """OPRA's administrative messages, including FLEX trade notices, live and past.

        Each row holds one message from one exchange: the text exactly as OPRA sends it in
        `Message`, the exchange's ID in `Exchange`, and its sequence number and SIP timestamp.
        The library never parses the text, so a FLEX trade's details stay in it, as in
        Massive's sample `TEXT CFLEX SPY LST PM EUR 4 LEGS 1 @ 733.36`.

        The call takes no symbols. The stream methods follow every exchange's messages live.
        `history()` reads every exchange's past messages from Massive's archive. To keep one
        exchange's messages, filter on `Exchange`. The rows have no `Symbol` column, so the
        query has no `last_by()`.

        Args:
            on_error: What a bad row does in this call (`dm.ErrorPolicy`). None, the default,
                takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.admin_messages.md.

        Raises:
            TypeError: If `on_error` isn't a `dm.ErrorPolicy`.

        Example:
            ::

                notices = m.options.admin_messages().append_only()
                flex = notices.where("Message.contains(`FLEX`)")  # as in the CFLEX sample
                past = m.options.admin_messages().history("2026-11-13", "2026-11-13")
        """
        return build_query(
            self._core, ADMIN_MESSAGES, query_type=NoSymbolStreamHistoryQuery, on_error=on_error
        )

    @raises_through(build_query, list_params, _contract_type, _chain_underlying, value_table)
    def chain(
        self,
        underlying: str | Table,
        *,
        contract_type: ContractType | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The snapshot of every contract in one underlying's option chain.

        Each row holds one contract's details, the session's prices, its last quote and trade, its
        greeks, implied volatility, and open interest. `Underlying` is the instrument Massive links
        the contract to, so an adjusted contract, such as one with root `AAPL1`, is listed beside
        the standard ones. `contracts()` tells them apart: a standard contract has
        `SharesPerContract` of 100.

        A table of underlyings is read value by value. A row that isn't a stock or index ticker
        follows the `noncanonical_symbols` cause of `on_error`, which skips and logs it by
        default. A live
        table is followed. In a static table, one failed underlying fails the call.

        Args:
            underlying: The underlying's exact Massive ticker, such as `"AAPL"` or `"I:SPX"`. A
                table of stock or index tickers in a `Symbol` column works too. `dm.ALL` is
                refused; use `snapshots(dm.ALL)` for every contract.
            contract_type: `dm.ContractType.CALL` or `PUT`; leave it out for both.
            sort: `"ticker"`, `"expiration_date"`, or `"strike_price"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A table of underlyings caps every underlying's
                requests instead.
            filters: Further endpoint parameters, such as `{"expiration_date": "2026-12-18"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.chain.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `underlying` is `dm.ALL`, or isn't Massive's exact ticker;
                - a table has no `Symbol` column;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                calls = m.options.chain("AAPL", contract_type=dm.ContractType.CALL).fetch()
        """
        if cast("object", underlying) is ALL:  # the type says str; a console caller may pass it
            # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
            logger.debug("options.chain refused: underlying=dm.ALL")
            raise ValueError(
                "options.chain reads one underlying's chain, so it does not take dm.ALL. For "
                "every contract, call m.options.snapshots(dm.ALL).fetch()."
            )
        values = (
            value_table(
                underlying, argument="underlying", parameter="underlyingAsset", underlying=True
            )
            if isinstance(underlying, Table)
            else None
        )
        params: dict[str, object] = {
            "underlyingAsset": None if values is not None else _chain_underlying(underlying),
            "contract_type": _contract_type(contract_type),
        }
        params.update(list_params(sort=sort, order=order))
        return build_query(
            self._core,
            CHAIN,
            query_type=FetchQuery,
            params=params,
            filters=filters,
            limit=limit,
            values=values,
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
        """The current snapshot of each requested options contract, with its greeks.

        Each row holds one contract's details, the session's prices, and its last quote, trade, and
        minute bar. It also holds the contract's greeks, implied volatility, open interest, and fair
        market value. `dm.ALL` lists every contract Massive has, which means millions of rows.

        Args:
            symbols: Option contracts, or `dm.ALL` for every contract.
            sort: `"ticker"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"ticker_gte": "O:AAPL"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.snapshots.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit;
                - `filters` holds `type`, which the library sets for `dm.ALL`.

        Example:
            ::

                contract = m.options.chain("AAPL", limit=1).fetch().view("Symbol")
                greeks = m.options.snapshots(contract).poll(5)
        """
        # Massive refuses `type` beside tickers, so only `dm.ALL` sends it. The key stays when
        # its value is None, which keeps `type` refused as a filter either way.
        listing = symbols is ALL
        logger.debug(
            "options.snapshots: %s, so type=%s is %s",
            "dm.ALL" if listing else "tickers given",
            _SNAPSHOT_TYPE,
            "sent" if listing else "left out",
        )
        params: dict[str, object] = {"type": _SNAPSHOT_TYPE if listing else None}
        params.update(list_params(sort=sort, order=order))
        return build_query(
            self._core,
            SNAPSHOTS,
            query_type=FetchQuery,
            symbols=symbols,
            params=params,
            filters=filters,
            limit=limit,
            on_error=on_error,
        )

    @raises_through(
        build_query, list_params, day_argument, _contract_type, _expired, _underlying, value_table
    )
    def contracts(
        self,
        underlying: str | Table | None = None,
        *,
        contract_type: ContractType | None = None,
        as_of: DateLike | None = None,
        expired: bool = False,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """Options contracts by underlying: call or put, exercise style, expiration, and strike.

        Lists the contracts of one underlying, or of every underlying, so you can find their
        tickers. To read contracts you can already name, use `contract_details()`.

        Args:
            underlying: Massive's exact ticker of the underlying, such as `"AAPL"` or `"I:SPX"`.
                A table of stock or index tickers in a `Symbol` column lists each one's
                contracts. With a live table, add an underlying and its contracts appear. Leave
                it out for every underlying.
            contract_type: `dm.ContractType.CALL` or `PUT`; leave it out for both.
            as_of: The day whose contracts to list; leave it out for today.
            expired: True asks Massive for expired contracts.
            sort: `"ticker"`, `"underlying_ticker"`, `"expiration_date"`, or `"strike_price"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A table of underlyings caps every
                underlying's rows separately instead.
            filters: Further endpoint parameters, such as `{"strike_price_gte": 100}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.contracts.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `underlying` isn't Massive's exact ticker;
                - `underlying` is a currency or crypto pair, such as `"C:EURUSD"`;
                - a table has no `Symbol` column;
                - `as_of` is a `datetime` or isn't a date;
                - `limit` isn't positive;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                listed = m.options.contracts("NVDA").fetch()
        """
        values = (
            value_table(
                underlying, argument="underlying", parameter="underlying_ticker", underlying=True
            )
            if isinstance(underlying, Table)
            else None
        )
        params: dict[str, object] = {
            "underlying_ticker": None
            if underlying is None or values is not None
            else _underlying(underlying),
            "contract_type": _contract_type(contract_type),
            "as_of": None if as_of is None else day_argument(CONTRACTS, as_of, "as_of"),
            "expired": _expired(expired),
        }
        params.update(list_params(sort=sort, order=order))
        return build_query(
            self._core,
            CONTRACTS,
            query_type=FetchQuery,
            params=params,
            filters=filters,
            limit=limit,
            values=values,
            on_error=on_error,
        )

    @raises_through(build_query, day_argument)
    def contract_details(
        self,
        symbols: SymbolsLike,
        *,
        as_of: DateLike | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The options contracts you name: call or put, exercise style, expiration, and strike.

        Reads each contract you name into the same columns as `contracts()`. Each contract is one
        request. To find a contract's ticker first, list its underlying's contracts with
        `contracts()`.

        By default, a contract Massive has no data for gets no row, and the log names it.

        Args:
            symbols: Option contracts to describe.
            as_of: Reads each contract as it stood on this day; leave it out for today.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.contract_details.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is None, empty, or `dm.ALL`;
                - a ticker isn't Massive's exact ticker of an option contract, such as
                  `"O:AAPL250117C00150000"`;
                - a table has no `Symbol` column;
                - `as_of` is a `datetime` or isn't a date.

        Example:
            ::

                details = m.options.contract_details(
                    "O:AAPL250117C00150000", as_of="2025-01-02"
                ).fetch()
        """
        if (
            isinstance(symbols, str)
            and symbol_class(symbols) in UNDERLYING_CLASSES
            and is_canonical(ClassFromPrefix.ANY, symbols)
        ):
            # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
            logger.debug("options.contract_details refused: symbols=%r is an underlying", symbols)
            raise ValueError(
                f"{symbols!r} is an underlying, not an options contract. To list its contracts, "
                f"call m.options.contracts({symbols!r}).fetch()."
            )
        return build_query(
            self._core,
            CONTRACT_DETAILS,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative="To list contracts, call m.options.contracts().fetch().",
            params={
                "as_of": None if as_of is None else day_argument(CONTRACT_DETAILS, as_of, "as_of")
            },
            on_error=on_error,
        )

    @raises_through(build_query, day_argument)
    def daily_open_close(
        self,
        symbols: SymbolsLike,
        date: DateLike,
        *,
        adjusted: bool = True,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The open, high, low, close, and volume of each options contract on one day.

        Each row holds one contract's day, with its pre-market and after-hours prices. Massive
        doesn't say here whether the prices are adjusted, so the library can't check `adjusted`.

        Args:
            symbols: Option contracts. `dm.ALL` is refused; for every contract's bar of one day, use
                `bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(day, day)`, which reads the
                flat files.
            date: The trading day, as a date; a `datetime` raises.
            adjusted: Adjusts for splits of the underlying.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.daily_open_close.md.

        Raises:
            TypeError: If `symbols` or `date` is of another type, or `adjusted` isn't a `bool`, or
                `on_error` isn't a `dm.ErrorPolicy`.
            ValueError: If:
                - `symbols` is `dm.ALL`, None, or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a table has no `Symbol` column;
                - `date` is a `datetime` or isn't a date.

        Example:
            ::

                day = m.options.daily_open_close("O:AAPL250117C00150000", "2025-01-02").fetch()
        """
        return build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            query_type=FetchQuery,
            symbols=symbols,
            all_alternative=(
                "For every contract's bar of one day, call m.options.bars(dm.ALL, "
                "dm.Timespan.DAY, adjusted=False).history('2025-01-02', '2025-01-02')."
            ),
            adjusted=require_bool("adjusted", adjusted),
            params={"date": day_argument(DAILY_OPEN_CLOSE, date, "date")},
            on_error=on_error,
        )

    @raises_through(list_query, one_or_any_of, _contract_type)
    def open_interest(
        self,
        symbols: SymbolsLike | None = None,
        *,
        root: str | Sequence[str] | None = None,
        contract_type: ContractType | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The open interest of each US-listed options contract at the end of each trading day.

        Each row holds one contract's open interest on one trading day, as OPRA reports it, with
        the contract's type, expiration, strike, OCC root, and underlying. Massive publishes a
        day's figures the next morning, so the latest `Date` is the previous trading day. Leave
        out `symbols` and narrow by `root`, `contract_type`, or `filters` to read many contracts.

        `Underlying` is Massive's ticker, with `I:` for an index (`I:SPX`), as in
        `m.options.contracts()`. To tell which roots are on an index, a session reads Massive's
        list of index tickers once. A root whose underlying isn't on that list keeps its bare
        ticker and costs nothing more. A root whose underlying is on it, such as `SPX`, costs one
        request to Massive's Contract Overview the first time the session meets that root. If
        Massive can't say, the library leaves the root's rows out and lists them in
        `m.problems()` as `unreadable_symbols`.

        Args:
            symbols: Option contracts; leave it out for every contract.
            root: An OCC root, such as `"SPXW"`, or several; leave it out for every root.
            contract_type: `dm.ContractType.CALL` or `PUT`; leave it out for both.
            start: The first trading day.
            end: The last trading day.
            sort: Any of Massive's field names, such as `"open_interest"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"expiration_date": "2026-12-18"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.open_interest.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - `root` is an empty sequence;
                - a bound isn't a date, or `end` is before `start`;
                - `limit` isn't positive;
                - `order` is given without `sort`, or with a `sort` that has its own `.asc` or
                  `.desc`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                weeklies = m.options.open_interest(root="SPXW", start="2026-11-13").fetch()
        """
        return list_query(
            self._core,
            OPEN_INTEREST,
            all_alternative="Leave symbols out to read every contract.",
            symbols=symbols,
            start=start,
            end=end,
            params={
                **one_or_any_of("root", root, every="every root"),
                "contract_type": _contract_type(contract_type),
            },
            arguments={"root.any_of": "root"},
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
            on_error=on_error,
        )

    @raises_through(list_query)
    def open_interest_by_underlying(
        self,
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """Each underlying's open interest at the end of each trading day, calls and puts apart.

        Each row holds one underlying's open interest on one trading day, summed over every
        contract on it, whatever its root (`SPX` and `SPXW` alike), with the calls' and the
        puts' shares apart. `Symbol` is the underlying, a stock, ETF, or index ticker. Massive
        publishes a day's figures the next morning, so the latest `Date` is the previous
        trading day. Leave out `symbols` to read every underlying.

        `Symbol` has `I:` for an index (`I:SPX`), as in `m.options.contracts()`. To tell which
        tickers are indices, a session reads Massive's list of index tickers once. A ticker not
        on that list costs nothing more. A ticker on it, such as `SPX`, costs one request to
        Massive's contracts list the first time the session meets it. If Massive can't say, the
        library leaves the ticker's rows out and lists them in `m.problems()` as
        `unreadable_symbols`. A ticker that names both a stock and an index, such as `DJT`,
        takes the class of the first contract Massive lists for it, while Massive's row may
        total the options on both.

        Args:
            symbols: Stock, ETF, or index tickers, such as `"AAPL"` or `"I:SPX"`; leave it
                out for every underlying.
            start: The first trading day.
            end: The last trading day.
            sort: Any of Massive's field names, such as `"open_interest"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"underlying_ticker_gte": "A"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.open_interest_by_underlying.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker of a stock or an index, such as
                  `"C:EURUSD"`;
                - a bound isn't a date, or `end` is before `start`;
                - `limit` isn't positive;
                - `order` is given without `sort`, or with a `sort` that has its own `.asc` or
                  `.desc`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                positioning = m.options.open_interest_by_underlying(
                    ["SPY", "I:SPX"], start="2026-11-13"
                ).fetch()
        """
        return list_query(
            self._core,
            OPEN_INTEREST_BY_UNDERLYING,
            all_alternative="Leave symbols out to read every underlying.",
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
            on_error=on_error,
        )

    @raises_through(list_query, _underlyings, _contract_type)
    def greeks(
        self,
        symbols: SymbolsLike | None = None,
        *,
        underlying: str | Sequence[str] | None = None,
        contract_type: ContractType | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """The end-of-day greeks and implied volatility of each US-listed options contract.

        Each row holds one contract's delta, gamma, theta, vega, rho, and implied volatility
        for one trading day, with the inputs Massive's solver used: the NBBO bid, ask, and
        midpoint 15 minutes before the close (`OptionPrice`, at `QuoteTimestamp`), the
        underlying's price then (`UnderlyingPrice`, at `UnderlyingTimestamp`), the dividend
        yield, the risk-free rate, the pricing model, and the exercise style, beside the day's
        volume and the open interest published that morning. Massive takes the market's state at
        15:45 ET (12:45 ET on an early close), so a day's rows arrive after its close, and a
        contract Massive can't price keeps its row with null greeks. Leave out `symbols` and
        narrow by `underlying`, `contract_type`, or `filters` to read many contracts.

        `Underlying` is Massive's ticker, with `I:` for an index (`I:SPX`), as in
        `m.options.contracts()`. To tell which roots are on an index, a session reads Massive's
        list of index tickers once. A root whose underlying isn't on that list keeps its bare
        ticker and costs nothing more. A root whose underlying is on it, such as `SPX`, costs one
        request to Massive's Contract Overview the first time the session meets that root. If
        Massive can't say, the library leaves the root's rows out and lists them in
        `m.problems()` as `unreadable_symbols`.

        Args:
            symbols: Option contracts; leave it out for every contract.
            underlying: The underlying's exact Massive ticker, such as `"AAPL"` or `"I:SPX"`, or
                several; leave it out for every underlying.
            contract_type: `dm.ContractType.CALL` or `PUT`; leave it out for both.
            start: The first trading day.
            end: The last trading day.
            sort: Any of Massive's field names, such as `"implied_volatility"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters, such as `{"expiration_date": "2026-12-18"}`.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.greeks.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - `underlying` is an empty sequence, isn't Massive's exact ticker, or is a
                  currency or crypto pair, such as `"C:EURUSD"`;
                - a bound isn't a date, or `end` is before `start`;
                - `limit` isn't positive;
                - `order` is given without `sort`, or with a `sort` that has its own `.asc` or
                  `.desc`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                surface = m.options.greeks(
                    underlying="AMD", start="2026-10-29", end="2026-10-29"
                ).fetch()
        """
        return list_query(
            self._core,
            GREEKS,
            all_alternative="Leave symbols out to read every contract.",
            symbols=symbols,
            start=start,
            end=end,
            params={**_underlyings(underlying), "contract_type": _contract_type(contract_type)},
            arguments={
                "underlying_ticker": "underlying",
                "underlying_ticker.any_of": "underlying",
            },
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
            on_error=on_error,
        )

    @raises_through(list_query)
    def unusual_trades(
        self,
        symbols: SymbolsLike | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
        on_error: ErrorPolicy | None = None,
    ) -> FetchQuery:
        """Large trades and sweeps of US-listed options contracts, flagged by Massive.

        Each row is one trade that meets at least one of Massive's unusual-activity cases.
        `Flags` names every case the trade meets:

        - `block_premium`: a notional premium (price x size x 100) of $100,000 or more;
        - `block_size`: 1,000 contracts or more;
        - `iso`: an intermarket sweep.

        Massive leaves out cancels, late reports, and compression trades. `poll()` appends each
        new trade.

        `Underlying` is Massive's ticker, with `I:` for an index (`I:SPX`), as in
        `m.options.contracts()`. To tell which roots are on an index, a session reads Massive's
        list of index tickers once. A root whose underlying isn't on that list keeps its bare
        ticker and costs nothing more. A root whose underlying is on it, such as `SPX`, costs one
        request to Massive's Contract Overview the first time the session meets that root. If
        Massive can't say, the library leaves the root's rows out and lists them in
        `m.problems()` as `unreadable_symbols`.

        Args:
            symbols: Option contracts; leave it out for every contract.
            start: The first instant, or the first day.
            end: The instant after the last, or the last day.
            sort: Any of Massive's field names, such as `"size"`.
            order: The direction of `sort`.
            limit: The most rows to return, in total. A watchlist's `fetch()` caps every request
                instead.
            filters: Further endpoint parameters. The range
                `{"ticker_gte": "O:SPY261218", "ticker_lt": "O:SPY261219"}` holds SPY's contracts
                that expire on 2026-12-18.
            on_error: What a bad symbol or row does in this call (`dm.ErrorPolicy`). None,
                the default, takes each cause from the session's `MassiveConfig.on_error`.

        Returns:
            A query. Its columns, plans, and methods:
            https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.unusual_trades.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If:
                - `symbols` is `dm.ALL` or empty;
                - a ticker isn't Massive's exact ticker of an option contract;
                - a bound isn't a date or an aware datetime, the bounds mix the two, or `end` is
                  before `start`;
                - `limit` isn't positive;
                - `order` is given without `sort`, or with a `sort` that has its own `.asc` or
                  `.desc`;
                - a filter isn't one the dataset takes, or its value doesn't fit.

        Example:
            ::

                unusual = m.options.unusual_trades(limit=100).poll(60)
        """
        return list_query(
            self._core,
            UNUSUAL_TRADES,
            all_alternative="Leave symbols out to read every contract.",
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
            on_error=on_error,
        )
