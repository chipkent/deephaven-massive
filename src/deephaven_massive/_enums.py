"""The closed categorical sets of the public API, as `StrEnum`s, and their runtime check.

Every closed set of values that appears in a public signature is an enum defined here. Each member's
value is the exact string Massive puts on the wire, so a member can be sent to the server unchanged:
`Timespan.MINUTE == "minute"`. Signatures accept only the enum, never a plain string, which gives
one spelling per value, tab completion in every IDE and in the Deephaven web console, and typo
safety at type-check time. At run time `_require_enum` turns a plain string into a `TypeError` that
names the member to use; `Timespan("minute")` is the documented way to convert text.
An enum of a library setting that never reaches the wire (`PollMode`, `HistorySource`,
`Reread`, `SymbolConvention`, `OnError`, `ConnectionTaken`, `Subscription`) takes its member
names in lower case as values.

Four further enums are internal and not exported: `Recency` (how fresh a WebSocket host's
data is) and `ChannelFamily` (which host a channel is served from), which appear as values in
the diagnostics tables, beside the wire package's `Transport`, which names WebSocket, REST, or
flat files once for both sides of the wire boundary; `ClassFromPrefix`, a plain `Enum`
(not a `StrEnum`, so it never equals an `AssetClass` value), whose one member `ANY` is the
class of a dataset whose symbols span classes, each read from its prefix; and `NotBars`, a
plain `Enum` too (so it never equals a `Timespan` value), whose one member `ANY` keys the one
WebSocket channel and flat-file prefix of a dataset that is not bars.

The twenty public enums are re-exported by `deephaven_massive/__init__.py`; this module is
private. `subscription_of` maps an asset class to the subscription that covers it.
"""

from __future__ import annotations

from enum import Enum, StrEnum
from typing import TypeVar, overload

__all__ = [
    "AssetClass",
    "ChannelFamily",
    "ClassFromPrefix",
    "ConditionDataType",
    "ConnectionTaken",
    "ContractType",
    "Direction",
    "FuturesVenue",
    "HistorySource",
    "Locale",
    "Market",
    "NotBars",
    "OnError",
    "Order",
    "PollMode",
    "Recency",
    "Reread",
    "SeriesType",
    "Sip",
    "Subscription",
    "SymbolConvention",
    "TickerType",
    "Timeframe",
    "Timespan",
    "subscription_of",
]


_E = TypeVar("_E", bound=StrEnum)


class AssetClass(StrEnum):
    """An asset class, such as stocks or crypto.

    Pin a streaming host with it in `MassiveConfig.websocket_hosts`, and filter the reference
    datasets `m.reference.ticker_types`, `m.reference.exchanges`, and `m.reference.conditions` by
    it. The diagnostics tables show it too.

    Example:
        ::

            import deephaven_massive as dm

            config = dm.MassiveConfig(websocket_hosts={dm.AssetClass.STOCKS: "socket.massive.com"})
    """

    STOCKS = "stocks"
    OPTIONS = "options"
    FUTURES = "futures"
    INDICES = "indices"
    FOREX = "forex"
    CRYPTO = "crypto"


class ClassFromPrefix(Enum):
    """The class of a dataset whose symbols span asset classes: each symbol's prefix names it.

    A few datasets take symbols of every class at once: the indicators, the reference data,
    and the unified `m.snapshots()`. They have no one `AssetClass`, so their `DatasetSpec`
    holds `ClassFromPrefix.ANY`, and every symbol rule given it reads each symbol's class from
    its prefix (`O:`, `I:`, `C:`, `X:`, and none for a stock); their days are New York days.
    Internal: no public signature takes it.
    """

    ANY = "any"
    """Each symbol's class is read from its prefix."""


class Timespan(StrEnum):
    """The unit of a bar or an indicator window.

    For `bars()`, the timespan also decides whether the bars stream: one-second and one-minute bars
    (`SECOND` and `MINUTE` with `multiplier=1`) stream, and every other bar is history only.
    `SESSION` is a futures trading session, and Massive serves it only for futures bars. A
    futures `DAY` bar also spans one session. The previous day's bar is a dataset of its own,
    `previous_day_bar()`.

    Example:
        ::

            import deephaven_massive as dm

            span = dm.Timespan("minute")  # text converts to a member
            assert span is dm.Timespan.MINUTE
    """

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
    SESSION = "session"


class NotBars(Enum):
    """The timespan key of a dataset that is not bars: its one channel or flat-file prefix.

    A dataset's WebSocket channels and flat-file prefixes are a mapping from the bar timespan
    they serve. A `bars()` dataset keys them by `Timespan`; every other dataset (trades,
    quotes, values, and the like) has at most one of each, serves every request with it, and
    keys it by `NotBars.ANY`. A request without a timespan selects that key through
    `catalog.timespan_key`. Internal: no public signature takes it.
    """

    ANY = "any"
    """Every request of the dataset, which has no timespan."""


class PollMode(StrEnum):
    """How a `poll()` table combines one poll's rows with the last.

    - `AUTO`, the default of `poll()`: the mode the dataset's page names. It's `APPEND` for the news
      and filings feeds, `UPSERT` for IPOs, and `REPLACE` for the rest.
    - `REPLACE`: a keyed table that holds exactly the latest poll's rows. A key the poll doesn't
      return is deleted, even when the poll returns no rows.
    - `UPSERT`: a keyed table that adds and updates rows but never deletes them.
    - `APPEND`: an append-only table that adds only rows it hasn't seen. The dataset's keys identify
      a row, such as a news article's id. A snapshot's update time is part of its key, so `APPEND`
      on a snapshot records each new state of a symbol. A later revision that changes none of the
      key columns isn't added. `UPSERT` follows revisions.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.PollMode("append") is dm.PollMode.APPEND
    """

    AUTO = "auto"
    REPLACE = "replace"
    UPSERT = "upsert"
    APPEND = "append"


class HistorySource(StrEnum):
    """Where `history()` reads from.

    - `AUTO`: the library picks flat files or REST. The choice depends on whether you set S3 keys,
      the dataset, the bars and `adjusted`, and how many symbols you ask for. When REST can serve
      them, it reads from REST any day whose file is missing or isn't in your plan.
    - `REST`: Massive's REST API.
    - `FLAT_FILES`: Massive's flat files, the daily CSV files it publishes on S3. They need S3 keys.
      Each file holds a whole day of data, and a read downloads all of it, however few
      symbols or short a range you ask for.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.HistorySource("flat_files") is dm.HistorySource.FLAT_FILES
    """

    AUTO = "auto"
    REST = "rest"
    FLAT_FILES = "flat_files"


class Reread(StrEnum):
    """Whether `history()` of a watchlist downloads a symbol again when you add it back.

    A `history()` of a watchlist reads each symbol you add over the call's range. It drops each
    symbol you delete. This setting decides what happens when a deleted symbol comes back. It has no
    effect when `symbols` isn't a live table.

    - `ALWAYS`, the default: the symbol is downloaded again and shows Massive's current data. It has
      no rows until the download lands.
    - `NEVER`: the symbol gets its earlier rows back at once, with no request. The result keeps
      every download in memory for as long as it lives, and `ReadAt` in `m.symbol_reads()` says when
      each was made.

    A symbol whose download failed is downloaded again when it comes back, under either member.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Reread("never") is dm.Reread.NEVER
    """

    ALWAYS = "always"
    NEVER = "never"


class FuturesVenue(StrEnum):
    """A futures exchange, which picks the flat files a futures `history()` reads.

    A futures dataset's `venues` argument takes a set of members. Left out, a read takes every
    exchange the plan allows and skips each one Massive refuses with a warning; the
    `refused_venues` cause of `dm.ErrorPolicy` says what happens when Massive refuses an
    exchange the call names. Both apply only to flat files: streams cover every exchange.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.FuturesVenue("cme") is dm.FuturesVenue.CME
    """

    CME = "cme"
    CBOT = "cbot"
    NYMEX = "nymex"
    COMEX = "comex"


class Direction(StrEnum):
    """Which side of the market `movers()` returns.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            gainers = m.crypto.movers(dm.Direction.GAINERS).poll(60)
            assert dm.Direction("losers") is dm.Direction.LOSERS
    """

    GAINERS = "gainers"
    LOSERS = "losers"


class ContractType(StrEnum):
    """The type of an options contract, for the `contract_type` argument of the options datasets.

    The chain, the contracts, and the open interest filter by it. Its values are the
    `contract_type` values those endpoints accept: calls and puts.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            puts = m.options.chain("AAPL", contract_type=dm.ContractType.PUT).fetch()
            assert dm.ContractType("call") is dm.ContractType.CALL
    """

    CALL = "call"
    PUT = "put"


class SeriesType(StrEnum):
    """The bar price an indicator is computed from.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.SeriesType("close") is dm.SeriesType.CLOSE
    """

    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"


class Timeframe(StrEnum):
    """The reporting period of the `m.stocks.financials` statements.

    `ANNUAL` is a fiscal year and `QUARTERLY` a fiscal quarter. `TRAILING_TWELVE_MONTHS` sums the
    four most recent quarters. The income and cash flow statements offer it, and a balance sheet,
    which is a snapshot at the period's end, doesn't.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Timeframe("annual") is dm.Timeframe.ANNUAL
    """

    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    TRAILING_TWELVE_MONTHS = "trailing_twelve_months"


class Order(StrEnum):
    """The sort direction of a REST list endpoint (the `order` parameter).

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Order("desc") is dm.Order.DESC
    """

    ASC = "asc"
    DESC = "desc"


class Market(StrEnum):
    """The market `m.reference.tickers` filters by.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Market("otc") is dm.Market.OTC
    """

    STOCKS = "stocks"
    OTC = "otc"
    INDICES = "indices"
    FOREX = "fx"
    CRYPTO = "crypto"


class TickerType(StrEnum):
    """The type code of a ticker, which `m.reference.tickers` filters by.

    The values are Massive's codes, upper case as Massive spells them; `m.reference.ticker_types()`
    lists them with their descriptions.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.TickerType("ETF") is dm.TickerType.ETF
    """

    CS = "CS"  # common stock
    ADRC = "ADRC"  # American depositary receipt, common
    ADRP = "ADRP"  # American depositary receipt, preferred
    ADRR = "ADRR"  # American depositary receipt, rights
    UNIT = "UNIT"  # unit
    RIGHT = "RIGHT"  # rights
    PFD = "PFD"  # preferred stock
    FUND = "FUND"  # fund
    SP = "SP"  # structured product
    WARRANT = "WARRANT"  # warrant
    INDEX = "INDEX"  # index
    ETF = "ETF"  # exchange-traded fund
    ETN = "ETN"  # exchange-traded note
    OS = "OS"  # ordinary shares
    GDR = "GDR"  # global depositary receipt
    OTHER = "OTHER"  # other security type
    NYRS = "NYRS"  # New York registry shares
    AGEN = "AGEN"  # agency bond
    EQLK = "EQLK"  # equity-linked bond
    BOND = "BOND"  # corporate bond
    ADRW = "ADRW"  # American depositary receipt, warrants
    BASKET = "BASKET"  # basket
    LT = "LT"  # liquidating trust


class Locale(StrEnum):
    """The locale the reference endpoints filter by.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Locale("us") is dm.Locale.US
    """

    US = "us"
    GLOBAL = "global"


class ConditionDataType(StrEnum):
    """The kind of tick a condition code applies to (`m.reference.conditions`).

    Example:
        ::

            import deephaven_massive as dm

            assert dm.ConditionDataType("trade") is dm.ConditionDataType.TRADE
    """

    TRADE = "trade"
    BBO = "bbo"
    NBBO = "nbbo"


class Sip(StrEnum):
    """The securities information processor a condition code belongs to.

    The values are upper case, as Massive spells them.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Sip("OPRA") is dm.Sip.OPRA
    """

    CTA = "CTA"
    UTP = "UTP"
    OPRA = "OPRA"


class SymbolConvention(StrEnum):
    """The convention a symbol is written in, which `canonical_symbol` converts from.

    Massive writes a share class as `.B` (`BRK.B`) and a preferred as a lower-case `p` and its
    series (`KIMpL`), as the CTA's CQS convention does, so CQS text needs no conversion. The
    other members name the exact grammar of one other US stock convention; only plain
    tickers, share classes, and preferreds convert, because Massive documents no spelling for
    warrants, units, rights, or when-issued shares.

    - `MASSIVE`: Massive's own spelling, the default. A canonical ticker comes back unchanged;
      a forex, crypto, index, or option symbol in another unambiguous spelling of its class
      converts (`EUR/USD` to `C:EURUSD`, `SPX` to `I:SPX` for the indices class); a stock
      ticker in another convention's spelling raises, since `-` means a class to Yahoo and a
      preferred to Nasdaq. It is the only member every class takes.
    - `YAHOO`: Yahoo Finance; a class is `-B` (`BRK-B`), a preferred `-P` and its series
      (`BAC-PL`).
    - `NASDAQ`: the Nasdaq Integrated Platform symbology (the "NASDAQ Symbol" column of
      `nasdaqtraded.txt`); a class is `.B`, a preferred `-` and its optional series (`KIM-L`).
    - `CMS`: the root, a space, and a suffix, as many broker and FIX systems write it; a class
      is ` B` (`BRK B`), a preferred ` PR` and its optional series (`BAC PRL`).

    `YAHOO`, `NASDAQ`, and `CMS` apply to stocks only. Each names a source of symbols, not an
    exchange: pick the one the tickers came from, not where the stock trades. `NASDAQ` reads
    `BRK-B` as a preferred, `BRKpB`, whatever exchange lists Berkshire.

    Example:
        ::

            import deephaven_massive as dm

            dm.canonical_symbol(
                "BAC-PL", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
            )  # 'BACpL'
    """

    MASSIVE = "massive"
    YAHOO = "yahoo"
    NASDAQ = "nasdaq"
    CMS = "cms"


class OnError(StrEnum):
    """What a problem does: fail, or keep going with or without a log record.

    One bad symbol or row never stops a table by default. The members:

    - `RAISE`: fail whatever met the problem. A check at the call raises, and so does a call
      with `wait=True`. A live table fails. A watchlist read by `fetch()` or `history()` fails
      only the symbols it met, and `m.symbol_reads()` shows them `FAILED`. A bad watchlist row
      under `noncanonical_symbols` is the exception: it fails the whole result.
    - `LOG`: skip the value and keep going. `m.problems()` lists each problem, and the first
      of each cause on each table logs one WARNING that points there; the rest log at DEBUG.
      A row of Massive's data the library can't read whole is never published, under any
      setting.
    - `IGNORE`: keep going as `LOG` does, with nothing logged above DEBUG.

    Each cause has its own setting in `dm.ErrorPolicy`, set for the session
    (`MassiveConfig.on_error`) or for one call (a dataset method's `on_error=`); its docstring
    lists the causes. The symbol converters take a member directly: the `on_error` argument of
    `canonicalize_symbols` (default `LOG`), and of `canonical_symbol` and the per-class
    converters (default `RAISE`, where `LOG` and `IGNORE` return None), for a value that
    doesn't convert.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.OnError("log") is dm.OnError.LOG
            strict = dm.MassiveConfig(
                on_error=dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE)
            )
    """

    RAISE = "raise"
    LOG = "log"
    IGNORE = "ignore"


class ConnectionTaken(StrEnum):
    """What a live table does when another connection with the same API key takes over its own.

    Massive allows your account one WebSocket connection per asset class. When another program
    connects with the same key, Massive keeps the newer connection and closes the older one.
    Another session on this server does the same when its streaming settings, such as
    `max_outage`, differ from this one's. `MassiveConfig.connection_taken`
    picks what the older connection's live tables do:

    - `FAIL`, the default: they fail at once, with a `MassiveError` that names the time and the
      fix.
    - `RECONNECT`: they reconnect after `reconnect_backoff` and take the connection back.
      Meanwhile, their `m.streams()` row's `Error` says when the connection was taken and when it
      is taken back. They miss the data of each gap, and if the other program does the same, the
      two take turns indefinitely.

    Sessions with the same key and streaming settings share one connection whatever this
    setting, and each table follows its own session's setting.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.ConnectionTaken("reconnect") is dm.ConnectionTaken.RECONNECT
            config = dm.MassiveConfig(connection_taken=dm.ConnectionTaken.RECONNECT)
    """

    FAIL = "fail"
    RECONNECT = "reconnect"


class Subscription(StrEnum):
    """One of Massive's five subscriptions, each with its own plan.

    Massive sells its data as five subscriptions, one per asset class, with forex and crypto
    sharing `CURRENCIES`. A subscription's plan may limit how many REST requests a minute it
    makes. All the API keys of your account share the limits.

    Give each limited subscription's limit in `MassiveConfig.rest_requests_per_minute`, so its
    requests keep to it from the first one. Without it, the library slows down after Massive's
    first rate-limit error. `m.rate_limits()` shows each subscription's state. Datasets Massive
    assigns to no subscription share a limit of their own, `other`. Reference, economy, and
    partner data are among them. The library learns that limit from Massive's rate-limit errors.

    Example:
        ::

            import deephaven_massive as dm

            # Options and currencies (forex and crypto) limited to 5 requests a minute.
            config = dm.MassiveConfig(
                rest_requests_per_minute={dm.Subscription.OPTIONS: 5, dm.Subscription.CURRENCIES: 5}
            )
    """

    STOCKS = "stocks"
    OPTIONS = "options"
    INDICES = "indices"
    CURRENCIES = "currencies"
    FUTURES = "futures"


_SUBSCRIPTIONS: dict[AssetClass, Subscription] = {
    AssetClass.STOCKS: Subscription.STOCKS,
    AssetClass.OPTIONS: Subscription.OPTIONS,
    AssetClass.INDICES: Subscription.INDICES,
    AssetClass.FOREX: Subscription.CURRENCIES,
    AssetClass.CRYPTO: Subscription.CURRENCIES,
    AssetClass.FUTURES: Subscription.FUTURES,
}


@overload
def subscription_of(asset_class: AssetClass) -> Subscription: ...
@overload
def subscription_of(asset_class: AssetClass | ClassFromPrefix) -> Subscription | None: ...
def subscription_of(asset_class: AssetClass | ClassFromPrefix) -> Subscription | None:
    """Return the subscription that covers an asset class's data.

    Args:
        asset_class: The class, or `ClassFromPrefix.ANY` for data of no single class.

    Returns:
        The class's subscription (`CURRENCIES` for forex and crypto), or None for
        `ClassFromPrefix.ANY`: data of no single class belongs to no subscription.
    """
    if isinstance(asset_class, ClassFromPrefix):
        return None
    return _SUBSCRIPTIONS[asset_class]


class Recency(StrEnum):
    """How fresh the data of a WebSocket host or a live table is (internal).

    `REALTIME` for Massive's real-time and FMV hosts, `DELAYED` for its delayed feed, a pinned
    host's as Massive's snapshot labels it, and `UNKNOWN` for a pinned host without such a
    label, or a REST table of a subscription
    listed in `MassiveConfig.delayed_subscriptions` (REST serves what Massive serves the key).
    `UNKNOWN` is never shown as real-time.
    """

    REALTIME = "REALTIME"
    DELAYED = "DELAYED"
    UNKNOWN = "UNKNOWN"


class ChannelFamily(StrEnum):
    """Which group of channels a WebSocket connection carries (internal).

    `FMV` (fair market value) streams only from the one host its page documents
    (`business.massive.com` today); every other channel is in the `TICK` family, whose host follows
    `MassiveConfig.delayed_subscriptions`, `MassiveConfig.websocket_hosts`, and Massive's recency
    label.
    """

    TICK = "TICK"
    FMV = "FMV"


def _require_enum(value: object, enum_type: type[_E], name: str) -> _E:
    """Return `value` if it is a member of `enum_type`; otherwise raise a `TypeError`.

    Every enum parameter of the public API goes through this check at query construction.
    Because a `StrEnum` member is also a `str`, the test is `isinstance(value, enum_type)`, so
    a plain string such as `"minute"` is rejected even when it equals a member's value. The
    message names the parameter, the enum, and the member to use: the member whose value or
    name matches the string (ignoring case) when there is one, otherwise the first member.

    Args:
        value: The argument as the caller passed it.
        enum_type: The enum the parameter requires.
        name: The parameter's name, used in the message.

    Returns:
        `value`, narrowed to `enum_type`.

    Raises:
        TypeError: If `value` is not a member of `enum_type`, for example
            `TypeError("timespan must be dm.Timespan, such as dm.Timespan.MINUTE, not 'minute'")`.
    """
    if isinstance(value, enum_type):
        return value
    members = list(enum_type)
    example = members[0]
    if isinstance(value, str):
        wanted = value.casefold()
        for member in members:
            if wanted in {member.value.casefold(), member.name.casefold()}:
                example = member
                break
    qualified = f"dm.{enum_type.__name__}"
    # Another enum's member reads as the user wrote it, text as quoted, and anything else by
    # its type, the form Python's own type errors take.
    if isinstance(value, Enum):
        actual = f"dm.{type(value).__name__}.{value.name}"
    elif isinstance(value, str):
        actual = repr(value)
    else:
        actual = type(value).__name__
    raise TypeError(f"{name} must be {qualified}, such as {qualified}.{example.name}, not {actual}")
