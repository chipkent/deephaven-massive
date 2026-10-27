"""The closed categorical sets of the public API, as `StrEnum`s, and their runtime check.

Every closed set of values that appears in a public signature is an enum defined here. Each member's
value is the exact string Massive puts on the wire, so a member can be sent to the server unchanged:
`Timespan.MINUTE == "minute"`. Signatures accept only the enum, never a plain string, which gives
one spelling per value, tab completion in every IDE and in the Deephaven web console, and typo
safety at type-check time. At run time `_require_enum` turns a plain string into a `TypeError` that
names the member to use; `Timespan("minute")` is the documented way to convert text.

Two further enums are internal and not exported: `Recency` (how fresh a WebSocket host's
data is) and `ChannelFamily` (which host a channel is served from). Both appear as values in
the diagnostics tables, beside the wire package's `Transport`, which names WebSocket, REST, or
flat files once for both sides of the wire boundary.

The fourteen public enums are re-exported by `deephaven_massive/__init__.py`; this module is
private.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import TypeVar

__all__ = [
    "AssetClass",
    "ChannelFamily",
    "ConditionDataType",
    "ContractType",
    "Direction",
    "FuturesVenue",
    "HistorySource",
    "Locale",
    "Market",
    "Order",
    "PollMode",
    "Recency",
    "SeriesType",
    "Sip",
    "Timeframe",
    "Timespan",
]

logger = logging.getLogger(__name__)

_E = TypeVar("_E", bound=StrEnum)


class AssetClass(StrEnum):
    """An asset class, named by its WebSocket cluster path.

    Used to pin a WebSocket host (`MassiveConfig.ws_hosts`), in the diagnostics tables, and by
    the reference endpoints that filter by asset class (`m.reference.ticker_types`,
    `m.reference.exchanges`, `m.reference.conditions`). The values are the cluster paths
    (`wss://<host>/stocks`, ...). On the REST reference endpoints `FOREX` is sent as `fx`.

    Example:
        ::

            import deephaven_massive as dm

            cfg = dm.MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "delayed.massive.com"})
    """

    STOCKS = "stocks"
    OPTIONS = "options"
    FUTURES = "futures"
    INDICES = "indices"
    FOREX = "forex"
    CRYPTO = "crypto"


class Timespan(StrEnum):
    """The unit of a bar or an indicator window.

    For `bars()` the timespan also selects the transport: `SECOND` and `MINUTE` (with
    `multiplier=1`) stream over WebSocket, `DAY` adds the previous-session `fetch()`, and every
    other combination is history only. `SESSION` exists only for futures, whose aggregates
    endpoint has a trading-session unit and no `DAY`.

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


class PollMode(StrEnum):
    """How a `poll()` table combines successive request cycles.

    - `REPLACE`: a keyed table that holds exactly the latest cycle; keys absent from it are
      deleted, including when the cycle returns no rows.
    - `UPSERT`: a keyed table that adds and updates rows but never deletes.
    - `APPEND`: an append-only table that keeps every row of every cycle.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.PollMode("append") is dm.PollMode.APPEND
    """

    REPLACE = "replace"
    UPSERT = "upsert"
    APPEND = "append"


class HistorySource(StrEnum):
    """Where `history()` reads from.

    - `AUTO`: the library picks flat files or REST from credentials, the dataset, the bar
      shape, publication dates, and the number of symbols.
    - `REST`: the REST history endpoint, paged internally.
    - `FLAT_FILES`: the gzip CSV files on S3 (`files.massive.com`); needs S3 credentials.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.HistorySource("flat_files") is dm.HistorySource.FLAT_FILES
    """

    AUTO = "auto"
    REST = "rest"
    FLAT_FILES = "flat_files"


class FuturesVenue(StrEnum):
    """A futures exchange, used only to pick the futures flat-file prefix.

    Futures streams always use the `/futures` cluster path whatever the venue, so the venue is
    not part of a stream's identity. `venue=None` on a futures dataset reads all four prefixes.

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

            assert dm.Direction("gainers") is dm.Direction.GAINERS
    """

    GAINERS = "gainers"
    LOSERS = "losers"


class ContractType(StrEnum):
    """The type of an options contract, as `options.chain` and `options.contracts` filter it.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.ContractType("call") is dm.ContractType.CALL
    """

    CALL = "call"
    PUT = "put"
    OTHER = "other"


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

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Timeframe("annual") is dm.Timeframe.ANNUAL
    """

    ANNUAL = "annual"
    QUARTERLY = "quarterly"


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
    FX = "fx"
    CRYPTO = "crypto"


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

    The values are upper case because Massive's wire values are.

    Example:
        ::

            import deephaven_massive as dm

            assert dm.Sip("OPRA") is dm.Sip.OPRA
    """

    CTA = "CTA"
    UTP = "UTP"
    OPRA = "OPRA"


class Recency(StrEnum):
    """How fresh the data of a WebSocket host is, as the host decision found it (internal).

    `UNKNOWN` means the evidence did not say (a pinned host, or a futures snapshot without
    timeframe fields); only then may an explicit server verdict re-route the connection.
    """

    REALTIME = "REALTIME"
    DELAYED = "DELAYED"
    UNKNOWN = "UNKNOWN"


class ChannelFamily(StrEnum):
    """Which group of channels a WebSocket connection carries (internal).

    `FMV` (fair market value) is Business-only and always served from `business.massive.com`;
    every other channel is in the `TICK` family, whose host depends on the key's plan.
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
            `TypeError("timespan must be dm.Timespan, e.g. dm.Timespan.MINUTE")`.
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
    raise TypeError(f"{name} must be {qualified}, e.g. {qualified}.{example.name}")
