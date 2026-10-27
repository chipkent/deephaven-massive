"""The hand-verified facts about Massive's REST wire format that its specification lacks.

The generator reads the vendor's specification for everything it can. This module holds the
rest, each entry with its reason, and nothing else:

- `NAMES`: the long name of a short key no lexicon can name, because Massive spells it out
  nowhere;
- `UNITS`: the unit of every timestamp-like integer field (or "not a timestamp"), verified
  against the documentation pages; the generator makes the field an `epoch` in that unit;
- `TAG_CONFLICTS`: the `UNITS` entries that contradict the specification's own unit tag
  (`x-polygon-go-type`), each with the reason the tag is wrong;
- `TYPE_FIXES`: declared conversions for fields whose vendor type is wrong;
- `RESULT_KEYS`: the paths whose rows do not sit under `results`;
- `PARAM_UNITS`: the unit of every timestamp-like string parameter (or "sent as date-time
  text").

Every entry names a path template exactly as the specification spells it, and a wire key or
parameter of that path. An entry that names something the snapshot no longer generates fails
the refresh (gate G4), so this module can only shrink as the vendor fixes its specification.

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from deephaven_massive._massive.spec.types import WireType, WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


__all__ = [
    "NAMES",
    "PARAM_UNITS",
    "RESULT_KEYS",
    "TAG_CONFLICTS",
    "TYPE_FIXES",
    "UNITS",
    "Name",
    "ResultKey",
    "TypeFix",
    "Unit",
]


@dataclass(frozen=True, kw_only=True)
class Name:
    """The long name of a short key that no lexicon covers.

    Attributes:
        long_name: The long name of the wire key's last segment, in snake case
            (`"accumulated_volume"`); the generator PascalCases it with the segments before it.
        reason: Why this name, given that Massive spells the key out nowhere.
    """

    long_name: str
    reason: str


@dataclass(frozen=True, kw_only=True)
class Unit:
    """The verified unit of a timestamp-like field or parameter.

    Attributes:
        unit: The unit the integer counts since the Unix epoch. `None` means the value is not
            an epoch integer: a field stays a plain `long`, and a parameter is sent as date or
            date-time text (`ParamKind.DATE`).
        reason: The evidence: the documentation's wording and sample, or the design's table.
        verified: False when the evidence is the vendor's word alone, with no sample that
            confirms the magnitude; such a unit is still used, and a live test retires it.
    """

    unit: WireUnit | None
    reason: str
    verified: bool = True


@dataclass(frozen=True, kw_only=True)
class TypeFix:
    """A declared conversion for a field whose vendor type is wrong.

    Attributes:
        declared: The type the specification gives the field, by the representation rules.
            If the specification changes it, the entry is stale and the refresh stops.
        wire_type: The type the field really has; the decoder converts every value from
            `declared` to it, and a value that does not convert fails loudly.
        reason: The evidence that the vendor's type is wrong.
    """

    declared: WireType
    wire_type: WireType
    reason: str

    def __post_init__(self) -> None:
        """Check that the entry converts something into a type a field can have.

        Raises:
            ValueError: If the two types are equal, or either is `EPOCH`, whose unit only a
                `UNITS` entry can give.
        """
        if self.declared is self.wire_type or WireType.EPOCH in {self.declared, self.wire_type}:
            raise ValueError(
                f"a type fix from {self.declared} to {self.wire_type} is not a conversion"
            )


@dataclass(frozen=True, kw_only=True)
class ResultKey:
    """Where a path's rows sit in its response body, when not under `results`.

    Attributes:
        key: An envelope key (`"last"`), a dotted path of keys (`"results.values"`), or `None`
            for a body that is itself the rows (a JSON list of rows, or one JSON-object row).
        reason: What the response looks like.
    """

    key: str | None
    reason: str


def _for(paths: Iterable[str], keys: Iterable[str], value: Unit) -> dict[tuple[str, str], Unit]:
    """Return one entry per path and key, all with the same value.

    Args:
        paths: The path templates.
        keys: The wire keys or parameter base names.
        value: The entry.

    Returns:
        `(path, key)` to `value`.
    """
    return {(path, key): value for path in paths for key in keys}


_STOCKS_SNAPSHOTS = (
    "/v2/snapshot/locale/us/markets/stocks/tickers",
    "/v2/snapshot/locale/us/markets/stocks/{direction}",
)
_FOREX_SNAPSHOTS = (
    "/v2/snapshot/locale/global/markets/forex/tickers",
    "/v2/snapshot/locale/global/markets/forex/{direction}",
)
_CRYPTO_SNAPSHOTS = (
    "/v2/snapshot/locale/global/markets/crypto/tickers",
    "/v2/snapshot/locale/global/markets/crypto/{direction}",
)
_INDICATORS = tuple(
    f"/v1/indicators/{name}/{{{ticker}}}"
    for name in ("ema", "macd", "rsi", "sma")
    for ticker in ("stockTicker", "cryptoTicker")
)
_BARS = (
    "/v2/aggs/grouped/locale/global/market/crypto/{date}",
    "/v2/aggs/grouped/locale/global/market/fx/{date}",
    "/v2/aggs/grouped/locale/us/market/stocks/{date}",
    *(
        f"/v2/aggs/ticker/{{{ticker}}}/{leaf}"
        for ticker in (
            "stocksTicker",
            "optionsTicker",
            "indicesTicker",
            "forexTicker",
            "cryptoTicker",
        )
        for leaf in ("prev", "range/{multiplier}/{timespan}/{from}/{to}")
    ),
)

NAMES: Mapping[tuple[str, str], Name] = MappingProxyType(
    {
        **{
            (path, "min.av"): Name(
                long_name="accumulated_volume",
                reason="The day's volume so far; Massive spells `av` out nowhere, and the "
                "WebSocket aggregates use the same key for the same value.",
            )
            for path in _STOCKS_SNAPSHOTS
        },
        **{
            (path, "symbol"): Name(
                long_name="ticker",
                reason="The vendor's `symbol` is the requested ticker (`AAPL`, `I:NDX`, "
                "`O:TSLA210903C00700000` in the samples); `Symbol` is reserved for the "
                "dataset key the library adds.",
            )
            for path in (
                "/v1/open-close/{stocksTicker}/{date}",
                "/v1/open-close/{optionsTicker}/{date}",
                "/v1/open-close/{indicesTicker}/{date}",
            )
        },
        **{
            (path, "symbol"): Name(
                long_name="pair",
                reason="The vendor's `symbol` is the currency pair as text (`BTC-USD`, "
                "`AUD/USD` in the samples), not a Massive ticker; `Symbol` is reserved for "
                "the dataset key the library adds.",
            )
            for path in ("/v1/open-close/crypto/{from}/{to}/{date}", "/v1/conversion/{from}/{to}")
        },
        **{
            (path, "min.dav"): Name(
                long_name="accumulated_decimal_volume",
                reason="The day's fractional volume so far: `av` with the `decimal_volume` "
                "precision of `dv`; Massive spells `dav` out nowhere.",
            )
            for path in _STOCKS_SNAPSHOTS
        },
    }
)
"""`(path, wire key)` to the long name of a short key no lexicon covers."""

_NS_TICKS = Unit(
    unit=WireUnit.NS,
    reason="Documented as a nanosecond Unix timestamp; the documentation samples have 19 digits.",
)
_MS_BARS = Unit(
    unit=WireUnit.MS,
    reason="Documented as a Unix millisecond timestamp; the documentation samples have 13 digits.",
)
_MS_SNAPSHOT = Unit(
    unit=WireUnit.MS,
    reason="Documented as millisecond accuracy; the documentation samples have 13 digits.",
)
_MS_LAST = Unit(
    unit=WireUnit.MS,
    reason="Tagged IMilliseconds and documented as a Unix millisecond timestamp, with 13-digit "
    "samples, but not yet confirmed against live data.",
    verified=False,
)

UNITS: Mapping[tuple[str, str], Unit] = MappingProxyType(
    {
        **_for(_BARS, ("t",), _MS_BARS),
        **_for(
            ("/v2/last/trade/{stocksTicker}", "/v2/last/trade/{optionsTicker}"),
            ("t", "y", "f"),
            _NS_TICKS,
        ),
        **_for(("/v2/last/nbbo/{stocksTicker}",), ("t", "y", "f"), _NS_TICKS),
        **_for(_STOCKS_SNAPSHOTS, ("updated", "lastTrade.t", "lastQuote.t"), _NS_TICKS),
        **_for(_STOCKS_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(_FOREX_SNAPSHOTS, ("updated", "lastQuote.t"), _MS_SNAPSHOT),
        **_for(_FOREX_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(_CRYPTO_SNAPSHOTS, ("updated", "lastTrade.t"), _MS_SNAPSHOT),
        **_for(_CRYPTO_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(
            ("/v1/last/crypto/{from}/{to}", "/v1/last_quote/currencies/{from}/{to}"),
            ("timestamp",),
            _MS_LAST,
        ),
        **_for(("/v1/conversion/{from}/{to}",), ("last.timestamp",), _MS_LAST),
        **_for(
            _INDICATORS,
            ("timestamp",),
            Unit(
                unit=WireUnit.MS,
                reason="Documented as 'Unix Msec'; the documentation samples have 13 digits.",
            ),
        ),
        **_for(("/futures/v1/aggs/{ticker}",), ("window_start",), _NS_TICKS),
        **_for(
            ("/futures/v1/quotes/{ticker}",),
            ("timestamp", "bid_timestamp", "ask_timestamp"),
            _NS_TICKS,
        ),
        **_for(("/futures/v1/trades/{ticker}",), ("timestamp",), _NS_TICKS),
        **_for(
            ("/futures/v1/snapshot",),
            (
                "last_quote.ask_timestamp",
                "last_quote.bid_timestamp",
                "last_quote.last_updated",
                "last_trade.last_updated",
            ),
            _NS_TICKS,
        ),
        **_for(
            ("/futures/v1/snapshot",),
            ("last_minute.last_updated",),
            Unit(
                unit=WireUnit.MS,
                reason="The minute bar's update time; the documentation sample has 13 digits, "
                "while the snapshot's other times have 19.",
            ),
        ),
        **_for(
            ("/v3/quotes/{stockTicker}",),
            ("participant_timestamp", "sip_timestamp", "trf_timestamp"),
            _NS_TICKS,
        ),
        **_for(("/v3/quotes/{optionsTicker}",), ("sip_timestamp",), _NS_TICKS),
        **_for(("/v3/quotes/{fxTicker}",), ("participant_timestamp",), _NS_TICKS),
        **_for(
            ("/v3/trades/{stockTicker}",),
            ("participant_timestamp", "sip_timestamp", "trf_timestamp"),
            _NS_TICKS,
        ),
        **_for(
            ("/v3/trades/{optionsTicker}",), ("participant_timestamp", "sip_timestamp"), _NS_TICKS
        ),
        **_for(
            ("/v3/trades/{cryptoTicker}",),
            ("participant_timestamp", "received_timestamp"),
            _NS_TICKS,
        ),
        **_for(
            ("/v3/snapshot",),
            (
                "fmv_last_updated",
                "last_quote.last_updated",
                "last_trade.last_updated",
                "last_trade.participant_timestamp",
                "last_trade.sip_timestamp",
                "last_updated",
                "underlying_asset.last_updated",
            ),
            _NS_TICKS,
        ),
        **_for(("/v3/snapshot/indices",), ("last_updated",), _NS_TICKS),
        **_for(
            ("/v3/snapshot/options/{underlyingAsset}",),
            (
                "day.last_updated",
                "fmv_last_updated",
                "last_quote.last_updated",
                "last_trade.sip_timestamp",
                "underlying_asset.last_updated",
            ),
            _NS_TICKS,
        ),
    }
)
"""`(path, wire key)` to the unit of a timestamp-like integer field."""

TAG_CONFLICTS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        (path, "timestamp"): "Tagged IMicroseconds, but documented as 'Unix Msec', and the "
        "documentation samples and the responses carry 13-digit milliseconds."
        for path in _INDICATORS
    }
)
"""`(path, wire key)` of a `UNITS` entry that contradicts its unit tag, with the reason."""

TYPE_FIXES: Mapping[tuple[str, str], TypeFix] = MappingProxyType({})
"""`(path, wire key)` to the declared conversion of a mistyped field. No REST field needs one."""


def _result_keys() -> dict[str, ResultKey]:
    """Return the `RESULT_KEYS` entries.

    Returns:
        Path to its result key, for every path whose rows are not under `results`.
    """
    body_object = "The body is one JSON object: the row itself, with the envelope keys."
    snapshot = "The full-market and movers snapshots list their rows under `tickers`."
    entries = {
        "/v1/last/crypto/{from}/{to}": ResultKey(key="last", reason="The last trade is `last`."),
        "/v1/last_quote/currencies/{from}/{to}": ResultKey(
            key="last", reason="The last quote is `last`."
        ),
        "/v1/conversion/{from}/{to}": ResultKey(key=None, reason=body_object),
        "/v1/marketstatus/now": ResultKey(key=None, reason=body_object),
        "/v1/marketstatus/upcoming": ResultKey(
            key=None, reason="The body is a JSON list of holiday rows, with no envelope."
        ),
        "/v1/open-close/crypto/{from}/{to}/{date}": ResultKey(key=None, reason=body_object),
        "/v1/open-close/{indicesTicker}/{date}": ResultKey(key=None, reason=body_object),
        "/v1/open-close/{optionsTicker}/{date}": ResultKey(key=None, reason=body_object),
        "/v1/open-close/{stocksTicker}/{date}": ResultKey(key=None, reason=body_object),
        "/vX/reference/tickers/{id}/events": ResultKey(
            key="results.events",
            reason="`results` holds the asset's name and its `events` list; the events are "
            "the rows.",
        ),
    }
    for path in (*_STOCKS_SNAPSHOTS, *_FOREX_SNAPSHOTS, *_CRYPTO_SNAPSHOTS):
        entries[path] = ResultKey(key="tickers", reason=snapshot)
    for path in _INDICATORS:
        entries[path] = ResultKey(
            key="results.values",
            reason="`results` holds the `underlying` aggregates and the indicator `values`; "
            "the values are the rows.",
        )
    return entries


RESULT_KEYS: Mapping[str, ResultKey] = MappingProxyType(_result_keys())
"""Path to where its rows sit, for every path whose rows are not under `results`."""

_NS_FILTER = Unit(
    unit=WireUnit.NS,
    reason="Documented as 'a YYYY-MM-DD date or a nanosecond Unix timestamp'.",
)

PARAM_UNITS: Mapping[tuple[str, str], Unit] = MappingProxyType(
    {
        **_for(
            (
                "/v3/trades/{stockTicker}",
                "/v3/trades/{optionsTicker}",
                "/v3/trades/{cryptoTicker}",
                "/v3/quotes/{stockTicker}",
                "/v3/quotes/{optionsTicker}",
                "/v3/quotes/{fxTicker}",
                "/futures/v1/trades/{ticker}",
                "/futures/v1/quotes/{ticker}",
            ),
            ("timestamp",),
            _NS_FILTER,
        ),
        **_for(("/futures/v1/aggs/{ticker}",), ("window_start",), _NS_FILTER),
        **_for(
            _INDICATORS,
            ("timestamp",),
            Unit(
                unit=WireUnit.MS,
                reason="Documented as 'a date with the format YYYY-MM-DD or a millisecond "
                "timestamp'.",
            ),
        ),
        **_for(
            (
                "/benzinga/v1/analyst-insights",
                "/benzinga/v1/bulls-bears-say",
                "/benzinga/v1/earnings",
                "/benzinga/v1/guidance",
                "/benzinga/v1/ratings",
            ),
            ("last_updated",),
            Unit(
                unit=None,
                reason="Accepts 'yyyy-mm-dd' or ISO 8601 text; its integer form is documented "
                "as seconds on some paths and nanoseconds on others, so it is sent as text.",
            ),
        ),
    }
)
"""`(path, base parameter)` to the unit of a timestamp-like string parameter."""
