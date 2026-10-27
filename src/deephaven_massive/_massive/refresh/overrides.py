"""The hand-verified facts about Massive's REST wire format that its specification lacks.

The generator reads the vendor's specification for everything it can. This module holds the
rest, each entry with its reason, and nothing else:

- `NAMES`: the long name of a short key Massive gives no long name anywhere, derived from
  Massive's own description of the field, which the entry quotes;
- `UNITS`: the unit of every timestamp-like integer field (or "not a timestamp" for a
  timestamp-like field that is not an epoch), verified against the documentation pages; the
  generator makes the field an `epoch` in that unit;
- `TAG_CONFLICTS`: the `UNITS` entries that contradict the specification's own unit tag
  (`x-polygon-go-type`), each with the reason the tag is wrong;
- `TYPE_FIXES`: the named conversion of each field whose documented samples its declared
  type does not fit (a wrong type, or a second form the vendor sends), each with the sample;
- `RESULT_KEYS`: the paths whose rows do not sit under `results`;
- `PARAM_UNITS`: the unit of every timestamp-like string parameter, and of any other string
  parameter that takes an epoch (or "sent as date or date-time text");
- `KINDS`: the dataset kinds, whose sources the `concept-collision` gate checks for one
  concept per column. The REST generator alone declares none; the WebSocket and flat-file
  generators add them.

Every entry names a path template exactly as the specification spells it, and a wire key or
parameter of that path. An entry that names something the snapshot no longer generates fails
the refresh (the `stale-entry` gate), so this module can only shrink as the vendor fixes its
specification.

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.refresh.lexicon import (
    BAR_PATHS,
    CRYPTO_SNAPSHOTS,
    FOREX_SNAPSHOTS,
    RANGE_PATHS,
    STOCKS_SNAPSHOTS,
)
from deephaven_massive._massive.spec.types import Conversion, WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

_V = TypeVar("_V")


__all__ = [
    "KINDS",
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
    """The long name of a short key Massive gives no long name anywhere.

    Attributes:
        long_name: The long name of the wire key's last segment, in snake case
            (`"accumulated_volume"`); the generator PascalCases it with the segments before it,
            as for every other field.
        description: Massive's description of the field, quoted exactly (whitespace aside):
            the name's only source. The refresh checks the quote against the specification,
            so a changed description makes the entry stale (the `stale-entry` gate).
    """

    long_name: str
    description: str


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
    """The named conversion of a field whose documented samples its declared type does not fit.

    The conversion (`spec.types.Conversion`) fixes both ends: the type the specification must
    still declare (one of `conversion.declarations`; if the specification changes it, the
    entry is stale) and the type the field gets (`conversion.wire_type`). A conversion to an
    `epoch` takes its unit from the field's `UNITS` entry, which it then requires.

    Attributes:
        conversion: The conversion the decoder applies to every value of the field; a value it
            cannot read fails loudly.
        reason: The evidence: the path, the field, and the documented sample the declared type
            does not fit.
    """

    conversion: Conversion
    reason: str


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


def _for(paths: Iterable[str], keys: Iterable[str], value: _V) -> dict[tuple[str, str], _V]:
    """Return one entry per path and key, all with the same value.

    Args:
        paths: The path templates.
        keys: The wire keys or parameter base names.
        value: The entry.

    Returns:
        `(path, key)` to `value`.
    """
    return {(path, key): value for path in paths for key in keys}


_INDICATORS = tuple(
    f"/v1/indicators/{name}/{{{ticker}}}"
    for name in ("ema", "macd", "rsi", "sma")
    for ticker in ("stockTicker", "cryptoTicker")
)

NAMES: Mapping[tuple[str, str], Name] = MappingProxyType(
    {
        **{
            (path, "min.av"): Name(
                long_name="accumulated_volume", description="The accumulated volume."
            )
            for path in STOCKS_SNAPSHOTS
        },
        **{
            (path, "min.dav"): Name(
                long_name="accumulated_decimal_volume",
                description="The accumulated volume including fractional shares, "
                "respresented as a string.",
            )
            for path in STOCKS_SNAPSHOTS
        },
    }
)
"""`(path, wire key)` to the long name of a short key Massive names nowhere."""

_NS_TICKS = Unit(
    unit=WireUnit.NS,
    reason="Documented as a nanosecond Unix timestamp; the documentation samples have 19 digits.",
)
_NS_SAMPLE = Unit(
    unit=WireUnit.NS,
    reason="The description names no unit; the documentation samples have 19 digits.",
)
_UPDATED_NS = Unit(
    unit=WireUnit.NS,
    reason="Described only as 'The last updated timestamp.'; the documentation samples have 19 "
    "digits.",
)
_UPDATED_MS = Unit(
    unit=WireUnit.MS,
    reason="Described only as 'The last updated timestamp.'; the documentation samples have 13 "
    "digits (one value, 1605330008999, repeated on the crypto pages).",
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
        **_for(BAR_PATHS, ("t",), _MS_BARS),
        **_for(
            ("/v2/last/trade/{stocksTicker}", "/v2/last/trade/{optionsTicker}"),
            ("t", "y", "f"),
            _NS_TICKS,
        ),
        **_for(("/v2/last/nbbo/{stocksTicker}",), ("t", "y", "f"), _NS_TICKS),
        **_for(STOCKS_SNAPSHOTS, ("lastTrade.t", "lastQuote.t"), _NS_TICKS),
        **_for(STOCKS_SNAPSHOTS, ("updated",), _UPDATED_NS),
        **_for(STOCKS_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(FOREX_SNAPSHOTS, ("lastQuote.t",), _MS_SNAPSHOT),
        **_for(FOREX_SNAPSHOTS, ("updated",), _UPDATED_MS),
        **_for(FOREX_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(CRYPTO_SNAPSHOTS, ("lastTrade.t",), _MS_SNAPSHOT),
        **_for(CRYPTO_SNAPSHOTS, ("updated",), _UPDATED_MS),
        **_for(CRYPTO_SNAPSHOTS, ("min.t",), _MS_BARS),
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
        **_for(("/futures/v1/quotes/{ticker}",), ("timestamp",), _NS_TICKS),
        **_for(("/futures/v1/quotes/{ticker}",), ("bid_timestamp", "ask_timestamp"), _NS_SAMPLE),
        **_for(("/futures/v1/trades/{ticker}",), ("timestamp",), _NS_TICKS),
        **_for(
            ("/futures/v1/snapshot",),
            ("last_quote.last_updated", "last_trade.last_updated"),
            _NS_TICKS,
        ),
        **_for(
            ("/futures/v1/snapshot",),
            ("last_quote.ask_timestamp", "last_quote.bid_timestamp"),
            _NS_SAMPLE,
        ),
        ("/futures/v1/snapshot", "details.settlement_date"): Unit(
            unit=WireUnit.NS,
            reason="The settlement day, sent as an epoch (its `TYPE_FIXES` entry); the "
            "specification example and the documentation sample, 1753851600000000000, have 19 "
            "digits.",
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
"""`(path, wire key)` to the unit of a timestamp-like field, or to "not a timestamp"."""

TAG_CONFLICTS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        (path, "timestamp"): "Tagged IMicroseconds, but documented as 'Unix Msec', and the "
        "documentation samples and the responses carry 13-digit milliseconds."
        for path in _INDICATORS
    }
)
"""`(path, wire key)` of a `UNITS` entry that contradicts its unit tag, with the reason."""

_ISO_TEXT = "The specification types it as text with no format; the documentation sample is "


def _iso(sample: str) -> TypeFix:
    """Return the fix of a timestamp the specification leaves as plain text.

    Args:
        sample: The documentation sample that shows ISO-8601 date-time text.

    Returns:
        The `iso-text-instant` conversion, citing the sample.
    """
    return TypeFix(
        conversion=Conversion.ISO_TEXT_INSTANT,
        reason=f"{_ISO_TEXT}ISO-8601 date-time text, {sample}.",
    )


TYPE_FIXES: Mapping[tuple[str, str], TypeFix] = MappingProxyType(
    {
        ("/benzinga/v1/analysts", "last_updated"): _iso('"2025-05-19T04:31:12Z"'),
        ("/benzinga/v1/firms", "last_updated"): _iso('"2020-01-07T12:41:25Z"'),
        ("/futures/v1/market-status", "timestamp"): _iso('"2025-12-04T23:00:00+00:00"'),
        ("/v1/marketstatus/now", "serverTime"): _iso('"2020-11-10T17:37:37-05:00"'),
        ("/futures/v1/products", "last_updated"): TypeFix(
            conversion=Conversion.ISO_DATE_TEXT,
            reason="The specification declares format date-time, but the documentation sample "
            'is the date "2025-02-22", which no date-time parser accepts.',
        ),
        ("/futures/v1/snapshot", "details.settlement_date"): TypeFix(
            conversion=Conversion.DATE_AS_EPOCH,
            reason="The specification declares format date, but its example and the "
            "documentation sample are the integer 1753851600000000000 (2025-07-30 05:00 UTC, "
            "midnight in Chicago): an epoch in nanoseconds, whose unit `UNITS` gives. Kept "
            "lossless as an instant. `/futures/v1/contracts` sends the same field as the date "
            '"2026-12-18" (a `date` column there), which this path\'s conversion refuses '
            "rather than guess a time zone.",
        ),
        ("/v1/open-close/{indicesTicker}/{date}", "from"): TypeFix(
            conversion=Conversion.MIDNIGHT_UTC_DATE,
            reason='The documentation sample is "2023-03-10T00:00:00.000Z", the requested '
            "date written as midnight UTC, where the specification declares a date and the "
            'stocks and options open-close pages send "2023-01-09". The column stays a date, '
            "so the open-close sources agree and no time is invented. Any other clock or "
            "offset is refused loudly: an indices day anchored elsewhere (the custom-bars "
            "sample starts at 06:00 UTC) would fail rather than shift a day.",
        ),
        ("/v1/open-close/crypto/{from}/{to}/{date}", "day"): TypeFix(
            conversion=Conversion.MIDNIGHT_UTC_DATE,
            reason='The documentation sample is "2020-10-09T00:00:00.000Z", the requested '
            "date written as midnight UTC, where the specification declares a date. The "
            "column stays a date, as on the other open-close sources; any other clock or "
            "offset is refused loudly.",
        ),
        **_for(
            CRYPTO_SNAPSHOTS,
            ("lastTrade.i",),
            TypeFix(
                conversion=Conversion.TEXT_OR_INTEGER_ID,
                reason="The trade ID is typed as text, but the full-market page's example and "
                "documentation sample send the integer 413131, while the movers page sends "
                '"517478762". Both snapshots join one kind, so both keep text and accept '
                "either form without loss.",
            ),
        ),
        ("/tmx/v1/corporate-events", "tmx_company_id"): TypeFix(
            conversion=Conversion.TEXT_OR_INTEGER_ID,
            reason="The specification types the ID as an int64, but its example and the "
            'documentation sample are the text "2208". An identifier is text: either form is '
            "kept as its digits.",
        ),
        ("/stocks/taxonomies/vX/risk-factors", "taxonomy"): TypeFix(
            conversion=Conversion.NUMBER_LABEL,
            reason="The specification types the taxonomy version as a number, but its example "
            'and both documentation sample rows are the text "1.0". A version is a label, so '
            "it is text; a number becomes its decimal text with a fraction.",
        ),
    }
)
"""`(path, wire key)` to the named conversion of a field its declared type does not fit."""


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
    for path in (*STOCKS_SNAPSHOTS, *FOREX_SNAPSHOTS, *CRYPTO_SNAPSHOTS):
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
            RANGE_PATHS,
            ("from", "to"),
            Unit(
                unit=WireUnit.MS,
                reason="Documented as 'a date with the format YYYY-MM-DD or a millisecond "
                "timestamp'.",
            ),
        ),
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

KINDS: Mapping[str, Mapping[str, Sequence[str]]] = MappingProxyType({})
"""Kind to asset-class group to its sources, in source order. The REST generator declares none."""
