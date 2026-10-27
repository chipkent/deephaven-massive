"""The hand-verified facts about Massive's wire formats that its specifications lack.

The generators read the vendor's specifications and documentation pages for everything they
can. This module holds the rest, each entry with its reason, and nothing else:

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
- `KINDS`: the dataset kinds: the sources, across transports and asset classes, whose rows
  merge into one table schema. The refresh writes each source's place in them onto the
  generated files (`Source.memberships`), checks one concept, one type, and two transports
  per column within them, and orders every member's columns from the kind's flat-file or
  WebSocket sources;
- `PROJECTIONS`: the fields a source contributes to one kind, under the kind's names;
- `TRANSPORT_ONLY`: the columns of a kind legitimately on one transport only;
- `VENUES`: the futures venues folded into a flat-file prefix's `{venue}` template.

Every entry names a source by its key (a REST path template exactly as the specification
spells it, a WebSocket `"<cluster>/<channel>"`, or a flat-file prefix) and a wire key or
parameter of it. An entry that names something the snapshot no longer generates fails the
refresh (the `stale-entry` gate), so this module can only shrink as the vendor fixes its
specifications.

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.refresh.lexicon import (
    BAR_PATHS,
    CRYPTO_SNAPSHOTS,
    FMV_CHANNELS,
    FOREX_SNAPSHOTS,
    RANGE_PATHS,
    STOCKS_SNAPSHOTS,
    WS_BARS,
)
from deephaven_massive._massive.spec.types import Conversion, WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

_V = TypeVar("_V")


__all__ = [
    "KINDS",
    "NAMES",
    "PARAM_UNITS",
    "PROJECTIONS",
    "RESULT_KEYS",
    "TAG_CONFLICTS",
    "TRANSPORT_ONLY",
    "TYPE_FIXES",
    "UNITS",
    "VENUES",
    "Name",
    "Projection",
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
class Projection:
    """The fields one source contributes to one dataset kind, under the kind's names.

    A projection limits a source to some of its fields, and renames them, inside one
    membership only: the source keeps its full fields everywhere else.

    Attributes:
        fields: Each kept wire key mapped to the long name it takes in the kind, in snake case
            (`{"last_updated": "timestamp"}`); the generator writes each through the naming
            rule, so the projection maps wire keys to PascalCase columns.
        reason: Why the source belongs to the kind only through these fields.
    """

    fields: Mapping[str, str]
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

# The WebSocket aggregates of the classes that carry each of the day's running figures.
_WS_EQUITY_BARS = ("stocks/A", "stocks/AM", "options/A", "options/AM")


def _named(sources: Iterable[str], key: str, name: Name) -> dict[tuple[str, str], Name]:
    """Return the `NAMES` entries of one key that several sources describe alike.

    Args:
        sources: The source keys.
        key: The wire key.
        name: The long name and the description it is derived from.

    Returns:
        `(source, key)` to `name`, for every source.
    """
    return {(source, key): name for source in sources}


def _name(long_name: str, description: str) -> Name:
    """Return a `Name`, so the table below reads one entry per line where it fits.

    Args:
        long_name: The long name derived from the description.
        description: The vendor's description, quoted exactly.

    Returns:
        The entry.
    """
    return Name(long_name=long_name, description=description)


NAMES: Mapping[tuple[str, str], Name] = MappingProxyType(
    {
        **_named(_WS_EQUITY_BARS, "av", _name("accumulated_volume", "Today's accumulated volume.")),
        **_named(
            ("stocks/A", "stocks/AM"),
            "dav",
            _name(
                "accumulated_decimal_volume",
                "Today's accumulated volume including fractional shares, respresented as a "
                "string decimal number",
            ),
        ),
        **_named(
            _WS_EQUITY_BARS, "op", _name("official_open_price", "Today's official opening price.")
        ),
        # One concept, one column: the index bars' official opening value is the same running
        # figure as the equity bars' official opening price.
        **_named(
            ("indices/A", "indices/AM"),
            "op",
            _name("official_open_price", "Today's official opening value."),
        ),
        **_named(_WS_EQUITY_BARS, "a", _name("day_vwap", "Today's volume weighted average price.")),
        **_named(
            (*_WS_EQUITY_BARS, "crypto/XA", "crypto/XAS"),
            "z",
            _name("average_trade_size", "The average trade size for this aggregate window."),
        ),
        **_named(
            WS_BARS,
            "e",
            _name(
                "end_timestamp",
                "The end timestamp of this aggregate window in Unix Milliseconds.",
            ),
        ),
        **_named(("stocks/LULD",), "h", _name("high_price", "The high price.")),
        **_named(("stocks/LULD",), "l", _name("low_price", "The low price.")),
        # Neither event has a long name for its time anywhere, and a name may never coin the
        # dataset key column `Timestamp`; the description says "Timestamp in Unix".
        **_named(("stocks/LULD",), "t", _name("unix_timestamp", "The Timestamp in Unix MS.")),
        **_named(("stocks/NOI",), "t", _name("unix_timestamp", "The Timestamp in Unix Ns.")),
        **_named(
            ("stocks/NOI",),
            "at",
            _name(
                "auction_time",
                "The time that the auction is planned to take place in the format (hour x "
                "100) + minutes in Eastern Standard Time, for example 930 would be 9:30 am EST, "
                "and 1600 would be 4:00 pm EST.",
            ),
        ),
        **_named(
            ("stocks/NOI",),
            "a",
            _name(
                "auction_type",
                "The auction type. `O` - Early Opening Auction (non-NYSE only) `M` - Core "
                "Opening Auction `H` - Reopening Auction (Halt Resume) `C` - Closing Auction "
                "`P` - Extreme Closing Imbalance (NYSE only) `R` - Regulatory Closing Imbalance "
                "(NYSE only)",
            ),
        ),
        **_named(("stocks/NOI",), "i", _name("symbol_sequence", "The symbol sequence.")),
        **_named(("stocks/NOI",), "o", _name("imbalance_quantity", "The imbalance quantity.")),
        **_named(("stocks/NOI",), "p", _name("paired_quantity", "The paired quantity.")),
        **_named(("stocks/NOI",), "b", _name("book_clearing_price", "The book clearing price.")),
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
"""`(source, wire key)` to the long name of a short key Massive names nowhere."""

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

_MS_WS = Unit(
    unit=WireUnit.MS,
    reason="Documented in Unix milliseconds; the documentation samples have 13 digits.",
)
_MS_WS_SAMPLE = Unit(
    unit=WireUnit.MS,
    reason="The description names no unit; the documentation samples have 13 digits, as do "
    "the other times of the same message.",
)
_NS_FILES = Unit(
    unit=WireUnit.NS,
    reason="Documented as a nanosecond Unix timestamp; the example rows have 19 digits.",
)
_NS_FILES_SAMPLE = Unit(
    unit=WireUnit.NS,
    reason="The description names no unit; the example rows have 19 digits, as do the "
    "file's other times.",
)
# Every flat file of tick or aggregate rows, and the timestamps each carries.
_FILE_TIMES: tuple[tuple[str, tuple[str, ...]], ...] = (
    *(
        (f"{prefix}/{file}", ("window_start",))
        for prefix in (
            "us_stocks_sip",
            "us_options_opra",
            "us_indices",
            "global_forex",
            "global_crypto",
        )
        for file in ("minute_aggs_v1", "day_aggs_v1")
    ),
    ("us_futures_{venue}/minute_aggs_v1", ("window_start",)),
    ("us_futures_{venue}/session_aggs_v1", ("window_start",)),
    ("us_stocks_sip/trades_v1", ("participant_timestamp", "sip_timestamp", "trf_timestamp")),
    ("us_stocks_sip/quotes_v1", ("participant_timestamp", "sip_timestamp", "trf_timestamp")),
    ("us_options_opra/trades_v1", ("sip_timestamp",)),
    ("us_options_opra/quotes_v1", ("sip_timestamp",)),
    ("global_forex/quotes_v1", ("participant_timestamp",)),
    ("global_crypto/trades_v1", ("participant_timestamp",)),
    ("us_futures_{venue}/trades_v1", ("timestamp",)),
    ("us_futures_{venue}/quotes_v1", ("timestamp",)),
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
        # The WebSocket messages.
        **_for(("stocks/T",), ("t", "pt", "trft"), _MS_WS),
        **_for(("stocks/Q",), ("t", "pt"), _MS_WS),
        **_for(("options/T", "options/Q", "forex/C", "indices/V"), ("t",), _MS_WS),
        **_for(("crypto/XT", "crypto/XQ"), ("t",), _MS_WS),
        **_for(("crypto/XT", "crypto/XQ"), ("r",), _MS_WS_SAMPLE),
        **_for(("futures/T", "futures/Q"), ("t",), _MS_WS),
        **_for(("futures/Q",), ("bt", "at"), _MS_WS_SAMPLE),
        **_for(WS_BARS, ("s", "e"), _MS_WS),
        ("stocks/LULD", "t"): Unit(
            unit=WireUnit.NS,
            reason="Documented as Unix milliseconds, but the documentation sample, "
            "1764086430905642800, has 19 digits: as milliseconds it would overflow the "
            "nanosecond range, so the sample's nanoseconds are taken until live data confirms.",
            verified=False,
        ),
        ("stocks/NOI", "t"): Unit(
            unit=WireUnit.NS,
            reason="Documented as Unix nanoseconds; the documentation sample has 19 digits.",
        ),
        ("stocks/NOI", "at"): Unit(
            unit=None,
            reason="The auction's planned time of day as hour x 100 + minutes (930 is 9:30 am "
            "Eastern), not an epoch.",
        ),
        **_for(
            FMV_CHANNELS,
            ("t",),
            Unit(
                unit=WireUnit.NS,
                reason="Documented as a nanosecond timestamp; the options sample has 19 digits, "
                "but the stocks, forex, and crypto samples have 13. The library tells the two "
                "apart by magnitude for this field alone.",
                verified=False,
            ),
        ),
        # The flat files.
        **{(prefix, key): _NS_FILES for prefix, keys in _FILE_TIMES for key in keys},
        **_for(
            ("us_futures_{venue}/quotes_v1",), ("ask_timestamp", "bid_timestamp"), _NS_FILES_SAMPLE
        ),
        ("us_indices/values_v1", "timestamp"): Unit(
            unit=WireUnit.NS,
            reason="Documented as Unix milliseconds, but every example row has 19 digits, as do "
            "the other index files' times.",
            verified=False,
        ),
    }
)
"""`(source, wire key)` to the unit of a timestamp-like field, or to "not a timestamp"."""

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
        # The WebSocket messages.
        ("forex/C", "x"): TypeFix(
            conversion=Conversion.QUOTED_INTEGER,
            reason="The specification types the exchange ID as an integer, but its example sends "
            'the text "44"; the forex quote stream sends the ID as decimal digits in text.',
        ),
        ("crypto/XT", "i"): TypeFix(
            conversion=Conversion.INTEGER_ID_AS_TEXT,
            reason="The trade ID is typed and sampled as an integer (14272084), while the crypto "
            "trade's ID is text on REST and in the flat files: an identifier is text, so one "
            "column keeps one type.",
        ),
        **_for(
            ("crypto/XA", "crypto/XAS"),
            ("v",),
            TypeFix(
                conversion=Conversion.INTEGER_AS_NUMBER,
                reason="The volume is typed as an integer, but the example sends 951.6112: a "
                "crypto volume is fractional, as the crypto REST and flat-file volumes are "
                "typed.",
            ),
        ),
        **_for(
            ("futures/A", "futures/AM"),
            ("dv",),
            TypeFix(
                conversion=Conversion.INTEGER_AS_NUMBER,
                reason="The dollar volume is typed as an integer, but the example sends 1353.1: "
                "a dollar value is fractional, as the futures flat files type it.",
            ),
        ),
        # The flat files.
        **_for(
            (
                "us_stocks_sip/trades_v1",
                "us_stocks_sip/quotes_v1",
                "us_options_opra/trades_v1",
                "global_crypto/trades_v1",
            ),
            ("conditions",),
            TypeFix(
                conversion=Conversion.ONE_CODE,
                reason="Documented as an integer, but described as a list of condition codes "
                'that appears quoted and comma-separated when there are several (sample "12,37"): '
                "a list, as on REST and the WebSocket.",
            ),
        ),
        ("us_stocks_sip/quotes_v1", "indicators"): TypeFix(
            conversion=Conversion.ONE_CODE,
            reason="Documented as an integer, but the quote's indicators are a list of codes, as "
            "on REST and the WebSocket; the file writes several as quoted, comma-separated "
            "codes, like its conditions.",
        ),
    }
)
"""`(source, wire key)` to the named conversion of a field its declared type does not fit."""


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


def _bars(ticker: str, *, grouped: str | None = None) -> list[str]:
    """Return the REST aggregate paths of one asset class, in source order.

    Args:
        ticker: The class's ticker placeholder, such as `"stocksTicker"`.
        grouped: The class's market in the grouped-daily path, if it has one.

    Returns:
        The previous-session bar, the range, and the grouped daily bars.
    """
    paths = [
        f"/v2/aggs/ticker/{{{ticker}}}/prev",
        f"/v2/aggs/ticker/{{{ticker}}}/range/{{multiplier}}/{{timespan}}/{{from}}/{{to}}",
    ]
    if grouped is not None:
        paths.append(f"/v2/aggs/grouped/locale/{grouped}/{{date}}")
    return paths


KINDS: Mapping[str, Mapping[str, Sequence[str]]] = MappingProxyType(
    {
        "trades": {
            "stocks": [
                "/v3/trades/{stockTicker}",
                "/v2/last/trade/{stocksTicker}",
                "stocks/T",
                "us_stocks_sip/trades_v1",
            ],
            "options": [
                "/v3/trades/{optionsTicker}",
                "/v2/last/trade/{optionsTicker}",
                "options/T",
                "us_options_opra/trades_v1",
            ],
            "futures": ["/futures/v1/trades/{ticker}", "futures/T", "us_futures_{venue}/trades_v1"],
            "crypto": [
                "/v3/trades/{cryptoTicker}",
                "/v1/last/crypto/{from}/{to}",
                "crypto/XT",
                "global_crypto/trades_v1",
            ],
        },
        "quotes": {
            "stocks": [
                "/v3/quotes/{stockTicker}",
                "/v2/last/nbbo/{stocksTicker}",
                "stocks/Q",
                "us_stocks_sip/quotes_v1",
            ],
            "options": ["/v3/quotes/{optionsTicker}", "options/Q", "us_options_opra/quotes_v1"],
            "futures": ["/futures/v1/quotes/{ticker}", "futures/Q", "us_futures_{venue}/quotes_v1"],
            "forex": [
                "/v3/quotes/{fxTicker}",
                "/v1/last_quote/currencies/{from}/{to}",
                "forex/C",
                "global_forex/quotes_v1",
            ],
            "crypto": ["crypto/XQ"],
        },
        "bars": {
            "stocks": [
                *_bars("stocksTicker", grouped="us/market/stocks"),
                "stocks/A",
                "stocks/AM",
                "us_stocks_sip/minute_aggs_v1",
                "us_stocks_sip/day_aggs_v1",
            ],
            "options": [
                *_bars("optionsTicker"),
                "options/A",
                "options/AM",
                "us_options_opra/minute_aggs_v1",
                "us_options_opra/day_aggs_v1",
            ],
            "futures": [
                "/futures/v1/aggs/{ticker}",
                "futures/A",
                "futures/AM",
                "us_futures_{venue}/minute_aggs_v1",
                "us_futures_{venue}/session_aggs_v1",
            ],
            "indices": [
                *_bars("indicesTicker"),
                "indices/A",
                "indices/AM",
                "us_indices/minute_aggs_v1",
                "us_indices/day_aggs_v1",
            ],
            "forex": [
                *_bars("forexTicker", grouped="global/market/fx"),
                "forex/CAS",
                "forex/CA",
                "global_forex/minute_aggs_v1",
                "global_forex/day_aggs_v1",
            ],
            "crypto": [
                *_bars("cryptoTicker", grouped="global/market/crypto"),
                "crypto/XAS",
                "crypto/XA",
                "global_crypto/minute_aggs_v1",
                "global_crypto/day_aggs_v1",
            ],
        },
        "values": {
            "indices": ["/v3/snapshot/indices", "indices/V", "us_indices/values_v1"],
        },
        "luld": {"stocks": ["stocks/LULD"]},
        "imbalances": {"stocks": ["stocks/NOI"]},
        "fmv": {
            "stocks": ["stocks/FMV"],
            "options": ["options/FMV"],
            "forex": ["forex/FMV"],
            "crypto": ["crypto/FMV"],
        },
    }
)
"""Kind to asset-class group to its sources, in source order.

Every dataset with a WebSocket or flat-file source has a kind, and each kind's group lists the
REST paths that serve the same rows (history, the latest row, grouped daily bars), then the
WebSocket channels, then the flat-file prefixes.
"""

PROJECTIONS: Mapping[tuple[str, str], Projection] = MappingProxyType(
    {
        ("values", "/v3/snapshot/indices"): Projection(
            fields={"ticker": "ticker", "value": "value", "last_updated": "timestamp"},
            reason="The indices snapshot answers the latest value of an index: its ticker, its "
            "value, and the time of that value, which the WebSocket and the flat file name "
            "`timestamp`. Its other fields (the session bar, the market status) belong to the "
            "snapshot, not to a value row.",
        ),
    }
)
"""`(kind, source)` to the fields the source contributes to that kind, under the kind's names."""

_RUNNING = (
    "The WebSocket aggregates carry the day's running figures and the window's end; the REST "
    "and flat-file bars do not."
)
_LAST = (
    "Only the REST last trade or last quote of this class carries it; the stream and the files "
    "do not."
)

TRANSPORT_ONLY: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("bars", "AccumulatedVolume"): _RUNNING,
        ("bars", "AccumulatedDecimalVolume"): _RUNNING,
        ("bars", "AverageTradeSize"): _RUNNING,
        ("bars", "DayVwap"): _RUNNING,
        ("bars", "DecimalVolume"): "The stocks WebSocket aggregates carry the fractional "
        "volume; the stocks REST and flat-file bars do not.",
        ("bars", "EndTimestamp"): _RUNNING,
        ("bars", "OfficialOpenPrice"): _RUNNING,
        ("bars", "Exchange"): "Only the futures minute-bar flat files carry the exchange.",
        ("bars", "Vwap"): "The forex REST bars carry a volume-weighted average price; the forex "
        "WebSocket and flat-file bars do not.",
        ("bars", "Pair"): "The forex and crypto WebSocket aggregates name the instrument `pair`, "
        "their own long name, where REST and the flat files say `ticker`; it is the dataset's "
        "symbol key, which the key rename makes `Symbol` on every transport.",
        ("trades", "Pair"): "The crypto WebSocket trade names the instrument `pair`, its own "
        "long name; it is the dataset's symbol key, which the key rename makes `Symbol`.",
        ("trades", "Ticker"): "Of the crypto trades, only the flat file carries the instrument "
        "as `ticker` (REST takes it from the path or names it `symbol`, the WebSocket `pair`); "
        "it is the dataset's symbol key, which the key rename makes `Symbol`.",
        ("trades", "Timestamp"): "The crypto REST last trade names its time `timestamp`, its own "
        "long name; it is that source's time key, which the key rename makes `Timestamp`.",
        ("trades", "Id"): _LAST,
        ("trades", "ParticipantTimestamp"): _LAST,
        ("trades", "Tape"): _LAST,
        ("trades", "TrfId"): _LAST,
        ("trades", "TrfTimestamp"): _LAST,
        ("trades", "Channel"): "Only the futures REST trades carry the feed channel.",
        ("trades", "Correction"): "Only the futures trade flat files carry the correction "
        "indicator.",
        ("trades", "Exchange"): "Only the futures trade flat files carry the exchange.",
        ("quotes", "Ask"): "The forex REST last quote names its ask `ask`, its own long name, "
        "where the stream and the files say `ask_price`.",
        ("quotes", "Bid"): "The forex REST last quote names its bid `bid`, its own long name, "
        "where the stream and the files say `bid_price`.",
        ("quotes", "Timestamp"): "The forex REST last quote names its time `timestamp`, its own "
        "long name; it is that source's time key, which the key rename makes `Timestamp`.",
        ("quotes", "Channel"): "Only the futures REST quotes carry the feed channel.",
        ("quotes", "Exchange"): "Only the futures quote flat files carry the exchange.",
    }
)
"""`(kind, column)` to why the column is legitimately on one transport only."""

VENUES: Mapping[str, str] = MappingProxyType(
    {
        "cme": "The Chicago Mercantile Exchange; the futures overview lists a us_futures_cme "
        "prefix for every futures dataset.",
        "cbot": "The Chicago Board of Trade; the futures overview lists a us_futures_cbot "
        "prefix for every futures dataset.",
        "nymex": "The New York Mercantile Exchange; the futures overview lists a "
        "us_futures_nymex prefix for every futures dataset.",
        "comex": "The Commodity Exchange; the futures overview lists a us_futures_comex prefix "
        "for every futures dataset.",
    }
)
"""The futures venues folded into the `{venue}` template of a flat-file prefix, with reasons."""
