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
- `KEY_FIXES`: the wire key of each field whose declared key every documented sample
  contradicts, each with the samples;
- `UNDECLARED_FIELDS`: each field Massive sends that its specification does not declare,
  with the schema it is generated from and the live response that sent it;
- `OPTIONAL_FIELDS`: each field the specification marks required that Massive omits, with the
  live response that omitted it;
- `DESCRIPTION_FIXES`: the corrected description of each field whose vendor description is
  clearly wrong for its source (another asset class's or event's text copied onto it, or a
  statement Massive's own documentation of the field contradicts) or missing where a sibling
  source describes the same field, for every use of the source or per asset class, each
  quoting the vendor's text (empty for a missing one) and giving the reason;
- `RESULT_KEYS`: the paths whose rows do not sit under `results`;
- `PARAM_UNITS`: the unit of every timestamp-like string parameter, and of any other string
  parameter that takes an epoch (or "sent as date or date-time text");
- `KINDS`: the dataset kinds: the sources, across transports and asset classes, whose rows
  merge into one table schema. The refresh writes each source's place in them onto the
  generated files (`Source.memberships`), checks one concept, one type, and two transports
  per column within them, and orders every member's columns from the kind's flat-file or
  WebSocket sources;
- `PROJECTIONS`: the fields a source contributes to one kind, under the kind's names;
- `TRANSPORT_ONLY`: the columns of a kind's group legitimately on one transport only;
- `VENUES`: the futures venues folded into a flat-file prefix's `{venue}` template.

Every entry names a source by its key (a REST path template exactly as the specification
spells it, a WebSocket `"<cluster>/<channel>"`, or a flat-file prefix) and a wire key or
parameter of it. An entry that names something the snapshot no longer generates fails the
refresh (the `stale-entry` gate), so this module can only shrink as the vendor fixes its
specifications.

The tables are plain read-only data, built once at import. The module is one subject although
it runs past 1,000 lines: every entry is a hand-verified wire fact with its reason, one per
line of evidence, so it grows with Massive's API, not with new subjects.
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
    "DESCRIPTION_FIXES",
    "KEY_FIXES",
    "KINDS",
    "NAMES",
    "OPTIONAL_FIELDS",
    "PARAM_UNITS",
    "PROJECTIONS",
    "RESULT_KEYS",
    "TAG_CONFLICTS",
    "TRANSPORT_ONLY",
    "TYPE_FIXES",
    "UNDECLARED_FIELDS",
    "UNITS",
    "VENUES",
    "DescriptionFix",
    "KeyFix",
    "Name",
    "Projection",
    "ResultKey",
    "TypeFix",
    "UndeclaredField",
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
        reason: The evidence: the documentation's wording and sample, or a live check's finding.
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
            does not fit, or the live response that showed the form.
        observed_live: True when the evidence is what Massive sent in a live response, with no
            documented sample in that form. The stale check reads only documented samples, so
            it never retires such an entry; the reason gives the date and the values seen.
    """

    conversion: Conversion
    reason: str
    observed_live: bool = False


@dataclass(frozen=True, kw_only=True)
class KeyFix:
    """The wire key of a field whose declared key every documented sample contradicts.

    The specification declares the field under one key while its response example and the
    documentation sample send it under another, and nothing declares that other key: the
    field would be null on every row, and the key the wire sends would be dropped as drift.
    The entry is keyed by the declared key and gives the key the samples send; the field keeps
    its declared type and description, and every other hand entry names it by its wire key.

    Attributes:
        wire_key: The dotted key the samples send in place of the declared one (`"type"`).
        reason: The evidence: the declared key and the samples that send the other key.
    """

    wire_key: str
    reason: str


@dataclass(frozen=True, kw_only=True)
class UndeclaredField:
    """A field Massive sends that its specification does not declare.

    The field would be dropped and reported as drift on every response. The entry declares it
    as the specification would: the generator reads it like any declared field of the source,
    names it by the lexicon of the object it sits in, types it by the representation rules
    (a timestamp-like integer needs a `UNITS` entry too), and gives it the description here,
    after the notes of its enclosing objects. It is never vendor-required. It goes after the
    last field the specification declares directly in its object (in the row itself, for a key
    with no dot). The entry is stale once the specification declares the key, or when that
    object holds no declared field of its own to place it by.

    Attributes:
        json_type: The JSON schema type of its values: `"string"`, `"number"`, `"integer"`,
            or `"boolean"`.
        description: What the field holds, in the words Massive uses for the same field of a
            sibling source wherever one has it.
        reason: The evidence: the live response that sent it, with its date and a value.
    """

    json_type: str
    description: str
    reason: str


@dataclass(frozen=True, kw_only=True)
class DescriptionFix:
    """The corrected description of a field whose vendor description is clearly wrong.

    Massive reuses one description across asset classes and endpoints, and sometimes the text
    is clearly wrong for the source it sits on: crypto trades priced "per whole share", a
    trade's timestamp described as when "the quote" was generated, a futures trade's value
    computed without the contract multiplier. The entry replaces that text for one
    `(source, wire key)` only; the field keeps its name and type, and every other source keeps
    its own description. A description that is merely terse or imprecise is kept. A field the
    vendor leaves undescribed, where a sibling source describes the same field, gets an entry
    that quotes the empty text (`vendor=""`) and takes the sibling's, so the entry goes stale
    once the vendor writes one.

    An entry may also name a nested object of the rows (`last_trade`): its text is the note
    every field flattened out of the object carries after its own description, and the entry
    corrects that note the same way. One quoting `""` gives a note to an object the vendor
    leaves undescribed.

    An entry takes one of two forms:

    - `text` replaces the field's description everywhere the source is used;
    - `asset_classes` gives, for a source that serves several asset classes (the unified
      snapshot), the description a dataset of each listed class shows; the field's own
      description, and the one every other class shows, stays the vendor's.

    Example:
        ::

            from deephaven_massive._massive.refresh.overrides import DescriptionFix

            fix = DescriptionFix(
                vendor="The price per whole share.",
                text="The price per coin.",
                reason="A crypto pair has no shares.",
            )

    Attributes:
        vendor: Massive's description, as plain text (`plain_text`: links written as
            `text (url)`), quoted exactly, whitespace aside. The refresh checks the quote, so a
            description the vendor rewrites makes the entry stale (the `stale-entry` gate) and
            the new text is reviewed instead of silently replaced.
        reason: Why the vendor's text is wrong for this source, and where the replacement
            comes from.
        text: The description the field gets in its place, as plain text. It is Massive's own
            wording for the same field of a sibling source wherever that is right and says
            what the field holds; only when no sibling text is adequate is it written here.
            `None` for an entry by asset class.
        asset_classes: Asset class (an `AssetClass` value, such as `"options"`) to the
            description a dataset of that class shows, chosen as `text` is. `None` for an
            entry that replaces the description everywhere.
    """

    vendor: str
    reason: str
    text: str | None = None
    asset_classes: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        """Check that exactly one form is given and that no replacement is empty.

        Raises:
            ValueError: If both or neither of `text` and `asset_classes` is given, or a
                replacement or the asset-class mapping is empty.
        """
        texts = [self.text] if self.text is not None else list((self.asset_classes or {}).values())
        if (self.text is None) == (self.asset_classes is None) or not texts or not all(texts):
            raise ValueError(
                "a description fix needs exactly one of a non-empty `text` or a non-empty "
                "`asset_classes` with non-empty texts"
            )


@dataclass(frozen=True, kw_only=True)
class Projection:
    """How one source's fields join one dataset kind, inside that membership only.

    A projection takes one of two forms, and the source keeps its own fields and names
    everywhere else:

    - `fields` keeps only the listed wire keys, each under the kind's name for it (the indices
      snapshot joins the values kind as its ticker, value, and time alone);
    - `renames` keeps every field and renames only the listed ones (the forex last quote's
      `ask` as `ask_price`), so a field the vendor adds later joins under its own name.

    Every long name is written through the naming rule, so a projection maps wire keys to
    PascalCase columns.

    Example:
        ::

            from deephaven_massive._massive.refresh.overrides import Projection

            prices = Projection(renames={"ask": "ask_price"}, reason="One column per field.")

    Attributes:
        reason: Why the source joins the kind this way.
        fields: The kept wire keys, each mapped to its long name in the kind, in snake case
            (`{"last_updated": "timestamp"}`); `None` for a projection that renames.
        renames: The renamed wire keys, each mapped to its long name in the kind; every other
            field keeps its own column. `None` for a projection that keeps some fields.
    """

    reason: str
    fields: Mapping[str, str] | None = None
    renames: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        """Check that exactly one form is given.

        Raises:
            ValueError: If both or neither of `fields` and `renames` is given, or the one
                given is empty.
        """
        if (self.fields is None) == (self.renames is None) or not (self.fields or self.renames):
            raise ValueError("a projection keeps some `fields` or `renames` some, not both")

    @property
    def named(self) -> Mapping[str, str]:
        """The wire keys the projection names, each mapped to its long name in the kind."""
        return self.fields or self.renames or {}


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

# The WebSocket aggregate channels of every asset class, per minute and per second.
_WS_BARS = (
    "stocks/AM",
    "stocks/A",
    "options/AM",
    "options/A",
    "indices/AM",
    "indices/A",
    "futures/AM",
    "futures/A",
    "forex/CA",
    "forex/CAS",
    "crypto/XA",
    "crypto/XAS",
)
# The WebSocket aggregates of the classes that carry each of the day's running figures.
_WS_EQUITY_BARS = ("stocks/AM", "stocks/A", "options/AM", "options/A")
# The WebSocket fair-market-value channels.
_FMV_CHANNELS = ("stocks/FMV", "options/FMV", "forex/FMV", "crypto/FMV")


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
            ("stocks/AM", "stocks/A"),
            "dav",
            _name(
                "accumulated_decimal_volume",
                "Today's accumulated volume including fractional shares, respresented as a "
                "string decimal number",
            ),
        ),
        # A neutral name for the day's official open: the stocks and options opening price and
        # the indices opening level (an index value, not a price), as `Open` is for every bar.
        **_named(_WS_EQUITY_BARS, "op", _name("official_open", "Today's official opening price.")),
        **_named(
            ("indices/AM", "indices/A"),
            "op",
            _name("official_open", "Today's official opening value."),
        ),
        **_named(
            (*_WS_EQUITY_BARS, "crypto/XA", "crypto/XAS"),
            "z",
            _name("average_trade_size", "The average trade size for this aggregate window."),
        ),
        **_named(
            _WS_BARS,
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
_UPDATED_MS_OR_NS = Unit(
    unit=WireUnit.MS_OR_NS,
    reason="Described only as 'The last updated timestamp.'; the documentation samples have 13 "
    "digits, milliseconds (one value, 1605330008999, repeated on the crypto pages), but the "
    "live run of 2026-11-02 received 19 digits, nanoseconds, on all four forex and crypto "
    "snapshot and movers paths (1793633526000000000 on the forex full-market snapshot, "
    "1793633580000000000 on the crypto one). Each value is read by its magnitude, so either "
    "unit reads.",
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
        **_for(FOREX_SNAPSHOTS, ("updated",), _UPDATED_MS_OR_NS),
        **_for(FOREX_SNAPSHOTS, ("min.t",), _MS_BARS),
        **_for(CRYPTO_SNAPSHOTS, ("lastTrade.t",), _MS_SNAPSHOT),
        **_for(CRYPTO_SNAPSHOTS, ("updated",), _UPDATED_MS_OR_NS),
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
        **_for(
            ("/futures/v1/snapshot",),
            ("last_minute.last_updated",),
            Unit(
                unit=WireUnit.MS_OR_NS,
                reason="The minute bar's update time; the documentation sample has 13 digits, "
                "milliseconds, but the live run of 2026-11-02 received 19 digits, nanoseconds "
                "(1793640180000000000, 2026-11-02 17:23 UTC), like the snapshot's other times. "
                "Each value is read by its magnitude, so either unit reads.",
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
        **_for(
            ("/v3/snapshot",),
            ("session.last_updated", "last_minute.last_updated"),
            Unit(
                unit=WireUnit.NS,
                reason="Undeclared fields (`UNDECLARED_FIELDS`) that the live run of 2026-11-02 "
                "received with 19 digits, 1793633547860533354 and 1793633535088572302, like "
                "the snapshot's documented nanosecond `last_updated` times.",
            ),
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
        **_for(_WS_BARS, ("s", "e"), _MS_WS),
        ("stocks/LULD", "t"): Unit(
            unit=WireUnit.MS_OR_NS,
            reason="Documented as Unix milliseconds, but the documentation sample, "
            "1764086430905642800, has 19 digits, nanoseconds. Each value is read by its "
            "magnitude until live data shows which unit the stream sends: a guess of either "
            "unit would drop every row of the other, and fail the stream.",
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
            _FMV_CHANNELS,
            ("t",),
            Unit(
                unit=WireUnit.MS_OR_NS,
                reason="Documented as a nanosecond timestamp; the options sample has 19 digits, "
                "but the stocks, forex, and crypto samples have 13. Each value is read by its "
                "magnitude.",
                verified=False,
            ),
        ),
        # The flat files.
        **{(prefix, key): _NS_FILES for prefix, keys in _FILE_TIMES for key in keys},
        **_for(
            ("us_futures_{venue}/quotes_v1",), ("ask_timestamp", "bid_timestamp"), _NS_FILES_SAMPLE
        ),
        ("us_indices/values_v1", "timestamp"): Unit(
            unit=WireUnit.MS_OR_NS,
            reason="Documented as Unix milliseconds, but every example row has 19 digits, "
            "nanoseconds, as do the other index files' times. Each value is read by its "
            "magnitude until live data shows which unit the files hold: a guess of either unit "
            "would drop every row of the other.",
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


# The crypto trade ID, one decision for every source that sends it.
_CRYPTO_TRADE_ID = TypeFix(
    conversion=Conversion.TEXT_OR_INTEGER_ID,
    reason="The trade ID is typed as text on the snapshots, but the full-market page's example "
    "and documentation sample send the integer 413131, while the movers page sends "
    '"517478762"; the WebSocket trade types and samples it as the integer 14272084 but sends '
    'the text "666025125" live, and the REST trades and the flat file keep it as text. All '
    "join one kind, so all keep text and accept either form without loss.",
)
# The codes of a flat file's list columns, one decision for every file that writes them.
_CODE_LIST = TypeFix(
    conversion=Conversion.CODE_LIST_TEXT,
    reason="Documented as an integer, but described as a list of codes that appears quoted "
    'and comma-separated when there are several (stocks trades sample "12,37"): a list, as on '
    "REST and the WebSocket.",
)

# The futures stream's prices, one decision for every channel that sends them.
_FUTURES_TEXT_PRICE = TypeFix(
    conversion=Conversion.DECIMAL_TEXT_NUMBER,
    reason="The specification types each futures price as a number and its examples send "
    "numbers, but the live run of 2026-11-02 received every price as text, on the real-time "
    'and the delayed host alike, for ESZ6: `"p": "7676.5"` on `T`, `"bp": "7676.25"` and '
    '`"ap": "7676.5"` on `Q`, and `"o": "7679"`, `"h": "7679.25"`, `"l": "7677.25"`, and '
    '`"c": "7678.5"` on `AM`. Either form is read as the number, and text that is not a plain '
    "finite decimal is refused. The per-second bar `A` sends the same message as `AM` and "
    "takes the same reading; that run did not subscribe it.",
    observed_live=True,
)

TYPE_FIXES: Mapping[tuple[str, str], TypeFix] = MappingProxyType(
    {
        ("/benzinga/v1/analysts", "last_updated"): _iso('"2025-05-19T04:31:12Z"'),
        ("/benzinga/v1/firms", "last_updated"): _iso('"2020-01-07T12:41:25Z"'),
        ("/futures/v1/market-status", "timestamp"): _iso('"2025-12-04T23:00:00+00:00"'),
        ("/v1/marketstatus/now", "serverTime"): _iso('"2020-11-10T17:37:37-05:00"'),
        ("/v1/marketstatus/upcoming", "open"): _iso('"2020-11-27T14:30:00.000Z"'),
        ("/v1/marketstatus/upcoming", "close"): _iso('"2020-11-27T18:00:00.000Z"'),
        ("/futures/v1/snapshot", "details.settlement_date"): TypeFix(
            conversion=Conversion.CHICAGO_MIDNIGHT_DATE,
            reason="The specification declares format date, but its example and the "
            "documentation sample are the integer 1753851600000000000 (2025-07-30 05:00 UTC, "
            "midnight in Chicago): the settlement day as the epoch nanoseconds of its Chicago "
            "midnight, the CME's calendar. The column stays a date, as on "
            '`/futures/v1/contracts`, which sends the same field as the date "2026-12-18", so '
            "the two join. Any instant other than a Chicago midnight is refused loudly rather "
            "than shifted to a day.",
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
        **_for(CRYPTO_SNAPSHOTS, ("lastTrade.i",), _CRYPTO_TRADE_ID),
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
            'the text "44"; the stream sends the integer 48 live. Either form is read as the '
            "integer, so the example's form also converts; text that is not decimal digits is "
            "refused.",
        ),
        ("crypto/XT", "i"): _CRYPTO_TRADE_ID,
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
        ("futures/T", "p"): _FUTURES_TEXT_PRICE,
        **_for(("futures/Q",), ("bp", "ap"), _FUTURES_TEXT_PRICE),
        **_for(("futures/AM", "futures/A"), ("o", "h", "l", "c"), _FUTURES_TEXT_PRICE),
        **_for(
            ("futures/AM", "futures/A"),
            ("dv",),
            TypeFix(
                conversion=Conversion.DECIMAL_TEXT_NUMBER,
                reason="The dollar volume is typed as an integer, but the example sends 1353.1: "
                "a dollar value is fractional, as the futures flat files type it. The live run "
                'of 2026-11-02 received it as text, `"dv": "23449609.75"` on `AM` for ESZ6, '
                "like the futures prices, so either form is read as the number.",
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
            _CODE_LIST,
        ),
        ("us_stocks_sip/quotes_v1", "indicators"): _CODE_LIST,
    }
)
"""`(source, wire key)` to the named conversion of a field its declared type does not fit."""

KEY_FIXES: Mapping[tuple[str, str], KeyFix] = MappingProxyType(
    {
        ("/vX/reference/tickers/{id}/events", "event_type"): KeyFix(
            wire_key="type",
            reason="The specification declares the event's type as the required `event_type`, "
            'but its response example and the documentation sample send `"type": '
            '"ticker_change"` on every event and never `event_type`. Massive\'s own Python '
            "client builds each event from exactly the keys `type`, `date`, and "
            "`ticker_change`, and would refuse an `event_type` key.",
        ),
    }
)
"""`(source, declared key)` to the wire key of a field every documented sample sends under it."""

_LIVE_RUN = "The live run of 2026-11-02 received it"
_DECIMAL_SIZE = (
    "The size of the trade including the fractional component. This is represented as a "
    "decimal string."
)
_VWAP_TEXT = "The volume weighted average price."
_ISSUER_SYMBOL = "The issuer's trading symbol, as the filing reports it."

UNDECLARED_FIELDS: Mapping[tuple[str, str], UndeclaredField] = MappingProxyType(
    {
        ("/v3/trades/{optionsTicker}", "decimal_size"): UndeclaredField(
            json_type="string",
            description=_DECIMAL_SIZE,
            reason=f'{_LIVE_RUN} on the options trades, `"decimal_size": "2.0"` for '
            "O:AAPL261204C00345000, as the stocks trades path declares it; the text is that "
            "path's.",
        ),
        ("/v2/last/trade/{optionsTicker}", "ds"): UndeclaredField(
            json_type="string",
            description=_DECIMAL_SIZE,
            reason=f'{_LIVE_RUN} on the options last trade, `"ds": "3.0"` for '
            "O:AAPL261204C00345000, as the stocks last trade declares it; the text is that "
            "path's.",
        ),
        **_for(
            FOREX_SNAPSHOTS,
            ("day.vw", "min.vw"),
            UndeclaredField(
                json_type="number",
                description=_VWAP_TEXT,
                reason=f"{_LIVE_RUN} on the forex full-market and movers snapshots, `day.vw` "
                "1.1199 and `min.vw` 1.1203 on the full-market one, as the stocks and crypto "
                "snapshots declare it; the text is theirs.",
            ),
        ),
        ("/v3/snapshot", "session.vwap"): UndeclaredField(
            json_type="number",
            description="The trading volume weighted average price for the session.",
            reason=f"{_LIVE_RUN} on the unified snapshot, `session.vwap` 341.4434 for a stock; "
            "the text follows the snapshot's own `last_minute.vwap`.",
        ),
        ("/v3/snapshot", "session.last_updated"): UndeclaredField(
            json_type="integer",
            description="The nanosecond timestamp of when this information was updated.",
            reason=f"{_LIVE_RUN} on the unified snapshot, `session.last_updated` "
            "1793633547860533354; the text is the snapshot's own `last_quote.last_updated`.",
        ),
        ("/v3/snapshot", "last_minute.last_updated"): UndeclaredField(
            json_type="integer",
            description="The nanosecond timestamp of when this information was updated.",
            reason=f"{_LIVE_RUN} on the unified snapshot, `last_minute.last_updated` "
            "1793633535088572302; the text is the snapshot's own `last_quote.last_updated`.",
        ),
        **_for(
            ("/stocks/filings/vX/form-3", "/stocks/filings/vX/form-4"),
            ("issuer_trading_symbol",),
            UndeclaredField(
                json_type="string",
                description=_ISSUER_SYMBOL,
                reason=f'{_LIVE_RUN} on the Form 3 and Form 4 filings, `"issuer_trading_symbol": '
                '"AAPL"` for Apple, the issuer\'s trading symbol of the SEC form.',
            ),
        ),
        ("/v3/reference/conditions", "sip_mapping.FINRA_TDDS"): UndeclaredField(
            json_type="string",
            description="Condition code from FINRA's Trade Data Dissemination Service (TDDS).",
            reason="The live run of 2026-11-03 received it on the conditions reference, "
            '`"FINRA_TDDS": "W"` in a condition\'s `sip_mapping`, beside the declared '
            "`CTA`, `OPRA`, and `UTP` codes; the text follows theirs.",
        ),
    }
)
"""`(source, wire key)` to a field Massive sends that its specification does not declare."""

OPTIONAL_FIELDS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("/v2/last/trade/{optionsTicker}", "y"): "The specification marks the participant "
        "timestamp required, but the live run of 2026-11-02 received an options last trade "
        "without it (O:AAPL261204C00345000). It is kept as an optional column, so its absence "
        "is not reported as drift.",
        **_for(
            ("/v3/snapshot/options/{underlyingAsset}",),
            (
                "break_even_price",
                "underlying_asset.change_to_break_even",
                *(
                    f"day.{key}"
                    for key in (
                        "open",
                        "high",
                        "low",
                        "close",
                        "previous_close",
                        "volume",
                        "vwap",
                        "change",
                        "change_percent",
                    )
                ),
            ),
            "The specification marks the contract's day bar and break-even price required, but "
            "the live run of 2026-11-03 received chain rows without them at about 10:52 New York "
            "time: rows among the first three SPX contracts (`limit=3`) lacked every `day` "
            "price, change, and volume field, `break_even_price`, and "
            "`underlying_asset.change_to_break_even`, and rows among the first twenty SPY calls "
            "(`contract_type=call&limit=20`) lacked the same `day` fields. They are kept as "
            "optional columns, so their absence is not reported as drift.",
        ),
    }
)
"""`(source, wire key)` to why a field the specification marks required is optional."""

# Massive's stock trade price, copied onto the crypto and options trade pages.
_SHARE_PRICE = (
    "The price of the trade. This is the actual dollar value per whole share of this trade. A "
    "trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00."
)
_CRYPTO_PRICE_TEXT = (
    "The price of the trade: the amount of the pair's second currency paid for one unit of its "
    "first (US dollars per bitcoin for X:BTCUSD)."
)
_CRYPTO_PRICE_REASON = (
    "Written here because no Massive text for a crypto trade's price is both right and "
    "unambiguous: the WebSocket crypto trade says only 'The price.', and the REST crypto trades "
    "say 'in the base currency of the crypto pair'. Massive's ticker reference defines the base "
    "currency as 'the currency that this asset is priced against' (`base_currency_symbol`), "
    "which makes that the second currency, while the usual pair convention makes the base the "
    "first currency, the one priced; the replacement names the currency explicitly and is "
    "right under either reading."
)
_CRYPTO_PRICE = DescriptionFix(
    vendor=_SHARE_PRICE,
    text=_CRYPTO_PRICE_TEXT,
    reason=f"Massive's stock text: a crypto pair has no shares and need not be priced in "
    f"dollars. {_CRYPTO_PRICE_REASON}",
)
_STOCK_TRADE_ID = (
    "The Trade ID which uniquely identifies a trade. These are unique per combination of "
    "ticker, exchange, and TRF. For example: A trade for AAPL executed on NYSE and a trade for "
    "AAPL executed on NASDAQ could potentially have the same Trade ID."
)
_CRYPTO_ID = DescriptionFix(
    vendor=_STOCK_TRADE_ID,
    text="The Trade ID which uniquely identifies a trade on the exchange that the trade "
    "happened on.",
    reason="Massive's stock text: a crypto trade has no trade reporting facility (TRF), and "
    "the example names a stock and two stock exchanges. The text is Massive's own for the "
    "crypto trade ID (`/v3/trades/{cryptoTicker}` `id`).",
)
_OPTIONS_PRICE_TEXT = (
    "The price of the trade. This is the price per share of the underlying, so the total "
    "dollar value of a trade is the price multiplied by the size and by the contract "
    "multiplier, which is typically 100 shares per contract."
)
_OPTIONS_PRICE_REASON = (
    "Massive's stock text: an option trades in contracts, and a contract's price is per share "
    "of the underlying, so the stock example's total understates the trade's value by the "
    "contract multiplier. The text is Massive's own for the options trade price "
    "(`/v3/trades/{optionsTicker}` `price`)."
)
_OPTIONS_PRICE = DescriptionFix(
    vendor=_SHARE_PRICE, text=_OPTIONS_PRICE_TEXT, reason=_OPTIONS_PRICE_REASON
)
_OPTIONS_ID_TEXT = (
    "The trade ID. Options trades do not have a trade ID, so this field is returned as an "
    "empty string."
)
_OPTIONS_ID_REASON = (
    "Massive's stock text, with a trade reporting facility and stock exchanges; the documented "
    "options last trade sends an empty ID. The text is Massive's own for the options trade ID "
    "(`/v3/trades/{optionsTicker}` `id`)."
)
_STOCK_EXCHANGES = "https://massive.com/docs/rest/stocks/market-operations/exchanges"
_OPTIONS_EXCHANGES = "https://massive.com/docs/rest/options/market-operations/exchanges"
_EXCHANGE_LEAD = "exchange ID. See Exchanges"
_EXCHANGE_TAIL = "for Massive's mapping of exchange IDs."
_OPTIONS_EXCHANGE_REASON = (
    "The link is to the stock exchanges, whose IDs are not the options venues' (the documented "
    "options last trade is on exchange 312); Massive's own options trades "
    "(`/v3/trades/{optionsTicker}` `exchange`) and options chain snapshot trade link the "
    "options exchanges, as here."
)


def _options_exchange(side: str = "") -> DescriptionFix:
    """Return the fix of an options exchange ID whose description links the stock exchanges.

    Args:
        side: `"ask side "` or `"bid side "` for a quote's exchange, empty for a trade's.

    Returns:
        The entry, linking the options exchanges instead.
    """
    lead = f"The {side}{_EXCHANGE_LEAD}"
    return DescriptionFix(
        vendor=f"{lead} ({_STOCK_EXCHANGES}) {_EXCHANGE_TAIL}",
        text=f"{lead} ({_OPTIONS_EXCHANGES}) {_EXCHANGE_TAIL}",
        reason=_OPTIONS_EXCHANGE_REASON,
    )


def _flat_file_conditions(asset: str) -> DescriptionFix:
    """Return the fix of a flat file's condition codes whose description links the stock ones.

    Args:
        asset: `"crypto"` or `"options"`, the asset class whose condition codes apply.

    Returns:
        The entry, linking that asset class's condition codes instead.
    """
    quoted = (
        "A list of condition codes. If there are multiple conditions, they will appear in "
        "quotations, separated by commas."
    )
    return DescriptionFix(
        vendor=f"{quoted} See Trade Conditions "
        "(https://massive.com/knowledge-base/article/stock-trade-conditions) for Massive\u2019s "
        "definitions of each condition.",
        text=f"{quoted} See {asset.capitalize()} Condition Codes "
        f"(https://massive.com/docs/rest/{asset}/market-operations/condition-codes) for what "
        "each code means.",
        reason=f"The link is to the stock trade conditions; Massive's own {asset} trades "
        f"(`/v3/trades/{{{asset}Ticker}}` `conditions`) link the {asset} condition codes, as "
        "here.",
    )


_TRADE_PARTICIPANT_TIME = DescriptionFix(
    vendor="The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp "
    "of when the quote was actually generated at the exchange.",
    text="The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of "
    "when the trade was actually generated at the exchange.",
    reason="The last trade's time is described as the quote's, Massive's text for a quote's "
    "participant timestamp; its trades (`/v3/trades/{optionsTicker}`, the stocks trades flat "
    "file) describe the same field of a trade as here.",
)
# Massive's futures bar dollar volume, which the WebSocket and flat-file bars misdescribe.
_FUTURES_DOLLAR_VOLUME_TEXT = (
    "The sum of price \u00d7 size across all trades in the window, expressed in the contract's "
    "quoted price units, which are the same units as the bar's open, high, low, and close. "
    "Despite the field name, no contract multiplier is applied, so this is not a notional "
    "dollar value. See the Products endpoint (https://massive.com/docs/rest/futures/products) "
    "for contract multiplier details. Because dollar_volume is a raw sum, you can use it to "
    "compute a volume-weighted average price (VWAP) over any span of bars: sum dollar_volume "
    "across the bars in the window and divide by the summed volume."
)
_FUTURES_DOLLAR_VOLUME_SOURCE = (
    "The text is Massive's own for the futures bar's dollar volume "
    "(`/futures/v1/aggs/{ticker}` `dollar_volume`), the same column of the bars kind."
)
_FUTURES_WS_DOLLAR_VOLUME = DescriptionFix(
    vendor="The total US dollar value of shares traded within the aggregate window.",
    text=_FUTURES_DOLLAR_VOLUME_TEXT,
    reason="Massive's stock text: a futures bar trades contracts, not shares, and its dollar "
    f"volume is not in US dollars. {_FUTURES_DOLLAR_VOLUME_SOURCE}",
)
_FUTURES_FILE_DOLLAR_VOLUME = DescriptionFix(
    vendor="The total dollar volume of the transactions that occurred within the timeframe.",
    text=_FUTURES_DOLLAR_VOLUME_TEXT,
    reason="Massive's own futures bars say this sum applies no contract multiplier and is not "
    f"a notional dollar value. {_FUTURES_DOLLAR_VOLUME_SOURCE}",
)
_FUTURES_PRICE = DescriptionFix(
    vendor="The price of the trade. This is the actual dollar value per whole contract of this "
    "trade. A trade of 100 contracts with a price of $2.00 would be worth a total dollar value "
    "of $200.00.",
    text="The trade price is quoted per unit of the underlying asset, with the total contract "
    "value determined by multiplying by the contract\u2019s specific multiplier.",
    reason="Massive's stock text with 'share' made 'contract': a futures price is quoted per "
    "unit of the underlying, so the example's total leaves out the contract multiplier, which "
    "Massive's own futures bars say their prices do not include. The text is Massive's own for "
    "the futures trade price (the `futures/T` channel's `p`).",
)
_FUTURES_FILES = "us_futures_{venue}"
# The unified snapshot serves every asset class, and its trade texts are the stock ones.
_UNIFIED_SNAPSHOT = "/v3/snapshot"

_PARAMETER_TEXT_REASON = (
    "Massive describes the response field with its request parameter's text (what passing it "
    "returns or filters), which says nothing about the value a row carries; the text says "
    "what the row's value means, keeping the vendor's own definition where it gives one."
)
"""Why a field described by its request parameter's text gets its own description."""

DESCRIPTION_FIXES: Mapping[tuple[str, str], DescriptionFix] = MappingProxyType(
    {
        ("/v1/last/crypto/{from}/{to}", "price"): _CRYPTO_PRICE,
        **_for(CRYPTO_SNAPSHOTS, ("lastTrade.p",), _CRYPTO_PRICE),
        ("global_crypto/trades_v1", "price"): _CRYPTO_PRICE,
        ("/v3/trades/{cryptoTicker}", "price"): DescriptionFix(
            vendor="The price of the trade in the base currency of the crypto pair.",
            text=_CRYPTO_PRICE_TEXT,
            reason="Ambiguous: Massive's own meaning of 'base currency' and the usual one name "
            f"opposite currencies of the pair. {_CRYPTO_PRICE_REASON}",
        ),
        **_for(CRYPTO_SNAPSHOTS, ("lastTrade.i",), _CRYPTO_ID),
        ("global_crypto/trades_v1", "id"): _CRYPTO_ID,
        ("global_crypto/trades_v1", "conditions"): _flat_file_conditions("crypto"),
        ("/v2/last/trade/{stocksTicker}", "y"): _TRADE_PARTICIPANT_TIME,
        ("/v2/last/trade/{optionsTicker}", "y"): _TRADE_PARTICIPANT_TIME,
        ("/v2/last/trade/{optionsTicker}", "p"): _OPTIONS_PRICE,
        ("/v3/snapshot/options/{underlyingAsset}", "last_trade.price"): _OPTIONS_PRICE,
        ("us_options_opra/trades_v1", "price"): _OPTIONS_PRICE,
        ("/v2/last/trade/{optionsTicker}", "i"): DescriptionFix(
            vendor=_STOCK_TRADE_ID, text=_OPTIONS_ID_TEXT, reason=_OPTIONS_ID_REASON
        ),
        ("/v2/last/trade/{optionsTicker}", "x"): _options_exchange(),
        ("options/T", "x"): _options_exchange(),
        ("us_options_opra/trades_v1", "exchange"): _options_exchange(),
        ("/v3/snapshot/options/{underlyingAsset}", "last_quote.ask_exchange"): _options_exchange(
            "ask side "
        ),
        ("/v3/snapshot/options/{underlyingAsset}", "last_quote.bid_exchange"): _options_exchange(
            "bid side "
        ),
        ("us_options_opra/trades_v1", "conditions"): _flat_file_conditions("options"),
        ("options/Q", "q"): DescriptionFix(
            vendor="The sequence number represents the sequence in which trade events happened. "
            "These are increasing and unique per ticker symbol, but will not always be "
            "sequential (e.g., 1, 2, 6, 9, 10, 11).",
            text="The sequence number represents the sequence in which quote events happened. "
            "These are increasing and unique per ticker symbol, but will not always be "
            "sequential (e.g., 1, 2, 6, 9, 10, 11).",
            reason="A quote's sequence is described as the trades' (the options trade "
            "channel's text); Massive's own options quotes (`/v3/quotes/{optionsTicker}` "
            "`sequence_number`) describe it as here.",
        ),
        (_UNIFIED_SNAPSHOT, "last_trade.price"): DescriptionFix(
            vendor=_SHARE_PRICE,
            asset_classes={"options": _OPTIONS_PRICE_TEXT},
            reason=f"For an option only, as on the options chain snapshot. {_OPTIONS_PRICE_REASON}",
        ),
        (_UNIFIED_SNAPSHOT, "last_trade.id"): DescriptionFix(
            vendor=_STOCK_TRADE_ID,
            asset_classes={"options": _OPTIONS_ID_TEXT},
            reason=f"For an option only, as on the options last trade. {_OPTIONS_ID_REASON}",
        ),
        (_UNIFIED_SNAPSHOT, "last_trade.exchange"): DescriptionFix(
            vendor=f"The {_EXCHANGE_LEAD} ({_STOCK_EXCHANGES}) {_EXCHANGE_TAIL}",
            asset_classes={
                "options": f"The {_EXCHANGE_LEAD} ({_OPTIONS_EXCHANGES}) {_EXCHANGE_TAIL}"
            },
            reason="For an option only, as on the options chain snapshot. "
            + _OPTIONS_EXCHANGE_REASON,
        ),
        (_UNIFIED_SNAPSHOT, "last_trade"): DescriptionFix(
            vendor="The most recent quote for this contract. This is only returned if your "
            "current plan includes trades.",
            text="The most recent trade for this contract. This is only returned if your "
            "current plan includes trades.",
            reason="The trade object is described as the quote: its own second sentence, and "
            "Massive's text for the same object on the options chain snapshot "
            "(`/v3/snapshot/options/{underlyingAsset}` `last_trade`), say trade. Every column "
            "of the object carries this text as its note. 'For this contract' stays: Massive "
            "uses it on most of the unified snapshot's objects (`details`, `greeks`, "
            "`last_quote`), and on a stock's row it is imprecise rather than another asset "
            "class's text, which is the bar for a fix.",
        ),
        (_UNIFIED_SNAPSHOT, "last_minute"): DescriptionFix(
            vendor="The most recent minute aggregate for this stock.",
            asset_classes={"options": "The most recent minute aggregate for this contract."},
            reason="The unified snapshot serves options too, and an option's row is a contract, "
            "not a stock. Written here because no Massive sibling exists: the options chain "
            "snapshot has no minute aggregate. Every column of the object carries this text as "
            "its note.",
        ),
        **_for(("futures/A", "futures/AM"), ("dv",), _FUTURES_WS_DOLLAR_VOLUME),
        **_for(
            (f"{_FUTURES_FILES}/session_aggs_v1", f"{_FUTURES_FILES}/minute_aggs_v1"),
            ("dollar_volume",),
            _FUTURES_FILE_DOLLAR_VOLUME,
        ),
        **_for(
            ("/futures/v1/trades/{ticker}", f"{_FUTURES_FILES}/trades_v1"),
            ("price",),
            _FUTURES_PRICE,
        ),
        ("/futures/v1/snapshot", "last_trade.price"): _FUTURES_PRICE,
        ("/futures/v1/snapshot", "details.product_code"): DescriptionFix(
            vendor="",
            text="The identifier for the contract's product.",
            reason="Massive gives the snapshot's product code no description, which leaves the "
            "`futures.snapshots` column blank. The text is Massive's own for the same field of "
            "the contracts endpoint (`/futures/v1/contracts` `product_code`); each snapshot "
            "row describes one contract, as each contracts row does.",
        ),
        ("/futures/v1/contracts", "date"): DescriptionFix(
            vendor="A date string in the format YYYY-MM-DD. This parameter will return "
            "point-in-time information about contracts for the specified day.",
            text="The day, YYYY-MM-DD, whose point-in-time state the row describes: whether the "
            "contract was tradeable that day (`active`) and its days to maturity from it.",
            reason=_PARAMETER_TEXT_REASON,
        ),
        ("/futures/v1/products", "date"): DescriptionFix(
            vendor="A date string in the format YYYY-MM-DD. This parameter will return "
            "point-in-time information about products for the specified day.",
            text="The day, YYYY-MM-DD, whose point-in-time state the row describes.",
            reason=_PARAMETER_TEXT_REASON,
        ),
        ("/futures/v1/schedules", "session_end_date"): DescriptionFix(
            vendor="The session end date for the schedules (also known as the trading date). "
            "This field is optional and can be used to filter results by a specific session "
            "end date. If left blank, schedules for all dates will be returned. Note that "
            "trading sessions end at 5 PM Central Time, so a session ending at 5 PM CT on "
            "January 1st would have a session_end_date of 2025-01-01.",
            text="The session end date (also known as the trading date) of the session the "
            "event belongs to. Trading sessions end at 5 PM Central Time, so a session ending "
            "at 5 PM CT on January 1st has a session_end_date of 2025-01-01.",
            reason=_PARAMETER_TEXT_REASON,
        ),
    }
)
"""`(source, wire key)` to the corrected description of a field whose vendor text is wrong,
or missing where a sibling source describes the same field."""


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
                "stocks/AM",
                "stocks/A",
                "us_stocks_sip/minute_aggs_v1",
                "us_stocks_sip/day_aggs_v1",
            ],
            "options": [
                *_bars("optionsTicker"),
                "options/AM",
                "options/A",
                "us_options_opra/minute_aggs_v1",
                "us_options_opra/day_aggs_v1",
            ],
            "futures": [
                "/futures/v1/aggs/{ticker}",
                "futures/AM",
                "futures/A",
                "us_futures_{venue}/minute_aggs_v1",
                "us_futures_{venue}/session_aggs_v1",
            ],
            "indices": [
                *_bars("indicesTicker"),
                "indices/AM",
                "indices/A",
                "us_indices/minute_aggs_v1",
                "us_indices/day_aggs_v1",
            ],
            "forex": [
                *_bars("forexTicker", grouped="global/market/fx"),
                "forex/CA",
                "forex/CAS",
                "global_forex/minute_aggs_v1",
                "global_forex/day_aggs_v1",
            ],
            "crypto": [
                *_bars("cryptoTicker", grouped="global/market/crypto"),
                "crypto/XA",
                "crypto/XAS",
                "global_crypto/minute_aggs_v1",
                "global_crypto/day_aggs_v1",
            ],
        },
        "values": {
            "indices": ["indices/V", "us_indices/values_v1"],
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
WebSocket channels in the order the specification's `x-polygon-order` gives them (the minute
aggregates before the second aggregates), then the flat-file prefixes.
"""

# Why a quote source that spells its prices as Massive's last-quote objects do (`ask`, `bid`)
# joins the quotes kind under the quotes data's spelling (`ask_price`, `bid_price`).
_QUOTE_PRICES = (
    "{source} spells the quote's prices `ask` and `bid`, as Massive's last-quote objects do, "
    "while the quotes data of every transport spells them `ask_price` and `bid_price`; in the "
    "quotes kind they take that spelling, so the merged table has one column per field. Every "
    "other field keeps its own name."
)

PROJECTIONS: Mapping[tuple[str, str], Projection] = MappingProxyType(
    {
        ("quotes", "/v1/last_quote/currencies/{from}/{to}"): Projection(
            renames={"ask": "ask_price", "bid": "bid_price"},
            reason=_QUOTE_PRICES.format(source="The forex REST last quote"),
        ),
        ("quotes", "crypto/XQ"): Projection(
            renames={"ap": "ask_price", "bp": "bid_price"},
            reason=_QUOTE_PRICES.format(
                source="The crypto quote stream, named after crypto's own last quote,"
            ),
        ),
    }
)
"""`(kind, source)` to the fields the source contributes to that kind, under the kind's names."""

_RUNNING = (
    "The WebSocket aggregates carry the day's running figures and the window's end; the REST "
    "and flat-file bars do not."
)
_LAST_TRADE = (
    "Only the REST options trades (the history and the last trade) carry it; the options "
    "stream and flat file do not."
)
_LAST_TRADE_ONLY = (
    "Only the REST options last trade carries it; the options trade history, stream, and flat "
    "file do not."
)


def _only(kind: str, column: str, reason: str, *groups: str) -> dict[tuple[str, str, str], str]:
    """Return the `TRANSPORT_ONLY` entries of one column in several groups of a kind.

    Args:
        kind: The dataset kind.
        column: The column.
        reason: Why one transport alone supplies it in each group.
        *groups: The asset-class groups.

    Returns:
        `(kind, group, column)` to `reason`, for every group.
    """
    return {(kind, group, column): reason for group in groups}


TRANSPORT_ONLY: Mapping[tuple[str, str, str], str] = MappingProxyType(
    {
        **_only("bars", "AccumulatedVolume", _RUNNING, "stocks", "options"),
        **_only("bars", "AccumulatedDecimalVolume", _RUNNING, "stocks"),
        **_only("bars", "AverageTradeSize", _RUNNING, "stocks", "options", "crypto"),
        **_only("bars", "DayVwap", _RUNNING, "stocks", "options"),
        **_only(
            "bars",
            "DecimalVolume",
            "The stocks WebSocket aggregates carry the fractional volume; the stocks REST and "
            "flat-file bars do not.",
            "stocks",
        ),
        **_only(
            "bars",
            "EndTimestamp",
            _RUNNING,
            "stocks",
            "options",
            "indices",
            "futures",
            "forex",
            "crypto",
        ),
        **_only("bars", "OfficialOpen", _RUNNING, "stocks", "options", "indices"),
        **_only(
            "bars",
            "Exchange",
            "Only the futures minute-bar flat files carry the exchange.",
            "futures",
        ),
        **_only(
            "bars",
            "Vwap",
            "The forex REST bars carry a volume-weighted average price; the forex WebSocket and "
            "flat-file bars do not.",
            "forex",
        ),
        **_only(
            "bars",
            "Pair",
            "The forex and crypto WebSocket aggregates name the instrument `pair`, their own long "
            "name, where REST and the flat files say `ticker`; it is the dataset's symbol key, "
            "which the key rename makes `Symbol` on every transport.",
            "forex",
            "crypto",
        ),
        **_only(
            "trades",
            "Pair",
            "The crypto WebSocket trade names the instrument `pair`, its own long name; it is the "
            "dataset's symbol key, which the key rename makes `Symbol`.",
            "crypto",
        ),
        **_only(
            "trades",
            "Ticker",
            "Of the crypto trades, only the flat file carries the instrument as `ticker` (REST "
            "takes it from the path or names it `symbol`, the WebSocket `pair`); it is the "
            "dataset's symbol key, which the key rename makes `Symbol`.",
            "crypto",
        ),
        **_only(
            "trades",
            "Timestamp",
            "The crypto REST last trade names its time `timestamp`, its own long name; it is that "
            "source's time key, which the key rename makes `Timestamp`.",
            "crypto",
        ),
        **_only("trades", "DecimalSize", _LAST_TRADE, "options"),
        **_only("trades", "Id", _LAST_TRADE, "options"),
        **_only("trades", "ParticipantTimestamp", _LAST_TRADE, "options"),
        **_only("trades", "Tape", _LAST_TRADE_ONLY, "options"),
        **_only("trades", "TrfId", _LAST_TRADE_ONLY, "options"),
        **_only("trades", "TrfTimestamp", _LAST_TRADE_ONLY, "options"),
        **_only(
            "trades", "Channel", "Only the futures REST trades carry the feed channel.", "futures"
        ),
        **_only(
            "trades",
            "Correction",
            "Only the futures trade flat files carry the correction indicator.",
            "futures",
        ),
        **_only(
            "trades", "Exchange", "Only the futures trade flat files carry the exchange.", "futures"
        ),
        **_only(
            "quotes",
            "Timestamp",
            "The forex REST last quote names its time `timestamp`, its own long name; it is that "
            "source's time key, which the key rename makes `Timestamp`.",
            "forex",
        ),
        **_only(
            "quotes", "Channel", "Only the futures REST quotes carry the feed channel.", "futures"
        ),
        **_only(
            "quotes", "Exchange", "Only the futures quote flat files carry the exchange.", "futures"
        ),
    }
)
"""`(kind, group, column)` to why the column is on one transport only in that group."""

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
