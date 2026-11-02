"""The description rules of dataset columns, and how every text names a transport.

Each column of a dataset schema carries one description, chosen for the dataset by the rules of
`column_description`, which `schema.dataset_schema` applies to every column it merges:

1. a column the dataset's own sources supply takes the first non-empty field description among
   them, trying the transports in `TRANSPORT_ORDER` (REST, flat files, WebSocket);
2. a column only other members of the dataset's kind supply says so and names them;
3. the library describes its own columns, the declared keys (`KEY_SENTENCES`) and the time
   columns it fills (`Date`, `TimestampStart`, `TimestampEnd`, `TimestampInsert`), each in a
   sentence of its own for the dataset, followed for the keys, `Date`, and `TimestampStart` by
   the wire key and quoted field description behind each source.

A dataset's note on a column follows its description. The derived columns no source carries,
`OPTION_ROOT` and `RAW_SYMBOL`, take the library's own sentences (`DERIVED_SENTENCES`), and
`OptionRoot` on a dataset of options and other rows ends with `OTHER_ROWS`. A column of nested
values kept as JSON text ends with `JSON_TEXT_NOTE`; `with_sentence` adds each closing sentence.

`TRANSPORT_LABELS` is how every text a user reads spells a transport, and `TRANSPORT_ORDER` the
one order in which a column's transports are listed: the key-column descriptions here,
`Column.supplied_by`, the `SuppliedBy` column of `columns()`, and the column reference all use
them. These sentences are the library's statements about its own columns, not hand input about
Massive, so they live here rather than in the refresh. The module holds only constants and pure
functions: no I/O, threads, Deephaven calls, or state.
"""

from __future__ import annotations

from datetime import UTC
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.dates import DAY_ZONE_NAMES, DAY_ZONES, NEW_YORK
from deephaven_massive._datasets.row_times import DATE, END, INSERT, START, TIMESTAMP, RowTimes
from deephaven_massive._datasets.source_columns import SourceColumn
from deephaven_massive._enums import AssetClass, ClassFromPrefix
from deephaven_massive._massive import Transport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import tzinfo

    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = [
    "DERIVED_SENTENCES",
    "JSON_TEXT_NOTE",
    "KEY_SENTENCES",
    "OPTION_ROOT",
    "OTHER_ROWS",
    "RAW_SYMBOL",
    "TRANSPORT_LABELS",
    "TRANSPORT_ORDER",
    "Contribution",
    "column_description",
    "with_sentence",
]


TRANSPORT_ORDER: Final = (Transport.REST, Transport.FLAT_FILES, Transport.WS)
"""The order in which a column's transports are listed and its descriptions are tried: REST
first, because its descriptions are the most complete."""

TRANSPORT_LABELS: Final[Mapping[Transport, str]] = MappingProxyType(
    {Transport.REST: "REST", Transport.FLAT_FILES: "flat files", Transport.WS: "WebSocket"}
)
"""How every text a user reads spells each transport: the key-column descriptions, the
`SuppliedBy` column of `columns()`, the column reference, and the `Transport` column of the
diagnostics tables all use it, so a filter copied from one works on another. The `Transport`
values stay internal names, which only logs and code show. Read-only."""

KEY_SENTENCES: Final[Mapping[str, str]] = {
    "Symbol": "The instrument, in the library's canonical symbol form.",
    TIMESTAMP: "The row's primary time, as an Instant.",
    "Underlying": (
        "The instrument Massive links the row to, exactly as Massive sends it; for an option, "
        "the linked instrument, not what the contract delivers."
    ),
}
"""The library's own sentence for each renamed key column of every dataset; its description
goes on to name the wire key or request parameter behind it on each transport."""

OPTION_ROOT: Final = "OptionRoot"
"""The derived column of every options dataset with a `Symbol`: the OCC root of the symbol."""

RAW_SYMBOL: Final = "RawSymbol"
"""The derived column of a dataset that reads its symbol from rows, in a session whose
`unreadable_symbols` is `LOG` (the default) or `IGNORE`: the text of a symbol the library could
not read."""

DERIVED_SENTENCES: Final[Mapping[str, str]] = {
    OPTION_ROOT: (
        "The OCC option root, read from `Symbol`: the symbol without its `O:` prefix and its "
        "last 15 characters (expiry, type, and strike), such as `SPXW` for "
        "`O:SPXW251219C06000000`. It is never mapped to the underlying: an adjusted contract "
        "keeps its own root, such as `AAPL1`."
    ),
    RAW_SYMBOL: (
        "The symbol exactly as Massive sent it, on a row whose spelling the library cannot "
        "read as the dataset's class (its `Symbol` is then null); null on every other row. "
        "Present unless the session's `unreadable_symbols` is `dm.OnError.RAISE`, which fails "
        "on such a row instead."
    ),
}
"""The library's description of each derived column."""

OTHER_ROWS: Final = "Null on a row of any other class."
"""The sentence that ends `OptionRoot`'s description on a dataset of options and other rows."""

JSON_TEXT_NOTE: Final = "The column holds Massive's value as JSON text."
"""The sentence that ends the description of every column of nested values kept as JSON text:
an object, a list of objects, or a value of several shapes (`WireType.JSON_TEXT`)."""

_QUOTED_ENDS: Final = ('."', '?"', '!"')
"""The endings of a quoted description that already closes a sentence, so the key column's
"From …" sentence takes no period after the closing quote."""

_ZONE_WORDS: Final[Mapping[tzinfo, str]] = {NEW_YORK: "New York", UTC: "UTC"}
"""How a day zone of `dates.DAY_ZONES` names a date in a description: "the New York date"."""

_ZONE_CLOCKS: Final[Mapping[str, str]] = {
    _ZONE_WORDS[zone]: name for zone, name in DAY_ZONE_NAMES.items()
}
"""How a day zone names a time of day in a description, by its date word: "00:00 New York
time" (`dates.DAY_ZONE_NAMES`)."""

_WINDOW_END: Final = (
    "The exclusive end of the bar's window, as an Instant: the first instant at which the bar "
    'is complete, so an as-of join on it, `t.aj(bars, on=["Symbol", "Timestamp>=TimestampEnd"])`, '
    "never attaches a bar that is still open. It is when the window closes, not when Massive "
    "published the bar, and Massive may correct a bar later."
)
"""The first sentences of every bars dataset's `TimestampEnd`."""

_INDEX_HOUR: Final = (
    "For indices that end is an hour later, 01:00 New York time, since whether an index day bar "
    "covers the New York or the Chicago day is not verified."
)
"""Why an index bar of a day or longer ends at 01:00 New York time."""

_NULL_DATE: Final = (
    "Joining an intraday row to the same day's daily bar on `Date` attaches values not known "
    "until that day ended, and a null `Date` matches another null, so join longer bars on "
    "`TimestampEnd`."
)
"""The join caveat every bars dataset's `Date` ends with."""

_INSERT_SENTENCE: Final = (
    "When the library received the row, as an Instant: the local clock when the REST page, "
    "flat-file block, or WebSocket frame that carried it arrived, the same for every row of one. "
    "It is not monotonic and is not Massive's own receive time; on history() rows it is the "
    "download time, and in a poll() table the latest poll's."
)
"""The description of every dataset's `TimestampInsert`."""

_FROM_PARTS: Final = frozenset({"Symbol", "Underlying", TIMESTAMP, START, DATE})
"""The library's columns whose description goes on to quote the wire key behind each source;
`TimestampEnd` does not, since the library recomputes it rather than keep Massive's."""


def _zone_word(spec: DatasetSpec) -> str | None:
    """Return how a dataset's day zone is named in its descriptions.

    Args:
        spec: The dataset.

    Returns:
        "New York" or "UTC" for a class with a day zone; None for futures, whose days are
        sessions, and for a dataset without a class, whose zone follows its symbols.
    """
    if not isinstance(spec.asset_class, AssetClass) or spec.asset_class not in DAY_ZONES:
        return None
    return _ZONE_WORDS[DAY_ZONES[spec.asset_class]]


def _date_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a tick or bar dataset's `Date`.

    Args:
        spec: A dataset whose rows carry `Date` (`RowTimes` other than `NONE`).

    Returns:
        The description.
    """
    zone = _zone_word(spec)
    times = spec.times
    if times is RowTimes.DAY:
        return f"The day the summary covers, as ISO yyyy-MM-dd text: a {zone} calendar day."
    if spec.asset_class is AssetClass.FUTURES:
        if times is RowTimes.TICK:
            return (
                "The futures session the row belongs to (its trading date), as ISO yyyy-MM-dd "
                "text: Massive's `session_end_date` from REST and the flat files. The WebSocket "
                "sends none, so a streamed row gets the session Massive's futures schedules "
                "place it in (between two sessions, the next one). A row they cannot place has "
                "a null Date, since the 17:00 Chicago rule can be wrong around a holiday, and "
                "the stream keeps running: a spread, a contract whose schedule lookup failed or "
                "did not load before the stream opened, or a row outside the loaded schedule "
                "logs a WARNING (once per product and date); a contract still being looked up "
                "(one a live watchlist or dm.ALL adds) logs at DEBUG only; a row with no "
                "readable symbol has a null Date too."
            )
        return (
            "The futures session of the bar (its trading date), as ISO yyyy-MM-dd text: "
            "Massive's `session_end_date` from REST and the flat files, and on the WebSocket the "
            "session as for futures trades; null for a bar of several sessions, a week or "
            f"longer, or an intraday bar that crosses 17:00 Chicago time. {_NULL_DATE}"
        )
    if times is RowTimes.TICK:
        return (
            f"The trading day of the row, as ISO yyyy-MM-dd text: the {zone} date of `Timestamp`."
        )
    where = (
        f"the {zone} date of `TimestampStart`"
        if zone is not None
        else "the date of `TimestampStart` in the day zone of the symbol's class (UTC for forex "
        "and crypto, New York otherwise)"
    )
    return (
        f"The trading day of the bar, as ISO yyyy-MM-dd text: {where} for an intraday or 1-day "
        "bar, and null for a bar that spans more than one date (several days, a week or longer, "
        f"or an intraday bar that crosses midnight). {_NULL_DATE}"
    )


def _start_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a bars dataset's `TimestampStart`.

    Args:
        spec: A dataset of bars (`RowTimes.BAR`) or daily summaries (`RowTimes.DAY`).

    Returns:
        The description, before the wire keys it quotes.
    """
    zone = _zone_word(spec)
    if spec.times is RowTimes.DAY:
        return (
            "The start of the day the summary covers, as an Instant: 00:00 "
            f"{_ZONE_CLOCKS[str(zone)]} of `Date`."
        )
    if spec.asset_class is AssetClass.FUTURES:
        return "The start of the bar's window, as an Instant: Massive's own window start."
    if zone is None:
        return (
            "The start of the bar the value is computed at, as an Instant. A bar of a day or "
            "longer starts at 00:00 of its window's first day in the day zone of the symbol's "
            "class (UTC for forex and crypto, New York otherwise)."
        )
    first = (
        "a week's Monday, a month's, quarter's, or year's first weekday"
        if spec.asset_class in (AssetClass.STOCKS, AssetClass.OPTIONS)
        else "a week's Sunday, a month's, quarter's, or year's 1st"
    )

    return (
        "The start of the bar's window, as an Instant. A bar of a day or longer starts at "
        f"00:00 {_ZONE_CLOCKS[zone]} of its window's first day ({first}), whichever source "
        "serves it; each "
        "source's own stamp, quoted next, is moved there."
    )


def _end_sentence(spec: DatasetSpec) -> str:
    """Return the library's description of a bars dataset's `TimestampEnd`.

    Args:
        spec: A dataset of bars (`RowTimes.BAR`) or daily summaries (`RowTimes.DAY`).

    Returns:
        The description.
    """
    zone = _zone_word(spec)
    indices = spec.asset_class is AssetClass.INDICES
    if spec.times is RowTimes.DAY:
        later = f" {_INDEX_HOUR}" if indices else ""
        return (
            "The exclusive end of the day the summary covers, as an Instant: 00:00 "
            f"{_ZONE_CLOCKS[str(zone)]} of the next day.{later}"
        )
    if spec.asset_class is AssetClass.FUTURES:
        return (
            f"{_WINDOW_END} An intraday bar ends its length after its start (a one-second "
            "WebSocket bar at the later of that and Massive's `e`); a bar of a session or "
            "longer at 17:00 Chicago time on its label, Massive's `session_end_date`, which is "
            "its last session. history() returns a bar of several sessions, or of a week or "
            "longer, only once no later session can still join it."
        )
    where = (
        f"00:00 {_ZONE_CLOCKS[zone]}"
        if zone is not None
        else "00:00, in the day zone of the symbol's class,"
    )
    later = f" {_INDEX_HOUR}" if indices else ""
    return (
        f"{_WINDOW_END} An intraday bar ends its length after its start; a bar of a day or longer "
        f"at {where} after its window's last day.{later}"
    )


def _library_sentence(spec: DatasetSpec, name: str) -> str:
    """Return the library's own sentence for one of a dataset's library columns.

    Args:
        spec: The dataset.
        name: A key column (`Symbol`, `Timestamp`, `Underlying`) or a time column the
            library fills (`Date`, `TimestampStart`, `TimestampEnd`, `TimestampInsert`).

    Returns:
        The sentence, before any wire keys it quotes.
    """
    if name == DATE:
        return _date_sentence(spec)
    if name == START:
        return _start_sentence(spec)
    if name == END:
        return _end_sentence(spec)
    if name == INSERT:
        return _INSERT_SENTENCE
    return KEY_SENTENCES[name]


Contribution = tuple[Transport, SourceColumn]
"""One column one source of a dataset contributes, with the source's transport."""


def _key_part(
    transport: Transport, column: SourceColumn, asset_class: str | None
) -> tuple[str, str]:
    """Say where one source's value of a renamed key column comes from.

    Args:
        transport: The source's transport.
        column: The source's contribution to the key column.
        asset_class: The dataset's asset class (an `AssetClass` value), or `None`; it picks
            the field's description (`Field.description_for`).

    Returns:
        The transport, spelled as `TRANSPORT_LABELS` spells it, and the rest of one item of the
        list `column_description` joins with "; ", with no closing punctuation:
        `` `sip_timestamp`: "<field description>"`` for a wire key with a description,
        `` `sip_timestamp` `` for one without, or ``: the request's `stocksTicker` parameter``
        for a key filled from the request. Sources whose rest is the same share one item.
    """
    label = TRANSPORT_LABELS[transport]
    if column.field is None:
        return label, f": the request's `{column.parameter}` parameter"
    described = column.field.description_for(asset_class)
    if described:
        return label, f' `{column.field.wire_key}`: "{described}"'
    return label, f" `{column.field.wire_key}`"


def column_description(
    spec: DatasetSpec,
    name: str,
    *,
    own: Sequence[Contribution],
    library: bool,
    json_text: bool,
) -> str:
    """Pick one column's description for one dataset (the rules of `dataset_schema`).

    Args:
        spec: The dataset.
        name: The column.
        own: The dataset's own contributions to the column, in description order.
        library: True when the library describes the column: one of the dataset's declared key
            columns, or a time column the library fills (`Date`, `TimestampStart`,
            `TimestampEnd`, `TimestampInsert`). A vendor field that merely carries one of those
            names in a dataset that has no such column of its own is described as any other.
        json_text: True for a column whose values are nested objects or lists kept as JSON
            text (`WireType.JSON_TEXT`), so the description says how to read the text.

    Returns:
        The description, with the dataset's note on the column (`DatasetSpec.column_notes`)
        after it, then `JSON_TEXT_NOTE` on a JSON-text column; empty when nothing describes
        the column.
    """
    text = _chosen(spec, name, own=own, library=library)
    for sentence in (spec.column_notes.get(name, ""), JSON_TEXT_NOTE if json_text else ""):
        text = with_sentence(text, sentence)
    return text


def with_sentence(text: str, sentence: str) -> str:
    """Return a description with one more sentence after it.

    Args:
        text: The description so far; may be empty, and the vendor's may lack its closing
            period.
        sentence: The sentence to add; empty adds nothing.

    Returns:
        `text`, closed with a period where it has no end of its own, then `sentence`.
    """
    if not text or not sentence:
        return text or sentence
    stop = "" if text.endswith((".", "!", "?", *_QUOTED_ENDS)) else "."
    return f"{text}{stop} {sentence}"


def _listed(labels: Sequence[str]) -> str:
    """Join transport labels for a sentence.

    Args:
        labels: At least one label.

    Returns:
        The last two joined by "and" and the rest by commas, such as "REST and flat files".
    """
    return ", ".join(labels[:-1]) + (" and " if labels[1:] else "") + labels[-1]


def _chosen(
    spec: DatasetSpec,
    name: str,
    *,
    own: Sequence[Contribution],
    library: bool,
) -> str:
    """Pick one column's description, before the dataset's note (`column_description`).

    Args:
        spec: The dataset.
        name: The column.
        own: The dataset's own contributions to the column, in description order.
        library: True when the library describes the column.

    Returns:
        The description; empty when no source describes the column.
    """
    # The wire spec keys its per-class descriptions by class value, and has none for a
    # dataset of every class.
    asset_class = None if spec.asset_class is ClassFromPrefix.ANY else spec.asset_class.value
    if library:
        # The transports whose source reads the same, in source order, by what they read.
        sources: dict[str, list[str]] = {}
        if name in _FROM_PARTS:
            for transport, column in own:
                label, rest = _key_part(transport, column, asset_class)
                labels = sources.setdefault(rest, [])
                if label not in labels:
                    labels.append(label)
        parts = [f"{_listed(labels)}{rest}" for rest, labels in sources.items()]
        sentence = _library_sentence(spec, name)
        if not parts:
            return sentence
        # One sentence after the library's own, so a lowercase label never starts a sentence.
        # A quote that already ends the sentence inside it gets no second period after it.
        listed = "; ".join(parts)
        end = "" if listed.endswith(_QUOTED_ENDS) else "."
        return f"{sentence} From {listed}{end}"
    for _, column in own:
        described = column.field.description_for(asset_class) if column.field else ""
        if described:
            return described
    return ""
