"""The field rules every generator shares: names, representation, units, conversions, examples.

A generator (`rest_sources`, `ws_sources`, `flat_file_sources`) finds a source's fields in its
vendor document and hands each one to `SourceFields` as a `RawField`: the dotted wire key, its
schema (an OpenAPI schema, or one built from a documentation table), and whether the vendor marks it
required. `SourceFields` turns it into a `spec.types.Field` by the same rules on every transport, so
one field gets one column and one type whichever transport serves it:

- **Wire keys.** A field is read under the key its source declares, unless a `KEY_FIXES`
  entry gives the key every documented sample sends in its place; from then on the field is
  that key, for its name and every other hand entry.
- **Names.** Each segment of a wire key is translated to Massive's long form: the vendor's own
  long key as it is; a short key through the lexicon of the object it sits in (`SHAPES`),
  whose entries cite the endpoint that spells the same field out; else an identity entry. The
  last segment is replaced by its `NAMES` entry when Massive names the key nowhere (a name
  derived from the field's description, which the entry quotes). `naming.column_name` joins
  the long names in PascalCase: `prevDay.vw` becomes `PrevDayVwap`.
- **Representation rules** decide each field's `WireType` (`representation`): an integer, or a
  number of format `integer`, `int32`, or `int64`, is `long`, or an `epoch` when `UNITS` gives
  it a unit; any other number is `double`; a boolean is `bool`; an array whose items those rules
  make a `long` is `long-array`, and one whose items are plain `string` is `string-array`; an
  array of anything else, a free-form object, and a choice of two or more shapes are
  `json-text`; a string with `format: date-time`, or no format and a name ending in `_utc`, is
  an `iso-instant`; a string with `format: date`, or named `date` or `*_date`, is a `date`; any
  other string is `string`. A `TYPE_FIXES` entry then gives the field its named `Conversion`,
  whose type replaces the rule's.
- **Units.** A timestamp-like field (a unit tag, a field documented as a timestamp, or a
  timestamp-like long name) is never left to a default: a `long` or `double` needs a `UNITS`
  entry, and so does text with no `format` unless a `TYPE_FIXES` entry types it.
- **Examples.** Every documented example value must convert through its generated field, or
  the `example-type` gate stops the refresh, unless a `SAMPLE_ERRORS` entry says the sample
  is wrong.
- **Descriptions.** Each field records the vendor's description as plain text (`plain_text`),
  or the correction its `DESCRIPTION_FIXES` entry gives when the vendor's is clearly wrong for
  the source: a replacement, or per-asset-class texts (`Field.class_descriptions`) for a source
  that serves several classes. No name, type, or gate depends on it, except that a `NAMES`
  entry quotes the one its name was derived from and a `DESCRIPTION_FIXES` entry the one it
  replaces. A field inside a nested object also carries the description of each enclosing
  object that has one (`RawField.notes`), outermost first, each after the field's own text
  as a sentence naming the object: ``From the `session` object: <its text>``. The vendor
  often documents a whole object's behavior only there (a `session` whose prices are
  omitted before the regular open, a `lastQuote` returned only on some plans, `greeks` left
  out for some contracts), and the flattened columns would otherwise lose it. The note is the
  object's whole text, never cut: no rule that shortened it could tell the sentence that
  explains a null from the one that introduces the object. A `DESCRIPTION_FIXES` entry under
  the object's wire key corrects its note as one under a field's key corrects the field's
  text, and one quoting `""` gives a note to an object the vendor leaves undescribed. A
  field a `KEY_FIXES` entry moves out of an object keeps only the notes of the objects that
  still enclose it.

Hand input is keyed by source: a REST path template, a WebSocket `"<cluster>/<channel>"`, or a
flat-file prefix, so one table serves every transport. A `SourceFields` collects its problems
instead of stopping, and records what it consumed (`gates.PathUse`) for the checks that span
sources. It imports the standard library and other `_massive` modules only.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import naming
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem
from deephaven_massive._massive.refresh.openapi import text
from deephaven_massive._massive.refresh.plain_text import plain_text
from deephaven_massive._massive.spec.convert import UnconvertibleError, convert, convert_field
from deephaven_massive._massive.spec.types import Field, WireType, WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from deephaven_massive._massive.refresh.hand_input import HandInput
    from deephaven_massive._massive.refresh.openapi import Json, JsonObject, OpenApi

__all__ = ["RawField", "SourceFields", "openapi_field", "representation"]

# The `x-polygon-go-type` names that tag an integer with a unit.
_TAG_UNITS: Final = {
    "INanoseconds": WireUnit.NS,
    "IMicroseconds": WireUnit.US,
    "IMilliseconds": WireUnit.MS,
}
# The types of the scalar schema types.
_SCALAR_TYPES: Final = {
    "integer": WireType.LONG,
    "number": WireType.DOUBLE,
    "boolean": WireType.BOOL,
}

# The formats that make a `number` an integer. The vendor declares a few ids as a `number` of
# format `int32` (the option chain's quote exchange ids), and an id is a `long` everywhere, so
# it joins the reference table of its ids with no cast.
_INTEGER_FORMATS: Final = frozenset({"integer", "int32", "int64"})

# The type of an array by the type its items take by the scalar rules; an array of anything else
# is `json-text`. Lists of integers and of plain text become Deephaven arrays (`long[]`,
# `String[]`), which `ungroup` and the query language's array functions read directly.
_ARRAY_TYPES: Final = {
    WireType.LONG: WireType.LONG_ARRAY,
    WireType.STRING: WireType.STRING_ARRAY,
}


@dataclass(frozen=True)
class RawField:
    """A leaf of a source's rows before it is named and typed.

    Attributes:
        wire_key: The dotted key below the rows (`day.c`), or a CSV header.
        schema: The leaf's resolved schema: its `type`, `format`, `description`, `example`,
            and `x-polygon-go-type`, as the specification gives them or as a generator builds
            them from a documentation table.
        required: Whether the vendor marks it, and every object above it, required.
        timestamp: Whether the vendor documents it as a timestamp in so many words (a
            flat-file "Columns" type `timestamp - integer`), which makes it timestamp-like
            whatever its name.
        items: The resolved schema of an array's items; `None` for any other field.
        notes: The enclosing objects, outermost first, each as its dotted wire key below the
            rows (`session`) and its description as the specification gives it (`""` when it
            gives none, which adds no note unless a `DESCRIPTION_FIXES` entry gives one);
            empty for a field at the top of the row.
    """

    wire_key: str
    schema: JsonObject
    required: bool
    timestamp: bool = False
    items: JsonObject | None = None
    notes: tuple[tuple[str, str], ...] = ()


def openapi_field(
    spec: OpenApi,
    wire_key: str,
    schema: JsonObject,
    *,
    required: bool,
    where: str = "",
    notes: tuple[tuple[str, str], ...] = (),
) -> RawField:
    """Return a field of an OpenAPI document, with an array's items resolved.

    A `RefreshError` from following the items' reference propagates.

    Args:
        spec: The document the schema comes from.
        wire_key: The field's dotted wire key.
        schema: The field's resolved schema.
        required: Whether the vendor marks it, and every object above it, required.
        where: The source and key, for messages; the wire key when omitted.
        notes: The enclosing objects (`RawField.notes`); none by default.

    Returns:
        The field.
    """
    items = None
    if schema.get("type") == "array":
        items = spec.schema(schema.get("items", {}), where or wire_key)
    return RawField(wire_key, schema, required, items=items, notes=notes)


def representation(schema: JsonObject, last: str) -> WireType | None:
    """Apply the representation rules to a field that is not an array.

    Args:
        schema: The field's resolved schema.
        last: The last segment of its wire key, as the vendor spells it.

    Returns:
        The field's type, or `None` for a schema type the rules do not cover.
    """
    kind = schema.get("type")
    form = schema.get("format")
    if "oneOf" in schema or "anyOf" in schema or kind == "object":
        return WireType.JSON_TEXT
    if kind == "string":
        if form == "date-time" or (form is None and last.endswith("_utc")):
            return WireType.ISO_INSTANT
        dated = form == "date" or last == "date" or last.endswith("_date")
        return WireType.DATE if dated else WireType.STRING
    if kind == "number" and form in _INTEGER_FORMATS:
        return WireType.LONG
    return _SCALAR_TYPES.get(kind) if isinstance(kind, str) else None


class SourceFields:
    """Name and type the fields of one source, collecting its problems instead of stopping.

    A generator creates one per source it attempts and passes each `RawField` to `field`;
    `use` then reports what the source consumed, even when it stopped on a problem, so its
    hand entries are not also reported stale. Used by one thread, the refresh's.

    Example:
        ::

            from deephaven_massive._massive.refresh.fields import RawField, SourceFields
            from deephaven_massive._massive.refresh.hand_input import HandInput

            rules = SourceFields(HandInput(), "stocks/T", origin="https://massive.com")
            price = rules.field(RawField("price", {"type": "number"}, required=True))
            assert price is not None and price.column == "Price"
    """

    def __init__(self, hand: HandInput, source: str, *, origin: str) -> None:
        """Prepare to generate the fields of `source`.

        Args:
            hand: The hand input.
            source: The source key the hand input names it by.
            origin: The documentation origin a root-relative link in a description is
                resolved against (`plain_text`).
        """
        self.hand = hand
        self.origin = origin
        self.source = source
        self.problems: list[Problem] = []
        self.wire_keys: set[str] = set()
        self.objects: set[str] = {""}
        self.unit_bases: set[str] = set()
        self.identities: set[str] = set()
        self.concepts: dict[str, str] = {}
        self.columns: dict[str, str] = {}
        # Each field's own documented `example`, by wire key.
        self.field_examples: dict[str, Json] = {}
        self.converted: set[str] = set()
        # Each generated field's type by the representation rules, before any conversion.
        self.declared_types: dict[str, WireType] = {}
        self.needed_conversions: set[str] = set()
        self.example_keys: set[str] = set()
        # The declared key of each field a `KEY_FIXES` entry reads under another, by wire key.
        self.declared_keys: dict[str, str] = {}
        # The wire keys whose `DESCRIPTION_FIXES` entry `description` looked up.
        self.described_keys: set[str] = set()
        # Each enclosing object's note and per-asset-class notes, by its wire key, once looked
        # up.
        self.notes: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {}

    def use(self) -> PathUse:
        """Return what the source consumed so far, for the checks that span sources.

        Returns:
            The wire keys, objects, `PARAM_UNITS` bases, identity keys, concepts, and the
            fields whose conversion a documented example needs.
        """
        return PathUse(
            wire_keys=frozenset(self.wire_keys),
            objects=frozenset(self.objects),
            unit_bases=frozenset(self.unit_bases),
            identities=frozenset(self.identities),
            concepts=dict(self.concepts),
            columns=dict(self.columns),
            converted=frozenset(self.converted),
            needed_conversions=frozenset(self.needed_conversions),
            example_keys=frozenset(self.example_keys),
            described_keys=frozenset(self.described_keys),
        )

    def problem(self, gate: Gate, key: str, message: str, *, description: str = "") -> None:
        """Record a problem of this source.

        Args:
            gate: The gate.
            key: The wire key or parameter.
            message: What is wrong.
            description: The vendor's description, if any.
        """
        self.problems.append(
            Problem(
                gate=gate, source=self.source, key=key, message=message, description=description
            )
        )

    def long_names(self, raw: RawField) -> list[str]:
        """Translate each segment of a wire key to its long name (lexicon, identity, NAMES).

        Args:
            raw: The field.

        Returns:
            The long name of each segment; a short key that nothing translates is kept as is,
            with a `short-key` problem.
        """
        segments = raw.wire_key.split(".")
        names: list[str] = []
        for index, segment in enumerate(segments):
            prefix = ".".join(segments[:index])
            lexicon_name = self.hand.shapes.get((self.source, prefix))
            entries = self.hand.lexicons[lexicon_name].entries if lexicon_name else {}
            override = self.hand.names.get((self.source, raw.wire_key))
            if index == len(segments) - 1 and override is not None:
                quoted = " ".join(override.description.split())
                if quoted != " ".join(text(raw.schema, "description").split()):
                    self.problem(
                        Gate.STALE_ENTRY,
                        raw.wire_key,
                        "NAMES quotes a description the specification no longer gives",
                        description=text(raw.schema, "description"),
                    )
                coined = naming.column_name([override.long_name])
                if naming.is_key_column(coined):
                    self.problem(
                        Gate.COLUMN_COLLISION,
                        raw.wire_key,
                        f"NAMES coins the library column {coined!r}; only the dataset key "
                        "rename and the library's time columns produce it",
                    )
                names.append(override.long_name)
            elif segment in entries:
                names.append(entries[segment].long_name)
            else:
                if segment in self.hand.identity:
                    self.identities.add(segment)
                elif naming.is_short(segment):
                    self.problem(
                        Gate.SHORT_KEY,
                        raw.wire_key,
                        f"short key {segment!r} has no lexicon, identity, or NAMES entry",
                        description=text(raw.schema, "description"),
                    )
                names.append(segment)
        return names

    def wire_type(self, raw: RawField) -> WireType | None:
        """Apply the representation rules to a field, before units and type fixes.

        Args:
            raw: The field.

        Returns:
            The field's type, or `None` (with a problem) for a schema with no usable type.
        """
        if raw.schema.get("type") == "array":
            # The items take the scalar rules, without the name rules of the field itself.
            item = representation(raw.items or {}, "")
            return _ARRAY_TYPES.get(item, WireType.JSON_TEXT) if item else WireType.JSON_TEXT
        found = representation(raw.schema, raw.wire_key.rsplit(".", 1)[-1])
        if found is None:
            self.problem(
                Gate.SHAPE, raw.wire_key, f"unsupported field type {raw.schema.get('type')!r}"
            )
        return found

    def field(self, raw: RawField) -> Field | None:
        """Name and type one field, checking its wire key, name, unit, and type fix.

        Args:
            raw: The field, under the key its source declares.

        Returns:
            The generated field, under its `KEY_FIXES` key if it has one, or `None` when its
            type cannot be decided.
        """
        self.wire_keys.add(raw.wire_key)
        key_fix = self.hand.key_fixes.get((self.source, raw.wire_key))
        if key_fix is not None:
            self.declared_keys[key_fix.wire_key] = raw.wire_key
            # The field now sits where the samples send it, so only the objects that still
            # enclose it there keep their notes.
            kept = tuple(n for n in raw.notes if key_fix.wire_key.startswith(n[0] + "."))
            raw = dataclasses.replace(raw, wire_key=key_fix.wire_key, notes=kept)
            self.wire_keys.add(raw.wire_key)
        long_names = self.long_names(raw)
        spec_type = self.wire_type(raw)
        if spec_type is None:
            return None
        description = text(raw.schema, "description")
        self.declared_types[raw.wire_key] = spec_type
        if "example" in raw.schema:
            self.field_examples[raw.wire_key] = raw.schema["example"]
        entry = self.hand.type_fixes.get((self.source, raw.wire_key))
        fix = entry.conversion if entry is not None else None
        if fix is not None and spec_type not in fix.declarations:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"TYPE_FIXES expects a vendor type in {sorted(fix.declarations)}, but the "
                f"specification now gives {spec_type}",
                description=description,
            )
            fix = None
        unit = self.unit(
            raw,
            spec_type,
            long_name=long_names[-1],
            description=description,
            fixed=fix is not None,
        )
        if unit is not None and fix is not None:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"UNITS gives a unit to a field TYPE_FIXES converts to {fix.wire_type}",
                description=description,
            )
            return None
        if unit is not None:
            wire_type = WireType.EPOCH
        else:
            wire_type = fix.wire_type if fix is not None else spec_type
        required = raw.required
        if (self.source, raw.wire_key) in self.hand.optional_fields:
            if not required:
                self.problem(
                    Gate.STALE_ENTRY,
                    raw.wire_key,
                    "OPTIONAL_FIELDS entry for a field the specification does not mark required",
                    description=description,
                )
            required = False
        described, class_texts = self.noted(raw, description)
        column = naming.column_name(long_names)
        self.concepts[column] = ".".join(naming.snake(name) for name in long_names)
        self.columns[raw.wire_key] = column
        return Field(
            wire_key=raw.wire_key,
            column=column,
            wire_type=wire_type,
            unit=unit,
            required=required,
            conversion=fix,
            description=described,
            class_descriptions=class_texts,
        )

    def noted(self, raw: RawField, vendor: str) -> tuple[str, tuple[tuple[str, str], ...]]:
        """Return a field's description followed by the notes of its enclosing objects.

        The field's own text and each object's note are corrected by their own
        `DESCRIPTION_FIXES` entries (`description`); an object's note is looked up once per
        source, so a stale entry on it is reported once. Each note reads
        ``From the `<object wire key>` object: <text>``.

        Args:
            raw: The field, under its `KEY_FIXES` key if it has one.
            vendor: The field's own description, as the specification gives it.

        Returns:
            The description and the per-asset-class descriptions (`Field.class_descriptions`).
            A class that the field's entry or any note's entry lists gets its own text: the
            field's text and each note for that class, the shared one where an entry lists
            no text for it.
        """
        own, own_classes = self.description(raw.wire_key, vendor)
        notes: list[tuple[str, str, dict[str, str]]] = []
        for key, note in raw.notes:
            if key not in self.notes:
                self.notes[key] = self.description(key, note)
            text, classes = self.notes[key]
            notes.append((key, text, dict(classes)))
        by_class = dict(own_classes)
        listed = sorted({*by_class, *(name for _, _, classes in notes for name in classes)})

        def joined(asset_class: str) -> str:
            # `""` names no class, so it reads the shared texts. An object without a text,
            # for this class or at all, adds no note.
            parts = [
                f"From the `{key}` object: {note}"
                for key, text, classes in notes
                if (note := classes.get(asset_class, text))
            ]
            return _with_notes(by_class.get(asset_class, own), parts)

        return joined(""), tuple((name, joined(name)) for name in listed)

    def description(self, wire_key: str, vendor: str) -> tuple[str, tuple[tuple[str, str], ...]]:
        """Return a field's description as plain text, corrected where the vendor's is wrong.

        Records the key as looked up (`PathUse.described_keys`) when it has a
        `DESCRIPTION_FIXES` entry, so an entry under any other key is reported stale.

        Args:
            wire_key: The field's wire key, after any `KEY_FIXES` entry, or an enclosing
                object's, for its note (`noted`).
            vendor: The vendor's description, as the specification gives it.

        Returns:
            The field's description and its per-asset-class descriptions
            (`Field.class_descriptions`). Without an entry, the vendor's description as plain
            text (`plain_text`) and no class descriptions. With an entry that quotes it, the
            entry's `text`, or the vendor's text and the entry's `asset_classes` sorted by
            class. An entry that quotes anything else is stale: the problem is recorded, with
            both texts, and the vendor's text kept.
        """
        text = plain_text(vendor, origin=self.origin)
        entry = self.hand.description_fixes.get((self.source, wire_key))
        if entry is None:
            return text, ()
        self.described_keys.add(wire_key)
        if " ".join(entry.vendor.split()) != text:
            self.problem(
                Gate.STALE_ENTRY,
                wire_key,
                "DESCRIPTION_FIXES quotes a description the vendor no longer gives: "
                f"{entry.vendor!r}",
                description=text,
            )
            return text, ()
        if entry.text is not None:
            return entry.text, ()
        return text, tuple(sorted((entry.asset_classes or {}).items()))

    def unit(
        self,
        raw: RawField,
        wire_type: WireType,
        *,
        long_name: str,
        description: str,
        fixed: bool,
    ) -> WireUnit | None:
        """Decide a field's unit from `UNITS`, checking the timestamp unit and the unit tag.

        A field is timestamp-like when the specification tags it with a unit, the vendor
        documents it as a timestamp (`RawField.timestamp`), or its long name is
        (`naming.is_timestamp_like`). A timestamp-like `long` or `double` needs a `UNITS`
        entry, and so does timestamp-like text with no `format`, unless a `TYPE_FIXES` entry
        types it: nothing timestamp-like is left to a default.

        Args:
            raw: The field.
            wire_type: Its type by the representation rules.
            long_name: The long name of its last segment.
            description: The vendor's description.
            fixed: Whether a `TYPE_FIXES` entry names the field.

        Returns:
            The unit of an epoch, or `None` for any other field.
        """
        key = (self.source, raw.wire_key)
        entry = self.hand.units.get(key)
        tag = raw.schema.get("x-polygon-go-type")
        tag_name = tag.get("name") if isinstance(tag, dict) else None
        tag_unit = _TAG_UNITS.get(tag_name) if isinstance(tag_name, str) else None
        timestamp_like = (
            tag_unit is not None or raw.timestamp or naming.is_timestamp_like(long_name)
        )
        untyped_text = wire_type is WireType.STRING and "format" not in raw.schema
        gated = wire_type in {WireType.LONG, WireType.DOUBLE} or (untyped_text and not fixed)
        if entry is None:
            if timestamp_like and gated:
                self.problem(
                    Gate.TIMESTAMP_UNIT,
                    raw.wire_key,
                    f"timestamp-like {wire_type} (tag {tag_name}) has no UNITS entry"
                    + (" or TYPE_FIXES entry" if untyped_text else ""),
                    description=description,
                )
            return None
        if entry.unit is not None and wire_type is not WireType.LONG:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"UNITS gives a unit to a {wire_type} field; only an integer is an epoch",
                description=description,
            )
            return None
        if wire_type not in {WireType.LONG, WireType.DOUBLE, WireType.STRING}:
            self.problem(
                Gate.STALE_ENTRY,
                raw.wire_key,
                f"UNITS names a {wire_type} field",
                description=description,
            )
            return None
        conflict = entry.unit is not None and tag_unit is not None and entry.unit is not tag_unit
        if conflict and key not in self.hand.tag_conflicts:
            self.problem(
                Gate.UNITS,
                raw.wire_key,
                f"UNITS says {entry.unit} but the specification tags {tag_name}; add a "
                "TAG_CONFLICTS entry with the reason the tag is wrong",
                description=description,
            )
        if not conflict and key in self.hand.tag_conflicts:
            self.problem(
                Gate.STALE_ENTRY, raw.wire_key, "TAG_CONFLICTS entry, but UNITS agrees with the tag"
            )
        return entry.unit

    def check_columns(self, fields: Sequence[Field]) -> None:
        """Report two fields with one column.

        Args:
            fields: The source's generated fields.
        """
        seen: dict[str, str] = {}
        for item in fields:
            if item.column in seen:
                self.problem(
                    Gate.COLUMN_COLLISION,
                    item.wire_key,
                    f"column {item.column!r} is also the column of {seen[item.column]!r}",
                )
            else:
                seen[item.column] = item.wire_key

    def check_examples(
        self, fields: Sequence[Field], rows: Sequence[Json], *, documented: bool, where: str
    ) -> None:
        """Convert every documented example value of the source by its generated field.

        The specification documents the wire twice: an example of the message or response,
        and a field's own `example`. A value that the field refuses means the generated type
        and the documented wire disagree, and real data shaped like the sample would fail at
        run time, so the `example-type` gate stops the refresh instead. A person then decides:
        a `TYPE_FIXES` entry when the type is wrong, or a `SAMPLE_ERRORS` entry when the
        sample is. A missing field and a null value are not checked; each field is reported
        once.

        It also records which fields' conversions some example value needs (a value the
        field's declared type alone refuses), for the stale-entry check of `TYPE_FIXES`; it
        reports an example in which no value sits where the rows are, since the gate would
        otherwise check nothing there without saying so; and it reports a `KEY_FIXES` entry
        as stale once no example row sends its key, or one sends the declared key.

        Args:
            fields: The source's generated fields.
            rows: The example's rows, each a JSON object keyed like the fields (empty or
                `[None]` when there is no example).
            documented: Whether the specification gives an example at all.
            where: Where the example's rows were looked for, for the message (`"under
                result key 'results'"`).
        """
        found = {
            item.wire_key: [_example_value(row, item.wire_key) for row in rows] for item in fields
        }
        if documented and not any(v is not None for vs in found.values() for v in vs):
            self.problem(
                Gate.EXAMPLE_TYPE,
                "",
                f"the specification's example has no field values {where}, so none could be "
                "checked",
            )
        for wire_key, declared in sorted(self.declared_keys.items()):
            sent = any(_example_value(row, wire_key) is not None for row in rows)
            if not sent or any(_example_value(row, declared) is not None for row in rows):
                self.problem(
                    Gate.STALE_ENTRY,
                    declared,
                    f"KEY_FIXES reads {declared!r} as {wire_key!r}, but the specification's "
                    f"example no longer sends {wire_key!r} instead",
                )
        for item in fields:
            own = self.field_examples.get(item.wire_key)
            values = [v for v in [*found[item.wire_key], own] if v is not None]
            self.check_values(item, values)

    def check_undeclared(self, example: Json, declared: Iterable[str]) -> None:
        """Report the keys a documented example sends that the source does not declare.

        Such a key is data the decoder would drop as drift on every row, so the
        `example-type` gate stops the refresh until the source declares the field or a
        `SAMPLE_ERRORS` entry, naming the key and its sample, says the example is wrong.

        Args:
            example: The documented example, a JSON object (anything else has no keys).
            declared: The keys the source declares, and any key it leaves out on purpose.
        """
        known = set(declared)
        for key, value in example.items() if isinstance(example, dict) else ():
            if key in known:
                continue
            excused = any(
                (error.path, error.wire_key) == (self.source, key)
                and _json_text(error.sample) == _json_text(value)
                for error in self.hand.sample_errors
            )
            if excused:
                self.example_keys.add(key)
            else:
                self.problem(
                    Gate.EXAMPLE_TYPE,
                    key,
                    f"the specification's example sends {key!r} ({json.dumps(value)[:80]}), "
                    "which the source does not declare; declare it, or add a SAMPLE_ERRORS "
                    "entry if the example is wrong",
                )

    def check_values(self, item: Field, values: Sequence[Json]) -> None:
        """Convert one field's documented example values, honouring `SAMPLE_ERRORS`.

        Args:
            item: The generated field.
            values: Its non-null example values, the message's or response's first, then its
                own `example`.
        """
        if item.conversion is not None:
            self.converted.add(item.wire_key)
        if item.conversion is not None and any(
            not _converts(self.declared_types[item.wire_key], v) for v in values
        ):
            self.needed_conversions.add(item.wire_key)
        refused = [v for v in values if not _converts_field(item, v)]
        for error in self.hand.sample_errors:
            if (error.path, error.wire_key) != (self.source, item.wire_key):
                continue
            # Samples compare as JSON text, so `1`, `1.0`, and `true` stay three samples.
            wrong = _json_text(error.sample)
            if any(_json_text(v) == wrong for v in refused):
                refused = [v for v in refused if _json_text(v) != wrong]
            else:
                self.problem(
                    Gate.STALE_ENTRY,
                    item.wire_key,
                    f"SAMPLE_ERRORS entry for the sample {json.dumps(error.sample)[:80]}, which "
                    "the specification no longer gives or the field now accepts",
                )
        if refused:
            self.problem(
                Gate.EXAMPLE_TYPE,
                item.wire_key,
                f"the specification's example value {json.dumps(refused[0])[:80]} does not "
                f"convert to {item.wire_type}; add a TYPE_FIXES entry citing it, or a "
                "SAMPLE_ERRORS entry if the sample is wrong",
            )


def _with_notes(text: str, notes: Sequence[str]) -> str:
    """Return a field's description followed by the notes of its enclosing objects.

    Args:
        text: The field's own description, as plain text; may be empty.
        notes: The rendered notes, outermost object first, none empty.

    Returns:
        The description and the notes joined by spaces, each part that ends without sentence
        punctuation closed with a period, so one part's last sentence never runs into the
        next; the description alone, unchanged, when there are no notes.
    """
    if not notes:
        return text
    parts = [text, *notes] if text else notes
    return " ".join(part if part.endswith((".", "!", "?")) else part + "." for part in parts)


def _json_text(value: Json) -> str:
    """Return a documented value as canonical JSON text, for comparing samples exactly.

    Args:
        value: A JSON value.

    Returns:
        Its JSON text, object keys sorted.
    """
    return json.dumps(value, sort_keys=True)


def _converts(wire_type: WireType, value: Json) -> bool:
    """Return whether the plain converter of a type accepts a value.

    Args:
        wire_type: The type.
        value: A documented example value, not `None`.

    Returns:
        True when `spec.convert.convert` accepts it.
    """
    try:
        convert(wire_type, value)
    except UnconvertibleError:
        return False
    return True


def _converts_field(item: Field, value: Json) -> bool:
    """Return whether a field, with its conversion if it has one, accepts a value.

    Args:
        item: The generated field.
        value: A documented example value, not `None`.

    Returns:
        True when `spec.convert.convert_field` accepts it.
    """
    try:
        convert_field(item, value)
    except UnconvertibleError:
        return False
    return True


def _example_value(row: Json, wire_key: str) -> Json:
    """Return the value at a dotted wire key of an example row.

    Args:
        row: One row of the specification's example.
        wire_key: The field's dotted wire key (`day.c`).

    Returns:
        The value, or `None` when the row, or an object on the way, lacks it.
    """
    for part in wire_key.split("."):
        row = row.get(part) if isinstance(row, dict) else None
    return row
