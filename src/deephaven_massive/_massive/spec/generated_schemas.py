"""Parse the generated `spec/*.json` files into the types of `spec.types`.

The refresh writes one schema file per transport (`rest.json`, `ws.json`, `flatfiles.json`)
and one `descriptions.json` beside the `spec` package. `parse` turns a schema file's text
into its sources, joining each field's descriptions, and `parse_descriptions` turns the text
of `descriptions.json` into a `DescriptionsFile`. Both are strict: a member that is missing
or of the wrong type raises, and nothing is coerced, so a generated file edited by hand fails
when it is loaded rather than when a value is decoded.

The loader in `spec` (`source`, `sources`, `provenance`) reads the files and calls these
once per file, under its lock; tests call them with files of their own. The functions keep
no state and return immutable values, so they are safe from any thread. This module imports
only the standard library and `spec.types`.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from deephaven_massive._massive.spec.types import (
    Conversion,
    Field,
    Membership,
    Param,
    ParamKind,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["DescriptionsFile", "parse", "parse_descriptions"]

# What `parse` joins when it is given no descriptions.
_NO_DESCRIPTIONS: Mapping[str, Mapping[str, str]] = MappingProxyType({})
_NO_CLASS_DESCRIPTIONS: Mapping[str, Mapping[str, Mapping[str, str]]] = MappingProxyType({})


@dataclass(frozen=True, kw_only=True)
class DescriptionsFile:
    """The parsed contents of `descriptions.json`, before they are joined to the sources.

    Internal to the loader: `parse_descriptions` builds it, and the loader keeps one and joins
    it into each transport's sources as they load. Nothing outside `spec` sees it; the library
    reads the same data through `Field.description` and `provenance()`. Immutable.

    Attributes:
        fetched: The day the vendor sources were downloaded.
        urls: Per transport, each source key mapped to the vendor URLs that document it.
        texts: Per transport, per source key, each described wire key mapped to its plain-text
            description. A field without a description has no entry.
        class_texts: Per transport, per source key, each wire key with per-asset-class
            descriptions mapped to them, by asset class. Only those fields have an entry.
    """

    fetched: dt.date
    urls: Mapping[Transport, Mapping[str, tuple[str, ...]]]
    texts: Mapping[Transport, Mapping[str, Mapping[str, str]]]
    class_texts: Mapping[Transport, Mapping[str, Mapping[str, Mapping[str, str]]]]


def parse(
    transport: Transport,
    text: str,
    descriptions: Mapping[str, Mapping[str, str]] = _NO_DESCRIPTIONS,
    *,
    class_descriptions: Mapping[str, Mapping[str, Mapping[str, str]]] = _NO_CLASS_DESCRIPTIONS,
) -> Mapping[str, Source]:
    """Turn the text of a generated file into its sources, joining their descriptions.

    `sources` calls this once per transport; tests call it with files of their own. A
    `TypeError` (a member of the wrong type; nothing is coerced) propagates.

    Args:
        transport: The transport the file describes.
        text: The file's JSON text: `{"sources": [{"key", "result_key", "params", "fields",
            "memberships"}, ...]}`, as the refresh writes it. `result_key` and `params` are
            read for REST only.
        descriptions: For each source key, each described wire key mapped to its description
            (the transport's part of `DescriptionsFile.texts`). A field it does not name gets
            `""`; by default nothing is described.
        class_descriptions: For each source key, each wire key with per-asset-class
            descriptions mapped to them by asset class (the transport's part of
            `DescriptionsFile.class_texts`), joined into `Field.class_descriptions`; by default
            none.

    Returns:
        A read-only mapping from key to source, in file order.

    Raises:
        ValueError: If a member is missing, a value breaks a type's invariants, two sources
            share a key, or a description or class description names a source or a wire key
            the file lacks.
    """
    loaded: dict[str, Source] = {}
    for entry in _list(_member(_object(json.loads(text), "the schema file"), "sources")):
        value = _source(
            transport, entry, descriptions=descriptions, class_descriptions=class_descriptions
        )
        if value.key in loaded:
            raise ValueError(f"two generated {transport} sources share the key {value.key!r}")
        loaded[value.key] = value
    for what, named in (
        ("descriptions", descriptions),
        ("class descriptions", class_descriptions),
    ):
        for key, described in named.items():
            found = loaded.get(key)
            if found is None:
                raise ValueError(f"{what} name the {transport} source {key!r}, which is absent")
            unknown = sorted(set(described) - {f.wire_key for f in found.fields})
            if unknown:
                raise ValueError(f"{what} of {transport} {key!r} name no field: {unknown}")
    return MappingProxyType(loaded)


def parse_descriptions(text: str) -> DescriptionsFile:
    """Turn the text of `descriptions.json` into its values.

    The loader calls this once; tests call it with files of their own. A `ValueError` (a
    member missing, a `fetched` that is not an ISO date, an unknown transport) or a
    `TypeError` (a member of the wrong type; nothing is coerced) propagates.

    Args:
        text: The file's JSON text: `{"generated_by", "fetched": "<YYYY-MM-DD>", "urls":
            {<transport>: {<source key>: [<url>, ...]}}, "descriptions": {<transport>:
            {<source key>: {<wire key>: <text>}}}, "class_descriptions": {<transport>:
            {<source key>: {<wire key>: {<asset class>: <text>}}}}}`, as the refresh writes
            it.

    Returns:
        The parsed values, every mapping read-only.
    """
    document = _object(json.loads(text), "the descriptions file")
    urls = _object(_member(document, "urls"), "urls")
    texts = _object(_member(document, "descriptions"), "descriptions")
    by_class = _object(_member(document, "class_descriptions"), "class_descriptions")
    return DescriptionsFile(
        fetched=dt.date.fromisoformat(_text(document, "fetched")),
        urls=MappingProxyType(
            {
                Transport(transport): MappingProxyType(
                    {
                        key: _string_list(found, f"URLs of {key!r}")
                        for key, found in _object(by_key, f"URLs of {transport}").items()
                    }
                )
                for transport, by_key in urls.items()
            }
        ),
        texts=MappingProxyType(
            {
                Transport(transport): MappingProxyType(
                    {
                        key: _strings(described, f"descriptions of {key!r}")
                        for key, described in _object(
                            by_key, f"descriptions of {transport}"
                        ).items()
                    }
                )
                for transport, by_key in texts.items()
            }
        ),
        class_texts=MappingProxyType(
            {
                Transport(transport): MappingProxyType(
                    {
                        key: MappingProxyType(
                            {
                                wire_key: _strings(texts_by_class, f"class descriptions of {key!r}")
                                for wire_key, texts_by_class in _object(
                                    described, f"class descriptions of {key!r}"
                                ).items()
                            }
                        )
                        for key, described in _object(
                            by_key, f"class descriptions of {transport}"
                        ).items()
                    }
                )
                for transport, by_key in by_class.items()
            }
        ),
    )


def _member(item: dict[str, object], key: str) -> object:
    """Return a member of a JSON object that the file format requires.

    Args:
        item: The object.
        key: The member's name.

    Returns:
        Its value.

    Raises:
        ValueError: If the object has no such member.
    """
    if key not in item:
        raise ValueError(f"missing member {key!r} in {sorted(item)}")
    return item[key]


def _object(value: object, what: str) -> dict[str, object]:
    """Return a JSON object.

    Args:
        value: The decoded JSON value.
        what: What it is, for the message.

    Returns:
        The object.

    Raises:
        TypeError: If the value is not an object.
    """
    if not isinstance(value, dict):
        raise TypeError(f"{what} is {value!r:.80}, not an object")
    return value


def _strings(value: object, what: str) -> Mapping[str, str]:
    """Return a JSON object of strings as a read-only mapping.

    Args:
        value: The decoded JSON value.
        what: What it is, for the message.

    Returns:
        The mapping.

    Raises:
        TypeError: If the value is not an object whose every value is a string.
    """
    found = _object(value, what)
    strings = {key: item for key, item in found.items() if isinstance(item, str)}
    if len(strings) != len(found):
        raise TypeError(f"{what} is {value!r:.80}, not an object of strings")
    return MappingProxyType(strings)


def _string_list(value: object, what: str) -> tuple[str, ...]:
    """Return a JSON array of strings as a tuple.

    Args:
        value: The decoded JSON value.
        what: What it is, for the message.

    Returns:
        The strings, in order.

    Raises:
        TypeError: If the value is not an array of strings.
    """
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TypeError(f"{what} is {value!r:.80}, not an array of strings")
    return tuple(value)


def _source(
    transport: Transport,
    entry: dict[str, object],
    *,
    descriptions: Mapping[str, Mapping[str, str]],
    class_descriptions: Mapping[str, Mapping[str, Mapping[str, str]]],
) -> Source:
    """Build one source from its entry in a generated file.

    Args:
        transport: The transport.
        entry: The decoded JSON object of the source.
        descriptions: The transport's descriptions, by source key and wire key.
        class_descriptions: The transport's per-asset-class descriptions, by source key, wire
            key, and asset class.

    Returns:
        A `RestPath` for REST, a `Source` otherwise.
    """
    key = _text(entry, "key")
    described = descriptions.get(key, {})
    by_class = class_descriptions.get(key, {})
    fields = tuple(_field(item, described, by_class) for item in _list(_member(entry, "fields")))
    memberships = tuple(_membership(item) for item in _list(_member(entry, "memberships")))
    if transport is not Transport.REST:
        return Source(key=key, transport=transport, fields=fields, memberships=memberships)
    return RestPath(
        key=key,
        fields=fields,
        memberships=memberships,
        result_key=_optional(entry, "result_key"),
        params=tuple(_param(item) for item in _list(_member(entry, "params"))),
    )


def _list(value: object) -> list[dict[str, object]]:
    """Return a JSON array of objects as a list.

    Args:
        value: The decoded JSON value.

    Returns:
        The list.

    Raises:
        ValueError: If the value is not an array of objects.
    """
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"expected an array of objects, got {value!r:.80}")
    return value


def _text(item: dict[str, object], key: str) -> str:
    """Return a string member of a JSON object.

    Args:
        item: The object.
        key: The member's name.

    Returns:
        The string.

    Raises:
        TypeError: If the member is not a string (`null` included); nothing is coerced.
    """
    value = _member(item, key)
    if not isinstance(value, str):
        raise TypeError(f"{key!r} is {value!r}, not a string")
    return value


def _optional(item: dict[str, object], key: str) -> str | None:
    """Return a string member of a JSON object that may be `null`.

    Args:
        item: The object.
        key: The member's name.

    Returns:
        The string, or `None` for `null`.
    """
    return None if _member(item, key) is None else _text(item, key)


def _flag(item: dict[str, object], key: str) -> bool:
    """Return a boolean member of a JSON object.

    Args:
        item: The object.
        key: The member's name.

    Returns:
        The boolean.

    Raises:
        TypeError: If the member is not `true` or `false`.
    """
    value = _member(item, key)
    if not isinstance(value, bool):
        raise TypeError(f"{key!r} is {value!r}, not a boolean")
    return value


def _field(
    item: dict[str, object],
    descriptions: Mapping[str, str],
    class_descriptions: Mapping[str, Mapping[str, str]],
) -> Field:
    """Build a field from its JSON object.

    A conversion `spec.types.Conversion` does not name, or one that gives another type than
    the field's, raises `ValueError` here, so the problem shows when the file is loaded rather
    than when a value is decoded.

    Args:
        item: `{"wire_key", "column", "type", "unit", "required", "conversion"}`.
        descriptions: The source's descriptions by wire key; the field's is joined, or `""`.
        class_descriptions: The source's per-asset-class descriptions by wire key; the
            field's are joined, sorted by asset class, or none.

    Returns:
        The field.
    """
    unit = _optional(item, "unit")
    conversion = _optional(item, "conversion")
    wire_key = _text(item, "wire_key")
    return Field(
        wire_key=wire_key,
        column=_text(item, "column"),
        wire_type=WireType(_text(item, "type")),
        unit=None if unit is None else WireUnit(unit),
        required=_flag(item, "required"),
        conversion=None if conversion is None else Conversion(conversion),
        description=descriptions.get(wire_key, ""),
        class_descriptions=tuple(sorted(class_descriptions.get(wire_key, {}).items())),
    )


def _param(item: dict[str, object]) -> Param:
    """Build a parameter from its JSON object.

    Args:
        item: `{"name", "kind", "in_path", "values", "unit", "default"}`, where `default` is
            a JSON scalar or `null`.

    Returns:
        The parameter.

    Raises:
        TypeError: If `values` is not an array of strings, or `default` is not a scalar.
    """
    unit = _optional(item, "unit")
    values = _member(item, "values")
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        raise TypeError(f"'values' is {values!r:.80}, not an array of strings")
    default = _member(item, "default")
    if default is not None and not isinstance(default, (str, int, float)):
        raise TypeError(f"'default' is {default!r:.80}, not a JSON scalar")
    return Param(
        name=_text(item, "name"),
        kind=ParamKind(_text(item, "kind")),
        in_path=_flag(item, "in_path"),
        values=tuple(values),
        unit=None if unit is None else WireUnit(unit),
        default=default,
    )


def _membership(item: dict[str, object]) -> Membership:
    """Build a membership from its JSON object.

    Args:
        item: `{"kind", "group", "position", "projection"}`, where `projection` is `null` or
            an object from wire key to column.

    Returns:
        The membership.

    Raises:
        TypeError: If the position is not an integer, or the projection is neither `null` nor
            an object of strings.
    """
    projection = _member(item, "projection")
    position = _member(item, "position")
    if not isinstance(position, int) or isinstance(position, bool):
        raise TypeError(f"membership position {position!r} is not an integer")
    if projection is not None and (
        not isinstance(projection, dict) or not all(isinstance(v, str) for v in projection.values())
    ):
        raise TypeError(f"membership projection {projection!r:.80} is not an object of strings")
    return Membership(
        kind=_text(item, "kind"),
        group=_text(item, "group"),
        position=position,
        projection=None if projection is None else dict(projection),
    )
