"""The run-time wire vocabulary: what each Massive REST path, channel, and flat file carries.

`spec` describes Massive's wire format as plain, frozen values, so that every client decodes
against the same description and no schema is written by hand:

- `spec.types` defines the value types: `Transport`, `Source` and `RestPath` (one REST path,
  WebSocket channel, or flat-file prefix), `Field` (one wire key and its long-form column),
  `Param` and `ParamKind` (REST parameters), `Membership` (a source's place in a dataset kind),
  `WireType` and `WireUnit` (a field's type and timestamp unit), and `DriftReport` (what a
  payload had that its source did not, and the reverse).
- `rest.json`, next to this module, is generated from Massive's OpenAPI specification by
  `python -m deephaven_massive._massive.refresh` and committed; it is never edited by hand.
  It ships in the wheel as package data, while the refresh that writes it does not.
- `spec.convert` checks one decoded JSON value against its `WireType` and converts it
  exactly (an ISO instant to integer nanoseconds, for example), for every JSON decoder,
  including a field's declared conversion from the vendor's wrong type.
- `source(transport, key)` and `sources(transport)` read a transport's generated file into
  those types the first time either is called, and return the same values ever after.

Loading happens once per transport, under a lock, so concurrent first calls from any number
of threads (with or without the GIL) parse the file once and all receive the same objects.
The loaded values are immutable, so they are shared freely between threads afterwards.

`spec` is a bottom layer of `_massive`: it imports only the standard library, never another
part of `_massive`, so the REST, WebSocket, and flat-file clients can all depend on it.

Example:
    ::

        from deephaven_massive._massive.spec import source
        from deephaven_massive._massive.spec.types import Transport

        trades = source(Transport.REST, "/v3/trades/{stockTicker}")
        assert trades.field("SipTimestamp").unit == "ns"
"""

from __future__ import annotations

import json
import threading
from importlib import resources
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, overload

from deephaven_massive._massive.spec.types import (
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

__all__ = ["source", "sources"]

# The generated file of each transport that has one, beside this module.
_FILES: Mapping[Transport, str] = MappingProxyType({Transport.REST: "rest.json"})

# The sources of each transport loaded so far. Written only under `_lock`, once per
# transport; a published mapping is never changed.
_lock = threading.Lock()
_loaded: dict[Transport, Mapping[str, Source]] = {}


@overload
def sources(transport: Literal[Transport.REST]) -> Mapping[str, RestPath]: ...
@overload
def sources(transport: Transport) -> Mapping[str, Source]: ...
def sources(transport: Transport) -> Mapping[str, Source]:
    """Return every generated source of a transport, loading its file on first use.

    Args:
        transport: The transport, such as `Transport.REST`.

    A `ValueError` from `parse` (a value that breaks a type's invariants, which means the
    generated file was edited by hand) propagates, and the transport stays unloaded.

    Returns:
        A read-only mapping from each source's key (a REST path template such as
        `"/v3/trades/{stockTicker}"`) to its value, in the generated file's order. For REST
        every value is a `RestPath`. Every call for one transport returns the same mapping.

    Raises:
        LookupError: If the transport has no generated file in this version of the library.
    """
    with _lock:
        loaded = _loaded.get(transport)
        if loaded is None:
            name = _FILES.get(transport)
            if name is None:
                raise LookupError(f"no generated schema exists for transport {transport}")
            text = resources.files(__name__).joinpath(name).read_text(encoding="utf-8")
            loaded = parse(transport, text)
            _loaded[transport] = loaded
    return loaded


@overload
def source(transport: Literal[Transport.REST], key: str) -> RestPath: ...
@overload
def source(transport: Transport, key: str) -> Source: ...
def source(transport: Transport, key: str) -> Source:
    """Return one generated source.

    Args:
        transport: The transport, such as `Transport.REST`.
        key: The source's key: a REST path template exactly as Massive's specification
            spells it, such as `"/v3/trades/{stockTicker}"`.

    A `LookupError` from `sources`, for a transport with no generated file, propagates.

    Returns:
        The source; a `RestPath` for REST.

    Raises:
        KeyError: If the transport has no generated source with that key (one that is
            deprecated, draft, excluded, or unknown); the message names the transport and key.
    """
    found = sources(transport).get(key)
    if found is None:
        raise KeyError(f"no generated {transport} source {key!r}")
    return found


def parse(transport: Transport, text: str) -> Mapping[str, Source]:
    """Turn the text of a generated file into its sources.

    `sources` calls this once per transport; tests call it with files of their own.

    Args:
        transport: The transport the file describes.
        text: The file's JSON text: `{"sources": [{"key", "result_key", "params", "fields",
            "memberships"}, ...]}`, as the refresh writes it. `result_key` and `params` are
            read for REST only.

    Returns:
        A read-only mapping from key to source, in file order.

    Raises:
        ValueError: If a value breaks a type's invariants, or two sources share a key.
    """
    loaded: dict[str, Source] = {}
    for entry in json.loads(text)["sources"]:
        value = _source(transport, entry)
        if value.key in loaded:
            raise ValueError(f"two generated {transport} sources share the key {value.key!r}")
        loaded[value.key] = value
    return MappingProxyType(loaded)


def _source(transport: Transport, entry: dict[str, object]) -> Source:
    """Build one source from its entry in a generated file.

    Args:
        transport: The transport.
        entry: The decoded JSON object of the source.

    Returns:
        A `RestPath` for REST, a `Source` otherwise.
    """
    fields = tuple(_field(item) for item in _list(entry["fields"]))
    memberships = tuple(_membership(item) for item in _list(entry["memberships"]))
    key = _text(entry, "key")
    if transport is not Transport.REST:
        return Source(key=key, transport=transport, fields=fields, memberships=memberships)
    return RestPath(
        key=key,
        fields=fields,
        memberships=memberships,
        result_key=_optional(entry, "result_key"),
        params=tuple(_param(item) for item in _list(entry["params"])),
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
    value = item[key]
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
    return None if item[key] is None else _text(item, key)


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
    value = item[key]
    if not isinstance(value, bool):
        raise TypeError(f"{key!r} is {value!r}, not a boolean")
    return value


def _field(item: dict[str, object]) -> Field:
    """Build a field from its JSON object.

    Args:
        item: `{"wire_key", "column", "type", "unit", "required", "declared_type"}`.

    Returns:
        The field.
    """
    unit = _optional(item, "unit")
    declared = _optional(item, "declared_type")
    return Field(
        wire_key=_text(item, "wire_key"),
        column=_text(item, "column"),
        wire_type=WireType(_text(item, "type")),
        unit=None if unit is None else WireUnit(unit),
        required=_flag(item, "required"),
        declared_type=None if declared is None else WireType(declared),
    )


def _param(item: dict[str, object]) -> Param:
    """Build a parameter from its JSON object.

    Args:
        item: `{"name", "kind", "in_path", "values", "unit"}`.

    Returns:
        The parameter.

    Raises:
        TypeError: If `values` is not an array of strings.
    """
    unit = _optional(item, "unit")
    values = item["values"]
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        raise TypeError(f"'values' is {values!r:.80}, not an array of strings")
    return Param(
        name=_text(item, "name"),
        kind=ParamKind(_text(item, "kind")),
        in_path=_flag(item, "in_path"),
        values=tuple(values),
        unit=None if unit is None else WireUnit(unit),
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
    projection = item["projection"]
    position = item["position"]
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
