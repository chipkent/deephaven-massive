"""The run-time wire vocabulary: what each Massive REST path, channel, and flat file carries.

`spec` describes Massive's wire format as plain, frozen values, so that every client decodes
against the same description and no schema is written by hand:

- `spec.types` defines the value types: `Transport`, `Source` and `RestPath` (one REST path,
  WebSocket channel, or flat-file prefix), `Field` (one wire key, its long-form column, and
  the vendor's description of it), `Param` and `ParamKind` (REST parameters), `Membership` (a
  source's place in a dataset kind), `WireType` and `WireUnit` (a field's type and timestamp
  unit), `DriftReport` (what a payload had that its source did not, and the reverse), and
  `Provenance` (the vendor snapshot's date and each source's vendor URLs).
- `rest.json`, `ws.json`, and `flatfiles.json`, next to this module, are generated from
  Massive's OpenAPI specification, its WebSocket specification, and its flat-file
  documentation pages by `python -m deephaven_massive._massive.refresh` and committed; they
  are never edited by hand. Each source there records its fields in its dataset kind's order
  and its memberships of the kinds (`Source.memberships`), so nothing at run time needs the
  refresh. `descriptions.json`, generated beside them by the same command, holds the vendor's
  description of each field, the snapshot's fetch date, and each source's vendor URLs. The
  descriptions are kept out of the schema files so that a refresh that only rewords them
  leaves those byte for byte unchanged. All ship in the wheel as package data, while the
  refresh that writes them does not.
- `spec.convert` checks one decoded JSON value against its `WireType` and converts it
  exactly (an ISO instant to integer nanoseconds, for example), for every JSON decoder,
  including a field's named conversion where the vendor's samples differ from its type.
- `source(transport, key)` and `sources(transport)` read a transport's generated file into
  those types the first time either is called, joining each field's description from
  `descriptions.json` into `Field.description`, and return the same values ever after.
  `provenance()` returns the fetch date and the URLs.

Each file is read and parsed once, under one lock, so concurrent first calls from any number
of threads (with or without the GIL) parse each file once and all receive the same objects.
The loaded values are immutable, so they are shared freely between threads afterwards.

`spec` is a bottom layer of `_massive`: it imports only the standard library, never another
part of `_massive`, so the REST, WebSocket, and flat-file clients can all depend on it.

Example:
    ::

        from deephaven_massive._massive.spec import provenance, source
        from deephaven_massive._massive.spec.types import Transport

        trades = source(Transport.REST, "/v3/trades/{stockTicker}")
        assert trades.field("SipTimestamp").unit == "ns"
        print(trades.field("Price").description)
        print(provenance().fetched, *provenance().urls[Transport.REST, trades.key])
"""

from __future__ import annotations

import datetime as dt
import json
import threading
from dataclasses import dataclass
from importlib import resources
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, overload

from deephaven_massive._massive.spec.types import (
    Conversion,
    Field,
    Membership,
    Param,
    ParamKind,
    Provenance,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["Conversion", "provenance", "source", "sources"]

# The generated file of each transport, beside this module.
_FILES: Mapping[Transport, str] = MappingProxyType(
    {Transport.REST: "rest.json", Transport.WS: "ws.json", Transport.FLAT_FILES: "flatfiles.json"}
)

# The generated file of the vendor's descriptions, fetch date, and URLs, beside this module.
_DESCRIPTIONS = "descriptions.json"

# What `parse` joins when it is given no descriptions.
_NO_DESCRIPTIONS: Mapping[str, Mapping[str, str]] = MappingProxyType({})


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
    """

    fetched: dt.date
    urls: Mapping[Transport, Mapping[str, tuple[str, ...]]]
    texts: Mapping[Transport, Mapping[str, Mapping[str, str]]]


@dataclass
class _Cache:
    """What has been loaded so far; read and written only under `_lock`.

    Each value is published once, after it passed every check, and never changed afterwards.

    Attributes:
        sources: The sources of each transport loaded so far.
        descriptions: The descriptions file, once read.
        provenance: The provenance, once built.
    """

    sources: dict[Transport, Mapping[str, Source]]
    descriptions: DescriptionsFile | None = None
    provenance: Provenance | None = None


_lock = threading.Lock()
_cache = _Cache(sources={})


def _read(name: str) -> str:
    """Return the text of a generated file beside this module.

    Args:
        name: The file's name, such as `"rest.json"`.

    Returns:
        Its text.
    """
    return resources.files(__name__).joinpath(name).read_text(encoding="utf-8")


def _load_descriptions() -> DescriptionsFile:
    """Return the descriptions file, reading it the first time. The caller holds `_lock`.

    A `ValueError` or `TypeError` from `parse_descriptions` (a malformed file) propagates, and
    the file stays unread.

    Returns:
        The parsed file.
    """
    loaded = _cache.descriptions
    if loaded is None:
        loaded = parse_descriptions(_read(_DESCRIPTIONS))
        _cache.descriptions = loaded
    return loaded


def _load(transport: Transport) -> Mapping[str, Source]:
    """Return a transport's sources, loading them the first time. The caller holds `_lock`.

    Loading joins the transport's descriptions into its fields and checks its URLs: the
    descriptions file must give a URL for exactly the sources the schema file has. A
    `ValueError` or `TypeError` from `parse` or `_load_descriptions` propagates, and the
    transport stays unloaded (a descriptions file that parsed stays read).

    Args:
        transport: The transport.

    Returns:
        Its sources, descriptions joined.

    Raises:
        ValueError: If a source has no URL, or a URL names a source the schema file lacks.
    """
    loaded = _cache.sources.get(transport)
    if loaded is None:
        described = _load_descriptions()
        loaded = parse(
            transport,
            _read(_FILES[transport]),
            described.texts.get(transport, _NO_DESCRIPTIONS),
        )
        urls = described.urls.get(transport, {})
        for what, keys in (
            ("sources without a URL", set(loaded) - set(urls)),
            ("URLs of sources the schema lacks", set(urls) - set(loaded)),
        ):
            if keys:
                raise ValueError(f"{_DESCRIPTIONS} has {transport} {what}: {sorted(keys)}")
        _cache.sources[transport] = loaded
    return loaded


@overload
def sources(transport: Literal[Transport.REST]) -> Mapping[str, RestPath]: ...
@overload
def sources(transport: Transport) -> Mapping[str, Source]: ...
def sources(transport: Transport) -> Mapping[str, Source]:
    """Return every generated source of a transport, loading its file on first use.

    The first call for a transport reads its file and, if no earlier call has, the
    descriptions file, joins each field's description into `Field.description`, and checks
    that every source has a URL and every URL a source.

    A `ValueError` (a value that breaks a type's invariants, a member missing, or a
    description or URL that does not match the schema) or a `TypeError` (a member of the wrong
    type) propagates; either means a generated file was edited by hand. The transport then
    stays unloaded, and a later call tries again.

    Args:
        transport: The transport, such as `Transport.REST`.

    Returns:
        A read-only mapping from each source's key (a REST path template such as
        `"/v3/trades/{stockTicker}"`, a channel such as `"stocks/T"`, or a prefix such as
        `"us_stocks_sip/trades_v1"`) to its value, in the generated file's order. For REST
        every value is a `RestPath`. Every call for one transport returns the same mapping.
    """
    with _lock:
        return _load(transport)


@overload
def source(transport: Literal[Transport.REST], key: str) -> RestPath: ...
@overload
def source(transport: Transport, key: str) -> Source: ...
def source(transport: Transport, key: str) -> Source:
    """Return one generated source.

    Args:
        transport: The transport, such as `Transport.REST`.
        key: The source's key: a REST path template exactly as Massive's specification
            spells it (`"/v3/trades/{stockTicker}"`), a WebSocket `"<cluster>/<channel>"`
            (`"stocks/T"`), or a flat-file prefix (`"us_futures_{venue}/trades_v1"`).

    A `ValueError` or `TypeError` from `sources`, for a generated file edited by hand,
    propagates.

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


def provenance() -> Provenance:
    """Return where the generated sources came from: the fetch date and each source's URLs.

    A generated document cites it so a reader can check a column against the vendor's own
    documentation. The first call loads every transport the descriptions file gives URLs for,
    exactly as `sources` would, so the URLs it returns have passed the same checks; every later
    call returns the same value. An error from that loading propagates as it does from
    `sources`, and the provenance stays unbuilt.

    Returns:
        The snapshot's fetch date and, by `(transport, source key)`, the sorted vendor URLs of
        every generated source: for REST, every documentation page that names the path.
    """
    with _lock:
        found = _cache.provenance
        if found is None:
            described = _load_descriptions()
            urls: dict[tuple[Transport, str], tuple[str, ...]] = {}
            for transport, by_key in described.urls.items():
                _load(transport)  # checks this transport's URLs against its sources
                urls.update({(transport, key): pages for key, pages in by_key.items()})
            found = Provenance(fetched=described.fetched, urls=urls)
            _cache.provenance = found
    return found


def parse(
    transport: Transport,
    text: str,
    descriptions: Mapping[str, Mapping[str, str]] = _NO_DESCRIPTIONS,
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

    Returns:
        A read-only mapping from key to source, in file order.

    Raises:
        ValueError: If a member is missing, a value breaks a type's invariants, two sources
            share a key, or a description names a source or a wire key the file lacks.
    """
    loaded: dict[str, Source] = {}
    for entry in _list(_member(_object(json.loads(text), "the schema file"), "sources")):
        value = _source(transport, entry, descriptions)
        if value.key in loaded:
            raise ValueError(f"two generated {transport} sources share the key {value.key!r}")
        loaded[value.key] = value
    for key, described in descriptions.items():
        found = loaded.get(key)
        if found is None:
            raise ValueError(f"descriptions name the {transport} source {key!r}, which is absent")
        unknown = sorted(set(described) - {f.wire_key for f in found.fields})
        if unknown:
            raise ValueError(f"descriptions of {transport} {key!r} name no field: {unknown}")
    return MappingProxyType(loaded)


def parse_descriptions(text: str) -> DescriptionsFile:
    """Turn the text of `descriptions.json` into its values.

    The loader calls this once; tests call it with files of their own. A `ValueError` (a
    member missing, a `fetched` that is not an ISO date, an unknown transport) or a
    `TypeError` (a member of the wrong type; nothing is coerced) propagates.

    Args:
        text: The file's JSON text: `{"generated_by", "fetched": "<YYYY-MM-DD>", "urls":
            {<transport>: {<source key>: [<url>, ...]}}, "descriptions": {<transport>:
            {<source key>: {<wire key>: <text>}}}}`, as the refresh writes it.

    Returns:
        The parsed values, every mapping read-only.
    """
    document = _object(json.loads(text), "the descriptions file")
    urls = _object(_member(document, "urls"), "urls")
    texts = _object(_member(document, "descriptions"), "descriptions")
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
    transport: Transport, entry: dict[str, object], descriptions: Mapping[str, Mapping[str, str]]
) -> Source:
    """Build one source from its entry in a generated file.

    Args:
        transport: The transport.
        entry: The decoded JSON object of the source.
        descriptions: The transport's descriptions, by source key and wire key.

    Returns:
        A `RestPath` for REST, a `Source` otherwise.
    """
    key = _text(entry, "key")
    described = descriptions.get(key, {})
    fields = tuple(_field(item, described) for item in _list(_member(entry, "fields")))
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


def _field(item: dict[str, object], descriptions: Mapping[str, str]) -> Field:
    """Build a field from its JSON object.

    A conversion `spec.types.Conversion` does not name, or one that gives another type than
    the field's, raises `ValueError` here, so the problem shows when the file is loaded rather
    than when a value is decoded.

    Args:
        item: `{"wire_key", "column", "type", "unit", "required", "conversion"}`.
        descriptions: The source's descriptions by wire key; the field's is joined, or `""`.

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
    values = _member(item, "values")
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
