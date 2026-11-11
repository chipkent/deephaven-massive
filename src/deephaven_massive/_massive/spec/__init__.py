"""The run-time wire vocabulary: what each Massive REST path, channel, and flat file carries.

`spec` describes Massive's wire format as plain, frozen values, so that every client decodes
against the same description and no schema is written by hand:

- `spec.types` defines the value types: `Transport`, `Source` and `RestPath` (one REST path,
  WebSocket channel, or flat-file prefix), `Field` (one wire key, its long-form column, and
  what it means), `Param` and `ParamKind` (REST parameters), `Membership` (a
  source's place in a dataset kind), `WireType` and `WireUnit` (a field's type and timestamp
  unit), `DriftReport` (what a payload had that its source did not, and the reverse), and
  `Provenance` (the vendor snapshot's date and each source's vendor URLs).
- `rest.json`, `ws.json`, and `flatfiles.json`, next to this module, are generated from
  Massive's OpenAPI specification, its WebSocket specification, and its flat-file
  documentation pages by `scripts/refresh.py` and committed; they are never edited by hand.
  Each source there records its fields in its dataset kind's order and its memberships of the
  kinds (`Source.memberships`), so nothing at run time needs the refresh. `descriptions.json`,
  generated beside them by the same command, holds each field's description (the vendor's, or
  the refresh's correction where the vendor's is clearly wrong, and for a source that serves
  several asset classes any per-class description), the snapshot's fetch date, each source's
  vendor URLs, and what each vendor page says about Massive's plans. The descriptions are kept
  out of the schema files so that a refresh that only rewords them leaves those byte for byte
  unchanged. All ship in the wheel as package data, while the
  refresh that writes them does not.
- `spec.values` writes one REST parameter value as wire text by its `ParamKind`
  (`encode_value`), for the REST client and for the library's filter check alike.
- `spec.convert` checks one decoded JSON value against its `WireType` and converts it
  exactly (an ISO instant to integer nanoseconds, for example), for every JSON decoder,
  including a field's named conversion where the vendor's samples differ from its type.
- `spec.generated_schemas` parses the text of a generated file into those types (`parse`)
  and the text of `descriptions.json` into its values (`parse_descriptions`), strictly:
  nothing is coerced.
- `source(transport, key)` and `sources(transport)` read a transport's generated file into
  those types the first time either is called, joining each field's descriptions from
  `descriptions.json` into `Field.description` and `Field.class_descriptions`, and return
  the same values ever after. `provenance()` returns the fetch date, the URLs, and the plans.

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

import threading
from dataclasses import dataclass
from importlib import resources
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, overload

from deephaven_massive._massive.spec.generated_schemas import (
    DescriptionsFile,
    parse,
    parse_descriptions,
)
from deephaven_massive._massive.spec.types import (
    Conversion,
    Provenance,
    RestPath,
    Source,
    Transport,
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
            described.texts.get(transport, {}),
            class_descriptions=described.class_texts.get(transport, {}),
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
    """Return where the generated sources came from: the fetch date, URLs, and plan tables.

    A generated document cites it so a reader can check a column against the vendor's own
    documentation. The first call loads every transport the descriptions file gives URLs for,
    exactly as `sources` would, so the URLs it returns have passed the same checks; every later
    call returns the same value. An error from that loading propagates as it does from
    `sources`, and the provenance stays unbuilt.

    Returns:
        The snapshot's fetch date; by `(transport, source key)`, the sorted vendor URLs of
        every generated source (for REST, every documentation page that names the path); and,
        by page URL, what each of those pages says about Massive's plans.
    """
    with _lock:
        found = _cache.provenance
        if found is None:
            described = _load_descriptions()
            urls: dict[tuple[Transport, str], tuple[str, ...]] = {}
            for transport, by_key in described.urls.items():
                _load(transport)  # checks this transport's URLs against its sources
                urls.update({(transport, key): pages for key, pages in by_key.items()})
            found = Provenance(
                fetched=described.fetched, urls=urls, plans=described.plans, titles=described.titles
            )
            _cache.provenance = found
    return found
