"""Render the generated files (`spec/*.json`) as canonical text.

The generators produce `RestPath` and `Source` values; this module turns them into the text of
the committed files the runtime loads (`spec.sources()`, `spec.provenance()`), and nothing
else; `generate` writes them. The text is deterministic, so a refresh that changes nothing
leaves each file byte for byte as it was, and the parity test can compare the committed files
with a fresh rendering. All are JSON with one line per field, parameter, and membership, so a
refresh diff shows each changed field as one changed line. `rest.json` holds the REST paths
with their result keys and parameters; `ws.json` the WebSocket channels and `flatfiles.json`
the flat-file prefixes, each with only its key, fields, and memberships.

The field descriptions go in `descriptions.json` alone, never in a schema file: they change
far more often than the schema, and a refresh that only rewords them must leave the schema
files unchanged, so a reviewer sees at once that no schema moved. The descriptions arrive
already plain text (`plain_text`, applied by the generators): the vendor's, or the refresh's
correction (`overrides.DESCRIPTION_FIXES`) where the vendor's is clearly wrong.

It imports the standard library and `spec.types` only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json
    from deephaven_massive._massive.spec.types import (
        Field,
        Membership,
        Param,
        RestPath,
        Source,
        Transport,
    )

__all__ = [
    "DESCRIPTIONS_FILE",
    "FLAT_FILES_FILE",
    "REST_FILE",
    "SPEC_DIRECTORY",
    "WS_FILE",
    "render_descriptions",
    "render_rest",
    "render_sources",
]

SPEC_DIRECTORY: Final = Path(__file__).parent.parent / "spec"
"""The directory of the generated files, the `spec` package."""

REST_FILE: Final = SPEC_DIRECTORY / "rest.json"
"""The generated REST schema the refresh writes."""

WS_FILE: Final = SPEC_DIRECTORY / "ws.json"
"""The generated WebSocket schema the refresh writes."""

FLAT_FILES_FILE: Final = SPEC_DIRECTORY / "flatfiles.json"
"""The generated flat-file schema the refresh writes."""

DESCRIPTIONS_FILE: Final = SPEC_DIRECTORY / "descriptions.json"
"""The generated file of the vendor's field descriptions, fetch date, and source URLs."""

# The command that writes the file, recorded in it.
_GENERATED_BY: Final = "python -m deephaven_massive._massive.refresh"


def _line(value: Mapping[str, Json]) -> str:
    """Return one JSON object on one line, keys sorted.

    Args:
        value: The object.

    Returns:
        Its compact JSON text.
    """
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _field_json(item: Field) -> dict[str, Json]:
    """Return a field as it is written to a schema file.

    Args:
        item: The field.

    Returns:
        Every attribute, enums by value.
    """
    return {
        "wire_key": item.wire_key,
        "column": item.column,
        "type": item.wire_type.value,
        "unit": item.unit.value if item.unit else None,
        "required": item.required,
        "conversion": item.conversion.value if item.conversion else None,
    }


def _param_json(item: Param) -> dict[str, Json]:
    """Return a parameter as it is written to `rest.json`.

    Args:
        item: The parameter.

    Returns:
        Every attribute, enums by value.
    """
    return {
        "name": item.name,
        "kind": item.kind.value,
        "in_path": item.in_path,
        "values": list(item.values),
        "unit": item.unit.value if item.unit else None,
    }


def _membership_json(item: Membership) -> dict[str, Json]:
    """Return a membership as it is written to a schema file.

    Args:
        item: The membership.

    Returns:
        Every attribute; the projection as an object from wire key to column, or `null`.
    """
    return {
        "kind": item.kind,
        "group": item.group,
        "position": item.position,
        "projection": dict(item.projection) if item.projection is not None else None,
    }


def _list(name: str, lines: Sequence[str], *, last: bool = False) -> list[str]:
    """Return a member of a source's object that holds one JSON value per line.

    Args:
        name: The member's name, such as `"fields"`.
        lines: The values, one JSON text each.
        last: Whether it is the object's last member, which takes no trailing comma.

    Returns:
        The member's lines, `[]` on one line when it is empty.
    """
    end = "" if last else ","
    if not lines:
        return [f'   "{name}": []{end}']
    return [f'   "{name}": [', *_rows(lines, "    "), f"   ]{end}"]


def _rows(lines: Sequence[str], indent: str) -> list[str]:
    """Indent JSON lines and separate them with commas.

    Args:
        lines: One JSON value per line.
        indent: The indentation to prefix.

    Returns:
        The lines, each indented, every one but the last followed by a comma.
    """
    return [f"{indent}{line}{',' if i < len(lines) - 1 else ''}" for i, line in enumerate(lines)]


def render_descriptions(
    sources: Mapping[Transport, Sequence[Source]],
    *,
    fetched: dt.date,
    urls: Mapping[Transport, Mapping[str, Sequence[str]]],
) -> str:
    """Return the canonical text of `descriptions.json`.

    Transports, sources, and fields keep the order they are given in. A field whose
    `description` is empty has no entry, and a source with no described field has none
    either; `class_descriptions` holds only the fields with per-asset-class descriptions.
    Every source given in `urls` keeps its URLs, sorted.

    Args:
        sources: The generated sources of each transport, each field's `description` already
            plain text (`plain_text`).
        fetched: The day the snapshot they came from was downloaded.
        urls: Per transport, the vendor URLs that document each source key: for REST, every
            documentation page that names the path (a path shared by several asset classes,
            such as `/v3/reference/tickers`, has one page per asset class).

    Returns:
        The file's text: `{"generated_by", "fetched", "urls": {<transport>: {<source key>:
        [<url>, ...]}}, "descriptions": {<transport>: {<source key>: {<wire key>: <text>}}},
        "class_descriptions": {<transport>: {<source key>: {<wire key>: {<asset class>:
        <text>}}}}}`, one line per URL and per description, ending in a newline.
    """
    texts: dict[str, dict[str, dict[str, str]]] = {}
    by_class: dict[str, dict[str, dict[str, dict[str, str]]]] = {}
    for transport, described in sources.items():
        texts[transport.value] = {}
        by_class[transport.value] = {}
        for source in described:
            fields = {f.wire_key: f.description for f in source.fields if f.description}
            if fields:
                texts[transport.value][source.key] = fields
            classed = {f.wire_key: dict(f.class_descriptions) for f in source.fields}
            classed = {key: found for key, found in classed.items() if found}
            if classed:
                by_class[transport.value][source.key] = classed
    document = {
        "generated_by": _GENERATED_BY,
        "fetched": fetched.isoformat(),
        "urls": {
            transport.value: {key: sorted(found) for key, found in by_key.items()}
            for transport, by_key in urls.items()
        },
        "descriptions": texts,
        "class_descriptions": by_class,
    }
    return json.dumps(document, indent=1, ensure_ascii=False) + "\n"


def render_rest(paths: Sequence[RestPath]) -> str:
    """Return the canonical text of `rest.json`.

    Paths keep their order; within a field or parameter, keys are sorted. The file records the
    command that wrote it, and nothing about the snapshot it came from (`descriptions.json`
    records the fetch date), so a vendor change that moves no schema leaves it unchanged.

    Args:
        paths: The generated paths.

    Returns:
        The file's text, ending in a newline.
    """
    return _render(
        [
            [
                f'   "result_key": {json.dumps(path.result_key)},',
                *_list("params", [_line(_param_json(p)) for p in path.params]),
            ]
            for path in paths
        ],
        paths,
    )


def render_sources(sources: Sequence[Source]) -> str:
    """Return the canonical text of `ws.json` or `flatfiles.json`.

    Sources keep their order; within a field or membership, keys are sorted. Like
    `rest.json`, the file records nothing about the snapshot it came from.

    Args:
        sources: The generated sources of one transport.

    Returns:
        The file's text, `{"generated_by", "sources": [{"key", "fields", "memberships"}]}`,
        ending in a newline.
    """
    return _render([[] for _ in sources], sources)


def _render(extras: Sequence[Sequence[str]], sources: Sequence[Source]) -> str:
    """Return the text of a schema file.

    Args:
        extras: For each source, the lines of its members between its key and its fields.
        sources: The sources.

    Returns:
        The file's text, ending in a newline.
    """
    out = [
        "{",
        f' "generated_by": {json.dumps(_GENERATED_BY)},',
        ' "sources": [',
    ]
    for index, (extra, source) in enumerate(zip(extras, sources, strict=True)):
        out += [
            "  {",
            f'   "key": {json.dumps(source.key)},',
            *extra,
            *_list("fields", [_line(_field_json(f)) for f in source.fields]),
            *_list(
                "memberships", [_line(_membership_json(m)) for m in source.memberships], last=True
            ),
            "  }" + ("," if index < len(sources) - 1 else ""),
        ]
    out += [" ]", "}"]
    return "\n".join(out) + "\n"
