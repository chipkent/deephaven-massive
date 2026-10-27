"""Render the generated files, `spec/rest.json` and `spec/descriptions.json`, as canonical text.

The generators produce `RestPath` values; this module turns them into the text of the
committed files the runtime loads (`spec.sources()`, `spec.provenance()`), and nothing else;
`generate` writes them. The text is deterministic, so a refresh that changes nothing leaves
each file byte for byte as it was, and the parity test can compare the committed files with a
fresh rendering. Both are JSON with one line per field (and, in `rest.json`, per parameter),
so a refresh diff shows each changed field as one changed line.

The vendor's descriptions go in `descriptions.json` alone, never in `rest.json`: they change
far more often than the schema, and a refresh that only rewords them must leave `rest.json`
unchanged, so a reviewer sees at once that no schema moved. The descriptions arrive already
plain text (`plain_text`, applied by the generators).

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
    from deephaven_massive._massive.spec.types import Field, Param, RestPath, Source, Transport

__all__ = [
    "DESCRIPTIONS_FILE",
    "REST_FILE",
    "SPEC_DIRECTORY",
    "render_descriptions",
    "render_rest",
]

SPEC_DIRECTORY: Final = Path(__file__).parent.parent / "spec"
"""The directory of the generated files, the `spec` package."""

REST_FILE: Final = SPEC_DIRECTORY / "rest.json"
"""The generated REST schema the refresh writes."""

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
    """Return a field as it is written to `rest.json`.

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
    either; every source given in `urls` keeps its URLs, sorted.

    Args:
        sources: The generated sources of each transport, each field's `description` already
            plain text (`plain_text`).
        fetched: The day the snapshot they came from was downloaded.
        urls: Per transport, the vendor URLs that document each source key: for REST, every
            documentation page that names the path (a path shared by several asset classes,
            such as `/v3/reference/tickers`, has one page per asset class).

    Returns:
        The file's text: `{"generated_by", "fetched", "urls": {<transport>: {<source key>:
        [<url>, ...]}}, "descriptions": {<transport>: {<source key>: {<wire key>: <text>}}}}`,
        one line per URL and per description, ending in a newline.
    """
    texts: dict[str, dict[str, dict[str, str]]] = {}
    for transport, described in sources.items():
        texts[transport.value] = {}
        for source in described:
            fields = {f.wire_key: f.description for f in source.fields if f.description}
            if fields:
                texts[transport.value][source.key] = fields
    document = {
        "generated_by": _GENERATED_BY,
        "fetched": fetched.isoformat(),
        "urls": {
            transport.value: {key: sorted(found) for key, found in by_key.items()}
            for transport, by_key in urls.items()
        },
        "descriptions": texts,
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
    out = [
        "{",
        f' "generated_by": {json.dumps(_GENERATED_BY)},',
        ' "sources": [',
    ]
    for index, path in enumerate(paths):
        out += [
            "  {",
            f'   "key": {json.dumps(path.key)},',
            f'   "result_key": {json.dumps(path.result_key)},',
            '   "params": [',
            *_rows([_line(_param_json(p)) for p in path.params], "    "),
            "   ],",
            '   "fields": [',
            *_rows([_line(_field_json(f)) for f in path.fields], "    "),
            "   ],",
            '   "memberships": []',
            "  }" + ("," if index < len(paths) - 1 else ""),
        ]
    out += [" ]", "}"]
    return "\n".join(out) + "\n"
