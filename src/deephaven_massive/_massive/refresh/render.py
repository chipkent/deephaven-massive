"""Render the generated REST schema, `spec/rest.json`, as its canonical text.

The generators produce `RestPath` values; this module turns them into the text of the
committed file the runtime loads (`spec.sources()`), and nothing else; `generate` writes it.
The text is deterministic, so a refresh
that changes nothing leaves the file byte for byte as it was, and the parity test can compare
the committed file with a fresh rendering. It is JSON with one line per field and per
parameter, so a refresh diff shows each changed field or parameter as one changed line.

It imports the standard library and `spec.types` only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._massive.refresh.openapi import Json
    from deephaven_massive._massive.spec.types import Field, Param, RestPath

__all__ = ["REST_FILE", "render_rest"]

REST_FILE: Final = Path(__file__).parent.parent / "spec" / "rest.json"
"""The generated REST schema the refresh writes."""

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


def render_rest(paths: Sequence[RestPath], *, openapi_sha256: str) -> str:
    """Return the canonical text of `rest.json`.

    Paths keep their order; within a field or parameter, keys are sorted. The file records the
    command that wrote it and the stored hash of the specification it came from.

    Args:
        paths: The generated paths.
        openapi_sha256: The stored hash of the specification they came from.

    Returns:
        The file's text, ending in a newline.
    """
    out = [
        "{",
        f' "generated_by": {json.dumps(_GENERATED_BY)},',
        f' "openapi_sha256": {json.dumps(openapi_sha256)},',
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
