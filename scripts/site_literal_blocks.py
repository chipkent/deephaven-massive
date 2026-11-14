r"""Render the docstrings' reST literal blocks as fenced code blocks on the documentation site.

The docstrings mark an example with reST's literal-block marker, as `help()` and Sphinx read it:
a paragraph ending in `::`, or a line holding only `::`, followed by an indented block. The
site's API reference renders docstrings as Markdown, which shows the `::` as text and the block
without highlighting. This griffe extension, which `zensical.toml` loads by path for the API
reference, rewrites each such block into a fenced code block before the docstring is parsed:
the trailing `::` becomes `:`, a line holding only `::` is dropped, and the block is fenced as
`python` when it parses as Python, and as plain text otherwise. The source is not changed.

>>> print(fence_literal_blocks("An example:\n    ::\n\n        m = 1\n        n = 2\n\nAfter."))
An example:
    ```python
    m = 1
    n = 2
    ```
<BLANKLINE>
After.
>>> print(fence_literal_blocks("A file such as::\n\n    key: [a, b\n\nAfter."))
A file such as:
<BLANKLINE>
```text
key: [a, b
```
<BLANKLINE>
After.
>>> print(fence_literal_blocks("A block::\n\n    if x:\n\n        y()\nEnd."))
A block:
<BLANKLINE>
```python
if x:
<BLANKLINE>
    y()
```
End.
>>> fence_literal_blocks("No block follows::\nsame indent.")
'No block follows::\nsame indent.'
>>> fence_literal_blocks("A slice a[::2] and C++ :: scopes stay.")
'A slice a[::2] and C++ :: scopes stay.'
"""

from __future__ import annotations

import ast
import re

import griffe
from typing_extensions import override

_MARKER = re.compile(r"^(?P<indent>[ ]*)(?P<text>.*?)::[ ]*$")


def _indent(line: str) -> int:
    """Return the number of spaces a line starts with."""
    return len(line) - len(line.lstrip(" "))


def _language(code: str) -> str:
    """Return `python` when a block parses as Python, else `text`."""
    try:
        ast.parse(code)
    except SyntaxError:
        return "text"
    return "python"


def fence_literal_blocks(text: str) -> str:
    """Return a docstring with each reST literal block rewritten as a fenced code block.

    Args:
        text: The docstring, without its quotes.

    Returns:
        The docstring with each `::` marker removed and the indented block after it fenced. A
        marker with no block indented deeper after it is left as it is.
    """
    lines = text.split("\n")
    out: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        marker = _MARKER.match(line)
        start = index + 1
        while start < len(lines) and not lines[start].strip():
            start += 1
        if marker is None or start == len(lines) or _indent(lines[start]) <= len(marker["indent"]):
            out.append(line)
            index += 1
            continue
        base = marker["indent"]
        end = start
        while end < len(lines) and (not lines[end].strip() or _indent(lines[end]) > len(base)):
            end += 1
        while not lines[end - 1].strip():
            end -= 1
        block = lines[start:end]
        depth = min(_indent(row) for row in block if row.strip())
        code = [row[depth:] for row in block]
        if marker["text"]:
            out.extend((f"{base}{marker['text']}:", ""))
        out.append(f"{base}```{_language(chr(10).join(code))}")
        out.extend(f"{base}{row}" if row else "" for row in code)
        out.append(f"{base}```")
        index = end
    return "\n".join(out)


class LiteralBlocks(griffe.Extension):
    """The griffe extension: fences the literal blocks of every docstring it loads."""

    @override
    def on_instance(self, **kwargs: object) -> None:
        """Rewrite the docstring of the object griffe has just loaded, if it has one.

        Args:
            **kwargs: The hook's arguments, which griffe passes by keyword: `obj` is the
                module, class, function, or attribute loaded.
        """
        obj = kwargs["obj"]
        if isinstance(obj, griffe.Object) and obj.docstring is not None:
            obj.docstring.value = fence_literal_blocks(obj.docstring.value)
