"""The knowledge-base articles the library relies on, and how one is turned into Markdown.

Massive's knowledge base (`https://massive.com/knowledge-base/article/<slug>`) states some
rules its API documentation does not, and the library depends on a few of them: the wording of
the HTTP 429 that marks a Basic subscription, for one. The documentation index (`llms.txt`)
links no article, and articles are published as HTML only (the `.md` address of one answers
HTTP 404), so they cannot be vendored the way documentation pages are. Instead:

- `ARTICLES` names each article the library relies on, by its slug, with what the library
  takes from it. It is short and reviewed: an article enters it only when code depends on
  what the article says;
- `fetch` downloads each article's HTML page (`article_url`) and stores the Markdown that
  `article_markdown` extracts from it, so `--check` reports a change of wording like any other
  vendored file, and `drift` names what the library takes from the article that changed.

`snapshot` owns where an article is stored (`knowledge-base/<slug>.md`). The page is
untrusted vendor HTML: it is parsed with the standard library's `html.parser`, never evaluated,
and only the text of its title and of its `<article>` element is kept, as plain Markdown.

It imports the standard library only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, TypeVar
from urllib.parse import urljoin

_F = TypeVar("_F")

if TYPE_CHECKING:
    from collections.abc import Mapping

    from typing_extensions import override
else:

    def override(method: _F) -> _F:
        """Return `method` unchanged: `typing.override` exists only from Python 3.12.

        Type checkers read `typing_extensions.override` instead, from its bundled stub, so
        overrides stay checked without a run-time dependency.
        """
        return method


__all__ = [
    "ARTICLES",
    "ARTICLE_PREFIX",
    "KnowledgeBaseError",
    "article_markdown",
    "article_url",
]

ARTICLE_PREFIX: Final = "https://massive.com/knowledge-base/article/"
"""Where every knowledge-base article is published, followed by its slug."""

ARTICLES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "what-is-the-request-limit-for-massives-restful-apis": (
            "the REST rate governor takes a 429 whose message says \"You've exceeded the "
            'maximum requests per minute" as the mark of a Basic subscription, and charges '
            "reference and cross-asset endpoints to a bucket of their own, because the article "
            "says they are counted in their own bucket"
        ),
    }
)
"""Each knowledge-base article the library relies on: slug to what the library takes from it."""

# The suffix Massive's pages add to an article's title in the `<title>` element.
_TITLE_SUFFIX: Final = " | Massive"
# The heading elements, by their Markdown level.
_HEADINGS: Final = MappingProxyType({f"h{level}": level for level in range(1, 7)})
# The inline elements written as Markdown emphasis, by their delimiter.
_EMPHASIS: Final = MappingProxyType({"strong": "**", "b": "**", "em": "*", "i": "*"})
# The elements that flow within a line of text; every other element starts a block.
_INLINE: Final = frozenset(
    {*_EMPHASIS, "a", "abbr", "br", "code", "mark", "small", "span", "sub", "sup", "u"}
)
# The elements that have no end tag, so they never hold children.
_VOID: Final = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "wbr"}
)
_LISTS: Final = frozenset({"ul", "ol"})
_WHITESPACE: Final = re.compile(r"\s+")


class KnowledgeBaseError(ValueError):
    """An article page lacks the title or the `<article>` element its Markdown is made of."""


def article_url(slug: str) -> str:
    """Return the address of a knowledge-base article's HTML page.

    Args:
        slug: The article's slug, one of `ARTICLES`.

    Returns:
        `ARTICLE_PREFIX` followed by the slug.
    """
    return ARTICLE_PREFIX + slug


@dataclass
class _Element:
    """One element of the article: its tag, attributes, and children (elements and text)."""

    tag: str
    attrs: dict[str, str]
    children: list[_Element | str] = field(default_factory=list)


class _Parser(HTMLParser):
    """Collect a page's title and the element tree of its first `<article>` element.

    Created by `article_markdown` for one page and fed it once. An end tag closes the nearest
    open element of its name and every element opened inside it; an end tag with no open
    element of its name is ignored, as a browser ignores it.
    """

    def __init__(self) -> None:
        """Start with no title and no article."""
        super().__init__(convert_charrefs=True)
        self.title: list[str] = []
        self.article: _Element | None = None
        self._titled = False
        self._in_title = False
        self._open: list[_Element] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Open an element of the article, the article itself, or the first title."""
        self._in_title = tag == "title" and not self._titled
        self._titled = self._titled or self._in_title
        element = _Element(tag, {name: value or "" for name, value in attrs})
        if self._open:
            self._open[-1].children.append(element)
        elif tag == "article" and self.article is None:
            self.article = element
        else:
            return
        if tag not in _VOID:
            self._open.append(element)

    @override
    def handle_endtag(self, tag: str) -> None:
        """Close the nearest open element named `tag`, and every element inside it."""
        self._in_title = False
        names = [element.tag for element in self._open]
        if tag in names:
            del self._open[len(names) - 1 - names[::-1].index(tag) :]

    @override
    def handle_data(self, data: str) -> None:
        """Keep the text of the title and of the article."""
        if self._in_title:
            self.title.append(data)
        elif self._open:
            self._open[-1].children.append(data)


def _collapse(text: str) -> str:
    """Return text on one line, with each run of whitespace one space and none at its ends.

    Args:
        text: The text.

    Returns:
        The collapsed text.
    """
    return _WHITESPACE.sub(" ", text).strip()


def _inline(node: _Element | str, url: str) -> str:
    """Return the Markdown of a node's text, with its links, emphasis, and code.

    A nested list contributes nothing: `_list` writes it as lines of its own.

    Args:
        node: An element or a piece of text.
        url: The page's address, which relative links are resolved against.

    Returns:
        The node's inline Markdown, its whitespace not yet collapsed.
    """
    if isinstance(node, str):
        return node
    if node.tag in _LISTS:
        return ""
    text = " " if node.tag == "br" else "".join(_inline(child, url) for child in node.children)
    return _marked(node, text, url)


def _marked(node: _Element, text: str, url: str) -> str:
    """Return an element's text with the Markdown its element calls for.

    Args:
        node: The element.
        text: The Markdown of its children.
        url: The page's address, which a relative link is resolved against.

    Returns:
        A link as `[text](url)`, emphasis between `**` or `*`, code (a `code` element, or an
        element of the `font-mono` class, which is how Massive's pages show code) between
        backticks, a block element's text between spaces, and any other text as it is.
    """
    if node.tag == "a" and "href" in node.attrs:
        return f"[{text}]({urljoin(url, node.attrs['href'])})"
    if node.tag not in _INLINE:
        return f" {text} "
    code = node.tag == "code" or "font-mono" in node.attrs.get("class", "").split()
    delimiter = "`" if code else _EMPHASIS.get(node.tag, "")
    return f"{delimiter}{text}{delimiter}"


def _list(node: _Element, url: str, depth: int) -> list[str]:
    """Return the lines of a list: one per item, each nested list indented below its item.

    Args:
        node: A `ul` or `ol` element.
        url: The page's address.
        depth: How many lists enclose this one.

    Returns:
        `- <item>` (or `<n>. <item>` for an ordered list) per item, indented two spaces per
        enclosing list.
    """
    lines: list[str] = []
    items = [child for child in node.children if isinstance(child, _Element)]
    for number, item in enumerate(items, start=1):
        marker = f"{number}." if node.tag == "ol" else "-"
        lines.append(f"{'  ' * depth}{marker} {_collapse(_inline(item, url))}")
        for child in item.children:
            if isinstance(child, _Element) and child.tag in _LISTS:
                lines += _list(child, url, depth + 1)
    return lines


def _rows(node: _Element) -> list[_Element]:
    """Return every row of a table, in order, whether or not a section holds it.

    Args:
        node: A `table` element, or a section of one.

    Returns:
        The `tr` elements.
    """
    rows: list[_Element] = []
    for child in node.children:
        if isinstance(child, _Element):
            rows += [child] if child.tag == "tr" else _rows(child)
    return rows


def _table(node: _Element, url: str) -> str:
    """Return a table in Markdown, its first row the header.

    Args:
        node: A `table` element.
        url: The page's address.

    Returns:
        One line per row and a separator after the first, every row padded to the widest;
        empty for a table without cells.
    """
    rows = [
        [
            _collapse(_inline(cell, url)).replace("|", "\\|")
            for cell in row.children
            if isinstance(cell, _Element)
        ]
        for row in _rows(node)
    ]
    rows = [row for row in rows if row]
    if not rows:
        return ""
    width = max(len(row) for row in rows)
    lines = [f"| {' | '.join(row + [''] * (width - len(row)))} |" for row in rows]
    lines.insert(1, f"|{'---|' * width}")
    return "\n".join(lines)


def _blocks(node: _Element, url: str) -> list[str]:
    """Return the Markdown blocks of an element's children.

    Text and inline elements that sit between blocks form a paragraph of their own.

    Args:
        node: An element that holds blocks.
        url: The page's address.

    Returns:
        Each non-empty block: a paragraph, a heading, a list, or a table.
    """
    blocks: list[str] = []
    run: list[str] = []
    # An empty paragraph at the end ends the last run of text like any block does.
    for child in [*node.children, _Element("p", {})]:
        if isinstance(child, str) or child.tag in _INLINE:
            run.append(_inline(child, url))
            continue
        blocks.append(_collapse("".join(run)))
        run = []
        if child.tag in _HEADINGS:
            text = _collapse(_inline(child, url))
            blocks.append(f"{'#' * _HEADINGS[child.tag]} {text}" if text else "")
        elif child.tag == "p":
            blocks.append(_collapse(_inline(child, url)))
        elif child.tag in _LISTS:
            blocks.append("\n".join(_list(child, url, 0)))
        elif child.tag == "table":
            blocks.append(_table(child, url))
        else:
            blocks += _blocks(child, url)
    return [block for block in blocks if block]


def article_markdown(html: str, url: str) -> str:
    """Extract an article's title and text from its HTML page, as Markdown.

    The title is the page's `<title>` without the `" | Massive"` Massive adds to it; the text
    is the first `<article>` element: its paragraphs, headings, lists, and tables, with links
    (resolved against `url`), emphasis, and code in their Markdown form. Navigation, scripts,
    and everything else outside the article are dropped, so the result changes only when the
    article does.

    Args:
        html: The page, decoded.
        url: The page's address, for relative links and the error message.

    Returns:
        `# <title>`, a blank line, then the article's blocks separated by blank lines, ending
        with a newline.

    Raises:
        KnowledgeBaseError: If the page has no title, no `<article>` element, or an article
            with no text.
    """
    parser = _Parser()
    parser.feed(html)
    parser.close()
    title = _collapse("".join(parser.title)).removesuffix(_TITLE_SUFFIX)
    if not title:
        raise KnowledgeBaseError(f"{url} has no title")
    if parser.article is None:
        raise KnowledgeBaseError(f"{url} has no <article> element")
    blocks = _blocks(parser.article, url)
    if not blocks:
        raise KnowledgeBaseError(f"{url} has an article with no text")
    return "\n\n".join([f"# {title}", *blocks]) + "\n"
