"""Tests for the four rules that turn a vendor description into plain text, and nothing else."""

from __future__ import annotations

import pytest

from deephaven_massive._massive.refresh.plain_text import plain_text

_ORIGIN = "https://massive.com"


@pytest.mark.parametrize(
    ("vendor", "plain"),
    [
        # Rule 1: HTML links.
        pytest.param(
            'See <a rel="nofollow" href="https://massive.com/glossary" target="_blank">the '
            "glossary</a>.",
            "See the glossary (https://massive.com/glossary).",
            id="html link with extra attributes",
        ),
        pytest.param(
            "<A HREF='https://x.example/a'>single\nquotes</A>",
            "single quotes (https://x.example/a)",
            id="html link in capitals with single quotes, spanning lines",
        ),
        pytest.param(
            "<a href=https://x.example/a>bare</a>",
            "bare (https://x.example/a)",
            id="html link with an unquoted href",
        ),
        pytest.param(
            '<a data-href="https://wrong.example" href="https://x.example/a">t</a>',
            "t (https://x.example/a)",
            id="an attribute ending in href is not href",
        ),
        pytest.param(
            '<a href="https://x.example/a" title="a>b">t</a>',
            "t (https://x.example/a)",
            id="a > inside a quoted attribute does not end the tag",
        ),
        pytest.param(
            '<a name="top">anchor</a>',
            '<a name="top">anchor</a>',
            id="an anchor without href is kept",
        ),
        # Rule 2: Markdown links.
        pytest.param(
            "See [Stocks Condition Codes](https://massive.com/docs/codes) for each code.",
            "See Stocks Condition Codes (https://massive.com/docs/codes) for each code.",
            id="markdown link",
        ),
        pytest.param(
            '[Exchanges](https://x.example/e "All exchanges") and [Codes](https://x.example/c '
            "'All codes')",
            "Exchanges (https://x.example/e) and Codes (https://x.example/c)",
            id="markdown links with titles",
        ),
        pytest.param(
            "[a [b] c](https://x.example/n)",
            "a [b] c (https://x.example/n)",
            id="markdown link with nested brackets",
        ),
        pytest.param("[x]() and [y]", "[x]() and [y]", id="no url is no link"),
        pytest.param(
            '[x]<a href="https://x.example/h">(https://x.example/m)</a>',
            "x (https://x.example/m) (https://x.example/h)",
            id="html links are rewritten before markdown links",
        ),
        # Rule 3: root-relative URLs.
        pytest.param(
            'See the <a href="/docs/rest/futures/products">Products endpoint</a>.',
            "See the Products endpoint (https://massive.com/docs/rest/futures/products).",
            id="root-relative html link",
        ),
        pytest.param(
            "[Exchanges](/docs/rest/stocks/exchanges)",
            "Exchanges (https://massive.com/docs/rest/stocks/exchanges)",
            id="root-relative markdown link",
        ),
        pytest.param(
            "[cdn](//cdn.example/x) and [page](docs/x) and the path /docs/x",
            "cdn (//cdn.example/x) and page (docs/x) and the path /docs/x",
            id="only a link's root-relative url is resolved",
        ),
        # Rule 4: whitespace.
        pytest.param(
            "  The trade's\n  exchange,\tsee\r\n\r\n the   list.  ",
            "The trade's exchange, see the list.",
            id="newlines and runs of spaces",
        ),
        # Nothing else.
        pytest.param(
            "The volume weighted average price.",
            "The volume weighted average price.",
            id="text with no link is unchanged",
        ),
        pytest.param(
            "SG&amp;A &lt; 5, <b>bold</b>, `code`, [not a link], and a very long sentence " * 3,
            (
                "SG&amp;A &lt; 5, <b>bold</b>, `code`, [not a link], and a very long sentence " * 3
            ).strip(),
            id="no unescaping, no other markup removed, no truncation",
        ),
        pytest.param("", "", id="empty"),
        pytest.param(" \n\t ", "", id="whitespace only"),
    ],
)
def test_plain_text(vendor: str, plain: str) -> None:
    assert plain_text(vendor, origin=_ORIGIN) == plain
