"""Tests for the knowledge-base article list and the Markdown extracted from an article page.

Each page is a small synthetic HTML document shaped like Massive's: a `<title>` ending in
" | Massive", navigation and scripts around one `<article>` element, and code shown as
`<span class="... font-mono ...">`.
"""

from __future__ import annotations

import re

import pytest

from deephaven_massive._massive.refresh import knowledge_base
from deephaven_massive._massive.refresh.knowledge_base import KnowledgeBaseError

URL = "https://massive.com/knowledge-base/article/a-question"


def page(article: str, *, title: str = "A question? | Massive", head: str = "") -> str:
    # A page with navigation and a script around the article, as Massive serves it.
    return (
        f"<!DOCTYPE html><html><head><meta charSet='utf-8'/><title>{title}</title>{head}</head>"
        "<body><nav><a href='/'>Home</a></nav><script>var x = '<article>';</script>"
        f"<p class='text-xl'>The summary above the article.</p>{article}"
        "<footer>Massive</footer></body></html>"
    )


def markdown(article: str, **kwargs: str) -> str:
    return knowledge_base.article_markdown(page(article, **kwargs), URL)


def test_every_article_has_a_plain_slug_and_a_reason() -> None:
    assert knowledge_base.ARTICLES
    for slug, reason in knowledge_base.ARTICLES.items():
        assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", slug)
        assert reason.strip()


def test_an_article_is_published_below_the_knowledge_base_prefix() -> None:
    assert knowledge_base.article_url("a-question") == URL


def test_paragraphs_and_headings_keep_only_the_article() -> None:
    article = (
        "<article><p>First   line\n  continues.</p>"
        "<h2 class='x'>A <!-- -->heading</h2><p>Second &amp; last.</p></article>"
    )
    assert markdown(article) == (
        "# A question?\n\nFirst line continues.\n\n## A heading\n\nSecond & last.\n"
    )


def test_links_emphasis_and_code_take_their_markdown_form() -> None:
    article = (
        "<article><p><strong>Bold</strong>, <b>bold</b>, <em>it</em>, <i>it</i>, "
        "<code>code</code>, <span class='rounded font-mono text-sm'>{\"a\":1}</span>, "
        "<span class='plain'>plain</span>, a<br>break, "
        "<a href='/knowledge-base/article/other'>relative</a>, "
        "<a href='https://example.com/x'>absolute</a>, and <a name='n'>no link</a>.</p>"
        "</article>"
    )
    assert markdown(article).splitlines()[2] == (
        '**Bold**, **bold**, *it*, *it*, `code`, `{"a":1}`, plain, a break, '
        "[relative](https://massive.com/knowledge-base/article/other), "
        "[absolute](https://example.com/x), and no link."
    )


def test_lists_number_ordered_items_and_indent_nested_ones() -> None:
    article = (
        "<article><ul>\n<li>One</li>\n<li>Two<ol><li>Two a</li><li><p>Two b</p></li></ol></li>"
        "</ul><ol><li>First</li><li>Second</li></ol></article>"
    )
    assert markdown(article) == (
        "# A question?\n\n- One\n- Two\n  1. Two a\n  2. Two b\n\n1. First\n2. Second\n"
    )


def test_tables_put_a_separator_after_the_first_row_and_pad_short_rows() -> None:
    article = (
        "<article><table>\n<thead><tr><th>Kind</th>\n<th>Limit</th></tr></thead>\n"
        "<tbody><tr><td>Basic</td><td>5 | minute</td></tr><tr><td>Paid</td></tr><tr></tr>"
        "</tbody><tr><td>Loose</td><td>row</td></tr></table></article>"
    )
    assert markdown(article) == (
        "# A question?\n\n| Kind | Limit |\n|---|---|\n| Basic | 5 \\| minute |\n"
        "| Paid |  |\n| Loose | row |\n"
    )


def test_text_between_blocks_and_inside_containers_forms_paragraphs() -> None:
    article = (
        "<article>Loose <em>text</em><div><p>Inside a div.</p>more loose</div>"
        "<span>Trailing <div>block in a span</div> text</span><hr><h3> </h3>"
        "<table></table><img src='x.png'></article>"
    )
    assert markdown(article) == (
        "# A question?\n\nLoose *text*\n\nInside a div.\n\nmore loose\n\n"
        "Trailing block in a span text\n"
    )


def test_a_stray_end_tag_is_ignored_and_an_open_one_is_closed_by_its_parent() -> None:
    article = "<article><p>One</em> <strong>two</p><p>three</p></article>"
    assert markdown(article) == "# A question?\n\nOne **two**\n\nthree\n"


def test_only_the_first_article_and_the_first_title_count() -> None:
    article = "<article><p>Kept.</p></article><article><p>Dropped.</p></article>"
    head = "<svg><title>An icon</title></svg>"
    assert markdown(article, head=head) == "# A question?\n\nKept.\n"


def test_a_title_without_massive_s_suffix_is_kept_whole() -> None:
    assert markdown("<article><p>x</p></article>", title=" A  title ") == "# A title\n\nx\n"


@pytest.mark.parametrize(
    ("html", "message"),
    [
        (page("<article><p>x</p></article>", title=" \n "), "has no title"),
        ("<html><body><article><p>x</p></article></body></html>", "has no title"),
        (page("<main><p>x</p></main>"), "has no <article> element"),
        (page("<article> <p> </p><ul></ul></article>"), "has an article with no text"),
    ],
    ids=["empty-title", "no-title", "no-article", "empty-article"],
)
def test_a_page_without_a_title_or_article_text_is_refused(html: str, message: str) -> None:
    with pytest.raises(KnowledgeBaseError, match=f"^{re.escape(URL)} {message}$"):
        knowledge_base.article_markdown(html, URL)
