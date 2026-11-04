"""Check a user page for the mechanical part of the user-docs standard.

The standard is `docs/dev/user-docs.md`. This script finds what a pattern can find: the AI word
list, filler phrases, terms the terminology table forbids, inline code and title case in
headings, stacked headings, bold-label lists, em dashes, exclamation points, emoji, talk about
the document, hard-coded release versions, and code blocks no sentence introduces. Those are
FAIL lines, and the script exits 1 when it prints one.

It also prints WARN lines for patterns that need a reader's judgment: long sentences,
semicolons, colons in the middle of a sentence, contrasts, hedges, so-chains, a page with no
contractions, and runs of one-sentence paragraphs. Read each one and fix it or keep it on
purpose. Neither kind of line replaces the editor's pass against the rubric.

Usage, from the repository root:

    uv run python .claude/skills/writing-user-docs/scripts/check_user_doc.py README.md

It reads Markdown, skips fenced code for the prose checks, and checks shell blocks for
hard-coded versions.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

LONG_SENTENCE_WORDS = 30
MEAN_SENTENCE_WORDS = 20
PROSE_WORDS_NEEDING_CONTRACTIONS = 150
ONE_SENTENCE_RUN = 3
MIN_TITLE_CASE_WORDS = 2
MIN_BOLD_LABEL_ITEMS = 2

BANNED_WORDS = re.compile(
    r"\b(simply|just|easily|easy|seamless(?:ly)?|powerful|robust|leverag(?:e|es|ing)|"
    r"comprehensive|unlock(?:s)?|dive in|delve|crucial|pivotal|showcase(?:s)?|"
    r"underscore(?:s)?|boasts?|serves as|note that|worth noting|in order to|whether you're|"
    r"whether you are|please|in summary|overall,|is able to|allows you to)\b",
    re.IGNORECASE,
)
FORBIDDEN_TERMS = re.compile(
    r"\b(web UI|web IDE|workers?|transports?|flush(?:es|ed)?|REPL|tiers?|keyed tables?|"
    r"refreshing tables?|snapshot tables?|followed tables?|bulk files|S3 credentials)\b",
    re.IGNORECASE,
)
POLYGON = re.compile(r"\bPolygon\b")
FORMERLY_POLYGON = re.compile(r"formerly Polygon")
METADISCOURSE = re.compile(
    r"\bthis (README|guide|page|section|document|doc)\b|"
    r"\b(see|shown|described|listed|mentioned|explained) (above|below)\b|"
    r"\bthe (table|example|section|code|step|list)s? (above|below)\b|"
    r"\bhere's (what|how|the)\b|\blet's\b",
    re.IGNORECASE,
)
DASHES = re.compile(f"[{chr(0x2014)}{chr(0x2013)}]")  # the em dash and the en dash
EMOJI = re.compile(f"[{chr(0x1F300)}-{chr(0x1FAFF)}{chr(0x2600)}-{chr(0x27BF)}]")
VERSION = re.compile(r"(@v?\d+\.\d+|==\s*\d+\.\d+|\bv\d+\.\d+\.\d+\w*)")
EXAMPLE_MARK = re.compile(r"\b(example|such as|e\.g\.)\b", re.IGNORECASE)
CONTRAST = re.compile(r"\bnot [^.;:]{1,60}\bbut\b|\brather than\b|\binstead of\b|\bnot just\b")
HEDGES = re.compile(r"\b(generally|typically|usually|may|might|perhaps|arguably)\b")
SO_CHAIN = re.compile(r", so [^.]*, so ")
CONTRACTION = re.compile(r"\b\w+'(s|t|re|ll|ve|d|m)\b", re.IGNORECASE)
PROPER_WORDS = {
    "API", "Apple", "CLI", "Deephaven", "Docker", "GitHub", "IDE", "Java", "Jupyter", "Linux",
    "Massive", "Massive's", "PyPI", "Python", "REST", "S3", "US", "WSL", "WebSocket", "Windows",
    "macOS", "Stocks", "Options", "Basic", "Starter", "Developer", "Advanced", "Business", "SEC",
    "CME", "I", "OK",
}  # fmt: skip
SMALL_WORDS = {"a", "an", "and", "as", "at", "by", "for", "in", "of", "on", "or", "the", "to"}


@dataclass
class Report:
    """The FAIL and WARN lines found in one page.

    Attributes:
        path: The page checked, as given on the command line.
        lines: Each finding, already formatted as `path:line: KIND rule: excerpt`.
        failures: How many FAIL lines `lines` holds.
    """

    path: str
    lines: list[str] = field(default_factory=list)
    failures: int = 0

    def fail(self, number: int, rule: str, text: str) -> None:
        """Record a rule the page breaks."""
        self.lines.append(f"{self.path}:{number}: FAIL {rule}: {text.strip()[:110]}")
        self.failures += 1

    def warn(self, number: int, rule: str, text: str) -> None:
        """Record a pattern a reader must judge."""
        self.lines.append(f"{self.path}:{number}: WARN {rule}: {text.strip()[:110]}")


@dataclass
class Block:
    """A run of prose lines: a paragraph, a list item, a table row, or a heading."""

    kind: str
    start: int
    text: str


def plain(text: str) -> str:
    """Reduce inline code, links, anchors, and emphasis to plain words.

    Returns:
        The prose a reader hears, with each code span read as the word CODE.
    """
    text = re.sub(r"<a id=\"[^\"]*\"></a>", "", text)
    text = re.sub(r"`[^`]*`", "CODE", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "IMAGE", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<https?://[^>]*>", "URL", text)
    return text.replace("**", "").replace("*", "")


def split_blocks(lines: list[str], report: Report) -> list[Block]:
    """Split a page into prose blocks, checking each fenced code block on the way.

    Returns:
        The headings, paragraphs, list items, and table rows, in page order.
    """
    blocks: list[Block] = []
    fence = ""
    current: Block | None = None
    previous_prose = ""
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        marker = re.match(r"^(`{3,}|~{3,})", stripped)
        if fence:
            if marker and stripped.startswith(fence) and stripped.strip("`~") == "":
                fence = ""
            elif VERSION.search(line) and not EXAMPLE_MARK.search(line):
                report.warn(number, "hard-coded version in code", line)
            continue
        if marker:
            fence = marker.group(1)
            if not previous_prose.rstrip().endswith(":"):
                report.fail(number, "code block not introduced by a sentence ending in ':'", line)
            current = None
            continue
        if not stripped or stripped.startswith("<"):
            current = None
            continue
        previous_prose = stripped
        kind = block_kind(stripped)
        if current is None or kind != "paragraph" or current.kind != "paragraph":
            current = Block(kind, number, stripped)
            blocks.append(current)
        else:
            current.text += " " + stripped
    return blocks


def block_kind(stripped: str) -> str:
    """Name the kind of prose block a line starts or continues.

    Returns:
        One of "heading", "table", "item", or "paragraph".
    """
    if stripped.startswith("#"):
        return "heading"
    if stripped.startswith("|"):
        return "table"
    if re.match(r"^([-*+]|\d+\.)\s", stripped):
        return "item"
    return "paragraph"


def check_heading(block: Block, previous: Block | None, report: Report) -> None:
    """Check one heading's form, and that text separates it from the one before."""
    title = re.sub(r"^#+\s*", "", block.text)
    if "`" in title:
        report.fail(block.start, "inline code in a heading", block.text)
    if previous is not None and previous.kind == "heading":
        report.fail(block.start, "two headings with no text between them", block.text)
    words = re.findall(r"[A-Za-z][\w']*", re.sub(r"^\d+\.\s*", "", title))
    capitalized = [
        word
        for word in words[1:]
        if word[0].isupper() and word not in PROPER_WORDS and not word.isupper()
    ]
    if len(capitalized) >= MIN_TITLE_CASE_WORDS:
        report.fail(block.start, "title case heading (use sentence case)", block.text)


def check_words(block: Block, report: Report) -> None:
    """Check one block's words against the banned lists."""
    text = plain(block.text)
    for pattern, rule in (
        (BANNED_WORDS, "AI word or filler phrase"),
        (FORBIDDEN_TERMS, "term the terminology table forbids"),
        (METADISCOURSE, "talk about the document, or above/below"),
        (DASHES, "em or en dash"),
        (EMOJI, "emoji"),
    ):
        match = pattern.search(text)
        if match:
            report.fail(block.start, f"{rule} ({match.group(0)!r})", block.text)
    if POLYGON.search(text) and not FORMERLY_POLYGON.search(text):
        report.fail(block.start, "Polygon outside 'formerly Polygon.io'", block.text)
    if "!" in re.sub(r"!\[|!=", "", text):
        report.fail(block.start, "exclamation point", block.text)
    if VERSION.search(block.text) and not EXAMPLE_MARK.search(block.text):
        report.warn(block.start, "hard-coded version (mark it as an example)", block.text)


def sentences(text: str) -> list[str]:
    """Split plain prose into sentences.

    Returns:
        Each sentence that holds at least one letter.
    """
    parts = re.split(r"(?<=[.?!])\s+(?=[A-Z0-9\"(])", text)
    return [part for part in parts if re.search(r"[A-Za-z]", part)]


def check_sentences(block: Block, report: Report) -> list[int]:
    """Warn about the sentence-level patterns in one block.

    Returns:
        The length of each of the block's sentences, in words.
    """
    text = plain(re.sub(r"^([-*+]|\d+\.)\s+", "", block.text))
    found = sentences(text)
    for sentence in found:
        words = len(sentence.split())
        if words > LONG_SENTENCE_WORDS:
            report.warn(block.start, f"sentence of {words} words (split it)", sentence)
        body = sentence.rstrip(":")
        if ";" in body:
            report.warn(block.start, "semicolon in prose (one idea per sentence)", sentence)
        if re.search(r"\w: [a-z]", body):
            report.warn(block.start, "colon mid-sentence (a reveal, or two ideas?)", sentence)
    for pattern, rule in (
        (CONTRAST, "contrast: is the misconception real?"),
        (HEDGES, "hedge: is the behavior really uncertain?"),
        (SO_CHAIN, "so-chain"),
    ):
        match = pattern.search(text)
        if match:
            report.warn(block.start, f"{rule} ({match.group(0)!r})", block.text)
    if text.count("(see") > 1:
        report.warn(block.start, "more than one '(see ...)' in a paragraph", block.text)
    if re.search(r"\?\s+[A-Z]", text):
        report.warn(block.start, "question answered at once?", block.text)
    return [len(sentence.split()) for sentence in found]


def check_lists(blocks: list[Block], report: Report) -> None:
    """Fail a list whose items open with bold labels, and warn about bold run-in paragraphs."""
    run: list[Block] = []
    for block in [*blocks, Block("end", 0, "")]:
        if block.kind == "item" and re.match(r"^([-*+]|\d+\.)\s+\*\*[^*]+\*\*", block.text):
            run.append(block)
            continue
        if len(run) >= MIN_BOLD_LABEL_ITEMS:
            report.fail(run[0].start, "list items that open with bold labels", run[0].text)
        run = []
        if block.kind == "paragraph" and re.match(r"^\*\*[^*]+\*\*", block.text):
            report.warn(
                block.start, "bold run-in paragraph (only for true alternatives)", block.text
            )


def check_paragraph_runs(blocks: list[Block], report: Report) -> None:
    """Warn about a run of one-sentence paragraphs."""
    run: list[Block] = []
    for block in [*blocks, Block("end", 0, "")]:
        if block.kind == "paragraph" and len(sentences(plain(block.text))) == 1:
            run.append(block)
            continue
        if len(run) >= ONE_SENTENCE_RUN:
            report.warn(run[0].start, f"{len(run)} one-sentence paragraphs in a row", run[0].text)
        run = []


def check(path: Path) -> Report:
    """Check one page.

    Returns:
        The page's FAIL and WARN lines.
    """
    report = Report(str(path))
    blocks = split_blocks(path.read_text(encoding="utf-8").splitlines(), report)
    lengths: list[int] = []
    previous: Block | None = None
    for block in blocks:
        if block.kind == "heading":
            check_heading(block, previous, report)
        else:
            check_words(block, report)
            if block.kind != "table":
                lengths += check_sentences(block, report)
        previous = block
    check_lists(blocks, report)
    check_paragraph_runs(blocks, report)
    words = sum(lengths)
    if lengths and words / len(lengths) > MEAN_SENTENCE_WORDS:
        report.warn(0, f"mean sentence length {words / len(lengths):.1f} words", "aim for 15 to 20")
    prose = " ".join(plain(block.text) for block in blocks if block.kind != "heading")
    if words > PROSE_WORDS_NEEDING_CONTRACTIONS and not CONTRACTION.search(prose):
        report.warn(0, f"no contractions in {words} words of prose", "write like a colleague")
    return report


def main(arguments: list[str]) -> int:
    """Check each page named on the command line.

    Returns:
        The exit status: 0 when no page has a FAIL line, 1 when one does, 2 without pages.
    """
    if not arguments:
        sys.stderr.write("usage: check_user_doc.py <page.md> [<page.md> ...]\n")
        return 2
    failures = 0
    for argument in arguments:
        report = check(Path(argument))
        sys.stdout.writelines(line + "\n" for line in report.lines)
        warnings = len(report.lines) - report.failures
        sys.stdout.write(f"{argument}: {report.failures} FAIL, {warnings} WARN\n")
        failures += report.failures
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
