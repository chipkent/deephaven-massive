"""Check a user page for the patterns of the user-docs standard that a script can find.

The standard is `docs/dev/user-docs.md`, and the judge is the skill's grader,
`agents/editor.md`. This script is a hint: it finds the patterns a regular expression finds
reliably, so the writer fixes them before an editor reads the page.

FAIL lines are rules with no exceptions: hype and filler words, history asides ("formerly"),
calling the repository private, talk about the document, em dashes, emoji, exclamation points,
inline code or title case in headings, stacked headings, and code blocks no sentence
introduces. The script exits 1 when it prints one.

WARN lines need a reader's judgment. Most are the structural tells that made pages read as
machine-written: two or more ", such as" or ", which" clauses in a sentence, asides in
parentheses, phrases repeated across the page, an alternative given after its option, a
paragraph that opens on a condition, estimates with no source, shell variables that never
change, an opening longer than the hook's budget, sections the owner didn't ask for, long or
uniform sentences, and long paragraphs.

Usage, from the repository root:

    uv run python .claude/skills/writing-user-docs/scripts/check_user_doc.py README.md

It reads Markdown, skips fenced code for the prose checks, and checks shell blocks for
hard-coded versions and constant variables.
"""

from __future__ import annotations

import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

LONG_SENTENCE_WORDS = 30
MEAN_SENTENCE_WORDS = 20
MIN_SENTENCES_FOR_RHYTHM = 15
UNIFORM_RHYTHM_DEVIATION = 6.0
LONG_PARAGRAPH_SENTENCES = 6
ONE_SENTENCE_RUN = 3
MIN_TITLE_CASE_WORDS = 2
MIN_BOLD_LABEL_ITEMS = 3
STACKED_CLAUSES = 2
HOOK_WORD_BUDGET = 150
REPEATED_PHRASE_WORDS = 4
REPEATED_PHRASE_COUNT = 3
MIN_CONTENT_WORDS_IN_PHRASE = 3
MAX_REPEATED_PHRASES = 12
MIN_ASIDE_WORDS = 2
ASIDES_PER_PARAGRAPH = 3

BANNED_WORDS = re.compile(
    r"\b(simply|just|easily|seamless(?:ly)?|powerful|robust|leverag(?:e|es|ing)|"
    r"comprehensive|unlock(?:s)?|dive in|delve|crucial|pivotal|showcase(?:s)?|"
    r"underscore(?:s)?|boasts?|serves as|note that|worth noting|in order to|whether you're|"
    r"whether you are|please|in summary|overall,|is able to|allows you to)\b",
    re.IGNORECASE,
)
HISTORY_ASIDE = re.compile(r"\bformerly\b|\bPolygon\b", re.IGNORECASE)
PRIVATE_REPOSITORY = re.compile(
    r"\bprivate (repo|repository)\b|\b(repo|repository) is private\b", re.IGNORECASE
)
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
CLAUSE_MARK = re.compile(r", (such as|which|including|like|as in)\b")
# A noun phrase set off by commas after a noun: "a watchlist, a table of symbols you edit, ..."
APPOSITIVE = re.compile(r"\w, (a|an|the) [^,.;:()]{3,60}, (?!and\b|or\b)")
QUESTION_ANSWERED = re.compile(r"\?\s+[A-Z]")
PARENTHETICAL = re.compile(r"\(([^()]*[A-Za-z]{3,}[^()]*)\)")
ESTIMATE = re.compile(
    r"\b(about|around|roughly|approximately|up to|at least|a few|within|takes?)\s+"
    r"(a|an|\d[\d,.]*)\s*(seconds?|minutes?|hours?|GB|MB|KB|ms)\b",
    re.IGNORECASE,
)
CONDITION_OPENER = re.compile(r"^(If|Once|Until|Unless|On|Before|After)\b")
LATE_ALTERNATIVE = re.compile(r"\binstead\b|^(Or|Alternatively)\b|^If you (use|prefer)\b")
CONTRAST = re.compile(r"\bnot [^.;:]{1,60}\bbut\b|\brather than\b|\bnot just\b")
SO_CHAIN = re.compile(r", so [^.]*, so ")
UNREQUESTED_SECTION = re.compile(
    r"^(recipes?|common errors|fix common errors|status|terms|glossary|how it works|how do i|"
    r"faq|frequently asked questions)\b",
    re.IGNORECASE,
)
SHELL_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Z][A-Z0-9_]*)=(\S+)\s*$")
SHELL_LANGUAGES = {"bash", "sh", "shell", "console", "zsh"}
STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "can", "code", "each", "for", "from", "if",
    "in", "into", "is", "it", "its", "not", "of", "on", "one", "or", "that", "the", "their",
    "then", "this", "to", "when", "with", "you", "your",
}  # fmt: skip
PROPER_WORDS = {
    "API", "Apple", "CLI", "Deephaven", "Docker", "GitHub", "IDE", "Java", "Jupyter", "Linux",
    "Massive", "Massive's", "PyPI", "Python", "REST", "S3", "US", "WSL", "WebSocket", "Windows",
    "macOS", "Stocks", "Options", "Basic", "Starter", "Developer", "Advanced", "Business", "SEC",
    "CME", "I", "OK", "Microsoft",
}  # fmt: skip


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
    """A run of page lines: a paragraph, a list item, a table row, a heading, or a code block.

    Attributes:
        kind: "heading", "table", "item", "paragraph", or "code".
        start: The page line the block starts on.
        text: The block's text; for code, its lines, each ending in a newline.
        language: For a code block, the fence's info string, such as "bash".
    """

    kind: str
    start: int
    text: str
    language: str = ""


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
    """Split a page into blocks, failing each code block no sentence introduces.

    Returns:
        The headings, paragraphs, list items, table rows, and code blocks, in page order.
    """
    blocks: list[Block] = []
    code: Block | None = None
    fence = ""
    current: Block | None = None
    previous_prose = ""
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        marker = re.match(r"^(`{3,}|~{3,})(\S*)", stripped)
        if code is not None:
            if marker and stripped.startswith(fence) and stripped.strip("`~") == "":
                code = None
            else:
                code.text += line + "\n"
            continue
        if marker:
            fence = marker.group(1)
            code = Block("code", number, "", marker.group(2).lower())
            blocks.append(code)
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
    """Check one heading's form, its words, and that text separates it from the one before."""
    title = re.sub(r"^#+\s*", "", block.text)
    if "`" in title:
        report.fail(block.start, "inline code in a heading", block.text)
    if previous is not None and previous.kind == "heading":
        report.fail(block.start, "two headings with no text between them", block.text)
    if re.match(r"^\d+\.", title):
        report.warn(block.start, "number in a heading (titles carry no sequence)", block.text)
    if UNREQUESTED_SECTION.search(title):
        report.warn(block.start, "a section the owner didn't ask for?", block.text)
    words = re.findall(r"[A-Za-z][\w']*", re.sub(r"^\d+\.\s*", "", title))
    capitalized = [
        word
        for word in words[1:]
        if word[0].isupper() and word not in PROPER_WORDS and not word.isupper()
    ]
    if len(capitalized) >= MIN_TITLE_CASE_WORDS:
        report.fail(block.start, "title case heading (use sentence case)", block.text)
    content = [word.lower() for word in words if word.lower() not in STOP_WORDS]
    repeated = sorted({word for word in content if content.count(word) > 1})
    if repeated:
        report.warn(block.start, f"word repeated in a heading ({', '.join(repeated)})", title)


def check_words(block: Block, report: Report) -> None:
    """Check one block's words against the rules with no exceptions, and flag estimates.

    A word in double quotes is mentioned, not used, so the word rules skip quoted text.
    """
    text = re.sub(r'"[^"]*"', "QUOTE", plain(block.text))
    for pattern, rule in (
        (BANNED_WORDS, "hype or filler word"),
        (HISTORY_ASIDE, "history aside: name the product as it is now"),
        (PRIVATE_REPOSITORY, "never describe the repository as private"),
        (METADISCOURSE, "talk about the document, or above/below"),
        (DASHES, "em or en dash"),
        (EMOJI, "emoji"),
    ):
        match = pattern.search(text)
        if match:
            report.fail(block.start, f"{rule} ({match.group(0)!r})", block.text)
    if "!" in re.sub(r"!\[|!=", "", text):
        report.fail(block.start, "exclamation point", block.text)
    if VERSION.search(block.text) and not EXAMPLE_MARK.search(block.text):
        report.warn(block.start, "hard-coded version (mark it as an example)", block.text)
    match = ESTIMATE.search(text)
    if match:
        report.warn(
            block.start, f"estimate ({match.group(0)!r}): cite its source or cut it", block.text
        )


def sentences(text: str) -> list[str]:
    """Split plain prose into sentences.

    Returns:
        Each sentence that holds at least one letter.
    """
    parts = re.split(r"(?<=[.?!])\s+(?=[A-Z0-9\"(])", text)
    return [part for part in parts if re.search(r"[A-Za-z]", part)]


def check_sentence(start: int, sentence: str, report: Report) -> int:
    """Warn about the patterns inside one sentence.

    Returns:
        How many asides the sentence holds: clauses such as ", which" and appositives.
    """
    words = len(sentence.split())
    if words > LONG_SENTENCE_WORDS:
        report.warn(start, f"sentence of {words} words (split it)", sentence)
    clauses = CLAUSE_MARK.findall(sentence)
    appositives = APPOSITIVE.findall(sentence)
    if len(clauses) + len(appositives) >= STACKED_CLAUSES:
        found = ", ".join([*clauses, *(f"{article} ..." for article in appositives)])
        report.warn(start, f"stacked asides ({found}): one per sentence at most", sentence)
    body = sentence.rstrip(":")
    if ";" in body:
        report.warn(start, "semicolon in prose (one idea per sentence)", sentence)
    if re.search(r"\w: [a-z]", body):
        report.warn(start, "colon mid-sentence (a reveal, or two ideas?)", sentence)
    return len(clauses) + len(appositives)


def check_sentences(block: Block, report: Report) -> list[int]:
    """Warn about the sentence- and paragraph-level patterns in one prose block.

    Returns:
        The length of each of the block's sentences, in words.
    """
    text = plain(re.sub(r"^([-*+]|\d+\.)\s+", "", block.text))
    found = sentences(text)
    asides = sum(check_sentence(block.start, sentence, report) for sentence in found)
    asides += sum(
        1
        for aside in PARENTHETICAL.findall(text)
        if len(aside.split()) >= MIN_ASIDE_WORDS and not aside.startswith("CODE")
    )
    if asides >= ASIDES_PER_PARAGRAPH:
        report.warn(
            block.start, f"{asides} asides in one paragraph: does the reader need each?", text
        )
    for pattern, rule in (
        (CONTRAST, "contrast: is the misconception real?"),
        (SO_CHAIN, "so-chain"),
        (QUESTION_ANSWERED, "question answered at once?"),
    ):
        match = pattern.search(text)
        if match:
            report.warn(block.start, f"{rule} ({match.group(0)!r})", block.text)
    if block.kind == "paragraph" and len(found) > 1 and CONDITION_OPENER.match(text):
        report.warn(block.start, "paragraph opens on a condition: point first?", text)
    if block.kind == "paragraph" and len(found) >= LONG_PARAGRAPH_SENTENCES:
        report.warn(block.start, f"paragraph of {len(found)} sentences: one topic?", text)
    return [len(sentence.split()) for sentence in found]


def check_code(block: Block, report: Report) -> None:
    """Check a code block for hard-coded versions and, in shell, variables that never change."""
    shell = block.language in SHELL_LANGUAGES
    for offset, line in enumerate(block.text.splitlines(), start=1):
        if VERSION.search(line) and not EXAMPLE_MARK.search(line):
            report.warn(block.start + offset, "hard-coded version in code", line)
        match = SHELL_ASSIGNMENT.match(line) if shell else None
        if match and "$(" not in match.group(2) and not match.group(1).startswith("MASSIVE"):
            report.warn(
                block.start + offset,
                f"shell variable {match.group(1)} never changes: write the value in place",
                line,
            )


def check_alternatives(blocks: list[Block], report: Report) -> None:
    """Warn about an alternative offered between two code blocks, after its first option."""
    for index in range(1, len(blocks) - 1):
        block = blocks[index]
        if block.kind != "paragraph" or blocks[index + 1].kind != "code":
            continue
        before = next((b for b in reversed(blocks[:index]) if b.kind != "heading"), None)
        if (
            before is not None
            and before.kind == "code"
            and LATE_ALTERNATIVE.search(plain(block.text))
        ):
            report.warn(
                block.start,
                "alternative given after its option: state 'A or B:' up front",
                block.text,
            )


def check_lists(blocks: list[Block], report: Report) -> None:
    """Warn about a run of list items that open with bold labels, and bold run-in paragraphs."""
    run: list[Block] = []
    for block in [*blocks, Block("end", 0, "")]:
        if block.kind == "item" and re.match(r"^([-*+]|\d+\.)\s+\*\*[^*]+\*\*", block.text):
            run.append(block)
            continue
        if len(run) >= MIN_BOLD_LABEL_ITEMS:
            report.warn(
                run[0].start, "list items that open with bold labels: running prose?", run[0].text
            )
        run = []
        if block.kind == "paragraph" and re.match(r"^\*\*[^*]+\*\*", block.text):
            report.warn(block.start, "bold run-in paragraph", block.text)


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


def check_hook(blocks: list[Block], report: Report) -> None:
    """Warn when a page's opening runs past the hook's budget before its first code or section."""
    if not blocks or not blocks[0].text.startswith("# "):
        return
    words = 0
    for block in blocks[1:]:
        if block.kind == "code" or block.text.startswith("## "):
            break
        if block.kind in {"paragraph", "item"}:
            words += len(plain(block.text).split())
    if words > HOOK_WORD_BUDGET:
        report.warn(
            blocks[0].start,
            f"opening of {words} words before its first code or section (aim for 80 to 90)",
            blocks[0].text,
        )


def check_repeated_phrases(blocks: list[Block], report: Report) -> None:
    """Warn about four-word phrases the page repeats three or more times."""
    seen: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for block in blocks:
        if block.kind not in {"paragraph", "item"}:
            continue
        for sentence in sentences(plain(block.text)):
            words = re.findall(r"[a-z][a-z']*", sentence.lower())
            for start in range(len(words) - REPEATED_PHRASE_WORDS + 1):
                phrase = tuple(words[start : start + REPEATED_PHRASE_WORDS])
                content = [word for word in phrase if word not in STOP_WORDS]
                if len(content) >= MIN_CONTENT_WORDS_IN_PHRASE:
                    seen[phrase].append(block.start)
    repeated = sorted(
        ((phrase, lines) for phrase, lines in seen.items() if len(lines) >= REPEATED_PHRASE_COUNT),
        key=lambda item: -len(item[1]),
    )
    for phrase, lines in repeated[:MAX_REPEATED_PHRASES]:
        where = ", ".join(str(line) for line in sorted(set(lines)))
        report.warn(
            lines[0], f"phrase repeated {len(lines)} times (lines {where})", " ".join(phrase)
        )


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
        elif block.kind == "code":
            check_code(block, report)
        else:
            check_words(block, report)
            if block.kind != "table":
                lengths += check_sentences(block, report)
        previous = block
    check_hook(blocks, report)
    check_alternatives(blocks, report)
    check_lists(blocks, report)
    check_paragraph_runs(blocks, report)
    check_repeated_phrases(blocks, report)
    if lengths and sum(lengths) / len(lengths) > MEAN_SENTENCE_WORDS:
        mean = sum(lengths) / len(lengths)
        report.warn(0, f"mean sentence length {mean:.1f} words", "aim for 15 to 20")
    if len(lengths) >= MIN_SENTENCES_FOR_RHYTHM:
        deviation = statistics.pstdev(lengths)
        if deviation < UNIFORM_RHYTHM_DEVIATION:
            report.warn(0, f"sentence lengths vary by only {deviation:.1f} words", "vary them")
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
