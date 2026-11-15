"""Check the record folders: the owner's decisions and the verification items, one file each.

`docs/dev/decisions/` and `docs/dev/verification-items/` hold one Markdown file per entry, named
`NNNN-<slug>.md`, in the format each folder's `README.md` gives. Agents list a folder by its
`Status:` lines and choose what to read by the file names, so a record must be findable and
consistent. The checks here:

- every file other than the README is named `NNNN-<slug>.md`, and no number is used twice;
- the first line is `# N. <title>`, with the file name's number;
- the file has one `Status:` line in its folder's forms;
- every decision a decision's `Status:` line links to exists, under the number the link shows;
- the slug has at least three words besides the generic ones (`update`, `misc`, `change`,
  `fix`, `decision`, `item`), so the name says when to open the file;
- a decision is a record or a pointer. A record has the README's six sections in order, a body
  of at most 300 words, and a title of plain words under 90 characters; a pointer
  (`absorbed by`, `moved to`) has only its heading, `Date:`, and `Status:` lines, and points at
  a record or at a page that holds the named section or rule;
- every `decision N` in `AGENTS.md`, the skills, and `docs/` names a decision file;
- a verification item dated after the records were rewritten has a title that asks its question.

Two pull requests that take the same number pass here on their own branches; the first run on
`main` after both merge fails, and a follow-up pull request renumbers one of them.
"""

import re
from collections import Counter
from pathlib import Path
from typing import Final

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_DEV = _ROOT / "docs" / "dev"
DECISIONS: Final = _DEV / "decisions"
VERIFICATION_ITEMS: Final = _DEV / "verification-items"

GENERIC_WORDS: Final = frozenset({"update", "misc", "change", "fix", "decision", "item"})
"""Words that say nothing about when to open a record, so they don't count toward a slug's three."""

_NAME: Final = re.compile(r"(\d{4})-([a-z0-9]+(?:-[a-z0-9]+)*)\.md")
_HEADING: Final = re.compile(r"# (\d+)\. \S")
_LINK: Final = re.compile(r"\[(\d+)\]\(([^)]+)\)")

DECISION_STATUS: Final = re.compile(
    r"accepted|superseded by .+|partly superseded: .+ by .+|absorbed by \[\d+\]\(.+\)|moved to .+"
)
ITEM_STATUS: Final = re.compile(
    r"open|settled \d{4}-\d{2}-\d{2}: .+|retired( \d{4}-\d{2}-\d{2}: .+)?"
)

SECTIONS: Final = ("Question", "Options", "Ruling", "Reason", "Consequences", "Rule")
"""The sections of a decision record, in order (`docs/dev/decisions/README.md`)."""
RECORD_WORDS: Final = 300
TITLE_LENGTH: Final = 90
EARLIER_FORM: Final = frozenset({58, 66, 81, 90, 93, 99, 103, 104, 107})
"""Decisions the symbol-rules pull request changes, kept in their earlier form until it lands."""
RECORD_FLOOR: Final = 30
FORM_DATE: Final = "2026-11-15"
"""The day the records took their present form; a verification item after it asks a question."""

_POINTER: Final = re.compile(r"Status: (absorbed by|moved to) (.+)")
_MOVED: Final = re.compile(r"`([^`]+)`(?:, \"([^\"]+)\")?")
_CITATION: Final = re.compile(r"\bdecisions? (\d+)\b")
_DATE_LINE: Final = re.compile(r"^Date: (\d{4}-\d{2}-\d{2})$", re.MULTILINE)


def name_problems(names: list[str]) -> list[str]:
    """Return what is wrong with a record folder's file names.

    Args:
        names: The names of every file in the folder.

    Returns:
        One line per name that is not `README.md` or `NNNN-<slug>.md`, per slug with fewer than
        three words besides `GENERIC_WORDS`, and per number used more than once.
    """
    problems = []
    numbers: Counter[int] = Counter()
    for name in names:
        if name == "README.md":
            continue
        match = _NAME.fullmatch(name)
        if match is None:
            problems.append(f"{name}: not named NNNN-<slug>.md")
            continue
        numbers[int(match.group(1))] += 1
        words = [word for word in match.group(2).split("-") if word not in GENERIC_WORDS]
        if len(words) < 3:
            problems.append(f"{name}: the slug needs three words that say when to open the file")
    problems += [
        f"number {n} is used by {count} files" for n, count in numbers.items() if count > 1
    ]
    return problems


def record_problems(name: str, text: str, status: re.Pattern[str]) -> list[str]:
    """Return what is wrong with one record's text.

    Args:
        name: The file name, `NNNN-<slug>.md`.
        text: The file's text.
        status: The forms the folder allows after `Status: `.

    Returns:
        One line each for a first line that is not `# N. <title>` with the file's number, and
        for a missing, repeated, or unknown `Status:` line.
    """
    problems = []
    number = int(name.split("-", 1)[0])
    heading = _HEADING.match(text.split("\n", 1)[0])
    if heading is None:
        problems.append(f"{name}: the first line is not '# {number}. <title>'")
    elif int(heading.group(1)) != number:
        problems.append(f"{name}: the heading's number is {heading.group(1)}, not {number}")
    statuses = [line[len("Status: ") :] for line in text.splitlines() if line.startswith("Status:")]
    if len(statuses) != 1:
        problems.append(f"{name}: {len(statuses)} Status: lines, not 1")
    elif not status.fullmatch(statuses[0]):
        problems.append(f"{name}: unknown status {statuses[0]!r}")
    return problems


def link_problems(name: str, text: str, names: set[str]) -> list[str]:
    """Return the decisions a decision's `Status:` line links to that don't exist.

    Args:
        name: The file name of the decision.
        text: The file's text.
        names: The names of every file in the decisions folder.

    Returns:
        One line per link whose file is not in `names` or whose number is not the link's text.
    """
    status = next((line for line in text.splitlines() if line.startswith("Status:")), "")
    return [
        f"{name}: the status links to {target}, which is not decision {number}"
        for number, target in _LINK.findall(status)
        if target not in names or int(target.split("-", 1)[0]) != int(number)
    ]


def decision_problems(name: str, text: str, names: set[str], *, root: Path) -> list[str]:
    """Return what is wrong with a decision's form: a record's sections, or a pointer's target.

    Args:
        name: The file name, `NNNN-<slug>.md`.
        text: The file's text.
        names: The names of every file in the decisions folder.
        root: The repository root, which a `moved to` path is relative to.

    Returns:
        For a pointer: a line for any text besides its heading, `Date:`, and `Status:`, for an
        `absorbed by` target that is itself a pointer, and for a `moved to` page that does not
        exist or does not hold the quoted section or rule. For a record: a line for sections
        other than `SECTIONS` in order, a body over `RECORD_WORDS` words, and a title over
        `TITLE_LENGTH` characters or holding code. Nothing for an `EARLIER_FORM` decision.
    """
    if int(name.split("-", 1)[0]) in EARLIER_FORM:
        return []
    lines = [line for line in text.splitlines() if line.strip()]
    pointer = next((_POINTER.fullmatch(line) for line in lines if _POINTER.fullmatch(line)), None)
    if pointer is not None:
        problems = (
            []
            if len(lines) == 3
            else [f"{name}: a pointer holds only its heading, date, and status"]
        )
        if pointer.group(1) == "absorbed by":
            target = _LINK.search(pointer.group(2))
            if target and target.group(2) in names:
                target_text = (DECISIONS / target.group(2)).read_text(encoding="utf-8")
                if _POINTER.search(target_text):
                    problems.append(f"{name}: absorbed by {target.group(2)}, which is a pointer")
        else:
            problems += moved_problems(name, pointer.group(2), root)
        return problems
    problems = []
    headings = tuple(line[3:] for line in lines if line.startswith("## "))
    if headings != SECTIONS:
        problems.append(f"{name}: the sections are {headings}, not {SECTIONS}")
    body = text.split("\nStatus:", 1)[-1].split("\n", 1)[-1]
    words = len([word for word in body.split() if word != "##" and word not in SECTIONS])
    if words > RECORD_WORDS:
        problems.append(f"{name}: the body is {words} words, over {RECORD_WORDS}")
    title = lines[0]
    if len(title) > TITLE_LENGTH or "`" in title:
        problems.append(f"{name}: the title is over {TITLE_LENGTH} characters or holds code")
    return problems


def moved_problems(name: str, target: str, root: Path) -> list[str]:
    """Return what is wrong with a `moved to` target.

    Args:
        name: The pointer's file name.
        target: The text after `moved to`: a path in backticks, then optionally a comma and the
            section heading or rule name in double quotes.
        root: The repository root the path is relative to.

    Returns:
        A line when the target is not in that form, the path is not a file, or the file does not
        hold the quoted text.
    """
    match = _MOVED.match(target)
    if match is None:
        return [f'{name}: moved to {target!r}, not a `path` and an optional "section"']
    page = root / match.group(1)
    if not page.is_file():
        return [f"{name}: moved to {match.group(1)}, which is not a file"]
    if match.group(2) and match.group(2) not in page.read_text(encoding="utf-8"):
        return [f"{name}: {match.group(1)} holds no {match.group(2)!r}"]
    return []


def citation_problems(root: Path, names: set[str]) -> list[str]:
    """Return each `decision N` in the rules and docs that names no decision file.

    Args:
        root: The repository root.
        names: The names of every file in the decisions folder.

    Returns:
        One line per citation, in `AGENTS.md`, the skills, and `docs/`, of a number no file has.
    """
    numbers = {int(name[:4]) for name in names if _NAME.fullmatch(name)}
    pages = [
        root / "AGENTS.md",
        *root.glob(".claude/skills/*/SKILL.md"),
        *root.glob("docs/**/*.md"),
    ]
    return [
        f"{page.relative_to(root)}: decision {number} has no file"
        for page in pages
        for number in _CITATION.findall(page.read_text(encoding="utf-8"))
        if int(number) not in numbers
    ]


def item_problems(name: str, text: str) -> list[str]:
    """Return what is wrong with a verification item written after `FORM_DATE`.

    Args:
        name: The file name.
        text: The file's text.

    Returns:
        A line when an item dated after `FORM_DATE` has a title that does not end in `?`.
    """
    date = _DATE_LINE.search(text)
    title = text.split("\n", 1)[0]
    if date and date.group(1) > FORM_DATE and not title.endswith("?"):
        return [f"{name}: the title of an item after {FORM_DATE} is its question, ending in '?'"]
    return []


def folder_problems(folder: Path, status: re.Pattern[str]) -> list[str]:
    """Return every problem of a record folder.

    Args:
        folder: The folder of records.
        status: The forms the folder allows after `Status: `.

    Returns:
        The problems `name_problems`, `record_problems`, and `link_problems` find.
    """
    names = sorted(path.name for path in folder.iterdir())
    problems = name_problems(names)
    for name in names:
        if _NAME.fullmatch(name):
            text = (folder / name).read_text(encoding="utf-8")
            problems += record_problems(name, text, status)
            problems += link_problems(name, text, set(names))
            if folder == DECISIONS:
                problems += decision_problems(name, text, set(names), root=_ROOT)
            else:
                problems += item_problems(name, text)
    return problems


@pytest.mark.parametrize(
    ("folder", "status"),
    [(DECISIONS, DECISION_STATUS), (VERIFICATION_ITEMS, ITEM_STATUS)],
    ids=["decisions", "verification-items"],
)
def test_every_record_is_findable_and_consistent(folder: Path, status: re.Pattern[str]) -> None:
    assert (folder / "README.md").is_file()
    assert folder_problems(folder, status) == []


def test_the_folders_hold_records() -> None:
    assert len(list(DECISIONS.glob("0*.md"))) > 100
    assert len(list(VERIFICATION_ITEMS.glob("0*.md"))) > 100
    records = [p for p in DECISIONS.glob("0*.md") if not _POINTER.search(p.read_text())]
    assert len(records) >= RECORD_FLOOR


def test_every_cited_decision_has_a_file() -> None:
    names = {path.name for path in DECISIONS.iterdir()}
    assert citation_problems(_ROOT, names) == []


def test_a_well_named_folder_has_no_problems() -> None:
    names = ["README.md", "0001-no-sdk-own-clients.md", "0002-schemas-generated-from-spec.md"]
    assert name_problems(names) == []


@pytest.mark.parametrize(
    "name",
    ["0001.md", "1-no-sdk-own-clients.md", "0001-No-Sdk-Clients.md", "0001-no_sdk_clients.md"],
)
def test_a_name_not_in_the_form_is_reported(name: str) -> None:
    assert name_problems([name]) == [f"{name}: not named NNNN-<slug>.md"]


@pytest.mark.parametrize(
    "name", ["0001-limit-rows.md", "0001-misc-update-fix.md", "0001-decision-change-limit-rows.md"]
)
def test_a_slug_without_three_telling_words_is_reported(name: str) -> None:
    assert name_problems([name]) == [
        f"{name}: the slug needs three words that say when to open the file"
    ]


def test_a_slug_with_a_generic_word_among_three_others_passes() -> None:
    assert name_problems(["0084-todays-change-snapshots-option-chain.md"]) == []


def test_a_number_used_twice_is_reported() -> None:
    names = ["0104-ticker-sent-as-written.md", "0104-futures-refused-venues-setting.md"]
    assert name_problems(names) == ["number 104 is used by 2 files"]


_GOOD = "# 7. Configuration through one object\n\nDate: 2026-10-23\n\nStatus: accepted\n\nBody.\n"


def test_a_good_record_has_no_problems() -> None:
    assert record_problems("0007-configuration-one-object.md", _GOOD, DECISION_STATUS) == []


def test_a_heading_with_another_number_is_reported() -> None:
    problems = record_problems("0008-configuration-one-object.md", _GOOD, DECISION_STATUS)
    assert problems == ["0008-configuration-one-object.md: the heading's number is 7, not 8"]


def test_a_record_without_a_numbered_heading_is_reported() -> None:
    text = _GOOD.replace("# 7. ", "# ")
    problems = record_problems("0007-configuration-one-object.md", text, DECISION_STATUS)
    assert problems == ["0007-configuration-one-object.md: the first line is not '# 7. <title>'"]


@pytest.mark.parametrize(("text", "count"), [(_GOOD.replace("Status", "State"), 0), (_GOOD * 2, 2)])
def test_a_missing_or_repeated_status_is_reported(text: str, count: int) -> None:
    problems = record_problems("0007-configuration-one-object.md", text, DECISION_STATUS)
    assert problems == [f"0007-configuration-one-object.md: {count} Status: lines, not 1"]


@pytest.mark.parametrize(
    ("status", "allowed"),
    [
        ("superseded by [9](0009-x.md)", DECISION_STATUS),
        ("partly superseded: its window by [9](0009-x.md)", DECISION_STATUS),
        ("open", ITEM_STATUS),
        ("settled 2026-11-02: every bar", ITEM_STATUS),
        ("retired", ITEM_STATUS),
    ],
)
def test_each_status_form_is_allowed(status: str, allowed: re.Pattern[str]) -> None:
    text = _GOOD.replace("Status: accepted", f"Status: {status}")
    assert record_problems("0007-configuration-one-object.md", text, allowed) == []


@pytest.mark.parametrize(
    ("status", "allowed"),
    [("open", DECISION_STATUS), ("accepted", ITEM_STATUS), ("settled 2026-11-02", ITEM_STATUS)],
)
def test_a_status_of_another_form_is_reported(status: str, allowed: re.Pattern[str]) -> None:
    text = _GOOD.replace("Status: accepted", f"Status: {status}")
    problems = record_problems("0007-configuration-one-object.md", text, allowed)
    assert problems == [f"0007-configuration-one-object.md: unknown status {status!r}"]


def test_a_link_to_an_existing_decision_passes() -> None:
    text = _GOOD.replace("accepted", "superseded by [9](0009-limit-total-rows.md)")
    names = {"0007-configuration-one-object.md", "0009-limit-total-rows.md"}
    assert link_problems("0007-configuration-one-object.md", text, names) == []


@pytest.mark.parametrize(
    ("number", "target"), [("9", "0009-limit-total-rows.md"), ("8", "0009-limit-total-rows-old.md")]
)
def test_a_link_to_a_missing_or_misnumbered_decision_is_reported(number: str, target: str) -> None:
    text = _GOOD.replace("accepted", f"partly superseded: its window by [{number}]({target})")
    names = {"0007-configuration-one-object.md", "0009-limit-total-rows-old.md"}
    problems = link_problems("0007-configuration-one-object.md", text, names)
    reason = f"the status links to {target}, which is not decision {number}"
    assert problems == [f"0007-configuration-one-object.md: {reason}"]


_RECORD = (
    "# 7. Configuration goes through one object\n\nDate: 2026-10-23\n\nStatus: accepted\n\n"
    + "".join(f"## {section}\n\nText.\n\n" for section in SECTIONS)
)
_NAMES = {"0007-configuration-one-object.md", "0083-never-mix-history-and-live-no-backfill.md"}


def test_a_record_in_the_form_has_no_problems() -> None:
    assert decision_problems("0007-configuration-one-object.md", _RECORD, _NAMES, root=_ROOT) == []


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        (_RECORD.replace("## Reason", "## Why"), "the sections are"),
        (_RECORD.replace("Text.", "word " * 60), "words, over 300"),
        (_RECORD.replace("one object", "`MassiveConfig`"), "holds code"),
    ],
)
def test_a_record_out_of_form_is_reported(text: str, problem: str) -> None:
    problems = decision_problems("0007-configuration-one-object.md", text, _NAMES, root=_ROOT)
    assert len(problems) == 1
    assert problem in problems[0]


def test_a_decision_in_its_earlier_form_is_not_checked() -> None:
    assert decision_problems("0066-bad-symbol-onerror.md", _GOOD, _NAMES, root=_ROOT) == []


_POINTER_HEAD = "# 7. Configuration goes through one object\n\nDate: 2026-10-23\n\n"


@pytest.mark.parametrize(
    "status",
    [
        "absorbed by [83](0083-never-mix-history-and-live-no-backfill.md)",
        'moved to `AGENTS.md`, "Layers"',
        "moved to `docs/dev/public-api.md`",
    ],
)
def test_a_pointer_to_a_record_or_a_page_passes(status: str) -> None:
    text = f"{_POINTER_HEAD}Status: {status}\n"
    assert decision_problems("0007-configuration-one-object.md", text, _NAMES, root=_ROOT) == []


@pytest.mark.parametrize(
    ("status", "problem"),
    [
        (
            "absorbed by [3](0003-no-seeding-last-by-from-rest-merge-history-with-stream.md)",
            "which is a pointer",
        ),
        ('moved to `AGENTS.md`, "No such rule anywhere"', "holds no"),
        ("moved to `docs/dev/no-such-page.md`", "not a file"),
        ("moved to AGENTS.md", "not a `path`"),
    ],
)
def test_a_pointer_to_nothing_is_reported(status: str, problem: str) -> None:
    names = _NAMES | {"0003-no-seeding-last-by-from-rest-merge-history-with-stream.md"}
    text = f"{_POINTER_HEAD}Status: {status}\n"
    problems = decision_problems("0007-configuration-one-object.md", text, names, root=_ROOT)
    assert len(problems) == 1
    assert problem in problems[0]


def test_a_pointer_with_a_body_is_reported() -> None:
    text = f'{_POINTER_HEAD}Status: moved to `AGENTS.md`, "Layers"\n\nThe old text.\n'
    problems = decision_problems("0007-configuration-one-object.md", text, _NAMES, root=_ROOT)
    assert problems == [
        "0007-configuration-one-object.md: a pointer holds only its heading, date, and status"
    ]


def test_a_citation_of_a_missing_decision_is_reported(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_text("A rule (decision 7) and another (decision 9999).\n")
    assert citation_problems(tmp_path, _NAMES) == ["AGENTS.md: decision 9999 has no file"]


@pytest.mark.parametrize(
    ("title", "date", "count"),
    [
        ("# 130. Is the field sent?", "2026-11-20", 0),
        ("# 130. The field", "2026-11-20", 1),
        ("# 13. The field", "2026-10-23", 0),
    ],
)
def test_a_new_item_s_title_asks_its_question(title: str, date: str, count: int) -> None:
    text = f"{title}\n\nDate: {date}\n\nStatus: open\n\nBody.\n"
    assert len(item_problems("0130-field-sent-on-wire.md", text)) == count
