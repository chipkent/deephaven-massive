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
  `fix`, `decision`, `item`), so the name says when to open the file.

Two pull requests that take the same number pass here on their own branches; the first run on
`main` after both merge fails, and a follow-up pull request renumbers one of them.
"""

import re
from collections import Counter
from pathlib import Path
from typing import Final

import pytest

_DEV = Path(__file__).resolve().parent.parent / "docs" / "dev"
DECISIONS: Final = _DEV / "decisions"
VERIFICATION_ITEMS: Final = _DEV / "verification-items"

GENERIC_WORDS: Final = frozenset({"update", "misc", "change", "fix", "decision", "item"})
"""Words that say nothing about when to open a record, so they don't count toward a slug's three."""

_NAME: Final = re.compile(r"(\d{4})-([a-z0-9]+(?:-[a-z0-9]+)*)\.md")
_HEADING: Final = re.compile(r"# (\d+)\. \S")
_LINK: Final = re.compile(r"\[(\d+)\]\(([^)]+)\)")

DECISION_STATUS: Final = re.compile(r"accepted|superseded by .+|partly superseded: .+ by .+")
ITEM_STATUS: Final = re.compile(r"open|settled \d{4}-\d{2}-\d{2}: .+|retired")


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
