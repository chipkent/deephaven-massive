"""The pull request description check: what it fails on and what it lets through.

The script is run by path, so the tests load it from `scripts/` and call `find_problems`
with a body, a changed-file list, and a diff, as the hook and the workflow do.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

_SCRIPT = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "check_pr_body.py"


@pytest.fixture(scope="module")
def check() -> ModuleType:
    """The script as a module.

    Returns:
        `scripts/check_pr_body.py`, loaded by path as the hook runs it.
    """
    spec = importlib.util.spec_from_file_location("check_pr_body", _SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CLEAN = """## Summary

A crypto pair Massive's list does not hold gives no rows.

**Check before merging:**

- **A public change the owner requested.** The owner, 2026-11-15: "I agree on not erroring on
  missing symbols. That should be a significant simplification."
- A new dependency floor: `typing_extensions>=4.4.0`.

## Why

The owner asked "why does it raise?" on 2026-11-11; see https://example.com/why?x=1.
"""

#: The owner's answer with the question it answers and its date: what approves a rule change.
ANSWERED = (
    '## Summary\n\nAsked "May the rules say this instead?", the owner answered on 2026-11-15: '
    '"I approve the principles, as written."\n'
)

AGENTS_DIFF = """diff --git a/AGENTS.md b/AGENTS.md
--- a/AGENTS.md
+++ b/AGENTS.md
@@ -1,2 +1,2 @@
-- **Old rule.** Text.
+- **New rule.** Text.
"""

AGENTS_ADDITION = """diff --git a/AGENTS.md b/AGENTS.md
--- a/AGENTS.md
+++ b/AGENTS.md
@@ -1,1 +1,2 @@
 - **Old rule.** Text.
+- **Another rule.** Text.
"""


def test_a_clean_description_passes(check: ModuleType) -> None:
    assert check.find_problems(CLEAN, ["src/x.py"], "") == []


@pytest.mark.parametrize(
    "item",
    [
        "- The rest are choices for the owner to confirm: the setting rename.",
        "- **Kept, for you to confirm:** the overloads.",
        "- open: the owner's answer on TLS retries.",
        "- Should S3 retry a handshake failure as REST does?",
        "- The TLS change is not yet approved.",
    ],
)
def test_an_item_that_asks_the_owner_fails(check: ModuleType, item: str) -> None:
    body = f"## Summary\n\n**Check before merging:**\n\n{item}\n\n## Why\n\nText.\n"
    problems = check.find_problems(body, [], "")
    assert len(problems) == 1
    assert "asks the owner something" in problems[0]


def test_a_phrase_wrapped_across_lines_still_asks(check: ModuleType) -> None:
    body = (
        "**Check before merging:**\n\n- The setting is a choice for the owner to\n"
        "  confirm before merging.\n"
    )
    problems = check.find_problems(body, [], "")
    assert len(problems) == 1
    assert "for the owner to confirm" in problems[0]


def test_a_question_mark_in_a_quote_code_span_or_link_asks_nothing(check: ModuleType) -> None:
    body = (
        "**Check before merging:**\n\n"
        '- The owner, 2026-11-14: "Shouldn\'t it accept validly specified symbols?" Yes.\n'
        "  > Q: Should the converters raise by default? A: yes, as the rest of the API does.\n"
        "- The call `m.stocks.trades(symbols=?)` is refused.\n"
        "- See https://github.com/chipkent/deephaven-massive/pull/613?diff=split.\n"
    )
    assert check.find_problems(body, [], "") == []


def test_a_question_outside_the_section_is_not_the_section_s(check: ModuleType) -> None:
    body = "## Why\n\nShould it? The rule says Check before merging holds notices.\n"
    assert check.find_problems(body, [], "") == []


@pytest.mark.parametrize(
    "claim",
    [
        "| The two `noqa` comments are owner-approved | Both classes are facades. |",
        "The owner decided on 2026-11-09 to ship it before 0.1.0.",
        '- The owner approved this form on 2026-11-12: "ok".',
        '- The owner approved the split on 2026-11-11: "I\'m certainly fine with splitting."',
        '- The owner decided on 2026-11-11, asked "May the split go ahead as proposed?"',
        '- The owner approved, asked "May the split go ahead as proposed?": "Yes, go ahead."',
        '- The owner approved "Yes." on 2026-11-15, the answer to "May the split go ahead?"',
    ],
)
def test_an_approval_claimed_without_its_source_fails(check: ModuleType, claim: str) -> None:
    problems = check.find_problems(f"## Decisions\n\n{claim}\n", [], "")
    assert len(problems) == 1
    assert "An approval is claimed" in problems[0]


def test_an_approval_with_its_question_and_the_owner_s_dated_answer_passes(
    check: ModuleType,
) -> None:
    body = (
        "## Decisions\n\n"
        '- The owner approved the split on 2026-11-11. Asked "May the split go ahead?": '
        '"I\'m certainly fine with splitting."\n'
        "- The owner decided on 2026-11-12:\n"
        "  > May tests use noqa for fake credentials?\n"
        "  > Yes, tests can use noqa for fake credentials\n"
        '- The owner approved on 2026-11-12, asked "Should this ship before 0.1.0?": "ok"\n'
        '- The owner agreed on 2026-11-13, asked "Is the guard still needed?": "Yes, why not?"\n'
    )
    assert check.find_problems(body, [], "") == []


_DECISION_96 = "docs/dev/decisions/0096-options-admin-messages.md"
ADDED_DECISION = (
    f"diff --git a/{_DECISION_96} b/{_DECISION_96}\n"
    "new file mode 100644\n"
    "--- /dev/null\n"
    f"+++ b/{_DECISION_96}\n"
    "@@ -0,0 +1,1 @@\n"
    "+# 96. Options admin messages\n"
)


@pytest.fixture
def decisions(tmp_path: pathlib.Path) -> pathlib.Path:
    """A decisions folder holding decisions 96 and 104.

    Returns:
        The folder, with one `NNNN-<slug>.md` file per decision.
    """
    folder = tmp_path / "decisions"
    folder.mkdir()
    (folder / "0096-options-admin-messages.md").write_text("# 96. Options admin messages\n")
    (folder / "0104-canonical-ticker-refused.md").write_text("# 104. Canonical ticker\n")
    return folder


def test_an_approval_recorded_in_an_existing_decision_passes(
    check: ModuleType, decisions: pathlib.Path
) -> None:
    body = "## Decisions\n\n- The owner decided that converters raise by default (decision 104).\n"
    assert (
        check.find_problems(body, ["src/x.py"], "", decisions=check.decision_files(decisions)) == []
    )


def test_a_decision_this_pull_request_adds_records_no_approval(
    check: ModuleType, decisions: pathlib.Path
) -> None:
    body = "## Summary\n\nThe owner decided to ship it before 0.1.0; decision 96 records it.\n"
    changed = ["docs/dev/decisions/0096-options-admin-messages.md", "src/x.py"]
    problems = check.find_problems(
        body, changed, ADDED_DECISION, decisions=check.decision_files(decisions)
    )
    assert len(problems) == 1


def test_a_decision_no_file_carries_records_no_approval(
    check: ModuleType, decisions: pathlib.Path
) -> None:
    body = "- The owner approved removing the guard (decision 9999).\n"
    assert len(check.find_problems(body, [], "", decisions=check.decision_files(decisions))) == 1


def test_a_decision_whose_status_this_pull_request_updates_still_counts(
    check: ModuleType, decisions: pathlib.Path
) -> None:
    body = "- The owner decided it (decision 104).\n"
    diff = ADDED_DECISION.replace("0096-options-admin-messages", "0104-canonical-ticker-refused")
    diff = diff.replace("new file mode 100644\n", "")
    changed = ["docs/dev/decisions/0104-canonical-ticker-refused.md"]
    assert check.find_problems(body, changed, diff, decisions=check.decision_files(decisions)) == []


def test_indented_sibling_bullets_never_share_a_quotation(check: ModuleType) -> None:
    body = (
        '  - The owner approved the split on 2026-11-15, asked "Is the split right?": "Yes."\n'
        "  - The owner approved removing the guard.\n"
    )
    assert len(check.units(body)) == 2
    assert len(check.find_problems(body, [], "")) == 1


def test_a_claim_wrapped_across_lines_is_still_a_claim(check: ModuleType) -> None:
    body = "- The owner\n  approved removing the guard.\n"
    assert len(check.find_problems(body, [], "")) == 1


def test_prose_between_two_quotations_is_not_a_question(check: ModuleType) -> None:
    body = (
        '- The owner approved on 2026-11-15: "ok"; was the setting renamed? it is "retry_limit".\n'
        '- The owner approved on 2026-11-15: "ok", then later repeated "ok".\n'
    )
    assert len(check.find_problems(body, [], "")) == 2


def test_a_blank_line_before_an_indented_blockquote_keeps_the_item_whole(
    check: ModuleType,
) -> None:
    body = (
        "- The owner approved on 2026-11-15:\n\n  > Should this guard stay as it is?\n"
        "  > Keep this guard exactly as written.\n\n"
        "- A second item.\n"
    )
    assert check.find_problems(body, [], "") == []
    assert len(check.units(body)) == 2


def test_a_paraphrase_of_what_the_owner_wants_is_a_claim(check: ModuleType) -> None:
    body = "| Basic runs on the main key | The owner wants no second key. |\n"
    assert len(check.find_problems(body, [], "")) == 1


def test_a_hypothetical_inside_a_quotation_is_not_a_claim(check: ModuleType) -> None:
    body = 'To answer "has the owner decided how limit works?", an agent reads the folder.\n'
    assert check.find_problems(body, [], "") == []


def test_a_claim_s_quote_in_another_paragraph_does_not_count(check: ModuleType) -> None:
    body = (
        '## Decisions\n\nAsked "May tests use noqa?", the owner, 2026-11-12: '
        '"Yes, for fake credentials."\n\n'
        "| Tests: a fake | The form the owner approved |\n"
    )
    assert len(check.find_problems(body, [], "")) == 1


@pytest.mark.parametrize(
    "body",
    [
        "## Summary\n\nRewords a skill.\n",
        '## Summary\n\nThe owner, 2026-11-15: "I approve the principles, as written."\n',
        '## Summary\n\nOn 2026-11-15 the owner was asked "May the rules say this instead?"\n',
    ],
)
def test_a_rule_change_needs_its_question_and_the_owner_s_dated_answer(
    check: ModuleType, body: str
) -> None:
    problems = check.find_problems(body, [".claude/skills/x/SKILL.md"], "")
    assert len(problems) == 1
    assert "without the question and the owner's dated answer" in problems[0]
    assert check.find_problems(ANSWERED, [".claude/skills/x/SKILL.md"], "") == []


def test_an_agents_md_rewrite_needs_its_decisions_touched_line(check: ModuleType) -> None:
    body = ANSWERED
    problems = check.find_problems(body, ["AGENTS.md"], AGENTS_DIFF)
    assert len(problems) == 1
    assert "Decisions touched" in problems[0]
    with_line = body + "\n**Check before merging:**\n\n- Decisions touched: 58, 66.\n"
    assert check.find_problems(with_line, ["AGENTS.md"], AGENTS_DIFF) == []
    with_none = body + "\n- Decisions touched: none.\n"
    assert check.find_problems(with_none, ["AGENTS.md"], AGENTS_DIFF) == []


def test_an_agents_md_addition_needs_only_the_answer(check: ModuleType) -> None:
    body = ANSWERED
    assert check.find_problems(body, ["AGENTS.md"], AGENTS_ADDITION) == []


def test_a_skill_rewrite_needs_its_decisions_touched_line_too(check: ModuleType) -> None:
    diff = AGENTS_DIFF.replace("AGENTS.md", ".claude/skills/pre-push-review/SKILL.md")
    body = ANSWERED
    problems = check.find_problems(body, [".claude/skills/pre-push-review/SKILL.md"], diff)
    assert len(problems) == 1
    assert "Decisions touched" in problems[0]


def test_a_git_quoted_path_in_a_diff_header_is_read(check: ModuleType) -> None:
    diff = (
        'diff --git "a/docs/caf\\303\\251.md" "b/docs/caf\\303\\251.md"\n'
        "new file mode 100644\n--- /dev/null\n+++ b/docs/x\n@@ -0,0 +1 @@\n+x\n"
    )
    assert check.added_paths(diff) == {"docs/caf\\303\\251.md"}
    assert check.find_problems("## Summary\n\nClean.\n", ["docs/x"], diff) == []


def test_a_deletion_in_another_file_is_not_a_rule_rewrite(check: ModuleType) -> None:
    diff = AGENTS_DIFF.replace("AGENTS.md", "README.md")
    body = ANSWERED
    assert check.find_problems(body, ["AGENTS.md", "README.md"], diff) == []


def test_units_keep_a_bullet_with_its_continuation(check: ModuleType) -> None:
    body = (
        "- first bullet\n  continued\n  - sub-bullet\n- second bullet\n\n"
        "A paragraph\nof two lines.\n| row |\n"
    )
    assert check.units(body) == [
        "- first bullet\n  continued\n  - sub-bullet",
        "- second bullet",
        "A paragraph\nof two lines.",
        "| row |",
    ]


def test_main_reads_a_body_file_and_reports_the_exit_status(
    *,
    check: ModuleType,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = tmp_path / "body.md"
    body.write_text("**Check before merging:**\n\n- choices for the owner to confirm\n")
    output = tmp_path / "out.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert check.main(["--body-file", str(body)]) == 1
    assert "asks the owner something" in capsys.readouterr().out
    assert "error_message<<" in output.read_text()
    body.write_text("## Summary\n\nClean.\n")
    assert check.main(["--body-file", str(body)]) == 0


def test_main_without_a_draft_in_the_worktree_passes_and_says_so(
    *,
    check: ModuleType,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert check.main([]) == 0
    assert "No .git-pr-body.md" in capsys.readouterr().out


def _fake_run(outputs: dict[str, str]) -> Callable[[list[str]], str]:
    def run(command: list[str]) -> str:
        key = " ".join(command)
        if key not in outputs:
            raise subprocess.CalledProcessError(1, command)
        return outputs[key]

    return run


_LISTING = "gh api repos/{owner}/{repo}/contents/docs/dev/decisions?ref=main --jq .[].name"


def test_main_reads_a_pull_request_with_gh(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    view = {"body": CLEAN, "files": [{"path": "AGENTS.md"}], "baseRefName": "main"}
    outputs = {
        "gh pr view 7 --json body,files,baseRefName": json.dumps(view),
        "gh pr diff 7": AGENTS_DIFF,
        _LISTING: "0104-canonical-ticker-refused.md\nREADME.md\n",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main(["--pr", "7"]) == 1
    assert "Decisions touched" in capsys.readouterr().out


def test_a_cited_decision_is_resolved_against_the_pull_request_s_base(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # The worktree holds decision 120; the reviewed PR's base does not.
    monkeypatch.chdir(tmp_path)
    folder = tmp_path / "docs" / "dev" / "decisions"
    folder.mkdir(parents=True)
    (folder / "0120-local-only.md").write_text("# 120. Local only\n")
    view = {"body": "- Owner-approved (decision 120).\n", "files": [], "baseRefName": "main"}
    outputs = {
        "gh pr view 7 --json body,files,baseRefName": json.dumps(view),
        "gh pr diff 7": "",
        _LISTING: "0104-canonical-ticker-refused.md\n",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main(["--pr", "7"]) == 1


def test_the_branch_is_measured_against_its_pull_request_s_base(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text(CLEAN)
    outputs = {
        "gh pr view --json baseRefName": json.dumps({"baseRefName": "fix/below"}),
        "git diff origin/fix/below...HEAD": "",
        "git diff --name-only origin/fix/below...HEAD": "src/x.py\n",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 0


def test_a_branch_without_a_pull_request_is_measured_against_main(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text("## Summary\n\nRewords a skill.\n")
    outputs = {
        "git diff origin/main...HEAD": "",
        "git diff --name-only origin/main...HEAD": ".claude/skills/x/SKILL.md\n",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 1


def test_a_question_wrapped_across_blockquote_lines_is_one_question(check: ModuleType) -> None:
    body = (
        "- The owner approved on 2026-11-15:\n"
        "  > May the rules keep refusing invalid\n"
        "  > inputs?\n"
        "  > Yes.\n"
    )
    assert check.find_problems(body, [], "") == []


def test_a_wrapped_question_s_second_line_is_not_its_answer(check: ModuleType) -> None:
    body = (
        "- The owner approved on 2026-11-15:\n"
        "  > May the rule permit a literal '?' in\n"
        "  > the documented examples?\n"
    )
    assert len(check.find_problems(body, [], "")) == 1
