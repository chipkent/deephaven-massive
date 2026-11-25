"""The pull request description check: what it fails on and what it lets through.

The script is run by path, so the tests load it from `scripts/` and call `find_problems`
with a body, a changed-file list, and a diff, as the hook and the workflow do.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import re
import subprocess
import textwrap
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / "scripts" / "check_pr_body.py"
#: The Conventional PR workflow, which builds the file list CI passes to the script.
_WORKFLOW = _ROOT / ".github" / "workflows" / "conventional-pr-check.yml"


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


#: The flags that keep the user's settings out of the diff the rules read.
_DIFF = (
    "git diff --no-color --no-ext-diff --no-textconv --no-relative --src-prefix=a/ --dst-prefix=b/"
)
#: The file list, NUL-separated and of the whole repository.
_NAMES = "git diff --name-only -z --no-relative"

_LISTING = "gh api repos/{owner}/{repo}/contents/docs/dev/decisions?ref=main --jq .[].name"


#: A skill path git quotes in a plain `--name-only` list, as it quotes every non-ASCII name.
_SKILL = ".claude/skills/café/SKILL.md"


@pytest.fixture
def pull_request_7(
    check: ModuleType, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, str]:
    """A clone whose `origin` holds `main` and pull request 7, as GitHub serves them.

    The pull request rewrites a rule of `AGENTS.md` and adds a skill. Its commit is reachable
    only from `refs/pull/7/head`, and the clone is made as a remote one is, holding only what
    `origin`'s branches reach, so it holds the commit only once the check fetches it. After
    the pull request branched, `main` gained `later.md`, which a two-dot diff would count as
    the pull request's and a three-dot one, as GitHub's and the workflow's, does not.

    Returns:
        The fields `gh pr view 7` gives for the base and the commits. The clone is the working
        directory.
    """
    # The user's own git settings (signing, hooks, a default branch) stay out of the test, and
    # so does the repository a git hook running these tests points at (`GIT_INDEX_FILE`).
    for name in [name for name in os.environ if name.startswith("GIT_")]:
        monkeypatch.delenv(name)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for role in ("AUTHOR", "COMMITTER"):
        monkeypatch.setenv(f"GIT_{role}_NAME", "Test")
        monkeypatch.setenv(f"GIT_{role}_EMAIL", "test@example.com")

    def _git(repo: pathlib.Path, *args: str) -> str:
        return str(check._run(["git", "-C", str(repo), *args])).strip()

    origin = tmp_path / "origin"
    _git(tmp_path, "init", "-q", "-b", "main", str(origin))
    (origin / "AGENTS.md").write_text("- **Old rule.** Text.\n")
    _git(origin, "add", "AGENTS.md")
    _git(origin, "commit", "-q", "-m", "rules")
    _git(origin, "checkout", "-q", "-b", "fix/rule")
    (origin / "AGENTS.md").write_text("- **New rule.** Text.\n")
    (origin / _SKILL).parent.mkdir(parents=True)
    (origin / _SKILL).write_text("A skill.\n")
    _git(origin, "add", "AGENTS.md", _SKILL)
    _git(origin, "commit", "-q", "-m", "new rule")
    head = _git(origin, "rev-parse", "HEAD")
    _git(origin, "update-ref", "refs/pull/7/head", head)
    _git(origin, "checkout", "-q", "main")
    _git(origin, "branch", "-q", "-D", "fix/rule")
    (origin / "later.md").write_text("Later.\n")
    _git(origin, "add", "later.md")
    _git(origin, "commit", "-q", "-m", "later")
    clone = tmp_path / "clone"
    # A local clone would copy every object, the pull request's commit too, so a check that
    # never fetched it would still pass.
    _git(tmp_path, "clone", "-q", "--no-local", str(origin), str(clone))
    with pytest.raises(subprocess.CalledProcessError):
        _git(clone, "cat-file", "-e", f"{head}^{{commit}}")
    monkeypatch.chdir(clone)
    base = _git(origin, "rev-parse", "main")
    return {"baseRefName": "main", "baseRefOid": base, "headRefOid": head}


def _gh_then_git(check: ModuleType, outputs: dict[str, str]) -> Callable[[list[str]], str]:
    """A stand-in for the script's runner that fakes gh and runs git for real.

    Args:
        check: The script, whose own runner runs git.
        outputs: gh's answer to each command, keyed by the command's words.

    Returns:
        The runner: a gh command not in `outputs` fails as gh would.
    """
    fake, real = _fake_run(outputs), check._run

    def run(command: list[str]) -> str:
        return real(command) if command[0] == "git" else fake(command)

    return run


_VIEW = "gh pr view 7 --json body,baseRefName,baseRefOid,headRefOid"


def test_a_pull_request_s_diff_and_files_come_from_git_as_the_workflow_builds_them(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, pull_request_7: dict[str, str]
) -> None:
    # gh answers only the body, the commits, and the decisions: `gh pr diff` refuses a diff
    # past 20,000 lines, so a call to it fails here.
    view = {"body": CLEAN, **pull_request_7}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\nREADME.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    body, changed, diff, decisions = check._from_pull_request(7)
    assert body == CLEAN
    assert changed == [_SKILL, "AGENTS.md"]
    assert "-- **Old rule.** Text.\n+- **New rule.** Text.\n" in diff
    assert "later.md" not in diff
    assert decisions == ["0104-canonical-ticker-refused.md"]


def test_main_reads_a_pull_request_with_gh_and_git(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
) -> None:
    view = {"body": CLEAN, **pull_request_7}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    assert check.main(["--pr", "7"]) == 1
    assert "Decisions touched" in capsys.readouterr().out


@pytest.mark.parametrize(
    "setting",
    [
        # Each of these two changes the `diff --git a/... b/...` header the rule check reads.
        ("diff.noprefix", "true"),
        ("color.ui", "always"),
        # A textconv filter that prints nothing leaves the rewrite no deleted line.
        ("diff.blank.textconv", "true"),
    ],
    ids=["noprefix", "color", "textconv"],
)
def test_the_user_s_diff_settings_never_hide_a_rule_rewrite(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
    setting: tuple[str, str],
) -> None:
    check._run(["git", "config", *setting])
    # The textconv filter applies to the files given its driver, here AGENTS.md.
    pathlib.Path(".git/info/attributes").write_text("AGENTS.md diff=blank\n")
    view = {"body": ANSWERED, **pull_request_7}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    assert check.main(["--pr", "7"]) == 1
    assert "Decisions touched" in capsys.readouterr().out


def test_a_rule_rewrite_is_read_from_a_subdirectory_with_relative_diffs(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
) -> None:
    # `diff.relative` would limit both the diff and the file list to `docs/`.
    check._run(["git", "config", "diff.relative", "true"])
    pathlib.Path("docs").mkdir()
    monkeypatch.chdir("docs")
    view = {"body": ANSWERED, **pull_request_7}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    assert check.main(["--pr", "7"]) == 1
    assert "Decisions touched" in capsys.readouterr().out


def test_a_file_name_the_encoding_cannot_read_is_listed_and_checked(
    *,
    check: ModuleType,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
) -> None:
    # The pull request also adds a file whose name is Latin-1, a byte UTF-8 never uses alone.
    # It is staged by name only, since some file systems refuse to create such a file.
    origin, head = tmp_path / "origin", pull_request_7["headRefOid"]
    name = os.fsdecode(b"docs/caf\xe9.md")

    def _git(*args: str) -> str:
        return str(check._run(["git", "-C", str(origin), *args])).strip()

    blob = tmp_path / "blob"
    blob.write_text("Latin-1.\n")
    entry = f"100644,{_git('hash-object', '-w', str(blob))},{name}"
    _git("checkout", "-q", "--detach", head)
    _git("update-index", "--add", "--cacheinfo", entry)
    _git("commit", "-q", "-m", "a Latin-1 name")
    head = _git("rev-parse", "HEAD")
    _git("update-ref", "refs/pull/7/head", head)
    _git("checkout", "-q", "main")
    view = {"body": ANSWERED, **pull_request_7, "headRefOid": head}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    assert check._from_pull_request(7)[1] == [_SKILL, "AGENTS.md", name]
    assert check.main(["--pr", "7"]) == 1
    assert "Decisions touched" in capsys.readouterr().out


#: The workflow's lines that build the changed-file list and the script's arguments from it.
_WORKFLOW_LIST = re.compile(
    r'^ *(git -C "\$repo" diff --name-only .*?^ *done < "\$RUNNER_TEMP/files")$',
    re.MULTILINE | re.DOTALL,
)


def test_the_workflow_s_file_list_matches_the_one_the_pr_form_builds(
    check: ModuleType, tmp_path: pathlib.Path, pull_request_7: dict[str, str]
) -> None:
    # The workflow fetches the base and the pull request into `pr/base` and `pr/head`.
    base, head = pull_request_7["baseRefOid"], pull_request_7["headRefOid"]
    check._run(["git", "fetch", "-q", "origin", f"+{base}:refs/pr/base", f"+{head}:refs/pr/head"])
    match = _WORKFLOW_LIST.search(_WORKFLOW.read_text())
    assert match is not None
    script = textwrap.dedent(match.group(1)) + "\nprintf '%s\\0' \"${args[@]}\"\n"
    env = {**os.environ, "repo": str(pathlib.Path.cwd()), "RUNNER_TEMP": str(tmp_path)}
    printed = subprocess.run(  # noqa: S603 - a fixed argv running the workflow's own lines
        ["bash", "-c", script],  # noqa: S607 - bash from PATH, as the workflow's runner finds it
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout
    arguments = printed.split("\0")[:-1]
    assert arguments[::2] == ["--changed-file"] * 2
    assert arguments[1::2] == check._diff(base, head)[1] == [_SKILL, "AGENTS.md"]


def test_a_pull_request_origin_does_not_hold_stops_with_what_to_do(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
) -> None:
    # gh reads another repository than `origin`, whose pull request 7 is another change.
    missing = "0" * 40
    view = {"body": CLEAN, **pull_request_7, "headRefOid": missing}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    with pytest.raises(SystemExit) as stopped:
        check.main(["--pr", "7"])
    assert stopped.value.code == 2
    err = capsys.readouterr().err
    base = pull_request_7["baseRefOid"]
    assert f"git could not fetch pull request 7's commits {base} and {missing} from" in err
    assert err.endswith(
        "Run the check from a clone whose origin is the repository gh reads "
        "(gh repo set-default --view).\n"
    )


def test_a_cited_decision_is_resolved_against_the_pull_request_s_base(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    pull_request_7: dict[str, str],
) -> None:
    # The worktree holds decision 120; the reviewed PR's base does not.
    folder = pathlib.Path("docs") / "dev" / "decisions"
    folder.mkdir(parents=True)
    (folder / "0120-local-only.md").write_text("# 120. Local only\n")
    view = {"body": "- Owner-approved (decision 120).\n", **pull_request_7}
    outputs = {_VIEW: json.dumps(view), _LISTING: "0104-canonical-ticker-refused.md\n"}
    monkeypatch.setattr(check, "_run", _gh_then_git(check, outputs))
    assert check.main(["--pr", "7"]) == 1
    assert "An approval is claimed ('Owner-approved')" in capsys.readouterr().out


def test_the_branch_is_measured_against_its_pull_request_s_base(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    # The pull request's base wins over a stale declaration in the draft.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text("<!-- base: fix/stale -->\n" + CLEAN)
    outputs = {
        "gh pr view --json baseRefName": json.dumps({"baseRefName": "fix/below"}),
        f"{_DIFF} origin/fix/below...HEAD": "",
        f"{_NAMES} origin/fix/below...HEAD": "src/x.py\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 0


#: Open pull requests: the one this branch was cut from, a farther one, one never fetched, one
#: stacked on this branch, and one whose branch already holds HEAD.
_OPEN = [
    {"headRefName": "feat/problems-table", "baseRefName": "main"},
    {"headRefName": "feat/older", "baseRefName": "main"},
    {"headRefName": "fork-branch", "baseRefName": "main"},
    {"headRefName": "fix/above", "baseRefName": "fix/mine"},
    {"headRefName": "fix/contains-mine", "baseRefName": "main"},
]
_STACKED = {
    "git branch --show-current": "fix/mine\n",
    "gh pr list --state open --limit 200 --json headRefName,baseRefName": json.dumps(_OPEN),
    "git rev-list --count origin/main..HEAD": "9\n",
    "git rev-list --count origin/feat/problems-table..HEAD": "2\n",
    "git rev-list --count origin/feat/older..HEAD": "5\n",
    "git rev-list --count origin/fix/above..HEAD": "1\n",
    "git rev-list --count origin/fix/contains-mine..HEAD": "0\n",
}


def test_a_stacked_branch_before_its_pull_request_is_measured_against_the_branch_below(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Against main, the base pull request's AGENTS.md edit would count as this branch's.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text("## Summary\n\nAdds a column.\n")
    outputs = {
        **_STACKED,
        f"{_DIFF} origin/feat/problems-table...HEAD": "",
        f"{_NAMES} origin/feat/problems-table...HEAD": "src/x.py\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 0
    assert "origin/feat/problems-table (the open pull request's branch" in capsys.readouterr().out


def test_a_declared_base_is_used_before_the_open_pull_requests_are_searched(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text(
        "<!-- base: feat/problems-table -->\n## Summary\n\nAdds a column.\n"
    )
    outputs = {
        "git rev-list --count origin/feat/problems-table..HEAD": "2\n",
        f"{_DIFF} origin/feat/problems-table...HEAD": "",
        f"{_NAMES} origin/feat/problems-table...HEAD": "src/x.py\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 0


def test_a_declared_base_that_already_holds_the_branch_is_ignored(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Declaring the branch itself would diff it against itself and hide its rule edit.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text(
        "<!-- base: fix/mine -->\n## Summary\n\nRewords a skill.\n"
    )
    outputs = {
        **_STACKED,
        "git rev-list --count origin/fix/mine..HEAD": "0\n",
        f"{_DIFF} origin/feat/problems-table...HEAD": "",
        f"{_NAMES} origin/feat/problems-table...HEAD": ".claude/skills/x/SKILL.md\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 1
    assert "Ignoring the declared base origin/fix/mine" in capsys.readouterr().out


def test_a_declared_base_git_cannot_find_is_ignored(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # A branch never pushed or fetched falls back to the search instead of a traceback.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text(
        "<!-- base: feat/unpushed -->\n## Summary\n\nAdds a column.\n"
    )
    outputs = {
        **_STACKED,
        f"{_DIFF} origin/feat/problems-table...HEAD": "",
        f"{_NAMES} origin/feat/problems-table...HEAD": "src/x.py\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 0
    assert "Ignoring the declared base origin/feat/unpushed: git cannot find it" in (
        capsys.readouterr().out
    )


def test_a_declaration_after_the_first_line_declares_nothing(check: ModuleType) -> None:
    assert check.DECLARED_BASE.match("## Summary\n<!-- base: feat/x -->\n") is None
    declared = check.DECLARED_BASE.match("<!-- base: feat/x -->\n## Summary\n")
    assert declared is not None
    assert declared.group("branch") == "feat/x"


def test_a_branch_cut_from_main_is_measured_against_main_and_told_how_to_declare(
    *,
    check: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Every open pull request's branch is as far as main or farther, so main stays the base.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text("## Summary\n\nRewords a skill.\n")
    outputs = {
        **_STACKED,
        "git rev-list --count origin/main..HEAD": "2\n",
        f"{_DIFF} origin/main...HEAD": "",
        f"{_NAMES} origin/main...HEAD": ".claude/skills/x/SKILL.md\0",
    }
    monkeypatch.setattr(check, "_run", _fake_run(outputs))
    assert check.main([]) == 1
    out = capsys.readouterr().out
    assert "origin/main (no open pull request's branch is closer)" in out
    assert "'<!-- base: <that branch> -->'" in out


def test_a_branch_whose_open_pull_requests_cannot_be_listed_is_measured_against_main(
    check: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".git-pr-body.md").write_text("## Summary\n\nRewords a skill.\n")
    outputs = {
        "git branch --show-current": "fix/mine\n",
        "git rev-list --count origin/main..HEAD": "2\n",
        f"{_DIFF} origin/main...HEAD": "",
        f"{_NAMES} origin/main...HEAD": ".claude/skills/x/SKILL.md\0",
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


@pytest.mark.parametrize(
    "quote",
    [
        "  > Should this guard stay as it is?  \n  > Yes.\n",
        "  > Should this guard remain? Please explain.\n  >\n  > Yes, keep it.\n",
    ],
    ids=["trailing spaces after the question", "a blank line between question and answer"],
)
def test_a_question_and_its_answer_on_their_own_lines_stay_apart(
    check: ModuleType, quote: str
) -> None:
    body = f"- The owner approved on 2026-11-15:\n{quote}"
    assert check.find_problems(body, [], "") == []
