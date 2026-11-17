---
name: plan-parallel-prs
description: "Use this skill whenever you coordinate work split across several agents or PRs at once, or hand any task to another agent: deciding which agent does what, writing a brief, setting a merge order, running their reviews, or telling the owner which PRs are ready. That includes \"start these issues in parallel\", \"fan this out\", \"spin up agents for #A, #B and #C\", \"should you also work on #N?\", \"work through the open issues\", and \"which PRs can merge?\". It maps overlap, groups work into PRs by content, stacks PRs that need each other's content in a planned merge order, reserves decision and verification-item numbers, writes briefs that quote the owner and the issue's acceptance list, holds every PR to its author's adversarial pre-push review, and checks each PR's diff against its request before calling it ready. Not for an agent doing a single PR (AGENTS.md, Workflow, covers that) or for writing one PR description."
---

# Planning parallel PRs

Parallel agents save time only when their PRs do not collide. When four PRs changed the same
module and merged in no set order, one of them merged `main` four times, each a full check and a
paid CI run, and decision numbers collided again and again. The owner also wants everything
started at once, not staged: plan the collisions, then launch.

## 1. Map the overlap

For each piece of work, and each open PR already in flight (`gh pr list`,
`gh pr diff <n> --name-only`), list the files, the user-doc pages and sections, and any new
decision or verification item it adds. Open issues are not parked: each one gets an agent, a PR,
a close reason, or a named owner question.

## 2. Group into PRs by content

One PR holds one coherent change and everything it needs. Two changes to the same code path go
in one PR. Unrelated pieces stay separate, however small; never split or merge PRs only by size.
Each PR must be able to merge on its own, whatever the open questions turn out to be. A
mechanical step a change needs (a rename, a file move, a regeneration, one rule applied
everywhere) is right by itself, so it gets its own PR and merges first: the rename of a setting
across 80 files lands, then the PR that changes what the setting covers, so the rename never
waits on a design question and the behavior PR stays small. Two parts of one behavior (the REST
half of a rule and its WebSocket half) are not such a split: each is wrong until the other
lands, so neither can merge. Never plan a step you already know is bad, such as text moved
verbatim to be rewritten later, to make a diff easier to review: fix the content as you move
it. Docs that share a file, a
generated page, or a generator go on one branch, never on parallel branches. Each user doc ships
only at a passing grade: one that fails is rewritten from its outline and graded again, and
only an obstacle a few rounds can't clear goes to the owner, as a request for help, never for
sign-off. A doc that passes is pushed at once, never held back for others.

## 3. Set the merge order

Contention at merge time (rebases, `main` merges, and the CI reruns they trigger) is a main
reason features land slowly. A planned merge order limits it; stacking does not remove it.

- Stack a PR only when it needs the code of the PR below it (it calls a function or uses a
  type that PR adds), never to avoid a conflict: a stacked PR inherits every merge of `main`
  the lower one makes and waits on its review, which costs more than resolving the conflict
  once when the second PR lands. Two PRs that only touch the same file both start from `main`.
  Don't start a wide change (many files, or a rewrite of a module) while an open PR is
  changing the same files; wait for it, or take its agent's work into yours. A PR that moves or
  renames files merges before the PRs it would conflict with, never after them. Use GitHub's native
  stacked PRs: each upper PR branches from the PR below it, opens with
  `gh pr create --base <lower branch>`, and joins the stack with `gh stack link <PRs bottom to
  top>` (`gh extension install github/gh-stack` once), and names the merge order in its body.
  `AGENTS.md` forbids the `gh stack` commands that rewrite pushed branches. A stack is one
  chain: when two PRs need
  the same lower PR but not each other, stack one on it and start the other from `main` once
  the lower PR merges.
- Plan the merge order now and tell the owner as one list ("merge #A, then #B"). The owner
  merges from the bottom up; after each merge GitHub re-targets and rebases the next PR, and its
  agent pulls it.
- GitHub's rebase is not always clean: squash-merging the bottom PR can still leave the next
  one conflicting with `main`. Budget one
  merge of `main` per stacked PR after the PR below it merges, each a full check and a CI run.
- PRs that do not need each other's content stay independent, each from `main`, even when they
  share files; the second to land merges `main` if it conflicts.
- When a PR renames a public name, tell every open agent the new name; the second PR to land
  merges `main` and reruns its tests, because a text merge cannot catch a semantic conflict.
  A ruling the owner gives while PRs are open goes, as the question and the owner's dated
  answer, to every agent whose PR touches its subject, the same day.
  A brief that asks for a repository-wide grep to prove an old name is gone excludes
  `docs/dev/decisions/` and `docs/dev/verification-items/`: their entries are never rewritten,
  so past ones keep the old name.
- Agents never force-push or rebase a pushed branch. Conflicts are resolved by the owning agent
  with a written rationale; the coordinator never pushes or resolves one itself.
- Stop and re-plan a PR that has drifted: the owner changed its design after it opened, its
  diff grew past the request (a three-file fix now touches twenty), or it is merging `main` a
  second time (one merge to resolve a conflict is normal). Each means the PR carries more than
  one change or waits on something its agent can't settle, and every further push pays for all
  of it. Have the agent land the settled part as a PR that is correct by itself and move the
  rest to an issue with what it learned.

## 4. Reserve numbers

Take the highest decision number in `docs/dev/decisions/` and verification-item number in
`docs/dev/verification-items/`, on `main` and in every open PR's diff, and assign each PR that
adds one its own next numbers, in merge order.

## 5. Launch

Run at most about seven coding agents at once: each agent's tests start Deephaven JVMs, and
sixteen at once overloaded the machine until nothing merged. Read-only agents don't count.
Prioritize what blocks the current goal.

Before briefing, record each ruling the owner gave you on the subject as a decision file
(`AGENTS.md`, Workflow; `writing-a-record`): the brief cites it, the file keeps it for every
later agent.

Each agent reads `AGENTS.md` and the project skills on its own, so a brief does not restate them.
A brief is the agent's request, so it carries the owner's intent, not your summary of it. It
says only what the agent cannot know:

- the goal in the owner's own words, quoted from where the owner said it, and the issue it
  closes;
- the issue's acceptance lines, quoted, and the owner's comments on it, quoted with their dates
  (`gh issue view <n> --comments`). The acceptance list is the requirement; the issue's
  approach and steps are background;
- for user-facing work (docs, an API, an error message), who reads the result and what they
  need from it;
- for a public change, the design the owner approved and their answer to it, quoted (the
  calls a user types, what is refused and why, the settings). No design yet means the agent's
  first deliverable is that design, not code (`AGENTS.md`, Workflow). For any change beyond a
  small fix, the agent drafts the design with Fable and has a second Fable instance attack it
  before code; a choice the two can't settle, or a point the brief leaves ambiguous, comes
  back to you as a question with options and a recommendation, never built on a guess;
- each protection the work may drop (a check, refusal, validation, test, setting, or guard
  against a mistake that stays possible), with the owner's yes to that removal and its
  question, or the
  instruction to ask before removing it: a brief's reading of a rule approves no removal;
- that review findings which add work go under Follow-ups with an issue, never into the diff;
- the base branch (`origin/main`, or the branch below it in its stack) and the stack's PR
  numbers, bottom to top;
- the reserved decision or verification-item numbers, or "none";
- the files other agents are changing at the same time, and any rename in flight;
- every other owner decision it must follow, as its decision number or, before a file
  records it, the question in plain words and the owner's answer as written, with its date
  and where, so the agent can quote both in its PR (a
  brief relays decisions; it never grants an approval `AGENTS.md` reserves for the owner's
  own message),
  and anything only this task has learned;
- any conflict you see between the request and a rule or an issue's text, named as a question
  for the owner, never settled in the brief;
- whether to stop at a local commit and report, or open the PR (and when to push).
- that it writes its PR-body file inside its own worktree, never under a shared scratchpad name
  such as `pr.md`, which another agent may overwrite.

## 6. Review and readiness

1. Before every push, each agent runs its own adversarial `pre-push-review`, so paid CI and the
   review bots run as rarely as possible. Only a trivial change (a typo or a one-word doc fix
   that touches no code, test, configuration, or documented behavior) skips it. Say so in every
   brief.
2. For a risky change (concurrency, connections, TLS, or the public contract) you may add a
   second, separate reviewer agent, in addition to the author's review and never instead of
   it. It reports blockers only (wrong behavior, data loss, a crash, a rule violation, or code
   bent to satisfy a rule, which it raises as a question about the rule), as a top-1% developer
   who flags design smells first. It answers from primary sources and builds a probe only when a
   number decides the question, kept minimal and run once; a probe that needs Massive's API
   with a key is the owner's to run (`AGENTS.md`, Limits).
3. Low findings go into one follow-up issue, which is worked right away, never into another
   push of the PR under review. Re-review only the fix to a blocker.
4. A PR is ready when its author's adversarial review is done and its findings fixed, CI is
   green, GitHub reports it mergeable, every review thread (the owner's included) has a reply,
   its issue is in `closingIssuesReferences`, any separate review left no blocker unfixed,
   and you have checked its description and diff yourself:
   - `uv run python scripts/check_pr_body.py --pr <n>` passes;
   - every approval the description claims is a decision file or an answer in the owner's
     messages to you, to the question shown (one you cannot find is a blocker, whoever wrote
     the body); a decision file it adds says no more than that answer; and its
     `Decisions touched:` line is right (grep the decisions for the subject);
   - each entry under Decisions that removes, reverses, or changes something public names
     the rule or ruling that made it the agent's to take;
   - its file list and diff (`gh pr diff <n> --name-only`, then `gh pr diff <n>`), read
     against the owner's request and the owner's rulings on its subject, recorded or not:
     every changed file serves the request, nothing it needs is missing, and nothing it
     removes or reverses lacks the owner's yes, except what `AGENTS.md` exempts (a
     guard deleted with the code it guarded, or replaced by one that catches the same
     mistake or more, with the description saying what replaced it).
   A large diff the request causes (a generator fix that regenerates every page) is
   fine; a small file the request doesn't need is not. Check each live before saying so. Sweep
   recently merged PRs too: a bot finding posted after the merge gets a reply, and a valid one
   gets a fix PR that the reply links.
5. Tell the owner the moment a PR is ready, so it merges before another PR conflicts with it,
   with the `writing-to-the-owner` skill.

## 7. Keep cost down

Most of an agent's cost is context re-read every turn. Start a fresh agent with a short brief
for each new round of work instead of resuming a long one; resume only for a small follow-up.
As coordinator, keep your own turns short and start a fresh session at each phase change, with
a handover note. Settle minor choices yourself instead of running multi-agent debates, and don't
re-run a broad review whose last pass found little. When an agent's PR opens, goes green, or
merges, start a fresh agent with a short handover note (the branch state, what is done, what
remains, any traps) instead of resuming the long one; the project settings also make agents
compact at about 300,000 tokens.

## 8. After a merge

Delete the merged PR's local branch and worktree; GitHub deletes the remote branch itself. A
branch whose PR closed without merging stays until the owner says to delete it. Before removing
a worktree, run `grep INSTALL_PYTHON .git/hooks/pre-push` in the main clone: the shared hooks
record the Python of whichever checkout last ran `pre-commit install`. If that is the worktree,
first run `uv run pre-commit install --hook-type pre-commit --hook-type pre-push` from the main
clone, or every agent's commit fails.
