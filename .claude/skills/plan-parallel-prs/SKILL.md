---
name: plan-parallel-prs
description: "Use this skill whenever you coordinate work split across several agents or PRs at once: deciding which agent does what, writing their briefs, setting a merge order, running their reviews, or telling the owner which PRs are ready. That includes \"start these issues in parallel\", \"fan this out\", \"spin up agents for #A, #B and #C\", \"work through the open issues\", and \"which PRs can merge?\". It maps overlap, groups work into PRs by content, stacks overlapping PRs in a planned merge order, reserves decision and verification-item numbers, writes short briefs, and holds every PR to its author's adversarial pre-push review. Not for an agent doing a single PR (AGENTS.md \"Working in parallel\" covers that) or for writing one PR description."
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
Never plan a step you already know is bad, such as text moved verbatim to be rewritten later, to
make a diff easier to review: fix the content as you move it. Docs that share a file, a
generated page, or a generator go on one branch, never on parallel branches. Each user doc gets
at most two grading rounds, then goes to the owner with its graders' notes, and a doc that
passes is pushed at once, never held back for others.

## 3. Set the merge order

Contention at merge time (rebases, `main` merges, and the CI reruns they trigger) is a main
reason features land slowly. Stacking is how parallel work avoids it, so use it.

- When parallel PRs touch the same files or build on each other, stack them with GitHub's
  native stacked PRs: each upper PR branches from the PR below it, opens with
  `gh pr create --base <lower branch>`, and joins the stack with `gh stack link <PRs bottom to
  top>` (`gh extension install github/gh-stack` once). Never run `gh stack submit`, `push`,
  `sync`, or `rebase`, which rewrite pushed branches. A stack is one chain, so a PR that two
  others need sits below both.
- Plan the merge order now and tell the owner as one list ("merge #A, then #B"). The owner
  merges from the bottom up; after each merge GitHub re-targets and rebases the next PR, and its
  agent pulls it. Only a conflict GitHub cannot resolve needs a merge of the base branch.
- PRs that share no files stay independent, each from `main`.
- When a PR renames a public name, tell every open agent the new name; the second PR to land
  merges `main` and reruns its tests, because a text merge cannot catch a semantic conflict.
  A brief that asks for a repository-wide grep to prove an old name is gone excludes
  `docs/dev/decisions.md` and `docs/dev/verification-items.md`: both are append-only, so their
  past entries keep the old name.
- Agents never force-push or rebase a pushed branch. Conflicts are resolved by the owning agent
  with a written rationale; the coordinator never pushes or resolves one itself.

## 4. Reserve numbers

Take the highest decision number in `docs/dev/decisions.md` and verification-item number in
`docs/dev/verification-items.md`, on `main` and in every open PR's diff, and assign each PR that
adds one its own next numbers, in merge order.

## 5. Launch

Run at most about seven coding agents at once: each pre-push hook runs the full suite, and
sixteen at once overloaded the machine until nothing merged. Read-only agents don't count.
Prioritize what blocks the current goal.

Each agent reads `AGENTS.md` and the project skills on its own, so a brief does not restate them.
A brief says only what the agent cannot know:

- the goal, the issue it closes, and what is out of scope;
- the base branch (`origin/main`, or the branch below it in its stack) and the stack's PR
  numbers, bottom to top;
- the reserved decision or verification-item numbers, or "none";
- the files other agents are changing at the same time, and any rename in flight;
- the owner decisions it must follow, quoted or linked, and anything only this task has learned;
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
   it. It reports blockers only (wrong behavior, data loss, a crash, or a rule violation), as a
   top-1% developer who flags design smells first. It answers from primary sources and builds a
   probe only when a number decides the question, kept minimal and run once.
3. Low findings go into one follow-up issue, which is worked right away, never into another
   push of the PR under review. Re-review only the fix to a blocker.
4. A PR is ready when its author's adversarial review is done and its findings fixed, CI is
   green, GitHub reports it mergeable, every review thread (the owner's included) has a reply,
   its issue is in `closingIssuesReferences`, and any separate review left no blocker unfixed.
   Check each live before saying so. Sweep recently merged PRs too: a bot finding posted after
   the merge gets a reply, and a valid one gets a fix PR that the reply links.
5. Tell the owner the moment a PR is ready, so it merges before another PR conflicts with it,
   with the `writing-to-the-owner` skill.

## 7. Keep cost down

Most of an agent's cost is context re-read every turn. Start a fresh agent with a short brief
for each new round of work instead of resuming a long one; resume only for a small follow-up.
As coordinator, keep your own turns short and start a fresh session at each phase change, with
a handover note. Settle minor choices yourself instead of running multi-agent debates, and don't
re-run a broad review whose last pass found little.

## 8. After a merge

Delete the merged PR's local branch and worktree; GitHub deletes the remote branch itself. A
branch whose PR closed without merging stays until the owner says to delete it. Before removing
a worktree, run `grep INSTALL_PYTHON .git/hooks/pre-push` in the main clone: the shared hooks
record the Python of whichever checkout last ran `pre-commit install`. If that is the worktree,
first run `uv run pre-commit install --hook-type pre-commit --hook-type pre-push` from the main
clone, or every agent's commit fails.
