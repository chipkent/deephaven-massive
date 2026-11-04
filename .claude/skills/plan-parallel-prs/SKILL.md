---
name: plan-parallel-prs
description: "Use this skill whenever you coordinate work split across several agents or PRs at once: deciding which agent does what, writing their briefs, setting a merge order, running their reviews, or telling the owner which PRs are ready. That includes \"start these issues in parallel\", \"fan this out\", \"spin up agents for #A, #B and #C\", \"work through the open issues\", and \"which PRs can merge?\". It maps overlap, groups work into PRs by content, stacks only what must stack, reserves decision and verification-item numbers, writes short briefs, and runs one blockers-only review per PR. Not for an agent doing a single PR (AGENTS.md \"Working in parallel\" covers that) or for writing one PR description."
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

## 3. Set the merge order

- PRs that share a file get a fixed merge order, told to the owner as one list ("merge #A, then
  #B").
- Stack a PR on another (GitHub native stacks, `gh stack link`, bottom to top) only when it
  needs the other's content. A squash merge still leaves the upper PR conflicting, so a stack
  costs one `main` merge per upper PR; a PR that merely shares a file branches from `main` and
  merges `main` once when its turn comes.
- When a PR renames a public name, tell every open agent the new name; the second PR to land
  merges `main` and reruns its tests, because a text merge cannot catch a semantic conflict.
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

## 6. Review and readiness

1. When a PR's CI passes, run one independent review that reports blockers only: wrong
   behavior, data loss, a crash, or a rule violation. The reviewer acts as a top-1% developer
   who flags design smells first.
2. Low findings go into one follow-up issue, which is worked right away, never into another
   push of the PR under review. Re-review only the fix to a blocker.
3. A PR is ready when CI is green, GitHub reports it mergeable, every review thread (the
   owner's included) has a reply, its issue is in `closingIssuesReferences`, and its blockers
   are fixed. Check each live before saying so.
4. Tell the owner the moment a PR is ready, so it merges before another PR conflicts with it,
   with the `writing-to-the-owner` skill.

## 7. Keep cost down

Most of an agent's cost is context re-read every turn. Start a fresh agent with a short brief
for each new round of work instead of resuming a long one; resume only for a small follow-up.
As coordinator, keep your own turns short and start a fresh session at each phase change, with
a handover note. Settle minor choices yourself instead of running multi-agent debates, and don't
re-run a broad review whose last pass found little.
