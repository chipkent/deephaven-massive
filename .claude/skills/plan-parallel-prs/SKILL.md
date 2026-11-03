---
name: plan-parallel-prs
description: "Use this skill whenever you coordinate work split across several agents or PRs at once: planning which agent does what, writing their briefs, setting a merge order, running their reviews, or reporting to the owner which PRs are ready. That includes requests such as \"start these issues in parallel\", \"fan this out\", \"spin up agents for #A, #B and #C\", and \"which PRs can merge?\". It maps where the pieces overlap, groups them into PRs, stacks overlapping PRs in a set merge order, reserves DESIGN.md numbers, gives every brief the same checklist, and runs one blockers-only review per PR. Not for an agent doing one PR (AGENTS.md \"Working in parallel\" covers that), and not for writing a single PR description."
---

# Planning parallel PRs

Parallel agents save time only when their PRs do not collide. On 2026-11-02, four PRs changed
the same ingest module and `DESIGN.md` and merged in no set order; one PR merged `main` four
times, each a full check and a paid CI run; `DESIGN.md` decision numbers collided again and
again; and reviews found blockers that a line in the brief would have prevented. Plan before
any agent starts.

## 1. Map the overlap

For each piece of work, list what it will change:

| Piece | Files | `DESIGN.md` sections | `README.md` sections |
|---|---|---|---|

Include the open PRs already in flight (`gh pr list`, `gh pr diff <n> --name-only`).

## 2. Group by content

- One PR holds related content; justify each piece by the change it serves.
- Two changes to the same code path go in one PR, never two.
- Unrelated pieces stay in separate PRs, even when they are small.

## 3. Set the merge order

- For PRs that share a file or section, fix the order now.
- Each later PR branches from the earlier one's branch, not `main`, and its body says
  "Builds on #<n>".
- After the earlier PR merges, the later one merges `main` once.
- Tell the owner the order, with each PR's number, in one list.

## 4. Reserve `DESIGN.md` numbers

Take the highest decision (§ 22) and verification item (§ 23) on `main` and in every open PR's
diff, and give each PR that adds one its own next numbers, in merge order. Put them in the brief.

## 5. Brief checklist

Every agent brief includes:

- **Rules most often missed**, from `AGENTS.md`:
  - 9: `DESIGN.md` first for a public signature, default, or table semantics;
  - 19: an enum member selects a mode, never `None`;
  - 22: log each decision and failure with its values, never a secret;
  - 23: supply Massive's data, never repair it;
  - 24: one bad symbol or row is logged and skipped under its own `OnError` setting, and never
    fails the table.
- **The Deephaven invariants** section of `AGENTS.md`, for any change that runs off the caller's
  thread, takes a lock, or creates a live table.
- **The pre-push hook is the check**: never run `./scripts/check.sh` before a push.
- **The base branch**: `origin/main`, or the branch of the PR it builds on.
- **The reserved numbers**, or "none".
- **The PR-description standard**: `docs/dev/pull-requests.md`, including "Builds on #<n>" and
  the reserved number.

## 6. Review loop

1. As soon as a PR's CI passes, run one review that reports blockers only.
2. Minor findings go into one follow-up issue, never another push.
3. After the blocker fixes are pushed, re-review only those fixes.
4. A PR is ready only when CI is green, GitHub reports it mergeable, every review thread has a
   reply, and every blocker is fixed.
5. Tell the owner the moment a PR is ready, so it merges before another PR conflicts with it.

## 7. Questions for the owner

Follow the `writing-to-the-owner` skill. In short:

- Check each question against the owner's recorded decisions and `DESIGN.md` first; ask only
  what they do not settle.
- Ask about a `DESIGN.md` change only when it changes what users see or the documented
  contract. A fix that keeps the documented intent and changes only the mechanism records the
  existing decision it follows, and the work proceeds.
- Frame it in user terms, with a concrete example, every term defined, and one recommendation.
- Research the facts and cite sources; never guess.
- Never change a recommendation silently: say it changed and why.
