# 130. Grading stops at three, reviews judge only the change, and agents hand over early

Date: 2026-11-17

Status: accepted

## Question

The skills set no number of grades, and one agent graded a PR description fourteen times.
Graders scored text the PR didn't change, and a push that only merged `main` got a full review,
a new grade, and a full local test run. How many grades does a text get, what does a grader or
reviewer judge, and when does an agent hand over?

## Options

- Two grades for a PR description and three for a user doc: proposed first.
- Three grades for both.

For the review scope and the handover, none recorded: one path was proposed.

## Ruling

On 2026-11-17, in the Claude Code session that produced pull request #759, the owner ruled:

- A text gets at most three fresh grades. A PR description that still fails is pushed, and its
  grader's remaining objections go to the owner. A user doc that still fails goes to the owner
  with the failing passage.
- A grader judges only what the pull request changes.
- A push that merges `main` is reviewed only for its conflict resolutions.
- An agent hands over to a fresh one at about 150,000 tokens of context or after a PR's first
  push, and batches its reads. The coordinator messages the owner only when something is ready
  or needs them.

## Reason

The owner wanted merges to take minimal work, not a review of what merged, and the cost seen in
that session not to recur. The owner gave no other reason.

## Consequences

A PR description can reach review below the grader's bar, and the owner reads its open
objections. Merged commits get no second review, though CI still tests them. A handover costs a
written note.

## Rule

`AGENTS.md`, Workflow, "Keep context small" and the merge bullet; `writing-a-pr-description`, Grade.
