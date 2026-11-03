---
name: working-in-parallel
description: "Use this skill whenever you start work on a branch in this repository, and again whenever your PR overlaps another open PR, the PR below yours merges, your branch conflicts with main, or you need a new decision or verification-item number. That includes \"start issue #N\", \"open a PR for this\", \"stack this on #M\", \"main moved, update the branch\", and \"add a decision\". It checks for overlapping PRs, stacks with GitHub's native stacked PRs, says when (and only when) to merge, forbids rewriting a pushed branch, and keeps agent context small. Not for coordinating several agents at once (the plan-parallel-prs skill covers that) and not for the review before a push (pre-push-review)."
---

# Working in parallel

Several agents work on this repository at once. Every collision costs a full check and a paid
CI run, so each agent checks for overlap before it starts and never rewrites history after it
pushes.

## Before starting

1. List the files and the user-doc pages and sections (`README.md`, `docs/guide/`) you will
   change.
2. Check the open PRs: `gh pr list`, then `gh pr diff <n> --name-only` for each likely overlap.
3. If an open PR changes the same files, stack yours on it with GitHub's native stacked PRs:
   branch from its branch, push, open your PR against that branch
   (`gh pr create --base <its branch>`), then run `gh stack link <its PR> <your PR>`, listing
   every PR of a longer stack bottom to top. Install the extension once:
   `gh extension install github/gh-stack`.

## After pushing

- **Never rewrite a pushed branch**: never force-push, and never rebase a branch you have
  pushed. Never run `gh stack submit`, `push`, or `sync`, which push with
  `--force-with-lease`, or `gh stack rebase`. GitHub's own stack rebases are allowed: the
  automatic one after a PR below yours merges, and the owner's **Rebase stack** click.
- **After the PR below yours merges**, GitHub re-targets yours and rebases its branch: pull the
  rebased branch (`git pull --rebase`) instead of merging `main` by hand.
- **Merge only when needed.** When GitHub cannot rebase the stack because of a conflict, merge
  your PR's base branch (`main` for the bottom PR); when your branch conflicts with `main`,
  merge `main`. Resolve the conflict in the merge, push, and say in the PR body how you
  resolved it. Never merge only to stay current, and never resolve a conflict by taking one
  side wholesale (`--ours`, `--theirs`) without reading both.
- **The pre-push hook is the check.** It runs `./scripts/check.sh` and picks the scope from the
  files the branch changes, so never run the check yourself just before a push.

## Taking a number

A new decision takes the next number in `docs/dev/decisions.md`, and a new verification item
the next in `docs/dev/verification-items.md`: the number the coordinator reserved for you, or
else the next after the highest on `main` and in every open PR's diff. Name the number in the
PR body. Both files are append-only: a changed decision gets a new entry, and the old one's
status becomes "superseded by N".

## Keeping context small

Every turn re-reads all the context so far, which is most of an agent's cost. Use `grep -n` and
read only the section you need; keep command output short (`-q`, `| tail`, `--stat`); write a
large output to a scratch file and read only what matters; and never re-read a file you just
edited.
