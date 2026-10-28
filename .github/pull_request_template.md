<!--
Write for two readers at once: a very intelligent reviewer arriving at the PR with little or no
context, and a less experienced end user reading it much later, trying to understand what the PR
or the feature does. Neither knows the library's internals, file layout, history, or jargon.
Never assume the reader knows what you know. The description fails if the reader has to ask
"but what does this actually do?" The Summary is the most important part; the engineering detail
goes in "How it works", after it, never in its place. See "Pull request descriptions" in AGENTS.md.
-->

## Summary

<!--
Several paragraphs if needed, in plain English, giving:
- the big picture: what the library is for, in one line; which feature this PR is about; what a
  user does with it; and where this PR fits in the overall work (what came before, what it unblocks);
- the background: every term the reader needs, defined in plain words;
- exactly what this PR does, step by step, covering every user-visible change and every rule it
  adds or changes;
- why it matters, with a concrete example a user would hit;
- what the reader should check or decide before merging.
No file names, internal names, review labels, or decision numbers. A reader of this section alone
must be able to explain the PR to someone else.

Example, for a hypothetical PR:

This library turns Massive's market data into Deephaven tables. One of its main features is
history(), which downloads past price bars, for example a year of daily bars for a list of
stocks. Massive can serve those bars from two places: its web API, one request at a time, or its
bulk "flat files", one file per day holding every symbol. The library picks between them for you.

Before this PR, the same daily bar came back with a different time label depending on which
source the library picked: midnight from one, 4 pm from the other. A table of AAPL bars and a
table of all stocks' bars therefore did not line up, and a join between them silently matched
nothing.

This PR gives every daily, weekly, and monthly bar one consistent label, whatever source served
it, and it refuses loudly, with an error explaining what to try instead, if Massive ever sends a
label it does not recognize. Nothing about which bars you get changes; only their labels become
consistent. It is one of the history fixes that follow the date-range work, and the next step,
adding a start and end time to every bar, builds on it.

Before merging, check that the label rule in the table below matches how you expect daily bars
to be dated.
-->

## Why

<!-- The problem, with one concrete example: a call, and what went wrong. -->

## What changes

<!-- One bullet per user-visible change. Add a short before-and-after example wherever the public API or a result changes. -->

## Decisions

<!-- Every owner decision this PR applies and every departure from the plan, each with a one-line reason. Write "None" if there are none. -->

## How it works

<!-- The engineering detail for a reviewer: the approach, the main modules and functions changed, and anything subtle (threading, liveness, data formats). Organize it by idea, not file by file. -->

## Verification

<!-- What was run and what it showed: scripts/check.sh, free-threaded tests, mutation probes, live checks. A few lines. -->

## Follow-ups

<!-- Work deliberately left out, each with its reason or the live check it waits on. Write "None" if there is none. -->

<!-- Never include review-finding labels, agent identifiers, commit-by-commit narration, first-person process, or counts that will go stale. Rewrite this description to the final state on every push that changes the PR. -->

<!-- Use "Refs #N" for a partial delivery, or replace the line below with "No tracking issue." -->
Closes #
