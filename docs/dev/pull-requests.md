# Pull request descriptions

The owner reads every pull request description to decide whether to merge it. Write it as an
editor would leave it: the big picture first, plain words, short sentences, every term defined,
and no process narrative. `.github/pull_request_template.md` holds the skeleton; this page is
the standard it points to.

## The two readers

Write for two readers at once: a very intelligent reviewer arriving at the PR with little or no
context, and a less experienced end user reading it much later, trying to understand what the
PR or the feature does. Neither knows the library's internals, file layout, history, or jargon.
Never assume the reader knows what you know.

The description fails if the reader has to ask "but what does this actually do?" A reader of
the Summary alone must be able to explain the PR to someone else. The Summary is the most
important part. The engineering detail is still required, but it comes after the Summary, in
How it works, never in its place.

## Sections, in this order

1. **Summary**, over several paragraphs when the PR needs them, in plain English:
   - the big picture: what the library is for, in one line; which feature the PR is about; what
     a user does with that feature; and where the PR fits in the overall work (what came
     before, what it unblocks next);
   - the background: every term the reader needs, defined in plain words;
   - exactly what the PR does, step by step in full sentences, covering every user-visible
     change and every rule it adds or changes;
   - why it matters, with a concrete example a user would hit;
   - what the reader should check or decide before merging.

   No file names, internal function names, review labels, or decision numbers.
2. **Why**: the problem, with one concrete example (a call, and what went wrong).
3. **What changes**: the behavior after the merge, one bullet per user-visible change, with a
   short before-and-after example wherever the public API or a result changes.
4. **Decisions**: every owner decision the PR applies and every departure from the plan, each
   with a one-line reason; "None" when there are none.
5. **How it works**: the engineering detail for a reviewer: the approach, the main modules and
   functions changed, and anything subtle (threading, liveness, data formats), organized by
   idea rather than file by file.
6. **Verification**: what was run and what it showed (`scripts/check.sh`, the free-threaded
   tests, mutation probes, live checks), in a few lines.
7. **Follow-ups**: work deliberately left out, each with its reason or the live check it waits
   on; "None" when there is none.
8. The issue link, on its own line (see below).
9. The required footer.

## Never include

Review-finding labels (H1, M2), agent identifiers, commit-by-commit narration, first-person
process ("I did X, then Y"), counts that will go stale, or anything else the reader must decode.

## Keep it current

The description describes the branch as it is. When a push changes the PR, rewrite the
description to the final state in the same batch (`gh pr edit <n> --body-file <file>`), instead
of appending to it.

## Close the issue

A PR that resolves an issue puts `Closes #<issue>` on its own line, so the owner's
merge closes the issue. A PR that delivers only part of an issue says `Refs #<issue>` instead,
and its description names what remains. A PR with no issue says "No tracking issue." Before
asking for review, check the link with:

```bash
gh pr view <n> --json closingIssuesReferences
```

Never close an issue by hand to make up for a missing link.

## A good Summary

For a hypothetical PR:

> This library turns Massive's market data into Deephaven tables. One of its main features is
> `history()`, which downloads past price bars, for example a year of daily bars for a list of
> stocks. Massive can serve those bars from two places: its web API, one request at a time, or
> its bulk "flat files", one file per day holding every symbol. The library picks between them
> for you.
>
> Before this PR, the same daily bar came back with a different time label depending on which
> source the library picked: midnight from one, 4 pm from the other. A table of AAPL bars and a
> table of all stocks' bars therefore did not line up, and a join between them silently matched
> nothing.
>
> This PR gives every daily, weekly, and monthly bar one consistent label, whatever source
> served it, and it refuses loudly, with an error explaining what to try instead, if Massive
> ever sends a label it does not recognize. Nothing about which bars you get changes; only
> their labels become consistent. It is one of the history fixes that follow the date-range
> work, and the next step, adding a start and end time to every bar, builds on it.
>
> Before merging, check that the label rule in the table below matches how you expect daily
> bars to be dated.

## Checklist before a push

Read the description twice, once as each reader, and confirm:

- the Summary alone lets either reader explain the PR to someone else, and it names no file,
  internal function, review label, or decision number;
- the sections above are present, in order; Decisions lists every departure from the plan with
  its reason, or says "None"; What changes shows before and after wherever the public API or a
  result changes;
- nothing from "Never include" appears, and every term is defined;
- it describes the branch as it will be after this push;
- the issue link sits on its own line, and `closingIssuesReferences` shows the issue.
