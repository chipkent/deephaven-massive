---
name: writing-user-docs
description: "Use this skill whenever you write, rewrite, edit, move, or review text a user of deephaven-massive reads: README.md (the hook, Install, Configuration, the teaching examples, recipes, troubleshooting), any page under docs/guide/, docs/tester-guide.md, or the hand-written intro of docs/datasets/. Use it for a whole page or a single paragraph, for 'write the Install section', 'add an example', 'fix this recipe', and 'this reads like AI' or 'this is slop'. Use it too when a code change alters something a user page describes (a default, a limit, an error, a column, an install step), even if the request never mentions docs. It drafts to the owner's requirements, section models, and sentence rules in docs/dev/user-docs.md, then runs cold editor passes that score the draft on the standard's O'Reilly rubric and revise it to the owner's pass bar. Not for docstrings (writing-docstrings), PR descriptions or issue bodies (writing-a-pr-description), or messages to the owner (writing-to-the-owner)."
---

# Writing user docs

The owner rejected the last round of user docs as "AI slop", though nearly every fact in them
was right. They read like a machine wrote them: sentences packed with three or four facts and
joined by colons and semicolons, the library as the subject of every sentence, clipped
telegraphic phrases, bold-label bullets, examples that were fragments, and paragraphs about how
the library works when the reader wanted to know how to use it. The bar now is prose a
technical editor with twenty years at O'Reilly would pass, for quants, traders who can barely
program, vibe coders, and software engineers at once.

[`docs/dev/user-docs.md`](../../../docs/dev/user-docs.md) is the standard. Its first
section, "This project", holds every project fact: the readers, the owner's requirements, the
install decision, the pitch, the page map, and the teaching plan. The rest is general: where
each fact goes, the sentence and form rules, the AI tells, the section models, the
terminology, and the rubric. Read it whole before your first draft in a session; it's long
because it's the whole contract. This skill is how to work to it.

## How to work

1. Read the standard, then the section model your task needs (the hook, Install,
   Configuration, first examples, the teaching sequence, or guide pages). A model is a shape,
   not text to copy: fill it with this task's facts.
2. Gather and verify the facts before you write. Trace each one to `src/deephaven_massive/`,
   a test, the generated pages in `docs/datasets/` (each dataset's columns, and the plans that
   include it), or Deephaven's or Massive's docs. When you rewrite existing text, list its
   facts first, so you can show at the end that each one survives or moved somewhere named.
3. Draft. Then save it and run the checker:

   ```bash
   uv run python .claude/skills/writing-user-docs/scripts/check_user_doc.py <file>
   ```

   Fix every FAIL. Judge every WARN: a colon that introduces an example is fine, and one that
   holds back the point is a reveal.
4. Edit it yourself first, cold: run the editor's pass at the end of the standard's rubric
   section (first sentences only, subjects and verbs, every colon and semicolon, every
   contrast, every series of three, read aloud). Then get a cold editor's pass (below), and
   revise until the draft meets the standard's pass bar or you've used two revision rounds.
5. Before it goes out: run `uv run pytest tests/test_docs.py -q` for any change to `README.md`
   or `docs/guide/`, check that every original fact survives or has a named new home, and
   rewrite the citations of any anchor you moved, in `src/` docstrings (by full GitHub URL)
   and on the other pages. Put the final score sheet in your report or PR description, and list what
   only a run with a Massive key can confirm, for the owner's test plan.

## Where drafts fail

Eval runs of this skill, graded blind, lost most of their points in the same places. Check
your draft for these before an editor sees it:

- An example that isn't complete. Every block runs as pasted into a fresh console: its
  imports, its session, and the settings it needs. Before the code, say the plan, any setting,
  and how to run it. A request that says earlier code has already run doesn't change this.
- Plan claims stated too broadly. Massive sells plans per asset class, so "every Massive plan"
  is wrong for a stocks example; say "every Stocks plan". A higher plan doesn't always include
  a lower one's feeds: some need an add-on. Check each plan you name in the dataset's page
  under `docs/datasets/`. The API key is a requirement too: "no settings" means none beyond
  it.
- Mechanism. A paragraph on how the library pages, paces, reconnects, or holds sessions is
  usually one the reader doesn't need. Say what they get and what they do.
- More than the task asked for. Each fact you add is one more place to be wrong and one more
  topic in a paragraph. Put a related fact you found on its own page, or in your notes for the
  owner, not in the draft.
- Paragraphs with two or three topics, and runs of one-sentence paragraphs. Give each
  paragraph one topic and three to five sentences.
- A caveat before the point, or a warning after the step it guards.
- Live-table timing stated loosely. A `last_by` table keeps its last rows after trading
  stops; it's empty only when you create it while the market is closed. The repository doesn't
  document trading-hour windows, so don't state them.
- Marketing lines in the hook. Selling copy is where the AI tells cluster: taglines, series of
  three, "never X you didn't ask for", and lead-ins that only announce a list. Sell with
  concrete facts in plain sentences.

## The editor's pass

A draft that only you have read is the draft the owner rejected, so get a cold read. Spawn a
fresh subagent with this brief. For the README's first screen (the hook, Install, and the
first examples), spawn two at once, each with the brief, so their grades are independent:

```text
You are a development editor at O'Reilly Media with twenty years of experience editing
technical books for working programmers and traders. Read the "This project", "AI tells", and
"editor's rubric" sections of <repo>/docs/dev/user-docs.md, then grade the draft at <file>, a
section of the user docs of deephaven-massive. Check its claims against <repo>/src and
<repo>/docs/datasets where they matter. Don't mark down a missing contact or a missing way to
ask for access: the install is for a reader who can already see the repository.

Score each criterion from 0 to 3; a 3 means you'd make no mark at all. Be the
editor authors dread, and don't round up. For each score below 3, quote the worst sentence and
give your rewrite. Then give one holistic score out of 10: how close this is to something
you'd send to print as it stands. Apply the standard's pass bar and say PASS or FAIL. Say in
one sentence whether the owner, who rejected the previous docs as "AI slop", would call this
draft slop.
<On a second or third read, add: "The previous editor's score sheet is below. Check whether
each fault it named is fixed, then grade the whole draft as you would have anyway.">
Return the score sheet only.
```

The pass bar, with its totals and per-criterion floors, is in the standard's rubric section.
A first-screen section passes only when both graders pass it.

Revise from the score sheets by fixing each fault's pattern everywhere it occurs, not just the
quoted sentence. Where several faults sit in one paragraph, rewrite the paragraph rather than
patching it: patches are how drafts pick up new faults between rounds. A pass covers the text
the editors read, so if you change anything afterwards beyond their own rewrites, the draft
needs another read.

Stop after two revision rounds, which means three reads at most. If the draft still falls
short, don't loop further: hand it to the owner with the last score sheets and notes, and say
plainly which parts of the bar it missed. Never call a draft that fails the bar done. If you
can't spawn a subagent (a nested run, or a concurrency limit), grade the draft yourself
against the rubric, reading it as if someone else wrote it, and say in your report that the
grade is your own.
