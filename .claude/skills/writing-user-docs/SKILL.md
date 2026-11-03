---
name: writing-user-docs
description: "Use this skill whenever you write, rewrite, move, or review a page users read: README.md, any page under docs/guide/, docs/tester-guide.md, or the hand-written intro text of the generated docs/datasets/ reference. Use it too for any code change that alters behavior a user page describes (a default, a limit, an error, a column, an install step), even when the request never mentions docs, and when someone says a page \"reads like AI\", is hard to follow, or needs a recipe, a section, or a fix. It finds the page that owns each fact, applies the voice rules, banned patterns, and terminology of docs/dev/user-docs.md, and checks examples, anchors, and claims before the change goes out. Not for docstrings (writing-docstrings), PR descriptions or issue bodies (writing-a-pr-description), or messages to the owner (writing-to-the-owner)."
---

# Writing user docs

The standard is `docs/dev/user-docs.md`; read it before you edit a user page. Four kinds of
reader use these pages, from quants to traders who can barely program, and each must follow
every page to the end. A page that only an engineer understands has failed.

## 1. Find the page that owns the fact

Apply the placement test: a fact a new user needs in the first hour goes in `README.md`; a fact
looked up only for a specific case goes on its `docs/guide/` page, with a one-line summary and a
link in the README. The page-ownership table in the standard names each page's subject. Each fact
has one home; other pages link to it.

For a code change, find every page that describes the behavior you changed:

```bash
git grep -n '<function, argument, setting, or error text>' -- README.md docs/guide docs/tester-guide.md
```

Update each hit in the same PR.

## 2. Write to the standard

- Second person, active voice, present tense; the outcome first.
- One question per section; bullets and tables over paragraphs.
- Every term defined at first use, in the words of the standard's terminology table.
- A number only when the reader acts on it.
- Install steps follow the standard's Install commands: `pip` first with `uv pip` as a one-line
  alternative, and the prerequisites stated plainly.
- No hard-coded release version: look up the newest tag, use plain `pip install`, or link to
  `releases/latest`; mark any shown version string as an example.

## 3. Check before it goes out

1. **Examples**: each block is complete, runs after the page's stated setup, and is followed by
   what you see. Run `uv run pytest tests/test_docs.py -q`.
2. **Claims**: trace every factual sentence you wrote to code, a test, or Deephaven's or
   Massive's docs. Cut what you cannot trace. Check claims about the Deephaven IDE in a running
   IDE.
3. **Anchors**: a section others cite has an explicit `<a id>`; a moved or renamed anchor
   rewrites its citations in `src/`, other pages, and the tester guide in the same PR.
4. **Banned patterns**: run the two greps in the standard's Banned patterns section, and read
   the draft against its table. Fix every hit.
5. **Terminology**: grep for the "Never" column's words (`worker`, `web UI`, `transports`,
   `flush`, ...) and fix each hit in user text.
6. **The reader test**: reread each section as a trader who has never written a loop. Would they
   know what to paste, what they will see, and what to do next?
