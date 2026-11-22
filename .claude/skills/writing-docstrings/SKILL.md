---
name: writing-docstrings
description: "Use this skill whenever you write or change a docstring, a dataset method's documentation, a config comment, a pytest marker description, or CLI help text in this repository. That includes adding a module, class, function, dataset, or query method, changing a public signature, and \"document this\". It gives the documentation standard: what each kind of docstring must say, how dataset docstrings link their generated docs/datasets/ page (by full GitHub URL) instead of repeating shared argument rules, where user and contributor documentation lives, how examples stay real code, and how a change to a public docstring is fact-checked and graded. Not for README.md or docs/troubleshooting.md (writing-user-docs), PR descriptions (writing-a-pr-description), or messages to the owner (writing-to-the-owner)."
---

# Writing docstrings

Docstrings serve two readers: a person in an IDE or the Deephaven console, and an agent that
knows nothing but the code. Write clear, edited prose. A one-line docstring on anything with
behavior worth knowing is a defect. The user docs (`README.md`, `docs/troubleshooting.md`,
`docs/configuration.md`, and the generated `docs/datasets/`) and the public docstrings are the
user contract, so a docstring that disagrees with the code is a bug.

## What each docstring says

- **Every docstring opens with purpose, not inventory.** Its first paragraph tells a reader who
  has never seen the code what the thing is for, in plain words, and when they would use it: the
  idea, the problem it solves, how it fits. A list of members, a table of names, or a
  restatement of the signature is not an explanation; it can follow the explanation, never
  replace it. Before you finish, read the first paragraph alone and ask whether a newcomer now
  understands why this exists.
- **Modules**: what the module is for, what it owns, and how it fits with its neighbors. The
  first line names its one subject. The package `__init__.py` is the front door: what the
  library does, the public API with a short example, where to read next, and any status a
  reader must know.
- **Classes**: the concept, its lifecycle, who creates it, and its thread-safety; public ones
  include a short example.
- **Functions and methods**: what they do and why one would call them; every argument (meaning,
  units, accepted values, default); the return value (for a table, its columns); every
  exception and when; side effects (network calls, threads, subscriptions, tables created); and
  blocking and threading constraints.

## Reviewing a public docstring

A public docstring is user documentation: `help()` shows it in the console. Any change to the
docstring of the package, or of a public module, class, function, method, setting, enum, or
exception, goes through the `writing-user-docs` review before it is pushed:

- **Purpose first.** Read each changed docstring's first paragraph alone: if it enumerates
  (members, names, a table) instead of explaining what the thing is for and when to use it,
  rewrite it before grading.
- **Describe less.** Say what a caller needs to call it correctly. Leave out the mechanism and
  intricate edge cases; a reader who needs them reads the code.
- **Check every fact.** A fresh subagent traces each changed claim to the code and marks it
  SOURCED, FALSE, or UNSOURCED. Fix every FALSE or UNSOURCED claim before grading.
- **Grade with two editors.** Spawn two fresh graders with
  `.claude/skills/writing-user-docs/agents/editor.md` and its four readers, scoped to the
  changed docstrings. Only `__init__.py`, the front door, is graded as a landing page, with the
  twenty-second hook and job-level pains; class and method docstrings are graded as reference
  text. Apply only the fixes both graders agree on, plus any false claim either finds.
- **Ship only at a passing grade.** Every grade uses fresh graders. When patching leaves a
  docstring short of the bar, rewrite it from what the caller needs and grade it again, with at
  most three fresh grades. If the third doesn't pass, send the owner the failing docstring and
  the graders' sheets as a request for help (`writing-to-the-owner`), never as finished
  text. Never call a failing docstring done.

Private and internal docstrings keep the lighter rules on this page, with no review round.

## Dataset methods and query methods link instead of repeating

An argument many datasets share (`symbols`, `start`, `end`, `as_of`, `sort`, `order`, `limit`,
`filters`, `adjusted`, `timespan`, `multiplier`, `venues`) has one rule, written once in
`src/deephaven_massive/_datasets/argument_rules.py`. `scripts/render_reference.py` prints the
shared rules once, under "Shared arguments" on the dataset index, `docs/datasets/README.md`,
each with its own anchor. A dataset page's Arguments table links each shared argument's name to
its rule on the index and prints beside it only the dataset's own line from its docstring's
`Args`. A rule about a public type, such as `dm.Order` or
`dm.HistorySource`, lives in that type's docstring. A dataset docstring:

- gives each argument one line that says only what is particular to this dataset, with no link:
  the index carries the shared rule. Every argument gets a line, because that line fills the
  page's Description column;
- names its page, `docs/datasets/<dataset>.md`, by its full GitHub URL in its Returns line, so
  it opens from an IDE or the console;
- runs about 15 to 25 lines: the summary, what a row is and what is unusual, one Args line per
  argument, Returns, exact Raises, and one or two example lines.

To change a shared rule, edit `argument_rules.py` and regenerate the pages. Any other citation,
such as a README section or a troubleshooting section, uses the full URL too:
`https://github.com/chipkent/deephaven-massive/blob/main/<page>#<anchor>`. `tests/test_docs.py`
fails on a cited page or anchor that does not exist, and on a bare path instead of a URL.

## Where documentation lives

- User pages are listed in the page map of `docs/dev/user-docs.md`, which says which page owns
  each fact, and the `writing-user-docs` skill covers writing them. Every Python block on them
  runs in `tests/test_docs.py`, so an example is real code.
- `docs/datasets/` is generated by `scripts/render_reference.py` from the catalog, the dataset
  docstrings, and `argument_rules.py`; never edit it by hand.
- Contributor material goes in `CONTRIBUTING.md` or `docs/dev/`; agent rules in `AGENTS.md` and
  the skills.

## Style

- Write to [How an editor writes](../../../docs/dev/user-docs.md#method): the rule in the
  first sentence, then the exception a caller hits; one case per sentence; three or more
  cases as a list. A setting's docstring is its reference entry on `docs/configuration.md`,
  so it keeps that entry's budget: about 120 words after the one-line summary.
- Examples are real code that runs against the documented API.
- Never restate the signature, use filler ("This function…"), or leave a term undefined.
- Pass the terminology test of `docs/dev/user-docs.md`: use the word of the community that
  owns the concept, Massive's for its data and Deephaven's for its tables ("server", never
  "worker"; "Deephaven IDE", never "web UI"; `last_by` is an aggregation).
- Never cite a decision number or a planning document; state the behavior or the reason.
- Describe the code as it is now, never how it used to be: a docstring is not a changelog.
- The text of an exception or a warning or error log is not a docstring: `writing-error-messages`
  covers it.
- Configuration text is documentation too: config comments, pytest marker descriptions
  (`name(args): meaning`, in plain sentences), and CLI help text meet the same standard.
