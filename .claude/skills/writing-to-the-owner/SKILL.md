---
name: writing-to-the-owner
description: "Use this skill whenever you write anything the project owner must read and act on: a question, a decision with options, a design or contract change to approve, a conflict between a rule and the request, whether a requested change breaks users' code, a recommendation, a status report, a list of what waits on them, a summary of what a PR does, or a reminder of an unanswered question. That includes replies to 'status?', 'what needs me?', 'do you need anything from me?', 'is X ready to merge?' and 'what does #N do?', questions placed in a PR description or review reply, and questions from subagents or reviewers that you pass on. Use it even for a short message: the owner usually lacks the context you have. It settles whether to ask at all, checks the facts live, and makes each question stand alone: background with a concrete example first, then the question, options with their consequences, and a recommendation. Not for docstrings, PR descriptions (writing-a-pr-description), or briefs to other agents."
---

# Writing to the owner

The owner decides from your message alone. They have not read your scratchpad, the agent
reports, the review, or your last message, and they are coming back from other work. A question
they cannot answer from the message in front of them costs a round trip and some of their trust.
Their complaints, each made more than once: "You give me questions without sufficient detail to
answer them", "I have no idea what these abbreviations are", "If you ever told me, it is buried
in tons of discussion", and "Stop obsessing about useless legal shit … What do you really need
to know?"

## 1. Ask, or decide?

**When unsure, ask.** Ask if you are not sure the decision is yours, if your choice rests on a
fact you could not check or a guess about what the owner wants, or if it would be hard to undo
(merged, published, posted outside the repository, or built on heavily). Asking about real
uncertainty is never the failure; asking without context, or asking what is settled or minor and
certain, is.

**Always the owner's:**

- what a user sees: a public change the request does not cover, or, from `0.1.0`, a change
  that breaks code written to the last release (`AGENTS.md`, The public API and the data). A public name a
  requested feature needs is part of the request, and a fix that makes code do what its docs
  say is not a public change;
- scope: adding or dropping a feature, dataset, or piece of work;
- money and access: plans, subscriptions, keys, paid tools. Never recommend a purchase; plans
  and tests must work with whatever the owner's key has;
- outward actions: merging, publishing, posting upstream;
- loosening a check: a `tests/test_layout.py` rule removed or widened, a lint limit raised or a
  rule turned off, coverage, a hook, `check.sh`, or a CI job. A stricter check, an entry under
  an existing rule, and a dependency the change needs are not questions: name them under Check
  before merging;
- every lint or type-check suppression, with its reason and why the design can't avoid it;
- a column suspected but not proven to be a field another dataset already names, with both
  fields' documentation quoted;
- a conflict: a rule against the request, two instructions against each other, or code bent
  to satisfy a rule (`AGENTS.md`, The request and the rules). Name both sides and why they
  clash, propose the edit to the instruction that ends it, and keep working on what the answer
  can't change;
- undoing something the owner decided.

**Yours to decide**, when you are confident (every fact checked, no earlier owner decision
touched) and it is easy to change afterwards: PR order and splits, scheduling, private names,
internal log levels and tuning, test mechanics, low-level implementation or DRY questions, and
any column name the naming policy settles (`_massive/refresh/naming.py`; raise only a real gap
in the policy, never a single column).
Have a Fable agent check anything you are tempted to ask that a top-1% developer would find
obvious; it should not reach the owner. List every choice you made yourself under **Decided**,
one line each, so the owner can object. Each line states the choice itself (the new name, the
value chosen), not just that a choice was made.

**Closed subjects:** never raise licensing, legal, affiliation, or "what users may post"
questions. Decide them yourself and do the useful thing. A check that needs Massive's API with a key
becomes a live test and a verification item (`docs/dev/verification-items.md`), and the claim it
supports is marked unverified until it runs. Either the owner runs one with their key, or you run it
after their yes in the session (`AGENTS.md`, Limits). When a decision waits on the result, ask:
give the exact command, what it costs (key use, and any WebSocket connection it takes from their
live tables), and what the result settles.

**Never re-ask what the owner already decided.** Before asking, check `docs/dev/decisions.md`,
`AGENTS.md`, the skills, the issue and its comments (`gh issue view <n> --comments`), and the
owner's earlier answers. If an earlier answer settles the case, follow it and cite it; if it
may cover the case but leaves a gap, quote it and ask only about the gap.

**A question from the owner is a question, not an instruction.** Answer it; change nothing
unless they say so. **If you can do a decided task yourself, do it** rather than handing it over.

## 2. Check every fact now

Not from memory or an agent's report:

- **PR and issue state** from GitHub (`gh pr view <n>`, `gh pr list --state all`): merged or
  open, CI, mergeable, unanswered review threads, and questions buried in PR bodies.
- **A claimed fix**: after `git fetch`, the commit is on the remote branch and its diff makes the
  fix. A merge of `main` is not a fix.
- **Facts about Massive or Deephaven**: from a primary source, linked or quoted. Mark anything
  unchecked as an assumption and say how you would check it.
- **Consistency**: the recommendation agrees with earlier owner decisions and with how the API
  treats the same concept elsewhere; name the decision it follows, or say plainly that it
  conflicts. State the simplest design that meets the need, and why more is needed if you
  recommend more. Never offer a hybrid that keeps the costs of both options.
- **One position**: if your recommendation changed since you last raised it, say so and why.

## 3. Vet a design before the owner sees it

A design or contract change the owner must approve (`AGENTS.md`, The public API and the data)
reaches them only after it survives these steps: options built straight from raw research come
out incomplete, and a long list of questions goes unread.

1. **A Fable agent proposes the complete design, and separate Fable critics attack it.** Revise
   until the critics run out of substantive objections. Every item a top-1% developer would find
   obvious is decided there and listed under **Decided**; only what is still a matter of taste,
   product, or reversing an owner decision reaches the owner, ideally fewer than five items.
2. **Complete.** Each option covers every dataset and case it touches, defines every term, and
   shows the calls a user writes.
3. **Researched, not assumed.** A choice of format, standard, or convention cites what widely
   used comparable tools do; convenience to us is not a reason. A rule about data correctness
   (a trading day, a time zone, a stamp) cites primary sources (Massive's docs and knowledge
   base, market-structure rules, live evidence), marks each gap unverified, and is attacked by a
   critic before you answer. Never offer an opinion as an answer.
4. **Not built on thin evidence.** One symbol or a few days that disagree may be bad vendor data
   (`AGENTS.md`, Supply Massive's data), not a reason to change behavior for everything: measure
   broadly first, with a live test the owner runs.
5. **Held to the project's design rules.** A public API design shows the import line a user
   types and the path `help()` prints; a public twin that re-exports a private module, or a
   name chosen for the library's convenience instead of the user's, fails. Closed sets are
   enums, never magic strings. The
   user's code works on any Massive plan, with no tier-specific configuration. Every API
   accepts a live table and follows it, and history and live calls never mix (decisions 82 and
   83). Whether a value is valid is Massive's to answer (`AGENTS.md`, Massive judges its own
   values); the library refuses only a wrong type, a value it cannot express, or one that
   would silently return wrong data, and never silently cleans input. The simplest behavior that delivers the data beats a clever rule, and an error comes
   only when no source can serve. The public API stays minimal: every type a public signature
   declares is public and documented, a private implementation may sit behind it, and a new
   public name or module is justified by the user code that needs it, never by a list. A design
   that needs a tool to look through a private name is wrong.
6. **When you don't know how to do something** (a git workflow, a tool), stop and have an agent
   research it with sources and a critic. Never propose one untried idea after another.
7. **The rules are vetted too.** When a design has to bend to satisfy an `AGENTS.md` rule (it
   meets the rule's words while defeating its intent, or takes a worse shape only to pass it),
   the rule is part of the question: put the rule, what it costs here, and the edit you propose
   to it before the owner, next to the design. Code a rule prescribes is compliance, and facts
   of Python, Deephaven, and Massive are constraints, not rules to raise.

## 4. The shape of a decision

Write each question for a reader with no context: the owner has not seen the code, the
discussion, or your last message. Each decision stands alone, in this order:

1. **A heading in plain words** naming the subject: "When one of several symbols doesn't
   exist", not "Q4: 404 handling".
2. **Background first**: what the thing is, in a line, with a concrete example (a call a user
   makes and what happens today), and every term the decision uses.
3. **The question**, in one sentence: "What should `history()` do then?"
4. **Two to four options**, with "do nothing" when it is real. Each says what the user sees (an
   example call and result), what changes in the project (which PR, files, docs), and what it
   costs or risks.
5. **A recommendation**, and why it beats the next-best option on the property that matters.
6. **What waits on the answer.** Silence is not consent: the question stays open.

Labels ("Q3", "option B", "L6", plan codes) work only inside one message. To ask again later,
restate the background in a line, the question, each option in one line, and the
recommendation.

When a message holds three or more decisions, or any public-API or design decision, have a
Fable agent with no other context read it first and list every question it could not answer
from the message alone. Fix those before sending.

## 5. Words

- Answer first. Background in a few sentences. No process narrative (which agent did what,
  which review round).
- Plain words, short sentences, the user's view before the implementation. File and function
  names come last.
- No labels, codes, agent names, or scratchpad paths the owner did not coin. No bare answers
  ("yes, yes, no").
- Never forward a subagent's or reviewer's question as written: rewrite it to this standard, or
  decide it under section 1 and list it under **Decided**.
- None of the AI tells in `docs/dev/user-docs.md#ai-tells` (asides, stacked "such as" and
  "which" clauses, repetition, invented numbers, meta-commentary, false contrasts, hype words,
  colon reveals).
- When the owner must read a file, send it (SendUserFile) rather than citing a path.

## 6. Status reports

Answer "status?", "what needs me?", and the like in this order, from live state:

1. **Needs you now.** Each PR ready to merge: number, link, one plain sentence on what it does
   for a user, and a line confirming, just now, that CI is green, it is mergeable, every review
   thread is answered, its author ran the adversarial `pre-push-review` and fixed what it
   found (only a typo or one-word doc fix skips it), and you read its file list and diff
   against the request (`plan-parallel-prs` §6). If a risky change also had a separate
   reviewer agent, say whether a blocker is left. A PR missing any of these is not ready, so
   say which. Each open decision (in full the first time; later its title, its
   options in a line each, and the recommendation). Any manual step only the owner can take.
   A user doc is ready to read only when a fresh calibrated-editor grade passes its bar
   (`writing-user-docs`); until then, report what blocks it, not the draft.
   If nothing, say "Nothing needs you" first.
2. **Decided**: one line each.
3. **In progress**: one plain line each, including what is blocked and on what.
4. **Remaining**, and **when you'll next be needed**. Every open issue has an agent on it, a
   PR, a close reason, or a named owner question; none is parked for later.

After the owner answers a list of questions, repeat back the answers you recorded and what is
still open.

## Example 1: a question without context, rewritten

As sent (the owner: "This open question doesn't have enough information for me to answer"):

> **Still open from you:** the `type=`/`id=` parameter names.
> - **A (recommended):** rename them to `ticker_type=` and `condition_id=`.
> - **B:** keep them and allow a `noqa: A002` exception.

Rewritten:

> **Two public argument names that hide Python built-ins**
>
> Two reference calls take arguments named after Python built-ins: `m.reference.tickers(type=…)`
> filters the ticker list by kind (common stock, ETF, and so on), and
> `m.reference.conditions(id=…)` looks up one trade-condition code. Massive's docs use those
> names. In Python, an argument named `type` or `id` hides the built-in of the same name inside
> the method, so the linter flags it (ruff rule A002).
>
> Should the two arguments get new names?
>
> - **A. Rename them to `ticker_type=` and `condition_id=`.** Users write
>   `m.reference.tickers(ticker_type=dm.TickerType.CS)`, and the library still sends Massive
>   `type=CS`. Cost: one edit in the open reference-data PR and a decision entry.
> - **B. Keep `type=` and `id=`, matching Massive's docs.** Needs a permanent `# noqa: A002`
>   on both.
>
> I recommend A: the new names say what they filter, and A avoids a permanent linter
> exception. The reference-data PR waits on this.

## Example 2: a choice that belonged under Decided

As sent (the owner: "L6: I have zero clue what you are suggesting here"):

> **L6. Approve the design changes that go with PR 1?** Low-level "gave up retrying" messages
> drop to INFO … the design gains the three new functions and the log-writer thread.
> *Recommended:* approve.

The agent had checked that the three functions and the thread are private and that the log
levels change nothing a user's call returns or raises, and each is a one-line change to undo.
So these were never questions. Rewritten:

> **Decided:**
> - A failure that has used up its retries is logged once, as an ERROR where it reaches your
>   code. The lower-level "gave up retrying" message that repeated it drops to INFO.
> - The design records the three private helper functions and the background log-writer thread
>   this PR adds.
