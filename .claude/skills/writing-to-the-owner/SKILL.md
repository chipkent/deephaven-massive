---
name: writing-to-the-owner
description: "Use this skill whenever you write anything the project owner must read and act on: a question, a decision with options, a design or contract change to approve, a conflict between a rule and the request, whether a requested change breaks users' code, a recommendation, a status report, a list of what waits on them, a summary of what a PR does, or a reminder of an unanswered question. That includes replies to 'status?', 'what needs me?', 'do you need anything from me?', 'is X ready to merge?' and 'what does #N do?', questions placed in a PR description or review reply, and questions from subagents or reviewers that you pass on. Use it even for a short message: the owner usually lacks the context you have. It settles whether to ask at all, checks the facts live, and makes each question stand alone every time it is asked, repeats included: the thing being decided shown (the text, diff, signature, or names, before and after), the problem with a concrete user example, what Massive or Python precedent says, then the question, options with their consequences, and a recommendation, checked by asking whether the owner could decide from that message alone. Not for docstrings, PR descriptions (writing-a-pr-description), or briefs to other agents."
---

# Writing to the owner

The owner decides from your message alone. They have not read your scratchpad, the agent
reports, the review, the diff, or your last message, and they are coming back from other work.
A question they cannot answer from the message in front of them costs a round trip and some of
their trust. Their complaints, each made more than once: "Gibberish. What info do I need to make
a decision?", "You have given me essentially zero context on this readme change but keep asking
for information", "I have no idea what these abbreviations are", "If you ever told me, it is
buried in tons of discussion", and "Stop obsessing about useless legal shit … What do you
really need to know?"

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
becomes a live test and a verification item (`docs/dev/verification-items/`), and the claim it
supports is marked unverified until it runs. Either the owner runs one with their key, or you run it
after their yes in the session (`AGENTS.md`, Limits). When a decision waits on the result, ask:
give the exact command, what it costs (key use, and any WebSocket connection it takes from their
live tables), and what the result settles.

**Never re-ask what the owner already decided.** Before asking, list the decisions
(`grep -m1 '^Status:' docs/dev/decisions/0*.md`) and read every one whose name touches the
subject; then check `AGENTS.md`, the skills, the issue and its comments
(`gh issue view <n> --comments`), and the owner's earlier answers. If an earlier answer settles the case, follow it and cite it; if it
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
   83). Whether a value is valid is Massive's to answer (`AGENTS.md`, Massive owns its data and
   its rules); the library refuses only a wrong type, a value it cannot express, or one that
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

The owner has not seen the code, the diff, or the discussion, and cannot approve a change they
have never seen. Each question carries everything needed to decide it, in this order:

1. **A heading in plain words** naming the subject: "When one of several symbols doesn't
   exist", not "Q4: 404 handling".
2. **The thing being decided, shown.** Quote it: the new text of a doc, the lines of a diff,
   the old and new signature, a table of old and new names. "The README note", "the logging
   changes", or "how the implementation changed" gives the owner nothing to judge. Show the
   lines that change, with enough around them to read.
3. **The problem it solves, with one concrete example**: a call a user makes or a page they
   read, what goes wrong today, and what they see after. If no user hits the problem, say so;
   the question may not be worth asking.
4. **Precedent, when it bears on the choice**: what Massive calls the thing (its field name or
   docs, quoted and linked), and what Python, pandas, numpy, or Deephaven do. This answers the
   owner's usual first questions, "why is this being made?" and "what does Massive name it?",
   before they have to ask.
5. **The question**, in one sentence: "Should the README keep this paragraph?"
6. **Two to four options**, with "do nothing" when it is real. Each says what the user sees (an
   example call and result), what changes in the project, and what it costs or risks.
7. **A recommendation**, and why it beats the next-best option on the property that matters.
8. **What waits on the answer.** Silence is not consent: the question stays open.

**Every time you ask, ask in full.** A question repeated in a later message (a reminder, a
status report, a list of what is still open) restates all of the above, because the earlier
message has scrolled away and the owner should not have to dig for it. A one-line pointer such
as "the Codex reply on #613", "the decision-file migration", "item 86", or "Q3-3" is not a
question: restate it in full or drop it from the list. If restating every question makes the
message too long, it holds too many.

**Few questions, each complete.** Two full questions get better answers than six thin ones.
Decide what is yours (section 1), send in full the questions that block work now, and hold the
rest for a later message, where they go out in full too. Until then, a status report gives only
how many wait (section 6).

**No label the owner did not coin, unless this message defines it.** Verification-item and
decision numbers, question codes ("Q3", "L6", "option B" from an earlier message), names that
grew up inside a PR ("the walk", "the lock"), function names, and agent or reviewer names mean
nothing to the owner. Say what the thing is in plain words; a function name may follow the
plain words, never replace them. A PR or issue number comes with its title.

### Before you send

Read each question as the owner will, arriving from other work, and ask: **could the owner
decide this from this message alone, without scrolling up or opening a link?** If not, add what
is missing. The usual gaps:

- a change described but not shown;
- a problem with no user-facing example;
- an unanswered "why?" or "what does Massive call it?";
- a label or name this message does not define;
- a repeat that points back instead of restating.

When a message holds three or more decisions, or any public-API or design decision, also have a
Fable agent with no other context read it and list every question it could not answer from the
message alone. Fix those before sending.

## 5. Words

- Answer first. Background in a few sentences. No process narrative (which agent did what,
  which review round).
- Plain words, short sentences, the user's view before the implementation. File and function
  names come last.
- No scratchpad paths, and no bare answers ("yes, yes, no").
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
   say which. Each open decision that blocks work now, in full every time (section 4). For
   questions held for a later message, give only how many wait, never their labels. Any
   manual step only the owner can take.
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

## Example 3: a change the owner had never seen

As sent (the owner: "You have given me essentially zero context on this readme change but keep
asking for information. This is unacceptable"):

> **Still open:** keep or drop the README note in #605?

The owner had not seen the note, did not know what problem it solved, and could not tell what
dropping it would cost. Rewritten:

> **A new paragraph in the README tutorial: close each example's session**
>
> The tutorial's thirteen examples are pasted one after another into the Deephaven console,
> and each opens its own session. Nothing tells the reader to close one before the next. A live
> table keeps polling until its session closes, so on a Basic key, which allows five requests a
> minute across every session using it, Example 5's news poll keeps spending those requests
> while Example 7 runs.
>
> PR #605 ("fix: rename Market.FX to FOREX and log debug to a file") adds this paragraph above
> the table of examples:
>
> > Close each example's session with `m.close()` before you paste the next, except Example 5's,
> > which Example 6 closes. A live table keeps running, and using your key's requests, until its
> > session closes. Every session that uses your key shares one request rate, such as Basic's
> > five requests a minute. Your key also gets one streaming connection per asset class, which
> > sessions in this server with the same settings share. A session with no live tables also
> > closes by itself, quietly, once no variable holds it.
>
> Should the README keep this paragraph?
>
> - **A. Keep it.** Readers close each session as they go, and a Basic key's requests go to the
>   example in front of them. Cost: five sentences before the examples start.
> - **B. Keep only the first two sentences.** Readers still close sessions, but don't learn
>   that sessions share the key's rate and connections, which they need once they write their
>   own scripts.
> - **C. Drop it.** The tutorial stays shorter; a Basic-key reader who runs the examples in
>   order has earlier polls competing with later examples, with no hint why.
>
> I recommend A: the sharing it explains is what a reader hits first after the tutorial, and
> the README is where they look. PR #605 waits on this.
