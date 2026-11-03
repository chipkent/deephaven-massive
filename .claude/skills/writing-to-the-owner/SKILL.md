---
name: writing-to-the-owner
description: "Use this skill whenever you are about to write anything the project owner must read and act on: a question, a decision with options, a recommendation, a status report, a list of what is waiting on them, a summary of what a PR does, or a reminder of a question they have not answered. That includes replies to 'status?', 'what needs me?', 'do you need anything from me?', 'is X ready to merge?' and 'what does #N do?', questions you put in a PR description or review reply, and questions from subagents or reviewers you pass on. Use it even when the message is short or the owner seems to know the context; they usually don't. It first settles whether to ask (when unsure, ask), then checks the facts live, then makes every question stand alone with plain-word background, concrete options and consequences, and a recommendation. Not for docstrings, not for whole PR descriptions (the writing-a-pr-description skill covers those), and not for briefs to other agents."
---

# Writing to the owner

The owner decides from your message alone. They have not read your scratchpad, the agent
reports, the review, or your previous message, and they are coming back from other work. A
question they cannot answer from the message in front of them costs a round trip and some of
their trust. Each rule below answers something the owner had to say more than once: "You give
me questions without sufficient detail to answer them", "I have no idea what these
abbreviations are", "If you ever told me, it is buried in tons of discussion", "Your writing is
gibberish", and "Stop obsessing about useless legal shit … What do you really need to know?"

## 1. Should the owner be asked at all?

### When unsure, ask

This rule comes first and overrides everything after it, except the one subject the owner has
closed (legal questions, at the end of this section).

- **If you are not sure whether a decision is the owner's, ask.**
- **If a choice you would otherwise make yourself rests on a fact you could not check, or on a
  guess about what the owner wants, ask.**
- **If it would be hard to reverse, ask.** That includes anything that would be merged,
  published, or posted outside the repository before the owner sees it, and anything other work
  would build on so heavily that undoing it would take more than a small follow-up change.
- **Never decide for the owner what to buy, spend, or subscribe to:** Massive plans, cloud
  services, paid tools. Plans and tests must work with whatever the owner chooses; never write
  a plan, test, or default that assumes a purchase the owner has not made.

Asking about a genuinely uncertain point is never the failure. The failure is asking without
context (sections 3 and 4 say how to give it), asking what the owner has clearly settled, or
asking about a minor point you are confident in (decide that one and list it under
**Decided**). A point that is minor but uncertain is still a question.

### What is the owner's

These decisions are always the owner's:

- **What a user sees:** a public name, signature, default, column, error message, document, or
  table behavior. It needs a `docs/dev/decisions.md` entry.
- **Scope:** adding or dropping a feature, dataset, or planned piece of work, including new
  items in the project plan.
- **Money, accounts, access:** billing, paid plans, subscriptions, keys, and settings you cannot
  change yourself.
- **Outward actions:** merging, publishing, posting anything upstream, making the repository
  public.
- **The exceptions `AGENTS.md` reserves for the owner:** a new ignored lint rule or `noqa`
  category, `# pragma: no cover`, a `tests/test_layout.py` change, a change to a dependency
  declaration.
- **Undoing something the owner already decided.**

### What you decide yourself

Decide a choice yourself only when both of these hold:

- **You are confident in it.** Every fact it rests on was checked (section 2), and you found no
  earlier owner decision it touches. If any part of it is a guess, you are not confident.
- **The owner could easily object and have it changed afterwards**, with a small follow-up
  change.

Choices that often pass both tests: the order of PRs and how to split them, scheduling, names of
private code, internal log levels, test mechanics, internal retry counts and similar tuning that
is not a public default, and the wording of comments and of messages to other agents. Even these
must pass both tests each time; if any point of "When unsure, ask" applies, ask instead.

List every choice you made yourself under **Decided**, one line each, in your next message to
the owner, so they see it and can object. A choice left off that list is hidden from the owner,
however small it is.

**Legal questions are closed.** The owner has settled this subject for good: never raise
anything legal, about licensing, or about what users may post. Users decide what they post, and
nothing in the library depends on it.

### Before asking

Check that the question is still open. Search `docs/dev/decisions.md`, `AGENTS.md`, your memory
notes, and the owner's earlier answers. Asking again what the owner has clearly answered
("You already said … WTF are you talking about here?") wastes their time. If an earlier answer
exists but you are not sure it covers the case in front of you, ask, and quote the earlier
answer in the background.

**A question from the owner is a question, not an instruction.** Answer it. Do not change a
design because of it unless they say so. The owner, after a question was treated as a design
change: "I asked a simple question … and you ran with that to make design decisions!!!"

**If you can do it, do it.** Rebase, check that your PR's issue link will close its issue on
merge, or collect the links yourself; don't hand the owner a task you could finish. This covers
work whose outcome is already decided. When the task itself involves a choice that is the
owner's, or one you are unsure of, ask first.

## 2. Check every fact now

Check each fact in the message at the moment of writing, not from memory or from an agent's
report:

- **PR and issue state from GitHub** (`gh pr view <n>`, `gh pr list --state all`): merged or
  open, CI, mergeable, and unanswered review threads.
- **Whether a deep review was done** (an adversarial review of the whole diff by an agent other
  than its author, beyond the bots): from your own notes or the review report. With no record,
  say "not reviewed".
- **A claim that a fix is pushed:** after `git fetch`, the commit is on the remote branch and
  its diff contains the fix. A merge of `main` is not a fix.
- **Facts about Massive or Deephaven:** from a primary source (Massive's docs or spec,
  deephaven-core's code or docs), linked or quoted. Mark anything you have not checked as an
  assumption, and say how you would check it.
- **Consistency:** the recommendation agrees with the owner's earlier decisions, the rest of the
  design, Deephaven's conventions, and how the API already treats the same concept elsewhere.
  Name the earlier decision it follows, and say so plainly if it conflicts with one.
- **Simplest first:** before recommending a design, state the simplest one that meets the need,
  and why it is not enough if you recommend more. Never offer a hybrid that keeps the costs of
  both options.

## 3. The shape of a decision

Each decision stands alone, in this order:

1. **A title in plain words** that names the user-visible question, not the code. Write "What
   should `history()` do when one of several symbols doesn't exist?", not "Q4: 404 handling".
2. **Background:** the feature in one line and what a user does with it. Then define every term
   the decision uses ("flat file", "governor", "blink table"), assuming the owner has not read
   the code or the earlier discussion.
3. **The problem, with a concrete example:** a call a user makes and what happens today.
4. **Options, two to four**, with "do nothing" when that is a real choice. Each says what the
   user sees (an example call and its result), what changes in the project (which PR, files or
   docs), and what it costs or risks.
5. **A recommendation**, and why it beats the next-best option on the property that matters here.
6. **What waits on the answer**, and what you will do until then. Usually that is nothing: the
   question stays open, and silence is not consent.

Label decisions within one message only. Never refer to one by its label alone, whether "L6",
"Q3", "option B", "the governor decision", or a plan code. That applies in later messages and
reminders too. To ask again, restate the title, each option in one line, and the
recommendation.

## 4. Words

- **Answer first.** Give background in a few sentences. Leave out process narrative: which
  agent did what, which review round it was, who is merging `main`.
- **Plain English, short sentences.** A smart reader new to the project should be able to
  explain the decision to someone else after reading it once.
- **No labels, codes, or abbreviations the owner did not coin:** plan item codes, review-finding
  labels, agent names, scratchpad paths. If a thing has no plain name yet, describe it.
- **No bare answers:** never "I recommend yes, yes, no, no, no".
- **The user's view first, the implementation after.** File and function names come last, once
  the decision is clear.
- **Never forward a question from a subagent or reviewer as written.** Rewrite it to this
  standard. Answer it yourself only when section 1 lets you decide it, and list the answer
  under **Decided**.

## 5. Status reports

Answer "status?", "what needs me?" and the like in this order:

1. **Needs you now.**
   - Each PR ready to merge: its number and link, one plain sentence on what it does for a user,
     and a line, checked just now, confirming CI is green, it is mergeable, every review thread
     is answered, and the deep review is done. A PR that misses any of these is not ready; say
     which one.
   - Each open decision: in full the first time; after that, its title, the options in one line
     each, and the recommendation.
   - Any manual step only the owner can take.
2. **Decided:** one line each.
3. **In progress:** one line each, in plain words. Say whether its deep review has happened,
   what runs in parallel, and what is blocked and on what. Name each PR with a merge conflict
   or new review comments, and what is being done about it.
4. **Remaining:** how many PRs are left before the project is done.
5. **Next time you're needed:** an estimate, such as "nothing for about an hour; the request
   governor PR is next."

When nothing needs the owner, say "Nothing needs you" first, then when that will change. After
the owner answers a list of questions, repeat back the answers you recorded and say whether
anything is still open.

## 6. Check before sending

Read the message as the owner coming back after a day away, with only this message in hand.
Check, for each question and for the message as a whole, that:

- they can answer it without asking you anything;
- every term is defined;
- it is theirs to decide, or you are unsure whether it is;
- every fact was checked just now;
- every choice you made yourself is listed under **Decided**.

When the message holds three or more decisions, or any decision about public API or design,
first have a reviewer agent with no other context (the owner prefers the Fable model) read it
and list every question it could not answer from the message alone. Fix those before sending.

## Example 1: a question with its context

The message as sent (the owner replied: "This open question doesn't have enough information for
me to answer"):

> **Still open from you:** the `type=`/`id=` parameter names.
> - **A (recommended):** rename them to `ticker_type=` and `condition_id=`.
> - **B:** keep them and allow a `noqa: A002` exception.

Rewritten:

> **Two public argument names hide Python built-ins. Rename them?**
>
> Two reference calls take arguments named after Python built-ins: `m.reference.tickers(type=…)`
> filters the ticker list by kind (common stock, ETF, and so on), and
> `m.reference.conditions(id=…)` looks up one trade-condition code. Massive's docs use those
> names. In Python, an argument named `type` or `id` hides the built-in function of the same
> name inside the method, so the linter flags it (ruff rule A002). A new linter exception needs
> your approval.
>
> - **A. Rename them to `ticker_type=` and `condition_id=`.** Users write
>   `m.reference.tickers(ticker_type=dm.TickerType.CS)`, and the library still sends Massive
>   `type=CS`. Cost: one edit in the open reference-data PR and a decision entry.
> - **B. Keep `type=` and `id=`, matching Massive's docs.** Needs a permanent `# noqa: A002`
>   on both, approved by you.
>
> I recommend A: the new names say what they filter, and A avoids a permanent linter
> exception. Waiting on this: the reference-data PR. Until you answer, it stays open.

## Example 2: a choice that belonged under Decided

The message as sent (the owner replied: "L6: I have zero clue what you are suggesting here"):

> **L6. Approve the design changes that go with PR 1?** Low-level "gave up retrying" messages
> drop to INFO … the design gains the three new functions and the log-writer thread.
> *Recommended:* approve.

The owner's complaint was that the message could not be understood, not that it was a question.
What made these choices the agent's to make is that each passes both tests in section 1:

- **Confident.** The agent had checked that the three functions and the thread are private:
  no user calls them, so recording them in the design was bookkeeping. The log levels change
  only how often a failure is logged, not what a user's call returns or raises.
- **Easy to undo.** Each is a one-line change if the owner objects.

Had any of the three functions been public, or had the agent not checked, the message would
instead ask about them in full, in the shape of Example 1. Rewritten, with every choice listed:

> **Decided:**
> - A failure that has used up its retries is logged once, as an ERROR where it reaches your
>   code. The lower-level "gave up retrying" message that used to repeat it drops to INFO.
> - The design now records the three private helper functions and the background thread that
>   writes log records, which this PR adds.
