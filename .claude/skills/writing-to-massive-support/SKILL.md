---
name: writing-to-massive-support
description: "Use this skill whenever you draft anything the owner will send to Massive or to Deephaven's maintainers: a support email, a reply or follow-up in a support thread, a bug report, a question, usability feedback, or a roadmap request, even a two-line reply. Not for issues in this repository (writing-a-pr-description) or messages to the owner (writing-to-the-owner)."
---

# Writing to Massive support

The reader is a support engineer who knows Massive's API but not this library. They will paste
our requests into their own tools, compare the answers with what we wrote, and reply to the
simplest explanation our evidence leaves open. Each message costs them time and costs the owner
credibility, so it should be worth sending, true when checked, and easy to act on.

One follow-up shows what goes wrong. It asked about missing futures spreads using tickers copied
from a week-old test run, mixed in tickers support had already answered, showed no control, and
gave requests with placeholders in them. Support read it as a reference-data delay. The reply
that worked showed one spread on one finished day: its trades from the trades endpoint, the same
for a control combo, then the contracts list and the snapshot returning only the control. Each
request was short, complete, run exactly as written, and followed by its trimmed response.

## Before writing: is the question sound?

Think like a practitioner in the market concerned. What is the instrument or field, who uses it,
why would Massive publish it, and is there a reason it might be intended (an FX Link spread has
an OTC spot leg, so leaving it out of a futures list may be deliberate)? Then rule out the easy
explanations support would reach for: our own request, another spelling, a delay, an
entitlement, or a known issue already in the thread. A question that survives this is worth
their time; one that doesn't would send them on a chase.

## The evidence

Show the problem with the smallest complete example: a ticker or two, one date that has already
ended, and a control in the same request where possible. Each request is a short REST line
support can run as written, with every parameter and no placeholder, followed by its request ID,
time, and the response, trimmed to what shows the point and marked as trimmed. Show the thing
existing where Massive has it, not only missing where it doesn't. List any further cases once,
rather than in long URLs. A problem in a flat file, a WebSocket stream, or a local bug is shown
the same way, by the shortest script or steps that reproduce it and their output.

Every fact rests on a request, a public page, or a log you can point to, dated. A test constant,
an earlier run's inputs, or our own mapping is a guess, not evidence. The owner runs any request
or script that needs a key, exactly as the message will show it, before it is sent (`AGENTS.md`,
Limits).

## The message

Answer the support agent's last message in their terms: don't re-raise what they acknowledged,
and say plainly when our last message was wrong. Lead with what we found, then the evidence,
then the question. A first report of several problems gets a section for each; a follow-up gets
only what it needs. Length follows the content, not the length of their reply.

Ask only what the owner wants asked: a fact they can look up, or whether something is intended.
A promise on the owner's behalf, a request to escalate, or any other action is the owner's to
choose. The prose meets `docs/dev/user-docs.md`: define terms, no AI tells, and never this
library's code as a reproduction.

## Handing it to the owner

The owner may not remember the details, so open with the problem in plain words: what users
lose, what Massive has said so far, and what this message asks. Then the full text, ready to
paste, and anything not yet checked. Before that, have an agent with no other context check each
claim against the raw logs, and the calibrated editor (`writing-user-docs`, `agents/editor.md`)
grade it as a message to Massive support, given the thread and the evidence. When something must
be checked before sending, give the owner the exact command to run, what it costs, and what it
settles. The owner sends it; record what was sent, and later the answer, on the issue it
touches.
