---
name: writing-to-massive-support
description: "Use this skill whenever you draft anything the owner will send to Massive or to Deephaven's maintainers: a support email, a reply or follow-up in a support thread, a bug report, a question, usability feedback, or a roadmap request, even a two-line reply. Not for issues in this repository (writing-a-pr-description) or messages to the owner (writing-to-the-owner)."
---

# Writing to Massive support

Support staff check what we send against their own systems, and they answer the simplest
explanation our evidence leaves open. A message has to be correct, verified, and quick to
check. One follow-up cited a request whose tickers came from a test constant written a week
earlier, mixed in two tickers support had already answered, and showed no request that returned
rows at the same time. Support read it as a reference-data delay, and the real question went
unanswered.

- **Follow the thread.** Read every message in it first, and answer the support agent's last
  message in their numbering. Don't re-raise what they acknowledged, or call it settled when it
  is only queued.
- **Every claim holds up when they check it.** Each fact rests on a request or a public page
  you can point to, dated, with its request ID where there is one. Prefer evidence from the run
  you cite; older evidence is fine if it still supports the claim and its date is given. A
  guess, a mapping we wrote, or a test constant is not evidence.
- **Show that the request worked.** An empty answer means little on its own. Put beside it a
  request of the same shape, at the same time, that returned rows.
- **Say no more than the data shows.** "All", "every", "still", and "correctly" are claims.
  Before calling Massive's docs wrong, read them in their market context: the same words can be
  right for one asset class and wrong for another.
- **Ask what they can answer:** a fact they can look up, or whether something is intended or
  planned.
- **Write to the project's standard.** The prose meets `docs/dev/user-docs.md`: the page an
  O'Reilly development editor would send to print, with none of its AI tells. Lead each item
  with its point, then the exact request, then the question. Define terms and leave out this
  library's internal names.

Before the owner sees a draft, have an agent with no other context check each claim against the
raw logs and pages, and have the calibrated editor (`writing-user-docs`, `agents/editor.md`)
grade it, reading as the support engineer who receives it. Give the owner the text, the message
it answers, and anything the checker couldn't confirm. The owner sends it; when support
answers, record the answer on the issue it touches.
