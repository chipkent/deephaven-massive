# 128. Error messages say what happened, whose answer it is, and what to do next

Date: 2026-11-17

Status: accepted

## Question

A user who passes `adjusted=True` to a read Massive refuses, or a ticker the library cannot
take, sees a message written in whatever form its author chose: facts joined by semicolons,
internal words such as `generation` or `topic`, the fix in parentheses or missing, and advice
no test ever ran. Should every error message follow one written rule, and where should that
rule and its procedure live?

## Options

- A rule in `AGENTS.md` with a new skill, `writing-error-messages`, holding the rubric and the
  procedure: one more skill to keep current.
- A section of `writing-docstrings`: that skill would gain a second subject with a different
  trigger and reader.
- The `_debug_log.py` docstring: its subject is the log, and most messages are exceptions.

## Ruling

On 2026-11-17, in a Claude Code session, the owner approved adding the rule and the skill. The
rule: an error message says what happened with its values, whose answer it is, and what to do
next. It reports Massive's refusals in one fixed form naming the call, the HTTP status, and
Massive's text; uses only words a user knows, public names and never internal ones; puts the
fix last as an imperative sentence; uses sentences rather than semicolon chains; and a test
runs every call it suggests and pins its full text.

## Reason

The owner gave no reason with the approval. The issue it answers, #758, asks that a human or
an AI agent reading any message understand the problem and the fix at once.

## Consequences

Every message a change adds or rewords takes more work: a test that runs its advice and pins
its whole text, and a grade of what a user sees. Messages written before the rule keep their
form until rewritten.

## Rule

`AGENTS.md`, Code and tests, "An error message says what happened".
