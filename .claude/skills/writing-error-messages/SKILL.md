---
name: writing-error-messages
description: "Use this skill whenever you write or change text that reports a failure in this repository: the message of a `raise`, a log call at WARNING or above, a live table's `Error` in `m.streams()`, a connection's `LastError`, a `Detail` in `m.problems()`, or a builder of such text (`_errors.py`, `_error_policy.py`, the REST response's error context). That includes 'add a check for this', 'raise a clearer error', 'fix this message', and a review finding about a message. It gives the seven-item rubric, how each surface applies it, the builders to call, and the procedure: render the message with real values first, run every call it suggests, pin its full text, and grade what a user sees. Not for docstrings (writing-docstrings), the troubleshooting page itself (writing-user-docs), or what a log record must hold and at which level (the `_debug_log.py` docstring)."
---

# Writing error messages

`AGENTS.md` sets the rule (Code and tests, **An error message says what happened**). This skill
holds the rubric and the procedure. A message has two readers: a user in the Deephaven console
reading a traceback, and an AI agent reading a log. Each must learn from the message alone what
happened, whose answer it is, and what to do next.

The wording is not the public contract: the exception classes and their attributes are, and the
`_errors.py` docstring tells users to branch on `status` instead of parsing messages. A rewording
lists itself in the PR and updates every `docs/troubleshooting.md` row that quotes it.
`tests/test_errors.py` checks every message site's layout (no semicolon, newline, or
`bounded_list`, at most 400 literal characters); the rest of the rubric is the grader's.

## The rubric

A message passes when all seven hold.

1. **What happened, with the values.** The first sentence names what the reader passed or asked
   for, with its value quoted (`'aapl'`, `max_outage=-5`), or the call Massive answered.
2. **Whose answer it is, in fixed words.**
   - Massive's, whatever the status:
     `Massive answered <call> with HTTP <status> (request_id <id>): "<Massive's text>"`, the
     parenthesis only when Massive sent a `request_id`. One verb for every status, so no site
     judges whether a 429 or a 503 is a refusal; the exception class says the rest.
   - No answer: `No answer from <host> after <n> tries: <error>`.
   - The user's code or settings: the fix, and never a bug report.
   - The library's: the `LIBRARY_BUG` sentence. Beside it goes a user step only when the user
     needs one to go on: the per-value `on_error` setting the failure policy prescribes, or a
     new session.
3. **Why, only if it changes the fix.** One sentence, after the what and before the fix. A cause
   the library cannot know is a possibility ("The usual cause is ..."), never asserted.
4. **What to do next, last, as an imperative sentence** with the exact text to type: `Pass
   adjusted=False.`, `Set rest_requests_per_minute in dm.MassiveConfig.` Alternatives join with
   "or". A pointer to a diagnostics table is a next step; with none possible, say so (`Nothing to
   do: the library reconnects.`). A fix the problem makes obvious is not repeated. A site that
   can tell two causes apart raises two messages.
   - Link a troubleshooting section, by full GitHub URL, only when no call can be named as the
     fix; a test checks that its anchor exists.
   - Never suggest upgrading a plan: the library knows no plans (decision 114). Massive's own
     text, quoted, says what it says.
5. **The reader's words.** A word the reader can type or see is allowed: a public name,
   argument, setting, enum member, column (`NoAckTopics`, `LastError`), or diagnostics table.
   Other internal words are not: `generation`, `topic` (say "subscription"), `flush`, `reaper`,
   `patience`, `probe`, `envelope`, `fanout`, `spec`, `upstream`, a session's `#1`, or a REST
   path template with braces. Write a setting as the user does
   (`dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE)`), and a list as prose (`X:FOOUSD and
   X:BARUSD`, `12 symbols, the first 3: ...`), never `2 [a, b]`.
6. **One form.** Sentences, each ending with a period, the last included. The one exception is
   the type refusal `<name> must be <expected>, not <actual>`, a lower-case fragment with no
   period, as Python's own are. A value refusal reads `<name>=<value!r> is not <expected>.` A
   sentence starts with a capital unless its first word is a value or a code name. No
   semicolons. No parenthetical asides: parentheses appear only in code the reader types and
   around a unit, a local time, or a `request_id`. No newline and
   no code block: link the docs section that holds the code.
7. **Advice that works.** A test runs every call, setting, or value the message tells the reader
   to use, on the case the message describes, and checks the outcome. A test pins the message's
   full text.

## By surface

- **Exception**: the rubric as written. Prefix the dataset and verb (`stocks.trades history:`)
  or the call (`m.reference.news(...).poll():`) when the traceback doesn't already show it.
- **Log record**: the same sentences on one line. The handler already prints the thread and the
  logger, so the prefix is the thing the record is about (`wss://socket.massive.com/stocks:`). A
  fix may point to a diagnostics table or a docs section; a tutorial belongs in the docs.
- **Table cell**: a failed table's `Error` in `m.streams()` and a connection's `LastError` in
  `m.connections()` are the exception's text. A `Detail` in `m.problems()` is a label:
  what happened and the values, ending in `(at <where>)`, with no fix, since the row's `Cause`
  leads to the fix in `docs/troubleshooting.md`.

## Builders

Call the builder that makes a form instead of writing its sentences by hand, so one fix to the
builder reaches every site: `library_bug`, `unmatched_form`, `required`, and
`required_instance` for a library defect, `user_defect` for the user's, `session_closed` for a
call on a closed session and `NEW_SESSION` after a sentence of your own (`_errors.py`),
`policy_hint` for the `on_error` setting of every failing cause (`_error_policy.py`),
`prose_list` for a list (`_log_limits.py`), and the REST response's `RequestTarget.answered`
for Massive's answer and `RequestTarget.unreadable` for one the library could not read. One
test pins each builder's whole output.

**Exempt** from the whole rubric, the advice test of item 7 included: a developer or contract
check that fails at import or in `scripts/refresh.py`. Its reader is a contributor holding the
source.

## Procedure

1. **Render it first.** Write the message as the reader will see it, with real values, before
   the code that builds it. Read it against the rubric, item by item.
2. **Run its advice.** Write the test that does what the message says on the case it describes
   (`writing-tests`): converts the value, passes the setting, calls the method. A message that
   names `m.stocks.news()` when news lives under `m.reference` fails here, not in a user's
   console.
3. **Pin the whole text.** Assert `str(error) == ...`, or `match=` anchored with `^...$` around
   `re.escape(...)`, with real values; a variable list is pinned with two items. A fragment pin
   lets the rest of the message rot.
4. **Grade what a user sees.** For a message a user meets at the call, a fresh grader grades
   the rendered text against this rubric with `.claude/skills/writing-user-docs/agents/editor.md`;
   fix what it finds before the push.
5. **Update the docs.** A new message a user can meet gets its row in `docs/troubleshooting.md`
   (`writing-user-docs`); a reworded one updates every row that quotes it
   (`git grep -n '<phrase>' -- README.md docs/`).

## Sources

The rubric takes the majority view of Google's and Microsoft's error-message guidelines,
Nielsen Norman Group's, the rustc dev guide, Elm's "Compiler Errors for Humans", and CPython's
own messages. Where they split (final periods, where the fix goes), it takes sentences, with
the fix last in the same message.
