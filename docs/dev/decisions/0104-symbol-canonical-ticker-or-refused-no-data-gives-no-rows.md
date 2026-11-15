# 104. A symbol is Massive's canonical ticker or is refused, and a ticker with no data gives no rows

Date: 2026-11-11

Status: accepted

The owner's intent: symbol handling follows a few general rules, and the same circumstances get
the same behavior whatever the verb, the transport, or where the symbols came from.

- **Form.** A symbol is Massive's canonical ticker for its class. Any other text is the user's
  mistake: in code it raises `ValueError` naming the form to write or the converter, and in a
  table the row follows `noncanonical_symbols`. The converters are strict: `RAISE` by default,
  with `LOG` and `IGNORE` returning None (`dm.canonicalize_symbols` keeps `LOG`).
- **Existence.** A canonical ticker Massive has no data for gives no rows, under one setting,
  `unavailable_symbols`. A crypto pair Massive's crypto ticker list doesn't hold is the same
  case as an HTTP 404: the list is only how the library learns it before sending.
- **Reachability.** A request the library needs and cannot complete is a transport failure,
  never "no data". An unreadable crypto ticker list fails a read as any failed request does,
  and in a stream it is an outage: the pairs wait, retried on the reconnect backoff, until
  `max_outage`.
- **Read-back.** Every published `Symbol` is canonical; Massive's data the library can't read
  is a library bug, under `unreadable_symbols`.

Why: the library should not invent causes Massive doesn't have, and a user should be able to
predict what happens from the cause alone.

Alternatives weighed: send any text and report Massive's answer (an unknown ticker gets an empty
200, so a typo looks like a quiet day); convert common spellings such as `BRK-B` (a wrong guess
serves another ticker's data); a separate `unlisted_symbols` setting for a pair missing from the
crypto list, raising at the call for pairs in code (two settings for one cause, and behavior
that depended on where the pair came from).
