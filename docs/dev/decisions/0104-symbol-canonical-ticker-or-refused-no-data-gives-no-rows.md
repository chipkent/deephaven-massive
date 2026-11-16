# 104. A symbol is Massive's canonical ticker or is refused, and a ticker with no data gives no rows

Date: 2026-11-15

Status: accepted

## Question

Should what the library does with a symbol depend on the verb, the transport, or where the
symbols came from? Should a crypto pair missing from Massive's crypto ticker list have a setting
of its own that raises at the call? What should a converter do with text it can't convert?

## Ruling

The owner ruled, in the coordinating chat session on 2026-11-15, that the same circumstances get
the same behavior. That session also settled two cases. A crypto pair missing from Massive's
crypto ticker list is the same case as an HTTP 404, so it has no setting of its own.
`dm.canonical_symbol` and the per-class converters raise by default. Under `LOG` or `IGNORE`
they return None.

Two earlier rulings stand. A symbol is Massive's canonical ticker, ruled on 2026-10-28
(decision 58). A ticker Massive has no data for is not an error, ruled in the chat session of
2026-11-11.

The owner ruled the principle. The four rules below are how the library applies it.

### Form

A symbol is Massive's canonical ticker for its class. In code, any other text raises
`ValueError` naming the form to write or the converter. In a table, each such row follows
`noncanonical_symbols`. A table whose every row is non-canonical raises `ValueError` at the
call, whatever the setting. `dm.canonicalize_symbols` defaults to `LOG`.

### Existence

A canonical ticker Massive has no data for gives no rows by default. `unavailable_symbols`
governs an HTTP 404. It also governs a crypto pair missing from Massive's crypto ticker list,
for a request or stream that needs the pair's two currencies. Under `RAISE`, either one fails
whatever met it. An empty 200 is no rows under any setting.

### Reachability

A request the library needs and can't complete is a transport failure, never "no data". When
Massive's crypto ticker list can't be read, a read fails as it does for any failed request. A
stream treats it as an outage. Its pairs wait while the list is read again on the reconnect
backoff. After `max_outage`, the stream fails. A failure no retry can fix, such as a rejected
key, fails the stream at once.

### Read-back

Every published `Symbol` is canonical. Data from Massive that the library can't read is a
library bug, under `unreadable_symbols`.

## Reason

The library shouldn't invent causes Massive doesn't have, and a user should predict what
happens from the cause alone. Three alternatives lost:

- Send any text and report Massive's answer. Massive answers an unknown ticker with an empty
  200, so a typo would look like a quiet day.
- Convert common spellings such as `BRK-B` without being told whose they are. A wrong guess
  serves another ticker's data.
- Keep a separate `unlisted_symbols` setting that raised at the call for pairs in code. One
  cause had two settings, and the behavior depended on where the pair came from.
