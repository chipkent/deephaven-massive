# 96. OPRA's admin messages are a stream without symbols, kept as sent

Date: 2026-11-09

Status: accepted

`m.options.admin_messages()` serves Massive's `options/AD` channel: OPRA's administrative messages
and FLEX trade notices, one row per message with `Exchange`, `MessageId`, `SequenceNumber`, the SIP
time as `Timestamp`, and the text as `Message`. Massive sends them per exchange, not per contract,
so the call takes no symbols, subscribes every exchange (`AD.*`), and its rows have no `Symbol`. A
dataset marks this with `names_symbols=False`: its query is a plain `StreamQuery` with `blink()`,
`append_only()`, and `ring()`, and no `last_by()`, whose default key is `Symbol`. Every stream whose
rows name their symbol is a `SymbolStreamQuery`, which adds `last_by()`; `StreamHistoryQuery` builds
on it. The text is never parsed: Massive documents no format, its sample does not match OPRA's FLEX
layout, and a parse would guess at Massive's data. The channel is real-time only, with no history; a
dataset Massive streams only in real time (`real_time_only`, also every fair-market-value stream)
refuses a session that reads its subscription from the delayed feed with `ValueError`, and a key
whose data Massive labels delayed with an `EntitlementError` that points to the dataset's page of
plans. A subscription Massive refuses fails the table with the same pointer.
