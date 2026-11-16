# 122. A stream Massive documents only in real time opens, and Massive's answer decides

Date: 2026-11-16

Status: accepted

## Question

Decision 96 had the library refuse a real-time-only stream (fair market value, the options
admin messages) before asking Massive. `m.stocks.fmv("AAPL").blink()` in a session listing
`dm.Subscription.STOCKS` as delayed raised `ValueError`, although FMV has its own host and the
listing never applies to it. `m.options.admin_messages().blink()` on a key whose options data
Massive labels delayed raised `EntitlementError` saying the key's plan does not include the
dataset. Should the library keep refusing these itself?

## Options

1. Remove both refusals: the stream opens on its documented host, or its class's feed, and a
   refusal from Massive fails the table with Massive's text and a link to the dataset's page.
   Admin messages on a delayed key then take two steps: the ordinary delayed-data error, then
   Massive's answer on the delayed feed.
2. Keep both: the library refuses before asking, including setups Massive may serve.

## Ruling

Option 1, ruled by the owner on 2026-11-16 in the coordinating session. The library doesn't
refuse a real-time-only stream (fair market value, the options admin messages) before asking
Massive. The stream opens on its documented host or its class's feed, and Massive's refusal
fails the table with Massive's text and a link to the dataset's page. Whether a key may stream
a dataset is learned from Massive's answer.

## Reason

The library cannot know what Massive serves a key, and Massive may serve more later; a refusal
built in blocks a setup that might work.

## Consequences

Such a stream may now open and fail on Massive's refusal instead of failing at the call. An FMV
connection opens beside a delayed connection of the same class, which no live run had checked
when this was decided.

## Rule

`src/deephaven_massive/_ingest/host_decision.py`, "the library never refuses it first".
