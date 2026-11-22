# 37. One snapshot read checks for delayed data before a real-time connection

Date: 2026-10-31

Status: accepted

## Question

A user on a delayed plan opens a real-time stream without setting `delayed_subscriptions`. Does
the library find out before connecting, or only from Massive's refusal of the stream?

## Options

1. Connect, and let Massive's refusal of the stream fail it.
2. Read one snapshot first and fail before connecting when its data is labeled delayed.

## Ruling

Option 2. Before a subscription's first real-time connection (decision 79), the library reads
one snapshot of a fixed liquid symbol; currencies have no delayed feed and are not checked. If
the response's `status` or `timeframe` field says `DELAYED`, the call fails naming
`delayed_subscriptions` as the fix. Anything else connects real-time, even if the snapshot read
itself was refused; a refusal of the stream then fails the stream as usual. From the design
documents, condensed into records in PR #514, dated 2026-10-31; no owner words are on record,
and the owner kept the rule on 2026-11-16.

## Reason

A delayed-plan user gets a clear error naming the setting before connecting.

## Consequences

Each subscription costs one REST request per session, counted against the rate limit. The read
uses the data's own delayed label, not a table of plans.

## Rule

`docs/configuration.md`, `delayed_subscriptions` and `websocket_hosts`.
