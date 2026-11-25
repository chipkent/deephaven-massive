# 136. A pinned WebSocket host is used as given, even for a subscription listed as delayed

Date: 2026-11-25

Status: accepted

## Question

A user whose key Massive serves index data delayed lists `indices` in `delayed_subscriptions`
and pins `indices` to `business.massive.com`, the one host Massive documents for index values.
`dm.MassiveConfig` raised `ValueError`, saying a listed subscription streams from the delayed
feed and asking for the listing or the pin to go. Without the listing, the library refuses the
key's index snapshots, which Massive labels delayed; without the pin, index values have no
host. Should the library keep refusing the pair?

## Options

1. Remove the refusal: the pinned host is used as given, and when Massive does not serve the
   key there, its refusal fails the stream with Massive's text. The two other checks of the
   pair stay.
2. Keep it: such a key cannot stream index values.

## Ruling

Option 1, ruled by the owner on 2026-11-25 in the coordinating chat session. A host pinned in
`websocket_hosts` is used as given, whether or not its subscription is in
`delayed_subscriptions`. A `delayed.` host still needs its subscription listed, and forex and
crypto still take none. A pinned host's recency is what Massive labels its class's snapshot,
never what the listing implies.

## Reason

The owner accepted it as a simplification: the library drops a check of its own and lets
Massive judge the pin. Decision 122 gives the evidence: the library cannot know what Massive
serves a key, and Massive may serve more later, so a refusal built in blocks a setup that might
work.

## Consequences

A pin of a host that does not serve the key fails at the stream, with Massive's refusal,
instead of at `dm.MassiveConfig`. A pinned host's label costs one REST request per class and
session even when its subscription is listed, where the listing used to cost none.

## Rule

`src/deephaven_massive/_config.py`, the `websocket_hosts` docstring of `MassiveConfig`.
