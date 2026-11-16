# 97. When another program takes the WebSocket connection, live tables fail by default

Date: 2026-11-10

Status: accepted

## Question

Massive keeps the newer of two WebSocket connections that share a key and an asset class, and
sends the older one a `max_connections` status, then close code 1008. Should the older client
reconnect, or fail its tables?

## Options

1. Reconnect, as for any drop. In three live runs (2026-11-03, and twice on 2026-11-10) two
   clients then took turns evicting each other indefinitely, each outage shorter than
   `max_outage`, and no table ever failed.
2. Fail the tables at once, naming the host, the time, and the fix.
3. A setting that chooses between them.

## Ruling

Massive runs one WebSocket cluster per asset class, forex and crypto separately though they
share a subscription, and keeps the newer of two connections on one key and cluster, closing the
older. By default, `connection_taken=dm.ConnectionTaken.FAIL`, the older program's tables fail
with an error saying to stop the other program or set `RECONNECT`; under `RECONNECT` it
reconnects after its backoff, and `m.streams()` records when it was taken. Two sessions in one
process with the same key and settings share one connection and never evict each other. The
owner ruled on 2026-11-10 that the user chooses the behavior by a setting, with the error as the
default; recorded in PR #546.

## Reason

Two programs reconnecting forever evict each other, and neither learns why.

## Consequences

A user who runs two programs on one key sees the older one's tables fail. A taken connection is
its own state, `TAKEN`, apart from a refusal at the key (`WAITING_SLOT`), which still waits
and fails at `max_outage`. `reconnect_backoff` keeps its 15-second minimum, now to limit
reconnect churn.

## Rule

`docs/configuration.md`, `connection_taken`; `AGENTS.md`, "The library never opens a
connection only to test whether one would work".
