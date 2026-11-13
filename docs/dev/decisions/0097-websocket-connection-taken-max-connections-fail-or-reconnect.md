# 97. A taken connection fails its live tables by default: `MassiveConfig.connection_taken`

Date: 2026-11-10

Status: accepted

Massive keeps the newer of two WebSocket connections with one key for an asset class. In three live
runs (2026-11-03 and twice on 2026-11-10), 2.6 to 8.3 s after the newer connection authenticated,
the older, authenticated socket got the status `max_connections` ("Maximum number of websocket
connections exceeded. You have reached the connection limit for your account...") and then close
code 1008. Treated as `WAITING_SLOT`, the older client reconnected after `reconnect_backoff` and
evicted the newer, so two clients took turns indefinitely, each outage (about 35 s) shorter than
`max_outage`, and no table ever failed. A `max_connections` reply to the key has never been
observed. The wire client now reports a `max_connections` status on an authenticated socket as its
own state, `TAKEN`, distinct from a refusal at the key (`WAITING_SLOT`), and the library applies
`dm.ConnectionTaken`. Under `FAIL`, the default, the tables fail at once with a `MassiveError`
naming the host, the time, and the fix (stop the other program, or set `RECONNECT`), through the
flush thread's normal failure path. Under `RECONNECT`, the connection reconnects after its reconnect
backoff, which takes it back; it shows `RECONNECTING`, and each table's `Error` says when it was
taken and when it is taken back. The setting is applied per table, from its session, and is no
connection setting (`CONNECTION_SETTINGS`): sessions differing only in it share connections, so they
never evict each other. When a shared connection is taken, the tables of `FAIL` sessions fail; it
reconnects only while a table of a `RECONNECT` session remains, and otherwise closes, showing
`CLOSED`. A plain drop still reconnects, and a refusal at the key still waits (`WAITING_SLOT`) and
fails at `max_outage`. `reconnect_backoff` keeps its 15 s minimum, now for limiting reconnect churn,
not because Massive refuses a sooner reconnect.
