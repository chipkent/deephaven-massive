"""Ingest: the only code that holds wire clients, and the transports built on them.

- `clients`: `SessionClients`, which owns a session's wire clients and connections and provides
  the transports to the session core through its `IngestPorts` protocol;
- `flatfiles`: `FlatFileHistory`, which serves `history()` from the S3 flat files through the
  session's `FlatFileClient`;
- `history_reads`: history reads from the flat files, with REST serving the days and files
  they cannot;
- `futures_calendar`: the futures session calendar, from Massive's schedules, that dates each
  streamed futures row's session;
- `rest`: the REST transport, which runs a call's requests on the session's `RestClient`,
  with `rest_requests` (the requests of a call) and `rest_rows` (page rows to batches);
- `symbol_reads`: reads by symbol, which give each symbol of a `fetch()` or `history()` its
  rows or its own failure;
- `rate_limits`: Massive's REST rate limits on the library side: the shared governors, their
  warnings, and their diagnostics;
- `recency_labels`: the real-time or delayed labels Massive puts on REST responses, and the
  refusal a delayed one causes;
- `shared_lookup`: a session's lookups that one caller makes while the others wait for it;
- `host_decision`: which WebSocket host serves an asset class and channel family, and the
  session's cache of those decisions;
- `ws`: the WebSocket transport, a session's registry of streams on shared connections;
- `ws_connection`: one connection per URL over `WsClient`, and the group that shares
  connections between sessions;
- `subscriptions`: the subscription protocol of one connection (the reconcile loop,
  acknowledgements, refusals).

Layer: ingest, above the wire (`_massive`), the vocabulary (the root `_config`, `_enums`,
`_errors`, `_log_limits`, `_types`), the datasets (`_datasets`), and the tables (`_tables`), all of
which it may import; only the API above it (`_session.py`, and only `clients`) imports it. Outside
the wire package itself, it is the only code that uses the wire clients. The package defines no
names of its own: import the module that owns a name.
"""
