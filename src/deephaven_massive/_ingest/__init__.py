"""Ingest: the only code that holds wire clients, and the transports built on them.

- `clients`: `SessionClients`, which owns a session's wire clients and connections and provides
  the transports to the session core through its `IngestPorts` protocol;
- `flatfiles`: `FlatFileHistory`, which serves `history()` from the S3 flat files through the
  session's `FlatFileClient`;
- `ws`: the WebSocket transport, one connection per URL over `WsClient`, and the
  subscription protocol (the reconcile loop, acknowledgements, refusals, the options cap).

Layer: ingest, above the wire (`_massive`), the vocabulary (the root `_config`, `_enums`,
`_errors`, `_log_limits`, `_types`), the datasets (`_datasets`), and the tables (`_tables`), all of
which it may import; only the API above it (`_session.py`, and only `clients`) imports it. Outside
the wire package itself, it is the only code that uses the wire clients. The package defines no
names of its own: import the module that owns a name.
"""
