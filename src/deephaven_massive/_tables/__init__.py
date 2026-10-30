"""The Deephaven side of a session: its tables, their liveness, and the library threads.

- `session_core`: `SessionCore`, the per-session state behind every query (the registries
  under `registry_lock`, the stream registry, the live set, the closers, and `close()`);
- `transports`: the transport ports a verb calls through, served by the ingest side attached
  to the core, and the values that cross them (`StreamHandle`, `HostDecision`);
- `execution_context`: `entered()` and the checks of where a call runs (a real execution
  context, the script session's query scope, not an update-graph thread, the update graph's
  lock);
- `liveness`: liveness scopes, result records, `QueueWorker`, and the reaper;
- `diagnostics`: the `connections()` and `streams()` tables and their writer thread;
- `columns`: the Deephaven type of each column type and the column definitions of a schema;
- `watchlist`: the check of a symbol table's rows and the watch that follows a reactive one;
- `symbols`: `canonicalize_symbols`, the table form of the symbol converters;
- `stream`: live streams' publishers, the flush thread, and the shared-batch fan-out.

Layer: tables, above the wire (`_massive`), the vocabulary (the root `_config`, `_enums`,
`_errors`, `_log_limits`, `_types`), and the datasets (`_datasets`), which it may import; it never
imports the ingest layer (`_ingest`) or the API, and never holds a wire client, so it is tested
without a fake server. The package defines no names of its own: import the module that owns a
name.
"""
