"""The Deephaven side of a session: its tables, their liveness, and the library threads.

- `core`: `SessionCore`, the per-session state behind every query (the registries under
  `registry_lock`, the stream registry, the closers) and the transport ports a verb calls
  through, served by the ingest side attached to it;
- `liveness`: liveness scopes, `entered()`, result records, `QueueWorker`, and the reaper;
- `diagnostics`: the `connections()` and `streams()` tables and their writer thread;
- `columns`: the Deephaven type of each column type and the column definitions of a schema;
- `watchlist`: the canonical view of a reactive symbol table.

Layer: tables, above the wire (`_massive`), the vocabulary (the root `_config`, `_enums`,
`_errors`, `_types`), and the datasets (`_datasets`), which it may import; it never imports
the ingest layer (`_ingest`) or the API, and never holds a wire client, so it is tested
without a fake server. The package defines no names of its own: import the module that owns a
name.
"""
