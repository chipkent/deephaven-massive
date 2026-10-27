"""The Deephaven side of a session: its tables, their liveness, and the library threads.

- `core`: `SessionCore`, the per-session state behind every query (the registries under
  `registry_lock`, the stream registry, the closers) and the transport ports a verb calls
  through;
- `liveness`: liveness scopes, `entered()`, result records, `QueueWorker`, and the reaper;
- `diagnostics`: the `connections()` and `streams()` tables and their writer thread.

Dataset semantics come from `_datasets`; the transports that hold wire clients live in
`_ingest`. The
package defines no names of its own: import the module that owns a name.
"""
