"""The Deephaven side of a session: its tables, their liveness, and the library threads.

- `liveness`: liveness scopes, `entered()`, result records, `QueueWorker`, and the reaper;
- `diagnostics`: the `connections()` and `streams()` tables and their writer thread.

Dataset semantics come from `_datasets`, and wire clients are held outside this package. The
package defines no names of its own: import the module that owns a name.
"""
