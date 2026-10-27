"""Ingest: the code that reads Massive data through the wire clients and turns it into tables.

- `flatfiles`: `FlatFileHistory`, which serves `history()` from the S3 flat files through the
  session's `FlatFileClient`.

The wire clients themselves live in `_massive`; dataset semantics come from `_datasets`, and
the tables and their liveness from `_tables`. The package defines no names of its own: import
the module that owns a name.
"""
