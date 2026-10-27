"""Dataset semantics: what each dataset is and how a request for it is shaped.

The modules here describe datasets without performing I/O or starting threads:

- `catalog`: the frozen `DatasetSpec` of every dataset and the filter-parameter rules;
- `schema`: the closed set of column types, the `Schema` of each dataset kind, and the
  flattening of REST models into rows;
- `timestamps`: the declared unit of every timestamp field and its conversion to nanoseconds;
- `dates`: date parsing and the day rules of `history()` and the time-axis filters;
- `symbols`: symbol normalization across the WebSocket, REST, and flat-file spellings;
- `history_source`: the partition dates of a window and the choice between flat files and REST.

The Deephaven-side machinery (tables and their liveness) lives in `_tables`, and the
code that holds wire clients lives in `_ingest`. The package defines no names of its own:
import the module that owns a name.
"""
