"""Dataset semantics: what each dataset is and how a request for it is shaped.

The modules here describe datasets without performing I/O, starting threads, or calling
Deephaven:

- `catalog`: the frozen `DatasetSpec` of every dataset, and the `Catalog` that holds them by
  name;
- `filters`: the `filters=` rules, the keys a dataset takes and the values it sends;
- `schema`: the closed set of column types, the `Schema` of each dataset, and the merge of a
  kind's sources into it;
- `source_columns`: the columns one source contributes to one dataset, after the key rename;
- `column_descriptions`: the description of every dataset column, and the spelling and order
  of the transports;
- `column_reference`: the generated dataset reference pages, and their sync;
- `request`: the `Request` record a verb hands to a transport, and the other plain records of a
  request;
- `timestamps`: the declared unit of every timestamp field and its conversion to nanoseconds;
- `row_times`: the library's time columns of every row, and the stamps of bars of a day or
  longer they are filled from;
- `dates`: date parsing and the day rules of `history()` and the time-axis filters;
- `futures_sessions`: the 17:00 Chicago rule of the futures trading session, for one instant
  and for a column;
- `bar_periods`: which bars of a timespan lie wholly inside a range, and the days a bar covers;
- `symbols`: the `symbols` argument and its check, the public converters to Massive's
  spellings, and the pair readers the transports use;
- `wire_symbols`: the spellings each transport sends and takes, read and written;
- `history_source`: the window of a `history()` call, its partition dates, when a flat file
  is published, and the choice between flat files and REST.

Layer: datasets, above the wire (`_massive`) and the vocabulary (the root `_config`, `_enums`,
`_errors`, `_log_limits`, `_types`), which it may import; it imports nothing from the tables
(`_tables`), the ingest code (`_ingest`), or the API, and imports `deephaven` only for type
checking. So any of it is safe to call under the session's `registry_lock` and on any thread. The
package defines no names of its own: import the module that owns a name.
"""
