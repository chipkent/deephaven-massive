## 14. Publishers, fan-out, and failure semantics

- **One stream per unique (dataset, channel, symbol key).** The symbol key is the sorted tuple
  of canonical symbols, `ALL`, or `id(table)` for a reactive table. A second verb on the same
  query, or an equal query, reuses the stream: `m.stocks.quotes(s).last_by()` twice costs one
  publisher and two derived tables. A futures `venue` is not part of the key.
- **Reactive symbol tables** are held strongly by the stream and managed in the stream scope;
  the stream's record also holds the watchlist listener strongly until the stream shuts down,
  and a failed watchlist fails its stream ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)).
  Two Python wrappers of one Java table have different ids and do not share a stream.
- **Library threads never create refreshing tables outside an explicit scope.** Refreshing
  tables are created only on the caller thread inside a result scope or a stream scope, and in
  the session's diagnostics scope. The flush thread creates only static tables.
- **The library never uses input tables.** They are for people editing data by hand, such as a
  watchlist typed in the web UI. Every table the library writes is a `blink_publisher` blink
  table or a shape of one; a keyed one is `last_by` on its keys (decision 92).
- **Every blink table is created under the UG shared lock.** `blink_publisher` holds the shared
  lock (the Java lock, as `release_scope` takes it) around `table_publisher`, so no update cycle
  runs while the table is created. Deephaven stamps a new table with the current cycle's step,
  and the creation stamp counts as the table's notification for that step: a table registered
  while a cycle runs can be refreshed in that cycle, and a first change delivered in the same
  step is fatal (Deephaven exits the process). Keyed input tables were seen to fail this way;
  a publisher's blink table is registered the same way, so the lock is a precaution for every
  blink table. It suffices because a cycle holds the exclusive lock from its start until it
  has listed its sources. A thread that already holds either lock (a console command) takes it
  again at once.
- Each stream owns one
  `blink_publisher(name=f"massive.{dataset}#{stream_id}", columns=<dataset schema>, on_shutdown=..., defer=<the session's reaper>)`
  (a `table_publisher` whose failure pins its table, section 6.1), created inside the stream scope under the session's captured `ExecutionContext` by
  `StreamTransport.open_stream(spec, request, channel, *, stream_id)`. The session mints
  `stream_id` and has registered the stream before the call, so the transport may call
  `SessionCore.fail_stream(stream_id, ...)` as soon as the publisher exists; the callback calls
  `SessionCore.stream_shut_down(stream_id)` before it releases the topics; the returned
  `StreamHandle` carries `stream_id`. `blink()`
  returns a described copy of the blink table, a new table managed in its own result scope
  ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)); `append_only()` is
  `blink_to_append_only(blink)`; `ring(n)` is `ring_table(blink, n)`; `last_by()` is as in
  [section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs); each shape is described last
  ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)).
- **Fan-out per flush.** Per connection and per channel, the flush thread drains the channel's
  row buffer once. The rows arrive as tuples of the source's fields; the flush turns them into
  columns in one call, renames them
  (`rename` in `_datasets/source_columns.py`), converts them (`_datasets/timestamps.py`), builds one
  Arrow batch in the dataset schema, and fills its time columns (`row_times.TimeRule`), each
  row's `TimestampInsert` the instant its frame arrived (each frame is buffered with that
  instant) and a futures row's `Date` corrected by the connection group's schedule calendar
  ([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)); a schedule miss sets the `Error` of
  the streams that receive the contract and fails nothing. The timestamp bound is applied as a vectorized mask: a row whose time column fails it is dropped
  from the batch and counted, and any other value that fails it is null
  ([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)); neither fails a stream. Rows the client dropped never
  enter a buffer. The batch is converted once with
  `deephaven.arrow.to_table`: one Python-to-Arrow-to-Java conversion per channel per flush.
  Then, for each stream subscribed to that channel: `ALL` streams get `publisher.add(batch)`;
  symbol-list streams get the part of the batch they hold. A symbol stream that holds none of
  the batch's distinct symbols gets nothing, and one that holds all of them (and the batch has
  no row without a valid symbol) gets the whole batch with no filter, so the common
  single-stream case costs one `add`. One that holds exactly one of them gets that symbol's
  rows from `batch.partition_by("Symbol")`, built once per flush the first time a stream needs
  it and shared by every stream of the flush (`SharedBatch`), when the batch holds at least
  5,000 rows and at least 8 of the flush's streams take one symbol each. A filter scans the
  whole batch (about 0.25 ms at 2,000 rows, 1.5 ms at 20,000), while the partition costs work
  for each distinct symbol of the batch and then about 0.3 ms per stream, so below either
  threshold each such stream filters; a non-gating benchmark records both paths. Every other
  stream that holds some of them gets
  `publisher.add(batch.where_in(stream.filter_table, "Symbol"))`, which keeps arrival order
  across its symbols, where `filter_table` is a cached static one-column table rebuilt only when
  the stream's symbol set changes. Target: Python work is O(rows) per channel per flush plus a
  set intersection per stream; Java work is at most one partition per flush, which grows with
  the batch's distinct symbols, plus one lookup or filter and one `add` per stream, with no
  Python per-row cost. A non-gating
  benchmark test records warmed median rows per second for 1, 10, and 100 streams at 200,
  2,000, and 20,000 rows per flush, against one-pass per-stream Python grouping. The default
  run records only 10 streams at 2,000 rows; the other points are marked `benchmark` and
  deselected by default, and `-m benchmark` runs them.
- Each flush applies the failures queued before it, then publishes, so a failed stream gets no
  row after its failure; rows buffered for it in that flush are dropped. A batch that cannot
  be converted or published fails that dataset's streams on the channel, with a `MassiveError`
  naming the cause, and the flush goes on with the other channels.
- `TablePublisher.add` is safe from the flush thread: the Java side snapshots the static batch
  and guarantees non-interleaved adds. `on_flush_callback` is not used (it blocks the update
  cycle).
- After its `add` calls, each flush reports once to each session whose streams it published:
  `SessionCore.update_streams({stream_id: StreamUpdate(rows_published=..., ...)})`, where
  `StreamUpdate` is a frozen keyword-only record whose fields left as None keep their values.
  One short critical section under `registry_lock`, about 20 times a second per connection at
  the default `flush_interval`; it updates the rows of every stream still in the registry
  (writing a row only once the stream has opened), ignores ids that have left it, and never
  changes `State` (only `fail_stream` does). An `error` there is a warning, and
  `StreamUpdate(clear_error=True)` sets `Error` back to null, for example once the options
  contract cap no longer defers anything. Neither touches the `Error` of a `FAILED` or `CLOSED`
  row: the cause of a stop is final, so a flush or a poll cycle that races `close()` or a
  failure never replaces it. `poll()` rows take their `StreamId` from the same
  allocator, `SessionCore.next_stream_id()`, so the two kinds never collide.
- A failure fails exactly the streams whose topics the server rejected. Nothing tears down a
  whole URL on an entitlement error. A failed stream's key is unregistered at once
  (`SessionCore.fail_stream`), so the next verb with the same key opens a fresh stream rather
  than returning the failed table. A stream's registry record lives until its holds are
  released and its blink table's `on_shutdown_callback` (`SessionCore.stream_shut_down`) has
  run ([section 6.1](06-massivesession-and-lifecycle.md#61-how-a-live-table-is-kept-alive-and-released)), so a failure is shown
  whenever it is reported: during the open, after the library's last result was released
  while a derived table or an open panel still keeps the publisher alive (the row moves from
  `RELEASED` to `FAILED` with its `Error`), or after the blink table shut down while results
  are still held. A failure reported for a stream that has left the registry is only logged.
- Each REST `poll()` table has its own publisher, named `massive.<dataset>#<stream id>` and
  created with the result in the result scope; every mode is shaped from its blink table, so
  `publish_failure` fails the table in every mode. (A keyed `input_table` cannot be failed:
  Deephaven notifies failures only inside an update cycle.) `APPEND` is
  `blink_to_append_only(blink)`, fed only the rows whose identity (`poll_keys`, then
  `revision_keys`) the poller has not appended yet ([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)); the
  poller thread alone owns that set of identities, so it needs no lock, and adds a cycle's
  identities only once the cycle is published; `UPSERT` is `blink.last_by(poll_keys)`, which
  always has a column to aggregate, since every schema ends with `TimestampInsert` and no
  dataset keys on it (so `reference.related_companies`, keyed on every vendor column, keeps
  each distinct row with the time of the latest poll that returned it); `REPLACE`
  is that `last_by` filtered on a hidden boolean column, to which each cycle adds one tombstone
  row for every key the previous cycle had and this one lacks, then dropped. Deephaven keys no
  aggregation on an array column, so when a poll key is a list (`long[]` or `String[]`, the
  13-F `OtherManagers`), the `last_by` keys on a hidden text column in its place, the JSON text
  of the row's list keys, which the poller writes beside every row and tombstone and the result
  drops (a DEBUG line names the keys the table uses); the poller's identities hold a list as
  a tuple. A list key compares item by item and in order, as Deephaven compares arrays: the
  same items in another order are another key, since the library never reorders what Massive
  sends. A cycle is one `add`,
  so its rows and its removals land in one update. The `last_by` state keeps a hidden row for
  every key ever seen, the trade-off `last_by()` streams already accept: a `REPLACE` poll over a
  churning key set (an options chain, the day's movers) grows with the keys it has held. The
  `poll()` docstring says so. The poller holds the session core only weakly; the poll's record
  in the session's registry keeps the session alive while the poll runs. The poller runs until
  the poll's blink table shuts down (nothing uses the table any more), it fails, or the session
  closes, which fails the table.
