## 16. `HistorySource.AUTO`

```
S3 possible   iff S3 credentials are configured
              and the dataset has a prefix (futures: one per venue of `venues`, all four by default)
              and (not bars, or ((adjusted is False or the dataset has no adjusted parameter)
                                  and (multiplier, timespan) in {(1, MINUTE), (1, DAY)}
                                      or, for futures, (1, SESSION)))
REST possible iff the dataset has a REST history endpoint
              and (symbols is not ALL or (bars and (1, DAY)))        # grouped daily
none possible -> ValueError listing, per source, the violated constraint
one possible  -> use it
both          -> S3 if symbols is ALL or len(symbols) >= flat_file_symbol_threshold, else REST
                 a followed symbol table (section 7.8): REST, whatever the symbol count
AUTO on S3, then, when REST is possible:
  S3 refuses the files (403) or the S3 credentials -> the whole call from REST, one WARNING
  each trading day whose file is missing -> that day from REST, clipped to the range, one WARNING
FLAT_FILES, or AUTO with REST not possible:
  a refusal -> raise
  a missing trading day with a later day's file -> a day without trading, skipped with a
    WARNING and named in a blocking read's table description
  a missing trading day with no later file within a week of the range's end:
    less than a week before today's file -> raise ("not published yet")
    a week or more before it              -> raise ("no flat file exists")
```

**AUTO serves from whichever source can; it errors only when none can** (decision 78).
Massive says a day's files land "by approximately 11:00 AM ET the following day", and the time
varies: weekday files landed at about 00:45 New York time in the live check of 2026-10-30, but
Friday's on Saturday at 10:27 and 11:17. So the library never guesses publication from the
clock, and has no publish cutoff setting. When REST can serve the request under `AUTO`,
nothing is guessed at all: every trading day whose flat file is missing is read from REST, one
day at a time, as a one-day read clipped to the call's range (for datetimes, the day's
instants within the range; for futures, its session's hours), and its rows are merged in date
order with the files' rows. No row of that day is kept from the files (for futures, of any
venue), so no day comes from both sources. A day REST returns no rows for was a day without
trading. Sending everything after a gap to REST instead would turn one holiday into a REST read
of the rest of a long range. A missing weekday with files on both sides is therefore never
skipped silently: if Massive missed it, REST serves it. One WARNING per call names the days REST
served and the columns REST leaves null that flat files fill, and a blocking read's table
description names REST and those days (`Read from flat files, and from REST for 2025-03-04,
whose files are missing.`); a background read's description is written before the read, so it
says that a day the flat files cannot serve is read from REST, and the WARNING names the days.
Only under `FLAT_FILES`, or when REST cannot serve the request, does the flat-file read infer:
Massive writes a day's file after the day ends, in date order, so a missing trading day's file
with a later day's file after it (in the range, or looked for after it, trading days only, up
to 7 days past the first missing one and never as far as the current day, whose file cannot
exist) is a day without trading, skipped with a WARNING, and one with no later file may simply
not be published yet. The inference can be wrong (Massive may have missed a trading day's
file), and the library supplies Massive's data without hiding a gap in it, so a blocking read's
table description also names every day it skipped this way (`No flat file exists for
2025-03-04, though a later day's does, so that day was taken for a day without trading (a
holiday?) and has no rows here.`); a background read's description is written before the read,
so only the WARNING names them. For a day with no later file, the call raises
`MassiveError("<dataset>: the flat file for <date> is not published yet. Massive usually publishes a day's flat files by about 11:00 ET the next day,
but the time varies. Retry later, or read REST with source=dm.HistorySource.REST")` (or, when
REST cannot serve it, `Retry later; REST cannot serve this read: <reason>`). Likewise, when S3
refuses the flat files a read needs (HTTP 403: the plan lacks them) or rejects the S3
credentials under `AUTO` and REST can serve the request, the whole call is read from REST,
with one WARNING naming the refusal; under `FLAT_FILES`, or when REST cannot
serve the request, the refusal is raised. When neither source can serve the request, the
`ValueError` lists every violated constraint per source; for split-adjusted bars of `dm.ALL`
other than 1-day bars, it says exactly why (only REST serves adjusted bars, and REST reads
them one symbol at a time, while flat files hold as-traded prices) and what to do: pass
`adjusted=False` for the as-traded flat files, or name the symbols. Futures files are named by
session date, and their day ends at the next midnight in America/Chicago, where Massive
standardizes futures data. `indices.values` is flat-file only.
`indices.bars`, `forex.bars`, and `crypto.bars` have no `adjusted`, so the predicate is satisfied.

The choice covers the whole range, except for the days whose flat files are missing, which
`AUTO` reads from REST as above. The partition dates are those of the rows the call keeps
([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)): for bars, the days of the bars whose
whole window fits, so the unfinished current day never needs a file. Bars of a day or longer
follow the same rule as every other dataset: the two sources may serve one call or the next by
symbol count, and the stamp check of
[section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries) stamps their bars alike and warns on a stamp
it does not know, so the choice never changes which day a bar is labelled with. If live REST
crypto bars turn out to be cut at midnight New York time, every REST crypto daily call
warns (the morning before yesterday's file is published included), and the owner
decides the source rule after that live check ([section 23](23-open-verification-items.md#23-open-verification-items)
item 1). `source=` selects either source explicitly. A dataset without instruments counts as `ALL`.
A followed symbol table ([section 7.8](07-query-classes-and-verbs.md#78-following-a-live-symbol-table)) ignores the symbol
count: `AUTO` reads REST whenever REST can serve the request, and flat files otherwise, so each
symbol, whenever it is added, is read from the source a `history()` of that one symbol would
use, and one added symbol never re-reads the flat files of the whole range. The missing-day
fallback to REST applies to its flat-file reads as to any other.
`HistoryQuery.history()` makes the choice (`choose_source` in `_datasets/history_source.py`) and then calls one
transport; the transports only do I/O. The REST predicate depends only on the dataset's REST
capabilities (`DatasetSpec.has_rest_history`, `has_grouped_daily`), and the `adjusted` check on
the typed `Request.adjusted`.

An `AUTO` call logs its choice and the reason once, for example
`stocks.trades history: AUTO chose REST (2 symbols < flat_file_symbol_threshold 50)`.

**The source is never silent.** The two sources of one dataset do not always fill the same
columns: a REST history endpoint and a flat file can each carry fields the other lacks (REST
trades have `DecimalSize`; futures files have `Exchange`, and futures trade files
`Correction`), and a column a source does not carry is null. Under `AUTO` the source changes
with the symbol count, so the same query could return a column filled for many symbols and
null for few. `choose_source` therefore classifies the source it passed over (`Alternative`):
`AVAILABLE` when it can serve the request; `UNAVAILABLE` when it cannot (no S3 credentials, no
file or endpoint for the request, adjusted bars, `dm.ALL` over REST). Unless the other source is
`UNAVAILABLE`, the query compares the columns of the chosen source's history sources (REST: the
history and grouped-daily paths; flat files: every prefix of the dataset) with the other
source's, by the generated schemas (`Catalog.history_columns`). When the other source fills
columns the chosen one leaves null, it logs one WARNING per session, dataset, and set of such
columns (`SessionCore.warnings`), naming them, the choice and its reason, and what fills them:
`stocks.trades history: AUTO read flat files (60 symbols >= flat_file_symbol_threshold 50); the
read leaves DecimalSize null, which the other source, REST, fills: pass
source=dm.HistorySource.REST to get them (logged once per session)`. An `UNAVAILABLE` alternative means the result cannot change
and the caller cannot pick the other source, so nothing is logged, as with an explicit
`source=`; a range no row can lie in reads nothing and logs nothing either. Every `history()`
table records its source in its table description ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)),
after the summary: `stocks.trades: Trades of US-listed stocks, tick by tick. Read from REST:
AUTO chose it (2 symbols < flat_file_symbol_threshold 50).`, or `Read from flat files:
requested with source=...`. The flat-file refusal of adjusted bars reads `adjusted=True needs
REST, since flat-file bars are unadjusted (as traded); pass adjusted=False for the flat files`,
so the description says why an adjusted read used REST. A range no row can lie in returns its
empty table before any source is read, with the summary alone. Each
transport then logs its own work: the flat-file transport `source=S3 files=<n> skipped=<dates>
unpublished_from=<date>` (`none` for either when nothing was missing or unpublished), the REST
transport `source=REST requests=<n>`.
Missing keys on non-trading dates are skipped. A trading date is a weekday, a forex Sunday, or
any crypto day, by the one test `dates.is_trading_day` that
the REST transport's weekend skipping uses too. A missing key on a trading date is read from
REST under `AUTO` when REST can serve the request, and otherwise judged as above (there is no
holiday calendar). A futures range named by datetimes that ends on a holiday reads the next
session's file in the inferring case, as [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries)
describes, and REST's hours of the holiday otherwise. A range in which no file exists and none
follows raises under `FLAT_FILES` (or reads REST under `AUTO`) rather than return an empty
table, and also logs the WARNING that the bucket, prefix, or venue may be wrong. A futures
venue whose file of a day is missing sends that day of every venue to REST; a partial series
of venues is not handled separately.
A missing file is a 404 whose body is S3's XML error with the code `NoSuchKey`. Any other
404 (another S3 code, no code, or a body that is not S3's, such as the HTML "Massive 404 Not
Found" page Massive's front end serves for a path it does not route, which is what a wrong
`s3_bucket` meets) raises `MassiveError` naming `s3://<bucket>/<key> at <endpoint>` and adding
"check MassiveConfig s3_bucket and s3_endpoint_url": `cannot read s3://flatfile/<key> at
https://files.massive.com: HTTP 404 (404: Not Found); check MassiveConfig s3_bucket and
s3_endpoint_url`. A connection that drops part-way through a file's body makes the read start
that file again from its first byte, at most `MAX_RETRIES` (3) more times, after the waits of
the shared retry schedule (0.5, 1, 2 s plus jitter). The rows already read from that file are
discarded, and freed before the wait, which is safe because no row leaves a file's read before
the file ends. The last failure raises `MassiveError` naming the file. Futures `venues`
reads one prefix per venue per date, four by default; a 403 on any prefix raises
`EntitlementError` naming the venue and the `venues=` parameter. A 403 whose S3 error code says the credentials themselves are wrong
(`InvalidAccessKeyId`, `SignatureDoesNotMatch`) raises `AuthenticationError` instead, for every
asset class, naming `s3_access_key_id` and `s3_secret_access_key`.

Flat files are read through `_massive.FlatFileClient` and `FlatFile`
([section 19.3](19-wire-clients-and-the-spec-snapshot.md#193-the-flat-file-client)); `_ingest/flatfiles.py` builds each file's `ReadPlan`.
`pyarrow.csv.open_csv` reads each `.csv.gz` key in 1 MiB blocks (gzip only: every documented
key is `.csv.gz`). Columns are matched **by name, never by position**: the reader first reads
the header line, parses it with the `csv` module (quotes and `\r\n` included), and passes those
names as `ReadOptions(column_names=…)`, then sets
`include_columns` to the documented headers with `include_missing_columns=True`, so a reordered
file reads identically, an extra header is reported as drift and ignored, a missing header
becomes a null column of its declared type, and a missing declared key header raises at once
([section 19.5](19-wire-clients-and-the-spec-snapshot.md#195-runtime-drift)). The CSV reader reads every header as text, and only an
empty field is null: `NA` and `NULL` are text, since both are valid tickers. Each block then
goes through pure Arrow operations in this order: the `Symbol` filter
(`pyarrow.compute.is_in`); the key check, which drops and counts a row whose key header is
empty or does not parse, or whose time the plan's converter cannot place, and fails the file
after its last block when it dropped every row of the requested symbols; the window's half-open `Timestamp` range in nanoseconds (for bars, the
starts of the bars whose whole window fits; not applied to futures trades, quotes, and session
bars named by date, which `_ingest/flatfiles.py` keeps by each row's own session); and, on the
surviving rows only, the conversion of each header to its column's type (epoch integers for
`Instant`s), the parsing of the list columns (`Conditions`, `Indicators`), where a value that
does not parse is null and reported, and the rename to long-form names. `_ingest/flatfiles.py` then applies
`rename` in `_datasets/source_columns.py` and the `_datasets/timestamps.py` routine, and moves each
1-day bar's stamp to 00:00 of its day in the class day zone, warning on a stamp it does not know
(`row_times.day_starts`). No block is converted
to Python objects. A
file's surviving rows become one static Deephaven table per 16 blocks of data and at the end of
the file (usually one table per file, since each table carries a fixed JVM-heap cost), and the
tables are merged.

Memory is bounded per concurrent reader, not per range: a reader decompresses its file on its
own thread and cuts the text after a line end into chunks of 16 blocks, which three parse threads
of its own read with `pyarrow.csv.read_csv` and filter while it decompresses the next, at most
four chunks ahead of the consumer and handed on in file order. So a reader holds about 64 MiB of
decompressed text, and the rows parsed from it, plus at most 16 blocks of surviving rows, and
at most `max_concurrency` readers run at once. The chunks give exactly the rows, drift, and
dropped rows of reading the file as one stream.

Rows come in partition-date order, then (futures) venue order (CME, CBOT, NYMEX, COMEX), then
file order. They are not sorted by time across venues, and may differ in order from REST.

Keys are read by a per-call pool of `min(max_concurrency, keys)` threads. A failed read, an
interrupt of the calling thread, or `m.close()` stops every other read at its next block, and
reads not yet started are cancelled; the failure of the earliest file is raised. After a close,
no request starts once a reader has seen the close; a request already in flight is discarded
(the client closes its connection pool once more as that request returns, so nothing it opened
outlives the close), and the call raises `MassiveError("MassiveSession #1 is closed")`,
whatever that request returned.
