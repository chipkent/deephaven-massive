# 99. A malformed flat-file line follows `MassiveConfig.malformed_rows`, `LOG` by default

Date: 2026-11-10

Status: accepted

Rule 24 gives each cause of a bad row its own `dm.OnError` setting (66, 81); a flat-file line that
is not a readable row had none: it was skipped with one WARNING per file. The setting is
`malformed_rows`, named like its siblings (`unreadable_symbols`, ...) for what it governs. A
malformed line is one that does not split into the header's fields (too few or too many, or a quote
that does not close) or is not UTF-8 text. Under `LOG`, the default and the former behavior, the
line is skipped and each file that had one logs one WARNING with the count and the first one's row
number and fault, never its text; under `IGNORE` that line is at DEBUG; under `RAISE`, `history()`
fails with `RowDataError` (`ErrorCause.LIBRARY_DEFECT`, data from Massive the library can't read) at
the first block after the line, naming the file and the row, and a `history()` of a live table marks
every symbol of the read `FAILED`, `PERMANENT`. A static `history()` has no `m.streams()` row, so
the log line is its only record under `LOG`, as for its siblings. A file whose every line is
malformed fails under every setting: that affects every row. A line that is not UTF-8 text used to
fail the whole `history()` call, not one block: Arrow refuses a chunk with an invalid UTF-8 value in
a column it reads (`CSV conversion error to string: invalid UTF8 data`), and a line that also has
the wrong field count fails before the skip handler runs, because pyarrow decodes the row's text
first. The reader now checks each chunk as UTF-8 in one zero-copy Arrow validation and, only for a
chunk that fails, removes the lines that do not decode before Arrow reads it, counting them as
malformed; Arrow's own UTF-8 check is off, since every chunk it reads is checked. Such a line is
skipped whole even when its bad byte is in a column the library drops.
