# 112. A data table holds only rows the library read whole

Date: 2026-11-15

Status: accepted

## Question

When the library cannot read a row of Massive's data whole, should it publish the row with a
library-made null or rounded value, or withhold it?

## Options

1. Keep publishing such rows: an unfit value or symbol as null, a long its `double` column
   rounds as rounded, a bar at an unexpected stamp with a null `Date`.
2. Withhold every such row and report it by cause.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-15, approving the
design of issue #666, with later answers that day. A data table holds only rows the library read
whole, and a null in it is Massive's. A row the library cannot read or derive whole is withheld
and reported. A cause's `dm.ErrorPolicy` setting decides only whether the table fails and how
loudly the log reports the row, never whether the row is published, so no column depends on a
setting. Two exceptions: a field Massive's documentation marks required but a row omits keeps
Massive's null, and a futures row the schedules cannot yet place keeps today's path until a
separate futures design replaces it.

## Reason

A trader can act on a row the library filled in, which looks like Massive's data; a withheld
row shows in the report.

## Consequences

Rows once published with a null are missing from their tables and reported instead. This
replaces decision 85, and the parts of decisions 35, 66, and 70 that publish such rows.

## Rule

`AGENTS.md`, "A data table holds only what Massive sent, read whole".
