# 78. The AUTO source reads from whichever source can, and fails only when none can

Date: 2026-10-30

Status: accepted

## Question

Under `source=AUTO`, `history()` may read flat files or REST. When a day's file is missing, or
flat files are refused, should the call fail, or guess from Massive's publish time?

## Options

1. Guess from Massive's approximate publish time which days have files.
2. Serve from whichever source can, and fail only when none can.

## Ruling

`history(..., source=dm.HistorySource.AUTO)` prefers flat files. A day whose file is missing is
read from REST, and a flat-file refusal reads the whole call from REST, each with a WARNING. It
never guesses from Massive's approximate publish time which days have files. The owner ruled on
2026-10-31 that the method should return the data if it can, since the publish time is
approximate; recorded in PR #514.

## Reason

Publish times are approximate; a guess would fail calls that could be served.

## Consequences

An `AUTO` call can be slower than expected, reading days from REST; the WARNING says when.

## Rule

The `history()` docstrings, `source`.
