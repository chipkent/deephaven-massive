# 123. Hold streamed futures rows until Massive's schedules date them

Date: 2026-11-16

Status: accepted

## Question

Massive's futures WebSocket rows carry no session date, so the library dates each from its
product's schedule, which loads after `m.futures.trades("ESZ6")` or a `dm.ALL` stream opens.
Rows that arrived before it loaded were published with a null `Date`, every spread's rows had
none, and a failed lookup was asked again only after 10 minutes.

## Options

- Publish undated rows and report them as problems: rejected.
- Hold rows until dated, with every product's calendar loaded up front: rejected for its cost.
- Hold rows until dated, loading only what the streams need: chosen.

## Ruling

Decided by the owner on 2026-11-16, in the coordinating chat session. Hold every streamed
futures row until Massive's schedules give it a Date. Never publish it with a null or guessed
Date, and never count a row that is only waiting as a problem. Retry a failed or empty lookup
within seconds, not minutes, and record a problem well before a row has waited long. Load
schedules only for what the streams need, never every calendar up front. Withhold the rows of
products Massive's schedules give no session for, under their own cause; report that gap to
Massive, and revisit if Massive says it is expected.

## Reason

A row that arrives early is valid data, and a null or guessed `Date` is unusable. Loading every
calendar can take 30 minutes on a paced key and wastes requests when one future is wanted.

## Consequences

A stream's first rows appear only after its lookups answer. Rows Massive's lists or schedules
can't date never appear until Massive's reference data covers them.

## Rule

`dm.ErrorPolicy`'s `unlisted_contracts` and `unscheduled_products`, and the docstring of
`_ingest/futures_calendar.py`.
