# 87. A row limit caps the whole call's rows, not each symbol's

Date: 2026-11-02

Status: accepted

## Question

What does `limit` cap: each symbol's rows, or the rows of one request to Massive?

## Options

1. Each symbol's rows, sending one request per symbol (decision 74).
2. Massive's total cap per request.

## Ruling

`limit=100` caps the call at 100 rows in total: the read stops once that many have arrived and
sends no further request, so twenty symbols may leave some with no rows. The one exception is a
dataset whose endpoint takes a single symbol per request, such as one ticker's news or an
indicator: it caps each symbol at 100, so twenty symbols give up to 2,000 rows. Each dataset's
page says which. The owner ruled on 2026-11-02 that sampling with `limit` and then reading
without it beats a twenty-minute download; recorded in PR #514. This replaces decision 74 and
the per-symbol limit of decision 84.

## Reason

The earlier design sent one request per symbol, so many-symbol reads took minutes; requests now
carry many symbols, and a cap per symbol would mean one request per symbol again.

## Consequences

Use `limit` to sample; read without it for complete data. A capped read of many symbols can
give some symbols no rows.

## Rule

The `limit` argument rule on the dataset pages.
