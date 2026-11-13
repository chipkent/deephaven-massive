# 69. More inputs and a list of datasets

Date: 2026-10-30

Status: accepted

Date arguments take what `deephaven.time` takes (Java instants and dates, numpy, pandas, Deephaven's
own text) but never integers or naive times; sub-microsecond instants round outward. `dm.datasets()`
lists every dataset; `deephaven_massive.queries` exposes the query types; `m.reference.news` follows
a watchlist.
