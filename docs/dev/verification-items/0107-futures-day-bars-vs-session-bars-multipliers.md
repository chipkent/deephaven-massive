# 107. Whether Massive's futures `day` bars are its `session` bars at every multiplier

Date: 2026-11-12

Status: open

`resolution=1day` answered with the same shape as `1session` (item 100), so `Timespan.DAY` is sent
as `day` and its bars are dated and kept as session bars. Settled by
`tests/namespaces/test_futures_integration.py`, which compares `1day` with
`1session` and `5day` with `5session` on the front E-mini contract, and records the library's `DAY`
bars beside them. `tests/queries/test_choice_integration.py` records what the futures aggregates
answer to a multiplier below 1 (`0session`), which the library sends for Massive to judge.
