# 115. What a path that takes the ticker answers for one holding `/`, sent as `%2F` (`BRK%2FB`)

Date: 2026-11-11

Status: settled 2026-11-16: Massive answered HTTP 307 with an empty body and no rows, so the refusal stands

A path value holding `/` is refused in `request_target` until read, on PR #125's finding that
Massive answers HTTP 404 for `%2F`, and a stock written `BRK/B` is refused by the grammar anyway.
Settled by `tests/massive/rest/test_query_integration.py`, which sends `/v3/trades/BRK%2FB` by
hand, past the refusal, and records Massive's answer: a 404 or an empty 200 keeps the refusal,
and rows would mean `%2F` reads as `BRK.B`, which the grammar's refusal still covers.

Evidence, 2026-11-16: `GET /v3/trades/BRK%2FB?limit=1` answered HTTP 307 with an empty message
and no rows; the run did not record the redirect's `Location`, which the test now records. No
answer yet has served Berkshire B's trades for `%2F`, so `request_target` keeps refusing a path
value holding `/`.
