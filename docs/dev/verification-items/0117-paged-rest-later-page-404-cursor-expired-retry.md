# 117. Why a later page of a paged REST read answers HTTP 404

Date: 2026-11-12

Status: open

The library assumes the page's cursor expired or its page moved, so a new call, with a new cursor,
may succeed: `CutShortError` is retryable, and a followed read reads the symbol again. Settled by
`tests/massive/rest/test_client_integration.py`, which asks for one `next_url` of
the ticker list at once and again after 90 seconds and records both answers.
