# 26. Whether Massive reads a percent-encoded colon in a path (`I%3ASPX`) as `I:SPX`

Date: 2026-10-28

Status: open

Every path value is percent-encoded whole; a value with `/` is refused. Settled by
`tests/namespaces/test_indices_integration.py`.
