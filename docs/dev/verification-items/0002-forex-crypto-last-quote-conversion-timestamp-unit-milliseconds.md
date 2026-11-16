# 2. Unit of REST forex `/v1/last_quote` `timestamp`, crypto `/v1/last` `timestamp`, and forex `/v1/conversion` `last.timestamp`

Date: 2026-10-23

Status: open

Read as milliseconds, as the spec tags them; a mismatch fails the page loudly. Settled by
`tests/datasets/test_timestamps_integration.py`.
