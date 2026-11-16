# 69. Whether a delayed futures plan's REST snapshot says `DELAYED`

Date: 2026-10-31

Status: open

Without it, only Massive's refusal on the real-time feed reveals delayed futures data. Settled by
`tests/ingest/test_host_decision_integration.py`, which records the futures snapshot's labels and
needs one that Massive labels delayed.
