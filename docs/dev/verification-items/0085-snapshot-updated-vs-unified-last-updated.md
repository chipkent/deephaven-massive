# 85. Whether the v2 snapshots' `updated` is the unified snapshot's `last_updated` for the same ticker

Date: 2026-11-11

Status: settled 2026-11-12: the v2 snapshots' `updated` is not the unified snapshot's `last_updated`

Kept apart as `Updated` until read. Settled by `tests/massive/refresh/test_overrides_integration.py`,
which reads both, for stocks and crypto, back to back and compares them within the
time between the requests plus 1 second.

Settled 2026-11-12: the unified snapshot sent no top-level `last_updated`, and the stocks and crypto
`updated` matched none of its nested times (forex's matched `last_quote.last_updated`), so `Updated`
keeps its name.
