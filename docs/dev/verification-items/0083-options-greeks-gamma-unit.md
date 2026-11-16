# 83. The unit of the options snapshot's `greeks.gamma`: per $0.01 move of the underlying, as Massive's description and the dataset page say, or per $1, the textbook unit

Date: 2026-11-10

Status: open

Passed through as sent, with Massive's description. Settled by
`tests/namespaces/test_options_integration.py`, which needs option chain snapshots and records each contract's
ratio to Black-Scholes gamma from its own inputs.
