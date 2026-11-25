# 83. The unit of the options snapshot's `greeks.gamma`: per $0.01 move of the underlying, as Massive's description and the dataset page say, or per $1, the textbook unit

Date: 2026-11-10

Status: settled 2026-11-25: per $1, not per $0.01: five SPY calls' gamma came within 1.02 to 1.03 times Black-Scholes gamma per $1; the chain's and snapshots' `Gamma` describe it per $1; recorded by `tests/namespaces/test_options_integration.py`

Passed through as sent, with Massive's description. Settled by
`tests/namespaces/test_options_integration.py` (Options Starter), which records each contract's
ratio to Black-Scholes gamma from its own inputs.

Settled 2026-11-25 by the owner's run of
`uv run pytest -m integration tests/namespaces/test_options_integration.py -k gamma` at 16:34
UTC, before the column was renamed from `GreeksGamma` to `Gamma`. Its evidence:

```text
SPY 699.475, 3-month yield 4.37% of 2026-11-23
O:SPY261218C00699000: GreeksGamma 0.008048230866990486, Black-Scholes per $1 0.00786104, ratio 1.024: per $1
O:SPY261218C00700000: GreeksGamma 0.008096319195917514, Black-Scholes per $1 0.00793905, ratio 1.02: per $1
O:SPY261224C00700000: GreeksGamma 0.00763642625318275, Black-Scholes per $1 0.00747963, ratio 1.021: per $1
O:SPY261231C00699000: GreeksGamma 0.007085397435672781, Black-Scholes per $1 0.00690853, ratio 1.026: per $1
O:SPY261231C00700000: GreeksGamma 0.007124099082533537, Black-Scholes per $1 0.0069709, ratio 1.022: per $1
units: ['per $1', 'per $1', 'per $1', 'per $1', 'per $1']; Massive's description says per $0.01
```
