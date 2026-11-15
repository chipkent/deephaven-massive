# 43. Massive's spelling of US stock tickers in partner datasets (Benzinga, ETF Global)

Date: 2026-10-28

Status: retired

The stock types were settled 2026-11-02 (all 1,276 warrant, unit, right, and preferred tickers fit
the grammar); a partner row spelled otherwise follows `unreadable_symbols`. Settled by
`tests/namespaces/test_reference_integration.py` with the partner add-ons.

Retired 2026-11-11: the library reads no ticker's shape and keeps every row's ticker as
Massive writes it (decision 104), so a partner row's spelling no longer matters.
