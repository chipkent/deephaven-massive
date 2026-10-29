"""Helpers of the live integration tests: what the key's plans are, and which days to read.

The live tests (`tests/**/test_*_integration.py`) run on whatever Massive plans the key holds
and on whatever day they are run. This package holds the logic that makes that possible, apart
from the fixtures that call it, so it can be unit-tested against the fakes:

- `plans`: the plans per asset class, which plan covers which, their documented history
  windows, the probe that asks Massive which plans a key holds, its day-long cache, and the
  verdict on a request refused for its timeframe;
- `classes`: which asset class, and so which rate-limit bucket, a REST request reads;
- `calendar`: the US exchanges' holidays and half days, and the recent days the tests read;
- `contracts`: the front futures contract of a day, by each product's roll rule;
- `events`: the choice of recent splits and special dividends to check.

`tests/conftest.py` turns them into fixtures, which the live test modules use; only the unit
tests of this package, under `tests/live/` (one per module, against the fakes), import it.
"""
