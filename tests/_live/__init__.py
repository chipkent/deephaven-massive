"""Helpers of the live integration tests: how they quote Massive's refusals, which days to read.

The live tests (`tests/**/test_*_integration.py`) run on whatever key the owner gives them and
on whatever day they are run: each sends its requests, and when Massive refuses one, it skips
and quotes Massive. This package holds the logic that makes that possible, apart from the
fixtures that call it, so it can be unit-tested against the fakes:

- `refusals`: Massive's refusal on one line (status, text, request), and the earliest day a
  request refused for its timeframe asked for;
- `classes`: which asset class, and so which rate-limit bucket, a REST request reads, and
  which markets list each class's tickers;
- `pacing`: the requests a test declares, and the terminal line printed while a request waits
  for its pace;
- `calendar`: the US exchanges' holidays and half days, and the recent days the tests read;
- `contracts`: the front futures contract of a day, by each product's roll rule;
- `events`: the choice of recent splits and special dividends to check;
- `bad_values`: the requests that send Massive a value the library refuses before sending,
  and the verdict on what Massive answers;
- `ticker_lists`: the forms of futures spread tickers, and the crypto pairs whose currencies
  do not join to their ticker;
- `streams`: when a stream must have delivered rows, and which stock session a row is in;
- `week_windows`: which seven days a week bar covers, and whether that is the library's week;
- `index_day_sources`: which day each index day source covers, and where the index flat files
  and REST disagree, summed over many indices;
- `futures_bar_buckets`: which bucket a futures bar of a session or longer covers, judged
  from the shorter bars inside it;
- `summary`: the open-questions summary that ends a live run;
- `same_field`: whether two Massive fields carry one quantity, read back to back and compared
  within a tolerance;
- `parallel`: which selected tests are live, and the refusal to run them on parallel workers;
- `key_check`: the key a live test may use, never under CI.

Its subpackage `live_run` holds the live-run program, which a tester runs with their own keys
through `scripts/live_run.py` to run every live check the key allows, unattended, for days.

`tests/conftest.py` turns the modules above into fixtures, which the live test modules use; a
live test module may also import a module's pure functions and types directly. The unit tests
of this package are under `tests/live/`, one per module, against the fakes.
"""
