"""Helpers of the live integration tests, and the live-run program that runs them for days.

The live tests (`tests/**/test_*_integration.py`) run on whatever Massive plans the key holds
and on whatever day they are run. This package holds the logic that makes that possible, apart
from the fixtures that call it, so it can be unit-tested against the fakes:

- `plans`: the plans per asset class, which plan covers which, their documented history
  windows, and the verdict on a request refused for its timeframe;
- `probe`: the probe that asks Massive which plans a key holds, and its day-long cache;
- `classes`: which asset class, and so which rate-limit bucket, a REST request reads;
- `calendar`: the US exchanges' holidays and half days, and the recent days the tests read;
- `contracts`: the front futures contract of a day, by each product's roll rule;
- `events`: the choice of recent splits and special dividends to check;
- `streams`: when a stream on a delayed plan must have delivered rows;
- `week_windows`: which seven days a week bar covers, and whether that is the library's week;
- `summary`: the open-questions summary that ends a live run;
- `parallel`: which selected tests are live, and the refusal to run them on parallel workers.

It also holds the live-run program, which a tester runs with their own keys through
`scripts/live_run.py` to run every live check the key allows, unattended, for days, and to
record Massive's raw replies and the library's results for diagnosis afterwards:

- `cli`: the command line, and its wiring to the real machine;
- `live_run`: the commands: check, run, status, pack, and the upgrade check;
- `run_context`: what a command needs from the world, and what one run keeps in memory;
- `run_state`: the state file, the entitlements, and when a check is due;
- `run_schedule` and `timetable`: the next step, by a fixed daily timetable;
- `attempts` and `children`: running each group in a child process, and recording its results;
- `outcomes`: the pytest plugin that records each test phase as it runs;
- `run_probe` and `machine_checks`: what the key can do, and whether the machine is ready;
- `group_child`: the child side of the built-in groups;
- `replies`, `stream_capture`, `stream_units`, `reconcile`, and `upgrade_check`: the built-in
  groups: the documentation's examples, a trading day of streams and its throughput, the
  comparison with history, and the attended upgrade check;
- `secret_scrub` and `results`: keeping the secrets out of the results; the status, the
  report, and the zip.

`tests/conftest.py` turns the first group into fixtures, which the live test modules use; only
the unit tests of this package, under `tests/live/` (one per module, against the fakes), and
the program import it.
"""
