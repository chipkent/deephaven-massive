"""The live-run program: every live check a key allows, run unattended for days, and recorded.

A tester runs it with their own keys through `scripts/live_run.py`; it records Massive's raw
replies and the library's results, so anything wrong can be diagnosed afterwards without
another live run. It is a test instrument for the first-user test, to retire or trim once that
test is over. Its parent, the commands a person types, starts no JVM, so only the children's
modules (`child_commands` and what it imports) import the library or Deephaven. Its modules:

- `cli`: the command line, and its wiring to the real machine;
- `child_commands` and `server`: the hidden commands of the children, and the servers they
  start;
- `commands`: the commands: check, run, status, pack, and the upgrade check;
- `context`: what a command needs from the world, and what one run keeps in memory;
- `state`: the state file, and when a check is due;
- `schedule` and `timetable`: the next step, by a fixed daily timetable;
- `attempts` and `children`: running each group in a child process, and recording its results;
- `outcomes`: the pytest plugin that records each test phase as it runs;
- `run_probe`, `probe_result`, and `machine_checks`: whether Massive takes the key, its clock
  and market closures, the probe's result as the parent reads it from its child, and whether
  the machine is ready;
- `group_child`: the child side of the built-in groups;
- `reply_examples`, `replies`, `stream_capture`, `stream_night`, `stream_units`, `reconcile`, and
  `upgrade_check`: the built-in groups: the documentation's examples, a trading day of streams
  and its throughput, a night of crypto and futures streams, the comparison with history, and
  the attended upgrade check;
- `secret_scrub` and `results`: keeping the secrets out of the results; the status, the
  report, and the zip.

Each module has a unit test under `tests/live/live_run/`, against the fakes.
"""
