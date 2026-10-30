"""The tester's tools: a setup check, the guide's examples run in one go, and a bug report.

`docs/tester-guide.md` walks a first-time user through the library. These modules back the
three commands of `scripts/tester.py` that save the tester typing and keep secrets out of what
the tester sends back. Like the live-run program beside them (`tests/_live/live_run/`), they
are a test instrument for the first-user test, to retire or trim once that test is over:

- `setup_check`: checks the two setups the tester builds, the console install and the
  source checkout, and that they hold the same commit, and probes the key;
- `examples`: runs every example of `docs/tester-examples/` in order, as the web console
  would, and saves what each printed, raised, and made;
- `bug_report`: collects the logs, the diagnostics, and the machine's description into one
  zip, cleaned of the secrets and scanned for them;
- `cli`: the command line of `scripts/tester.py`.

Each module has a unit test under `tests/live/tester/`, against the fakes.
"""
