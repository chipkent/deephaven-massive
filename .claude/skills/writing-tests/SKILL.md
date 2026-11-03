---
name: writing-tests
description: "Use this skill whenever you write, change, or debug a test in this repository: a unit test, a fixture, a fake server, a live integration test, or a test that hangs, crashes the JVM, or flakes. That includes \"add a test for this\", \"cover this branch\", \"the suite hangs\", \"hs_err file appeared\", and \"add a live check for item N\". It covers the one embedded Deephaven JVM per test process, the fixtures to use instead of sleeps, input tables that stand in for watchlists, and the verifies and settles markers of the live tests. Not for running the full check before a push (the pre-push hook does that)."
---

# Writing tests

The rules that place and limit tests (the mirror layout, 100% coverage, fakes instead of
Massive, `tests/test_layout.py` being the owner's) are hard rules in `AGENTS.md`. This skill
holds the harness details that keep a test deterministic and keep the JVM alive.

## The embedded JVM

- `tests/conftest.py` starts one embedded Deephaven server per pytest process before
  collection, so test modules may import `deephaven` at module level. Under `-n`, each xdist
  worker starts one and the controller starts none; `-n` with the live tests is refused.
- Never start or stop another `Server` in that process (a subprocess may). Read the comments on
  the `trylast` hook and the JVM shutdown block in `tests/conftest.py` before touching either.
- The JVM raises SIGSEGV on purpose, so never call `faulthandler.enable()` once it runs. Any
  process that starts a `Server` leaves through `os._exit`, as `scripts/render_reference.py`
  does.
- An `hs_err_pid*.log` file is a JVM crash, and a `jvm_fatal_pid*.log` file holds the message
  of a JVM fatal error that exited the process: investigate either, never delete it and move on.

## Deterministic tests

- Take the execution context, table helpers, and fakes from fixtures (`dh_exec_ctx`,
  `dh_to_pylist`, `load_fixture`, `make_csv_gz`).
- Drive state through the fakes in `tests/_fakes/` and explicit flush or poll calls, never
  `time.sleep()`: a sleep is slow when it is long enough and flaky when it is not.
- Payload fixtures live under `tests/data/<test owner path>/`.
- Every behavior change gets a test that fails without the change.

## Input tables that stand in for a watchlist

The library never uses input tables, but a test may, to play a user's watchlist.

- Create it through `dh_input_table`, which holds the update-graph shared lock only for the
  creation. Release that lock before any blocking add or delete, which raise under it.
- To delete rows from a keyed input table, pass a static key table, never a live view of the
  same table: that can hang against the update-graph refresh thread.

## Live tests

Live tests (`tests/**/test_*_integration.py`) call Massive with a real key; agents write them
but never run them with a key. `docs/live-tests.md` is their runbook.

- A live test names the open questions it answers with `@pytest.mark.verifies("N")` (it asserts
  the answer the library assumes) or `@pytest.mark.settles("N")` (it records evidence a person
  reads). `N` is an item of `docs/dev/verification-items.md`; `tests/live/test_summary.py`
  fails on a number the file does not list.
- A new open question gets a new item in that file, with the next free number; a settled item
  gets its one-line settlement there.
- Each test's docstring says what its evidence means.
