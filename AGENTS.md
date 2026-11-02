# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables: a Python library managed with `uv`. Until the rework ends,
`plans/rework/DESIGN.md` is the specification and wins over this file. The library has no users
yet, so nothing is kept, aliased, or shimmed for compatibility: a rename is an ordinary edit.

## Commands

Run everything from the repository root, through `uv run`. The tests need JDK 17 or later.

```bash
uv sync --upgrade                        # create or update .venv with the newest versions
uv run pre-commit install --hook-type pre-commit --hook-type pre-push   # once per clone
./scripts/check.sh                       # full gate: ruff, format, mypy, pytest (up to 4 workers), 100% coverage
./scripts/check.sh fast                  # ruff (fixing what it can) and mypy only
uv run pytest tests/test_layout.py -q    # one file, no coverage, debugger-friendly
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -q   # free-threaded
uv run python -m deephaven_massive._massive.refresh   # regenerate _massive/spec/*.json; no network
uv run python scripts/render_reference.py             # regenerate docs/datasets/
uv build                                 # sdist and wheel
```

- The free-threaded run keeps its own `.venv-314t`: a plain `--python 3.14t` replaces `.venv`.
- `uv.lock` is not committed and `check.sh` upgrades on every run, so a new ruff or mypy release
  can flag code you did not touch. Fix it in your PR, or propose an ignore for owner approval.
- Live tests (`tests/**/test_*_integration.py`) use the owner's Massive keys, and only the owner
  runs them. Never run one with a key. `docs/live-tests.md` is their runbook.

## Hard rules

1. **Pull requests only.** Never commit or push to `main`, and never merge a PR (`gh pr merge`
   or otherwise): only the owner merges.
2. **Never bypass a hook**: no `--no-verify`, no `core.hooksPath` tricks, no skipped failing
   check. When a hook fails, fix the cause.
3. **Branches and commits.** Branches are `<type>/<topic>`, cut from `origin/main` after
   `git fetch`. Commits and PR titles follow Conventional Commits: `feat:`, `fix:`, `docs:`,
   `test:`, `refactor:`, `ci:`, `chore:`, and their wording meets the Title section of
   `docs/dev/pull-requests.md`.
4. **Check, review, push.** Before opening a PR and before every push to one, get
   `./scripts/check.sh` (full) green, run the `pre-push-review` skill, and fix its findings. Every
   CI job must pass, the required free-threaded 3.14t job too: locking must work without the GIL.
5. **Push in batches.** Each push to a PR branch runs paid CI and review bots: push once when the
   PR opens, then once per finished batch of fixes. Never push work in progress or to see
   whether CI passes. Merge `main` into a PR branch only for a conflict or a change you need, then
   rerun the full check before the push.
6. **Answer every review comment**, from people and bots: name the commit that fixes it and say
   how, or say why nothing changes. Never resolve a thread without a reply.
7. **PR descriptions read as if an O'Reilly technical editor wrote them**, and a reader
   skimming the headings and bullets gets every fact that matters: no slop, no narration, no
   padding, never a fact cut for length, and the context a reviewer new to the PR needs to
   follow it, no more. Write them with the `writing-a-pr-description` skill
   (`gh pr create --body-file` never shows the template); the sections, banned patterns, and
   self-edit checklist of `docs/dev/pull-requests.md` all apply. Rewrite the description to the
   final state on every push, and end it with `Closes #<issue>` on its own line. Issue bodies
   and agents' reports meet the same bar.
8. **Dependency declarations** in `pyproject.toml` change only in dedicated `chore(deps):` PRs.
9. **`DESIGN.md` first.** A change to a public signature, default, or table semantics updates
   `DESIGN.md` first: an owner decision.
10. **Tests mirror the package**, one leading underscore dropped per path component:
    `_massive/rest/client.py` is tested by `tests/massive/rest/test_client.py`. Two helper
    packages sit outside the rule: `tests/_fakes/`, the fake servers, and `tests/_live/`, the
    logic behind the live tests' fixtures. Each `tests/_live/` module has a unit test at the
    same mirrored path (`tests/_live/plans.py` is tested by `tests/live/test_plans.py`); the
    fakes have none. A new helper package, or tests for another one, needs owner approval.
11. **Coverage is 100%** of statements and branches. Delete a dead defensive branch instead of
    covering it. `# pragma: no cover` needs a reason on its line and owner approval in the PR.
12. **Every suppression has a reason**: each ruff ignore entry, `# noqa`, and
    `# type: ignore[code]`. A new ignored rule or `noqa` category needs owner approval recorded
    in the PR; fix the code first. For a public argument that would shadow a builtin, choose a
    descriptive name (`ticker_type`, not `type_`) and map it to the wire name.
13. **Unit tests never reach Massive.** They use the fake servers in `tests/_fakes/` and payload
    fixtures under `tests/data/<test owner path>/` (`tests/data/datasets/symbols/` for
    `tests/datasets/test_symbols.py`). Only `*_integration.py` tests go live.
14. **One wire package.** `src/deephaven_massive/_massive/` owns all Massive wire access (REST,
    WebSocket, S3 flat files) and imports neither Deephaven nor the rest of the library. Its own
    tests, under `tests/massive/`, may import any `_massive` module; all other code, tests
    included, uses only `deephaven_massive._massive` and its `__all__`.
15. **Layers**, lowest first: wire; vocabulary (`_config`, `_enums`, `_errors`, `_log_limits`,
    `_types`); `_datasets`; `_tables`; `_ingest`; API (`__init__`, `_debug_log`, `_session`,
    `_query`, `_namespaces`). A module imports only its own layer or lower, under `TYPE_CHECKING`
    too; pass objects down.
16. **`tests/test_layout.py` is the owner's.** It enforces the mirror, wire, and layer rules.
    Never widen its layer map, allow lists, or exceptions to pass a change; ask the owner.
17. **Never edit generated or vendored files by hand.** `_massive/spec/*.json` changes only
    through the refresh, with hand input only in `refresh/lexicon.py`, `overrides.py`, and
    `exclusions.py`, each entry with a reason; `vendor/massive/` only through `refresh --fetch`;
    `docs/datasets/` only through `render_reference.py`, committed in the same PR as its cause.
    The procedure, including wrong vendor descriptions, is in `docs/dev/spec-refresh.md`.
18. **Placement and names.** A module holds one subject, named in the first line of its
    docstring; `DESIGN.md` § 4.1 says where new code goes and lists the pattern words no module
    name uses. Every name (function, argument, column, environment variable, pytest marker) says
    what the thing is or does, in words a newcomer knows: no abbreviation you coined.
19. **No `None` mode sentinels.** Select a mode with an enum member; `None` means only absent.
20. **Dates are ISO strings.** Every calendar-date column, vendor dates included, stores
    `yyyy-MM-dd` text, never a `LocalDate` or a value computed on read: one type joins with every
    other date. Instants stay `Instant`s. Every dataset table is built by `_tables/columns.py`
    `dataset_table`, which shares one string per date, and the library owns the names `Date` and
    `Timestamp*`.
21. **No planning references outside `plans/`.** Code, comments, docstrings, test names, error
    messages, CI step names, and config never cite the planning documents, their sections,
    planned-PR names, or decision numbers. State the behavior or the reason directly.
22. **Every change logs its decisions and failures, with their values, and never a secret**:
    the rules and their checks are in the `src/deephaven_massive/_debug_log.py` docstring.
23. **Supply Massive's data; never repair it.** The library returns what Massive sends,
    converted faithfully: units, types, column names, and canonical symbols. It never fills,
    corrects, filters out, re-stamps, or replaces values to make wrong or missing vendor data
    look right. When Massive's data is wrong, record the evidence and draft a report to Massive
    for the owner to review, instead of adding a workaround.

## Documentation standard

Docstrings serve two readers: a person in an IDE or the Deephaven console, and an agent that
knows nothing but the code. Write clear, edited prose; a one-line docstring on anything with
behavior worth knowing is a defect.

- **Modules**: what the module is for, what it owns, how it fits with its neighbors. The package
  `__init__.py` is the front door: what the library does, the public API with a short example,
  where to read next, and any status a reader must know.
- **Classes**: the concept, its lifecycle, who creates it, its thread-safety; public ones include
  a short example.
- **Functions and methods**: what they do and why one would call them; every argument (meaning,
  units, accepted values, default); the return value (for a table, its columns); every exception
  and when; side effects (network calls, threads, subscriptions, tables created); blocking and
  threading constraints.
- **Dataset methods and verbs** link instead of repeating. `README.md` defines each shared
  argument (`symbols`, `start` and `end`, `as_of`, `sort`, `order`, `limit`, `filters`,
  `adjusted`, `timespan`, `multiplier`, `venues`, the verb arguments) and each concept (bars,
  history sources, split adjustment, live-table lifetime) once. A docstring gives a shared
  argument one line with only this dataset's difference and `See README.md#<anchor>.`, and its
  Returns line names its columns page, `docs/datasets/<dataset>.md`. A dataset docstring runs
  about 15 to 25 lines: the summary, what a row is and what is unusual, one Args line per
  argument, Returns, exact Raises, and one or two example lines. `tests/test_docs.py` fails on a
  cited anchor or page that does not exist.
- **User documentation is one file.** Everything hand-written for users goes in `README.md`;
  `docs/datasets/` is generated; contributor material goes in `CONTRIBUTING.md` or `docs/dev/`.
  Every Python block in the README runs in `tests/test_docs.py`, so an example is real code.
- **Examples** are real code that runs against the documented API. **Never** restate the
  signature, use filler ("This function…"), or leave a term undefined.
- **Configuration text is documentation too**: config comments, pytest marker descriptions
  (`name(args): meaning`, in plain sentences), and CLI help text meet this same standard.

## Deephaven invariants

These prevent deadlocks and leaks.

- **Enter the execution context on every library thread**, before any Deephaven call, through a
  fresh wrapper (`with entered(exec_ctx):`). A shared wrapper entered on two threads loses one
  thread's context, and Deephaven raises `No execution context`.
- **Update-graph (UG) lock first, then `registry_lock`.** `registry_lock` is a leaf: while holding
  it, make no Deephaven call, network call, or blocking wait. Table operations such as
  `where_in`, `last_by`, `Table.snapshot()`, and `deephaven.arrow.to_table` may take the UG lock,
  so never run one under `registry_lock`.
- **Release refreshing tables under the UG lock**: every liveness scope through `release_scope`,
  never a bare `scope.release()`, and never under a library lock. Where the lock is off-limits (a
  listener, a shutdown callback, a publish lock), hand the scope to `SessionCore.release_later`.
- **Never block in a table listener.** It runs on a UG thread holding the UG lock: compute the
  diff, hand it to a worker, return. No table operations, I/O, or waits on library threads.
- **The console holds the UG lock.** Deephaven runs every console command under the exclusive
  UG lock, so every call must work there: only UG threads are refused, and a call holding the
  lock never waits for an update cycle or on a thread that may need the lock (the diagnostics
  writer, a poller, another caller's stream open, the reaper).
- **Every live result has its own liveness scope.** Create its tables inside its `LivenessScope`
  on the caller thread; a refreshing table made outside an explicit scope lives as long as the
  console session.
- **Streams tear down from the blink table's `on_shutdown_callback`**: topic reference counts and
  flush routing are released there, not when a result record is released. The callback only
  updates the registry, queues a failure's pin release on the reaper, and calls
  `call_soon_threadsafe`; it never sends and never calls Java.
- **Fail a blink table only through `BlinkPublisher`.** Library code creates every publisher
  with `blink_publisher` (`_tables/publisher.py`), never with `table_publisher`. Deephaven queues
  a published failure until the next update cycle; if another thread destroys the table
  meanwhile, Deephaven can deliver the failure twice and exit the process. `publish_failure`
  pins the table until the failure has been delivered.
- **Connection state is loop-confined.** Only a connection's client thread touches its
  subscription sets; other threads use `loop.call_soon_threadsafe` with plain data.
- **Never rely on the atomicity of `dict`, `set`, or `list` operations.** Every shared container
  is behind a lock or has a single owning thread.

## Tests and the embedded JVM

- `tests/conftest.py` starts one embedded Deephaven server per pytest process before collection,
  so test modules may import `deephaven` at module level. Under `-n`, each xdist worker starts
  one and the controller starts none; `-n` with the live tests is refused. Never start or stop
  another `Server` in that process (a subprocess may). Read the comments on its `trylast` hook
  and JVM shutdown block before touching either. An `hs_err_pid*.log` file is a JVM crash, and
  a `jvm_fatal_pid*.log` file holds the message of a JVM fatal error that exited the process:
  investigate either.
- The JVM raises SIGSEGV on purpose: never call `faulthandler.enable()` once it runs. Any process
  that starts a `Server` leaves through `os._exit`, as `scripts/render_reference.py` does.
- Take the execution context, table helpers, and fakes from fixtures (`dh_exec_ctx`,
  `dh_to_pylist`, `load_fixture`, `make_csv_gz`). Drive state through the fakes and explicit
  flush or poll calls, never `time.sleep()`.
- To delete rows from a keyed `input_table`, pass a static key table, never a live view of the
  same table: that can hang against the update-graph refresh thread.

## Where to look

| Task | Read first |
|---|---|
| Your planned PR | `plans/rework/PLAN.md`, then its GitHub issue |
| Where new code goes; the layout cleanup and its feature pause | `plans/rework/DESIGN.md` § 4.1, `plans/rework/LAYOUT.md` |
| Design, by topic | `plans/rework/DESIGN.md`: § 3 exports, § 4 layout and layers, § 6 session and liveness, § 7 and § 11 API and enums, § 8 datasets, § 13 connections, § 14 fan-out, § 18 threads and locks, § 19 wire and schemas, § 20 tooling |
| PR descriptions | `docs/dev/pull-requests.md` |
| Vendor changes and drift | `docs/dev/spec-refresh.md` |
| Live tests | `docs/live-tests.md` |
| Cutting a release | `docs/dev/releasing.md` |
| User docs and the dataset reference | `README.md`, `docs/datasets/README.md`, `CONTRIBUTING.md` |
