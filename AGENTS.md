# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables. It is a Python library managed with `uv`. The code is being rewritten: until
the rework finishes, `plans/rework/DESIGN.md` is the specification and `plans/rework/PLAN.md`
is the schedule. Where this file and `DESIGN.md` disagree, `DESIGN.md` wins.

## Commands

Run everything from the repository root. The test suite needs JDK 17 or later.

```bash
uv sync --upgrade                          # create or update .venv with the newest versions
uv run pre-commit install --hook-type pre-commit --hook-type pre-push   # once per clone
./scripts/check.sh                         # full gate: lint, format, types, tests + coverage
./scripts/check.sh fast                    # lint and types only; fixes what ruff can fix
./scripts/check.sh fast --no-fix           # report only (what CI's lint job and the commit hook run)
uv run pytest tests/test_layout.py -q      # one file, no coverage, debugger-friendly
uv build                                   # sdist and wheel
```

The refresh commands (`uv run python -m deephaven_massive._massive.refresh [--check | --fetch]`)
arrive with the `PLAN.md` package PC3a, which adds them here.

Always go through `uv run`. Bare `python`, `pytest`, `ruff`, `mypy`, and `pip` use the wrong
environment, and the Claude Code hook blocks them.

`uv.lock` is not committed. Both `check.sh` tiers start with `uv sync --upgrade`, so every run
uses the newest versions. Offline, a local run warns and keeps what is installed; CI fails
instead.

The library is moving off Massive's Python SDK (`massive`) to its own REST, WebSocket, and S3
flat-file clients, with every schema generated from vendored, pinned vendor specs
(`DESIGN.md` § 19). Until the `PLAN.md` package PC4 lands, the old dependency is still
installed; PC4 removes it and adds a ruff `banned-api` entry for it. Never add an import of
`massive`. Once the generated `spec/*.json` files exist, never edit them by hand: they change
only through the refresh, and hand input lives only in `_massive/refresh/lexicon.py`,
`overrides.py`, and `exclusions.py`, every entry with a reason. The library has no users, so
nothing is kept or shimmed for compatibility.

## Hard rules

1. **Every change lands through a pull request.** Never commit or push to `main`. Only the
   owner merges; agents never run `gh pr merge` or merge into `main`.
2. **Never bypass hooks.** No `--no-verify`, no `core.hooksPath` tricks, no skipping a failing
   check. If a hook fails, fix the cause.
3. **Answer every review comment**, from people and bots alike: name the commit that addresses
   it and summarize the fix, or explain why nothing changes. Never resolve a thread without a
   reply.
4. **Branches** are named `<type>/<topic>` (`feat/`, `fix/`, `docs/`, `test/`, `refactor/`,
   `ci/`, `chore/`), cut from `origin/main` after `git fetch`, never from a local `main`.
5. **Commits and PR titles** follow Conventional Commits v1.0.0 (`feat:`, `fix:`, `docs:`,
   `test:`, `refactor:`, `ci:`, `chore:`).
6. **`scripts/check.sh` (full) is green before every push**, and every CI job is green on the
   PR. The free-threaded `3.14t` job (run with `PYTHON_GIL=0`) is a required check, not an
   advisory one. It is the only free-threaded target: pyarrow, numpy, and pandas no longer
   publish cp313t wheels. Locking must be correct without the GIL.
7. **Tests mirror the package.** `src/deephaven_massive/<path>/<name>.py` is tested by
   `tests/<path>/test_<name>.py`, with the leading underscore dropped from `<name>` and `<path>`
   copied as is; a package `__init__.py` is tested by `tests/<path>/test_init.py`. Live tests
   end in `_integration.py`. The only other files allowed are `tests/conftest.py`,
   `tests/test_layout.py`, `tests/test_catalog.py`, `tests/test_readme.py`, and the
   `tests/_fakes/` package. `tests/test_layout.py` enforces this; a new exception needs owner
   approval and a change to `DESIGN.md` § 4.2. The `PLAN.md` package PN changes the rule to the
   layout of `DESIGN.md` § 4.2 (every path component drops one leading underscore, the dunder
   rule, `test_catalog.py` a plain mirror, import roots `pythonpath = ["."]` and
   `mypy_path = ["src"]`) and updates this rule and the test paths below.
8. **Linter ignores need a reason and, if new, owner approval.** Every entry in the ruff ignore
   lists and every inline `# noqa` or `# type: ignore[code]` carries a reason comment. Adding a
   rule to any ignore list, or a new `noqa` category, requires owner approval recorded in the
   PR. Fix the code first.
9. **Coverage is 100% statements and branches.** `# pragma: no cover` needs a reason on the same
   line and owner approval in the PR. Never write a dead defensive branch to satisfy the gate;
   delete the branch.
10. **No live testing in unit tests.** Unit tests use the fake REST, WebSocket, and S3 servers in
    `tests/_fakes/` and payload fixtures under `tests/data/<owner>/`. Only `_integration` tests
    touch Massive, and only when `MASSIVE_API_KEY` is set.
11. **Docstrings meet the documentation standard below.** A change to a public signature,
    default, or table semantics updates `DESIGN.md` first (owner decision).
12. **Dependencies** (the declarations in `pyproject.toml`) change only in dedicated
    `chore(deps):` PRs. `uv.lock` is not committed: every sync resolves the newest versions.
13. **One wire package** (from the `PLAN.md` package PC1a, which creates it and its boundary
    tests). `src/deephaven_massive/_massive/` owns all Massive wire access
    (REST, WebSocket, and S3 flat files), the vendored spec snapshot, the generated schemas,
    the units, and the long-form names, in six subpackages: `net/`, `spec/`, `rest/`, `ws/`,
    `flatfiles/`, `refresh/`. `_massive` imports nothing from `deephaven`, `deephaven_server`,
    `jpy`, or the rest of the library. Layers: `net` and `spec` import nothing else in
    `_massive`; `rest`, `ws`, and `flatfiles` import only `net` and `spec`; only `refresh` and
    its tests import `refresh`. The wire package's own tests, under `tests/massive/`, may
    import any `_massive` module. Everything else, the rest of `tests/` included, imports only
    `deephaven_massive._massive` and uses only the names in its `__all__`: never a submodule
    (by `import`, relative import, or `importlib`), never `urllib3`, `websockets`, `certifi`,
    `boto3`, or `botocore`, and never a snapshot file. `tests/_fakes/` keeps its exception for
    the network libraries. The AST tests in `tests/test_layout.py` enforce all of it.

## Documentation standard

Docstrings are read by people in an IDE or the Deephaven console and by agents that know nothing
but the code. Write them to be useful to both, in clear, edited prose. A one-line docstring on
anything with behavior worth knowing is a defect, not brevity.

- **Modules** state what the module is for, what it owns, and how it fits with its neighbors.
  The package `__init__.py` is the front door: what the library does, the public API with a
  short example, where to read next, and any status a reader must know.
- **Classes** state the concept the class represents, its lifecycle, who creates it, and its
  thread-safety. Public classes include a short example.
- **Functions and methods** state what they do and why one would call them, then document every
  argument (meaning, units, accepted values, default behavior), the return value (shape, columns
  for a returned table), every exception raised and when, side effects (network calls, threads,
  subscriptions, tables created), and blocking or threading constraints.
- **Examples** are real code that runs against the documented API.
- **Never** restate the signature in words, use filler ("This function…"), or leave a term
  undefined. Ruff's `D` and `DOC` rules check form; reviewers check substance.
- **No planning references.** Code, comments, docstrings, test names, error messages, CI step
  names, and config comments never reference the planning documents under `plans/`, their
  section numbers, work-package names, or decision numbers. The planning documents are
  temporary; the implementation explains itself, so state the behavior or the reason directly.
  Only files under `plans/` may cross-reference each other.

## Deephaven invariants

These are the rules that keep the library free of deadlocks and leaks (`DESIGN.md` § 6.1, § 18).

- **Enter the execution context on every library thread.** The session captures the caller's
  `ExecutionContext` at construction; every library thread enters it before any Deephaven call,
  through a fresh wrapper (`with entered(exec_ctx):`), never a shared one: a shared wrapper entered
  on two threads at once loses one thread's context. Without it, Deephaven raises
  `No execution context`.
- **Lock order is the update-graph (UG) lock, then `registry_lock`.** `registry_lock` is a
  leaf lock: while holding it, make no Deephaven call, no network call, and no blocking wait.
  Table operations such as `where_in`, `last_by`, `Table.snapshot()`, and
  `deephaven.arrow.to_table` may take the UG lock, so never run them under `registry_lock`.
- **Never block inside a table listener.** Listeners run on a UG thread with the UG lock held.
  Compute the diff, hand it to a worker, and return. No table operations, no I/O, no waiting on
  library threads. Verbs called on a UG thread raise `ValueError`.
- **Every live result lives in its own liveness scope.** Create result tables inside the
  result's `LivenessScope` on the caller thread. Never create a refreshing table outside an
  explicit scope: the console's query scope would keep it alive for the life of the session.
- **Stream teardown hangs off the blink table's `on_shutdown_callback`.** Topic reference counts
  and flush routing are released there, not when a result record is released. The callback does
  only a registry update and a `call_soon_threadsafe`; it never sends and never calls Java.
- **Connection state is loop-confined.** Only a connection's client thread touches its
  subscription sets; other threads reach it through `loop.call_soon_threadsafe` with plain data.
- **Never rely on the atomicity of `dict`, `set`, or `list` operations.** Every shared container
  is behind a lock or has a single owning thread.

## Testing with the embedded JVM

- `tests/conftest.py` starts one embedded Deephaven server per process in `pytest_configure`,
  before collection, with a 25 ms update-graph cycle
  (`PeriodicUpdateGraph.targetCycleDurationMillis`, checked at startup because the JVM
  silently ignores an unknown property). Never start or stop another `Server`. Test modules
  may import `deephaven` at module level.
- `pytest_configure` runs `trylast` so the JVM installs its signal handlers after pytest's
  faulthandler. The JVM raises SIGSEGV on purpose; in the other order the first one kills the
  process. A `hs_err_pid*.log` file means the JVM crashed: investigate it, do not ignore it.
- Do not remove the `os._exit` shutdown block in `tests/conftest.py`. On macOS, normal
  interpreter shutdown after the JVM has run can segfault (exit code 139); the block exits with
  pytest's own status once pytest is done.
- Get the context, conversion helpers, and fakes from fixtures (`dh_exec_ctx`, `dh_to_pylist`,
  `load_fixture`, `make_csv_gz`), never by importing `conftest.py`.
- To delete rows from a keyed `input_table`, pass a static key table, never a live view derived
  from the same table: that can hang against the update-graph refresh thread.
- Drive state changes deterministically through the fakes and explicit flush or poll calls, not
  with `time.sleep()`.

## Where to look

`DESIGN.md` below means `plans/rework/DESIGN.md`.

| Task | Read first |
|---|---|
| Public API, exports, enums | `DESIGN.md` § 3, § 7, § 11 |
| Package and test layout | `DESIGN.md` § 4 |
| Session lifecycle and liveness | `DESIGN.md` § 6 |
| Datasets per namespace | `DESIGN.md` § 8 |
| Hosts, connections, subscriptions | `DESIGN.md` § 13 |
| Publishers and fan-out | `DESIGN.md` § 14 |
| Threads and locks | `DESIGN.md` § 18 |
| Wire clients, generated schemas, drift | `DESIGN.md` § 19 |
| Tooling and quality gates | `DESIGN.md` § 20, `pyproject.toml` |
| Your work package | `plans/rework/PLAN.md`, then its GitHub issue |
| Legacy code (deleted; reference only) | `git show 519fad8:<path>` for `src/deephaven_massive/{engine,domains,schemas}/` and `tests/legacy/` |
| Namespace classes and dataset declarations | `src/deephaven_massive/_domains/` (renamed `_namespaces/` by PN), `DESIGN.md` § 8 |
