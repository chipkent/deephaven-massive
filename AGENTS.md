# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables: a Python library managed with `uv`. This file wins over every other document.
The user docs (`README.md` and `docs/guide/`) and the public docstrings are the user contract,
enforced by tests; `docs/dev/decisions.md` records why the contract is what it is. The library
has no users yet, so nothing is kept, aliased, or shimmed for compatibility: a rename is an
ordinary edit.

## Commands

Run everything from the repository root, through `uv run`. The tests need JDK 17 or later.
`CONTRIBUTING.md` has the setup commands and the whole-suite and build commands.

```bash
./scripts/check.sh                       # full gate: ruff, format, mypy; the whole suite with 100% coverage only if the change can affect the tests
./scripts/check.sh fast                  # ruff (fixing what it can) and mypy only
uv run pytest tests/test_layout.py -q    # one file, no coverage, debugger-friendly
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -q   # free-threaded
uv run python -m deephaven_massive._massive.refresh   # regenerate _massive/spec/*.json; no network
uv run python scripts/render_reference.py             # regenerate docs/datasets/
```

- The free-threaded run keeps its own `.venv-314t`: a plain `--python 3.14t` replaces `.venv`.
- `uv.lock` is not committed and `check.sh` upgrades on every run, so a new ruff or mypy release
  can flag code you did not touch. Fix it in your PR, or propose an ignore for owner approval.
- Live tests (`tests/**/test_*_integration.py`) call Massive with a real key. Agents never run
  one with a key; the owner runs them, and outside testers run them with their own keys through
  `docs/tester-guide.md`. `docs/live-tests.md` is their runbook.

## Hard rules

1. **Pull requests only.** Never commit or push to `main`, and never merge a PR (`gh pr merge`
   or otherwise): only the owner merges.
2. **Never bypass a hook**: no `--no-verify`, no `core.hooksPath` tricks, no skipped failing
   check. When a hook fails, fix the cause.
3. **Branches and commits.** Branches are `<type>/<topic>`, cut from `origin/main` after
   `git fetch`. Commits and PR titles follow Conventional Commits: `feat:`, `fix:`, `docs:`,
   `test:`, `refactor:`, `ci:`, `chore:`, and their wording meets the Title section of
   `docs/dev/pull-requests.md`.
4. **Review, then push through the check.** Before opening a PR and before every push to one,
   run the `pre-push-review` skill and fix its findings; the push's pre-push hook then runs
   `./scripts/check.sh`, which must pass. Every CI job must pass, the required free-threaded
   3.14t job too: locking must work without the GIL.
5. **Push in batches.** Each push to a PR branch runs paid CI and review bots: push once when the
   PR opens, then once per finished batch of fixes. Never push work in progress or to see
   whether CI passes. Merge `main` into a PR branch only as "Working in parallel" says.
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
9. **Contract changes first.** A change to a public signature, default, column, or failure
   behavior is an owner decision: add an entry to `docs/dev/decisions.md` and update the user
   docs (`README.md` and `docs/guide/`), the docstrings, and the tests in the same PR. A change
   that keeps the documented behavior and changes only how it is achieved cites, in its PR, the
   existing decision it follows, and proceeds.
10. **Tests mirror the package**, one leading underscore dropped per path component
    (`_massive/rest/client.py` → `tests/massive/rest/test_client.py`). Two helper packages sit
    outside the rule: `tests/_fakes/`, the fake servers, which have no tests, and `tests/_live/`,
    the live tests' fixture logic, each module tested at its mirrored path (`tests/live/`). A
    new helper package, or tests for another one, needs owner approval.
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
    docstring; new code goes where that line names its subject, or in a new module, never in a
    neighbor whose summary grows "…, and X". Module names are subject nouns, never pattern
    words (`base`, `core`, `common`, `helpers`, `util`). A rule has one body, in the module that
    owns its concept; an exception lives where it is raised, or in `_errors.py`
    (`_massive/net/errors.py`) when several modules or a lower layer need it. A module over
    about 1,000 lines is split or says why it is one subject. `queries.py` is the one public
    module; another needs owner approval. Every name says what the thing is or does, in words
    a newcomer knows: no abbreviation you coined.
19. **No `None` mode sentinels.** Select a mode with an enum member; `None` means only absent.
20. **Dates are ISO strings.** Every calendar-date column, vendor dates included, stores
    `yyyy-MM-dd` text, never a `LocalDate` or a value computed on read: one type joins with every
    other date. Instants stay `Instant`s. Every dataset table is built by `_tables/columns.py`
    `dataset_table`, which shares one string per date, and the library owns the names `Date` and
    `Timestamp*`.
21. **No decision or planning references in code.** Code, comments, docstrings, test names,
    error messages, CI step names, and config never cite decision numbers, planning documents,
    or planned-PR names: state the behavior or the reason directly. Only the live tests'
    `verifies` and `settles` markers name verification items, by their number in
    `docs/dev/verification-items.md`.
22. **Every change logs its decisions and failures, with their values, and never a secret**:
    the rules and their checks are in the `src/deephaven_massive/_debug_log.py` docstring.
23. **Supply Massive's data; never repair it.** The library returns what Massive sends,
    converted faithfully: units, types, column names, and canonical symbols. It never fills,
    corrects, filters out, re-stamps, or replaces values to make wrong or missing vendor data
    look right. When Massive's data is wrong, record the evidence and draft a report to Massive
    for the owner to review, instead of adding a workaround.
24. **One bad symbol or row never fails the table.** A problem confined to one symbol or one
    row (a symbol that is not Massive's exact ticker, one Massive does not list, one a path cannot
    serve, a value the library cannot read) affects only that symbol or row. By default it is
    skipped or kept with a null, logged once per distinct value (at WARNING, or at ERROR for a
    Massive spelling the library cannot read, a library gap), and the table keeps running; a live
    table also lists the symbols it skips in its `m.streams()` `Error`. Each cause has its own
    `dm.OnError` setting in `MassiveConfig`, named for the cause, as `noncanonical_symbols` and
    `unreadable_symbols` are: `LOG` (the default), `IGNORE` (logged only at DEBUG), or `RAISE`
    (fail). A new cause gets a new setting, never a hard-coded failure or a silent skip. Short of
    `RAISE`, only a problem that affects every row fails the whole table: authentication, a
    closed session, a plan that lacks the dataset, a library defect; a symbol table whose every
    row is bad is refused at the call. A `str` or list passed as `symbols` is the caller's code,
    not data: a value in it that is not Massive's exact ticker raises `ValueError` at the call,
    whatever the setting.
25. **No probe connections, no exposed secrets.** No connection is ever opened to test anything.
    Sessions and queries expose no introspection attributes (no `name`, `config`, or `dataset`),
    so the API key stays out of objects users leave in console globals.

## Working in parallel

Several agents work at once, and every collision costs a full check and a paid CI run. Follow
the `working-in-parallel` skill when you start a branch, stack on another PR, merge, or take a
decision or verification-item number. In short: stack on any open PR that changes your files;
never rewrite a pushed branch (no force-push, no rebase once pushed); merge `main` or your base
branch only to resolve a conflict, never to stay current; and never run the check yourself just
before a push, since the pre-push hook is the check.

## Documentation

Docstrings serve a person in an IDE or the Deephaven console and an agent that knows only the
code: a one-line docstring on anything with behavior worth knowing is a defect. User docs have
three parts: `README.md`, the complete landing page that takes a new user from install to
everyday use; `docs/guide/`, reference pages looked up for specific cases; and `docs/datasets/`,
generated. `docs/dev/user-docs.md` is their standard. Follow the `writing-user-docs` skill
whenever you write, move, or review a user page, or change behavior one describes, and the
`writing-docstrings` skill for docstrings and other text in code.

## Deephaven invariants

These prevent deadlocks, leaks, and JVM crashes.

- **Lock order**: the update-graph (UG) lock, then a connection's flush lock, then a stream's
  publish lock, then `registry_lock`, then the live set's lock. A connection group's lock comes
  only after the UG lock and is never held with `registry_lock`.
- **`registry_lock` is a leaf**: under it, make no Deephaven call, network call, or blocking
  wait, and no table operation that may take the UG lock (`where_in`, `last_by`,
  `Table.snapshot()`, `deephaven.arrow.to_table`).
- **Enter the execution context on every library thread**, before any Deephaven call, through a
  fresh wrapper (`with entered(exec_ctx):`): a shared wrapper entered on two threads loses one
  thread's context, and Deephaven raises `No execution context`.
- **Release refreshing tables under the UG lock**, every liveness scope through `release_scope`,
  never a bare `scope.release()`, and never under a library lock. Where the lock is off-limits (a
  listener, a shutdown callback, a publish lock), hand the scope to `SessionCore.release_later`.
- **Never block in a table listener.** It runs on a UG thread holding the UG lock: compute the
  diff, hand it to a worker, return. No table operations, I/O, or waits on library threads.
- **The console holds the UG lock** for every command, so every call must work there: only UG
  threads are refused, and a call holding the lock never waits for an update cycle or on a
  thread that may need the lock (the diagnostics writer, a poller, a stream open, the reaper).
- **Every live result has its own liveness scope.** Create its tables inside its `LivenessScope`
  on the caller thread; a refreshing table made outside one lives as long as the console session.
- **Streams tear down from the blink table's `on_shutdown_callback`**, which releases topic
  reference counts and flush routing. It only updates the registry, queues a failure's pin
  release on the reaper, and calls `call_soon_threadsafe`; it never sends and never calls Java.
- **Fail a blink table only through `BlinkPublisher`**, created with `blink_publisher`
  (`_tables/publisher.py`), never `table_publisher`. Deephaven delivers a published failure at
  the next cycle and, if another thread destroys the table first, can deliver it twice and exit
  the process; `publish_failure` pins the table until delivery.
- **The library never uses input tables.** They are for people editing data by hand, not for
  code or volume: every table the library writes is a `blink_publisher` blink table or a shape
  of one, and a keyed one is `last_by` on its keys. Tests may stand in for a user's watchlist
  with an input table made by `dh_input_table`.
- **Create a table under the UG shared lock**, as `blink_publisher` and `dh_input_table` do, for
  the creation only: a table registered while a cycle runs and changed in that same cycle is
  fatal. Release the lock before a blocking input-table add or delete, which raise under it.
- **Shared state has one owner or one lock.** Only a connection's client thread touches its
  subscription sets (other threads use `loop.call_soon_threadsafe` with plain data), and no code
  relies on the atomicity of `dict`, `set`, or `list` operations.
- **One embedded JVM per test process.** `tests/conftest.py` starts it; never start or stop
  another `Server` there, and never call `faulthandler.enable()` once it runs (the JVM raises
  SIGSEGV on purpose). Follow the `writing-tests` skill whenever you write or debug a test.
