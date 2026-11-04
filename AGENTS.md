# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables: a Python library managed with `uv`. This file wins over every other document.
The user docs (`README.md` and `docs/guide/`) and the public docstrings are the user contract,
enforced by tests; `docs/dev/decisions.md` records why the contract is what it is. The library
has no users yet, so nothing is kept, aliased, or shimmed for compatibility: a rename is an
ordinary edit.

## Commands

Run everything from the repository root, through `uv run` (a hook blocks bare `python`,
`pytest`, `ruff`, `mypy`, and `pip`). The tests need JDK 17 or later. `CONTRIBUTING.md` has
setup, whole-suite, and build commands.

```bash
./scripts/check.sh fast                  # ruff (fixing what it can) and mypy
uv run pytest tests/test_layout.py -q    # one file, no coverage, debugger-friendly
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -q   # free-threaded, own venv
uv run python -m deephaven_massive._massive.refresh   # regenerate _massive/spec/*.json; no network
uv run python scripts/render_reference.py             # regenerate docs/datasets/
```

- `uv.lock` is not committed and `check.sh` upgrades on every run, so a new ruff or mypy release
  can flag code you did not touch. Fix it in your PR, or propose an ignore for owner approval.
- Live tests (`tests/**/test_*_integration.py`) call Massive with a real key. Agents write them
  but never run one with a key; `docs/live-tests.md` is their runbook.

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
9. **Contract changes are the owner's.** A change to a public signature, default, column, or
   failure behavior needs an owner decision: a new `docs/dev/decisions.md` entry, with the user
   docs, docstrings, and tests updated in the same PR. A change that keeps the documented
   behavior and changes only how it is achieved cites the decision it follows, and proceeds.
10. **Tests mirror the package**, one leading underscore dropped per path component
    (`_massive/rest/client.py` → `tests/massive/rest/test_client.py`). The exceptions are
    `tests/_fakes/` (fake servers, untested) and `tests/_live/` (live-test fixture logic, tested
    under `tests/live/`); another helper package needs owner approval.
11. **Coverage is 100%** of statements and branches. Delete a dead defensive branch instead of
    covering it. `# pragma: no cover` needs a reason on its line and owner approval in the PR.
12. **Every suppression has a reason** (each ruff ignore, `# noqa`, `# type: ignore[code]`), and
    a new ignored rule or `noqa` category needs owner approval. Fix the code first: a public
    argument that would shadow a builtin gets a descriptive name (`ticker_type`, not `type_`)
    mapped to the wire name.
13. **Unit tests never reach Massive.** They use the fakes in `tests/_fakes/` and payload
    fixtures under `tests/data/<test owner path>/`.
14. **One wire package.** `src/deephaven_massive/_massive/` owns all Massive access (REST,
    WebSocket, S3 flat files) and imports neither Deephaven nor the rest of the library. Outside
    `_massive` and `tests/massive/`, code uses only `deephaven_massive._massive.__all__`.
15. **Layers**, lowest first: wire; vocabulary (`_config`, `_enums`, `_errors`, `_log_limits`,
    `_types`); `_datasets`; `_tables`; `_ingest`; API (`__init__`, `_debug_log`, `_session`,
    `_query`, `_namespaces`). A module imports only its own layer or lower, under `TYPE_CHECKING`
    too; pass objects down.
16. **`tests/test_layout.py` is the owner's.** It enforces rules 10, 14, and 15. Never widen its
    layer map, allow lists, or exceptions to pass a change; ask the owner.
17. **Never edit generated or vendored files by hand**: `_massive/spec/*.json` (through the
    refresh; hand input only in `refresh/lexicon.py`, `overrides.py`, `exclusions.py`, each entry
    with a reason), `vendor/massive/` (through `refresh --fetch`), and `docs/datasets/` (through
    `render_reference.py`, committed with its cause). See `docs/dev/spec-refresh.md`.
18. **Placement and names.** A module holds one subject, named in the first line of its
    docstring; new code goes where that subject is, or in a new module, never in a neighbor
    whose summary would grow "…, and X". No pattern-word module names (`base`, `core`, `common`,
    `helpers`, `util`). A module over about 1,000 lines is split or says why it is one subject.
    An exception lives where it is raised, or in `_errors.py` when several modules need it.
    `queries.py` is the one public module. Every name says what the thing is or does, in words
    a newcomer knows.
19. **No `None` mode sentinels.** Select a mode with an enum member; `None` means only absent.
20. **Dates are ISO strings.** Every calendar-date column, vendor dates included, stores
    `yyyy-MM-dd` text, never a `LocalDate` or a value computed on read; instants stay
    `Instant`s. Every dataset table is built by `_tables/columns.py` `dataset_table`, and the
    library owns the column names `Date` and `Timestamp*`.
21. **No decision or planning references in code**, comments, docstrings, test names, error
    messages, CI step names, or config: state the behavior or the reason. Only the live tests'
    `verifies` and `settles` markers cite numbers, from `docs/dev/verification-items.md`.
22. **Log every decision and failure, with its values, and never a secret**: the rules are in
    the `src/deephaven_massive/_debug_log.py` docstring.
23. **Supply Massive's data; never repair it.** Convert faithfully (units, types, column names,
    canonical symbols), and never fill, correct, filter out, re-stamp, or replace values to make
    wrong or missing vendor data look right. When Massive's data is wrong, record the evidence
    and draft a report to Massive for the owner instead of a workaround.
24. **One bad symbol or row never fails the table.** It is skipped or kept with a null, logged
    once per distinct value, and listed in a live table's `m.streams()` `Error`. Each cause has
    its own `dm.OnError` setting in `MassiveConfig` (`LOG` default, `IGNORE`, `RAISE`); a new
    cause gets a new setting, never a hard-coded failure or a silent skip. Only a problem that
    affects every row fails the table (authentication, a closed session, a plan without the
    dataset, a library defect). A `str` or list passed as `symbols` is code, not data: a value
    in it that is not Massive's exact ticker raises `ValueError`. Decisions 66 and 81 hold the
    details.
25. **No probe connections, no exposed secrets.** No connection is opened to test anything.
    Sessions and queries expose no introspection attributes (`name`, `config`, `dataset`), so
    the API key stays out of objects users leave in console globals.

## Working in parallel

Several agents work at once, and every collision costs a full check and a paid CI run.

- **Before starting**, check `gh pr list` and `gh pr diff <n> --name-only` for open PRs that
  change your files. If yours needs another PR's content, stack on it: branch from its branch,
  open your PR with `--base <its branch>`, and link them with `gh stack link <PRs bottom to
  top>` (`gh extension install github/gh-stack` once). If you only share a file, stay on `main`
  and expect one merge later.
- **Never rewrite a pushed branch**: no force-push, no rebase, and no `gh stack submit`,
  `push`, `sync`, or `rebase`. GitHub's own stack rebases are fine; after one, `git pull
  --rebase`.
- **Merge `main` (or your base branch) only to resolve a conflict**, never to stay current.
  Read both sides of every conflict, never take one wholesale, and say in the PR body how you
  resolved it. When an open PR renames a public name, merge `main` and rerun the tests once it
  lands, because a text merge cannot catch the old name in your new code.
- **The pre-push hook is the check**: never run `./scripts/check.sh` yourself just before a
  push.
- **Numbers**: a new decision or verification item takes the number the coordinator reserved
  for you, or else the next after the highest on `main` and in every open PR's diff; name it in
  the PR body. Both files are append-only: a changed decision gets a new entry, and the old
  one becomes "superseded by N".
- **Keep context small**: `grep -n` and read only the section you need, keep command output
  short (`-q`, `| tail`, `--stat`), and never re-read a file you just edited.

## Documentation

Docstrings serve a person in an IDE or the Deephaven console and an agent that knows only the
code: a one-line docstring on anything with behavior worth knowing is a defect. User docs have
three parts: `README.md`, the landing page from install to everyday use; `docs/guide/`, pages
looked up for specific cases; and `docs/datasets/`, generated. Follow the `writing-user-docs`
skill for any user page, or a change to behavior one describes, and the `writing-docstrings`
skill for docstrings and other text in code.

## Deephaven invariants

Read these before touching threads, locks, listeners, liveness, or live tables. Each one
prevents a deadlock, a leak, or a JVM crash.

- **Lock order**: the update-graph (UG) lock, then a connection's flush lock, then a stream's
  publish lock, then `registry_lock`, then the live set's lock. A connection group's lock comes
  only after the UG lock and is never held with `registry_lock`.
- **`registry_lock` is a leaf**: under it, no Deephaven call, network call, blocking wait, or
  table operation that may take the UG lock (`where_in`, `last_by`, `Table.snapshot()`,
  `deephaven.arrow.to_table`).
- **Enter the execution context on every library thread**, before any Deephaven call, through a
  fresh wrapper (`with entered(exec_ctx):`): a shared wrapper entered on two threads loses one
  thread's context.
- **Release refreshing tables under the UG lock**, every liveness scope through `release_scope`,
  never a bare `scope.release()`, and never under a library lock. Where the UG lock is
  off-limits (a listener, a shutdown callback, a publish lock), use `SessionCore.release_later`.
- **Never block in a table listener.** It runs on a UG thread holding the UG lock: compute the
  diff, hand it to a worker, return.
- **The console holds the UG lock** for every command, so every call must work there: only UG
  threads are refused, and a call holding the lock never waits for an update cycle or on a
  thread that may need the lock.
- **Every live result has its own liveness scope**, created on the caller thread; a refreshing
  table made outside one lives as long as the console session.
- **Streams tear down from the blink table's `on_shutdown_callback`**, which only updates the
  registry, queues a failure's pin release on the reaper, and calls `call_soon_threadsafe`; it
  never sends and never calls Java.
- **Fail a blink table only through `BlinkPublisher`** from `blink_publisher`
  (`_tables/publisher.py`), never `table_publisher`: `publish_failure` pins the table until the
  failure is delivered, because a table destroyed first can get it twice and exit the process.
- **The library never uses input tables**: every table it writes is a `blink_publisher` blink
  table or a shape of one, and a keyed one is `last_by` on its keys.
- **Create a table under the UG shared lock**, for the creation only, as `blink_publisher` does:
  a table registered while a cycle runs and changed in that same cycle is fatal.
- **Shared state has one owner or one lock.** Only a connection's client thread touches its
  subscription sets (others use `loop.call_soon_threadsafe` with plain data), and no code relies
  on the atomicity of `dict`, `set`, or `list` operations.

Tests run in one embedded JVM per process; follow the `writing-tests` skill for any test.
