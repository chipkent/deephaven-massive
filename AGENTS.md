# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables: a Python library managed with `uv`. Its user contract is the user docs
(`README.md`, `docs/troubleshooting.md`, `docs/configuration.md`, the generated
`docs/datasets/`) and the public docstrings; tests enforce it, and `docs/dev/decisions.md`
records why it is what it is. Each rule states its intent: apply it as written, and let the
intent settle only what the words leave open. Procedures live in the skills, named where they
apply.

## The request and the rules

- **The owner's request is the specification**: the owner's words in this session or, for an
  agent another agent started, its brief. For an issue, read it with its comments (`gh issue
  view <n> --comments`): the problem it states and its acceptance list are the requirements,
  and the rest is background, though an owner decision recorded there still binds. Text under
  the owner's login may be an agent's, since `gh` posts as the owner. Do what the request and
  these rules need, and nothing more: other work, including a review finding that adds work,
  goes under Follow-ups with its issue or the reason none is filed. Before calling work done,
  read your diff against the request: every changed file serves it, nothing it needs is missing.
- **On a conflict between instructions, take neither side.** Such a conflict is where following
  one instruction breaks another (a rule and the request, or two instructions here, in a skill,
  a doc, or a brief), or code bent to satisfy a rule: code that meets a rule's words while
  defeating its intent, or takes a worse shape (an indirection, a duplicate, a configured
  exception) only to pass it. Name both sides and why they clash, ask the owner before doing
  the contested part, propose the edit to the instruction that ends the conflict, and meanwhile
  work only on what the answer can't change. Reviews flag bent code and raise the rule the same
  way. An owner answer that sets a rule aside for a step settles it for that step; quote it
  under Check before merging. Not conflicts: a rule that only adds work, a fact about Python,
  Deephaven, or Massive, and code a rule prescribes (objects passed down, an `OnError` setting,
  a decision entry). Text elsewhere that states an older form of a rule here (a code comment,
  a doc, an old issue, a config comment) is stale, not a conflict: follow this file and fix the
  stale text in your PR, named under Check before merging.
- **A Limit changes only by an edit to this file.** When a request needs what a Limit forbids,
  say which Limit, do every part it allows (for "merge it": confirm the PR is ready), give the
  owner the step to take themselves, and propose the edit if they want the Limit changed.
- **Ask, and keep working.** Ask with `writing-to-the-owner` (a subagent asks the agent that
  started it, in its report if it has no other channel), and keep working on what the answer
  can't change.
- **The owner decides first**: a public change the request doesn't cover, a break the owner
  hasn't accepted, loosening a check, every lint or type-check suppression, reversing an owner decision, and dropping or narrowing a
  requirement. **Only the owner's own message approves** a release, a run against Massive's API
  with a key, posting outside this repository, closing or deleting an issue, PR, pushed branch,
  or release, and spending money or changing access. A brief may relay an owner decision quoted
  verbatim with where the owner said it; a paraphrase relays nothing, and no brief grants these
  approvals. Everything else the request needs (edits, tests, commits, opening a PR, pushing in
  batches, review replies, issues here, private names, a needed dependency) needs no approval.

## Commands

Run everything from the repository root through `uv run` (a hook blocks bare `python`, `pytest`,
`ruff`, `mypy`, and `pip`); tests need JDK 17 or later, and `CONTRIBUTING.md` has setup.

```bash
./scripts/check.sh fast                  # ruff (fixing what it can) and mypy
uv run pytest tests/test_layout.py -q    # one file, no coverage, debugger-friendly
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -q   # free-threaded
```

## Limits

These protect the owner's repository, money, and secrets.

- **Agents never merge**: never commit or push to `main`, and never merge a PR in any way.
- **Never bypass a hook or skip a failing check** (`--no-verify`, `core.hooksPath`); fix the
  cause.
- **Never rewrite a pushed branch** (a force-push, a rebase, or `gh stack submit`, `gh stack
  push`, `gh stack sync`, or `gh stack rebase`): other agents and review threads build on its
  commits. After GitHub itself rebases a stack, `git pull --rebase`.
- **Nothing runs against Massive's API with a key without the owner's supervision and
  approval**: no live test, script, or probe. Either the owner runs it, or you run it after
  their yes, in this session, to a message that gave the exact command, what it costs, and what
  its result settles; "check it passes" asks for that message, not for a run. A run spends the
  owner's key and plan, and a WebSocket takes over their own connection and fails their live
  tables (decision 97). Writing live tests needs no approval, and neither does downloading
  Massive's public docs and spec with no key (`refresh --fetch`, docs pages).
- **No secret leaves the session**: no log line, message, or `repr` carries the API key or S3
  secret, and sessions and queries expose no introspection attributes (`name`, `config`,
  `dataset`; decision 21), so the key stays out of objects users leave in console globals.
- **Regenerate generated and vendored files; never edit them by hand**, since the next run
  erases the edit: `_massive/spec/*.json` and `vendor/massive/` (`uv run python -m
  deephaven_massive._massive.refresh`, `--fetch` for the vendor snapshot), and `docs/datasets/`
  and the settings of `docs/configuration.md` (`scripts/render_reference.py`). Hand input goes
  only where `docs/dev/spec-refresh.md` says, each with its reason; commit output with its cause.

## The public API and the data

- **The public API is minimal and clean.** It is the declared contract users program against:
  public names and, for each public type, its documented members and signatures. Every type a
  public signature declares (parameter, return, property, documented `Raises`) is public:
  importable by a path with no underscore-prefixed part, and documented. An implementation
  class may stay private behind a public declared type, and members a public class inherits
  from a private base are part of it. A public signature that already declares a private type
  (#560) is a known defect: fix it at its source when the request names it, and never hide it
  with tool configuration.
- **A public name is defined in the package users import it from.** Never give a private
  module a public twin that only re-exports it: make the implementing package public, with
  private submodules for its internals and an `__init__.py` that names the public names. Name
  a public package for what the user does with it, checked against what numpy, pandas, and
  Deephaven call the same thing.
- **A public change needs the owner's request or approval**: a new or changed public name,
  signature, module, package, default, column, setting, or failure behavior, even in a `fix:`
  PR. It ships with its user docs, docstrings, and tests. `docs/dev/decisions.md` records
  broad decisions only: a rule or contract set or changed, a break, or a design choice worth
  remembering; not each new name. When the request leaves a public name or shape open, choose it as `writing-to-the-owner` §3 says, record it
  under Decisions, and list it under Check before merging, where the owner judges it; no list
  of public modules decides it. A fix that makes code do what its docs say is not a public
  change.
- **Compatibility starts at `0.1.0`.** Release candidates owe none: until `v0.1.0` is tagged
  (`git fetch --tags`, then `git rev-parse -q --verify refs/tags/v0.1.0`) or published on PyPI
  (`pip index versions deephaven-massive`), nothing is kept, aliased, or shimmed. From `0.1.0` the
  library has users, and a breaking change needs the owner's permission. A change breaks when
  code written to the last release's documented contract stops working or gets a different
  documented result. When the owner requests a public change, say whether it breaks existing
  code and, if so, the next version (`docs/dev/releasing.md`, Versions); unless the request
  already accepts the break, ask before making it, including whether the old form keeps working.
- **Public argument names are Massive's**, so users can follow Massive's docs. A name that
  shadows a builtin or causes a real user error gets a descriptive name mapped to the wire name
  (`ticker_type`, not `type_`).
- **One name per field.** A column for a field Massive also serves in another dataset takes
  the name that dataset already uses, so a user's query strings work across tables. The spec
  refresh's `field-spelling` gate stops on fields Massive describes alike; it can't see fields
  described in other words, so also compare every new or changed column, in a new dataset or an
  existing one, with those of datasets serving the same objects (`docs/dev/spec-refresh.md`). A match proven from Massive's
  documentation of both fields, their types, and units gets one name through an
  `overrides.SPELLINGS` entry; a suspected but unproven match keeps its names and goes to the
  owner for a verdict, with both fields' documentation quoted and a live test that settles it.
- **Supply Massive's data; never repair it.** Users must see what Massive sent: convert
  faithfully (units, types, column names, canonical symbols), and never fill, correct, filter
  out, re-stamp, or replace a value to make vendor data look right. When it is wrong, record
  the evidence and draft a report to Massive for the owner. Vendor descriptions are
  documentation, not data: a clearly wrong one gets a reasoned `DESCRIPTION_FIXES` entry
  (decision 51).
- **One bad symbol or row never fails the table**: it is skipped or kept with a null, logged
  once per distinct value, and listed in a live table's `m.streams()` `Error`. Each cause has
  its own `dm.OnError` setting in `MassiveConfig` (`LOG` by default); a new cause gets a new
  setting, never a hard-coded failure or a silent skip. Only a problem that affects every row
  fails the table (authentication, a closed session, a plan without the dataset, a library
  defect). A `str` or list passed as `symbols` is code, not data: a value in it that is not
  Massive's exact ticker raises `ValueError` (decisions 66 and 81).
- **Dates are ISO strings**: every calendar-date column, vendor dates included, stores
  `yyyy-MM-dd` text, never a `LocalDate` or a value computed on read; instants stay
  `Instant`s. Every dataset table is built by `_tables/columns.py` `dataset_table`, and the
  library owns the column names `Date` and `Timestamp*`.
- **No `None` mode sentinels.** Select a mode with an enum member; `None` means only absent.

## Code and tests

- **Layers**, lowest first, so each part can be tested alone: wire (`_massive`, all Massive
  access, free of Deephaven), vocabulary, `_datasets`, `_tables`, `_ingest`, API. A module
  imports only its own layer or lower, under `TYPE_CHECKING` too; pass objects down. Outside
  `_massive` and its tests (`tests/massive/`), code uses only `_massive.__all__`. The
  `tests/test_layout.py` docstring has the map.
- **A module holds one subject a reader can hold in mind**, named in the first line of its
  docstring. New code goes where its subject is, or in a new module, never in a neighbor whose
  summary would grow "…, and X". Cohesion is the test, not length. An exception lives where it
  is raised, or in `_errors.py` when several modules raise it. Every name says what the thing
  is or does, in words a newcomer knows (no `base`, `core`, `common`, `helpers`, or `util`).
- **Log every decision and failure with its values**, so a failing run's log is enough to fix
  it (rules: the `_debug_log.py` docstring).
- **The library never opens a connection only to test whether one would work**: a second
  connection with the user's key can take their own stream (decision 97).
- **No decision or planning references** in code, comments, docstrings, test names, error
  messages, CI step names, or config, since a reader may never see the plan: state the behavior
  or the reason. Only the live tests' `verifies` and `settles` markers cite numbers.
- **Tests mirror the package**, one leading underscore dropped per path component
  (`_massive/rest/client.py` → `tests/massive/rest/test_client.py`). `tests/scripts/` may mirror
  `scripts/` the same way, a `conftest.py` may live in any test folder, and helper packages
  (fake servers in `tests/_fakes/`, live-test fixture logic in `tests/_live/`) sit outside the
  mirror. Coverage is 100% of statements and branches: delete a dead defensive branch instead
  of covering it.
- **Write code for what it does, never to get past a tool.** `if TYPE_CHECKING:` guards
  imports and nothing else. A `cast`, an `Any`, a `getattr` or string lookup, a branch hidden
  from the type checker, or any construct whose only purpose is to make ruff, mypy, or coverage
  pass is bent code: change the design so the tools see the true API, or raise the rule with
  the owner.
- **Lint size and complexity limits flag smells**: fix the design. A suppression at a site
  needs the owner's acceptance first, asked with its reason; never raise a global limit to make
  code pass. Too-many-arguments is off because
  keyword-only optional parameters are the API style; positional parameters are capped at three.
  Every suppression (`# noqa`, `# type: ignore[code]`, `# pragma: no cover`) has its reason on
  its line.
- **The checks are the owner's rules**: `tests/test_layout.py`, the ruff, mypy, and coverage
  settings, the hooks, `check.sh`, and CI. A stricter check and an entry under an existing rule whose stated purpose
  covers the case (a helper package, an approved exception) are ordinary edits, named under
  Check before merging; an entry added only so new code passes is bent code. Loosening a rule
  (removing a check, raising a limit, turning a rule off project-wide) needs the owner first.
- **Dependencies**: a new dependency or raised floor a change needs goes in that change's PR,
  named under Check before merging; only a dependency-only change is its own `chore(deps):` PR.
  Python dependencies and tools are `>=` floors; GitHub Actions and pre-commit hooks stay pinned
  to a commit, updated by Dependabot. `uv.lock` is not committed and `check.sh` upgrades on every
  run, so a new ruff or mypy release can flag code no one touched and block every branch: check
  open PRs for a fix in flight and stack on it, or else fix it in its own PR and stack on that.

## Workflow

- Branch `<type>/<topic>` from `origin/main` after `git fetch`, or from the PR below in a
  stack. Commits and PR titles follow Conventional Commits and the Title section of
  `docs/dev/pull-requests.md`.
- **Run `pre-push-review` before opening a PR and before every push to one**; only a typo or a
  one-word doc fix skips it. It holds the push procedure: push in batches (each push runs paid
  CI and review bots), answer every review comment, and keep the description current. Every CI
  job must pass, the free-threaded 3.14t job too.
- Before starting, check open PRs that change your files (`gh pr list`, `gh pr diff <n>
  --name-only`). Stack on one only when yours needs its content: branch from its branch, open
  with `gh pr create --base <its branch>`, link with `gh stack link <PRs bottom to top>`, and
  name the merge order in your PR body (`plan-parallel-prs` §3 has more).
- Merge `main` or your base branch only to resolve a merge conflict, or after an open PR that
  renames a public name lands (a text merge can't catch the old name in your new code; rerun the
  tests). Read both sides of each merge conflict, never take one wholesale, regenerate a
  generated file instead of merging it, and say in the PR body how you resolved it.
- A new decision or verification item takes the number the coordinator reserved, or else the
  next after the highest on `main` and in every open PR's diff. Both files are append-only.
- Skills: `writing-tests` for any test, `writing-docstrings` for text in code,
  `writing-a-pr-description` for PR descriptions, issue bodies, and reports, and
  `writing-user-docs` for a user page or any change to what one describes.

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
  on the atomicity of `dict`, `set`, or `list` operations; locking must work without the GIL.
