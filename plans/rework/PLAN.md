# deephaven-massive rework — execution plan

**Status:** approved plan, awaiting P0 merge. **Companion:** [`DESIGN.md`](DESIGN.md) is the
specification; this file is the schedule. When they disagree, `DESIGN.md` wins and this file
gets a `docs:` fix.

## Purpose and lifetime of this file

This file turns the approved rework into work packages that a fresh-context agent can execute
with nothing but `DESIGN.md`, this file, `AGENTS.md`, and the code. It is **temporary by
design**: P7 moves the durable parts (standards, invariants, workflow) into `README.md` and
`AGENTS.md`, then deletes `plans/rework/`. Nothing in this file is a long-term reference, so do
not link to it from docstrings or from `README.md`.

Citations of the form "Appendix B" through "Appendix I" refer to
[`RESEARCH.md`](RESEARCH.md), the reference material gathered during planning: the data
catalog, endpoint status, tooling research, and the bug lists for the current code.
Appendix H (the SDK evaluation) predates the decision to drop the SDK; its notes on SDK
behaviour are history, and the SDK defects it records are now requirements of the library's own
clients (DESIGN § 19).

## How progress is tracked

- One GitHub issue per work package, all children of one parent tracking issue. Each issue
  carries the package's live checklist; the checklists below are the *initial* copy.
- Each package links its issue; the parent tracking issue is #37. Live progress is tracked in
  the issues' checklists, not by ticking boxes in this file.
- A package is **done** when its PR is merged by the owner and the issue is closed with a link to
  the merged PR. Agents never merge and never close issues on their own authority.
- Packages are referred to by ID (`P3`, `PC1`) in commit bodies and PR descriptions
  (`Refs #<issue>`), never by title, so renames do not break traceability.

## Table of contents

1. [Dependency graph](#dependency-graph)
2. [Global rules for every package](#global-rules-for-every-package)
3. [Parallelism guide](#parallelism-guide)
4. [Legacy code transition](#legacy-code-transition)
5. [Package template](#package-template)
6. Library packages
   - [P0 docs: rework design and plan](#p0)
   - [P1 chore: tooling foundation](#p1)
   - [P2 refactor: core skeleton](#p2)
   - [PC1–PC4 own clients and the spec snapshot](#pc)
   - [P3 feat: REST transport and tier detection](#p3)
   - [P4 feat: WebSocket layer](#p4)
   - [P5 feat: flat-file history](#p5)
   - [P6a–P6h feat: domain datasets](#p6)
   - [P7 chore: finish rework](#p7)
   - [P8 test: live integration suite](#p8)
7. [Upstream SDK issues (optional)](#upstream)
8. [Final step: archive the fork](#archive-fork)
9. [End-to-end verification](#end-to-end-verification)
10. [Decisions recorded during planning](#decisions-recorded-during-planning)

---

## Dependency graph

```mermaid
flowchart LR
    P0["P0 docs: design + plan"] --> P1["P1 chore: tooling foundation"]
    P1 --> P2["P2 refactor: core skeleton"]
    P2 --> PC1["PC1 feat: REST client"]
    P2 --> PC2["PC2 feat: WebSocket client"]
    P2 --> PC3["PC3 feat: spec snapshot,<br/>generator, name lock"]
    P2 --> P5["P5 feat: flat-file history"]
    PC1 & PC2 & PC3 --> PC4["PC4 chore(deps): drop massive"]
    PC1 & PC3 & PC4 --> P3["P3 feat: REST + tier detection"]
    PC2 & PC3 & PC4 --> P4["P4 feat: WebSocket layer"]
    P3 & P4 & P5 --> P6["P6a–P6h feat: domain datasets<br/>(parallel)"]
    P6 --> P7["P7 chore: finish rework"]
    P7 --> P8["P8 test: live integration suite"]
    P8 --> AF["Archive the fork<br/>(owner, after shipping)"]
```

P0, P1, and P2 are merged; P5 is in review (#51). PC1, PC2, and PC3 own disjoint files and run
in parallel with each other and with P5. PC4 removes `massive` once PC3 has removed its last
import.

Critical path: `PC2 → PC4 → P4 → P6 → P7`. PC3 (the generator and the lexicons) is about as
long as PC2 and gates P3, P4, and every P6 package, so start both at once.

---

## Global rules for every package

These rules apply to every package in both repositories. Package sections only add to them.

1. **Branching.** One PR per package on a branch named `<type>/<topic>` off `main`. Never
   commit directly on `main`. Never force-push a branch another agent may have checked out.
2. **Conventional commits.** Commit subjects and PR titles follow Conventional Commits v1.0.0
   (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `ci:`, `chore:`), with the PR title given in
   each package below. Commit bodies end with the attribution lines the harness supplies.
3. **Check gate.** `./scripts/check.sh` (full tier) must be green before every push, and every
   CI job must be green on the PR, including the free-threaded `3.14t` job (`PYTHON_GIL=0`),
   which is a required check. 3.14t is the only free-threaded target: pyarrow, numpy, and pandas
   no longer publish cp313t wheels. Until P1 lands, the gate is the legacy
   `scripts/pre-commit.sh`; from P1 on, hooks enforce it automatically.
4. **Fresh-context implementers.** Each package is executed by a subagent whose only inputs are
   `DESIGN.md`, this file, `AGENTS.md`, and the code. If a package cannot be completed from those
   inputs, the fix is to improve those files (a `docs:` PR), not to pass tribal knowledge along.
5. **No merging.** Agents never run `gh pr merge`, `git merge` into `main`, or `git push` to
   `main`. Only the owner merges, always via PR.
6. **Owner approvals.** The following are outward-facing and require explicit owner approval
   each time: opening a PR or issue in this repository, filing an issue on
   `massive-com/client-python` ([Upstream SDK issues](#upstream); pull requests there are never
   opened), and any change to branch protection. Agents prepare the text and stop at the gate.
7. **Test layout** ([DESIGN § 4.2 Test layout](DESIGN.md#42-test-layout)). `tests/` mirrors
   `src/deephaven_massive/`: the module `<path>/<name>.py` is tested by
   `tests/<path>/test_<name>.py`, where `<name>` has its leading underscore removed and `<path>`
   is copied as is. So `_config.py` is tested by `tests/test_config.py`, `_domains/stocks.py` by
   `tests/_domains/test_stocks.py`, and `_catalog.py` by `tests/test_catalog.py` (which also
   holds the coverage assertions). A package `__init__.py` is mirrored by
   `tests/<path>/test_init.py`; `tests/test_init.py` holds the public-surface tests
   (`dm.__all__`, `__version__`). Live tests end in `_integration.py`. The approved exceptions
   to the mirror rule are exactly `tests/conftest.py`, `tests/test_layout.py`,
   `tests/test_catalog.py` (for its coverage assertions), `tests/test_readme.py` (the
   DESIGN § 21 examples; it mirrors `README.md`), and the `tests/_fakes/` package
   (DESIGN § 4.2; see [Decisions recorded during planning](#decisions-recorded-during-planning)).
   `tests/_fakes/` is imported only by `tests/conftest.py`, whose thin fixtures hand the fakes
   to test modules; a test module imports it directly only when no fixture can provide what it
   needs, with a comment saying why. `tests/test_layout.py` enforces the rule; a new exception
   needs owner approval and a change to DESIGN § 4.2.
8. **Linter-ignore policy.** The global `ignore` list, every `per-file-ignores` entry, and every
   inline `# noqa` or `# type: ignore[...]` needs a reason comment. Adding a *new* rule to any
   ignore list, or a new `noqa` category, requires owner approval recorded in the PR. Fix the
   code first.
9. **Coverage.** 100% statement and branch coverage over `src/deephaven_massive`. `pragma: no
   cover` is allowed only with a written reason on the same line and owner approval in the PR;
   `exclude_also` holds exactly the approved patterns from `pyproject.toml`. Never write dead
   defensive branches to satisfy the gate; delete the branch instead.
10. **Deephaven invariants.** `AGENTS.md` lists them (execution context on every library thread,
    no blocking inside listeners, the registry lock is a leaf lock, no refreshing tables outside
    an explicit liveness scope). A package that touches `src/deephaven_massive/_*.py` restates
    the invariants it relies on in its PR description.
11. **No live testing as design input** ([DESIGN § 22](DESIGN.md#22-decisions), item 19). Unit tests use fake REST/WS/S3 servers and
    fixtures shaped from the OpenAPI spec's examples and the documentation samples. Live tests
    are P8 only.
12. **Docstrings and docs sync.** Every module, class, and function, public or private, has a
    Google-style docstring that meets `AGENTS.md`'s documentation standard (purpose, arguments,
    returns, raises, side effects, threading, and an example where it helps). Minimal docstrings
    are a defect. `D` and `DOC201/501/502` check form; review checks substance. A change to a public signature, default, or
    table semantics updates `DESIGN.md` (owner decision required) and, after P7, `README.md`.
13. **One package, one concern.** If a package needs a change to a file another package owns,
    land that change first in a small separate PR (`refactor:` or `chore:`), rebase, and continue.
14. **Answer every review comment.** Reply to each review thread on a package's PR, from people
    or bots: name the commit that addresses it and summarize the fix, or explain why no change
    is made. No thread is left unanswered, and none is resolved without a reply.

---

## Parallelism guide

### Which packages can run concurrently

| Wave | Packages | Notes |
|---|---|---|
| 0–2 | P0, P1, P2 | Merged. |
| 3 | PC1, PC2, PC3, P5 | Disjoint files (see below). P5 is in review (#51); PC3 and P5 both edit `_catalog.py`, and the second to merge rebases. |
| 4 | PC4 | Serial: removes `massive` once PC1–PC3 are merged. |
| 5 | P3, P4 | Disjoint files. |
| 6 | P6a–P6h | Eight domain packages, disjoint files by construction. |
| 7 | P7 | Serial: deletes legacy code, rewrites docs, adds the catalog coverage assertions. |
| 8 | P8 | After P7; needs a key; owner runs it. |
| — | Archive the fork | Owner step after shipping. |

### How file conflicts are avoided

The module names are fixed by [DESIGN § 4.1 Source modules](DESIGN.md#41-source-modules). The
skeleton created in P2 is designed so that later packages own disjoint files:

- **Client and transport packages own distinct modules.** PC1 owns `_restclient.py`; PC2 owns
  `_wsclient.py`; PC3 owns `_specgen.py`, the generated `_restspec.py`,
  `scripts/refresh_massive_spec.py`, `scripts/massive_spec/`, and
  `.github/workflows/spec-drift.yml`, and is the package that reworks `_schema.py` and
  `_catalog.py`. P3 owns `_rest.py`, `_hosts.py`, and `_poll.py`. P4 owns `_ws.py`, `_stream.py`,
  and `_watchlist.py`. P5 owns `_flatfiles.py`. Each owns its mirrored test file(s) and a
  fixture directory `tests/data/<module>/`. The fake servers follow the clients: P1 created the
  empty `tests/_fakes/__init__.py`, PC1 owns `tests/_fakes/rest.py`, PC2 `tests/_fakes/ws.py`,
  and P5 `tests/_fakes/s3.py`. Each also adds its own thin fixture to `tests/conftest.py` as a
  separate, self-contained block (one fixture function and its import), so the three edits are
  disjoint hunks. `_session.py` exposes
  one factory hook per transport that P2 stubs (`NotImplementedError`) and each transport
  package fills; the hooks are separate functions, so three PRs can edit `_session.py` without
  overlapping hunks. If overlap is unavoidable, P3 lands first, then P5, then P4.
- **Domain packages own one module each.** P6x edits only `_domains/<domain>.py`,
  `tests/_domains/test_<domain>.py`, and `tests/data/<domain>/`. P6h owns
  `_domains/partners.py`, which holds the four vendor classes. P6g owns
  `_domains/reference.py`, `_domains/indicators.py`, and `_domains/economy.py`, and is the one
  domain package that also edits `_session.py`: it fills `MassiveSession.snapshots()`, because
  DESIGN places the unified snapshot on the session rather than in a namespace. The dataset's
  record, `reference.snapshots`, is declared in `_domains/reference.py` like every other
  record (DESIGN § 8.11), so `_catalog.datasets()` finds it without a `_catalog.py` edit.
- **No shared registry file is edited by domain packages** (P6g's `_session.py` edit above
  excepted).
  Every cross-cutting table is composed from per-domain declarations rather than written
  centrally:
  - Each `_domains/<domain>.py` exports `DATASETS: tuple[DatasetSpec, ...]`. A `DatasetSpec`
    (defined in P2's `_catalog.py`, reworked by PC3) carries the dataset name, query class, REST
    path template(s), WebSocket channel(s), flat-file prefix(es), schema overrides, timestamp
    units per transport and field, the `start`/`end` wire parameter pair, default `PollMode`,
    and the page size.
  - `_catalog.py` aggregates `DATASETS` from a **fixed module list written in P2** (every domain
    module exists from P2 as a stub exporting `DATASETS = ()`), so P6 packages never touch
    `_timeunits.py` or `_schema.py`.
  - The namespace classes (`_Stocks`, `_Options`, …) live in their domain modules; `_session.py`
    instantiates them from P2 on, so P6 packages never touch `__init__.py`.
  - Catalog exclusions with `# reason:` live only in `tests/test_catalog.py` (P7).
- **Generated content is regenerated in one place.** `_restspec.py`, `_specgen.py`, and the
  vendored snapshot under `scripts/massive_spec/` change only in PC3, P7, and dedicated spec
  refresh PRs, never inside a domain PR. A domain package runs
  `scripts/refresh_massive_spec.py` offline with the snapshot unchanged, which leaves
  `_restspec.py` as it is and only appends its datasets' entries to its own lock file,
  `_names/<domain>.json`, the one generated file a domain package owns. PC3 delivers the
  lexicons for every allow-listed path, so a domain refresh never needs a lexicon edit; if one
  does, land it first in a separate PR.
- **`pyproject.toml` dependency changes** happen only in P1, PC4, P7, and `chore(deps):` PRs.
  `uv.lock` is not committed.
  Feature PRs do not add dependencies; if one is needed, open a `chore(deps):` PR first.
- **Fixture data is per package.** Never add a shared fixture file; put payloads in
  `tests/data/<owner>/` and load them through the `load_fixture` fixture from `conftest.py`.

---

## Legacy code transition

The current implementation (`src/deephaven_massive/engine/`, `domains/`, `schemas/`, and the
legacy `config.py`, `session.py`, `__init__.py`) stays in the tree until P7 as a reference for
implementers (the appendices cite its line numbers), but it is **not** part of the new gate.

- **P1** adds temporary exclusions, each tagged with the grep-able marker `LEGACY-EXCLUSION`:
  - `[tool.ruff] extend-exclude`, `[tool.mypy] exclude`, and `[tool.coverage.run] omit` for
    `src/deephaven_massive/{engine,domains,schemas}/**` and the three legacy top-level files;
  - legacy tests move unchanged to `tests/legacy/` and are excluded from collection
    (`collect_ignore_glob` in `tests/conftest.py`) and from ruff/mypy;
  - `tests/test_layout.py` skips the same paths via one `LEGACY_PATHS` constant.
- **P2** rewrites `__init__.py`, replaces the legacy `config.py` and `session.py` with
  `_config.py` and `_session.py`, and removes those three files from every exclusion list. From
  P2 on, the legacy set is exactly the three subpackages.
- **P6x** packages may read legacy normalizers for field provenance but must not import them.
- **P7** deletes the three subpackages, `tests/legacy/`, and every `LEGACY-EXCLUSION` line.
  `grep -rn LEGACY-EXCLUSION .` must return nothing after P7.

---

## Package template

Every package below has the same sections, in this order: header (PR title, branch, issue),
goal, DESIGN sections, dependencies and parallelism, files, tasks, acceptance criteria,
verification, pitfalls, out of scope, approval gates. "DESIGN §" references use the section
numbers and titles of `DESIGN.md` and link to its headings; the "DESIGN sections implemented"
line of each package is the authoritative list, and prose elsewhere may cite a section by number
alone.

---

<a id="p0"></a>
## P0 — `docs: rework design and plan`

| | |
|---|---|
| **Branch** | `docs/rework-design-plan` |
| **Issue** | Parent tracking issue #37 (P0 itself merged in #11) |

**Goal.** Commit the approved specification and this execution plan so every later package has
a stable source of truth, and open the tracking issues that hold the live checklists.

**DESIGN sections implemented.** All of `DESIGN.md` is the deliverable (Appendix A v3 with the
"Required v3 edits" applied, rewritten in O'Reilly style), including
[§ 1 Purpose and scope](DESIGN.md#1-purpose-and-scope) and
[§ 22 Decisions](DESIGN.md#22-decisions), which no code package implements; no code.

**Depends on.** Nothing. **Parallel with.** F0.

**Files.**
- Create: `plans/rework/DESIGN.md`, `plans/rework/PLAN.md` (this file).
- Modify: none. Delete: none.

**Tasks.**
- [ ] Write `plans/rework/DESIGN.md` from Appendix A plus every "Required v3 edit" (lifecycle
      once-only flag; stream lifetime bound to `on_shutdown_callback`; `HeldResults` column;
      explicit-verdict rule narrowed; stream key `(dataset, channel, symbol key)`; lock order
      sentence; `last_by` keeps every symbol ever seen; `venue` on futures factories and six query
      classes; `/v3/snapshot/options/{u}/{o}` "covered by"; Python `>=3.11`; enums only with the
      runtime `TypeError`; base Business TICK host rule with WARNING; diagnostics scope; fork list
      minus #9/#10 plus the handshake `max_connections` item (F2.6); owner answers to §20).
- [ ] Write this file.
- [ ] Draft the parent tracking issue and one issue per package (P0–P8, F0–F7) using
      each package's goal, checklist, and acceptance criteria. **Stop for owner approval before
      creating any issue.**
- [ ] After approval: create the issues, replace every `Issue: TBD` in this file, commit.
- [ ] Open the PR (title above) with the owner's approval. PR body links the parent issue.

**Acceptance criteria.**
- [ ] `DESIGN.md` contains every required edit; a reviewer can tick each of the 13 edit items.
- [ ] Every package in this file has all template sections filled.
- [ ] Every `Issue: TBD` is replaced (or the PR description says issue creation was deferred by
      the owner).
- [ ] The legacy gate (`./scripts/pre-commit.sh`) still passes; no code changed.

**Verification.**
```bash
./scripts/pre-commit.sh                       # legacy gate; must be unchanged and green
git diff --stat main -- . ':!plans/'          # must be empty: P0 touches only plans/
grep -n "Issue: TBD" plans/rework/PLAN.md     # empty after issues are created
```

**Pitfalls.** Appendix G §6 lists statements in the current `README.md`/`AGENTS.md` that are
false (not strict mypy, wrong test counts, "Zero-Copy", "lock-free"); do not copy prose from
them into `DESIGN.md`.

**Out of scope.** Any code or tooling change; `AGENTS.md`/`README.md` edits (P1 and P7).

**Approval gates.** Creating issues; opening the PR. Merge by owner only.

---

<a id="p1"></a>
## P1 — `chore: tooling foundation`

| | |
|---|---|
| **Branch** | `chore/tooling-foundation` |
| **Issue** | #12 |

**Status.** Merged (#42). Its fork source (`[tool.uv.sources]`, the `massive` dependency, and
the installed-commit steps in `check.sh` and CI) is removed by PC4; the text below records what
P1 did.

**Goal.** Replace the permissive, four-way-duplicated gate with one strict, agent-friendly
toolchain: ruff `ALL` on the latest release, strict mypy, pytest 9 native config, a single `scripts/check.sh`
entry point, git and Claude Code hooks, a CI matrix that includes free-threaded CPython, and a
no committed lockfile: every sync resolves the newest versions, including the latest SDK fork. Legacy code is excluded from the gate until P7.

**DESIGN sections implemented.** [§ 20 Quality gates](DESIGN.md#20-quality-gates) (the whole
section); [§ 4.2 Test layout](DESIGN.md#42-test-layout) (`tests/conftest.py` and
`tests/test_layout.py`); [§ 11 Enums and types](DESIGN.md#11-enums-and-types) (the `>=3.11`
floor and the CI matrix); [§ 18.4 Free-threaded specifics](DESIGN.md#184-free-threaded-specifics)
(free-threaded CI is gating); the `[tool.uv.sources]` branch source of the former § 19 (the
SDK fork, since replaced by [§ 19 Wire clients and the spec snapshot](DESIGN.md#19-wire-clients-and-the-spec-snapshot)). The exact configuration is [§ 20.1 Tooling configuration](DESIGN.md#201-tooling-configuration).

**Depends on.** P0 (merged), F0 (the `deephaven-fixes` branch must exist to be tracked).
**Parallel with.** F1–F7.

**Files.**
- Create: `scripts/check.sh`, `.claude/settings.json`, `.claude/hooks/post-edit-ruff.sh`,
  `.claude/hooks/stop-check.sh`, `.claude/hooks/pre-bash-uv-only.sh`, `tests/test_layout.py`,
  `tests/test_init.py`, `tests/_fakes/__init__.py` (empty; P3, P4, and P5 each add one module),
  `tests/legacy/` (moved files). `uv.lock` stays git-ignored.
- Modify: `pyproject.toml` (rewrite), `.pre-commit-config.yaml` (rewrite), `.gitignore` (trim to
  ~20 lines), `.github/workflows/ci.yml` (rewrite), `.github/workflows/conventional-pr-check.yml`
  (runner `ubuntu-latest`), `.github/dependabot.yml` (GitHub Actions and pre-commit
  updates only), `AGENTS.md` (rewrite), `tests/conftest.py` (rewrite), `src/deephaven_massive/__init__.py`
  (minimal: `__version__` from `importlib.metadata` only).
- Delete: `scripts/pre-commit.sh`, `scripts/install-hooks.sh`, `tests/__init__.py`.
- Move: `tests/test_*.py` (eight legacy files) → `tests/legacy/`.

**Tasks.**
- [ ] **`pyproject.toml`** per Appendix D §5 with these overrides from the settled decisions
      (decisions override the appendix where they differ):
  - `requires-python = ">=3.11"`, `target-version = "py311"`, mypy `python_version = "3.11"`,
    classifiers 3.11–3.14; PEP 639 `license = "Apache-2.0"`, `license-files = ["LICENSE"]`;
    `authors = [{name = "Chip Kent", email = "chipkent@illumon.com"}]`; `[project.urls]` to
    `github.com/chipkent/deephaven-massive`; `Typing :: Typed`.
  - `dependencies`: `massive>=2.9.0`, `deephaven-core>=42.0`, `pyarrow>=14`, `boto3>=1.28`,
    `websockets>=14`. `[tool.uv.sources] massive = { git = "https://github.com/chipkent/massive-client-python", branch = "deephaven-fixes" }` (always the latest commit).
    `orjson` as an optional extra (`[project.optional-dependencies] orjson = ["orjson>=3.9"]`).
  - `[dependency-groups] dev`: `ruff>=…` (floor only; every sync installs the latest), `mypy`, `types-boto3[s3]`, `pytest>=9`,
    `pytest-cov`, `pytest-timeout`, `pyright`, `pre-commit`, `deephaven-server>=42.0`. No
    `pytest-asyncio` ([DESIGN § 20.1](DESIGN.md#201-tooling-configuration)).
  - Ruff: `select = ["ALL", "DOC201", "DOC501", "DOC502"]`, `preview = true`,
    `explicit-preview-rules = true`, global ignores exactly: `COM812`,
    `COM819`, `W191`, `E111`, `E114`, `E117`, `D206`, `D300`, `Q000`–`Q003`, `TRY003`, `EM101`,
    `EM102`, `PLR0913`, `FBT003`, `CPY001`, each with a reason comment. **Keep enforced**: `E501`
    at 100, `ANN401`, `TC*`, `SLF001` in src, `C901` (`max-complexity = 12`), `PLR091x`,
    `PLR0917` with `max-positional-args = 3`. Tests-only ignores: `S101`, `PLR2004`, `INP001`,
    `SLF001`, `ARG001`, `ARG002`, `ARG005`, `FBT001`, `FBT002`, `D100`–`D107`. Google
    convention; `docstring-code-format = true`. **Do not** add Appendix D's `banned-module-level-imports`
    for `deephaven` ([DESIGN § 20.1](DESIGN.md#201-tooling-configuration): module-level imports; the JVM starts in `pytest_configure`).
  - Mypy: `strict`, `warn_unreachable`, `warn_unused_configs`, the eight `enable_error_code`
    entries from [DESIGN § 20.1](DESIGN.md#201-tooling-configuration). Overrides: `ignore_missing_imports` for `pyarrow.*` only;
    the fork's `massive` ships `py.typed` (F4).
  - Pytest 9 `[tool.pytest]`: `minversion = "9.0"`, `strict = true`, `filterwarnings = ["error"]`,
    `addopts = ["-ra", "--import-mode=importlib"]`, `timeout = "60"`,
    `faulthandler_timeout = "120"` (both strings: pytest and pytest-timeout declare these
    options as strings and reject TOML numbers), `faulthandler_exit_on_timeout = true`,
    `testpaths = ["tests"]`.
    No coverage flags in `addopts`.
  - Coverage: `source = ["deephaven_massive"]`, `branch = true`, `fail_under = 100`,
    `show_missing`, `exclude_also = ["if TYPE_CHECKING:", "@(typing\\.)?overload"]` only.
  - Pyright `[tool.pyright]` standard mode over `src` (non-gating).
  - `LEGACY-EXCLUSION` entries (see [Legacy code transition](#legacy-code-transition)).
- [ ] Keep `uv.lock` git-ignored. Trim `.gitignore` to the ~20 lines in Appendix G finding 13.
- [ ] **`scripts/check.sh [fast|full] [--no-fix]`**: fast = `ruff check --fix` + `ruff format` +
      `mypy src tests`; full = fast + `pytest --cov --cov-report=term-missing:skip-covered`;
      `--no-fix` = `ruff check` + `ruff format --check` (CI and hooks). `CI=true` adds
      `--output-format github`. Exit non-zero on first failure; print each step's name.
- [ ] **`.pre-commit-config.yaml`**: local hooks calling `uv run` for the fast tier at
      `pre-commit`; the full tier at `pre-push`; `pre-commit-hooks` `no-commit-to-branch`
      (`main`), `check-yaml`, `end-of-file-fixer`, `trailing-whitespace`. Pin `rev`s.
- [ ] **`.claude/settings.json`** hooks: `PostToolUse` on `Edit|Write` of `*.py` → ruff fix +
      format on that file, remaining diagnostics to stderr with exit 2; `Stop` → `check.sh fast
      --no-fix` (respect `stop_hook_active`); `PreToolUse` on `Bash` → block bare
      `python|pytest|ruff|mypy|pip` not prefixed by `uv run`. Scripts live in `.claude/hooks/`.
- [ ] **CI** (`ci.yml`): jobs `lint` (`check.sh fast --no-fix` with
      GitHub output format, plus `pyright src` with `continue-on-error: true`), `test` matrix
      `["3.11", "3.12", "3.13", "3.14", "3.14t"]` with JDK 17, `uv sync` (a fresh resolve),
      `PYTHON_GIL=0` on `3.14t` (the only free-threaded target: pyarrow, numpy, and pandas no
      longer publish cp313t wheels), `pytest --cov --cov-report=xml`; `build` (`uv build`
      then `uv run --isolated --with dist/*.whl python -c "import deephaven_massive"`). Pin
      action SHAs as today.
- [ ] **`tests/conftest.py`** rewrite: start the embedded `deephaven_server.Server` in
      `pytest_configure` (before collection), `-DPeriodicUpdateGraph.defaultRefreshIntervalMillis=25`,
      keep the documented `_final_jvm_exit`/`os._exit` shutdown workaround in an isolated,
      commented block; fixtures (not importable helpers): `dh_exec_ctx`, `dh_to_pylist`,
      `load_fixture(owner, name)`, `make_csv_gz`; `collect_ignore_glob = ["legacy/*"]`.
      Port: an implementer's technical choice, not an owner decision. Try
      `deephaven_server.Server(port=0)` (an ephemeral port lets two test runs share a machine);
      if the server does not support it, use the fixed port `10099` with a comment saying why.
      Make `tests/_fakes/` importable from `conftest.py` under `--import-mode=importlib` (for
      example `pythonpath = ["tests"]` in `[tool.pytest]`, with a reason comment, so the
      fixtures can `from _fakes.ws import …`).
- [ ] **`tests/test_layout.py`**: walk `src/deephaven_massive/**/*.py`, assert the mirrored test
      file exists per DESIGN § 4.2 (leading underscore stripped from the module name; the
      directory path copied as is; `__init__.py` ↔ `test_init.py`), assert no test file
      lacks a source module unless it is one of the approved exceptions (`tests/conftest.py`,
      `tests/test_layout.py`, `tests/test_catalog.py`, `tests/test_readme.py`), ignore the
      `tests/_fakes/` package entirely, skip `LEGACY_PATHS`.
- [ ] **`tests/test_init.py`** + minimal `__init__.py`: `__version__` equals
      `importlib.metadata.version("deephaven-massive")`.
- [ ] Move the eight legacy test files to `tests/legacy/`; delete `tests/__init__.py`.
- [ ] Delete `scripts/pre-commit.sh` and `scripts/install-hooks.sh`. (`IMPLEMENTATION_PLAN.md`
      was already deleted in P0; DESIGN.md supersedes it.)
- [ ] **`AGENTS.md`** rewrite (≤ 150 lines): commands (`uv sync --upgrade`, `./scripts/check.sh`,
      `uv run pre-commit install --hook-type pre-commit --hook-type pre-push`); the hard rules
      (merges via PR, only the owner merges, answer every review comment, branch naming,
      conventional commits, test layout,
      linter-ignore policy, coverage pragma policy, free-threaded CI is gating, no live testing
      in unit tests); the Deephaven invariants from DESIGN § 18 and § 6.1 rewritten as rules;
      JVM test gotchas; a "where to look" table pointing at `DESIGN.md` until P7. Drop every
      count and every claim a tool now enforces.
- [ ] PR description documents the manual owner step: GitHub ruleset on `main` requiring a PR,
      the required status checks `test (3.11)` … `test (3.14)` and `test (3.14t)`, no direct
      pushes.

**Acceptance criteria.**
- [ ] `./scripts/check.sh` (full) is green locally on 3.11; CI is green on all five Python jobs.
- [ ] `uv sync` succeeds from a clean checkout.
- [ ] `uv run pre-commit run --all-files` passes; `pre-push` stage runs the full tier.
- [ ] Claude Code hooks fire: editing a `.py` file with a lint error surfaces it (exit 2); a
      bare `pytest` command is blocked.
- [ ] `tests/test_layout.py` and `tests/test_init.py` pass; coverage of the measured set is 100%.
- [ ] `grep -rn LEGACY-EXCLUSION` lists every temporary exclusion and nothing else is excluded.
- [ ] `AGENTS.md` contains the merges-via-PR rule, the layout rule, and the ignore policy.

**Verification.**
```bash
uv sync --upgrade
./scripts/check.sh                       # full tier
./scripts/check.sh fast --no-fix
uv run pre-commit run --all-files
uv run pre-commit run --hook-stage pre-push --all-files
uv run pytest tests/test_layout.py tests/test_init.py -q
uv build && uv run --isolated --with dist/*.whl python -c "import deephaven_massive"
grep -rn LEGACY-EXCLUSION pyproject.toml tests/conftest.py tests/test_layout.py
```

**Pitfalls (Appendix G).** Unused
`massive` dependency (G#2); blanket `ignore_missing_imports` discarding real types for
`deephaven`/`websockets` (G#3); `pytest-asyncio` with zero async tests (G#4); four gate
definitions (G#6); tests importing helpers from `conftest.py` plus `pythonpath=["."]` (G#7);
`exclude_lines` re-adding `pragma: no cover` (G#8); duplicated version (G#9); placeholder author
and deprecated license form (G#10); dev deps as a PyPI extra (G#11); `websockets>=11` with the
legacy `connect` (G#12); 220-line `.gitignore` (G#13); `conftest.py` importing the private
`_ensure_deephaven_server` and hard-coding port 10099 (G P2). Appendix D's draft config has
`py310`, `pytest-asyncio`, a `TC003` ignore, an `ANN401` ignore, `PLR0917` ignored, and a
`deephaven` module-level import ban; the settled decisions override all six.

**Out of scope.** Any change to legacy code behaviour; new library modules (P2); README rewrite
(P7); removing the legacy exclusions (P7).

**Approval gates.** Opening the PR; the ruleset on `main` (owner); any addition to the ignore
lists beyond those enumerated above.

---

<a id="p2"></a>
## P2 — `refactor: core skeleton`

| | |
|---|---|
| **Branch** | `refactor/core-skeleton` |
| **Issue** | #13 |

**Status.** Merged (#46). Its SDK-derived parts (`_schema.model_schema` and the model
flattening, the SDK fields of `DatasetSpec`, the empty `SDK_PARAMS` block, and the tests that
import `massive`) are removed by PC3; the text below records what P2 did.

**Goal.** Create the new package layout with the public surface fixed (exports, config, session
shell, query classes, enums, types, errors), the cross-cutting engines that transports and domains
build on (schema registry, time units, symbols, dates, liveness, diagnostics, dataset catalog),
and stub domain modules so P3–P6 can proceed in parallel without touching shared files. No
network code.

**DESIGN sections implemented.** [§ 2 Principles and terms](DESIGN.md#2-principles-and-terms);
[§ 3 Exports](DESIGN.md#3-exports); [§ 4 Package layout](DESIGN.md#4-package-layout) (every
P2 module of § 4.1 exists after P2, the domain modules as stubs; the transport modules
`_rest.py`, `_hosts.py`, `_poll.py`, `_ws.py`, `_stream.py`, `_watchlist.py`, and
`_flatfiles.py` are created by P3, P4, and P5);
[§ 5 `MassiveConfig`](DESIGN.md#5-massiveconfig);
[§ 6 `MassiveSession` and lifecycle](DESIGN.md#6-massivesession-and-lifecycle) including
[§ 6.1 How a live table is kept alive and released](DESIGN.md#61-how-a-live-table-is-kept-alive-and-released);
[§ 7 Query classes and verbs](DESIGN.md#7-query-classes-and-verbs) (class shapes,
[§ 7.2 synchronous `ValueError`s](DESIGN.md#72-synchronous-valueerrors-for-parameter-values),
and [§ 7.3 the runtime enum check](DESIGN.md#73-runtime-enum-check); transport-dependent verb
bodies raise `NotImplementedError` until their transport lands);
[§ 10 Symbols and `ALL`](DESIGN.md#10-symbols-and-all);
[§ 11 Enums and types](DESIGN.md#11-enums-and-types);
[§ 12 Timestamp units and day boundaries](DESIGN.md#12-timestamp-units-and-day-boundaries);
[§ 17 Diagnostics](DESIGN.md#17-diagnostics) (table definitions and the session-owned scope);
[§ 18 Concurrency model](DESIGN.md#18-concurrency-model) (registry lock, reaper thread, lock
order).

**Depends on.** P1. **Parallel with.** F1–F7.

**Files.** Module names are those of DESIGN § 4.1; every module below `deephaven_massive/`
other than `__init__.py` is private, and the public surface is `__init__.py` alone. The six
query classes and the type aliases are importable from `deephaven_massive._query` and
`deephaven_massive._types` for annotations only; there is no public `query` module.
- Create under `src/deephaven_massive/`:
  - `__init__.py` (rewrite): exactly the names of DESIGN § 3, `__all__`, and `__version__`.
  - `_config.py`: frozen `MassiveConfig`; `MassiveConfig()` reads the environment; validation
    (`reconnect_backoff` positive and `max_reconnect_backoff >= reconnect_backoff`, else
    `ValueError`; `ws_hosts` hostname-only).
  - `_session.py`: `MassiveSession`, namespaces, the `snapshots()` shell (P6g fills it),
    `connections()`, `streams()`, `release()`, `close()`, context manager, `registry_lock`,
    reaper thread, the captured `ExecutionContext` with fail-fast,
    second-session WARNING, per-transport factory hooks (stubs).
  - `_query.py`: the six query classes and their verbs.
  - `_errors.py`: `MassiveError`, `AuthenticationError`, `EntitlementError`.
  - `_enums.py`: the fourteen exported `StrEnum`s plus the internal `Recency` and
    `ChannelFamily`; the `_require_enum(value, EnumType, name)` check raising the decision-23
    `TypeError`.
  - `_types.py`: `Symbols`, `ALL`/`_All`, `DateLike`, `FilterValue`, and the `bars` overload
    aliases (`StreamSpan`, `DaySpan`, `FuturesSpan`).
  - `_schema.py`: schema registry, SDK-model flattening to PascalCase, dtype coercion, the
    override-table types, the four hand-written tick schemas with per-transport key maps.
  - `_timeunits.py`: `(dataset, transport, field) → unit` lookup composed from `DatasetSpec`s;
    refuses unregistered fields; the `[1990, 2100]` defensive check; the FMV `>= 10**17` rule.
  - `_symbols.py`: normalization across WS/REST/S3 forms for every class; `Underlying` parsing.
  - `_dates.py`: `DateLike` parsing (tz required), class day zones, half-open `[start, end)`,
    futures session rules, `flat_file_publish_cutoff` logic (pure function).
  - `_liveness.py`: result and stream scopes, `ResultRecord`, weakref → reaper queue, once-only
    `released` flag.
  - `_diagnostics.py`: `connections`/`streams` `input_table` definitions and update helpers
    (library threads only).
  - `_catalog.py`: `DatasetSpec`, the aggregation over the fixed domain module list, and an
    empty, clearly delimited `SDK_PARAMS` block that P3 generates.
  - `_domains/__init__.py` (empty), `_domains/base.py` (the shared dataset factory), and
    stubs `_domains/{stocks,options,futures,indices,forex,crypto,reference,indicators,economy,partners}.py`,
    each with its namespace class(es) and `DATASETS: tuple[DatasetSpec, ...] = ()`.
    `partners.py` holds the four vendor classes.
- Create the mirrored test for every module above under `tests/` (`tests/test_config.py`,
  `tests/test_session.py`, `tests/test_query.py`, …, `tests/_domains/test_base.py`,
  `tests/_domains/test_stocks.py`, …), update `tests/test_init.py`, and add
  `tests/_domains/test_init.py` (the mirror of `_domains/__init__.py`).
- Modify: `pyproject.toml` (remove the three top-level files from `LEGACY-EXCLUSION` lists),
  `tests/test_layout.py` (`LEGACY_PATHS` shrinks to the three subpackages).
- Delete: the legacy `config.py` and `session.py` (replaced by `_config.py` and
  `_session.py`); the legacy subpackages remain until P7.

**Tasks.**
- [ ] Implement `_enums.py`, `_types.py`, `_errors.py`; test: every exported enum is a `StrEnum`
      whose values are Massive wire strings; the `_require_enum` message names the member.
- [ ] Implement `_config.py`; tests cover env resolution order (`MASSIVE_API_KEY` then
      `POLYGON_API_KEY`), missing key → `AuthenticationError`, every validation rule, defaults
      live in exactly one place (assert no second default table exists).
- [ ] Implement `_symbols.py` and `_dates.py` as pure functions with parametrized tests from
      DESIGN § 10 and § 12 (every input form listed there; naive datetimes rejected; futures
      session widening).
- [ ] Implement `_schema.py`: flatten an SDK `@modelclass` instance (nested models → `Parent.Child`
      → `ParentChild` PascalCase), the explicit override table type, one dtype per union column,
      `int[]` for condition lists, `LocalDate`/`Instant` mapping. Tests use real SDK models
      (`massive.rest.models`).
- [ ] Implement `_timeunits.py` + the `[1990, 2100]` guard; test the FMV rule at the boundary.
- [ ] Implement `_catalog.py` (`DatasetSpec`, aggregation, `SDK_PARAMS` loader interface that P3
      fills, `extra_params`); test that the fixed module list equals the set of modules under
      `_domains/`.
- [ ] Implement `_liveness.py` and the reaper in `_session.py` exactly per DESIGN § 6.1:
      scope per result, `manage`, weakref callback enqueues only, reaper wraps each job in
      try/except with ERROR log, once-only flag flipped under `registry_lock`, `release()`
      verifies the weakref resolves to the same object. Tests run inside the embedded JVM with
      a `table_publisher` blink table and assert `on_shutdown_callback` fires after the last
      result is released, and does not fire while a derived table is alive.
      Also assert that two `blink()` calls on one query return distinct wrappers with distinct
      records, and that deleting one releases only its own record.
- [ ] Implement `_diagnostics.py` in a session-owned `LivenessScope` released in `close()`.
- [ ] Implement `_query.py` verbs' synchronous validation (`ALL` rules, exhausted generator,
      `ring(capacity)` bounds, `last_by` keys, `poll` interval) and the `ValueError` when called
      on the UG thread; transport-dependent bodies call session hooks that raise
      `NotImplementedError` (covered by tests asserting that exact behaviour until P3–P5 replace
      them).
- [ ] Implement `_session.py` namespaces, `close()` semantics (publishers not failed, WARNING
      once, streams `CLOSED`), `__enter__/__exit__`, fail-fast without an `ExecutionContext`.
- [ ] Add the enum-only signature test in `tests/test_enums.py` (DESIGN § 11): walk every
      public callable reachable from `deephaven_massive` and the session namespaces; assert no
      parameter annotation contains a `Literal` of `str` ([DESIGN § 11](DESIGN.md#11-enums-and-types)), and that every enum
      mirroring an SDK enum has the same value set. The `dm.__all__` test lives in
      `tests/test_init.py`.
- [ ] Remove the three top-level files from the `LEGACY-EXCLUSION` lists.

**Acceptance criteria.**
- [ ] `import deephaven_massive as dm; dm.__all__` equals the DESIGN § 3 list (test).
- [ ] `MassiveSession()` outside a Deephaven worker raises a clear error naming the requirement.
- [ ] Liveness tests prove: release via `m.release`, via GC, and via `close()`; double release is
      a no-op with no second shutdown callback.
- [ ] No `Literal[str]` in any public signature; every enum parameter rejects `str` with the
      documented `TypeError`.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs including `3.14t`.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_init.py tests/test_config.py tests/test_session.py tests/test_query.py \
  tests/test_enums.py tests/test_types.py tests/test_errors.py tests/test_liveness.py \
  tests/test_schema.py tests/test_timeunits.py tests/test_symbols.py tests/test_dates.py \
  tests/test_catalog.py tests/test_diagnostics.py tests/_domains -q
uv run mypy src tests
PYTHON_GIL=0 uv run --python 3.14t pytest tests/test_liveness.py tests/test_session.py -q
```

**Pitfalls (Appendix E).** Unbounded `_post_attach_sigs` (E C1) — no per-row Python sets in any
engine path; streams never shut down (E C3) — every publisher is created with
`on_shutdown_callback` inside a scope; zero logging and swallowed exceptions (E H2) — every
module has `logger = logging.getLogger(__name__)` and no `contextlib.suppress(Exception)`; test
hooks in the public constructor (E H6) — `MassiveSession(config)` only; `_is_deephaven_table`
duck-typing matched `pyarrow.Table` (E H7) — use `isinstance(obj, deephaven.table.Table)`;
defensive clamps `max(0, n-1)` (E H8) — raise on double release; triplicated config defaults
(E M); `exec_ctx: ... | None` deferred failures (E M) — mandatory, fail fast; `to_utc_ns`
magnitude sniffing (F M6) — units are declared per `(dataset, transport, field)`.

**Out of scope.** Any REST, WebSocket, or S3 I/O (P3–P5); dataset definitions (P6); the `bars`
overloads (P6, per domain); deleting legacy code (P7).

**Approval gates.** Opening the PR. Any deviation from DESIGN § 3 or from the module names of
DESIGN § 4.1 needs an owner decision first.

---
<a id="pc"></a>
## PC1–PC4 — own clients and the spec snapshot

The owner decided on 2026-10-26 to drop Massive's Python SDK and the fork
([DESIGN § 22](DESIGN.md#22-decisions), decisions 1, 2, and 22 to 25). These four packages
build what replaces it, ahead of P3 and P4, split by content: the REST client, the WebSocket
client, the spec generator with the name lock, and the dependency change. PC1, PC2, and PC3 own
disjoint files and run in parallel; PC4 lands once no code imports `massive`.

<a id="pc1"></a>
### PC1 — `feat: REST client`

| | |
|---|---|
| **Branch** | `feat/rest-client` |
| **Issue** | TBD (owner approval to create) |

**Goal.** A REST client that speaks Massive's documented REST protocol exactly, shared safely by
every thread of a session, with a protocol-faithful fake server. It knows nothing of datasets or
Deephaven; P3 builds the REST transport on it.

**DESIGN sections implemented.** [§ 19.1 The REST client](DESIGN.md#191-the-rest-client);
[§ 13.4 REST](DESIGN.md#134-rest) (the error semantics);
[§ 18.2 Shared state and ownership](DESIGN.md#182-shared-state-and-ownership) and
[§ 18.4 Free-threaded specifics](DESIGN.md#184-free-threaded-specifics) (the shared client).

**Depends on.** P2. **Parallel with.** PC2, PC3, P5.

**Files.**
- Create: `src/deephaven_massive/_restclient.py`, `tests/test_restclient.py`,
  `tests/_fakes/rest.py` (a real HTTP server on `127.0.0.1`, a `ThreadingHTTPServer`, speaking
  Massive's envelope), `tests/data/_restclient/`.
- Modify: `tests/conftest.py` (one thin `fake_rest` fixture that starts the fake server).
- Delete: none.

**Tasks.**
- [ ] `RestClient(api_key, *, base_url, max_concurrency)` with
      `get(path, params, *, result_key="results") -> Page`,
      `pages(path, params, *, result_key="results", limit=None) -> Iterator[Page]`, and
      `close()`. Every bullet of
      DESIGN § 19.1 is implemented and has a named test: bearer authentication and no key in any
      URL (the fake rejects an `apiKey` parameter); proxy selection from `HTTPS_PROXY` and
      `NO_PROXY`; the CA bundle from `SSL_CERT_FILE`/`SSL_CERT_DIR` or `certifi`; query encoding
      (enum, `bool`, `date`, exact `datetime` nanoseconds, `.any_of` lists, `None` omitted, naive
      `datetime` rejected); `next_url` paging with the foreign-host refusal and the total-row
      `limit`; retries on 429/500/502/503/504 and connection errors with `Retry-After` in both
      forms and capped exponential backoff with upward jitter; a reset mid-pagination retries that
      page only; the error mapping (401, 403, `NOT_AUTHORIZED`, 429, other statuses, `ERROR`
      envelopes, connection failures), each message naming the status, path, and `request_id`;
      strict decoding (invalid JSON and non-UTF-8 bodies raise on the first and on a later page;
      result keys; a single object wrapped; a bare list; a missing result key with `OK` or
      `DELAYED` is empty); `User-Agent`; DEBUG logging with the key and header never logged.
- [ ] The result key (the `result_key` argument) and the page size (a query parameter) come
      from the caller; the client reads no generated table and has no dataset knowledge, so PC1
      does not depend on PC3.
- [ ] Sleeping goes through an injectable clock, so retry tests are deterministic and never call
      `time.sleep()`.
- [ ] `tests/_fakes/rest.py`: scripted routes (status, headers, body, a dropped connection,
      a delay), recorded requests (path, query, headers), and paging by `next_url`.
- [ ] Free-threaded stress: 16 threads page through one client concurrently against the fake
      (including a 429 with `Retry-After` and a mid-pagination reset), every result correct,
      run on `3.14t` with `PYTHON_GIL=0`.

**Acceptance criteria.**
- [ ] `urllib3` and `certifi` are imported only in `_restclient.py`.
- [ ] Every rule of DESIGN § 19.1 has a named test; `caplog` proves the key never appears in a log
      record at any level.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs, including the stress test on
      `3.14t`.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_restclient.py -q
PYTHON_GIL=0 uv run --python 3.14t pytest tests/test_restclient.py -q
```

**Pitfalls.** The SDK defects this client must not reproduce: a `datetime` converted through a
float (97% of microsecond values off); invalid JSON or a missing `results` key returned as an
empty list; an error without its HTTP status; urllib3's `MaxRetryError` leaking out; a 0, 0.2,
0.4 s backoff on 5xx. urllib3's `PoolManager` does not read `HTTPS_PROXY`; build a
`ProxyManager` yourself. Construct the pool with `retries=False`, or urllib3 retries a second
time underneath the client. Until PC4 declares them, `urllib3` and `certifi` are installed only
as dependencies of `massive`; that is expected.

**Out of scope.** Datasets, Arrow, and Deephaven (P3); host resolution (P3).

**Approval gates.** Opening the PR.

---

<a id="pc2"></a>
### PC2 — `feat: WebSocket client`

| | |
|---|---|
| **Branch** | `feat/websocket-client` |
| **Issue** | TBD (owner approval to create) |

**Goal.** A WebSocket client for one `wss://host/path` that speaks Massive's documented
WebSocket protocol, confined to its own loop thread, with a protocol-faithful fake server. It
knows nothing of datasets or Deephaven; P4 builds the connection layer on it.

**DESIGN sections implemented.** [§ 19.2 The WebSocket client](DESIGN.md#192-the-websocket-client);
the client's side of [§ 13.3 Connection and subscription protocol](DESIGN.md#133-connection-and-subscription-protocol)
(callbacks, sends, `reconnect()`, `close()`); [§ 13.1 Invariants](DESIGN.md#131-invariants)
item 4 (backoff); [§ 5 `MassiveConfig`](DESIGN.md#5-massiveconfig) (`reconnect_backoff`,
`max_reconnect_backoff`).

**Depends on.** P2. **Parallel with.** PC1, PC3, P5.

**Files.**
- Create: `src/deephaven_massive/_wsclient.py`, `tests/test_wsclient.py`, `tests/_fakes/ws.py`
  (a fake Massive server on `websockets.asyncio.server`), `tests/data/_wsclient/` (status-frame
  fixtures: `connected`, `auth_success`, `auth_failed`, `max_connections`).
- Modify: `tests/conftest.py` (one thin `fake_ws_server` fixture).
- Delete: none.

**Tasks.**
- [ ] `WsClient(url, api_key, *, on_open, on_frame, on_state, reconnect_backoff,
      max_reconnect_backoff)` with `start()`, loop-thread `subscribe(generation, topics)`,
      `unsubscribe(generation, topics)`, and `reconnect()`, and thread-safe `close()` and
      `join(timeout)`. Every bullet of DESIGN § 19.2 is implemented and has a named test.
- [ ] Handshake: `connected`, auth, reply, within the 10 s handshake timeout; `auth_success`
      opens a generation; `auth_failed` is fatal; `max_connections` reports `WAITING_SLOT`,
      waits for the server's close, and backs off.
- [ ] One `connect()` per attempt (never websockets' reconnecting `async for` iterator, whose
      own schedule would bypass the backoff), `max_size=None`, the CA rule of § 19.1, and the
      `User-Agent`.
- [ ] Reconnect after every close or failure not requested and not fatal, a clean server close
      included; the backoff doubles from `reconnect_backoff` to `max_reconnect_backoff` with
      clamped upward jitter; it resets only after 60 s of stable uptime. Classify `connect()`
      failures with `websockets.asyncio.client.process_exception`.
- [ ] One sender coroutine sends queued frames in order and at once, chunked at 1,000 topics;
      frames of a stale generation are dropped.
- [ ] `close()` from any thread cancels the task through `call_soon_threadsafe`, interrupting a
      backoff wait at once, and closes an open socket with code 1000.
- [ ] Time goes through an injectable clock and sleep, so backoff tests never wait in real time.
- [ ] `tests/_fakes/ws.py`: scripted handshakes (accept, reject, `max_connections` in the reply
      or later, silent after the upgrade, silent after auth, HTTP 404 and 503 at the upgrade),
      per-topic acknowledgements and refusals, server-initiated clean and abnormal closes, and a
      record of every frame received.
- [ ] Free-threaded stress: `close()` from 16 threads while the client runs, and 16 clients at
      once against the fake, on `3.14t` with `PYTHON_GIL=0`.

**Acceptance criteria.**
- [ ] `websockets` is imported only in `_wsclient.py` and `tests/_fakes/ws.py`.
- [ ] A clean server close (1000) reconnects; `A.*` and `AM.X` are sent and kept independently;
      a change made while disconnected is never sent to the old socket.
- [ ] `close()` returns at once, and the thread ends within a second, while the client sits in a
      120 s backoff wait.
- [ ] The auth frame and the key never appear in a log record (`caplog`).
- [ ] `./scripts/check.sh` green; CI green on all five test jobs.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_wsclient.py -q
PYTHON_GIL=0 uv run --python 3.14t pytest tests/test_wsclient.py -q
```

**Pitfalls.** The SDK defects this client must not reproduce: a clean server close that ends
the client; a wildcard subscribe that drops topics sharing its prefix; changes made while down
that are lost or resurrected; instant reconnects (Massive needs at least 10–30 s to free the
slot); subscriptions sent only after a 1 s receive timeout; a lifetime reconnect count; no
thread-safe close; `max_connections` only logged; a server silent after the upgrade that hangs
`connect()` forever. `orjson.dumps` returns bytes, which would send binary frames; encode
outgoing frames with the standard library.

**Out of scope.** Topics, reference counts, acknowledgements, watchdogs, routing (P4).

**Approval gates.** Opening the PR.

---

<a id="pc3"></a>
### PC3 — `feat: spec snapshot, generator, and name lock`

| | |
|---|---|
| **Branch** | `feat/spec-generator` |
| **Issue** | TBD (owner approval to create) |

**Goal.** Make Massive's OpenAPI spec the single source of REST parameters, fields, and types,
and the committed name lock the single source of column names, then remove every SDK-derived
part of the merged P2 core.

**DESIGN sections implemented.**
[§ 19.3 The spec snapshot, the generator, and the name lock](DESIGN.md#193-the-spec-snapshot-the-generator-and-the-name-lock);
[§ 9 Filters](DESIGN.md#9-filters) (validation against the generated parameters and the `_` to
`.` mapping); [§ 11 Enums and types](DESIGN.md#11-enums-and-types) (spec types to column types;
enums against spec enums); [§ 12 Timestamp units and day boundaries](DESIGN.md#12-timestamp-units-and-day-boundaries)
(the unit-tag cross-check); [§ 8.12 Excluded endpoints](DESIGN.md#812-excluded-endpoints) (the
allow-list); [§ 20 Quality gates](DESIGN.md#20-quality-gates) (the spec gates and the advisory
drift job); [§ 4.1 Source modules](DESIGN.md#41-source-modules) (`_specgen.py`, `_restspec.py`,
`_names/`).

**Depends on.** P2. **Parallel with.** PC1, PC2, P5. If P5 (#51) merges first, rebase: its
`_catalog.py` change (the flat-file key-map check at import) stays.

**Files.**
- Create: `scripts/massive_spec/` (`openapi.json`, `websocket.json`, `llms.txt`, the flat-file
  documentation pages, `manifest.json`), `scripts/refresh_massive_spec.py`,
  `src/deephaven_massive/_specgen.py`, `src/deephaven_massive/_restspec.py` (generated),
  `src/deephaven_massive/_names/<domain>.json` (one per domain module, each `{}`),
  `tests/test_specgen.py`, `tests/test_restspec.py`, `.github/workflows/spec-drift.yml`.
- Modify: `src/deephaven_massive/_schema.py`, `src/deephaven_massive/_catalog.py`,
  `tests/test_schema.py`, `tests/test_catalog.py`, `tests/test_enums.py`,
  `.github/workflows/ci.yml` (check out the full history with tags for the compatibility test).
- Delete: none (`scripts/refresh_massive_catalog.py` and the `SDK_PARAMS` block were never
  generated).

**Tasks.**
- [ ] Fetch the four sources once with `scripts/refresh_massive_spec.py --fetch` and commit them
      with `manifest.json`.
- [ ] `_specgen.py`, pure and offline: the allow-list (every REST path of DESIGN § 8), the seven
      non-`results` result keys, spec flattening (`allOf`, nested objects, filtering of
      `x-polygon-ignore`, `x-polygon-draft`, and `deprecated`), type mapping, parameter
      extraction, the override table (rejecting overrides that name a missing path), the record
      families with their object-shape assignments and **complete lexicons for every
      allow-listed path**, the identity entries for short readable keys, the naming rule, the
      gate (short unknown key; PascalCase collision) with the description-match suggestion, and
      lock appending. Domain packages never edit `_specgen.py`, so the refresh must pass on the
      whole allow-list when PC3 merges.
- [ ] `scripts/refresh_massive_spec.py`: `--fetch`, the offline regeneration of `_restspec.py`,
      lock appending for every declared dataset, and `--check`.
- [ ] Generate and commit `_restspec.py`; the parity test in `tests/test_restspec.py`.
- [ ] `_schema.py`: delete `model_schema`, `ModelSchema`, `flatten`, `_flatten_into`,
      `_model_fields`, `_unwrap_optional`, the hint mapping of `_column_type`, and `_jsonable`;
      keep `Schema`, `Column`, `ColumnType`, `ColumnOverride` (now keyed by wire path), and
      `KeyMap`; make `Schema.to_arrow` columnar (one list per column straight from the decoded
      rows); re-key the tick schemas' REST key maps by wire key (aggregates `t`, `o`, `h`, `l`,
      `c`, `v`, `vw`, `n`; `/v3/trades` and `/v3/quotes` keys as the spec writes them); apply the
      owner's WebSocket names: `AggregateVwap` becomes `DayVwap`, `AverageSize` becomes
      `AverageTradeSize`, and `BARS` gains `AccumulatedDollarVolume` (`dav`). No release has
      shipped, so these renames need no `BREAKING_RENAMES` entry.
- [ ] `_catalog.py`: drop `rest_model`, `rest_columns`, `fetch_method`, `history_method`,
      `grouped_daily_method`, `SymbolParam.PAIR`, `extra_params`, `SCHEMA_FROM_MODEL`,
      `SDK_PARAMS`, and `sdk_params`; `rest_paths` names path templates from the allow-list;
      `time_axis` names wire parameters (`timestamp.gte`, `timestamp.lt`); a REST dataset's
      schema comes from `_restspec.py` and its domain's lock; filter validation per DESIGN § 9;
      the lock loader (package data through `importlib.resources`).
- [ ] `tests/test_catalog.py`: the completeness checks (lock entries for every wire path; the
      WebSocket key maps against `websocket.json`, with a reviewed dropped-key list; the
      allow-list against `llms.txt`; the flat-file key maps against the documented headers),
      the unit-tag cross-check with its reviewed conflict list (the 20 indicator
      `IMicroseconds` tags first), and the compatibility test against the lock at the last
      release tag with an empty `BREAKING_RENAMES`. The comparison is a pure function tested
      directly; its git read is tested against a temporary repository made with `git init`.
- [ ] `tests/test_enums.py`: compare the public enums with the parameter enums of the vendored
      snapshot instead of the SDK's enums.
- [ ] `tests/test_schema.py`: replace the SDK-model tests with tests of the remaining functions;
      no test imports `massive`.
- [ ] `.github/workflows/spec-drift.yml`: weekly `schedule` and `workflow_dispatch`; fetch into a
      temporary directory, compare with `scripts/massive_spec/`, and open or update one issue
      labelled `spec-drift` with the summary of DESIGN § 20; `permissions: issues: write` only;
      never pushes or opens a PR; not a required check.
- [ ] `ci.yml`: `fetch-depth: 0` on the checkout of the test jobs.

**Acceptance criteria.**
- [ ] `uv run python scripts/refresh_massive_spec.py --check` exits 0 offline.
- [ ] `git grep -nE "^(from|import) massive" -- src tests ':!tests/legacy'` is empty.
- [ ] Negative tests: an unknown short key fails the refresh and prints its suggestion; a
      PascalCase collision fails; an override naming a missing path fails; a changed lock entry
      without a `BREAKING_RENAMES` entry fails the compatibility test.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs.

**Verification.**
```bash
./scripts/check.sh
uv run python scripts/refresh_massive_spec.py --check
uv run pytest tests/test_specgen.py tests/test_restspec.py tests/test_catalog.py tests/test_schema.py tests/test_enums.py -q
```

**Pitfalls.** `https://api.massive.com/specs/rest.json` is a stale 33-path subset; pin
`/openapi`. Descriptions churn about twenty times as often as keys: never derive a name from
one, and keep descriptions out of `_restspec.py` so a description edit produces no generated
diff. The same short key means different things by record family (`c` is close or conditions,
`z` is tape or average trade size), so a flat key table cannot work. The spec's
`x-polygon-go-type` unit tags are wrong on indicators; they are a cross-check, never a source.
The drift workflow can be run only after it reaches `main`; the owner triggers its first run
by hand.

**Out of scope.** Declaring datasets (P6); the REST transport (P3); the catalog's final
coverage assertions and exclusion lists (P7).

**Approval gates.** Opening the PR. The lexicon, the family assignments, and the override table
are reviewed by the owner in the PR. Running `--fetch` reads public Massive URLs (read-only;
note it in the PR).

---

<a id="pc4"></a>
### PC4 — `chore(deps): replace massive with urllib3, websockets, and certifi`

| | |
|---|---|
| **Branch** | `chore/deps-own-clients` |
| **Issue** | TBD (owner approval to create) |

**Goal.** Remove the `massive` dependency and its fork source, and declare the libraries the
clients use directly.

**DESIGN sections implemented.** [§ 1 Purpose and scope](DESIGN.md#1-purpose-and-scope) (the
runtime dependencies); [§ 20.1 Tooling configuration](DESIGN.md#201-tooling-configuration)
(packaging, the entry point, mypy without a `massive` override).

**Depends on.** PC1, PC2, PC3 (no code imports `massive` any more). **Parallel with.** P5.

**Files.**
- Modify: `pyproject.toml` (remove `massive>=2.9.0`, the `[tool.uv.sources]` table, and its
  comment block; add floors for `urllib3` and `certifi`; raise the `websockets` floor to the
  first release whose asyncio client reads proxies from the environment), `scripts/check.sh`
  (remove the installed-`massive`-commit step and its offline message),
  `.github/workflows/ci.yml` (remove the step that prints the `massive` commit and the build
  job's fork override).
- Create, delete: none.

**Tasks.**
- [ ] The dependency edits above, each floor checked against the code's use.
- [ ] The `check.sh` and CI edits, which the removal needs: both read `massive`'s install record.
- [ ] Confirm that a clean `uv sync --upgrade` resolves from PyPI alone.

**Acceptance criteria.**
- [ ] `git grep -n "massive-client-python\|deephaven-fixes\|tool.uv.sources" -- ':!plans'` is
      empty.
- [ ] `uv run python -c "import massive"` fails after a clean sync.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs and the build job.

**Verification.**
```bash
rm -rf .venv && uv sync --upgrade
./scripts/check.sh
uv build && uv run --isolated --with dist/*.whl python -c "import deephaven_massive"
```

**Pitfalls.** A `chore(deps):` PR changes declarations only; the `check.sh` and CI edits are
here because they would break without `massive`, and nothing else rides along.

**Out of scope.** Any library code.

**Approval gates.** Opening the PR.

---

<a id="p3"></a>
## P3 — `feat: REST transport and tier detection`

| | |
|---|---|
| **Branch** | `feat/rest-transport` |
| **Issue** | #14 |

**Goal.** Put every REST dataset on the library's own REST client (PC1) and the generated spec
table (PC3): requests built from a `DatasetSpec`, pages decoded straight into bounded columnar
Arrow batches, 403/429 handling, host resolution from REST evidence, and the `poll()` verb with
all three modes including reactive symbol tables. After P3, `Query.fetch()`, `Query.poll()`,
`HistoryQuery.history(source=REST)`, and the `fetch()` of the latest-queries work for any
`DatasetSpec` that declares REST paths.

**DESIGN sections implemented.** [§ 7 Query classes and verbs](DESIGN.md#7-query-classes-and-verbs)
(`fetch`, `poll`, REST `history`); [§ 9 Filters](DESIGN.md#9-filters) (`start`/`end` on the wire);
[§ 13.1 Invariants](DESIGN.md#131-invariants) and [§ 13.2 Host decision](DESIGN.md#132-host-decision);
[§ 13.4 REST](DESIGN.md#134-rest); the transport paragraph of
[§ 19.1 The REST client](DESIGN.md#191-the-rest-client);
[§ 14 Publishers, fan-out, and failure semantics](DESIGN.md#14-publishers-fan-out-and-failure-semantics)
(the `poll()` publisher and `input_table` rules); [§ 17 Diagnostics](DESIGN.md#17-diagnostics)
(REST rows in `connections()`, `RequestsPerCycle`);
[§ 18 Concurrency model](DESIGN.md#18-concurrency-model) (poller thread,
`ThreadPoolExecutor(max_concurrency)`).

**Depends on.** PC1, PC3, PC4. **Parallel with.** P4, P5 (disjoint files; P3 lands first if
`_session.py` hunks overlap).

**Files.**
- Create: `src/deephaven_massive/_rest.py` (request building, columnar decoding into Arrow
  batches, error handling, the rate-limit WARNING once), `_hosts.py` (host resolution and the
  `HostDecision` cache, per session, kept by the REST transport under its own lock), `_poll.py`
  (REPLACE/UPSERT/APPEND pollers, reactive symbol snapshot per cycle, fan-out executor),
  `tests/test_rest.py`, `tests/test_hosts.py`, `tests/test_poll.py`, `tests/data/_rest/`,
  `tests/data/_hosts/` (snapshot fixtures per class and tier).
- Modify: `_session.py` (fill the REST factory hook; the session's `RestClient` and its closer;
  `connections()` REST rows), `_query.py` (REST-backed verb bodies replace
  `NotImplementedError`), `_domains/base.py` (REST dataset factory helper used by P6).
- Delete: none.

**Tasks.**
- [ ] `_rest.py`: one `RestClient` per session (PC1), built from `MassiveConfig`
      (`rest_base_url`, `api_key`, `max_concurrency`) on first use and closed by `m.close()`
      through a closer that holds the core weakly. Every request goes through it.
- [ ] Build each request from the `DatasetSpec`: the path template with its path segments, the
      symbol parameter (`SymbolParam`), the `start`/`end` wire pair, the validated `filters`
      (mapped to wire names by `_catalog.py`, PC3), and the page size; pass the path's result
      key from `_restspec.py` as `result_key`; `limit` stays the total-row cap of
      `RestClient.pages`. Tests cover a path whose result key is not `results` (for example
      `/v2/last/trade/{t}`, key `results`, beside `/v1/last/crypto/{from}/{to}`, key `last`).
- [ ] Decode each page straight to columns: for each wire path the dataset's schema names, one
      Python list per column, timestamp units converted (`_timeunits.py`), one Arrow batch per
      page. Never hold more than one page of rows; build no row objects.
- [ ] Error handling per DESIGN § 13.4: the client's exceptions reach `fetch()` and `history()`
      unchanged; the 429 WARNING about Basic-plan limits is logged once per session; in `poll()`,
      `EntitlementError` fails the table through `publish_failure`.
- [ ] `_hosts.py`: `resolve(asset_class, family) -> HostDecision` exactly per DESIGN § 13.2:
      FMV fixed; `ws_hosts` pin; forex/crypto without a REST call; one evidence call per class
      through `RestClient.get` (raw rows); ordered timeframe fields excluding
      `underlying_asset`; DELAYED wins; the no-timeframe branches incl. the base-Business
      WARNING and futures `UNKNOWN`; HTTP 403 → `EntitlementError`; transport error → nothing
      cached. Synchronous on the calling thread; the REST transport caches each decision per
      session under its own leaf lock, released before the REST call.
- [ ] `_poll.py`: one thread per `poll()` table; `REPLACE` and `UPSERT` on a keyed
      `input_table` with static Arrow key tables for deletes (incl. empty cycles); `APPEND` on a
      `table_publisher`; reactive symbols read with `Table.snapshot()` on the poller thread, never
      in a listener; fan-out under `max_concurrency`; `RequestsPerCycle` in `streams()`; the
      release path stops the poller (uses P2's `_liveness`); 403 inside a cycle →
      `publish_failure(EntitlementError)`.
- [ ] `fetch()` on `StreamHistoryLatestQuery` (one request per symbol via the "last" endpoints;
      `ALL` → `ValueError` naming `snapshots()`); `fetch()` on `HistoryLatestQuery` (`/prev`).
- [ ] REST `history()` paging with no `limit`; grouped-daily path for `ALL` + DAY. This is the
      REST transport that P5's `HistorySource.AUTO` hands off to (`core.rest_transport().history(...)`).
- [ ] Tests against the `fake_rest` fixture (PC1), with fixtures in `tests/data/_rest/` shaped
      from the spec's response examples and the documentation samples (never invented keys):
      exact path and query for each code path, `next_url` paging with the page-size cap, 403,
      429, bad JSON, and all three poll modes under the embedded JVM.
- [ ] Free-threaded test: 16 threads calling `fetch()` on one session concurrently.

**Acceptance criteria.**
- [ ] `urllib3` is imported only in `_restclient.py`; `_rest.py`, `_hosts.py`, and `_poll.py` make
      every request through the session's `RestClient`.
- [ ] Every host-decision branch in DESIGN § 13.2 has a fixture test naming it.
- [ ] `poll(mode=REPLACE)` evicts keys absent from the latest cycle, including on an empty cycle.
- [ ] A session that used this transport and was dropped without `close()` is collected when
      unreferenced (`gc.collect()`), the `garbage-collected without close()` WARNING is logged,
      and the transport's closer ran (model: `test_a_session_collected_unclosed_runs_its_closers`;
      the closer and anything the transport keeps must hold the core weakly, DESIGN § 6).
- [ ] `./scripts/check.sh` green; CI green on all five test jobs.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_rest.py tests/test_hosts.py tests/test_poll.py tests/test_catalog.py -q
uv run python scripts/refresh_massive_spec.py --check    # generated table and locks are current
PYTHON_GIL=0 uv run --python 3.14t pytest tests/test_rest.py tests/test_poll.py -q
```

**Pitfalls (Appendices E, F, H).** A second HTTP path beside `RestClient` (the legacy session's
`_request_json` had its own retry loop); `limit` used as page size (F H1:
`/v3/reference/tickers` max 1000 → HTTP 400); envelope sniffing (`"converted" not in payload …`,
F H4) — use the generated result key; ms epoch ints forwarded to `timestamp.gte`, which expects
ns (F M6); `datetime.isoformat()` placed in the aggs path (F M6); option seeding with N
per-contract snapshot calls (F M7) — use `/v3/snapshot?ticker.any_of=` 250 per request; network
I/O in a poller `__init__` (E M); `__PollId` written but never read (E M); a second `poll()` on
the same query silently stopping the first (E M); "absence ⇒ delayed" applied to futures (v3 §19
M2).

**Out of scope.** The REST client itself (PC1); the generator (PC3); WebSocket (P4); S3 (P5);
any `DatasetSpec` beyond the two or three used as test fixtures (P6); `HistorySource.AUTO`
arbitration (P5, which owns `_flatfiles.py`; P3's `history()` handles only `source=REST`); the
catalog coverage assertions (P7).

**Approval gates.** Opening the PR.

**Follow-ups from the P2 review (PR #46).** `poll()` must handle a verb that races `close()`,
as `SessionCore._claim` does for streams: a `poll()` that passed the verb guard just before
`close()` ran must not start a poller the session will never stop. Register the poller's
cleanup through `SessionCore.on_close`, which runs it at once on a closed session, or recheck
the closed flag under `registry_lock` before starting it.

---

<a id="p4"></a>
## P4 — `feat: WebSocket layer`

| | |
|---|---|
| **Branch** | `feat/websocket-layer` |
| **Issue** | #15 |

**Goal.** Implement live streams on the library's own WebSocket client (PC2): one connection per
URL, a reconcile loop with one in-flight step, acknowledgement parsing and watchdogs, the
explicit-verdict re-resolve, per-stream publishers with shared-batch fan-out, the options cap
with deferral, reactive watchlists, and the live diagnostics. After P4, every `StreamQuery` verb
works for any `DatasetSpec` that declares a channel.

**DESIGN sections implemented.**
[§ 13.3 Connection and subscription protocol](DESIGN.md#133-connection-and-subscription-protocol)
(the library's side; the client's side is PC2);
[§ 14 Publishers, fan-out, and failure semantics](DESIGN.md#14-publishers-fan-out-and-failure-semantics);
[§ 15 Options contract cap](DESIGN.md#15-options-contract-cap);
[§ 17 Diagnostics](DESIGN.md#17-diagnostics) (connection and stream rows, `HeldResults`,
`Deferred`, `State`); [§ 18 Concurrency model](DESIGN.md#18-concurrency-model) (WS client
thread, flush thread, loop-confined state, lock order);
[§ 6.1 How a live table is kept alive and released](DESIGN.md#61-how-a-live-table-is-kept-alive-and-released)
(stream scope; `on_shutdown_callback` → ref-count decrement).

**Depends on.** PC2, PC3 (the renamed `BARS` columns), PC4. **Parallel with.** P3, P5.

**Files.**
- Create: `src/deephaven_massive/_ws.py` (connection registry keyed by URL, single-flight
  creation, one `WsClient` per connection, the callbacks, reconcile loop, wildcard coverage, ack
  parser, watchdogs, verdict rule, the options contract cap, idle close after 30 s grace),
  `_stream.py` (per-stream publisher, blink table, and stream scope created by
  `StreamTransport.open_stream`; the session's registry from P2 already deduplicates streams
  by key and owns each stream's record, so `_stream.py` keeps no second registry), per-channel
  `SimpleQueue`s, flush thread, one Arrow→Java conversion
  per channel per flush, `where_in` fan-out with cached filter tables, cap deferral),
  `_watchlist.py` (`listen(..., do_replay=True)` on the symbol table; diffs dispatched off the
  UG thread; `last_by().where_in(symbol_table, "Symbol")`), `tests/test_ws.py`,
  `tests/test_stream.py`, `tests/test_watchlist.py`, `tests/data/_ws/` (status-frame fixtures
  per cluster: `success subscribed to: …`, `error not authorized`, `max_connections` after
  authentication, comma-list acks).
- Modify: `tests/_fakes/ws.py` only if a scenario needs a new script (PC2 owns the module),
  `_session.py` (fill the WS factory hook; `connections()`/`streams()` live rows), `_query.py`
  (stream verb bodies), `_domains/base.py` (stream dataset factory helper).
- Delete: none.

**Tasks.**
- [ ] `_ws.py`: one `WsClient` (PC2) per `wss://host/path`, created on the first stream for that
      URL with `MassiveConfig`'s `reconnect_backoff` and `max_reconnect_backoff`, started on its
      daemon thread. Wire `on_open`, `on_frame`, and `on_state` per DESIGN § 13.3; every
      connection-state writer runs on the client's loop thread, and the flush thread never
      touches the client.
- [ ] Generations: `on_open` cancels the old generation's watchdogs, clears S and A, shows
      `CONNECTED`, and reconciles, which re-sends the whole wire set W. Watchdogs are bound to
      their generation; one that fires for a stale generation, or for a topic that left S or W,
      is cancelled without escalating.
- [ ] Decode each raw frame once (stdlib `json`, or `orjson` when importable, logged once at
      INFO); route `ev == "status"` objects to the status handler and data objects to
      per-channel queues.
- [ ] Reconcile loop per DESIGN § 13.3: D from reference counts minus deferred contracts; W
      without the explicit topics of a channel whose wildcard is in D; one in-flight step;
      `client.unsubscribe` for removals and `client.subscribe` for additions (≤ 1,000 topics
      per frame, chunked by the client); per-topic watchdog `ack_timeout` → one single-topic
      re-send → `NO_ACK` + ERROR + `client.reconnect()`; `status error` naming a topic fails
      exactly that topic's streams; unattributed `error` applies to the in-flight step;
      explicit-verdict re-resolve only when `Recency=UNKNOWN`, every topic of the step was
      refused, and the host is not pinned, at most once per (class, family).
- [ ] `auth_failed` and fatal handshake errors (reported by the client as `FAILED`) fail every
      stream on the connection through `publish_failure` (`AuthenticationError`, or a
      `MassiveError` naming the HTTP status and host); `max_connections`, in the auth reply or
      as a later status, shows `WAITING_SLOT`, streams alive; `RECONNECTING` with
      `NextReconnectAt` from the client.
- [ ] `_stream.py`: `open_stream(spec, request, channel, *, stream_id)` creates
      `table_publisher(name=f"massive.{dataset}#{stream_id}",
      col_defs=<schema>, on_shutdown_callback=…)` created in the stream's `LivenessScope` under
      the captured `ExecutionContext`; shutdown callback does only registry update under
      `registry_lock` and `loop.call_soon_threadsafe`; flush thread: drain → one Arrow batch →
      `to_table` once → per stream `add(batch)` or `add(batch.where_in(filter_table, "Symbol"))`;
      `on_flush_callback` unused; schema mismatch → `publish_failure` + ERROR.
- [ ] Options cap: count topics per `/options` connection; static over-cap → `ValueError`
      at verb time; reactive input → sorted subscribe up to the cap, the rest `Deferred`,
      `State=RUNNING`, WARNING on each transition, promote on freed capacity.
- [ ] `_watchlist.py`: listener computes adds/removes only; dispatches to a session worker;
      subscribe order: insert, enable, subscribe; unsubscribe order: disable, unsubscribe,
      delete; symbol table held strongly by the stream in its scope.
- [ ] Library threads never take the UG lock while holding `registry_lock`; verbs on the UG
      thread raise `ValueError` (P2 helper).
- [ ] Idle connection close in the connection loop (30 s grace), not the reaper.
- [ ] Tests against the `fake_ws_server` fixture (PC2); deterministic drives (`flush_now()`
      style test seams on private objects, allowed under tests-only `SLF001`); every line of the
      DESIGN § 13.3 protocol block has a test, among them: acknowledgements in a generation's
      first frame are kept; a reconnect re-sends the whole wire set; a resend after an
      acknowledgement timeout is for that topic alone and advances to `NO_ACK`; a topic
      released while its watchdog runs is never re-sent; adding and removing a wildcard over
      explicit topics sends the expected frames; a `max_connections` status after
      authentication shows `WAITING_SLOT` once the socket closes; a server silent after
      authentication reaches `NO_ACK` within 2 × `ack_timeout`. The fan-out benchmark
      (non-gating, `-m benchmark`, deselected by default) records rows/second for 1, 10, 100
      streams; free-threaded stress: 16 threads × concurrent subscribe/unsubscribe/release
      against the fake server, run on `3.14t`.

**Acceptance criteria.**
- [ ] A refused topic fails only its streams; other streams on the same connection keep
      ticking (test).
- [ ] A session that used this transport and was dropped without `close()` is collected when
      unreferenced (`gc.collect()`), the `garbage-collected without close()` WARNING is logged,
      and the transport's closer ran (model: `test_a_session_collected_unclosed_runs_its_closers`;
      the closer and anything the transport keeps must hold the core weakly, DESIGN § 6).
- [ ] Releasing the last result of a stream unsubscribes its topics; a derived table keeps the
      stream alive (test under the embedded JVM).
- [ ] Deferred option contracts subscribe automatically when capacity frees (test).
- [ ] `m.close()` returns promptly while a connection sits in a backoff wait: it calls
      `WsClient.close()`, which interrupts the wait, and joins the daemon thread with a bounded
      timeout (test with a long `reconnect_backoff` against the fake server).
- [ ] No Python per-row work per stream in the flush path (benchmark shows per-stream cost
      independent of row count in Python; recorded in the PR).
- [ ] `./scripts/check.sh` green; CI green on all five test jobs including the stress tests.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_ws.py tests/test_stream.py tests/test_watchlist.py -q
PYTHON_GIL=0 uv run --python 3.14t pytest tests/test_ws.py tests/test_stream.py tests/test_watchlist.py -q
uv run pytest -m benchmark tests/test_stream.py -q    # non-gating
```

**Pitfalls (Appendices E, H).** `_post_attach_sigs` growth (E C1); seed cutover losing rows (E
C2; gone: `last_by` never seeds); streams never shut down (E C3); all decode/JVM work on the
asyncio thread (E H1); swallowed exceptions and unobserved `create_task` results (E H2); seed
failure leaving a half-started stream (E H3); idle connections never closed (E H4); unlocked
`_stream_registry` (E H5); `_emit_action` under a lock with a user hook (E M); tearing down the
whole URL on an entitlement error (DESIGN § 14); prefix matching between topics (`A.*` must
never touch `AM.X`) — topics are exact strings, and the only wildcard rule is the same-channel
coverage of DESIGN § 13.3; `custom_json=orjson`-style encoding of outgoing frames (`dumps`
returns bytes, H §3) — decode only.

**Out of scope.** The WebSocket client itself (PC2); REST (P3); S3 (P5); channel definitions for
datasets (P6; P4 uses `T`/`Q` fixtures only); live tests (P8).

**Approval gates.** Opening the PR.

**Follow-ups from the P2 review (PR #46).** The transport review of P2 (B1–B9) left these notes
for the implementer:

- Tighten `StreamTransport.open_stream`'s `channel` parameter from `str | None` to `str`: every
  WebSocket dataset has a channel, and `None` only means "no channel" for REST datasets.
- If `open_stream` raises after creating its stream scope or taking topic references, it must
  release both itself before the exception propagates; the session never sees a handle for it.
- The blink table's shutdown callback must catch the `RuntimeError` that
  `loop.call_soon_threadsafe` raises once the connection's loop is closed, and only log it: the
  callback can run long after the connection ended.
- The flush builds Arrow columns directly from the WebSocket key maps. (P2's
  `Schema.to_arrow` built per-row dictionaries, too slow for the flush path; PC3 makes it
  columnar, so the flush may use it if it fits.)
- Forex and crypto frames carry `sym` in the wire spelling (`EUR/USD`, `BTC-USD`). Canonicalise
  it before the `where_in` fan-out, so it matches `canonical_view`'s `C:EURUSD` and `X:BTCUSD`.
- The forex `C` channel sends `x` (the exchange id) as a quoted string; coerce it to `int`.
- `blink()` on a stream that failed during its open returns the failed table, but
  `append_only()`, `ring()` and `last_by()` raise Deephaven's `DHError` ("Can not listen to
  failed table") while shaping it (DESIGN § 6.1). Wrap that error in a `MassiveError` that
  names the stream's failure.

**Key maps for the other asset classes.** The owner decided (PR #46) that the shared tick
schemas are the union across asset classes: P2 added the nullable columns `ReceivedTimestamp`
(crypto `r`, ms), `SessionEndDate`, `Channel`, and `ReportSequence` to `TRADES` and `QUOTES`,
a single `Exchange` and `BidTimestamp`/`AskTimestamp` to `QUOTES`, and `DollarVolume`,
`SessionEndDate`, and `SettlementPrice` to `BARS`; PC3 adds `AccumulatedDollarVolume` (`dav`) to
`BARS` and renames its WebSocket-only columns to `DayVwap` (`a`) and `AverageTradeSize` (`z`).
This package maps the crypto `XT`/`XQ` keys (`x`, `r`) and the futures WebSocket keys (`dv` to
`DollarVolume`, `dav` to `AccumulatedDollarVolume`) onto them, and declares the units of the new
timestamp fields in the dataset specs.

---

<a id="p5"></a>
## P5 — `feat: flat-file history`

| | |
|---|---|
| **Branch** | `feat/flat-file-history` |
| **Issue** | #16 |

**Goal.** Stream `.csv.gz` flat files from `files.massive.com` into static tables with pure-Arrow
per-batch transforms, symbol pushdown, per-venue futures prefixes, partition-date rules, the
publish-cutoff rule, and the `HistorySource.AUTO` arbitration that decides between S3 and REST.

**DESIGN sections implemented.** [§ 16 `HistorySource.AUTO`](DESIGN.md#16-historysourceauto);
[§ 12 Timestamp units and day boundaries](DESIGN.md#12-timestamp-units-and-day-boundaries)
(flat-file partition dates, futures session keys with the fail-loud check);
[§ 7 Query classes and verbs](DESIGN.md#7-query-classes-and-verbs) (`history(source=…)`); the
flat-file columns of [§ 8 Namespace tree](DESIGN.md#8-namespace-tree) (as `DatasetSpec`
fields); [§ 5 `MassiveConfig`](DESIGN.md#5-massiveconfig) (`s3_*`,
`flat_file_symbol_threshold`, `flat_file_publish_cutoff`).

**Depends on.** P2. **Parallel with.** P3, P4, PC1–PC4. The move to own clients (PC1–PC4)
leaves P5 (open PR #51) unaffected, except that its REST hand-off,
`core.rest_transport().history(...)`, now reaches P3's transport on the library's REST client
instead of the SDK. PC3 rewrites `_catalog.py`; whichever of P5 and PC3 merges second rebases
and keeps the other's change.

**Files.**
- Create: `src/deephaven_massive/_flatfiles.py`, `tests/test_flatfiles.py`,
  `tests/_fakes/s3.py` (the fake S3 opener), `tests/data/_flatfiles/` (CSV fixtures with the
  **documented** headers per prefix, e.g. `us_stocks_sip/trades_v1`:
  `conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,ticker,trf_id,trf_timestamp`).
- Modify: `tests/conftest.py` (one thin `fake_s3` fixture that builds the `tests/_fakes/s3.py`
  opener), `_session.py` (S3 factory hook), `_domains/base.py` (flat-file prefix helper).
  `_query.py` needs no change: `history()` already hands `source=AUTO` (and `FLAT_FILES`) to
  `FlatFileTransport.history(..., source=...)`, which runs `choose_source` and calls
  `core.rest_transport().history(...)` itself when REST is the better source.
- Delete: none.

**Tasks.**
- [ ] `boto3` client from config (`s3_endpoint_url`, `s3_bucket`, credentials); typed via
      `types-boto3[s3]`. Missing credentials → S3 "not possible" in AUTO; explicit
      `source=FLAT_FILES` → `ValueError` listing the violated constraint.
- [ ] Read each key with `pyarrow.csv.open_csv` in 16 MB chunks; per batch: rename headers to
      the dataset's PascalCase columns, `pc.cast` to the registered dtypes, timestamp units from
      `_timeunits.py` (ns for ticks and `window_start`), `Symbol` pushdown with `pc.is_in` on each
      batch, list columns (`Conditions`, `Indicators`) parsed with Arrow compute on surviving
      rows only, half-open `[start, end)` filter on `Timestamp`. Hand batches to Deephaven
      incrementally (one `to_table` per batch, then `merge`), never accumulate the whole range.
- [ ] Partition selection per class day zone; missing non-trading dates skipped silently,
      missing weekday WARNING, missing last requested date →
      `MassiveError("… not yet published; flat files land ~11:00 ET next day")`; the publish
      cutoff from config.
- [ ] Futures: `us_futures_{venue}` prefixes, `venue=None` reads all four, 403 on any prefix →
      `EntitlementError` naming the venue and the `venue=` parameter; the session-date
      assumption is verified on the first file (date column vs key) and fails loudly.
- [ ] `choose_source()` implementing the AUTO predicate verbatim from DESIGN § 16; each `history()`
      logs `source=S3 files=<n> skipped=<dates>` or `source=REST requests=<n>`.
- [ ] Tests: fake S3 (`tests/_fakes/s3.py`, through the `fake_s3` fixture) via a `botocore`
      stubber or an injected opener on the private client;
      gzip and plain streams; pushdown keeps only matching symbols; a 3-day range with one
      missing weekday; the cutoff rule at 11:29 and 11:31 ET; futures venue fan-out and 403;
      every AUTO branch (none/one/both possible, threshold at 49/50/`ALL`).

**Acceptance criteria.**
- [ ] `grep -n "to_pylist\|to_pydict" src/deephaven_massive/_flatfiles.py` is empty.
- [ ] Peak memory is bounded by one chunk (test monitors `pa.total_allocated_bytes()` across a
      multi-chunk fixture).
- [ ] Every branch of DESIGN § 16 is covered by a named test; `./scripts/check.sh` green; CI
      green.
- [ ] A session that used this transport and was dropped without `close()` is collected when
      unreferenced (`gc.collect()`), the `garbage-collected without close()` WARNING is logged,
      and the transport's closer ran (model: `test_a_session_collected_unclosed_runs_its_closers`;
      the closer and anything the transport keeps must hold the core weakly, DESIGN § 6).

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_flatfiles.py tests/test_query.py -q
```

**Pitfalls (Appendix F).** `to_pylist()` per chunk and accumulating every batch (F C2);
`us_futures/` umbrella prefix and futures `day_aggs_v1` never existed (F, Appendix C);
`load()` positional overloading and seven aliases (F H5); AUTO disabled whenever
`adjusted=True` (F M4) — AUTO requires `adjusted=False` only for datasets that *have* the
parameter, and `indices.bars` has none; `ignore_missing = len(dates) > 1` dropping days silently
(F LOW); fixture headers that are not Massive's real columns (F LOW).

**Out of scope.** Dataset prefixes beyond the fixtures (P6 declares them); REST history (P3);
live S3 tests (P8).

**Approval gates.** Opening the PR.

---

<a id="p6"></a>
## P6a–P6h — `feat: <namespace> datasets`

All eight packages share this section. Each implements its namespace's rows of
[DESIGN § 8 Namespace tree](DESIGN.md#8-namespace-tree) on top of P3–P5, owns disjoint files
(see [Parallelism guide](#parallelism-guide)), and follows the same tasks and acceptance
criteria. Per-package specifics follow the shared text.

**Shared goal.** Declare every dataset of the namespace as a `DatasetSpec` in the namespace's
module, implement the namespace class's factory methods with exact DESIGN signatures (enums
only; `bars` overloads where applicable), and prove each dataset with realistic fixtures: exact
path and params, normalized column values, and timestamp units.

**DESIGN sections implemented.** The namespace's subsection of
[§ 8 Namespace tree](DESIGN.md#8-namespace-tree) (§ 8.1 to § 8.11, named per package below);
[§ 7.1 `bars()` overloads](DESIGN.md#71-bars-overloads) where the namespace has `bars`;
[§ 9 Filters](DESIGN.md#9-filters) (the dataset's `start`/`end` axis pair and `filters` keys);
[§ 12 Timestamp units and day boundaries](DESIGN.md#12-timestamp-units-and-day-boundaries)
(the namespace's rows); [§ 10 Symbols and `ALL`](DESIGN.md#10-symbols-and-all) (the
namespace's input forms); [§ 11 Enums and types](DESIGN.md#11-enums-and-types) (the
namespace's parameters).

**Depends on.** P3, P4, P5 (and through them PC1–PC4). **Parallel with.** every other P6
package. Every REST path, including `/fed/v1/funding-conditions` (P6g) and
`/v1/open-close/crypto/{from}/{to}/{date}` (P6f), is an ordinary allow-listed path of the
generated table.

**Shared files.** Replace the P2 stub `src/deephaven_massive/_domains/<domain>.py`, append the
domain's entries to its name lock `src/deephaven_massive/_names/<domain>.json` (by running
`scripts/refresh_massive_spec.py` offline; never by hand), and create or extend
`tests/_domains/test_<domain>.py` and `tests/data/<domain>/*.json` (plus `*.csv` for flat-file
datasets). Nothing else, with the single exception noted for P6g. If a shared module
needs a change, land it first in a separate `refactor:` PR.

**Shared tasks (per dataset).**
- [ ] `DatasetSpec` with: name (`"<ns>.<dataset>"`), query class, REST path template(s) from
      the allow-list, WS channel(s) and family, flat-file prefix(es) and documented headers, the
      per-transport key maps for tick schemas (WS key / REST wire key / CSV header, explicit, no
      fallback chains), its primary time key and fan-out key where the naming rule needs them,
      schema overrides (spec types that do not match the data, dtype narrowing), timestamp
      units per `(transport, field)`, the `start`/`end` wire parameter pair, default
      `PollMode`, page size.
- [ ] Run `uv run python scripts/refresh_massive_spec.py` (offline). With the snapshot unchanged
      it leaves `_restspec.py` as it is and appends the domain's datasets to
      `_names/<domain>.json`; review every new column name in the lock diff. If the gate stops
      on an unknown short key or a collision, the lexicon in `_specgen.py` needs a line: land it
      first in a separate PR (DESIGN § 19.3).
- [ ] Factory method on the namespace class with the exact DESIGN § 8 signature and a Google
      docstring stating the transports, the `limit` total-row semantics, and `ALL` behaviour.
- [ ] Fixture shaped from the spec's response example and the docs sample (not invented keys);
      test asserts the exact request path and params, the normalized PascalCase column values
      for every column the fixture supplies, the `Instant` of each timestamp column, and
      `Symbol`/`Underlying` per DESIGN § 10.
- [ ] Synchronous `ValueError`s listed in DESIGN § 7.2 for the dataset (`ALL` where
      disallowed, `Timespan.DAY` on futures, `SESSION` outside futures, `history(source=REST)` on
      `indices.values`, …).
- [ ] For streamed datasets: a fake-server test that one data frame lands in `blink()` with the
      dataset schema; for flat-file datasets: a one-batch CSV fixture round trip.
- [ ] Docstring examples: once the namespace's datasets exist, give the query classes in
      `_query.py`, `MassiveSession` in `_session.py`, the enums in `_enums.py`, and
      `EntitlementError` in `_errors.py` real examples that use them again (P2 removed or
      reduced every example that used a dataset, because AGENTS.md requires examples that run
      against the documented API).

**Shared acceptance criteria.**
- [ ] Every row of the namespace's table in DESIGN § 8 has a `DatasetSpec` and a test module
      section named after it.
- [ ] `tests/test_timeunits.py` (P2) passes with the new specs: every timestamp-like field of
      every REST field the dataset uses, every WebSocket key, and every flat-file header has a
      unit, and every unit-tag conflict is listed (DESIGN § 12).
- [ ] The name-lock completeness and compatibility tests in `tests/test_catalog.py` pass with the
      domain's new entries (DESIGN § 19.3).
- [ ] No fallback chains (`_first_present`, `row.get("a") or row.get("b")`) in the module.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs.

**Shared verification.**
```bash
./scripts/check.sh
uv run pytest tests/_domains/test_<domain>.py tests/test_timeunits.py tests/test_catalog.py -q
```

**Shared pitfalls (Appendices B, C, F).** Nine wrong REST paths (F C1): `/futures/vX/*`,
`/stocks/filings/13f/vX/holdings`, `insider-transactions/vX/*`, `/stocks/filings/vX/disclosures`,
`/benzinga/v1/consensus-ratings?ticker=`, `/fable/v1/eu/*`; invented wire keys in 13-F, insider,
NOI, Fable normalizers (F C3); null anchors from wrong keys — ratios `current_ratio` (real
`current`), ETF `fund_flows` (real `fund_flow`), TMX `description` (real `name`), 8-K `text`
(real `items_text`), Benzinga `buy_count` (real `buy_ratings`), `ArcaShortVolume` ←
`adf_short_volume`, IPO `SharesOffered` ← `shares_outstanding` (F M3); related companies
`Symbol` null (F M1); futures `tick_size`/`min_tick` (real `trade_tick_size`), product
`sub_asset_class` (real `asset_sub_class`) (F M2); `_normalize_bar` treating `"s"` as a start
timestamp and `_normalize_quote` falling back to `p` (F LOW); 23 aliases and ~3,500 copy-paste
lines (F H3) — one name per dataset, no aliases; Benzinga transcripts never existed (C).

**Shared out of scope.** Shared modules; README (P7); catalog exclusions (P7); live tests (P8).

**Shared approval gates.** Opening the PR.

**Follow-ups from the P2 review (PR #46): type overrides.** The packages that declare these
datasets add `ColumnOverride`s, each with a test, where the generated type does not match the
data or the design: the options contract `correction` carries an integer, whatever the spec says
(P6b; the SDK hinted `str`); the snapshot `LastQuoteAskSize` and `LastQuoteBidSize` columns are
sizes, so `double` (P6a, P6g); and every `*Exchange` and `*Tape` column is an `int` id
(DESIGN § 11), whatever its spec type. The futures and
crypto packages (P6c, P6f) map their REST and flat-file fields onto the shared columns P2 added
(see P4) and declare their timestamp units.

### P6a — `feat: stocks datasets` — branch `feat/stocks-datasets` — Issue: #17
DESIGN [§ 8.1 `m.stocks`](DESIGN.md#81-mstocks). Owns `_domains/stocks.py`, including the
nested `financials` and `filings` namespaces as inner classes (DESIGN § 4.1 fixes one module
per namespace; splitting it would need an owner decision). Datasets: `trades`, `quotes`, `bars` (overloads;
`A`/`AM`; `/prev`; range; grouped daily; `minute_aggs_v1`/`day_aggs_v1` adjusted=False only),
`luld`, `imbalances` (NOI wire keys `T,t,at,a,i,x,o,p,b`), `fmv`, `snapshots(include_otc)`,
`movers(Direction)`, `daily_open_close`, `dividends`, `splits`, `ipos`, `short_interest`,
`short_volume`, `share_float`, `financials.{balance_sheets,income_statements,
cash_flow_statements,ratios}(timeframe: Timeframe)`, `filings.{index,sections_10k,text_8k,
disclosures_8k,risk_factors,institutional_holdings,insider_holdings,insider_transactions,
risk_factor_taxonomy,disclosure_taxonomy}`. Filings paths are the spec's
`/stocks/filings/vX/{13-F,form-3,form-4}`, `/stocks/filings/8-K/vX/disclosures`,
`/stocks/taxonomies/vX/*`.

### P6b — `feat: options datasets` — branch `feat/options-datasets` — Issue: #18
DESIGN [§ 8.2 `m.options`](DESIGN.md#82-moptions). Owns `_domains/options.py`. Datasets: `trades` (latest), `quotes` (stream + history, no "last"),
`bars`, `fmv`, `chain(underlying, contract_type: ContractType)`, `snapshots` (unified
`/v3/snapshot?ticker.any_of=O:…` 250 per request; `Underlying` parsed; `ALL` allowed),
`contracts`, `daily_open_close`. OCC symbol normalization via `_symbols.py` (no hand-rolled
parser in the domain). `/v3/snapshot/options/{u}/{o}` is recorded in the spec as *covered by*
`snapshots`.

### P6c — `feat: futures datasets` — branch `feat/futures-datasets` — Issue: #19
DESIGN [§ 8.3 `m.futures`](DESIGN.md#83-mfutures). Owns `_domains/futures.py`. Datasets: `trades(symbols, *, venue=None)`, `quotes(…, venue=None)`,
`bars` (futures overloads: no `DAY`; `SESSION` allowed; `resolution = f"{multiplier}{unit}"`;
WS `A` and `AM`), `snapshots(product_code)`, `contracts`, `products`, `exchanges`,
`market_status`, `schedules`. Paths `/futures/v1/*`; `window_start` ns; session-date rules from
`_dates.py`; prefixes `us_futures_{venue}/{trades,quotes,minute_aggs,session_aggs}_v1`. Streams
always `/futures`.

### P6d — `feat: indices datasets` — branch `feat/indices-datasets` — Issue: #20
DESIGN [§ 8.4 `m.indices`](DESIGN.md#84-mindices). Owns `_domains/indices.py`. Datasets: `values` (WS `V` in the TICK family; `fetch()` projects
`/v3/snapshot/indices` to `Value`/`Timestamp=last_updated`; history flat files only, REST →
`ValueError`), `bars` (no `adjusted`), `snapshots`, `daily_open_close`. The `V` not-authorized
reply fails the stream with a message naming `ws_hosts[AssetClass.INDICES]` and
`business.massive.com` (fixture test for both replies).

### P6e — `feat: forex datasets` — branch `feat/forex-datasets` — Issue: #21
DESIGN [§ 8.5 `m.forex`](DESIGN.md#85-mforex). Owns `_domains/forex.py`. Datasets: `quotes`
(latest via `/v1/last_quote/currencies`, unit UNVERIFIED → ms with the `[1990, 2100]` guard),
`bars` (`CAS`/`CA`; grouped `…/fx/{date}`), `fmv`, `snapshots` (`ALL` = all pairs), `movers`,
`conversion(from_currency, to_currency, amount, precision)`. Pair forms `EUR/USD` ↔
`C:EURUSD`. Forex has no daily open-close dataset.

### P6f — `feat: crypto datasets` — branch `feat/crypto-datasets` — Issue: #22
DESIGN [§ 8.6 `m.crypto`](DESIGN.md#86-mcrypto). Owns `_domains/crypto.py`. Datasets: `trades` (latest via `/v1/last/crypto`, unit UNVERIFIED),
`quotes` (stream only), `bars` (`XAS`/`XA`; grouped), `fmv`, `snapshots`, `movers`,
`daily_open_close` (`/v1/open-close/crypto/{from}/{to}/{date}`, the pair filling `{from}` and
`{to}`). Pair forms `BTC-USD` ↔ `X:BTCUSD`.

### P6g — `feat: reference, indicators, economy, and unified snapshots` — branch `feat/reference-datasets` — Issue: #23
DESIGN [§ 8.7 `m.reference`](DESIGN.md#87-mreference), [§ 8.8 `m.indicators`](DESIGN.md#88-mindicators),
[§ 8.9 `m.economy`](DESIGN.md#89-meconomy), and [§ 8.11 `m.snapshots`](DESIGN.md#811-msnapshots).
Owns `_domains/reference.py` (`tickers(market: Market, …)`, `ticker_details`, `ticker_types`,
`ticker_events`, `related_companies`, `news`, `exchanges`, `conditions(data_type:
ConditionDataType, sip: Sip)`, `market_status` (one row; nested maps flattened),
`market_holidays`), `_domains/indicators.py` (`sma`, `ema`, `rsi`, `macd`; `series_type:
SeriesType`; symbol prefix selects the class; axis `timestamp` ms), and `_domains/economy.py`
(`treasury_yields`, `inflation`, `inflation_expectations`, `labor_market`,
`funding_conditions`, `/fed/v1/funding-conditions`). It also fills
`MassiveSession.snapshots()` in `_session.py` (`m.snapshots(symbols, …F)`: `/v3/snapshot`, 250 per request, mixed classes,
per-ticker `NOT_ENTITLED` rows keep `Error`/`Message` and log once) and adds that dataset's
record, `reference.snapshots` (`asset_class=None`), to `_domains/reference.py` (DESIGN § 8.11),
not `_catalog.py`; its tests for the unified snapshot go in `tests/test_session.py`. P6g is
the only domain package that edits `_session.py`. `cusip` is typed `str` in
the library, as the spec types it.

### P6h — `feat: partner datasets` — branch `feat/partner-datasets` — Issue: #24
DESIGN [§ 8.10 `m.partners`](DESIGN.md#810-mpartners). Owns `_domains/partners.py`, which
holds the four vendor classes (`m.partners.benzinga`, `.etf_global`, `.tmx`, `.fable`), with
`tests/_domains/test_partners.py` and `tests/data/partners/`. Benzinga: `analysts`,
`analyst_insights`, `ratings`, `consensus_ratings(symbols)` (path param
`/benzinga/v1/consensus-ratings/{ticker}`, one request per symbol), `bulls_bears_say`,
`guidance`, `earnings`, `firms`, `news` (`/benzinga/v2/news`, axis `published`, default
`APPEND`). ETF Global: `analytics`, `constituents`, `fund_flows`, `profiles`, `taxonomies`
(`Dict[str, float]` exposures flattened per the schema rules). TMX: `corporate_events`. Fable:
`merchant_aggregates`, `merchant_hierarchy` (`/consumer-spending/eu/v1/*`; real keys
`merchant_ticker`, `total_transactions`, `transaction_currency`).

---

<a id="p7"></a>
## P7 — `chore: finish rework`

| | |
|---|---|
| **Branch** | `chore/finish-rework` |
| **Issue** | #25 |

**Goal.** Close the rework: prove 100% Massive coverage against the vendored spec and the docs
with reviewed exclusions, delete the legacy code and every temporary exclusion, run the README
examples end to end against fake servers, rewrite `README.md`, move the durable content of
`plans/rework/` into `README.md` and `AGENTS.md`, and delete `plans/rework/`.

**DESIGN sections implemented.** [§ 8.12 Excluded endpoints](DESIGN.md#812-excluded-endpoints)
(as the catalog test's exclusion lists); [§ 21 Examples](DESIGN.md#21-examples) (as
executable tests); [§ 20 Quality gates](DESIGN.md#20-quality-gates) (the layout and catalog
gates run with no legacy exclusions); the whole document becomes `README.md` reference
material.

**Depends on.** P6a–P6h (all merged). **Parallel with.** nothing (serial).

**Files.**
- Create: `tests/test_readme.py` (the README example tests; an approved non-mirrored file
  that mirrors `README.md`, DESIGN § 4.2).
- Modify: `tests/test_catalog.py` (add the coverage assertions and exclusion lists to the P2
  and PC3 unit tests), `scripts/massive_spec/` and `_restspec.py` (refreshed, if Massive has
  changed), `README.md` (rewrite, ≈250–350 lines, O'Reilly style), `AGENTS.md` (absorb the
  durable rules from this file: parallelism/ownership conventions, the spec refresh flow,
  legacy-free statement), `pyproject.toml`,
  `tests/conftest.py`, `tests/test_layout.py` (remove every `LEGACY-EXCLUSION`),
  `.github/workflows/ci.yml` only if a job name changed.
- Delete: `src/deephaven_massive/engine/`, `domains/`, `schemas/`, `tests/legacy/`,
  `plans/rework/` (both files).

**Tasks.**
- [ ] Coverage assertions in `tests/test_catalog.py` per Appendix B §3, all against the
      vendored sources in `scripts/massive_spec/`: (A) every path of the OpenAPI snapshot ∈
      catalog ∪ `SPEC_EXCLUSIONS`; (B) every documented `GET` path in `llms.txt` ∈ catalog ∪
      `DOCS_EXCLUSIONS`, and every catalog path ∈ the allow-list; (C) every channel of
      `websocket.json` ∈ catalog ∪ `WS_EXCLUSIONS`; (D) flat-file prefixes == documented set
      and schema columns ⊆ documented headers. Exclusion lists carry `# reason:`
      per entry, exactly the DESIGN § 8.12 list (deprecated: `/v3/reference/{dividends,splits}`,
      `/benzinga/v1/news`, `/vX/reference/financials`; undocumented: `/v1/summaries`, crypto
      `/book`, `XL2`, `LV`, Launchpad, WS `/futures/{venue}`, spec-only paths; superseded:
      `/v3/snapshot/options/{u}/{o}`; legacy `/v1/historic/*`, `/v2/ticks/*`).
- [ ] Run `scripts/refresh_massive_spec.py --fetch`; if Massive has changed, review and commit
      the refreshed snapshot, `_restspec.py`, and locks (DESIGN § 19.3).
- [ ] Delete the legacy subpackages and `tests/legacy/`; remove every `LEGACY-EXCLUSION`;
      `grep -rn LEGACY-EXCLUSION .` is empty; `tests/test_layout.py` runs with no skip list.
- [ ] `tests/test_readme.py`: each DESIGN § 21 snippet runs in the embedded JVM against the
      `fake_rest` and `fake_ws_server` fixtures (examples 1–6); assertions on table columns and the
      `connections()` rows the comments claim.
- [ ] `README.md` rewrite: install (from the repository until the owner publishes to PyPI),
      a line saying the library uses Massive's documented REST and WebSocket APIs and its
      published OpenAPI spec, configuration table, quickstart,
      verbs and table shapes, symbols and `ALL`, host resolution summary, namespace index (one
      row per dataset: transports, query class), lifecycle (`release`, `close`, the console
      caveats), diagnostics, development (`check.sh`, hooks, layout rule), with no marketing
      prose and no counts.
- [ ] `AGENTS.md`: fold in the durable parts of this file; point "where to look" at modules.
- [ ] Delete `plans/rework/`.

**Acceptance criteria.**
- [ ] `tests/test_catalog.py` passes; adding a path to a copy of the snapshot without a catalog
      entry or an exclusion fails it (negative test included).
- [ ] `git grep -n "engine/\|domains/\|schemas/\|LEGACY-EXCLUSION\|plans/rework"` returns only
      `_domains/` references.
- [ ] All six README examples execute as tests in `tests/test_readme.py`.
- [ ] `./scripts/check.sh` green; CI green on all five test jobs; `uv build` wheel imports.

**Verification.**
```bash
./scripts/check.sh
uv run pytest tests/test_catalog.py tests/test_layout.py tests/test_readme.py -q
uv run python scripts/refresh_massive_spec.py --check
git grep -n "LEGACY-EXCLUSION\|plans/rework" ; test $? -eq 1
uv build && uv run --isolated --with dist/*.whl python -c "import deephaven_massive as dm; print(dm.__version__)"
```

**Pitfalls (Appendix G §6).** README claims that were false ("Zero-Copy", "lock-free", bounded
memory for S3, `pip install` of an unpublished package); reversed alias documentation; AGENTS.md
counts and the "commit messages are enforced" claim (the action lints PR titles only).

**Out of scope.** Live tests (P8); PyPI publication (an owner decision); any new dataset.

**Approval gates.** Opening the PR.

---

<a id="p8"></a>
## P8 — `test: live integration suite`

| | |
|---|---|
| **Branch** | `test/live-integration` |
| **Issue** | #26 |

**Goal.** Add `*_integration.py` tests that run only with `MASSIVE_API_KEY` (and S3 keys) set,
so the owner can retire every UNVERIFIED note in `DESIGN.md`/`README.md` with evidence: the base
Business TICK host ([DESIGN § 13.2](DESIGN.md#132-host-decision)), the indices `V` host, forex/crypto "last" timestamp units,
futures flat-file session keys, ack message text per cluster, and the options cap counting rule.

**DESIGN sections implemented.** [§ 23 Open verification items](DESIGN.md#23-open-verification-items)
(the live-test column), which retires the UNVERIFIED markers in
[§ 13 Host resolution and connection protocol](DESIGN.md#13-host-resolution-and-connection-protocol),
[§ 12 Timestamp units and day boundaries](DESIGN.md#12-timestamp-units-and-day-boundaries),
[§ 15 Options contract cap](DESIGN.md#15-options-contract-cap), and
[§ 18.4 Free-threaded specifics](DESIGN.md#184-free-threaded-specifics).

**Depends on.** P7. **Parallel with.** nothing.

**Files.**
- Create the files DESIGN § 23 names: `tests/test_hosts_integration.py`,
  `tests/test_ws_integration.py`, `tests/test_timeunits_integration.py`,
  `tests/test_flatfiles_integration.py`, `tests/_domains/test_stocks_integration.py` (each
  mirrors a source module per the layout rule; `skipif` without the key; marker `integration`).
- Modify: `pyproject.toml` (`markers = ["integration: …"]`; CI deselects `-m "not integration"`),
  `tests/conftest.py` (key fixture), `README.md` (how to run), `.github/workflows/ci.yml`.

**Tasks.**
- [ ] One test per row of DESIGN § 23, each printing the evidence it observed.
- [ ] Tests subscribe to at most one symbol per class and release immediately; they never
      exceed one connection slot per cluster.
- [ ] Document that the owner runs the suite locally; CI never has a key.

**Acceptance criteria.**
- [ ] `uv run pytest -m integration` is skipped entirely without a key and passes with one.
- [ ] A follow-up `docs:` PR removes each retired UNVERIFIED note, citing the test.

**Verification.**
```bash
./scripts/check.sh                                   # integration tests skipped
MASSIVE_API_KEY=… uv run pytest -m integration -q    # owner only
```

**Pitfalls.** Per [DESIGN § 22](DESIGN.md#22-decisions), item 19: these tests are evidence for retiring notes, never design input;
never add a probe for an unsupported endpoint (`/v1/summaries`, `/futures/{venue}`).

**Out of scope.** Changing any behaviour (file a `fix:` package if a note turns out wrong).

**Approval gates.** Opening the PR; the owner runs the suite.

**Follow-ups from the P2 review (PR #46).** Regenerate `tests/data/_symbols/crypto_tickers.csv`
from the live reference endpoint (`/v3/reference/tickers?market=crypto`) instead of the
current fixture, and note the date of the snapshot in the PR.

---

<a id="upstream"></a>
## Upstream SDK issues (optional, off the 1.0 path)

The library no longer uses Massive's Python SDK or the fork `chipkent/massive-client-python`
([DESIGN § 22](DESIGN.md#22-decisions), decisions 1 and 25). Real bugs found in
`massive-com/client-python` may be reported upstream as goodwill. This is never on the critical
path and never blocks a package.

- **Issues only, never pull requests.** No fork branch, no draft pull request, no integration
  branch.
- **The owner reviews the text first.** An agent drafts the issue (a minimal reproduction against
  the released package, the expected and the actual behavior, no mention of Deephaven) and stops
  at the gate; posting needs the owner's approval each time.
- **Candidates**, each reproduced against the latest release before drafting: a wildcard
  subscribe that drops other topics sharing its prefix (`A.*` removes `AM.X`); a clean server
  close that ends `connect()` for good; subscription changes made while disconnected being lost
  or resurrected; instant reconnects; a `datetime` parameter converted to nanoseconds through a
  float; invalid JSON or a missing `results` key returned as an empty list.

---

<a id="archive-fork"></a>
## Final step — archive the fork

After P8, once the project is fully shipped (the owner decides when), the owner archives
`github.com/chipkent/massive-client-python`. Nothing in this repository depends on it from PC4
on, so archiving changes no build. This is an owner step; no
agent archives a repository.

---

## End-to-end verification

The rework is complete when all of the following hold on `main`:

- [ ] `./scripts/check.sh` is green locally; every CI job is green, including `3.14t` with
      `PYTHON_GIL=0`.
- [ ] `tests/test_layout.py`, `tests/test_catalog.py`, and the 100% branch-coverage gate pass
      with no `LEGACY-EXCLUSION` and no unapproved `pragma: no cover`.
- [ ] The README examples (DESIGN § 21) run in the embedded test JVM against the fake REST
      and WS servers.
- [ ] `grep -rn "Literal\[\"" src/deephaven_massive/*.py src/deephaven_massive/_domains` finds no
      public string categorical.
- [ ] `plans/rework/` no longer exists; `AGENTS.md` and `README.md` carry the durable content.
- [ ] No dependency on `massive` and no `[tool.uv.sources]`; `git grep -nE "^(from|import) massive" -- src tests`
      finds nothing; `scripts/refresh_massive_spec.py --check` is clean.
- [ ] After merge, the owner runs P8 with a key and the UNVERIFIED notes are retired by a
      `docs:` PR.
- [ ] After shipping, the owner archives the fork ([Final step](#archive-fork)).

---

<a id="decisions-recorded-during-planning"></a>
## Decisions recorded during planning

The owner settled the last open questions before P1. They are already applied throughout this
file and `DESIGN.md`; this list records them in one place.

1. **`__init__.py` mirroring:** a package `__init__.py` is mirrored by
   `tests/<path>/test_init.py`; `tests/test_init.py` holds the `dm.__all__` and `__version__`
   tests (DESIGN § 4.2).
2. **README example tests:** they live in `tests/test_readme.py` (lowercase per PEP 8 and ruff
   `N999`; it mirrors `README.md`), an approved non-mirrored file created in P7.
3. **Shared fakes:** `tests/_fakes/` is an approved package exempt from the mirror rule; P1
   creates `__init__.py`, P3 `rest.py`, P4 `ws.py`, P5 `s3.py`; only `tests/conftest.py` imports
   it, through thin fixtures.
4. **Embedded test server port:** an implementer's technical choice in P1, not an owner
   decision: `deephaven_server.Server(port=0)` if supported, else fixed port `10099` with a
   comment.
5. **Own clients, no SDK** (2026-10-26; replaces the fork plan and its F2.6 decision). The
   library has its own REST and WebSocket clients and generates its REST parameters and columns
   from a pinned OpenAPI snapshot; PC1–PC4 build that ahead of P3 and P4. The fork is no longer
   used and is archived after shipping; upstream SDK bugs may be filed as issues only, after
   owner review (DESIGN § 22, decisions 1, 2, and 22 to 25).
