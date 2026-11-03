## 20. Quality gates

Every change must pass `scripts/check.sh full`, and every CI job the change selects, the
free-threaded 3.14t job included whenever the tests run; `ci-ok` is the required check that
enforces it. A pull request into `main` runs only the CI jobs its changed files can affect, read
from the diff between `main` and GitHub's merge of the pull request (a renamed file counts under
both paths):

- **Always:** the lint job, which lists the changed files and decides, and `ci-ok`, the single
  required check. `ci-ok` passes only when the lint job passed, each decision is exactly true or
  false, every job the change selected passed, and every other job was skipped.
- **Lint** (ruff, mypy, pyright): a file under `src/`, `tests/`, or `scripts/`, or
  `pyproject.toml`.
- **Tests** (the whole matrix, 3.14t and the lowest-direct job included): a file under `src/`,
  `tests/`, `scripts/`, `vendor/`, or `docs/datasets/`, or `pyproject.toml` or `.gitattributes`.
- **Build** (the sdist, the wheel, and the installed-wheel check): a file under `src/`,
  `tests/`, or `docs/datasets/`, or `scripts/check_wheel.py`, `pyproject.toml`, `README.md`,
  `LICENSE`, `.gitattributes`, or `.gitignore`.
- **Nothing beyond the decision:** files no job reads: `plans/`, `docs/dev/`,
  `docs/live-tests.md`, `AGENTS.md`, `.claude/`, the pull request and issue templates,
  `.github/dependabot.yml`, and `.pre-commit-config.yaml`.
- **Everything:** a change under `.github/workflows/` or `.github/actions/`; any changed path
  that matches none of the lists above, so a new input (a tool's configuration file at the
  root, a new directory, a name git must quote) fails closed until a rule covers it; a pull
  request into a branch other than `main`; every push to `main`; and a weekly scheduled run, so
  a dependency release that breaks the library is caught within a week even when nothing
  reaches `main`.

The gates, in brief ([section 20.1](#201-tooling-configuration) gives the exact configuration):

- **Coverage:** 100% statement and branch coverage. `pragma: no cover` needs a written reason
  and owner approval.
- **Ruff:** `select = ["ALL"]` with a documented ignore list, latest version, Google docstring
  convention, `E501` at 100 columns, complexity limits enforced.
- **Mypy:** `strict` plus `warn_unreachable` and the extra error codes; the only override is
  `ignore_missing_imports` for `pyarrow.*`. Pyright runs as a non-gating second checker.
- **Pytest 9:** `strict = true`, `filterwarnings = ["error"]`, `--import-mode=importlib`,
  deadlock detection through `faulthandler_timeout` and `pytest-timeout`, and parallel
  workers (pytest-xdist) in the local full check.
- **Layout:** `tests/test_layout.py` passes: the mirror rule and naming of
  [section 4.2](04-package-layout.md#42-test-layout), the library's layers of [section 4.3](04-package-layout.md#43-layers), and the
  `_massive` boundary and layers of [section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot).
- **Spec:** `tests/massive/refresh/test_generate.py` passes (parity of `spec/*.json` byte for
  byte, documented = generated ⊎ excluded ⊎ (deprecated ∪ draft), and every refresh gate),
  and `tests/datasets/test_catalog.py` passes (the catalog gates, generated = used ⊎ deferred) ([section
  19.4](19-wire-clients-and-the-spec-snapshot.md#194-generated-schemas)). They read only vendored files, never the network.
- **Dataset reference:** `tests/datasets/test_column_reference.py` passes: `docs/datasets/` holds
  exactly the rendered pages, byte for byte, with no stale, missing, or extra file, and every
  dataset method's docstring names its own page; `tests/namespaces/test_query_building.py` passes:
  every dataset method's docstring starts with its `DatasetSpec.summary`
  ([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)).
- **Negative tests:** every gate has a negative test that fails when the gate is deleted.
- **Process:** conventional-commit titles; every change lands through a pull request; only the
  owner merges.

**Spec drift is advisory, never a gate.** `.github/workflows/spec-drift.yml` runs
`python -m deephaven_massive._massive.refresh --check` weekly and on demand, the same command a
person runs locally ([section 19.6](19-wire-clients-and-the-spec-snapshot.md#196-refresh-and-drift)). It runs without S3 credentials,
so flat-file coverage comes from the documentation pages and overviews.

- **No drift:** the job does nothing: no issue, no comment.
- **Drift:** it opens, or updates in place, the single standing issue "Massive API drift"
  (found by that title and the `spec-drift` label, which it creates when missing; reopened if
  closed; never a duplicate), and comments once when the report of an open issue changed. The
  body lists each item with its action:
  - **unaccounted items** (a documented REST endpoint, WebSocket channel, or flat-file dataset
    neither generated nor excluded): refresh and add a dataset, or add an `EXCLUSIONS` entry
    with a reason;
  - **field, type, and column changes** (fields and parameters added, removed, or retyped;
    WebSocket keys; CSV headers; changed unit tags; keys the gates would stop on): run
    `--fetch` and the plain refresh, adding a lexicon, `NAMES`, or `UNITS` line where a gate
    asks;
  - **deprecations** (a generated path newly deprecated or removed): replace the dataset's path
    or add it to `EXCLUSIONS` with a reason.
- Its permissions are `contents: read` (the checkout of the private repository) and
  `issues: write` (the label and the standing issue), nothing else. It never pushes, never
  opens a pull request, and is never a required check, because a change at Massive must not
  block unrelated pull requests.

### 20.1 Tooling configuration

This is the exact configuration the tooling planned PR applies. Every lint ignore carries a
reason comment in `pyproject.toml`; adding one needs owner approval.

- **Ruff.**
  - `select = ["ALL"]`. Linters and type checkers track their latest releases: the dev group
    declares floors only (`ruff>=…`, `mypy>=…`, `pyright>=…`), and with no committed `uv.lock`
    every `check.sh` run (`uv sync --upgrade`) and every CI job installs the newest releases; a
    plain `uv sync` or `uv run` keeps the local lock. Because `ALL` enables new rules as they ship, a new
    release may surface new findings; the next PR fixes them (or proposes an ignore for owner
    approval).
  - `preview = true` with `explicit-preview-rules = true`, enabling only the preview rules
    selected by name: `DOC201` and `DOC501` (docstring Returns and Raises sections must cover
    the code), and `PLR0904`, `PLR0914`, and `PLR0916` (the Pylint limits on public
    methods, local variables, and boolean expressions, which are still preview rules). Preview
    also switches some stable rules to their preview behavior.
  - Global ignores: `COM812`, which conflicts with `ruff format` (it is the only one of ruff's
    documented formatter-overlap rules that reports anything on formatted code; the others stay
    enabled), plus `TRY003`, `EM101`, `EM102`, `PLR0913`, `CPY001`, and `DOC502` (many functions raise
    through helpers, which it cannot follow; `tests/test_errors.py` checks the public API's
    Raises sections in both directions instead).
  - Enforced on purpose: `E501` at 100 columns, `ANN401` (no `Any`), the `TC` import rules,
    `SLF001` in `src/`, `C901` at a maximum of 12, the Pylint size limits (`PLR0904` and
    `PLR0911`–`PLR0916`, except `PLR0913` above), and `PLR0917` with `max-positional-args = 3`.
  - Ignored in `tests/` only: `S101`, `PLR2004`, `SLF001`, `ARG001`, `ARG002`, `FBT001`
    (listener stand-ins keep Deephaven's `on_update(update, is_replay)` signature), and
    `D100`–`D103` and `D107` (test docstrings are optional, but any that exist must be well
    formed).
  - `src = ["src", "."]`, the same import roots as mypy and pytest, and
    `namespace-packages = ["tests"]` (`tests/` has no `__init__.py`), so `INP001` needs no
    ignore. `banned-api` bans both `conftest` and `tests.conftest`.
  - Google docstring convention; `docstring-code-format = true`.
- **Mypy.** `strict`, `warn_unreachable`, `warn_unused_configs`, and `enable_error_code =
  ["ignore-without-code", "redundant-expr", "truthy-bool", "truthy-iterable",
  "possibly-undefined", "unused-awaitable", "deprecated", "explicit-override"]`, with
  `mypy_path = ["src"]`. The dev group adds `types-boto3[s3]`. The only override is
  `ignore_missing_imports` for `pyarrow.*`, which ships no type information; `urllib3`,
  `websockets`, and `certifi` ship their own. No other override is allowed. Pyright runs as a
  non-gating second checker.
- **Pytest 9** (native `[tool.pytest]` table). `strict = true`, `filterwarnings = ["error"]`,
  `addopts = ["-ra", "--import-mode=importlib", "-m",
  "not integration and not benchmark and not soak"]`, `pythonpath = ["."]` (test code imports
  helpers as `tests._fakes…`, so `tests/massive/` never shadows a top-level `massive`),
  `faulthandler_timeout = 120` with
  `faulthandler_exit_on_timeout = true`, and `pytest-timeout` at 60 seconds. Neither coverage
  nor `-n` is in `addopts`, so single-test runs and debuggers work in one process.
- **Parallel workers.** `scripts/check.sh full` runs pytest with `-n auto --maxprocesses=4`
  (pytest-xdist): each worker is its own process with its own embedded JVM, and the controller
  that hands out the tests starts none. pytest-cov combines the workers' coverage, so the 100%
  gate is unchanged. A run that only collects or lists fixtures collects in the main process,
  as xdist does, so that process starts a JVM. A parallel run that selects any live test is a
  usage error, since live tests share rate limits and one evidence log: once a worker has
  collected, the controller reads the ids of the tests it selected (after `-m`, `-k`, and every
  other deselection) and ends the run before handing out any test if one is in a
  `*_integration.py` file. The decision rests on the selection, never on the `-m` expression,
  so `-m ""` and `-m "integration and needs"` are refused like `-m integration`. CI runs the
  suite in one process: its standard runners have 2 CPUs, where two workers gained nothing
  once their startup was paid.
- **Coverage.** `branch = true`, `fail_under = 100`, `exclude_also = ["if TYPE_CHECKING:",
  "@(typing\\.)?overload"]`, each with a reason.
- **Deephaven imports.** Module-level wherever used at run time; imports used only in
  annotations go under `TYPE_CHECKING` (enforced by `TC`). `tests/conftest.py` starts the
  embedded JVM in `pytest_configure`, before collection.
- **One entry point.** `scripts/check.sh [fast|full] [--no-fix]`. Both tiers first run
  `uv sync --upgrade`, so they use the newest versions; offline, a local run warns and keeps
  what is installed, and CI fails. The fast tier then runs ruff fix, ruff format, and mypy; the
  full tier adds pytest with coverage, on up to four worker processes. Local hooks and CI call
  the same script; CI passes `--output-format github` to ruff.
- **Agent hooks** (committed `.claude/settings.json`). After an agent edits a `.py` file, ruff
  fixes and formats it and reports remaining diagnostics to the agent. Before an agent ends its
  turn, `scripts/check.sh fast --no-fix` runs. Bare `python`, `pytest`, `ruff`, `mypy`, and `pip`
  commands not prefixed by `uv run` are blocked.
- **Git hooks** (`.pre-commit-config.yaml`). The commit stage runs the fast tier plus
  `no-commit-to-branch` (main), `check-yaml`, `end-of-file-fixer`, and `trailing-whitespace`.
  The full tier runs at the pre-push stage.
- **Packaging.** `uv.lock` is not committed, and CI resolves afresh on every run. The wheel
  excludes all of `refresh/` (`[tool.hatch.build.targets.wheel] exclude =
  ["src/deephaven_massive/_massive/refresh"]`), the snapshot included; `spec/*.json` ship as
  package data, and the CI build job asserts both. The build job also installs the wheel, with
  `deephaven-server`, into a fresh environment and runs `scripts/check_wheel.py` there: inside
  a started server it imports every shipped module, resolves every public name, renders the
  column reference from the installed catalog and compares it with `docs/datasets/`, and opens
  and closes a session. Development dependencies use a PEP 735
  `[dependency-groups]` table; license metadata follows PEP 639. `__version__` comes from
  `importlib.metadata`. `deephaven-server` is a development dependency only. The runtime
  dependencies are declared directly, with floors: `deephaven-core`, `pyarrow`, `boto3`,
  `botocore` (a floor whose `Config` takes `proxies` and `retries={"total_max_attempts": …}`),
  `urllib3`,
  `websockets` (a floor whose `websockets.client` defines `process_exception`, 17.1, which
  also reads proxies from the environment in `websockets.asyncio.client.connect`), and
  `certifi`. There is no `massive` dependency and no `[tool.uv.sources]` table: every
  dependency comes from PyPI. `.gitignore` lists only the files this project produces.
- **Banned import.** `[tool.ruff.lint.flake8-tidy-imports.banned-api]` bans `massive` with the
  message "The library has its own clients; see deephaven_massive._massive". A ban is not an
  ignore, so it needs no approval.
- **Release by one manual run; PyPI publishing off until the owner turns it on.**
  `.github/workflows/release.yml` runs only when started by hand (`workflow_dispatch`) on
  `main`, with the version as a required input; nothing is edited to release, since the tag is
  the only source of the version (see the next bullets). One run refuses a version not of the
  PEP 440 form `X.Y.Z[aN|bN|rcN]`, whose tag `v<version>` exists, or that is not greater than
  every tagged version (`packaging.version` order); runs the whole CI workflow on `main`'s
  newest commit through `workflow_call`, whose build job builds the wheel and sdist once with
  that version (`SETUPTOOLS_SCM_PRETEND_VERSION`) and fails unless the installed
  `deephaven_massive.__version__` and the sdist's `PKG-INFO` equal it; and only then pushes the annotated tag `v<version>` on that exact commit with the run's
  own `GITHUB_TOKEN` and creates a GitHub Release, marked as a pre-release for a candidate,
  with both distributions attached and notes that git-cliff (`.github/cliff.toml`) generates
  from the squash-merged pull request titles, grouped by Conventional Commits type. A
  `dry_run` input does everything but tag, release, and publish. There is no hand-written
  changelog: the generated notes are the history. Publishing to PyPI through trusted
  publishing (`id-token: write`, no long-lived token) is the last job and runs only when the
  repository variable `PUBLISH_TO_PYPI` is `true`, release candidates included. Candidate tags
  and GitHub pre-releases stay after the final release. The first release candidate is
  0.1.0rc1. **Hazard:** a PyPI version can never be re-uploaded, so a release run after the switch is on
  is final. `docs/dev/releasing.md` gives the steps.
- **Metadata.** `authors = [{ name = "Chip Kent", email = "chipkent@illumon.com" }]`; repository
  `https://github.com/chipkent/deephaven-massive`.
- **Version from the git tag.** No file holds the version: `dynamic = ["version"]` with
  hatch-vcs (`[tool.hatch.version] source = "vcs"`) takes it from the tag `v<version>` at build
  time, and `deephaven_massive.__version__` reads the installed metadata. An untagged commit
  builds a development version past the latest tag (`0.1.0rc2.dev5+g1a2b3c4`). CI checks out the
  whole history, and `.git_archival.txt` (`export-subst`) gives a `git archive` export its
  version. CI's build job proves a tagged build carries exactly the tag's version.
