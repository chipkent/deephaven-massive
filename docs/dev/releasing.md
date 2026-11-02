# Releasing

A release is one manual run of the Release workflow, `.github/workflows/release.yml`, on
`main`. The run tests `main`'s newest commit, and only when everything passes does it tag that
commit and create a GitHub Release with generated notes and the built wheel and sdist attached.
Publishing to PyPI is part of the workflow but stays off until the owner turns it on (see the
last section).

## Cut a release

1. **Merge a version-bump pull request.** Change `version` in `pyproject.toml`, for example to
   `0.1.0`, in a PR titled `chore: release 0.1.0`, and merge it. `main` accepts changes only
   through pull requests, so the workflow cannot change the version itself. The version in
   `pyproject.toml` is the package's only version: the wheel, the sdist, the tag, and
   `deephaven_massive.__version__` all take it from there.
2. **Run the Release workflow** on `main`: under Actions, Release, Run workflow, or

   ```bash
   gh workflow run release.yml --ref main
   ```

   Tick **dry run** (`-f dry_run=true`) to do everything except tagging, releasing, and
   publishing; the run's summary page then shows the notes the release would have.
3. **Check the Release page.** The run takes about as long as CI, about half an hour. It ends
   with the tag `v0.1.0` on the tested commit and the release on the repository's Releases
   page. Edit the release there to add a summary above the generated notes.

## What the run does

1. **Checks the version**, in seconds: the run must be on `main`; the version must have the
   form `X.Y.Z`, or `X.Y.Z` followed by `aN`, `bN` or `rcN`; and the tag `v<version>` must not
   exist yet. It also writes the release notes.
2. **Runs the same tests as every pull request** on that commit, every job included, the
   free-threaded 3.14t and lowest-dependency ones too: it calls the CI workflow itself. CI's
   build job builds the wheel and the sdist once and checks the wheel in a clean environment.
   If any job fails, nothing is tagged or released.
3. **Tags and releases**: pushes the annotated tag `v<version>` on the exact commit the tests
   ran on, even if `main` has moved on since, then creates the GitHub Release with the CI
   build's wheel and sdist attached. It uses only the run's own `GITHUB_TOKEN`, in that one
   job; a tag pushed with that token starts no other workflow.
4. **Publishes to PyPI**, only when turned on.

The notes list the pull requests merged since the previous release, by title and number,
under Features, Fixes, Docs, Tests, Refactors and Chores (CI and dependency changes count as
chores), plus Other for a title with no recognized type. They come from the squash-merge
commit subjects on `main`, through [git-cliff](https://git-cliff.org) and its settings in
`.github/cliff.toml`.

## A release candidate

A candidate goes through the same steps with a version such as `0.1.0rc1` (`aN` and `bN` work
the same way for an alpha or a beta). The release is marked as a pre-release, so GitHub never
shows it as the latest. Its notes list what changed since the previous tag of any kind, so the
notes of `rc2` show what `rc1` lacked; the notes of a final release list everything since the
previous final release. Testers install a candidate from its tag, as `docs/tester-guide.md`
describes.

## When a run fails

- **A flaky job**: re-run the failed jobs from the run's page; the release follows when they
  pass.
- **A real problem**: nothing was tagged or released. Fix it in a pull request and run the
  workflow again; the version can stay the same.
- **A bad release after the fact**: delete the release and its tag on GitHub, or release the
  next version.

## Publishing to PyPI

Publishing is off. The `publish to PyPI` job runs only when the repository variable
`PUBLISH_TO_PYPI` is exactly `true`; otherwise it shows as skipped. It uses PyPI's trusted
publishing: PyPI accepts a short-lived token that GitHub signs for that one job, so no PyPI
token is stored anywhere. To turn it on:

1. On PyPI, under your account's Publishing settings, add a pending trusted publisher with
   project `deephaven-massive`, owner `chipkent`, repository `deephaven-massive`, workflow
   `release.yml`, and environment `pypi`.
2. Optionally, on GitHub, under Settings, Environments, create the environment `pypi` with a
   required reviewer, so each upload waits for your approval.
3. Flip the switch: under Settings, Secrets and variables, Actions, Variables, add the
   repository variable `PUBLISH_TO_PYPI` with the value `true`.

From then on, every release, candidates included, uploads its wheel and sdist to PyPI after
the GitHub Release is created; `pip install deephaven-massive` skips pre-releases unless asked
for them with `--pre`. A version uploaded to PyPI can never be uploaded again, even after it is
deleted, so a broken release needs a new version. To turn publishing off, delete the variable.
