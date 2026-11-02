# Releasing

A release is a version tag pushed to GitHub. The release workflow,
`.github/workflows/release.yml`, does everything else: it tests the tagged commit, then creates
a GitHub Release with generated notes and the built wheel and sdist attached. Publishing to
PyPI is part of the workflow but stays off until the owner turns it on (see the last section).

## Cut a release

1. **Set the version in a pull request.** Change `version` in `pyproject.toml`, for example to
   `0.1.0`, in a PR titled `chore: release 0.1.0`, and merge it. The version in
   `pyproject.toml` is the package's only version: the wheel, the sdist, and
   `deephaven_massive.__version__` all take it from there.
2. **Tag the merged commit and push the tag.** The tag is `v` followed by the exact version:

   ```bash
   git fetch origin
   git log -1 --format='%h %s' origin/main   # check that this is the version change
   git tag -a v0.1.0 -m v0.1.0 origin/main
   git push origin v0.1.0
   ```

3. **Watch the run** under Actions, Release. It takes as long as CI, about half an hour, and
   ends with the release on the repository's Releases page.

## What the workflow does

1. **Checks the tag**, in seconds: the tag must equal `v` plus the version in `pyproject.toml`,
   the version must have the form `X.Y.Z` or `X.Y.Z` followed by `aN`, `bN` or `rcN`, and the
   tagged commit must be on `main`. A failure here stops the run before any test.
2. **Runs CI** on the tagged commit: the same workflow as every pull request, with every job,
   including the free-threaded 3.14t and lowest-dependency jobs. Its build job builds the wheel
   and the sdist once and checks the wheel in a clean environment. If any job fails, nothing is
   released.
3. **Creates the GitHub Release**, named after the tag, with the CI build's wheel and sdist
   attached. The notes list the pull requests merged since the previous release, by their
   titles and numbers, under Features, Fixes, Docs, Tests, Refactors and Chores (CI and
   dependency changes count as chores), plus Other for a title with no recognized type. They
   come from the squash-merge commit subjects on `main`, through
   [git-cliff](https://git-cliff.org) and its settings in `.github/cliff.toml`. Edit the
   release on GitHub afterwards to add a summary at the top.
4. **Publishes to PyPI**, only when turned on.

## A release candidate

A release candidate goes through the same steps with a version such as `0.1.0rc1` and the tag
`v0.1.0rc1` (`aN` and `bN` work the same way for an alpha or a beta). The release is marked as
a pre-release, so GitHub never shows it as the latest. Its notes list what changed since the
previous tag of any kind, so the notes of `rc2` show what `rc1` lacked; the notes of a final
release list everything since the previous final release. Testers install a candidate from its
tag, as `docs/tester-guide.md` describes.

## When a run fails

- **A flaky job**: re-run the failed jobs from the run's page; the release follows when they
  pass.
- **A real problem**: delete the tag, fix the problem in a pull request, then tag the fixed
  commit on `main` with the same name, as in step 2 above:

  ```bash
  git push --delete origin v0.1.0rc1
  git tag -d v0.1.0rc1
  ```

- **After the GitHub Release exists**, delete the release on GitHub too before tagging again.

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

From then on, every release tag, release candidates included, uploads its wheel and sdist to
PyPI after the GitHub Release is created; `pip install deephaven-massive` skips pre-releases
unless asked for them with `--pre`. A version uploaded to PyPI can never be uploaded again,
even after it is deleted, so a broken release needs a new version. To turn publishing off,
delete the variable.
