# Releasing

A release is one manual run of the Release workflow, `.github/workflows/release.yml`, on `main`,
given the version to release. The git tag is the only source of the package's version, so no
file is edited to release. The run tests `main`'s newest commit, builds the package with that
version, and only when everything passes tags the commit and creates a GitHub Release with
generated notes and the wheel and sdist attached. Publishing to PyPI is part of the workflow but
stays off until the owner turns it on (see the last section).

## Cut a release

1. **Run the Release workflow** on `main` with the version: under Actions, Release, Run
   workflow, or

   ```bash
   gh workflow run release.yml --ref main -f version=0.1.0rc1
   ```

   Add `-f dry_run=true` (or tick **dry run**) to do everything except tagging, releasing, and
   publishing; the run's summary page then shows the notes the release would have.
2. **Check the Release page.** The run takes about as long as CI, about half an hour. It ends
   with the tag `v0.1.0rc1` on the tested commit and the release on the repository's Releases
   page. Edit the release there to add a summary above the generated notes.

## Versions

### Which number to raise

A change breaks when code written to the last release's documented contract stops working or
gets a different documented result. The next version depends on the stage:

- **Before `0.1.0`**: alphas, betas, and release candidates are pre-releases and owe no
  compatibility. Any change goes into the next pre-release (`0.1.0rc3`).
- **In `0.x`, from `0.1.0`**: a breaking change raises the minor version (`0.1.4` → `0.2.0`);
  anything else raises the patch (`0.1.4` → `0.1.5`).
- **From `1.0.0`**: standard [Semantic Versioning](https://semver.org/): a breaking change
  raises the major version, a backward-compatible feature the minor, and a fix the patch.

From `0.1.0` on, a breaking change needs the owner's permission, and its pull request names the
next version.

### How a version is spelled

Versions follow [PEP 440](https://peps.python.org/pep-0440/), Python's version standard: pip and
PyPI read and order versions by it. Semantic Versioning's pre-release form, `0.1.0-rc.1`, is
not a PEP 440 version; tools normalize it to `0.1.0rc1` or reject it, so the release uses the
PEP 440 spelling directly, and the tag, the files, and `deephaven_massive.__version__` all
show the same text. The workflow accepts:

| Version | Meaning | Order |
|---|---|---|
| `0.1.0a1` | alpha | before `0.1.0b1` |
| `0.1.0b1` | beta | before `0.1.0rc1` |
| `0.1.0rc1` | release candidate | before `0.1.0` |
| `0.1.0` | final release | |

A version must be greater, in that order, than every version already tagged: after `v0.1.0`,
the next candidate is `0.1.1rc1` or `0.2.0rc1`, never `0.1.0rc3`.

Between releases, a build of `main` gets a development version past the latest tag, such as
`0.1.0rc2.dev5+g1a2b3c4` for the fifth commit after `v0.1.0rc1`.

## What the run does

1. **Checks the version**, in seconds: the run must be on `main`; the version must have a form
   from the table above, its tag `v<version>` must not exist, and it must be greater than every
   tagged version. It also writes the release notes.
2. **Runs the same tests as every pull request** on that commit, every job included, the
   free-threaded 3.14t and lowest-dependency ones too: it calls the CI workflow itself. CI's
   build job builds the wheel and the sdist once, with the version being released, installs
   the wheel in a clean environment, and fails unless `deephaven_massive.__version__` and the
   sdist both equal that version exactly. If any job fails, nothing is tagged or released.
3. **Tags and releases**: pushes the annotated tag `v<version>` on the exact commit the tests
   ran on, even if `main` has moved on since, then creates the GitHub Release with the CI
   build's wheel and sdist attached. A pre-release (`a`, `b`, `rc`) is marked as one, so GitHub
   never shows it as the latest. The run uses only its own `GITHUB_TOKEN`, in that one job; a
   tag pushed with that token starts no other workflow.
4. **Publishes to PyPI**, only when turned on.

The notes list the pull requests merged since the previous release, by title and number,
under Features, Fixes, Docs, Tests, Refactors and Chores (CI and dependency changes count as
chores), plus Other for a title with no recognized type. They come from the squash-merge
commit subjects on `main`, through [git-cliff](https://git-cliff.org) and its settings in
`.github/cliff.toml`. A candidate's notes list what changed since the previous tag of any kind,
so the notes of `rc2` show what `rc1` lacked; a final release's notes list everything since
the previous final release.

Candidate tags and their GitHub pre-releases stay after the final release; nothing removes
them. Testers install a candidate from its tag, as `docs/tester-guide.md` describes.

## When a run fails

- **A flaky job**: re-run the failed jobs from the run's page; the release follows when they
  pass.
- **A real problem**: nothing was tagged or released. Fix it in a pull request and run the
  workflow again with the same version.
- **A bad release after the fact**: release the next version. Deleting a GitHub Release and its
  tag is possible, but a version published to PyPI can never be uploaded again.

## Publishing to PyPI

Publishing is off. The `publish to PyPI` job runs only when the repository variable
`PUBLISH_TO_PYPI` is exactly `true`; otherwise it shows as skipped, as it does on every dry
run. It uses PyPI's trusted publishing: PyPI accepts a short-lived token that GitHub signs for
that one job, so no PyPI token is stored anywhere. To turn it on:

1. On PyPI, under your account's Publishing settings, add a pending trusted publisher with
   project `deephaven-massive`, owner `chipkent`, repository `deephaven-massive`, workflow
   `release.yml`, and environment `pypi`.
2. Optionally, on GitHub, under Settings, Environments, create the environment `pypi` with a
   required reviewer, so each upload waits for your approval.
3. Flip the switch: under Settings, Secrets and variables, Actions, Variables, add the
   repository variable `PUBLISH_TO_PYPI` with the value `true`.

From then on, every release uploads its wheel and sdist to PyPI after the GitHub Release is
created, release candidates included: `pip install deephaven-massive` skips pre-releases and
installs one only when asked, with `--pre` or an exact version such as
`deephaven-massive==0.1.0rc1`. A version uploaded to PyPI can never be uploaded again, even
after it is deleted, so a broken release needs a new version. To turn publishing off, delete
the variable.
