# The documentation site

The site publishes the user pages and an API reference generated from the docstrings, built by
[Zensical](https://zensical.org) from [`zensical.toml`](../../zensical.toml). It reads the
pages where they are, so the README, [`docs/configuration.md`](../configuration.md),
[`docs/troubleshooting.md`](../troubleshooting.md), and [`docs/datasets/`](../datasets/README.md)
stay the sources, and GitHub renders them as before. The API reference comes from the three
one-line pages under `docs/api/`, one per public package. Each lists the names in its package's
`__all__`, so a new public name needs no edit there.

## Build and preview it locally

The site's tools are in the `site` dependency group. Build the whole site into `site/`, as CI
does:

```bash
uv run --only-group site zensical build --strict --clean
```

`--strict` fails the build on a broken link or anchor in a published page. It doesn't fail on a
docstring problem: griffe, which reads the docstrings, only prints a line starting `griffe:`,
such as for an `Args` entry naming a parameter that doesn't exist. CI fails on those lines, so
fix each one you see.

To preview the site at <http://localhost:8000>, rebuilt as you edit:

```bash
uv run --only-group site zensical serve --open
```

## The `site` CI job

The `site` job of [`ci.yml`](../../.github/workflows/ci.yml) builds the site the same way and
fails when:

- the doctests of [`scripts/site_literal_blocks.py`](../../scripts/site_literal_blocks.py), the
  griffe extension that renders the docstrings' `::` examples as code blocks, fail;
- the strict build fails;
- the build prints a griffe warning;
- a built page shows a docstring's `::` marker as text, which means an example didn't become a
  code block.

It runs on a pull request into `main` that changes a site source: `zensical.toml`, `docs/api/`,
the extension, `README.md`, `docs/configuration.md`, `docs/troubleshooting.md`,
`docs/datasets/`, `docs/images/`, `src/deephaven_massive/`, or `pyproject.toml`. Like every
job, it also runs on a push to `main`, a scheduled run, a pull request into another branch, a
change to a workflow, and a change to a path no rule covers. `ci-ok` requires it whenever it
runs.

## Publishing to GitHub Pages

Publishing is off. [`docs-site.yml`](../../.github/workflows/docs-site.yml), the Docs site
workflow, runs on every push to `main` and by hand (Actions, Docs site, Run workflow). Its two
jobs, build the site and publish to GitHub Pages, run only when the repository variable
`PUBLISH_DOCS` is exactly `true`; otherwise both show as skipped, so a merge publishes nothing.

Publish at the public launch, not before: GitHub Pages serves a private repository's site
publicly on every plan except GitHub Enterprise Cloud. To turn publishing on:

1. Under Settings, Pages, Build and deployment, set Source to "GitHub Actions".
2. Under Settings, Secrets and variables, Actions, Variables, add the repository variable
   `PUBLISH_DOCS` with the value `true`.
3. Publish the current `main`: under Actions, Docs site, choose Run workflow on `main`.

The site is then at <https://chipkent.github.io/deephaven-massive/>, and each push to `main`
publishes it again. A custom domain also needs `site_url` in `zensical.toml` changed to match.

To stop publishing, delete the variable or set it to anything but `true`. Later pushes skip
both jobs, and the site stays online as last published. To take the site offline too, unpublish
it under Settings, Pages, or delete the Pages site:

```bash
gh api --method DELETE repos/chipkent/deephaven-massive/pages
```

Turning it back on after a deletion starts again from step 1.
