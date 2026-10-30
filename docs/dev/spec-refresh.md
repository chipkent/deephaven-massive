# Massive's specifications and the generated schemas

The library does not hand-write the shape of anything Massive sends. Every wire schema
(`src/deephaven_massive/_massive/spec/*.json`) is generated from a pinned copy of Massive's
published specifications, and the column reference (`docs/datasets/`) is generated from the
dataset schemas. This page says how to take a change from Massive, how to answer a refresh
gate, and what to do when a refresh adds something new.

## The pieces

- **The snapshot**, `vendor/massive/`: Massive's published specifications, vendored as the
  generator's input, and the knowledge-base articles the library relies on, vendored so the
  drift check sees them change. They are Massive's content, not covered by this repository's license, and
  shipped in neither the wheel nor the sdist (CI's build job checks both).
- **The refresh**, `deephaven_massive._massive.refresh`: reads the snapshot and writes
  `spec/*.json`. It never touches the network unless asked to.
- **The hand input**: the only place a person adds knowledge the vendor's specifications lack.
  Every entry carries its reason. Tests check most tables for one, but not all: write it anyway.
  - `refresh/lexicon.py`: lexicon entries, `SHAPES`, and `IDENTITY`;
  - `refresh/overrides.py`: `NAMES`, `UNITS`, `TAG_CONFLICTS`, `TYPE_FIXES`, `KEY_FIXES`,
    `DESCRIPTION_FIXES`, `RESULT_KEYS`, `PARAM_UNITS`, `KINDS`, `PROJECTIONS`,
    `TRANSPORT_ONLY`, and `VENUES`;
  - `refresh/exclusions.py`: `EXCLUSIONS` and `SAMPLE_ERRORS`.
- **The column reference**, `docs/datasets/<namespace>.md` (and `snapshots.md` for
  `m.snapshots`), rendered by
  `scripts/render_reference.py`, which owns the whole directory: it rewrites stale pages and
  deletes any file it did not render.

Never edit `spec/*.json` or `docs/datasets/` by hand. Their parity tests compare them byte for
byte with what the generators produce.

## Commands

```bash
uv run python -m deephaven_massive._massive.refresh           # regenerate spec/*.json; no network
uv run python -m deephaven_massive._massive.refresh --fetch   # replace the snapshot with the live sources
uv run python -m deephaven_massive._massive.refresh --check   # report drift from the live sources; changes nothing
uv run pytest tests/massive/refresh -q                        # the parity test and every refresh gate
uv run python scripts/render_reference.py                     # regenerate docs/datasets/
uv run pytest tests/datasets/test_column_reference.py -q      # the column-reference parity tests
```

## Taking a change from Massive

1. Run `--fetch`, then the plain refresh.
2. When a gate stops the refresh, it prints each problem: the gate, the source, the key, and the
   vendor's description. Answer each with an entry and its reason in the matching hand-input
   table (listed above), never by editing a generated file.
3. A vendor description that is clearly wrong for its source (another asset class's or event's
   text, or a statement Massive's own documentation of the field contradicts) is corrected the
   same way, by a `DESCRIPTION_FIXES` entry that quotes it.
4. Run `scripts/render_reference.py`: every page shows the snapshot's fetch date, so any refresh
   changes the column reference. Before it writes the pages, the command prints one line per
   dataset whose columns the refresh changed (`columns: m.forex.quotes: added …; removed …;
   retyped …`): the refresh itself cannot see datasets, so this is where a refresh shows what
   it does to each table. Put those lines in the pull request.
5. Put the snapshot diff, the `spec/*.json` diff, the hand-input lines, and the regenerated
   `docs/datasets/` in one pull request.

Rerun `render_reference.py` too whenever a dataset, a namespace, a dataset summary, or the
column-description rule changes; the parity test fails until you do.

## When a refresh adds a source

A source is one endpoint, channel, or flat-file prefix the refresh generates, and every one must
be read by a dataset. When a refresh adds one, the same pull request adds a dataset, excludes the
source for good, or defers it with an issue. The module docstring of `refresh/exclusions.py`
explains the three choices, and the coverage test in `tests/datasets/test_catalog.py` says what
it expects when it fails. An exclusion never means "not yet".

## Knowledge-base articles

Some rules the library depends on are stated only in Massive's knowledge base, not in its API
documentation: the wording of the HTTP 429 that marks a Basic subscription, and that reference
and cross-asset endpoints are counted in a rate-limit bucket of their own, both from "What is
the request limit for Massive's RESTful APIs?". `ARTICLES` in `refresh/knowledge_base.py`
lists each such article by its slug, with what the library takes from it. Articles are HTML
pages only, so `--fetch` extracts each one's title and article text as Markdown and stores it
as `vendor/massive/knowledge-base/<slug>.md`, with its URL and hashes in the manifest.

- **To depend on another article**, add its slug and the reason to `ARTICLES`, run `--fetch`,
  and cite the vendored file from the code that relies on it.
- **When `--check` reports an article's text changed**, read the new text and check that the
  rule named in the report still holds. If it does, `--fetch` takes the new text; if not, fix
  the code first, in the same pull request.

## Drift

`--check` reports what differs between the live sources and the snapshot, each item under the
action that resolves it. `.github/workflows/spec-drift.yml` runs it weekly and keeps one standing
"Massive API drift" issue. It is advisory, never a required check.
