# Massive's specifications and the generated schemas

The library does not hand-write the shape of anything Massive sends. Every wire schema
(`src/deephaven_massive/_massive/spec/*.json`) is generated from a pinned copy of Massive's
published specifications, and the dataset reference (`docs/datasets/`) is generated from the
dataset schemas. This page says how to take a change from Massive, how to answer a refresh
gate, and what to do when a refresh adds something new.

## The pieces

- **The snapshot**, `vendor/massive/`: Massive's published specifications, vendored as the
  generator's input, and the knowledge-base articles the library relies on, vendored so the
  drift check sees them change. They are Massive's content, not covered by this repository's license, and
  shipped in neither the wheel nor the sdist (CI's build job checks both).
- **The refresh**, `deephaven_massive._massive.refresh`: reads the snapshot and writes
  `spec/*.json`. It never touches the network unless asked to. Run it through
  `scripts/refresh.py`, which starts the embedded Deephaven server first: the refresh is part
  of the library, and importing the library imports Deephaven, which needs a running JVM
  (JDK 17 or later).
- **The hand input**: the only place a person adds knowledge the vendor's specifications lack.
  Every entry carries its reason. Tests check most tables for one, but not all: write it anyway.
  Hand input names, groups, and verifies; it never lists a schema's columns, and an entry that
  names something the snapshot no longer generates fails the refresh.
  - `refresh/lexicon.py`: lexicon entries, `SHAPES`, and `IDENTITY`;
  - `refresh/overrides.py`: `NAMES`, `UNITS`, `TAG_CONFLICTS`, `TYPE_FIXES`, `KEY_FIXES`,
    `UNDECLARED_FIELDS`, `OPTIONAL_FIELDS`, `DESCRIPTION_FIXES`, `RESULT_KEYS`,
    `PARAM_UNITS`, `KINDS`, `PROJECTIONS`, `TRANSPORT_ONLY`, `VENUES`, and `SPELLINGS` and
    `SHARED_DESCRIPTIONS`, which give one field one name across datasets;
  - `refresh/exclusions.py`: `EXCLUSIONS`, `SAMPLE_ERRORS`, and `IGNORED_KEYS`.
- **The dataset reference**, `docs/datasets/`: an index, `README.md`, and one page per dataset,
  `<dataset>.md` (`stocks.trades.md`, `snapshots.md`), each with Massive's plan tables for
  its pages, rendered by
  `scripts/render_reference.py`, which owns the whole directory: it rewrites stale pages and
  deletes any file it did not render.
- **The README's plans table**: which of Massive's plans include the data the tutorial reads,
  rendered by the same command from the same plan tables, between two marker lines of
  `README.md`. Its rows name Massive's pages (`_datasets/readme_plans.py`, `ROWS`), and its
  columns are the plans those pages list.

Each WebSocket channel in `ws.json` also carries the hosts its page's "Connection" table
lists, one per feed (`Real-Time`, `Delayed`, `Business`). The library streams from those
hosts and names them in its errors, so a host Massive moves arrives with the next refresh, and
a feed label the refresh does not know stops it.

A schema is fixed before data arrives, merged at run time from the generated files and never
inferred from data: a live table needs its full column set before its first row, the first
message may omit optional fields, and two sessions must agree on a shape. Each dataset page
links only its own Massive documentation: for a source several classes share, its own class's
page.

Never edit `spec/*.json` or `docs/datasets/` by hand. Their parity tests compare them byte for
byte with what the generators produce.

## Commands

```bash
uv run python scripts/refresh.py                             # regenerate spec/*.json; no network
uv run python scripts/refresh.py --fetch                     # replace the snapshot with the live sources
uv run python scripts/refresh.py --check --report drift.md   # write the drift report; changes nothing
uv run pytest tests/massive/refresh -q                       # the parity test and every refresh gate
uv run python scripts/render_reference.py                    # regenerate docs/datasets/ and the README's plans table
uv run pytest tests/datasets/test_column_reference.py tests/datasets/test_readme_plans.py -q   # their parity tests
```

## Taking a change from Massive

1. Run `--fetch`, then the plain refresh.
2. When a gate stops the refresh, it prints each problem: the gate, the source, the key, and the
   vendor's description. Answer each with an entry and its reason in the matching hand-input
   table (listed above), never by editing a generated file.
3. A `field-spelling` stop means two sources name one field two ways: fields Massive
   describes with the same text take different long names, or fields with the same wire key
   and text sit in different columns. Prove the two are the same field from Massive's
   documentation of both (quote it) and their types and units; then give them one name with a
   `SPELLINGS` entry that takes the spelling Massive uses on another endpoint and cites it. If
   they are different fields that happen to share a text, add a `SHARED_DESCRIPTIONS` entry
   saying why. If the documentation leaves any doubt, write a live test that reads both fields
   for one symbol back to back, record a verification item, and ask the owner before renaming
   ([decision 101](decisions/0101-same-field-one-column-name-across-datasets-spellings.md)).
4. A vendor description that is clearly wrong for its source (another asset class's or event's
   text, or a statement Massive's own documentation of the field contradicts) is corrected the
   same way, by a `DESCRIPTION_FIXES` entry that quotes it.
5. Run `scripts/render_reference.py`: every page shows the snapshot's fetch date, so any refresh
   changes the dataset reference. Before it writes the pages, the command prints one line per
   dataset whose columns the refresh changed (`columns: m.forex.quotes: added …; removed …;
   retyped …`): the refresh itself cannot see datasets, so this is where a refresh shows what
   it does to each table. Put those lines in the pull request.
6. Put the snapshot diff, the `spec/*.json` diff, the hand-input lines, the regenerated
   `docs/datasets/`, and the README's regenerated plans table in one pull request.

Rerun `render_reference.py` too whenever a dataset, a namespace, a dataset summary, or the
column-description rule changes; the parity test fails until you do.

## When live data contradicts the specification

A live run can show Massive sending what its specification does not say: a value in another
form, a field it does not declare, a declared required field it omits, or a key that carries
nothing. Each is answered by a hand entry whose reason quotes the live values and their date,
never by editing a generated file:

- a value in a form the declared type refuses: a `TYPE_FIXES` entry with
  `observed_live=True` (no documented sample shows the form, so the stale check leaves it be);
- a field the specification does not declare: an `UNDECLARED_FIELDS` entry, which the refresh
  generates as if declared;
- a required field Massive omits: an `OPTIONAL_FIELDS` entry;
- a timestamp in another unit than documented: its `UNITS` entry, `ms|ns` when the two disagree;
- an undocumented key that carries nothing: an `IGNORED_KEYS` entry, which the decoders drop
  without reporting drift.

## When a refresh adds a source

A source is one endpoint, channel, or flat-file prefix the refresh generates, and every one must
be read by a dataset. When a refresh adds one, the same pull request adds a dataset, excludes the
source for good, or defers it with an issue. The module docstring of `refresh/exclusions.py`
explains the three choices, and the coverage test in `tests/datasets/test_catalog.py` says what
it expects when it fails. An exclusion never means "not yet".

Which endpoints the library supports: every documented one, plus a new one the specification
has before the documentation catches up. A deprecated or sunset endpoint is excluded with its
reason, and an undocumented one is not supported.

A new dataset's columns must not bring a second name for a field another dataset already has.
The `field-spelling` gate catches the fields Massive documents with the same text; it cannot
see a field documented in other words, so also compare the new dataset page's columns with
those of the datasets that serve the same objects (the other snapshots, the reference data of
the same instruments) before the pull request, and answer a match as step 3 of "Taking a change
from Massive" says.

## Knowledge-base articles

Some rules the library depends on are stated only in Massive's knowledge base, not in its API
documentation: the wording of the HTTP 429 that says a subscription is over its per-minute
request limit, and that reference and cross-asset endpoints are counted in a rate-limit bucket
of their own, both from "What is the request limit for Massive's RESTful APIs?". `ARTICLES` in
`refresh/knowledge_base.py` lists each such article by its slug, with what the library takes
from it. Articles are HTML pages only, so `--fetch` extracts each one's title and article text
as Markdown and stores it as `vendor/massive/knowledge-base/<slug>.md`, with its URL and hashes
in the manifest.

- **To depend on another article**, add its slug and the reason to `ARTICLES`, run `--fetch`,
  and cite the vendored file from the code that relies on it.
- **When `--check` reports an article's text changed**, read the new text and check that the
  rule named in the report still holds. If it does, `--fetch` takes the new text; if not, fix
  the code first, in the same pull request.

## Drift

`--check` reports what differs between the live sources and the snapshot, each item under the
action that resolves it, in the Markdown file `--report` names: the embedded server writes its
own startup lines to standard output, so the report never goes there.
`.github/workflows/spec-drift.yml` runs it weekly and keeps one standing "Massive API drift"
issue. It is advisory, never a required check.
