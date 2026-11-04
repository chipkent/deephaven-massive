# Writing user documentation

This page is the standard for every page a user reads: `README.md`,
`docs/troubleshooting.md`, the tester guide, and the hand-written text of the generated dataset
reference (its intro and the shared argument rules). It tells you where each fact goes, how to write the sentences, what shape each kind
of section takes, and how an editor grades the result. The `writing-user-docs` skill walks you
through it for each edit. Docstrings follow the `writing-docstrings` skill, and PR
descriptions follow [`pull-requests.md`](pull-requests.md).

The bar is the one an O'Reilly development editor sets. A reader should feel that a senior
colleague is explaining the library at a whiteboard. If a page reads like a specification, a
sales brochure, or a chatbot, it fails, even when every fact in it is right.

Contents:

1. [This project](#this-project): the readers, the owner's requirements, the README's job,
   the install decision, the pitch, the page map, and the teaching plan
2. [Where each fact goes](#placement)
3. [Sentences](#sentences)
4. [Form: prose, lists, tables, steps, and code](#form)
5. [AI tells](#ai-tells)
6. [Section models](#models)
7. [Terminology](#terminology)
8. [Accuracy and anchors](#accuracy)
9. [The editor's rubric](#rubric)

<a id="this-project"></a>

## This project

Everything specific to deephaven-massive is in this section: who reads the docs, what the owner
requires, what the README must do, the install decision, the README's pitch, which page owns
which fact, and the planned teaching sequence. The rest of the standard is general. When the
project changes, update this section and leave the rules alone.

<a id="readers"></a>

### Who reads the user docs

Four kinds of reader use these pages, and each must understand every page to the end:

| Reader | What they bring | What they need from the page |
|---|---|---|
| Quants | Statistics, pandas, research habits | The exact data: units, time zones, adjustment, gaps |
| Traders who can barely program | Markets, Massive's products | Complete code they paste and run, and what they'll see |
| Vibe coders | An AI assistant and a goal | Correct, whole examples that an assistant copies faithfully |
| Software engineers | Python, threads, APIs | The behavior behind the API: lifetimes, failures, limits |

Picture the reader as intelligent but new to the topic. Then test each section against the
reader least like you: would a trader who has never written a loop follow it to the end? Clarity
and organization come before everything else, length included.

<a id="owner-requirements"></a>

### Owner requirements

These are the owner's specifications for the user docs, gathered from his messages between
October 23 and November 4, 2026. Each one is binding. The date shows where it came from, so a
reader can tell which later message refined an earlier one.

#### Quality and voice

- The docs read as if an O'Reilly technical editor with two decades of experience edited them,
  and never as if an AI agent wrote them. (10-23, 11-03, 11-04)
- Every claim is technically accurate, and the page is engaging to read. Check a doubtful claim
  with a web search or the source before you write it. (11-03)
- Clarity and organization win above all. The README is in the top 1% of GitHub projects, and
  it gets there through research into what's best practice, not through habit. (11-03)
- Bullets, tables, and lists beat walls of text, so a reader can scan a page quickly. Use a
  paragraph only where an explanation needs one. (11-02, 11-03)
- More isn't better. Better, clearer, and easier to use is better. Never cut a key fact to meet
  a word budget, though: put it where the reader needs it. (10-30, 11-02)
- Users want to know how to use the library, not how it works. Write about outcomes; leave
  mechanism and internals to the contributor docs, or out. (11-04)

#### Readers

- Four audiences must fully understand every page: quants, traders who can barely program,
  vibe coders, and software engineers. (11-03)
- A newcomer gets up to speed from the README very quickly, and can quickly find how to do what
  they want. (10-30)

#### The README

- The README is a complete landing page: the sales pitch, the quick start, and everyday use, so
  a reader goes from "what is this?" to real work without leaving it. (11-03)
- It opens with one or two paragraphs that sell the library, written the way a top marketing
  professional would write a first screen. (11-03, 11-04)
- It has a slot for a hero image, which the owner will supply. (11-03, 11-04)
- It carries badges. While the repository is private, use static badges; add workflow badges
  when it's public. (11-03)
- Its models come from research into READMEs recognized as excellent as a whole, not assumed
  ones and not models for the install section alone. (11-04)
- It has no "Terms" section. The prose defines each term where the reader meets it. (11-04)
- It teaches through a series of examples, not through "How it works" or "How do I" sections.
  (11-04)
- Documenting the API doesn't take thousands of lines. The API repeats one pattern, so its
  effective size is small. (10-30)
- Small topics stay in the README rather than scattered across many 40-line files. Reference
  material looked up only for a specific case goes in the generated reference (the dataset
  pages and the docstrings) or in troubleshooting, never on a hand-written topic page.
  (10-30, 11-03, 11-04)
- It has no affiliation or legal notice section. (11-03)

#### Install and configuration

- The install is best practice: clear, scannable, in order, and free of details a new user
  doesn't need. (11-03, 11-04)
- PyPI is the gold standard. Until the PyPI release, install from git at the newest release
  tag, looked up in the same step. (11-03)
- Show both `pip` and `uv`: `pip` first, because many users don't have `uv`, and the `uv` form
  as the alternative. (11-03, 11-04)
- Never hard-code a release version. (11-03)
- Never tell the reader to open the Deephaven console before a step has started the server.
  (11-04)
- List every environment variable together, in Configuration. Settings made in code belong in
  Configuration, never in Install. (11-04)

#### Examples

- Every example is complete and runnable by an end user as pasted, with the imports and the
  session it needs, never a fragment. (11-04)
- Each example says which Massive plan or account it needs, any setting it needs, and exactly
  how to run it, then shows what you see. (11-04)
- A free-plan example comes first, and a separate, clean paid example follows it. Don't lump
  unrelated comments about plans into one example. (11-04)
- The docs cover live tables driven by a table of symbols: subscribing as symbols arrive and
  unsubscribing as they leave. (10-30, 11-02)

#### Deephaven and Massive

- Describe Deephaven correctly, in Deephaven's own terms. Research deephaven.io and
  deephaven-core before writing about it. (11-03)
- Keep Massive's names for its products, datasets, and columns; don't rename away from
  Massive's docs without a compelling case. (10-27, 10-30)
- Show what each Massive plan includes in a form that's easy to update when Massive changes its
  plans. (10-30)
- Dates in examples and tables are ISO strings, such as "2026-10-31". (10-28)

#### Specific topics

- Document split adjustment thoroughly: how to get adjusted data, how to get the data to adjust
  it yourself, and that unadjusted data jumps at a split. (10-28)
- Where Massive's data has a known problem, the docs say clearly how to filter it out. (11-02)
- Column meanings are documented for end users in the generated dataset pages, which regenerate
  without leaving orphaned files. (10-27)
- The tester guide is easy to follow: every step, the commands to run, example code, and what
  to put in a bug report, automated as far as possible. (10-30)

#### Process

- A code change that alters documented behavior updates the user docs in the same PR. (10-31)
- Material moved to another page is rewritten to this standard, never moved verbatim. Every
  user page meets the standard, not only the README. (11-03)
- Before docs go out, an adversarial reviewer checks whether they read as an O'Reilly editor's
  work, and they're fixed until they do. (11-04)

### The README's job

A prospective user goes from "what is this?" to real, productive work without leaving the
README. It's the sales page, the quick start, and the guide to everyday use at once, and it's
never a thin page of links. Reading only the README, you can:

1. See what the library is and why you'd want it.
2. Install it and make a first table, with every step saying what you should see.
3. Find every setting in one place.
4. Learn the core through a sequence of complete examples, each adding one idea.
5. Copy complete recipes for two main jobs: an option chain driving streams, and history that
   follows a watchlist. The teaching sequence covers the live board and history continued
   live.
6. Fix the handful of errors new users actually hit.
7. Know where to go next for depth.

Its length is whatever that job needs. The README carries no affiliation or legal notice
section.

<a id="install-decision"></a>

### The install decision

Until the PyPI release, the install step looks up the newest release tag itself, so the docs
never need an edit at release time:

```bash
TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
pip install deephaven-server "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
```

- `gh release list` includes release candidates; `gh release view` skips them.
- The uv form uses `uv venv` for the environment and `uv pip install` with the same arguments,
  written out in full.
- At the PyPI release, the step becomes `pip install deephaven-server deephaven-massive`, with
  `--pre` while only release candidates exist, and the GitHub prerequisites go.
- Link to a release as `https://github.com/chipkent/deephaven-massive/releases/latest`. A
  version string shown in the docs is marked as an example.
- The prerequisites are the Python version in `pyproject.toml`; the Java version on Deephaven's
  own install page, with `JAVA_HOME` set (Deephaven's server reads it first); a Massive API key
  in `MASSIVE_API_KEY`; and, while the repository is private, git and the GitHub CLI, signed
  in with `gh auth login` and set up for git with `gh auth setup-git`, so the install command
  works.
- The install gives functional instructions for a reader who can already see the repository.
  It has no step for asking for access and no contact: someone who can't see a private
  repository won't find its README.
- `deephaven server` opens the Deephaven IDE in the browser, already signed in, and prints a
  line that starts `Deephaven is running at`.
- The settings a new user needs, in Configuration, are `rest_basic_subscriptions` and
  `delayed_subscriptions`.

<a id="pitch"></a>

### The README's pitch

The library's big idea is that one query gives you both the past and the present, with the
same columns: `history()` for years of data, and a live verb such as `last_by()` for now. It
holds only for datasets that offer both (see each page's Verbs row in `docs/datasets/`). The
hook's proof points are checkable facts: how many datasets and asset classes the library
covers, live tables that follow a watchlist, what the free plan reads, and that live tables
are real time unless you turn on delayed data with one setting.

<a id="page-map"></a>

### Which page owns each fact

Each fact has one home. Other pages link to it instead of repeating it.

| Page | Owns |
|---|---|
| `README.md` | Everything the placement test keeps: the pitch, install, the one table of environment variables, the teaching sequence, two recipes (the option chain and the history of a watchlist), and the 8 to 10 errors new users hit most |
| `docs/troubleshooting.md` | The one home of every message and symptom and its fix, the diagnostics tables, the log, reporting a bug |
| `docs/datasets/` | Every dataset's call, verbs, arguments (each with its shared rule), plans, and columns; generated by `scripts/render_reference.py` from the catalog, the dataset docstrings, and `src/deephaven_massive/_datasets/argument_rules.py`, never edited by hand |
| Docstrings, read with `help()` | The fine detail of every verb, setting, enum, and exception |
| `docs/tester-guide.md` | What release-candidate testers do and report |

Installing and running Deephaven belongs to Deephaven's docs, and what each Massive plan includes
belongs to Massive's site: link them, never copy them. A rule several datasets share is written
once, in `argument_rules.py`, and every dataset page that takes the argument prints it.

Contributor material goes in `CONTRIBUTING.md` or `docs/dev/`, never on a user page.

<a id="teaching-plan"></a>

### The planned teaching sequence

The README's examples follow this plan. When the README changes, change the plan with it.

| # | Task | The one idea | Plan |
|---|---|---|---|
| 1 | See the whole US stock market's day | A query names the data; a verb returns a Deephaven table; `dm.ALL` | Free |
| 2 | Compare a few stocks over a year | `symbols` as a list; dates as `yyyy-MM-dd` text; aggregation | Free |
| 3 | Find the dataset you need | The pattern, now named; `dm.datasets()`, `columns()`, `fetch()` | Free |
| 4 | Price each dividend against the stock | Tables from two datasets join, because dates are text | Free |
| 5 | Follow the news as it breaks | `poll()`: the first live table, and how long it runs | Free |
| 6 | Learn what your plan allows | Plans, `dm.EntitlementError`, `rest_basic_subscriptions` | Free |
| 7 | Watch trades arrive, four ways | The four stream shapes; aggregating a blink table | Currencies Starter |
| 8 | Build a live board against yesterday's close | A live table joins to a static one; delayed data | Stocks Starter |
| 9 | Drive the board from a watchlist | A live symbol table drives the subscription | Stocks Starter |
| 10 | Start with today's history, then continue live | Backfill and stream from one query | Stocks Starter |

<a id="placement"></a>

## Where each fact goes

Two rules decide where a fact goes: the placement test picks its page, and the reader's need
picks its place on that page.

<a id="placement-test"></a>

### The placement test

For each fact, ask two questions:

- Will a new user need it in the first hour of real use? Then it belongs in the README.
- Is it looked up only when a specific case comes up? Then it belongs in the reference: a
  message or symptom in troubleshooting, a shared argument rule on the dataset pages, and any
  other detail of the API in its docstring. The README keeps a one-line summary with a link.

Reference material moves out by this test: each argument's full rules, edge cases, internals
a user acts on only rarely, full syntax and column-by-column tables, the full troubleshooting
table, and install paths beyond the main one. The page map in [This project](#page-map) says
where each goes.

### Keep every fact, and put it where the reader needs it

The project never cuts a true fact to save space. That rule produced the old README's worst
habit, though: sentences that carried three or four facts joined by colons, semicolons, and
parentheses, so that nothing read as the main point. The fix is to move facts, not to delete
them. Each fact gets its own sentence, in the place the reader needs it:

| Where the fact is now | Where it goes |
|---|---|
| A caveat inside the sentence that states the main point | A sentence of its own, after the main point |
| A warning after the step it guards | Inside that step, before the command |
| A troubleshooting note in the middle of a procedure | Under the step it explains, or in a "Fix a step that fails" table |
| An exception to a table cell | One sentence under the table |
| A detail only some readers need | The reference (troubleshooting, the dataset pages, or a docstring), with a one-line pointer where the reader meets the topic |
| A number the reader can't act on, such as a per-flush cost in milliseconds | The docstring or a contributor page; it isn't a user fact |
| How the library works inside: paging, pacing, reconnecting, threads, locks | The contributor docs, or nowhere. Keep only the outcome the reader sees, and what they do about it. |

Before:

> Massive's paid plans include flat files: one compressed file per day holding every trade,
> quote, or bar of a whole market, the fast way to read a lot of history.

After, one fact to a sentence:

> Massive's paid plans include flat files. Each one is a compressed file that holds a whole
> market's trades, quotes, or bars for one day. They're the fastest way to read a lot of
> history.

When you move a fact to another page, say where in the PR description, so a reviewer can check
that nothing was lost.

<a id="sentences"></a>

## Sentences

These rules come from the line editor's pass. Each one has a criterion in the
[rubric](#rubric), given in brackets.

### Write about outcomes, not mechanism [1]

Users want to know how to use the library, not how it works. Tell them what they get, what
they do, and what they'll see. Leave out how the library gets there, unless the reader has to
act on it.

| Before | After |
|---|---|
| The library pages through Massive's REST API, retries on 429, and reconnects the WebSocket when it drops. | `history()` returns every row in the range as one table. A live table keeps updating through a dropped connection. |
| You can't change a session's settings after you create it, so those two plans need a second session. | On Stocks Starter or Developer, create the session with the setting below. |

### Talk to the reader like a colleague [9]

Write in the second person, active voice, and present tense. Use contractions where you'd use
them out loud ("you'll", "isn't", "here's"), and "we" where the authors speak for the project.
Keep one person through a passage. Leave out exclamation points, "please", "simply", "easy",
and "just".

| Before | After |
|---|---|
| Outside testers follow the tester guide, which walks through every step. | If you're testing the library, the tester guide walks you through every step. |
| The library returns one row per trade to the user. | You get one row per trade. |

### Put the point first [3]

Open every section and every paragraph with its main point. Conditions, caveats, and
exceptions follow it. You should be able to read only the first sentence of each paragraph and
still follow the page.

| Before | After |
|---|---|
| The library runs on macOS and Linux, and on Windows through WSL2, on a computer with at least 8 GB of memory. It is not on PyPI, and while its repository is private... | Setup takes about 15 minutes. You'll run a Deephaven server on your own computer and use the library from your browser. |

### Make the doer the subject and the action the verb [5]

Who does what should be visible. Watch for "there is", "occurs", "allows you to", and verbs
turned into nouns. Start from what the reader sees, not from how the code is built.

| Before | After |
|---|---|
| A verb a dataset cannot serve does not exist on its query. Calling one raises `AttributeError`... | If you call a verb the dataset doesn't support, Python raises `AttributeError` and lists the verbs you can use. |
| There is a variable called `met_trick` that stores the current accuracy. | A variable named `met_trick` stores the current accuracy. |

### One idea per sentence, old before new [6]

A sentence carries one main idea. It starts with something the reader already knows and ends
with what's new. No sentence links more than two clauses with colons and semicolons, and in
running prose a colon introduces a list, a code block, or an example, never a reveal. Aim for
about 15 to 20 words a sentence on average, and split any sentence over 30 words.

| Before | After |
|---|---|
| Live data is real-time unless you ask for delayed data; on a delayed plan, such as Stocks Starter, the live line raises an error that names the setting to add. | Live tables are real-time by default. If your plan streams only delayed data, as Stocks Starter does, the `latest` line raises an error that tells you which setting to add. |

### Be concrete, and give a number when the reader acts on it [7]

Prefer the specific to the general: a real symbol, a real date, a real limit. Replace an
adjective with the number behind it. A number earns its place when it changes what the reader
does or lets them check a claim. "Keep the filter narrow: the options connection carries at
most 1,000 topics" passes. "Each stream costs the server about 0.3 ms at every flush" doesn't:
the reader can't act on it, so it's precision theater.

| Before | After |
|---|---|
| Each stream costs the server about 0.3 ms at every flush that brings it rows. | Put many symbols in one stream. Hundreds of one-symbol streams make every table fall behind. |
| Type the query and use tab completion. | Type the query followed by a dot in the console and press Tab. |

### Concise, but never telegraphic [8]

Cut filler such as "in order to", "is able to", and "note that". Keep the articles, verbs, and
connectives that make a sentence English. Strunk's standard is that every word tell, not that
every sentence be short.

| Before | After |
|---|---|
| A minute both hold shows the stream's bar. | When both tables have a bar for the same minute, the merged table keeps the live one. |
| Read once: the watchlist's symbols now, never again. | The table holds the symbols on the watchlist when you call it. Later edits don't change it. |

### Define each term once, where the reader meets it [12]

Define a term in one sentence the first time a page uses it, with a link to Deephaven's or
Massive's docs where they define it. Then use the same word every time; elegant variation
reads as AI. Don't coin a term where a plain word works. Use the words in the
[terminology](#terminology) table.

| Before | After |
|---|---|
| Massive delivers data three ways, which this guide calls *transports*. | Head the table column "Source" and define the three sources in a short list under it. |

<a id="form"></a>

## Form: prose, lists, tables, steps, and code

These rules come from the copyeditor's pass and from Google's and Microsoft's guides to scannable
writing.

### Pick the form that fits the content [13]

| Content | Form | Rules |
|---|---|---|
| An argument, a tradeoff, a reason, a cause | Prose | Lists strip out the "because" and the "so". Paragraphs run three to five sentences, never more than about seven. A run of one-sentence paragraphs means the organization is faulty. |
| Steps where order matters | Numbered list | One action per step. State the goal before the action and the result after it, in the same step. Start each step with a verb. Prefix an optional step with "Optional:". |
| Parallel items in no particular order | Bulleted list | Two to seven short items, parallel in grammar. Never a list of one. |
| Terms with definitions | Definition list | The term, then a short definition. |
| Items with three or more attributes each | Table | A meaningful header on each column, and no more than two sentences in a cell. A one-column table is a list. |
| Two or three short items inside a sentence | A run-in series | Break it out into a list when the items carry technical choices. |

Introduce every list, table, and code block with a complete sentence, and end that sentence
with a colon when the block follows at once. Make the sentence state the point the block
supports ("Massive's data goes well beyond stock prices:"), never only that a block follows
("These four facts describe what you can read:"). Editors score an empty lead-in as a
signpost (criterion 18). Leave list items without end
punctuation unless an item is a complete sentence. If one item is, punctuate them all.

### Headings [14]

- Use sentence case: "Install the library", not "Install The Library".
- Start a task heading with a bare verb ("Read daily bars"). Make a concept heading a noun
  phrase ("Split adjustment"). Avoid "-ing" openers.
- Keep headings at one level parallel in shape.
- Put no inline code or other formatting in a heading. If a code item must appear, give it a
  noun: "Sort and limit the rows", not "`sort`, `order`, and `limit`".
- Never put two headings in a row without text between them, and never skip a level.
- One heading level is usually enough for a page or two. A section with one subtopic needs no
  subheadings.

### Cross-references [15]

Make each cross-reference a live link that names its target ("see
[the option chain](../../README.md#option-chain)"). Never write "above" or "below". A
paragraph carries at most one parenthetical "see X". Collect background links in a closing
sentence: "For the details, see *Symbols* and *Where history comes from*."

### Code examples: introduce, show, explain [10]

1. Introduce. Open with what the reader will get, in a sentence: the point comes first. Then
   say which Massive plan the example needs, any setting it needs, and how to run it (for
   example, paste it into the console of the Deephaven IDE). The sentence right before the
   block says what the code does, ending with a colon.
2. Show. Give the smallest example that runs as pasted by a reader who has run nothing else on
   the page: it has its imports and creates the session it needs, with the settings it needs.
   Never show a fragment that depends on an earlier block. Make it a real task, never "Hello,
   World". Wrap lines at about 80 characters, and mark omitted code with a comment, never with
   a bare `...`. A comment says why, not what: don't echo the code.
3. Explain. After the block, say what the reader will see: the table's columns, how many rows,
   how it changes. Then explain the line that does the work by what it does ("`where` keeps
   only the rows above the threshold"), never with a setup line such as "The line that
   matters is...".
4. Keep the prose and the code in sync. The prose must describe the call the code makes.

When several examples in one section share a plan, settings, and the way to run them, say so
once, at the top of the section. Each block still repeats its imports and session, because it
must run alone, but the prose doesn't repeat the plan sentence: editors score the repetition as
rhetorical patterning (criterion 17).

Before (the prose sends the watchlist to the wrong call):

> Pass a live watchlist to `history()` and the table follows it.

After:

> Pass the watchlist as the `symbols` argument, as in `m.stocks.bars(names, ...)`, and the
> table that `history()` returns follows it.

Never invent output. When you can't verify what a table holds without an API key, describe its
shape and columns, and leave a note for the owner's keyed run to capture real rows.

<a id="ai-tells"></a>

## AI tells

The owner hears these patterns as "written by an AI". One hit proves nothing, but a cluster of
them is what a reader hears, so the [rubric](#rubric) passes a page only when none is left.

<a id="tell-contrasts"></a>

### False contrasts and staged reveals [16]

| Pattern | Before | After |
|---|---|---|
| Negative parallelism ("not X but Y", "not just X", "Y rather than X") against a misconception nobody holds | "where you type Python that runs inside the server. It is not a terminal." | "where you type Python that runs inside the server." |
| A setup line that announces instead of stating | "Here's what made it work:" "Two matter from the start." | "Two settings change how the library behaves on a free or delayed plan." |
| A question answered at once | "The cost? Nothing." | "Adding a symbol costs nothing." |
| A colon reveal | "The answer: one stream." | "Use one stream." |

A contrast is fine when the misconception is real and likely: "`m.snapshots(...)` is a method of
the session itself, not a namespace" passes in a table of namespaces.

<a id="tell-patterning"></a>

### Rhetorical patterning [17]

| Pattern | Before | After |
|---|---|---|
| A reflexive triplet | "so you can build, store, and reuse queries freely" | "so you can build a query once and make several tables from it" |
| The anaphora formula | "Fast to start. Easy to scale. Built for traders." | "Ten years of daily prices arrive as one table you can sort, join, and plot." |
| A tagline closer | "No exceptions." "That's it." | Cut it. |
| A comma chain of four or five clauses | "It pages, retries, paces, merges, and reconnects." | Say which of those the reader cares about, with what it does for them. |
| Sentences of one length and shape, paragraph after paragraph | | Vary them. Read the page aloud. |

Keep a series of three when the items really are three.

<a id="tell-filler"></a>

### Filler, hedges, and metadiscourse [18]

| Pattern | Before | After |
|---|---|---|
| Talk about the document | "This README is the whole user guide." "Words the rest of this guide uses without defining them again:" | Start with the content. Let the heading say what the section is. |
| Signposts with no content | "Two other ways fit other needs." | Name the needs: "If your list never changes, read it once instead." |
| Restating summaries | "In summary", "Overall" | Cut them. |
| Placeholder phrases | "Note that", "It's worth noting", "In order to", "Whether you're" | Start with the fact: "To...", or the subject itself. |
| Compulsive hedges | "generally", "may", "can" where the behavior is certain | State the behavior. Keep a hedge only where the outcome really varies, and say what it depends on. |
| Defensive justification | "so no table looks live while it is not" | Cut it. State the behavior; the reader doesn't need the library's defense. |
| Invented claims | "Deephaven builds nothing new on it" | Say what Deephaven does, in its own words, with a link: "the table no longer updates". |
| Hype and the AI word list | "simply", "easily", "seamless", "powerful", "robust", "leverage", "comprehensive", "unlock", "dive in", "delve", "crucial", "pivotal", "showcase", "underscore" | Cut the word, or replace it with the concrete fact it gestures at. |
| Marketing verbs for "is" and "has" | "serves as", "boasts", "features" | "is", "has" |
| Pasted rule text | "`LOG`, the default, skips the symbol or row and logs each distinct value once at WARNING; `IGNORE` skips it and logs only at DEBUG; `RAISE` fails the read or the table instead." | A table of the three settings: what you see in the table, and what you see in the log. |
| So-chains | "fails with `...`, so no table looks live while it is not. ... builds nothing new on it, so take a static copy" | Two sentences: what happens, then what you do. "The table stops updating and shows the error. To keep its rows, take a static copy:" |

<a id="tell-formatting"></a>

### Formatting tells [19]

| Pattern | Before | After |
|---|---|---|
| A list where every item opens with a bold label | "- **One query, past or live.** `week` and `latest`..." | Plain items that open with the outcome: "- `week` and `latest` have the same columns, so..." |
| A paragraph opened by a bold run-in label | "**One session is enough.** Create one session..." | A topic sentence that does the label's job: "Create one session at the top of your script and use it for everything." |
| Bold for emphasis | "This **never** changes." | Plain text. Bold is for UI elements, such as **Console**, and for the term in a definition list. |
| Bullets where an argument needs connected prose | Five bullets that are really one explanation | A paragraph with its "because" and "so" put back. |
| Title case headings | "## Make Your First Table" | "## Make your first table" |
| Telegraphic compression in a parenthesis | "`State` (`CONNECTED`, `RECONNECTING`, `WAITING_SLOT` when another program uses your key's connection, `FAILED`)" | A table: each state, what it means, and what you do about it. |
| Emoji, em dashes, exclamation points | | None. Use a comma, a colon before a list, parentheses, or a new sentence. |

Bold run-ins are right in one place: three or so true alternatives compared one after another,
each with its own paragraph and code, such as the three ways to read a watchlist's history.

Before a review, run the skill's checker, which finds the mechanical part of these patterns:

```bash
uv run python .claude/skills/writing-user-docs/scripts/check_user_doc.py <page>
```

<a id="models"></a>

## Section models

Each model below comes from documentation that outside reviewers have singled out as
excellent: Django's tutorial, FastAPI's README, Stripe's and Twilio's quickstarts, Prisma's
quickstart, the Diátaxis framework, and dplyr's introduction. Follow the model's shape. Change
its content to fit the page.

<a id="model-readme"></a>

### The whole README

The README's outline comes from READMEs that sources with stated criteria praise as a whole,
checked against their current text on 2026-11-04:

| Model | Who praises it, and for what | What we take from it |
|---|---|---|
| [FastAPI](https://github.com/fastapi/fastapi) | Codecov's [five hallmarks](https://about.codecov.io/blog/5-hallmarks-of-great-documentation-from-analyzing-fastapi-docs/): "Optimize for time to Hello World", snippets that are copyable and work, quick starts kept apart from advanced features | The whole arc: tagline, badges, features, requirements, a one-line install, then an example you create, run, and check against its exact output, then an "Example upgrade" that extends it, then a link to the tutorial |
| [revup](https://github.com/Skydio/revup) | [awesome-readme](https://github.com/matiassingers/awesome-readme): "Animated GIF demo + GIFs for major stages of the step-by-step tutorial. Concise explanation, installation instructions" | A light and dark logo in `<picture>`, a one-sentence hook, a demo, install with a command that checks it worked, credentials set up before the first example, then a tutorial in numbered stages |
| [python-game-server](https://github.com/feberts/python-game-server) | awesome-readme: "A diagram that shows at a glance what the project is about... Selected badges that only show relevant information" | A hero diagram with light and dark variants, a "Use cases" section that says who it's for, and only the badges that matter |
| [notecharts](https://github.com/dmunish/notecharts) | awesome-readme: a banner with a gallery, a "clear comparison to existing Python visualization tools and concise code snippets" | A Python data library that shows its output first, and says why you'd pick it over the tools you know |
| [Requests](https://github.com/psf/requests) | [Write the Docs](https://www.writethedocs.org/guide/writing/beginners-guide-to-docs/): "Provide a small code example. Requests is a great example." | A hook sentence followed at once by a short example that starts with its import and shows its output |

They share one order, broad to specific, which the Art of README calls the "cognitive funnel".
The README follows it:

1. The title, then the hero image.
2. A few badges, with no heading.
3. The hook: a headline, one or two selling paragraphs, a short example, and the proof points
   (see [The hook](#model-hook)).
4. Install, with a check that it worked (see [Install](#model-install)).
5. Configuration: every environment variable in one table, then the settings made in code
   (see [Configuration](#model-configuration)).
6. The teaching sequence: numbered examples, free plan first, each one complete (see
   [The teaching sequence](#model-teaching)).
7. The recipes, such as the option chain and the history of a watchlist, in the recipe shape
   (see [Recipes and reference sections](#model-guide-page)).
8. The 8 to 10 errors new users hit most, with their fixes. Every other message lives only in
   the troubleshooting page.
9. Where to go next: troubleshooting, the dataset reference, and Deephaven's own docs.
10. Status and contributing, last.

#### The hero image

The owner will supply the hero image, in light and dark versions. Put it right after the title,
as revup and python-game-server do, with GitHub's
[`<picture>` element](https://github.blog/changelog/2022-08-15-specify-theme-context-for-images-in-markdown-ga)
and relative paths:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/hero-dark.png">
  <img alt="Screenshot of the Deephaven IDE with a live board of minute bars for five stocks, updating as each minute ends." src="docs/images/hero-light.png">
</picture>
```

Write the alt text to GitHub's [style guide](https://github.com/github/docs/blob/main/content/contributing/style-guide-and-content-model/style-guide.md):

- Say what the image means, not what it looks like literally.
- Start with the kind of image ("Screenshot of", "Diagram that shows"), never with "Image of".
- Keep it to 40 to 150 characters, and end it with a period.
- Give one alt text on the `<img>`, written to cover both variants.
- Never let the image carry a fact the text doesn't also state.

PyPI doesn't render `<picture>`, and it breaks relative links. When the package goes to PyPI,
give the PyPI description absolute image URLs.

#### Badges

Keep a few, each with alt text, and ask of each one what it tells a typical reader. While the
repository is private, GitHub's workflow badges and shields.io can't read it, so use static
badges (the Python versions, the license). At the public launch, add the CI workflow badge and
the PyPI version.

<a id="model-hook"></a>

### The hook

The README opens by selling the job the library does. A reader decides on the first screen
whether to keep reading, so it carries six things, in this order:

1. The title, the hero image and badges (see [The whole README](#model-readme)), then a
   headline of about ten words that names the job in the reader's terms, from
   [the pitch](#pitch).
2. One or two short paragraphs, about the outcome. The first names the job the reader has.
   The second says what they get. Say what the library spares the reader in a clause at most:
   how it pages, paces, or reconnects is mechanism, and it doesn't belong here. Scope every
   claim to what's true: a big idea that holds for some datasets says which.
3. One sentence that defines Deephaven for a reader who's never heard of it, in Deephaven's
   own words: a real-time query engine that represents static and streaming data as tables. It
   runs on your own computer, and you type Python into its console in your browser. Put it
   where the hook first names Deephaven, never after.
4. A short, complete code block that shows the big idea. If it needs a paid plan, say before
   the block which plan it runs on as pasted, and point free-plan readers to the first free
   example. The sentence right before the block says what the code does. A sentence after it
   says what each table holds. State the big idea once; the hook is short enough that a
   repeat reads as padding.
5. Three to five proof points, as plain bullets without bold labels, each a checkable fact
   (the pitch lists them). State each one positively: "you never get X you didn't ask for"
   reads as a false contrast (criterion 16).
6. One call to action that leads to a working start: install, then the first example.

Anchor on what the reader already knows: "formerly Polygon.io" for quants, and a watchlist or a
spreadsheet-like grid for traders. Never promise what the free plan can't deliver: if the
headline example needs a paid plan, say so in the sentence after it. The hook carries no
setup facts (memory, private repositories, errors). Those belong in Install.

<a id="model-install"></a>

### Install

Install takes a new user from nothing to a first table. It follows Google's procedure rules and
the quickstarts of Django, Stripe, and Prisma:

- Open with how long it takes and what you'll end up with, in one or two sentences.
- List the prerequisites in a "Check what you need" table: what you need, why, how to
  check it, and where to get it.
- Give numbered steps with verb-first headings, one action each. Introduce each command with a
  sentence, and after it say what you should see.
- Show one path. An alternative gets a short labeled block ("If you use uv, ...") or a link.
- List only the prerequisites that make the commands work. Leave out how to request access
  and whom to contact: the install is for a reader who can already see the repository.
- Install with `pip` first, and give the `uv` commands as that alternative, written out in
  full. Many users don't have `uv`.
- Never hard-code a release version; follow [the install decision](#install-decision).
- Put the API key in an environment variable, set in the terminal that starts the server. Say
  why: the server takes its environment from that terminal when it starts, and a key kept out
  of your code never ends up in a script or notebook you save or share. Don't claim that it
  hides the key from other console users; the console can read the server's environment.
- Show what the server prints, then where to go. Never send the reader to the console before
  the step that starts the server.
- End with a first table and what it holds, then a "Fix a step that fails" table whose lead-in
  says where most problems come from. Give every heading at this level the same verb-first
  shape.
- Move everything else out: other ways to run the server, and every setting beyond the API key
  (Configuration).

The shape, with placeholders for this project's facts:

````markdown
## Install

<How long setup takes, and what the reader has at the end, in one or two sentences.>

### Check what you need

<The computer it needs, then a lead-in that states the point of the table, ending in a colon:>

| What | Why | Check | Get it |
|---|---|---|---|
| <a prerequisite and its minimum version> | <why the reader needs it> | <a command that checks it> | <where to get it> |

<Any one-time setup the commands need, such as signing in to a tool, becomes step 1, so no
second list of numbered items sits before the steps.>

### 1. <Verb-first heading for one action>

<A sentence that says what the step does, ending with a colon:>

```bash
<the commands for this one action>
```

<What the reader sees, and how to check that it worked.>

<One step for each action: the environment, the install (with the uv alternative written out
in full), the API key, the server, and the first table.>

### Fix a step that fails

<A lead-in that says where most problems come from, ending with a colon:>

| You see | Why | What to do |
|---|---|---|
| <the exact message> | <the cause> | <the fix> |
````

<a id="model-configuration"></a>

### Configuration

Configuration is reference, so it describes and doesn't narrate, as Docker's CLI reference
does. It has two parts:

- Environment variables, in one table with the columns Variable, Default, and What it does.
  This is the one place each variable is named. Say once, above the table, where to set them
  (the terminal that starts the server) and that a running server doesn't see a change.
- Settings in code. Say first that most sessions need none. Then give a table of the settings a new
  user needs (the install decision names them), with when to set each and an example, then a
  short, complete code block. Point to `help(dm.MassiveConfig)` for
  the rest, and to `help(dm.MassiveConfig.from_file)` for the settings file.

<a id="model-first-examples"></a>

### First examples

The first example is the smallest one that makes a reader say "oh, that's useful", and it
works on Massive's free plan. A paid live example follows it, introduced by what it adds.
Each example has:

- A task heading in the reader's words, which says when the example needs no paid plan.
- A first sentence that says what the reader will get.
- Then, before the code, the plan it needs, any setting, and how to run it.
- One sentence that introduces the code, then the code: complete, with its imports and its
  session.
- What you see: which tables appear, their rows and columns, and how they change.
- One to three sentences on why it works, with a link to the page that has the rest.

<a id="model-teaching"></a>

### The teaching sequence

The README teaches the library through one sequence of examples (the plan is in
[This project](#teaching-plan)). Each adds one idea to
the ones before it, and each is complete: it runs as pasted into a fresh console, with its own
imports and its own session. An example builds on the earlier ones' ideas, never on their
variables. The README has no
"Terms", "How it works", or "How do I" section: each term is defined in the example that first
uses it, and the reference tables move to the generated reference.

These patterns come from Diátaxis, the Django and FastAPI tutorials, and dplyr:

- Teach the mental model through examples, and name it only afterwards: the reader uses the
  call pattern several times before an example names it.
- Add one idea per example, and build on the one before.
- Make the first success fast and real, on the free plan.
- Name each example by the task it does, in a trader's words. A name for a part of the API
  isn't a title.
- Show what the reader should see after every example: the tables, their shape, their
  columns, and which column to watch.
- Explain afterwards, in one to three sentences, and link the page that has the rest.
- Keep each block to about 5 to 20 lines, and complete: a reader who pastes only that block
  gets the result the text describes.
- Give one path. No example shows an option that matters only in a special case.
- The free-plan examples come first. Every example says, before its code, the plan it needs,
  any setting, and how to run it.
- An example for a plan that streams delayed data creates its session with the setting that
  accepts delayed data, and says in one sentence that real-time plans leave it out.

Each example takes this shape:

````markdown
### <N>. <The task, in the reader's words>

<What the reader will get, in a sentence. The plan it needs, any setting beyond the API key,
and how to run it.>

<One sentence that says what the code does, without restating the first sentence, ending
with a colon:>

```python
import deephaven_massive as dm

m = dm.MassiveSession(<only the settings this example needs>)

<5 to 20 lines that do one task>
```

<What the reader sees: the tables, their rows and columns, and how they change.>

<One to three sentences on why it works, naming the one new idea, with a link to the page
that has the rest.>
````

<a id="model-guide-page"></a>

### Recipes and reference sections

A hand-written section is one of two kinds: a recipe in the README, or a reference section, such
as troubleshooting's tables. Keep them apart, as Diátaxis does.

A how-to section (a recipe) serves a reader at work who has a job to do:

- A task title in the reader's words.
- The problem in one or two sentences, from the reader's side ("Suppose you keep a watchlist
  and want...").
- When there's a choice, a table that compares the options before the details, so the reader
  picks first.
- For each option, a sentence that introduces the code, the code, what you see, and its cost
  or limit, kept next to it. When the options share a plan and settings, say so once, before
  the first option.
- A closing sentence with the links for the details.

A reference section serves a reader looking something up. It describes and only describes:
tables of arguments, settings, columns, or errors, consistent from section to section, with a
one-sentence introduction. It doesn't tell a story, and it doesn't repeat the README.

<a id="terminology"></a>

## Terminology

Use Deephaven's words for Deephaven and Massive's names for Massive's products. Link a term's
definition the first time a page uses it. When a page first mentions Deephaven, use its own
description: a real-time query engine that represents both static and streaming data as tables,
and whose table operations work the same way on both.

| Say | Never | Meaning |
|---|---|---|
| Deephaven IDE | web UI, web IDE, UI | Deephaven's browser interface, opened by `deephaven server` |
| console | terminal, REPL | The Console panel of the Deephaven IDE, where you run Python inside the server |
| server | worker, JVM (in user text) | The Deephaven process you start; "worker" is a Deephaven Enterprise term |
| static table | snapshot table | A table that never changes after it's made |
| live table, ticking table | refreshing table (except when quoting Deephaven) | A table that changes as new data arrives |
| update cycle | flush, tick | Deephaven applies a batch of changes to live tables during a cycle, about once a second by default; between cycles every table is consistent |
| blink table | | A live table that holds only the rows added in the current update cycle |
| append-only table | | A live table that only adds rows, keeping every one |
| ring table | | A live table that keeps the last N rows |
| `last_by` | a table type, keyed table | An aggregation: one row per key, the latest value for each |
| `snapshot()` | | A static copy of a live table as it stands now |
| Massive | Polygon (except "formerly Polygon.io") | The market-data vendor |
| plan | tier | What a Massive account pays for, by Massive's names, such as Stocks Basic or Options Starter |
| API key | token, PSK | The key Massive gives you; the PSK is Deephaven's login password, a different thing |
| flat files, S3 keys | bulk files, S3 credentials | Massive's daily bulk files and the separate keys that read them |
| REST, WebSocket | transports | Massive's request API and its streaming API |
| symbol | | The library's word for a Massive ticker, spelled exactly as Massive spells it; say "ticker" for Massive's own field |
| symbol table | | A Deephaven table with a column of symbols, passed as `symbols` |
| watchlist | followed table | A live symbol table: a call given one reads or streams the symbols it holds, and keeps up as you add or delete rows |

<a id="accuracy"></a>

## Accuracy and anchors

A page earns trust when every claim checks out and every link still lands.

### Accuracy [11]

- Verify every claim against the code, a test, Deephaven's docs, or Massive's docs. Cut a claim
  you can't trace, or keep it only after you've verified it.
- Check every number, plan name, and limit. Turn a vague quantity ("some", "a lot", "fast")
  into a measured one, or cut it.
- Every example runs. `tests/test_docs.py` runs each Python block on a user page against the
  fake Massive servers.
- Check claims about the Deephaven IDE (what a panel shows, what a hover reveals) in a running
  IDE before they go in.

### Anchors

- Give every cited section an explicit anchor, `<a id="sort-order-limit"></a>`, on the line
  before its heading. Docstrings and other pages cite the anchor, never a heading's generated
  slug, so rewording a heading breaks nothing.
- A docstring cites a page or section by its full GitHub URL, so the link opens from an IDE or
  the console: `https://github.com/chipkent/deephaven-massive/blob/main/` followed by the page,
  such as `README.md#<anchor>` or `docs/troubleshooting.md#<anchor>`. A user page links to another with a relative
  path.
- Keep an anchor's id when you reword its section.
- When you move or rename an anchor, rewrite its citations in the same PR: in `src/`, in other
  user pages, and in the tester guide. `tests/test_docs.py` fails on a cited anchor or page
  that doesn't exist.

<a id="rubric"></a>

## The editor's rubric

Every user page, and every section you write or rewrite, passes this rubric before it goes
out. It condenses what O'Reilly's development editor, technical reviewers, line editor, and
copyeditor check, with four criteria added for the habits that make text read as machine
written.

### How a grader scores a section

The grader is a cold reader playing an O'Reilly technical editor with twenty years'
experience: someone who didn't write the section and sees only the section and this standard.
For each of the 19 criteria below, the grader asks one question about the whole section and
answers with a score from 0 to 3:

| Score | What it means, in plain words |
|---|---|
| 3 | The editor wouldn't mark anything. |
| 2 | One or two slips, each a quick fix. |
| 1 | The same problem keeps coming back, and a reader would notice. |
| 0 | The problem is how the whole section is written. |

The 19 scores add up to a total out of 57. For every score below 3, the grader quotes the
worst sentence and gives a rewrite, so the writer knows exactly what to fix. Then the grader
steps back and gives one holistic score out of 10: how close the section is to something an
O'Reilly editor would send to print as it stands.

<a id="pass-bar"></a>

### The pass bar

A section passes only when all four of these hold:

1. The total is 52 or more out of 57, which is over 90%.
2. No criterion scores below 2.
3. Criteria 16 to 19, the AI tells, all score 3, because those are what the owner hears.
4. The holistic score is 9 or more out of 10.

The README's first screen (the hook, Install, and the first examples) must pass with two
independent graders who don't see each other's sheets. Every other section needs one.

A section still short of the bar after two rounds of revision goes to the owner, with the last
graders' score sheets and notes. Nobody loops on it further.

### The 19 criteria

Each row gives the criterion, the question the grader asks in plain words, and what a 3 looks
like:

| # | Criterion | The grader asks | A 3 means |
|---|---|---|---|
| 1 | Reader and goal | Do I know who this is for and what I'll get? | The opening says who the section is for, what they need first, and what they'll be able to do. It's about what the reader gets and does, not how the library works inside. |
| 2 | Progression | Does each idea come when I need it, and not before it's explained? | Ideas come in the order a newcomer needs them. Nothing is used before it's explained, and reference detail comes after the task that needs it. |
| 3 | Point first | Does each paragraph say its main point first? | Every section and paragraph opens with its main point. Reading only first sentences, you follow the page. |
| 4 | Paragraph unity | Does each paragraph stick to one topic? | Each paragraph has one topic and runs three to five sentences. No runs of one-sentence paragraphs. |
| 5 | Characters and actions | Can I see who does what in each sentence? | The doer is the subject and the action is the verb. |
| 6 | One idea, old before new | Does each sentence carry one idea, starting from something I know? | One main idea per sentence, starting from the known. No sentence chains more than two clauses with colons and semicolons. |
| 7 | Concrete and specific | Do I get real names, dates, and numbers instead of vague words? | Real symbols, dates, and limits in place of abstractions; a number wherever the reader acts on one. |
| 8 | Concise, not telegraphic | Is it short without dropping the small words that make it English? | No filler, and no dropped articles or verbs. |
| 9 | Colleague voice | Does it sound like a colleague talking to me? | Second person, contractions, one person throughout; no exclamation points, "please", "simply", or "easy". |
| 10 | Examples framed | Can I paste each example as it stands, and do I know what I'll see? | Every example says its plan, settings, and how to run it; runs as pasted, with its imports and session; and is followed by what you see. Comments say why. |
| 11 | Accuracy | Is every fact true, and do the words match the code? | Prose and code agree. Every number, plan name, and limit is checked. |
| 12 | Terms consistent | Is each term explained where I meet it, and used the same way after? | Each term is defined where the reader meets it and used the same way after. No coined jargon, and the [terminology](#terminology) table's words. |
| 13 | Right form | Are steps numbered, lists listed, and reasons in sentences? | Prose for reasons, numbered steps for sequences, bullets for parallel items, tables for three or more attributes. No cell runs past two sentences. |
| 14 | Headings | Do the headings tell me what each section does? | Sentence case, specific, parallel, and free of inline code. No stacked headings. |
| 15 | Cross-references | Do links name where they go? | Live, specific links; no "above" or "below"; at most one parenthetical "see X" a paragraph. |
| 16 | False contrasts, reveals | Does it state things plainly, without staged reveals or straw-man contrasts? | None of the [false contrasts and staged reveals](#tell-contrasts). |
| 17 | Rhetorical patterning | Is it free of formula rhythms, like reflexive lists of three and taglines? | None of the [rhetorical patterns](#tell-patterning). |
| 18 | Filler, hedges, metadiscourse | Is it free of filler, hedging, and talk about the document itself? | None of the [filler, hedges, and metadiscourse](#tell-filler). |
| 19 | Formatting tells | Is it free of formatting habits like bold labels on every bullet? | None of the [formatting tells](#tell-formatting). |

### The editor's pass

The editor's pass reads the draft cold, in this order:

1. Read only the first sentence of each paragraph. Does the section still make sense?
2. Underline each subject and verb. Is the doer the subject and the action the verb?
3. Find every colon and semicolon in running prose. If one doesn't introduce a list, code, or
   an example, split the sentence.
4. Find every "not ... but", "rather than", and "instead of". Is the misconception real?
5. Count the series of three. Keep one only if the items really are three.
6. Look for "there is", "there are", "occurs", "allows you to", "is able to", "in order to",
   "note that", "please", "simply", "easy", "just", and "very".
7. Read it aloud. A sentence you stumble over gets rewritten.
