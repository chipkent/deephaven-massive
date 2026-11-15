# Writing user documentation

This is the standard for every page a user reads: `README.md`, `docs/troubleshooting.md`,
`docs/configuration.md`, the tester guide, and the hand-written text of the generated dataset
reference. The
`writing-user-docs` skill is the workflow, and its grader,
[`agents/editor.md`](../../.claude/skills/writing-user-docs/agents/editor.md), scores a page
against this standard. Docstrings follow the `writing-docstrings` skill, which has the same
editor grade a public docstring as reference text. PR descriptions follow
[`pull-requests.md`](pull-requests.md).

The bar is a page an O'Reilly development editor would send to print. That editor checks
organization, clarity, concision, level of detail for the reader, and length, in that order;
correct sentences come last. Every reader finishes thinking the library is simple, because it
is: a session, a query, and a method that returns a Deephaven table. A reader who comes away
thinking it's hard was misled by the page. [How an editor writes](#method) is the method, and
it applies to every text an agent writes, not only user pages.

Contents:

1. [This project](#this-project): readers, pains, owner requirements, page map, install facts,
   teaching plan
2. [How an editor writes](#method): the eight rules and the budgets
3. [Facts need sources](#accuracy)
4. [Scope: only what the reader needs here](#placement)
5. [The hook](#hook)
6. [Headings and example titles](#headings)
7. [Teaching with examples](#teaching)
8. [Sentences](#sentences)
9. [AI tells](#ai-tells)
10. [Terminology](#terminology)
11. [Section models](#models)
12. [Anchors](#anchors)
13. [Grading and the pass bar](#rubric)

<a id="this-project"></a>

## This project

Everything specific to deephaven-massive is here, and the rest of the standard is general. When
the project changes, update this section and leave the rules alone.

<a id="readers"></a>

### Readers

Four kinds of reader use these pages. Each must understand every page, and each must finish it
believing the library is simple:

| Reader | What they bring | What they need from the page |
|---|---|---|
| Quant researcher | Statistics, pandas, research habits | The exact data: units, time zones, adjustment, gaps |
| Quantitative trader | Markets, Massive's products, and Python for their own signals | Code that runs live as pasted, the plan and delay of its data, and what they'll see |
| Vibe coder | An AI assistant and a goal | Whole, correct examples an assistant copies faithfully |
| Software engineer | Python, threads, APIs | Lifetimes, failures, limits |

<a id="pains"></a>

### Pains and the library's answer

A hook names the reader's job-level pain: the months, the team, or the missing data that stand
between an idea and a result. A technical irritation (429 errors, pagination, reconnect code) is
not a pain at this level; it's mechanism. These pains come from practitioner forums and vendor
blogs, gathered on 2026-11-04. The evidence is anecdotal, so state a pain in the reader's words
and keep numbers out of it unless you cite where they come from.

| Reader | Pain | What the library changes |
|---|---|---|
| Quant researcher | Months go into data infrastructure before an idea is tested, and research code is rewritten for live trading | One query gives history and a live table with the same columns, and the same Deephaven code runs on both |
| Quant researcher | Finding affordable data and making it usable | 112 datasets, listed by `dm.datasets()`, each with documented columns |
| Quantitative trader | A signal waits on live infrastructure before it trades; screeners don't compute their own signal, live | Their own signal on their own stocks is a live table, a few lines in a browser console |
| Vibe coder | The assistant builds the prototype but not the real-time parts | Live tables, reconnects, and watchlist-driven subscriptions are already built; the assistant writes table logic |
| Software engineer | A real-time stack is several systems and most of a year of work | The feed, the processing, and the UI are one Python process of Deephaven tables |

What the library doesn't do: backtest, route orders, send alerts, or read any vendor but
Massive. Never imply it does.

<a id="owner-requirements"></a>

### Owner requirements

The owner's specifications, with the dates (in 2026) they came from. Each is binding, and a
later date refines an earlier one:

| Topic | Requirement | Dates |
|---|---|---|
| Quality | The docs read as an experienced O'Reilly editor's work, never as AI-written. Clarity and organization come first; scannable lists and tables beat walls of text. | 10-23, 11-03, 11-04 |
| Simplicity | Every reader finishes a page believing the product is simple and easy to use. A passage that makes it seem hard (a needless step, reference detail in a tutorial, a stack of caveats, jargon, length) is a fault of the page. | 11-04 |
| Accuracy | Every factual claim has a source in the code, Massive's vendored docs, or Deephaven's docs, or it's cut. Nothing invented: no estimated durations or sizes. | 11-03, 11-04 |
| Outcomes | Users want to know how to use the library, not how it works. | 11-04 |
| Hook | The README sells the library in 20 seconds of reading, through the reader's job-level pains: short bullets, each led by a bold outcome. A code sample stays only if it shows a visible result, such as a chart. | 11-04, 11-05 |
| Approved text | Owner-approved intent and content bind every revision and every grader. Wording may improve, but a revision never dilutes an approved message's clarity or directness. | 11-05 |
| Only requested sections | The README holds the hook, Install, the examples, Configuration, next steps, and contributing, in that order. No recipes, no common-errors section, no status section, no "Terms", "How it works", or "How do I" sections. | 11-04, 11-05 |
| Order | Don't bury the lead: the hook, then Install, then the tutorial. Install reaches the first result by the shortest path and sets every key in one step. Reference (environment variables, settings, configuration tables) comes after the tutorial. | 11-05 |
| Install | Best practice: in order, scannable, every command working as written. Until the PyPI release, install from git at the newest release tag, looked up in the same step, never hard-coded. Offer `pip` and `uv` as a choice stated up front. Set every key in one step. | 11-03, 11-04 |
| Configuration | Environment variables say what the reader gets from each. Settings made in code are documented with `MassiveConfig`, the object they belong to. Every configuration concept a page mentions links to its reference page, generated from the code: `docs/configuration.md` for `MassiveConfig` and the settings file. | 11-04, 11-05 |
| Examples | Each is complete and runs as pasted, with its imports and session. Each says the plan it needs, any setting, and how to run it, then what you see. A free-plan example comes first. The examples form a visible teaching progression, written as a teacher writes. | 11-04 |
| Terms | Industry-standard words: Massive's for its data, Deephaven's for its operations, practitioners' for market concepts, Python's for Python. | 11-04 |
| Deephaven and Massive | Describe Deephaven in its own terms, from deephaven.io. Keep Massive's names for its products, datasets, and columns. Dates in examples are ISO strings. | 10-27, 10-28, 11-03 |
| Coverage | Live tables driven by a watchlist; which plans include each dataset, generated so it follows Massive's changes; split adjustment, documented with the `adjusted` argument; how to filter out a known problem in Massive's data; column meanings on the generated dataset pages. | 10-27, 10-28, 10-30, 11-02 |
| Tester guide | Every step, its commands, example code, and what to put in a report, automated as far as possible. | 10-30 |
| Hero image and badges | A slot for the owner's hero image after the title. Only badges that render for the page's viewers and can't go stale; the CI badge renders on github.com for people with access to the private repository. At the public launch and the PyPI release, add the badges that become possible then, such as a PyPI version badge. | 11-03, 11-05 |
| Repository | Never describe the repository as private. No legal, licensing, data-sharing, access, or contact instructions. Testers report on a GitHub issue. | 11-04 |
| Process | A code change that alters documented behavior updates the user docs in the same PR. Moved text is rewritten to this standard. Docs are graded before they go out. | 10-31, 11-03, 11-04 |

<a id="page-map"></a>

### Which page owns each fact

Each fact has one home, and other pages link to it:

| Page | Owns |
|---|---|
| `README.md` | The hook, Install, the teaching examples, Configuration (environment variables and the `MassiveConfig` settings a new user needs), next steps, contributing |
| `docs/configuration.md` | Every `MassiveConfig` setting, its settings-file key, values, and default, and how to write and load a settings file; the settings are generated from the code |
| `docs/troubleshooting.md` | Every message and symptom with its fix, the diagnostics tables, the log, reporting a bug |
| `docs/datasets/` | Each dataset's call, methods, arguments, plans, and columns; generated, never edited by hand |
| Docstrings, read with `help()` | What a caller needs to use each method, setting, enum, and exception correctly |
| `docs/tester-guide.md` | What release-candidate testers do and report |

Installing and running Deephaven belongs to Deephaven's docs, and Massive's plans to Massive's
site: link them, never copy them. Contributor material goes in `CONTRIBUTING.md` or `docs/dev/`.

<a id="install-decision"></a>

### Install facts

Each of these has a source. Check it again before you write:

- Python 3.11 or later (`pyproject.toml`); Java 17 or later with `JAVA_HOME` set (Deephaven's
  [pip install guide](https://deephaven.io/core/docs/getting-started/pip-install/)).
- Until the PyPI release, sign in with `gh auth login` and `gh auth setup-git`, then install at
  the newest release tag, looked up in the same command. `gh release list` includes release
  candidates, and `gh release view` skips them. Write the repository name out, because it
  never changes:

  ```bash
  TAG=$(gh release list --repo chipkent/deephaven-massive --limit 1 --json tagName --jq '.[0].tagName')
  pip install deephaven-server deephaven-plugin-ui deephaven-plugin-plotly-express \
    "deephaven-massive @ git+https://github.com/chipkent/deephaven-massive.git@$TAG"
  ```

- `deephaven-plugin-ui` (imported as `from deephaven import ui`) and
  `deephaven-plugin-plotly-express` (imported as `import deephaven.plot.express as dx`) are
  separate packages from `deephaven-server` (deephaven.io's
  [deephaven.ui install](https://deephaven.io/core/ui/docs/installation/) and
  [Deephaven Express](https://deephaven.io/core/plotly/docs/) pages).

- `deephaven` is a command inside the environment, so the step that starts the server runs it
  from the activated environment.
- `MASSIVE_API_KEY` is required. `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY`
  are optional, set both or neither (`_config.py`), and make a `history()` of 50 or more
  symbols, or of `dm.ALL`, read flat files instead of a request per symbol. Set all three in
  one step.
- `deephaven server` prints a line starting `Deephaven is running at` and opens the Deephaven
  IDE in the browser, signed in (`deephaven_server/cli.py`). `-Xmx4g` is the example on
  Deephaven's install page; no source gives a minimum memory or a startup time.
- The `MassiveConfig` settings a new user needs are `rest_basic_subscriptions` (free Basic
  plans, five requests a minute) and `delayed_subscriptions` (Starter and Developer plans that
  stream late).

<a id="teaching-plan"></a>

### Teaching plan

Approved by the owner on 2026-11-04, with stopping moved last on 2026-11-13. Each example adds
one idea, in dependency order: the call pattern, static tables, charts, polling, watchlists,
settings, streaming, then stopping. Each title names the idea, then what the example does.
Examples 1 to 6 and 13 run on Stocks Basic, the free plan, and 7 to 12 on Stocks Starter or
higher.

| # | Title | Builds on | Plan |
|---|---|---|---|
| 1 | Queries: read a week of Apple's daily bars | | Stocks Basic |
| 2 | Symbol lists: rank every listed US stock by one day's dollar volume | 1 | Stocks Basic |
| 3 | Date ranges: compare three stocks' price returns over a year | 1, 2 | Stocks Basic |
| 4 | Charts: plot a stock's daily closes for a year | 3 | Stocks Basic |
| 5 | Live tables: poll for the latest news headlines | 1 to 3 | Stocks Basic |
| 6 | Loading data for new symbols without restarting: add and remove stocks from a live table of previous closes | 5 | Stocks Basic |
| 7 | Plans and settings: load your settings from a file and read a delayed market snapshot | 1 | Stocks Starter or higher |
| 8 | Streaming: stream minute bars for the stocks on your list | 6, 7 | Stocks Starter or higher |
| 9 | Live calculations: track each stock's change since the previous close | 6, 8 | Stocks Starter or higher |
| 10 | Table types: keep the latest bar, the last 30 minutes, or the whole stream since you started | 8 | Stocks Starter or higher |
| 11 | Backfill: start with today's minute bars, then continue live | 3, 10 | Stocks Starter or higher |
| 12 | Dashboards: show a live chart and table together with deephaven.ui | 4, 9 | Stocks Starter or higher |
| 13 | Stopping live tables: release one table or close the session | 5 | Stocks Basic |

The owner's decisions behind it:

- Example 6's title is the owner's wording. Examples 3 and 10 say "price returns" and "table
  types" (Deephaven's word), where the approved draft said "returns" and "live-table shapes".
- Example 7 loads its settings with `dm.MassiveConfig.from_file`. `dm.MassiveConfig()` never
  reads a file, and `DEEPHAVEN_MASSIVE_CONFIG_FILE` names the file only for `from_file()` with
  no path; never imply that a session reads it.
- Examples 4 and 12 use the Deephaven Express and deephaven.ui plugins, which
  `deephaven-server` doesn't include, so the install command adds them.
- Stock streams carry data only while stocks trade, so the streaming examples say so and
  offer crypto, which trades around the clock. Free-plan examples stay under five requests a
  minute where they poll.
- Install ends by starting the server and hands off to Example 1, without repeating its code.
- Stopping comes last (owner, 2026-11-13: "Why not put the close example as the last one?
  Simple fix."), so no earlier example closes a session a later one needs. It runs on its own,
  making the live tables it stops, and says to open a new session after closing ("the close
  example could have a note about needing to recreate the session to run other examples").

<a id="method"></a>

## How an editor writes

Correct sentences are not enough. A page can be free of every tell and still be unreadable,
because it is organized for the code and not for the reader. These eight rules are what the
grader checks first.

### Eight rules

1. **Write for the reader's next action.** Name the reader and what they will do after
   reading: set a value, fix a row, decide to merge, call a method. Every sentence serves that
   action. What the code does on the way is mechanism, and mechanism is cut unless the reader
   acts on it.

2. **Rule first, then the exception.** State the general rule in one short sentence. Then the
   exception a reader can actually hit, in its own sentence. Never write out a decision tree
   as prose: "A, and B when C, or D unless E" is a table or three sentences. A case that
   belongs to another subject moves there, with a link. A case nobody hits is cut.

3. **One case per sentence.** One subject, one verb, one case. Joins that smuggle a second
   case into a sentence: ", and ... when", ", or ... unless", "; ", ": ", "as X does", "X,
   say", "until Y". Each is a place to end the sentence.

4. **Three or more parallel cases are a list or a table**, never a paragraph. Term and
   definition: a list with the term first, as O'Reilly's variable list. Steps: a numbered
   list. Symptom and fix, before and after, setting and effect: a table with one case per row.

5. **Show, then tell.** Where a reader could misread a rule, give the example before the rule,
   or the before-and-after. Say why, not only what: "so a table never fails over one bad row."

6. **Use the reader's word, and define any other at first use.** Massive's word for its data,
   Deephaven's for tables, Python's for Python, and code font for every value, `DEBUG`
   included. No name coined inside a PR or a plan, no decision or item number in place of the
   fact. If a term needs defining and the reader won't meet it again, don't use it.

7. **Say it once.** A fact lives on one page, in one sentence. Say it again only through a
   different channel: an example, a table, a comment in code.

8. **Read it aloud.** If you'd stop for breath mid-sentence, or you wouldn't say it to a
   colleague over coffee, rewrite it. Aim for sentences of 20 words or fewer on average, and
   cut every sentence the reader wouldn't miss.

### Budgets

A length a text can't meet is a sign it holds the wrong facts, not a reason to compress.

- A setting's reference entry: the rule, the values, where to see the effect, and the one
  exception a user hits. About 120 words. It opens "Set `<value>` when ...", never "Change it
  when ...".
- A troubleshooting row: one symptom, one cause, one fix.
- A verification item: three short sentences: what is unknown, what the library does
  meanwhile, which test settles it.
- A rule in `AGENTS.md`: the rule in one sentence, the reason in one.
- A PR description: under about 600 words. The Summary is two to four sentences. "Check
  before merging" is at most seven one-line items; an approval is a date and a quote of
  fifteen words or fewer, plus the question it answers, in full. No history of the branch's earlier forms, and no table of
  differences from `main`.

### Examples

Before, the entry for one setting (`docs/configuration.md`, 2026-11-15):

> Under `log` or `ignore`, a pair the list doesn't hold is one Massive has no data for: a
> stream keeps its other pairs and lists the skipped ones in its `m.streams()` row's `Error`,
> until a live table's rows are deleted; a `poll()` lists the pair in `Error`; and a `fetch()`
> of a live table shows it `DONE` with 0 rows in `m.symbol_reads()`. A stream or `fetch()`
> whose pairs are all skipped opens and gives no rows. A pair the library couldn't look up,
> because the list couldn't be read, is not skipped when you pass it in code: a stream raises
> that read's error at the call. [...]

Eleven cases in seven sentences, in the code's order, and no sentence states the rule. After:

> A crypto pair that isn't on Massive's crypto ticker list is a pair Massive has no data
> for. By default the library skips it and runs the rest of your pairs, so one bad pair never
> stops a table. Set `raise` when a stream must not run without every pair you asked for.
>
> [the three values]
>
> To see which pairs were skipped, read the table's `Error` in `m.streams()`. [...]
>
> If Massive's ticker list itself can't be read, no pair can be checked, and the failed read
> is the error. [...]

The rule, then where to look, then the one exception, each its own paragraph.

Before, a troubleshooting row:

> The table skips a row that isn't Massive's exact ticker and lists it in `Error`. A crypto
> stream does the same with a pair Massive doesn't list, and a `poll()` or `fetch()` gives that
> pair no rows. In a watchlist made by `dm.canonicalize_symbols`, a row that didn't convert has
> a null `Symbol`: every dataset skips it, and `Error` doesn't list it.

After, as two rows, each with one symptom:

> A symbol on your watchlist never appears | The row isn't Massive's exact ticker, so the
> table skips it and names it in `Error`. | Fix the row. `symbols` shows Massive's spelling
> for each asset class.
>
> A converted row never appears | `dm.canonicalize_symbols` couldn't convert it, so its
> `Symbol` is null and every dataset skips it. | `watchlist.where("isNull(Symbol)")` lists
> the rows to fix in the table you converted.

The crypto case moved to its own row. The three symptoms stopped sharing one cell.

<a id="accuracy"></a>

## Facts need sources

Every factual claim traces to a source, or it's cut. A claim is anything a reader could check:
a number, a duration, a size, a count, a plan's behavior, a column, an error message, a
command, or what a command prints.

- Sources are the code in `src/`, the generated pages in `docs/datasets/`, Massive's vendored
  docs in `vendor/massive/`, and Deephaven's docs. Keep a list of each claim and its source
  while you write, because the grader asks for it.
- Never estimate. "Setup takes about 15 minutes", "after about a minute", and "at least 8 GB"
  were invented, and each one cost the page its credibility. If no source gives a number, the
  sentence has no number.
- Every command works as written, in the state the previous step left: the right directory,
  the activated environment, the variables set. Readers come back in a new terminal, so a step
  that needs an earlier step's state restores it. Run each command, or trace each piece to its
  source.
- Every example runs: `tests/test_docs.py` runs each Python block on a user page against fake
  Massive servers. Never invent output: describe the table's shape and columns, and leave real
  rows for the owner's keyed run.
- Claims about another product use that product's words, with a link.

<a id="placement"></a>

## Scope: only what the reader needs here

Each sentence must serve the reader at this point on this page. A fact this reader doesn't
need here goes to its home page, or nowhere.

- Write only the sections the owner asked for (see [Owner requirements](#owner-requirements)).
  A section nobody asked for is a fault, however useful it seems.
- Put each fact on the page that owns it ([page map](#page-map)). Reference detail (every
  argument, edge case, error, or column) belongs in the reference, never in a tutorial.
- Document a setting with the object it belongs to: `MassiveConfig` settings under
  `MassiveConfig`, never under a heading like "Settings in code".
- Describe each environment variable or setting by what the reader gets ("Reads whole-market
  history faster on plans with flat files"), not by how the library uses it.
- Take out every needless step, variable, and choice: a shell variable that never changes, a
  key set in a second place, an alternative only a special case needs.
- Mechanism (paging, pacing, retries, threads) stays out unless the reader acts on it.

<a id="hook"></a>

## The hook

A visitor decides within about 10 seconds whether to stay, and most leave by 30 (NN/g, *How
Long Do Users Stay on Web Pages*). The hook sells the library in the first 20 seconds of
reading, about 80 to 90 words of prose:

1. **Who, what, outcome** in the first sentence, 10 to 20 words: who it's for, what it reads,
   and what they get. "[Name] is a [category] for [job]" is the pattern of the best READMEs.
2. **The pain, at the job level**, then the outcome: one or two short sentences, or two to
   four short bullets, each a before and after from [the pains table](#pains). "Months of
   plumbing before an idea is tested" is a job-level pain; "429 errors" isn't.
3. **An optional sample of six lines or fewer**, only if its result is visible: a comment or
   output that shows what the table holds. No prose between the tagline and the sample, and no
   plan caveats before it.
4. **One specific call to action**: "Install it with one `pip` command", not "Get started".

A sample earns its place only by showing a visible result, such as a chart, in a few lines,
followed by one or two plain sentences on how little it took. Code that shows no result goes.

Everything else moves down the page: plan details, dataset counts, prerequisites, and
definitions. The hook has no asides, no history ("formerly ..."), and no caveats. Link
"Massive" and "Deephaven" instead of explaining them. The hero image goes after the title,
with alt text that says what it shows. Give every doc asset a plain, conventional name, such as
`docs/images/hero.webp`.

<a id="headings"></a>

## Headings and example titles

Readers scan the first two or three words of a heading (NN/g, *First 2 Words*).

- Task sections get an imperative verb and an object: "Install the library", "Set your API
  keys". Reference sections get a noun phrase: "Configuration", "Environment variables". Never
  start with a gerund.
- Say exactly what the section delivers, in the reader's words, keyword first, sentence case,
  under about 60 characters. No numbers for sequence, no inline code, no idioms, no repeated
  word.
- Keep headings at one level parallel. Use standard names for standard sections:
  "Prerequisites", "Troubleshooting", "Next steps", "Contributing".
- An example title names the reader's goal or skill as an imperative, 3 to 7 words, with the
  domain noun up front: "Join live bars to the previous close", "Backfill a live table". Test
  it alone in the table of contents: could a reader predict the code? A title that carries two
  ideas ("and the methods it has") or a coined word ("board") fails.

<a id="teaching"></a>

## Teaching with examples

The examples teach the library, so there's no "How it works" section. They follow worked-example
research (Sweller), Diátaxis tutorials, and the Rust book:

- **Goal before code.** One to three sentences: the question a trader or researcher asks, the
  plan it needs, and how to run it. Nothing about how the code works yet.
- **One new idea per example.** The code is at most about 15 lines, complete, with realistic
  symbols and dates, and brings in one idea the reader hasn't met. Two or three comments label
  steps; they don't narrate.
- **What to notice, after the code.** Describe the result (the table's shape, one useful thing
  in it), then state the new idea as a rule that holds beyond the example. Never narrate the
  code line by line, and never restate what the code shows (`head(20)` means 20 rows).
- **Visible progression.** A short table before the first example lists each example's goal
  and new idea. Each example ends with one sentence on what the next adds. Explain an idea
  once. Later examples rely on it and carry less prose.
- **No diversions.** Edge cases, error variants, tables of alternatives, and reference detail
  get a link, not a paragraph. A table in an example teaches that example's idea or goes.

<a id="sentences"></a>

## Sentences

- **Rule first.** Open each section and paragraph with its rule, then the exception. A
  reader who reads only first sentences should know what the library does.
- **One case per sentence**, 20 words or fewer on average. The subject and its verb sit
  close together. A second case joined by "and ... when", "or ... unless", a semicolon, or a
  colon is a second sentence.
- **State a choice up front.** "Create an environment with venv or uv:", then the two blocks
  side by side. Never "do A", other material, then "or you could do B".
- **One topic per paragraph.** Every sentence's subject is about the paragraph's topic; a
  sentence that drifts goes where its topic lives.
- **Talk to the reader.** Second person, active voice, present tense, contractions. No
  exclamation points, "please", "simply", "easy", or "just".
- **Concrete over vague**, but only with sourced numbers ([Facts need sources](#accuracy)).

<a id="ai-tells"></a>

## AI tells

One tell proves nothing; readers hear a cluster. The owner calls two things AI-written: the
lexical tells a word list finds, and precise, exhaustive prose that enumerates every case the
code handles. The second passes every word check, so check structure first:

| Tell | Test | Fix |
|---|---|---|
| Asides: appositives and parentheticals ("Massive (formerly Polygon.io)", "a watchlist, a table of symbols you edit,") | Delete the aside. Is the point intact? Does this reader need it here? | Delete it, or give it its own sentence where it's needed |
| Stacked clauses: two or more of ", such as", ", which", ", including", "like" in one sentence | Count them | One per sentence |
| Case stacking: "A, and B when C, or D unless E"; a semicolon or colon chain | Count the cases in the sentence | One case per sentence, or a table |
| The code's order: cases listed in the order the implementation checks them, with no sentence stating the rule | Can the reader say the rule after the first sentence? | Rule first, then the exception a reader hits; cut the rest |
| An enumerated paragraph: three or more parallel cases in prose | Count them | A list or a table |
| A, digression, "or B" | Does an alternative come more than a sentence after its option? | State "A or B:" up front |
| Topic drift | Underline each subject. Is each about the topic sentence? | Cut or move the sentence |
| Repetition | A content word repeated in a heading or sentence; a phrase of four words or more reused on the page; a fact stated twice | Say it once |
| Invented numbers | Can the number be traced to a source? | Cite it or cut it |
| Caveat before the point | Does the paragraph open with "If", "On", "Once", or "Until"? | Rule first |
| Defensive justification | Would the reader act differently without the "because"? | Cut it |
| Narrating code | Does the prose restate what the code says? | Say what to notice instead |

The checker finds most of the lexical tells:

- A contrast ("not X but Y") with a misconception nobody holds
- Reflexive series of three, and taglines ("That's it.")
- A question answered at once, or a colon that reveals the point
- Signposts and summaries ("Here's what", "In summary")
- Hype ("powerful", "seamless", "robust", "leverage", "delve"), and "serves as" for "is"
- Bold labels on bullets of running explanation, bold for emphasis, em dashes, and emoji

Before:

> A query names the data you want from Massive (formerly Polygon.io), such as the minute bars
> of Apple and Microsoft.

After:

> A query names the data you want, such as Apple's minute bars.

<a id="terminology"></a>

## Terminology

Use each concept's own community's word. Ask whose concept a term names:

| The concept belongs to | Use | Example |
|---|---|---|
| Massive's data and products | Massive's word, from its docs | "plan", not "tier"; Massive's dataset and column names |
| Deephaven's tables and operations | Deephaven's word, from deephaven.io | "server", not "worker" (an Enterprise term); "Deephaven IDE"; "blink table" |
| Markets and trading | Practitioners' word | "backfill", "watchlist", "previous close" |
| Python | Python's word | "method", "argument", "virtual environment" |
| Something only this library has | A coined word, defined at first use | Rare: check that no community already has a word |

There's no list of approved words. The grader reads the page as a quant researcher, a
quantitative trader, a vibe coder, and a software engineer, flags each word that reader
wouldn't use, and names the word they would, with a source. Once you pick a word, use it every
time: elegant variation reads as AI.

Lead with a feature's idea, not its everyday example. When the point of a feature is that you
can change what data a live table loads while it runs, say that first; a watchlist is only the
example. "Watchlist" stays the practitioner's word for the reader's own list of symbols, and
never becomes the headline concept.

<a id="models"></a>

## Section models

The README runs broad to specific, and never buries the lead: the title and hero image, the
badges, the hook, Install, the examples, then reference (Configuration), next steps,
contributing. Install reaches the first result by the shortest path and sets every key in one
step, and no reference stands between it and the first example.

<a id="model-install"></a>

### Install

- A short prerequisites table: what, a command to check it, where to get it.
- Steps in order, one action each, under verb-first headings with no numbers: the order of
  the headings is the sequence (Google's style guide). A sentence introduces each
  command; after it, say what the reader sees. No duration estimates.
- State each choice up front ("with venv or uv:") and show both blocks together.
- Set every key in one step, in the terminal that starts the server.
- Start the server from the activated environment, and say what it prints.
- End with a first table and what it holds. Send failures to troubleshooting with one link.

<a id="model-configuration"></a>

### Configuration

Reference, so it describes and doesn't narrate:

- Environment variables, in one table: Variable, What you get, Default. Say once where to set
  them and that a running server doesn't see a change.
- `MassiveConfig`, under its own name: what it is in one sentence, a table of the settings a
  new user needs (setting, set it when, what changes), one complete code block, and
  `help(dm.MassiveConfig)` for the rest.

<a id="model-structure"></a>

### File formats and data structures

A page that documents a file format, a settings file, or a data structure shows its full shape:
every level of nesting, which values are single words or numbers, lists, or mappings, and at
least one example of each nested form in ordinary block syntax. Inline examples in table cells
don't count: the reader must never infer the structure from them.

Its reference also shows one complete example that sets every field, each with a one-line
comment, generated from the code so that a new field can't be left out. Its tables name each
nested key by its full path, such as `websocket_hosts.indices`. For each field, a reader can
decide from the page alone: what it does for them, the values it takes, its default, when to
change it, and what each choice does.

The page must pass a sufficiency test. Give a fresh reader only the page and about eight
realistic tasks, and ask for the file each task needs and for anything the page left them to
guess. The page passes when every file is right and nothing is left to guess. The
`configuration-page-sufficiency` eval case is the test for `docs/configuration.md`.

<a id="model-teaching"></a>

### Examples

Each example has a goal-first title, the goal and plan in one to three sentences, one complete
block, then what to notice and the one idea, and a bridge to the next. See
[Teaching with examples](#teaching).

<a id="anchors"></a>

## Anchors

- Give every cited section an explicit anchor, `<a id="install"></a>`, on the line before its
  heading. Keep the id when you reword the heading.
- A docstring cites a page by its full URL,
  `https://github.com/chipkent/deephaven-massive/blob/main/<page>#<anchor>`; a user page links
  another with a relative path. When you move or rename an anchor, rewrite its citations in the
  same PR; `tests/test_docs.py` fails on a missing one.

<a id="rubric"></a>

## Grading and the pass bar

[`agents/editor.md`](../../.claude/skills/writing-user-docs/agents/editor.md) in the skill is
the grader. It reads cold, as an O'Reilly editor and then as each of the four readers. It returns
every finding with its quoted sentence and a rewrite, and every claim without a source. Its ten
criterion scores come from counted findings, and its holistic score is out of 10.

A page passes only when all of these hold:

1. All four readers answer yes to "Is this product simple and easy to use?"
2. No claim is unsourced or false, and every command works as written.
3. No criterion scores below 2.
4. The holistic score is 9 or more.

Each round has a separate fact check, by a subagent that traces every claim, command, and
column name in the changed lines and any new page to its source, and one pair of independent
graders scoped to the changed sections. Apply only the fixes both graders agree on, plus every
false claim either finds, and fix wording nits directly. A second pair grades only when the
first finds a false or unsourced claim or the page misses the bar. A grader may flag owner-approved text but never
rewrites its intent or content, and it judges a revised hook against the approved version for
clarity and directness. A page passes when both graders pass it with a mean holistic score of 9
or more.

A page gets at most two grading rounds. One still short of the bar after the second goes to the
owner with its graders' score sheets, and nobody loops on it further.
