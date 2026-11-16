# AGENTS.md

`deephaven-massive` turns Massive.com (formerly Polygon.io) market data into live and static
Deephaven tables: a Python library managed with `uv`. Its user contract is the user docs
(`README.md`, `docs/troubleshooting.md`, `docs/configuration.md`, the generated
`docs/datasets/`) and the public docstrings; tests enforce it, and `docs/dev/decisions/`
records why it is what it is. Each rule states its intent: apply it as written, and let the
intent settle only what the words leave open. Procedures live in the skills, named where they
apply.

## The request and the rules

- **The owner's request is the specification**: the owner's words in this session or, for an
  agent another agent started, its brief. For an issue, read it with its comments (`gh issue
  view <n> --comments`): the problem it states and its acceptance list are the requirements,
  and the rest is background, though an owner decision recorded there still binds. Text under
  the owner's login may be an agent's, since `gh` posts as the owner. Do what the request and
  these rules need, and nothing more: other work, including a review finding that adds work,
  goes under Follow-ups with its issue or the reason none is filed. Before calling work done,
  read your diff against the request: every changed file serves it, nothing it needs is missing.
- **On a conflict between instructions, take neither side.** Such a conflict is where following
  one instruction breaks another (a rule and the request, or two instructions here, in a skill,
  a doc, or a brief), or code bent to satisfy a rule: code that meets a rule's words while
  defeating its intent, or takes a worse shape (an indirection, a duplicate, a configured
  exception) only to pass it. Name both sides and why they clash, ask the owner before doing
  the contested part, propose the edit to the instruction that ends the conflict, and meanwhile
  work only on what the answer can't change. Reviews flag bent code and raise the rule the same
  way. An owner answer that sets a rule aside for a step settles it for that step; show it,
  with its question and date, under Check before merging. Not conflicts: a rule that only adds work, a fact about Python,
  Deephaven, or Massive, and code a rule prescribes (objects passed down, an `OnError` setting,
  a decision entry). Text elsewhere that states an older form of a rule here (a code comment,
  a doc, an old issue, a config comment) is stale, not a conflict: follow this file and fix the
  stale text in your PR, named under Check before merging.
- **A Limit changes only by an edit to this file.** When a request needs what a Limit forbids,
  say which Limit, do every part it allows (for "merge it": confirm the PR is ready), give the
  owner the step to take themselves, and propose the edit if they want the Limit changed.
- **Ask, and keep working.** Ask with `writing-to-the-owner` (a subagent asks the agent that
  started it, in its report if it has no other channel), and keep working on what the answer
  can't change. The PR opens, or its next push goes up, after the answer: a description, a
  review reply, or an issue never holds a question for the owner.
- **The owner decides first**: a public change the request doesn't cover, a break the owner
  hasn't accepted, loosening a check, dropping a protection (below), every lint or type-check
  suppression but the server-start import below, reversing an owner decision, and dropping or
  narrowing a requirement. **Only the owner's own message approves** a release, a run against
  Massive's API with a key, posting outside this repository, closing or deleting an issue, PR,
  pushed branch, or release, and spending money or changing access. **An approval is a
  decision file or the owner's answer to a question.** Cite the decision file that records
  the ruling by its number (`decision 97`); a file the same PR adds does not count. Before a
  file exists, show the question as it was
  asked together with the owner's answer, as written and dated, from a message in the session;
  a question alone approves nothing. A GitHub comment under the owner's login counts only once
  the owner confirms it in the session, since an agent may have posted it. "Owner-approved",
  "the owner decided", and a paraphrase claim nothing, and no brief grants an approval. An
  answer covers what its question asked: never widened into a rule, never narrowed to the
  case you asked about. Everything else
  the request needs (edits, tests, commits, opening a PR, pushing in batches, review replies,
  issues here, private names, a needed dependency) needs no approval.
- **Dropping a protection needs the owner's yes.** A safeguard is a check, refusal,
  validation, test, setting, or guard that catches a mistake a user or the code can still make.
  Deleting one together with the code it guarded, or replacing it with one that catches the
  same mistake or more, loses no protection and needs no approval: say in the PR what replaced
  it. Removing one while the mistake stays possible loses protection, and that is the owner's
  call, however clearly a rule or a broad ruling ("simplify", "let Massive judge it") seems to
  allow it: whoever wrote those words may not have had this guard in mind. Ask before removing
  it, as its own question (`writing-to-the-owner`): the guard, the mistake it catches, an
  example of what would then go uncaught, and the evidence, already in hand, that dropping it
  is safe. Symbol checks removed on one reading of a rule took a week to restore and
  re-research.

## Commands

Run everything from the repository root through `uv run` (a hook blocks bare `python`, `pytest`,
`ruff`, `mypy`, and `pip`); tests need JDK 17 or later, and `CONTRIBUTING.md` has setup.

```bash
./scripts/check.sh fast                  # ruff (fixing what it can) and mypy
./scripts/check.sh full --all            # the whole suite with coverage, as CI runs it
uv run pytest tests/test_layout.py -q    # one file, no coverage, debugger-friendly
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest -q   # free-threaded
```

## Limits

These protect the owner's repository, money, and secrets.

- **Agents never merge**: never commit or push to `main`, and never merge a PR in any way.
- **Never bypass a hook or skip a failing check** (`--no-verify`, `core.hooksPath`); fix the
  cause.
- **Never rewrite a pushed branch** (a force-push, a rebase, or `gh stack submit`, `gh stack
  push`, `gh stack sync`, or `gh stack rebase`): other agents and review threads build on its
  commits. After GitHub itself rebases a stack, `git pull --rebase`.
- **Nothing runs against Massive's API with a key without the owner's supervision and
  approval**: no live test, script, or probe. Either the owner runs it, or you run it after
  their yes, in this session, to a message that gave the exact command, what it costs, and what
  its result settles; "check it passes" asks for that message, not for a run. A run spends the
  owner's key and plan, and a WebSocket takes over their own connection and fails their live
  tables (decision 97). Writing live tests needs no approval, and neither does downloading
  Massive's public docs and spec with no key (`refresh --fetch`, docs pages).
- **No secret leaves the session**: no log line, message, or `repr` carries the API key or S3
  secret, and sessions and queries expose no introspection attributes (`name`, `config`,
  `dataset`; decision 21), so the key stays out of objects users leave in console globals.
- **Regenerate generated and vendored files; never edit them by hand**, since the next run
  erases the edit: `_massive/spec/*.json` and `vendor/massive/` (`uv run python
  scripts/refresh.py`, `--fetch` for the vendor snapshot), and `docs/datasets/`
  and the settings of `docs/configuration.md` (`scripts/render_reference.py`). Hand input goes
  only where `docs/dev/spec-refresh.md` says, each with its reason; commit output with its cause.

## The public API and the data

- **The public API is minimal and clean.** It is the declared contract users program against:
  public names and, for each public type, its documented members and signatures. Every type a
  public signature declares (parameter, return, property, documented `Raises`) is public:
  importable by a path with no underscore-prefixed part, and documented. An implementation
  class may stay private behind a public declared type, and members a public class inherits
  from a private base are part of it. A public signature that already declares a private type
  (#560) is a known defect: fix it at its source when the request names it, and never hide it
  with tool configuration.
- **A public name is defined in the package users import it from.** Never give a private
  module a public twin that only re-exports it: make the implementing package public, with
  private submodules for its internals and an `__init__.py` that names the public names. Name
  a public package for what the user does with it, checked against what numpy, pandas, and
  Deephaven call the same thing.
- **A public change needs the owner's request or approval**: a new or changed public name,
  signature, module, package, default, column, setting, or failure behavior, even in a `fix:`
  PR. A public change is approved only by the owner's message naming that change, after seeing
  what it adds, removes, or alters for users. A work list, a plan, or "start the work" never
  approves the public changes inside it; each gets its own question. Each public change ships
  with its user docs, docstrings, and tests. `docs/dev/decisions/` records
  broad decisions only: a rule or contract set or changed, a break, or a design choice worth
  remembering; not each new name. When the request leaves a public name or shape open, choose it as `writing-to-the-owner` §3 says, record it
  under Decisions with the alternatives you weighed, and the owner judges it in review; no list
  of public modules decides it. A fix that makes code do what its docs say is not a public
  change.
- **Compatibility starts at `0.1.0`.** Release candidates owe none: until `v0.1.0` is tagged
  (`git fetch --tags`, then `git rev-parse -q --verify refs/tags/v0.1.0`) or published on PyPI
  (`uv run --with pip pip index versions deephaven-massive`), nothing is kept, aliased, or shimmed. From `0.1.0` the
  library has users, and a breaking change needs the owner's permission. A change breaks when
  code written to the last release's documented contract stops working or gets a different
  documented result. When the owner requests a public change, say whether it breaks existing
  code and, if so, the next version (`docs/dev/releasing.md`, Versions); unless the request
  already accepts the break, ask before making it, including whether the old form keeps working.
- **Public argument names are Massive's**, so users can follow Massive's docs. A name that
  shadows a builtin or causes a real user error gets a descriptive name mapped to the wire name
  (`ticker_type`, not `type_`).
- **One name per field.** A column for a field Massive also serves in another dataset takes
  the name that dataset already uses, so a user's query strings work across tables. The spec
  refresh's `field-spelling` gate stops on fields Massive describes alike; it can't see fields
  described in other words, so also compare every new or changed column, in a new dataset or an
  existing one, with those of datasets serving the same objects (`docs/dev/spec-refresh.md`). A match proven from Massive's
  documentation of both fields, their types, and units gets one name through an
  `overrides.SPELLINGS` entry; a suspected but unproven match keeps its names and goes to the
  owner for a verdict, with both fields' documentation quoted and a live test that settles it.
- **Massive owns its data and its rules; the library owns the plumbing.** Massive decides
  whether a ticker exists, a parameter value or combination is allowed, what a plan includes,
  and what the data is. The library owns everything between the user and Massive: building
  correct requests, connections, rate limits, retries, converting data faithfully into tables,
  and telling the user clearly what happened. So:
  - **The code never knows Massive's plans** (decision 114). Massive decides what each key
    may read, how late, and how fast, and changes its plans over time. The library and its
    tests learn these only from Massive's answers (an entitlement refusal, a rate-limit
    answer) and from settings the user gives, never from a plan's name, a probe for a plan,
    or a built-in table of what a plan includes. They work for whatever plans a user holds.
    A live test sends its requests and, on an entitlement refusal, skips and quotes Massive.
    User docs may still say which plans include a dataset, generated from Massive's spec so
    they follow Massive's changes.
  - **Massive's answer is the error.** The library reports Massive's refusal as Massive's: its
    status, its text, and the call that caused it. It never reinterprets or guesses.
  - **A ticker Massive has no data for gives no rows by default** (decision 104), because that
    is Massive's answer, not an error. `unavailable_symbols` governs an HTTP 404 and a crypto
    pair missing from Massive's crypto ticker list. An empty 200 is no rows under any setting.
  - **The library never sends a request only to check a ticker**, because whether a ticker
    exists is Massive's answer to give. Users check Massive's lists, such as
    `m.reference.tickers()`.
  - **The library takes only Massive's canonical ticker as a symbol** (decisions 58 and 104),
    because most of Massive's endpoints answer an unknown ticker with an empty 200
    (verification item 62), so a typo looks like a quiet day. In code, a non-canonical symbol
    raises `ValueError`. In a table, its row follows `noncanonical_symbols`. A form on
    Massive's ticker lists that the library refuses is a library defect.
  - **The library refuses any other value only** when it is the wrong Python type, when the
    library can't express it in a request, or when sending it would return wrong data with no
    error. Each such check says which, with its evidence. Beyond ticker spelling, the library
    never encodes Massive's data particulars.
  - **The library's own errors are its own:** a transport failure after retries, a closed
    session, a misuse of the API, or a library defect, each saying it is the library's.
- **Supply Massive's data; never repair it.** Users must see what Massive sent: convert
  faithfully (units, types, column names, canonical symbols), and never fill, correct, filter
  out, re-stamp, or replace a value to make vendor data look right. When it is wrong, record
  the evidence and draft a report to Massive for the owner. Vendor descriptions are
  documentation, not data: a clearly wrong one gets a reasoned `DESCRIPTION_FIXES` entry
  (decision 51).
- **A data table holds only what Massive sent, read whole.**
  - **A null in a data table is Massive's:** a null it sent, or a field it omitted.
  - **A row the library cannot read or derive whole is withheld:** a symbol or key it can't
    read, a value that doesn't fit its column, a number the column would round, and a line that
    isn't a row. A withheld row is never published with a null, a default, a rounded or
    re-stamped value, or a guess; it is reported with its cause, the symbol as Massive sent it,
    and its count.
  - **A column the dataset's docs define as the library's derivation is the library's:**
    `Date`, `TimestampEnd`, a day bar's 00:00. Where Massive sends its own value, it is never
    replaced.
  - **One bad symbol or row never fails the table.** Each cause has its own `dm.ErrorPolicy`
    setting, set for a session in `MassiveConfig(on_error=...)` and for one call with the
    dataset method's `on_error=`. The setting decides only whether the table fails (`RAISE`)
    and how loudly the log reports the problem (`LOG`, the default, at WARNING; `IGNORE` at
    DEBUG), never whether a row is published; a live table also lists the problem in its
    `m.streams()` `Error`. A refusal of part of what a call explicitly asked for (a futures
    exchange the plan lacks) defaults to `RAISE`, because asking for data the plan lacks is a
    bug in the caller. A new cause gets a new setting, never a hard-coded failure or a silent
    skip.
  - **Only a problem that affects every row fails the table whatever the setting:**
    authentication, a closed session, a plan without the dataset, a refusal of everything a call
    asked for, a read for which Massive sent rows inside the requested symbols and window and
    the library could read none of them whole (on REST judged per page, on flat files per file,
    on a stream by its dataset's run on the channel; rows outside the window count neither
    way), and a library defect.
- **Dates are ISO strings**: every calendar-date column, vendor dates included, stores
  `yyyy-MM-dd` text, never a `LocalDate` or a value computed on read; instants stay
  `Instant`s. Every dataset table is built by `_tables/columns.py` `dataset_table`, and the
  library owns the column names `Date` and `Timestamp*`.
- **No `None` mode sentinels.** Select a mode with an enum member; `None` means only absent.

## Code and tests

- **Layers**, lowest first, so each part can be tested alone: wire (`_massive`, all Massive
  access, free of Deephaven), vocabulary, `_datasets`, `_tables`, `_ingest`, API. A module
  imports only its own layer or lower, under `TYPE_CHECKING` too; pass objects down. Outside
  `_massive` and its tests (`tests/massive/`), code uses only `_massive.__all__`. The
  `tests/test_layout.py` docstring has the map.
- **A module holds one subject a reader can hold in mind**, named in the first line of its
  docstring. New code goes where its subject is, or in a new module, never in a neighbor whose
  summary would grow "…, and X". Cohesion is the test, not length. An exception lives where it
  is raised, or in `_errors.py` when several modules raise it. Every name says what the thing
  is or does, in words a newcomer knows (no `base`, `core`, `common`, `helpers`, or `util`).
- **Log every decision and failure with its values**, so a failing run's log is enough to fix
  it (rules: the `_debug_log.py` docstring).
- **The library never opens a connection only to test whether one would work**: a second
  connection with the user's key can take their own stream (decision 97).
- **No decision or planning references** in code, comments, docstrings, test names, error
  messages, CI step names, or config, since a reader may never see the plan: state the behavior
  or the reason. Only the live tests' `verifies` and `settles` markers cite numbers.
- **Tests mirror the package**, one leading underscore dropped per path component
  (`_massive/rest/client.py` → `tests/massive/rest/test_client.py`). `tests/scripts/` may mirror
  `scripts/` the same way, a `conftest.py` may live in any test folder, and helper packages
  (fake servers in `tests/_fakes/`, live-test fixture logic in `tests/_live/`) sit outside the
  mirror. Coverage is 100% of statements and branches: delete a dead defensive branch instead
  of covering it.
- **Write code for what it does, never to get past a tool.** `if TYPE_CHECKING:` guards
  imports and nothing else. A `cast`, an `Any`, a `getattr` or string lookup, a branch hidden
  from the type checker, or any construct whose only purpose is to make ruff, mypy, or coverage
  pass is bent code: change the design so the tools see the true API, or raise the rule with
  the owner. `assert` states an internal fact the code has just established (`assert x is not
  None` after the check that guarantees it), for readers and the type checker; it never
  validates input or guards anything at run time, since `python -O` removes it. Prefer it to a
  `cast`, which asserts the same fact without checking it.
- **Every choice over a closed set handles each case by name** (an enum's members, a union of
  types, the forms of `symbols`, a fixed set of values), so a new member or type fails loudly in
  a test instead of corrupting data or dropping rows. A `match` or `if` chain ends in
  `assert_never` where mypy can check it, or else raises `BrokenGuaranteeError`; a lookup table
  over an enum has a test that every member is in it. No `else` or default case, `dict.get`
  default, type-filtering comprehension, or silent `except` absorbs a value nobody planned for;
  an intended fallback (a documented `OnError` behavior, an optional field Massive may omit)
  says why in a comment.
- **Lint size and complexity limits flag smells**: fix the design. A suppression at a site
  needs the owner's acceptance first, asked with its reason; never raise a global limit to make
  code pass. Too-many-arguments is off because
  keyword-only optional parameters are the API style; positional parameters are capped at three.
  Every suppression (`# noqa`, `# type: ignore[code]`, `# pragma: no cover`) has its reason on
  its line. One case is accepted in advance: a script that starts the Deephaven server imports
  what loads Deephaven after the start, each line with `# noqa: E402 - needs the server above`.
- **The checks are the owner's rules**: `tests/test_layout.py`, the ruff, mypy, and coverage
  settings, the hooks, `check.sh`, CI, and the PR-description check (`scripts/check_pr_body.py`). A stricter check and an entry under an existing rule whose stated purpose
  covers the case (a helper package, an approved exception) are ordinary edits, named under
  Check before merging; an entry, exception, or rule edit made only so code passes, new or
  already on `main`, is bent code: fix the code, or ask the owner which to change, and never
  write the exception unasked. Loosening a rule
  (removing a check, raising a limit, turning a rule off project-wide) needs the owner first.
- **Dependencies**: a new dependency or raised floor a change needs goes in that change's PR,
  named under Check before merging; only a dependency-only change is its own `chore(deps):` PR.
  Python dependencies and tools are `>=` floors; GitHub Actions and pre-commit hooks stay pinned
  to a commit, updated by Dependabot. `uv.lock` is not committed and `check.sh` upgrades on every
  run, so a new ruff or mypy release can flag code no one touched and block every branch: check
  open PRs for a fix in flight and stack on it, or else fix it in its own PR and stack on that.

## Workflow

- **Design before code, for every change.** Write down what will change and why before
  writing it, sized to the change: a one-line fix gets a few lines (the cause, the fix, the
  test that catches it); a feature, a new module, or a change across many files gets a short
  design (the calls a user types, what is refused and why, the settings, which files change).
  The costly failures of one week were code written before its design settled: a public shape
  the owner first met as a 184-file diff, and a PR whose rules changed twice after it opened,
  each change paid for in merges and reviews. So for anything beyond a small fix, draft the
  design with the most capable model available (Fable) and have a second Fable instance attack
  it as an adversary before any code: a flaw found on paper costs a paragraph, and the same
  flaw found in the diff costs the diff. Where the design is ambiguous, or the design and its
  critic can't agree on a choice, stop and ask (the owner, or the coordinator for a subagent)
  with the options and a recommendation, instead of picking one and building; keep working
  only on what the answer can't change. A public change, and any dropped protection, goes to
  the owner in that design form before any code is written (`writing-to-the-owner` §3). Every
  other design the critic accepts is yours to settle: record it in the PR under Decisions and
  start.
- **One PR per change that can merge on its own.** A PR holds one change with everything it
  needs to be correct: the code, its tests, and its docs together (a new column lands with its
  docs page and tests in one PR). A mechanical step the change needs (a rename, a file move, a
  regeneration, one rule applied everywhere) is right by itself, so it goes in its own PR and
  merges first: rename the setting across 80 files in one PR, then change what it covers in
  the next, so the rename never waits on a design question and the behavior PR stays small.
  Never split one behavior into parts that are each wrong until the others land, such as the
  REST half of a new rule in one PR and the WebSocket half in another: neither can merge alone.
  Unrelated fixes found on the way go in their own PR or an issue, however small.
- Branch `<type>/<topic>` from `origin/main` after `git fetch`, or from the PR below in a
  stack. Commits and PR titles follow Conventional Commits and the Title section of
  `docs/dev/pull-requests.md`.
- **Run `pre-push-review` before opening a PR and before every push to one**; only a typo or a
  one-word doc fix skips it. It holds the push procedure: push in batches (each push runs paid
  CI and review bots), answer every review comment, and keep the description current. Every CI
  job must pass, the free-threaded 3.14t job too.
- **Branch from another PR only when yours needs its code.** Before starting, check the open
  PRs that change your files (`gh pr list`, `gh pr diff <n> --name-only`). If your change calls
  a function or uses a type that an open PR adds, branch from that PR's branch, open with
  `gh pr create --base <its branch>`, link with `gh stack link <PRs bottom to top>`, and name
  the merge order in your PR body (`plan-parallel-prs` §3 has more). If you only touch the same
  files, branch from `main` as usual: basing on the other PR to avoid a merge conflict makes
  yours wait on its review and inherit each merge of `main` it makes, which costs more than
  resolving the conflict once when the second PR lands. Don't start a wide change (many files,
  or a rewrite of a module) while an open PR is changing the same files; wait for it to land,
  or join it. A PR that moves or renames files merges before the PRs it would conflict with,
  not after them, so no other PR merges `main` to follow the move.
- **Stop and re-plan a PR that has drifted.** Three signs: the owner changed the design after
  the PR opened; the diff grew past the request (a three-file fix now touches twenty); or the
  PR is merging `main` a second time. Each means the PR carries more than one change, or waits
  on something it can't settle, and every further push pays for all of it. Land what is settled
  now, as a PR that is correct by itself, and move the rest to an issue with what you learned.
  One merge of `main` to resolve a conflict is normal; the second is the sign.
- Merge `main` or your base branch only to resolve a merge conflict, or after an open PR that
  renames a public name lands (a text merge can't catch the old name in your new code; rerun the
  tests). Read both sides of each merge conflict, never take one wholesale, regenerate a
  generated file instead of merging it, and say in the PR body how you resolved it.
- **A decision record is the evidence behind a rule, not the rule.** Rules live here, in the
  user docs and docstrings, and in the `docs/dev/` page on their subject; a record holds the
  owner's ruling that set one, and the rule links it (`decision N`). A verification item is a
  fact about Massive or Deephaven that only a live read settles and whose answer changes the
  code.
  Each decision is one file, `docs/dev/decisions/NNNN-slug.md`, and each verification item one
  file in `docs/dev/verification-items/`; the folder README gives the format, and
  `writing-a-record` says how to write one. The slug says when
  a reader needs the entry, in the words they would search for. A new entry takes the number the
  coordinator reserved, or the next after the highest on `main` and in every open PR. Never
  rename or delete an entry, except to renumber one of two that landed with the same number: a
  changed decision is a new file, and the old one's `Status:` line names it; settling an item
  changes its `Status:` line. Before citing decisions, asking the owner, or removing or
  weakening anything, list them (`grep -m1 '^Status:' docs/dev/decisions/0*.md`) and read
  every entry whose name touches the subject. An owner ruling that sets, changes, or reverses
  a rule or contract becomes a decision file in the next PR on the subject (the coordinator's
  own `docs:` PR when none is open): a ruling that lives only in chat is broken again by the
  next agent, who never saw it. The file states the ruling in plain words, with no quotation:
  the question asked, the ruling, its reason, its date, and where it was decided. Read its
  wording back to the owner before writing it (a subagent, through the agent that started
  it), and show its text in the PR, so a stretched meaning is caught before merge. A PR that
  changes a rule in this file or a skill shows the question and the owner's dated answer for
  it and, when it rewrites or removes a rule, says under Check before merging which decisions
  it touches (`Decisions touched: 58, 66` or `none`), each given a new `Status:` line;
  `scripts/check_pr_body.py` fails without the line and the answer.
- Skills: `writing-tests` for any test, `writing-docstrings` for text in code,
  `writing-a-pr-description` for PR descriptions, issue bodies, and reports,
  `writing-a-record` for a decision or verification item, and `writing-user-docs` for a user
  page or any change to what one describes.

## Deephaven invariants

Read these before touching threads, locks, listeners, liveness, or live tables. Each one
prevents a deadlock, a leak, or a JVM crash.

- **Lock order**: the update-graph (UG) lock, then a connection's flush lock, then a stream's
  publish lock, then `registry_lock`, then the live set's lock. A connection group's lock comes
  only after the UG lock and is never held with `registry_lock`.
- **`registry_lock` is a leaf**: under it, no Deephaven call, network call, blocking wait, or
  table operation that may take the UG lock (`where_in`, `last_by`, `Table.snapshot()`,
  `deephaven.arrow.to_table`).
- **Enter the execution context on every library thread**, before any Deephaven call, through a
  fresh wrapper (`with entered(exec_ctx):`): a shared wrapper entered on two threads loses one
  thread's context.
- **Release refreshing tables under the UG lock**, every liveness scope through `release_scope`,
  never a bare `scope.release()`, and never under a library lock. Where the UG lock is
  off-limits (a listener, a shutdown callback, a publish lock), use `SessionCore.release_later`.
- **Never block in a table listener.** It runs on a UG thread holding the UG lock: compute the
  diff, hand it to a worker, return.
- **The console holds the UG lock** for every command, so every call must work there: only UG
  threads are refused, and a call holding the lock never waits for an update cycle or on a
  thread that may need the lock.
- **Every live result has its own liveness scope**, created on the caller thread; a refreshing
  table made outside one lives as long as the console session.
- **Streams tear down from the blink table's `on_shutdown_callback`**, which only updates the
  registry, queues a failure's pin release on the reaper, and calls `call_soon_threadsafe`; it
  never sends and never calls Java.
- **Fail a blink table only through `BlinkPublisher`** from `blink_publisher`
  (`_tables/publisher.py`), never `table_publisher`: `publish_failure` pins the table until the
  failure is delivered, because a table destroyed first can get it twice and exit the process.
- **The library never uses input tables**: every table it writes is a `blink_publisher` blink
  table or a shape of one, and a keyed one is `last_by` on its keys.
- **Create a table under the UG shared lock**, for the creation only, as `blink_publisher` does:
  a table registered while a cycle runs and changed in that same cycle is fatal.
- **Shared state has one owner or one lock.** Only a connection's client thread touches its
  subscription sets (others use `loop.call_soon_threadsafe` with plain data), and no code relies
  on the atomicity of `dict`, `set`, or `list` operations; locking must work without the GIL.
