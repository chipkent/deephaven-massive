# deephaven-massive: Design Specification

This document is the approved design for the clean rework of `deephaven-massive`, a Python
library that exposes Massive.com (formerly Polygon.io) market data as live and static
[Deephaven](https://deephaven.io) tables. It is the source of truth for every implementation
planned PR in `PLAN.md`. It changes only by owner decision.

The design was checked against Massive's OpenAPI spec (`https://api.massive.com/openapi`,
147 paths), its WebSocket spec (`https://api.massive.com/specs/websocket.json`, 37 channels),
`llms.txt` and the REST pages it links, and the flat-file dataset and overview pages, all as
of 2026-10-26;
`deephaven-core` 42.7 (Python package; Java behaviour from the `deephaven-core` `main` branch);
and the massive.com documentation as of 2026-10-23. Facts that could not be verified from those
sources are marked **UNVERIFIED**, carry the conservative handling that applies, and are
collected in [section 23](design/23-open-verification-items.md).

## Sections

Each top-level section is its own file under [`design/`](design/). Read only the file for
your topic. A reference to "DESIGN.md § N" means section N below.

| § | Section | What it decides |
|---|---|---|
| 1 | [Purpose and scope](design/01-purpose-and-scope.md) | What the library turns into tables (WebSocket, REST, S3 flat files) and how it reaches Massive. |
| 2 | [Principles and terms](design/02-principles-and-terms.md) | The design principles every API follows and the terms the rest of the document uses. |
| 3 | [Exports](design/03-exports.md) | The exact names `deephaven_massive` exports and nothing else. |
| 4 | [Package layout](design/04-package-layout.md) | Where each module lives, the mirrored test layout, and the import layers. |
| 5 | [`MassiveConfig`](design/05-massiveconfig.md) | The `MassiveConfig` fields, their defaults, and how credentials and settings files are read. |
| 6 | [`MassiveSession` and lifecycle](design/06-massivesession-and-lifecycle.md) | The `MassiveSession` object, its shutdown, and how a live table is kept alive and released. |
| 7 | [Query classes and verbs](design/07-query-classes-and-verbs.md) | The query classes, their verbs (`fetch`, `poll`, `history`, `last_by`, ...), column descriptions, and argument validation. |
| 8 | [Namespace tree](design/08-namespace-tree.md) | Every dataset method under each namespace (`m.stocks`, `m.options`, ...) and the excluded endpoints. |
| 9 | [Filters](design/09-filters.md) | The typed parameters and the validated `filters` mapping that narrow a request. |
| 10 | [Symbols and `ALL`](design/10-symbols-and-all.md) | How symbols are accepted and checked: strings, lists, live tables, and `dm.ALL`. |
| 11 | [Enums and types](design/11-enums-and-types.md) | The Python floor and the public enums and type aliases. |
| 12 | [Timestamp units and day boundaries](design/12-timestamp-units-and-day-boundaries.md) | The unit of every timestamp field and how dates map to trading-day boundaries. |
| 13 | [Host resolution and connection protocol](design/13-host-resolution-and-connection-protocol.md) | Which host serves each feed, and the connection, subscription, and REST protocols. |
| 14 | [Publishers, fan-out, and failure semantics](design/14-publishers-fan-out-and-failure-semantics.md) | How streams are shared between tables, and how failures reach them. |
| 15 | [Options contract cap](design/15-options-contract-cap.md) | How the per-connection options contract limit is counted and enforced. |
| 16 | [`HistorySource.AUTO`](design/16-historysourceauto.md) | How `HistorySource.AUTO` chooses between S3 flat files and REST. |
| 17 | [Diagnostics](design/17-diagnostics.md) | The diagnostics tables (`m.streams()`, `m.connections()`, ...) and the debug log. |
| 18 | [Concurrency model](design/18-concurrency-model.md) | The library's threads and locks, and the rules that keep them free of deadlocks without the GIL. |
| 19 | [Wire clients and the spec snapshot](design/19-wire-clients-and-the-spec-snapshot.md) | The library's own REST, WebSocket, and flat-file clients, the generated schemas, and drift handling. |
| 20 | [Quality gates](design/20-quality-gates.md) | The checks every change must pass: tooling, coverage, CI jobs, and gates. |
| 21 | [Examples](design/21-examples.md) | Worked examples, each run as a test. |
| 22 | [Decisions](design/22-decisions.md) | The numbered owner decisions behind the design, with their rationale. |
| 23 | [Open verification items](design/23-open-verification-items.md) | The facts not yet verified against Massive, their conservative handling, and the live tests that retire them. |
