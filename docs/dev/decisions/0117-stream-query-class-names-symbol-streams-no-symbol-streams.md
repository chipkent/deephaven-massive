# 117. A symbol stream's query is a StreamQuery; a stream without symbols is named for that

Date: 2026-11-15

Status: accepted

## Question

Since decision 96, `m.stocks.trades("AAPL")`, `m.stocks.luld("AAPL")`, and every other stream
whose rows name their symbol returned a `SymbolStreamQuery`, with `last_by()`, while
`m.options.admin_messages()`, the one stream without a `Symbol` column, returned a
`StreamQuery`, without it. The name users met most often carried a qualifier, and neither name
said why one class had `last_by()` and the other didn't. What should the two classes be called?

## Options

- Swap the names: the symbol streams return `StreamQuery`, with `last_by()`, and the stream
  without symbols returns a class named for what it lacks, `NoSymbolStreamQuery`.
- Rename only the symbol-stream class, to `LastByStreamQuery`: the name says what it adds, and
  the common class keeps a qualifier.
- Keep the names: no change, and the common class keeps the unclear qualifier.

## Ruling

Swap the names. Every stream whose rows name their symbol returns `StreamQuery`, which has
`last_by()`; a stream whose rows name no symbol returns `NoSymbolStreamQuery`, which doesn't.
`FetchQuery`, `HistoryQuery`, and `StreamHistoryQuery` keep their names, and
`StreamHistoryQuery` stays a `StreamQuery`. The owner ruled on 2026-11-15 in the coordinating
chat session, choosing the swap so the code reads clearly; recorded in the pull request that
made the rename.

## Reason

The class most streams return should have the plain name, and the rare one should say why it
lacks a method.

## Consequences

Code that imported `SymbolStreamQuery` imports `StreamQuery` instead, and code that annotated
the admin messages with `StreamQuery` uses `NoSymbolStreamQuery`. A helper that takes any
stream annotates `StreamQuery | NoSymbolStreamQuery`. No alias is kept, since no compatibility
is owed before 0.1.0.

## Rule

The `deephaven_massive.queries` package docstring.
