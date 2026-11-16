# 95. The datasets list calls its column of query methods Methods

Date: 2026-11-05

Status: accepted

## Question

What does `dm.datasets()` call its column of query methods?

## Options

None recorded: one path was proposed.

## Ruling

The table `dm.datasets()` returns has a `Methods` column listing `fetch`, `history`, and the
stream methods each dataset offers, and the dataset pages use the same word. Decided in PR #517
on 2026-11-05; no owner words are on record.

## Reason

The docstrings already call these methods.

## Consequences

None recorded.

## Rule

The `dm.datasets()` docstring.
