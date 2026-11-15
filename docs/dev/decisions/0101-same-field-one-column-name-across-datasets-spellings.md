# 101. One Massive field has one column name in every dataset

Date: 2026-11-11

Status: accepted

## Question

Massive spells one field two ways on two endpoints. Does each table keep its endpoint's spelling,
or does the field take one column name everywhere?

## Options

1. Each table keeps its endpoint's spelling, with no cross-table synonyms (decision 49).
2. One field takes one of Massive's own spellings in every dataset.

## Ruling

When Massive spells one field two ways on two endpoints, every table uses one of Massive's
spellings, so a user's query strings work across tables. A pair is renamed only when Massive's
docs prove the fields, types, and units match; a doubtful pair keeps both names until a live
test settles it and the owner decides. The owner ruled on 2026-11-11 for one name where
possible, and a verdict of theirs on a suspected pair; recorded in PR #581. This replaces
decision 49. Decision 108 (2026-11-13, PR #611) applied it to three pairs: the option chain's
day change takes the snapshots' `TodaysChange` and `TodaysChangePerc`; the forex conversion's
last quote takes the forex snapshot's `LastQuoteBidPrice`, `LastQuoteAskPrice`,
`LastQuoteExchange`, and `LastQuoteParticipantTimestamp`; and the v2 `updated` keeps `Updated`.
The owner kept these on 2026-11-16.

## Reason

One spelling, taken from Massive, beats aliases: a user learns one name and finds it in
Massive's docs.

## Consequences

Object prefixes stay where dropping them would merge different fields. The partners keep their
own spellings. A new dataset that brings a second name for a known field stops the refresh at
its `field-spelling` gate.

## Rule

`AGENTS.md`, "One name per field"; `docs/dev/spec-refresh.md`, `overrides.SPELLINGS`.
