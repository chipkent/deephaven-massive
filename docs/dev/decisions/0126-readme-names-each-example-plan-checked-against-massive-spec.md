# 126. The README names each example's plan, checked against Massive's spec by a test

Date: 2026-11-17

Status: accepted

## Question

Each README example said by hand which plan it needs ("Stocks Basic", "Stocks Starter or
higher", "the free Stocks Basic plan"), in about 30 places, some inside sentences. Decision 114
let user docs name plans only where generated from Massive's spec, and nothing checked these.
How should the README tell a reader which plan each example needs?

## Options

1. Keep the hand-written labels, with an exception to decision 114.
2. One generated table of the plans that include the tutorial's data, and no plan names in the
   examples.
3. A `**Plan:**` line under each example's heading, in Massive's plan names, that a test checks
   against the refreshed spec; no plan names in the examples' prose; plus one generated table
   of each plan's recency and history for the tutorial's data.

## Ruling

Option 3, decided by the owner in the coordinating chat session on 2026-11-17. A user doc may
name Massive's plans where the names are generated from Massive's spec or checked against it by
a test. The README's plans table is generated, and is never edited by hand.

## Reason

A reader needs to see at each example which plan runs it. A test against Massive's spec keeps
those lines true when Massive changes its plans, which hand-written labels were not.

## Consequences

A refresh that changes which plans include a dataset fails the test until the README's Plan
lines are updated. An example's prose names no plan; it refers to the plans table instead.

## Rule

AGENTS.md, "The code never knows Massive's plans" and Limits, "Regenerate generated and vendored
files"; docs/dev/user-docs.md, "Owner requirements".
