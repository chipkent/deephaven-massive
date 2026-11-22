# 114. The code never knows Massive's plans; it learns only from Massive's answers

Date: 2026-11-15

Status: partly superseded: where user docs may name plans, by [126](0126-readme-names-each-example-plan-checked-against-massive-spec.md)

## Question

May the library or its tests know what each Massive plan includes, such as a tier's request rate
or delay?

## Options

1. A built-in table of plans, or a probe for the user's plan.
2. Learn only from Massive's answers and the user's settings.

## Ruling

No table of plan tiers, and no probe of the user's plan. The code learns what a key may read,
how late, and how fast only from Massive's refusals and rate-limit answers and from the user's
settings. A live test sends its requests and skips when Massive refuses one. User docs may
describe Massive's offerings, written to be easy to update. The owner ruled on 2026-10-29,
2026-11-11, and 2026-11-15; recorded in PR #669, and the owner confirmed this wording on
2026-11-16.

## Reason

Massive changes plans and keeps old users on legacy ones, so built-in knowledge is brittle.

## Consequences

A user on any plan gets Massive's own refusal for what the plan lacks, and nothing predicts it.
A live run that a plan probe would have stopped runs every test that can run.

## Rule

`AGENTS.md`, "The code never knows Massive's plans".
