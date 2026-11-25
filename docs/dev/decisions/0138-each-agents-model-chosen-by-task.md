# 138. Each agent's model is chosen by the task

Date: 2026-11-25

Status: accepted

## Question

Agents defaulted to the most capable model, Fable, for every task, including builds, test fixes, lookups and review replies, though only designs and their critiques were assigned to it. Which model should each kind of agent work use?

## Options

1. Leave it to each coordinator, which in practice meant Fable everywhere.
2. A written rule matching the model to the hardest judgment the task needs.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-25. Fable designs, attacks a design or a finished diff as its critic, and judges concurrency or data-integrity risk. Opus builds from a settled design and writes most code and tests. Sonnet does lookups and inventories, applies given edits, makes mechanical refactors, watches CI, and answers review threads. A mixed task lets the cheaper model gather while Fable judges only the hard part, and every Agent call names its model.

## Reason

Cost and speed: the work that didn't need Fable was paying for it.

## Consequences

Coordinators name a model in every Agent call. An unnamed call inherits the coordinator's model.

## Rule

`.claude/skills/plan-parallel-prs/SKILL.md` §7, "Keep cost down", and `AGENTS.md`, "Keep context small".
