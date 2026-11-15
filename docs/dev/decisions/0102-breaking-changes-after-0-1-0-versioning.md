# 102. From 0.1.0, a breaking change needs the owner's permission and a new version

Date: 2026-11-11

Status: accepted

## Question

When does the library owe users compatibility, and what does a breaking change require?

## Options

None recorded: one path was proposed.

## Ruling

Before `0.1.0` nothing is kept or aliased. From `0.1.0`, when the owner asks for a public
change, the agent says whether it breaks code written to the last release's documented behavior
and which version number comes next, and asks before making it. In `0.x` a break raises the
minor version. The owner ruled on 2026-11-11 that the API is not broken without their
permission, and that they are told when a change breaks; recorded in PR #580.

## Reason

From the first release the library has users.

## Consequences

In `0.x` a break raises the minor version and anything else the patch; from `1.0.0`, standard
Semantic Versioning.

## Rule

`AGENTS.md`, "Compatibility starts at `0.1.0`"; `docs/dev/releasing.md`, "Which number to
raise".
