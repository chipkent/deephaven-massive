# 13. Python 3.11 and newer, with free-threaded 3.14t a required CI job

Date: 2026-10-23

Status: accepted

## Question

Which Python versions does the library support, and which free-threaded build does CI test?

## Options

None recorded: one path was proposed.

## Ruling

`requires-python >= 3.11`. CI runs the suite on free-threaded 3.14t, Python without the global
interpreter lock, and that job must pass; 3.13t is not tested. The owner ruled on 2026-10-24
that 3.13t be dropped, and again on 2026-10-31; recorded in PR #514.

## Reason

pyarrow, numpy, and pandas publish no wheels for 3.13t, so 3.14t is the only free-threaded
target. The 3.11 floor is where `StrEnum`, the enum type of the library's fixed choices, begins;
the owner confirmed this reason on 2026-11-16.

## Consequences

Every change must pass on 3.14t without the GIL, so no code relies on the GIL for its locking.

## Rule

`pyproject.toml` `requires-python`; `AGENTS.md`, "Shared state has one owner or one lock",
and the 3.14t command under Commands.
