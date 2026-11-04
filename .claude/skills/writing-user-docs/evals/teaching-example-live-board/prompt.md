---
tags: [writing-user-docs]
max_turns: 60
timeout_seconds: 1800
allowed_tools: [Read, Glob, Grep, Skill, Write, Edit, Bash, Agent]
---

Write example 8 of the README's teaching sequence, 'Build a live board against yesterday's close'. Its one idea: a live table joins to a static one, and on Stocks Starter the live data comes 15 minutes delayed. Examples 1 to 7 already ran in the same console, so `m` (a MassiveSession) exists and `from deephaven import agg` is imported. Save the example to example-8.md.
