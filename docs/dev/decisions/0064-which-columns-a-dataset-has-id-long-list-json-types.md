# 64. Columns join, merge, and show only what a dataset fills

Date: 2026-10-29

Status: accepted

A dataset keeps the columns of its kind that some source of its own can fill, never depending on a
call's arguments. Every numeric id is a `long`, a list of text a `String[]`, and other nested values
JSON text. The unified snapshot is `snapshots` everywhere.
