# 68. Long downloads read in the background with `wait=False`

Date: 2026-10-29

Status: partly superseded: its opt-in by [100](0100-fetch-history-default-wait-false-background-read-errors.md)

`fetch()` and `history()` return at once a refreshing, empty table that receives the complete result
in one update. No `Future`, because its `result()` could not wait inside a console command. *Its
opt-in is superseded by 100: reading in the background is the default.*
