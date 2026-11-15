# 84. `history()` and `fetch()` follow a live symbol table

Date: 2026-11-02

Status: partly superseded: its per-symbol `limit` by [87](0087-limit-total-row-cap-per-request.md)

An added symbol is read over the same fixed range and lands whole in one cycle; a removed one
leaves. `reread=dm.Reread.ALWAYS` is the default; `m.symbol_reads()` shows each read. *Its
per-symbol `limit` is superseded by 87.*
