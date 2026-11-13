# 91. `last_by()` on a live symbol table shows a row by update cycle, not by receive time

Date: 2026-11-02

Status: accepted

`last_by()` on a live symbol table shows a row by update cycle, not by receive time, so a quiet
symbol's first row is not hidden until its next tick.
