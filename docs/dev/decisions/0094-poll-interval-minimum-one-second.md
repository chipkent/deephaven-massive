# 94. `poll()` refuses an interval shorter than 1 second

Date: 2026-11-03

Status: accepted

A shorter interval is almost always a typo that would send requests back to back; a stream serves
anything faster.
