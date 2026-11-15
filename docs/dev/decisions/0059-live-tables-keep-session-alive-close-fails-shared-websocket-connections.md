# 59. Live tables keep their session running, and `close()` fails them

Date: 2026-10-28

Status: accepted

A live table keeps alive what it needs and nothing else does; `m.close()` fails every live table
with a clear message rather than leave tables that look live. Sessions with one key and equal
connection settings share WebSocket connections, so they do not knock each other off a plan's one
slot.
