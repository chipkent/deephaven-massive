# 6. Lifecycle is automatic plus explicit

Date: 2026-10-23

Status: accepted

A garbage-collected result releases its scope through a weakref and the reaper; `m.release(table)`
and `m.close()` give deterministic control. Topic reference counts follow the blink table's real
shutdown, so derived tables keep a stream alive. Queries have no `stop()`.
