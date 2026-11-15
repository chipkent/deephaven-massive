# 76. The debug log says what it needs to

Date: 2026-10-30

Status: accepted

The debug log says what it needs to: pauses, permit waits, close reasons, connection uptime, the git
commit and platform; `DEEPHAVEN_MASSIVE_LOG_FILE` alone turns it on; the library never rotates the
file.
