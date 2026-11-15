# 24. No client object is shared unless it is proven thread-safe

Date: 2026-10-26

Status: accepted

The REST client is shared by the session's threads; each WebSocket client is confined to its loop
thread.
