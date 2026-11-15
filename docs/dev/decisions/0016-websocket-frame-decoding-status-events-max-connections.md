# 16. WebSocket frames are decoded by the client

Date: 2026-10-26

Status: partly superseded: its `max_connections` part by [97](0097-websocket-connection-taken-max-connections-fail-or-reconnect.md)

`WsClient` decodes each frame once against `ws.json` into status events, rows, rejects, and drift. A
`max_connections` status shows `WAITING_SLOT`, and streams stay alive. *Its `max_connections` part
is superseded by 97.*
