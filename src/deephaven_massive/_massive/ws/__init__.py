"""The WebSocket client of the Massive wire package.

It connects to one Massive WebSocket endpoint (`wss://<host>/<cluster>`), authenticates,
sends subscription frames, decodes every received frame against the cluster's generated
sources, and reconnects with backoff, all on its own thread. The library decides what to
subscribe and where the rows go; this package only speaks the protocol.

- `ws.client`: `WsClient`, its loop thread and lifecycle, and `WsState`.
- `ws.protocol`: control frames, frame decoding into `Status`, rows, and `Reject`s, and the
  classification of connection failures as fatal or retryable.
- `ws.backoff`: how long to wait before the next attempt.

It may import only `net` and `spec` from `_massive`. The rest of the library reaches these
names through `deephaven_massive._massive`, never through this package.
"""

from deephaven_massive._massive.ws.client import WsClient, WsState
from deephaven_massive._massive.ws.protocol import Reject, Status, StatusKind

__all__ = ["Reject", "Status", "StatusKind", "WsClient", "WsState"]
