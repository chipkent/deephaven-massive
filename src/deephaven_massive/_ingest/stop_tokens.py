"""A background read's stop token, taken back from the tables layer as the wire's `Cancel`.

`SessionClients.read_stop` makes every stop token a `_massive.Cancel`, which the wire clients
follow. The tables layer cannot name the wire's client types, so the ports carry the token as
the `threading.Event` it derives from, and the ingest layer takes it back here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive import _massive
from deephaven_massive._errors import required_instance

if TYPE_CHECKING:
    import threading

__all__ = ["as_cancel"]


def as_cancel(stop: threading.Event | None) -> _massive.Cancel | None:
    """Return a background read's stop token as the `Cancel` it is.

    Args:
        stop: The token from `SessionClients.read_stop`, or None for a read only the session's
            close stops.

    Returns:
        `stop` itself.


    An event no `read_stop` made raises the `BrokenGuaranteeError` of `required_instance`.
    """
    return None if stop is None else required_instance(stop, _massive.Cancel, "a read's stop token")
