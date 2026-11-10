"""The recency labels Massive puts on REST responses, and the refusal a delayed one causes.

Massive says whether a REST response's data is real-time or delayed in two places: the
envelope's `status`, `DELAYED` on a delayed plan's response, and the `timeframe` field of a
snapshot or "last" row and of its nested `last_trade`, `last_quote`, and `last_minute`
objects, `DELAYED` or `REAL-TIME`. `read_labels` reads both, in that order, into one
`RecencyLabels`; `underlying_asset.timeframe` is never read, since Massive's option-chain
sample shows the underlying delayed beside a real-time contract.

Two callers use them. The WebSocket host decision (`host_decision.py`) reads its snapshot's
labels to tell whether a subscription's plan is delayed, and the REST transport (`rest.py`)
reads every page of a `fetch()` or `poll()` of a subscription's current data, so neither
serves delayed data to a user who did not ask for it in `MassiveConfig.delayed_subscriptions`.
`delayed_refusal` builds the `DelayedDataError` both raise, with the evidence and the exact
configuration that asks for the delayed data. Pure functions: nothing here logs, waits, or
calls Deephaven.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._config import delayed_fix
from deephaven_massive._errors import EntitlementError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._enums import Subscription

__all__ = [
    "TIMEFRAME_FIELDS",
    "DelayedDataError",
    "RecencyLabels",
    "delayed_refusal",
    "read_labels",
    "shows_real_time",
]

DELAYED: Final = "DELAYED"
"""The label of delayed data, in the envelope `status` and in a `timeframe` field."""

REAL_TIME: Final = "REAL-TIME"
"""The `timeframe` label of real-time data."""

TIMEFRAME_FIELDS: Final = (
    "timeframe",
    "last_trade.timeframe",
    "last_quote.timeframe",
    "last_minute.timeframe",
)
"""The dotted row fields, in reading order, whose `DELAYED` or `REAL-TIME` value states the
recency of a row's data: an index snapshot's own `timeframe`, and the nested objects of the
stock, option, and futures snapshots (futures carry them in the OpenAPI spec only)."""


@dataclass(frozen=True, slots=True)
class RecencyLabels:
    """What a response's labels say about its data's recency.

    Attributes:
        delayed: The evidence of delayed data, such as `"status=DELAYED"` or
            `"last_trade.timeframe=DELAYED"`; None when nothing is labelled delayed.
        real_time: The first field labelled `REAL-TIME`, such as `"timeframe=REAL-TIME"`, or
            None.
        unknown: The first field and value that is neither label, or None.
    """

    delayed: str | None
    real_time: str | None
    unknown: tuple[str, object] | None


def _value(row: Mapping[str, object], dotted: str) -> object:
    """Read a dotted field of a raw JSON row, such as `last_trade.timeframe`.

    Args:
        row: One decoded row.
        dotted: The field's path.

    Returns:
        The value, or None when a step is missing or not an object.
    """
    value: object = row
    for step in dotted.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(step)
    return value


def read_labels(status: str, rows: Sequence[Mapping[str, object]]) -> RecencyLabels:
    """Read a response's recency labels: its envelope status, then each row's timeframes.

    Args:
        status: The envelope's `status` (`""` when the body has none).
        rows: The response's raw JSON rows, every key kept.

    Returns:
        The labels. An envelope `DELAYED` is the delayed evidence even when a row says
        `REAL-TIME`; otherwise the first `DELAYED` field of the first row that has one is.
    """
    delayed = f"status={DELAYED}" if status == DELAYED else None
    real_time: str | None = None
    unknown: tuple[str, object] | None = None
    for row in rows:
        for field in TIMEFRAME_FIELDS:
            value = _value(row, field)
            if value is None:
                continue
            if value == DELAYED:
                delayed = delayed or f"{field}={DELAYED}"
            elif value == REAL_TIME:
                real_time = real_time or f"{field}={REAL_TIME}"
            else:
                unknown = unknown or (field, value)
    return RecencyLabels(delayed, real_time, unknown)


def shows_real_time(evidence: str) -> bool:
    """Return whether a host decision's evidence is a snapshot label that says real-time.

    The host decision keeps `RecencyLabels.real_time` as its evidence when the snapshot labels
    the class's data real-time, which proves the key's plan is real-time. Any other evidence
    (no label, an undocumented label, a pinned host, the configuration) proves nothing, so a
    refusal's message keeps suggesting the delayed feed for it.

    Args:
        evidence: The decision's evidence, such as `"last_trade.timeframe=REAL-TIME"`.

    Returns:
        True when the evidence is one of `TIMEFRAME_FIELDS` labelled `REAL-TIME`.
    """
    return any(evidence == f"{field}={REAL_TIME}" for field in TIMEFRAME_FIELDS)


class DelayedDataError(EntitlementError):
    """Massive labels a subscription's data delayed, and the session did not ask for delayed data.

    Internal, raised as the public `EntitlementError` it is: its own class lets a caller tell
    this refusal from Massive's other refusals, as the stream of a real-time-only dataset does
    (`host_decision.require_real_time`), whose plan then does not include it at all.
    """


def delayed_refusal(
    subscription: Subscription, what: str, evidence: str, *, streams: bool
) -> DelayedDataError:
    """Return the error of delayed data for a subscription not listed as delayed.

    Args:
        subscription: The subscription, not in `MassiveConfig.delayed_subscriptions`.
        what: What would be delayed, such as `"options streams"` or
            `"stocks.snapshots data"`.
        evidence: The label that shows it (`RecencyLabels.delayed`).
        streams: True for a stream, whose fix is Massive's delayed feed; False for a REST
            read, whose fix accepts the plan's delayed data and also moves the subscription's
            streams to the delayed feed.

    Returns:
        The error, for example: `options streams would be delayed (Massive reports
        last_trade.timeframe=DELAYED), and dm.Subscription.OPTIONS is not in
        MassiveConfig.delayed_subscriptions, so the library serves only real-time options
        data. To use Massive's delayed options feed:
        dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})`.
    """
    remedy = (
        f"To use Massive's delayed {subscription} feed"
        if streams
        else f"To accept delayed {subscription} data (its streams then use Massive's delayed "
        "feed too)"
    )
    return DelayedDataError(
        f"{what} would be delayed (Massive reports {evidence}), and "
        f"dm.Subscription.{subscription.name} is not in MassiveConfig.delayed_subscriptions, "
        f"so the library serves only real-time {subscription} data. {remedy}: "
        f"{delayed_fix(subscription)}"
    )
