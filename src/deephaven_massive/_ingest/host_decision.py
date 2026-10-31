"""Which WebSocket host serves an asset class and channel family: the user's choice, checked.

Massive serves each WebSocket cluster (`/stocks`, `/options`, ...) from several hosts:
`socket.massive.com`, the real-time feed; `delayed.massive.com`, the delayed feed (15 minutes
behind, 10 for futures); and `business.massive.com` for the Business plans' fair-market-value
(FMV) channels. The user chooses the feed per subscription with
`MassiveConfig.delayed_subscriptions`, and the choice is deterministic: a stream never moves
between feeds. This module decides the host once per (asset class, channel family), before the
first connection, and never by opening a connection to try:

- the FMV family is always `business.massive.com`, real-time only, so a subscription listed as
  delayed refuses it;
- a TICK host pinned in `MassiveConfig.ws_hosts` is used as is (the configuration has checked
  it against `delayed_subscriptions`);
- a listed subscription uses `delayed.massive.com`, and when its snapshot shows a real-time
  plan, one WARNING says that removing it from `delayed_subscriptions` gets real-time data;
- forex and crypto have one documented host, `socket.massive.com`, real-time on every plan, so
  no request is made;
- every other subscription uses the real-time feed. Its class's snapshot (`SNAPSHOT_PROBES`) is
  read first, and when Massive labels it delayed (`recency_labels.read_labels`: the envelope
  status, or a `timeframe` field), the verb raises `EntitlementError` naming the evidence and
  the exact fix rather than open a connection Massive would refuse. Without a delayed label
  nothing is guessed: a refused snapshot, an undocumented label, or no label at all streams
  from the real-time host, and a refusal from Massive fails the refused streams.

Recency follows the host (`recency_of`): the real-time and Business hosts serve real-time
data, the delayed host delayed data, and any other pinned host `UNKNOWN`, which is never shown
as real-time.

`decide` is pure apart from logging: the REST request itself is made by the caller's `fetch`
callable, which the REST transport (`_ingest/rest.py`) supplies, so every HTTP error has
already become its public exception there. A decision that raises is not cached, so the next
verb decides again and sees a plan upgraded meanwhile. `HostDecisionCache` holds a session's
decisions, single-flight, behind a leaf lock of its own; the REST transport fills it and writes
the `m.connections()` rows that record them. Nothing here starts a thread or calls Deephaven.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeVar

from deephaven_massive._config import BUSINESS_HOST, DELAYED_HOST, DELAYED_PREFIX, REALTIME_HOST
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Recency,
    Subscription,
    subscription_of,
)
from deephaven_massive._errors import EntitlementError
from deephaven_massive._ingest.recency_labels import RecencyLabels, delayed_refusal, read_labels
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._log_limits import log_once
from deephaven_massive._tables.transports import HostDecision

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

__all__ = [
    "SNAPSHOT_PROBES",
    "HostDecisionCache",
    "SnapshotAnswer",
    "SnapshotProbe",
    "decide",
    "recency_of",
    "succeeded_result",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SnapshotProbe:
    """The one REST request whose response shows an asset class's data recency.

    Attributes:
        path: The generated REST path template, such as `"/v3/snapshot"`.
        path_params: A value for each placeholder of the template.
        query: The query parameters, by wire name.
    """

    path: str
    path_params: Mapping[str, object]
    query: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class SnapshotAnswer:
    """What a host-decision request returned.

    Attributes:
        status: The envelope's `status` (`"OK"`, `"DELAYED"`, ...; `""` when absent).
        rows: The raw JSON rows, every key kept.
    """

    status: str
    rows: Sequence[Mapping[str, object]]


SNAPSHOT_PROBES: Final[Mapping[AssetClass, SnapshotProbe]] = {
    AssetClass.STOCKS: SnapshotProbe("/v3/snapshot", {}, {"ticker.any_of": ("SPY", "AAPL")}),
    AssetClass.OPTIONS: SnapshotProbe(
        "/v3/snapshot/options/{underlyingAsset}", {"underlyingAsset": "SPY"}, {"limit": 1}
    ),
    AssetClass.INDICES: SnapshotProbe("/v3/snapshot/indices", {}, {"ticker.any_of": ("I:SPX",)}),
    AssetClass.FUTURES: SnapshotProbe(
        "/futures/v1/snapshot", {}, {"product_code": "ES", "limit": 1}
    ),
}
"""The snapshot each class's TICK host is checked against: the unified snapshot of two liquid
stocks, one contract of the SPY option chain, the S&P 500 index, and one E-mini S&P contract."""


def recency_of(host: str) -> Recency:
    """Return the recency of a WebSocket host's data.

    Args:
        host: The hostname.

    Returns:
        `REALTIME` for the real-time and Business hosts, `DELAYED` for a delayed feed (a host
        whose name starts with `delayed.`), and `UNKNOWN` for any other (a pinned host the
        library does not know).
    """
    if host in (REALTIME_HOST, BUSINESS_HOST):
        return Recency.REALTIME
    if host.startswith(DELAYED_PREFIX):
        return Recency.DELAYED
    return Recency.UNKNOWN


def _ws_hosts_hint(asset_class: AssetClass) -> str:
    """Return the configuration that pins the class's host, for log messages.

    Args:
        asset_class: The class.

    Returns:
        For example `MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "<host>"})`.
    """
    return f'MassiveConfig(ws_hosts={{dm.AssetClass.{asset_class.name}: "<host>"}})'


def _real_time(asset_class: AssetClass, evidence: str) -> HostDecision:
    """Return the real-time host's decision.

    Args:
        asset_class: The class, for the log.
        evidence: What the snapshot showed, for the log and the `m.connections()` row.

    Returns:
        `socket.massive.com`, `REALTIME`, with the evidence.
    """
    logger.info("%s WebSocket host: %s (%s)", asset_class.value, REALTIME_HOST, evidence)
    return HostDecision(REALTIME_HOST, Recency.REALTIME, evidence)


def _unconfirmed_real_time(asset_class: AssetClass, evidence: str) -> HostDecision:
    """Return the real-time host's decision when the snapshot gave no usable answer, with a WARNING.

    Args:
        asset_class: The class.
        evidence: What the snapshot showed, for the log and the `m.connections()` row.

    Returns:
        `socket.massive.com`, `REALTIME`, with the evidence.
    """
    logger.warning(
        "%s WebSocket host: %s; streaming from the real-time feed %s, and a refusal from "
        "Massive fails the refused streams with its text (pin another host with %s)",
        asset_class.value,
        evidence,
        REALTIME_HOST,
        _ws_hosts_hint(asset_class),
    )
    return HostDecision(REALTIME_HOST, Recency.REALTIME, evidence)


def _from_labels(
    asset_class: AssetClass,
    *,
    subscription: Subscription,
    labels: RecencyLabels,
    rows: Sequence[Mapping[str, object]],
) -> HostDecision:
    """Decide the real-time feed of a subscription not listed as delayed, from its snapshot.

    Args:
        asset_class: The class (stocks, options, indices, or futures).
        subscription: Its subscription.
        labels: The snapshot's recency labels.
        rows: The snapshot's rows, without the per-ticker error rows.

    Returns:
        The real-time host, `REALTIME`, with the label as evidence; a WARNING when the label
        is undocumented, or when an index snapshot has none. Stocks and options whose rows
        have a top-level `fmv` field and no label (a Business key without a trades and quotes
        expansion) get the Business host, real-time too, with one WARNING.

    Raises:
        EntitlementError: If a label says delayed (`recency_labels.delayed_refusal`).
    """
    if labels.delayed is not None:
        refusal = delayed_refusal(
            subscription, f"{asset_class} streams", labels.delayed, streams=True
        )
        raise refusal
    if labels.unknown is not None:
        field, value = labels.unknown
        return _unconfirmed_real_time(
            asset_class, f"{field}={value!r}, neither DELAYED nor REAL-TIME, in the snapshot"
        )
    if labels.real_time is not None:
        return _real_time(asset_class, labels.real_time)
    if asset_class in (AssetClass.STOCKS, AssetClass.OPTIONS) and any("fmv" in r for r in rows):
        log_once(
            logger,
            f"business-host:{asset_class.value}",
            "%s WebSocket host: a Business key without trades and quotes; streaming from "
            "%s (pin another with %s)",
            asset_class.value,
            BUSINESS_HOST,
            _ws_hosts_hint(asset_class),
            level=logging.WARNING,
        )
        return HostDecision(
            BUSINESS_HOST,
            Recency.REALTIME,
            "Business key without trades/quotes; base plan streams FMV-based aggregates "
            "(UNVERIFIED host)",
        )
    evidence = f"no timeframe in the {asset_class} snapshot"
    if asset_class is AssetClass.INDICES:  # the field is documented: its absence is anomalous
        return _unconfirmed_real_time(asset_class, evidence)
    return _real_time(asset_class, evidence)


def _delayed(
    asset_class: AssetClass,
    probe: SnapshotProbe,
    fetch: Callable[[SnapshotProbe], SnapshotAnswer],
) -> HostDecision:
    """Decide the delayed feed of a listed subscription, warning when the plan is real-time.

    Args:
        asset_class: The class.
        probe: Its snapshot request.
        fetch: Makes the request.

    Returns:
        `delayed.massive.com`, `DELAYED`, evidence `"MassiveConfig.delayed_subscriptions"`.
        A refused snapshot changes nothing; one labelled `REAL-TIME` and nowhere `DELAYED`
        logs one WARNING per class and process.
    """
    try:
        answer = fetch(probe)
    except EntitlementError:
        answer = SnapshotAnswer("", ())
    labels = read_labels(answer.status, [r for r in answer.rows if not is_error_row(r)])
    if labels.delayed is None and labels.real_time is not None:
        log_once(
            logger,
            f"real-time-plan-on-delayed-feed:{asset_class.value}",
            "your plan streams %s real-time (%s), but MassiveConfig.delayed_subscriptions "
            "lists dm.Subscription.%s, so its streams use the delayed feed %s; remove it "
            "from delayed_subscriptions to get real-time data",
            asset_class.value,
            labels.real_time,
            subscription_of(asset_class).name,
            DELAYED_HOST,
            level=logging.WARNING,
        )
    logger.info(
        "%s WebSocket host: %s (MassiveConfig.delayed_subscriptions)",
        asset_class.value,
        DELAYED_HOST,
    )
    return HostDecision(DELAYED_HOST, Recency.DELAYED, "MassiveConfig.delayed_subscriptions")


def decide(
    asset_class: AssetClass,
    family: ChannelFamily,
    *,
    ws_hosts: Mapping[AssetClass, str],
    delayed_subscriptions: AbstractSet[Subscription],
    fetch: Callable[[SnapshotProbe], SnapshotAnswer],
) -> HostDecision:
    """Decide the WebSocket host of an asset class and channel family.

    The rules, in order: the FMV family is `business.massive.com`; a TICK host pinned in
    `ws_hosts` is used, with its host's recency; a subscription in `delayed_subscriptions`
    uses `delayed.massive.com` (`_delayed`); forex and crypto use `socket.massive.com` with no
    request; every other class reads its snapshot (`SNAPSHOT_PROBES`) once through `fetch`,
    drops the per-ticker rows that carry an `error` (a ticker the plan does not include counts
    as absent), and streams from the real-time feed unless a label says delayed
    (`_from_labels`). A refused snapshot (HTTP 403 or `NOT_AUTHORIZED`) says nothing certain
    about the WebSocket plan: the class streams from the real-time feed, a WARNING names
    Massive's text, and a refusal from the WebSocket server fails the refused streams.

    Runs on the calling thread; the only I/O is `fetch`, which blocks for one request.

    Args:
        asset_class: The class of the stream.
        family: Its channel family.
        ws_hosts: `MassiveConfig.ws_hosts`, the pinned TICK hosts.
        delayed_subscriptions: `MassiveConfig.delayed_subscriptions`.
        fetch: Makes the probe's request and returns its envelope status and raw JSON rows.
            It raises the public errors of the REST transport: `AuthenticationError`,
            `EntitlementError`, or `MassiveError`.

    Returns:
        The decision.

    Raises:
        ValueError: If the family is FMV and the class's subscription is in
            `delayed_subscriptions`: FMV is real-time only.
        EntitlementError: If the subscription is not in `delayed_subscriptions` and Massive
            labels its snapshot delayed; the message names the label and the fix.
        AuthenticationError: If `fetch` raises it (a rejected key).
        MassiveError: If `fetch` fails otherwise (a transport failure, a server error).
    """
    subscription = subscription_of(asset_class)
    listed = subscription in delayed_subscriptions
    if family is ChannelFamily.FMV:
        if listed:
            raise ValueError(
                f"{asset_class} fair-market-value (FMV) streams are real-time only: Massive "
                f"serves them from {BUSINESS_HOST} and has no delayed FMV feed, but "
                f"MassiveConfig.delayed_subscriptions lists dm.Subscription.{subscription.name}; "
                "stream FMV from a session whose delayed_subscriptions does not list it"
            )
        return HostDecision(
            BUSINESS_HOST, Recency.REALTIME, "FMV is Business-only; fixed host (docs)"
        )
    pinned = ws_hosts.get(asset_class)
    if pinned is not None:
        return HostDecision(pinned, recency_of(pinned), "ws_hosts")
    if asset_class in (AssetClass.FOREX, AssetClass.CRYPTO):
        return HostDecision(
            REALTIME_HOST, Recency.REALTIME, "single documented host; Starter is real-time"
        )
    probe = SNAPSHOT_PROBES[asset_class]
    if listed:
        return _delayed(asset_class, probe, fetch)
    try:
        answer = fetch(probe)
    except EntitlementError as error:
        return _unconfirmed_real_time(
            asset_class, f"the host check's snapshot ({probe.path}) was refused: {error}"
        )
    entitled = [row for row in answer.rows if not is_error_row(row)]
    labels = read_labels(answer.status, entitled)
    return _from_labels(asset_class, subscription=subscription, labels=labels, rows=entitled)


class HostDecisionCache:
    """The WebSocket host decisions of one session, by (asset class, channel family).

    `RestState` holds one per session, and `RestEndpoints.resolve_host` reads and fills it,
    single-flight: the first caller of a (class, family) claims the decision and makes it,
    every concurrent caller waits on the decision's future, and a failed decision is forgotten
    so that the next caller tries again. Thread-safe: the decisions sit behind one leaf lock,
    held only to read or change them; every caller waits on a future outside it.
    """

    __slots__ = ("_hosts", "_lock")

    def __init__(self) -> None:
        """Start with no decision; nothing is requested."""
        self._lock = threading.Lock()
        self._hosts: dict[tuple[AssetClass, ChannelFamily], Future[HostDecision]] = {}

    def host_future(
        self, key: tuple[AssetClass, ChannelFamily]
    ) -> tuple[Future[HostDecision], bool]:
        """Return the decision of a (class, family), or claim it for the caller to make.

        Args:
            key: The asset class and channel family.

        Returns:
            The decision's future and whether the caller created it: the creator makes the
            decision and completes the future; everyone else waits on it, outside every lock.
        """
        with self._lock:
            future = self._hosts.get(key)
            if future is not None:
                return future, False
            future = self._hosts[key] = Future()
            return future, True

    def known_host(self, key: tuple[AssetClass, ChannelFamily]) -> HostDecision | None:
        """Return the decision of a (class, family) if one is made, without claiming it.

        A failed decision is removed before its future fails, but this check reads the future
        under the lock and looks at it after: its maker may forget and fail it in between. So
        only a decision counts; a failed future gives None, and the caller then claims or
        waits as usual (`RestEndpoints.resolve_host`). The failed decision is forgotten by
        then, so the caller claims a fresh one and makes the decision itself.

        Args:
            key: The asset class and channel family.

        Returns:
            The decision, or None while none is made, one is being made, or the one being made
            has just failed.
        """
        with self._lock:
            future = self._hosts.get(key)
        return succeeded_result(future)

    def forget_host(
        self, key: tuple[AssetClass, ChannelFamily], future: Future[HostDecision]
    ) -> None:
        """Remove a failed decision, so that the next caller tries again.

        Args:
            key: The asset class and channel family.
            future: The failed decision's future; another one under the key is kept.
        """
        with self._lock:
            if self._hosts.get(key) is future:
                del self._hosts[key]


T = TypeVar("T")
"""The result type of a shared lookup's future."""


def succeeded_result(future: Future[T] | None) -> T | None:
    """Return a shared lookup's result once it has succeeded, and None in every other state.

    Args:
        future: The lookup's future, or None when no lookup is claimed.

    Returns:
        The result; None when the lookup is not claimed, still running, or failed (its
        future then holds the error, which only the claim-or-wait path may act on).
    """
    if future is None or not future.done() or future.exception() is not None:
        return None
    return future.result()
