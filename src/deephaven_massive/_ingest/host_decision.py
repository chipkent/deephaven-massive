"""Which WebSocket host serves an asset class and channel family: the user's choice, checked.

Massive serves each WebSocket cluster (`/stocks`, `/options`, ...) from several hosts:
`socket.massive.com`, the real-time feed; `delayed.massive.com`, the delayed feed (15 minutes
behind, 10 for futures); and `business.massive.com` for the Business plans' fair-market-value
(FMV) channels. The user chooses the feed per subscription with
`MassiveConfig.delayed_subscriptions`, and the choice is deterministic: a stream never moves
between feeds. This module decides the host once per (asset class, channel family), before the
first connection, and never by opening a connection to try:

- the FMV family is always `business.massive.com`;
- a TICK host pinned in `MassiveConfig.websocket_hosts` is used as is (the configuration has checked
  it against `delayed_subscriptions`);
- a listed subscription uses `delayed.massive.com`, with no request: the user asked for the
  delayed feed, so no answer from Massive changes the host;
- forex and crypto have one documented host, `socket.massive.com`, real-time on every plan, so
  no request is made;
- every other subscription uses the real-time feed. Its class's snapshot (`SNAPSHOT_PROBES`) is
  read first, and when Massive labels it delayed (`recency_labels.read_labels`: the envelope
  status, or a `timeframe` field), the verb raises `EntitlementError` naming the evidence and
  the exact fix rather than open a connection Massive would refuse. Without a delayed label
  nothing is guessed: a refused snapshot, an undocumented label, or no label at all streams
  from the real-time host, and a refusal from Massive fails the refused streams.

A dataset Massive streams only in real time (`DatasetSpec.real_time_only`: fair market value,
the options admin messages) has no delayed feed, yet it shares its class's decision, which is
cached per class and family. So `require_real_time` checks each such stream when it opens: a
session whose `delayed_subscriptions` lists its subscription is refused with `ValueError`, and a
key whose data Massive labels delayed with an `EntitlementError` saying that the plan does not
include the dataset.

Recency follows the host (`recency_of`): the real-time and Business hosts serve real-time
data, the delayed host delayed data, and any other pinned host `UNKNOWN`, which is never shown
as real-time.

`decide` is pure apart from logging: the REST request itself is made by the caller's `fetch`
callable, which the REST transport (`_ingest/rest.py`) supplies, so every HTTP error has
already become its public exception there. A decision that raises is not cached, so the next
verb decides again and sees a plan upgraded meanwhile. The REST transport holds a session's
decisions, single-flight (`_ingest/shared_lookup.py`), and writes the `m.connections()` rows
that record them. Nothing here starts a thread or calls Deephaven.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from deephaven_massive._config import BUSINESS_HOST, DELAYED_HOST, DELAYED_PREFIX, REALTIME_HOST
from deephaven_massive._datasets.column_reference import page_url
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Recency,
    Subscription,
    subscription_of,
)
from deephaven_massive._errors import EntitlementError
from deephaven_massive._ingest.recency_labels import (
    DelayedDataError,
    RecencyLabels,
    delayed_refusal,
    read_labels,
)
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._tables.transports import HostDecision

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = [
    "FUTURES_PROBE_CONTRACTS",
    "SNAPSHOT_PROBES",
    "SnapshotAnswer",
    "SnapshotProbe",
    "decide",
    "real_time_note",
    "recency_of",
    "refused_snapshot",
    "require_real_time",
    "unlabelled",
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


FUTURES_PROBE_CONTRACTS: Final = 100
"""How many E-mini S&P contracts the futures host check reads: enough to reach the traded
contracts past the far-dated ones the snapshot lists first, in one request."""

SNAPSHOT_PROBES: Final[Mapping[AssetClass, SnapshotProbe]] = {
    AssetClass.STOCKS: SnapshotProbe("/v3/snapshot", {}, {"ticker.any_of": ("SPY", "AAPL")}),
    AssetClass.OPTIONS: SnapshotProbe(
        "/v3/snapshot/options/{underlyingAsset}", {"underlyingAsset": "SPY"}, {"limit": 1}
    ),
    AssetClass.INDICES: SnapshotProbe("/v3/snapshot/indices", {}, {"ticker.any_of": ("I:SPX",)}),
    AssetClass.FUTURES: SnapshotProbe(
        "/futures/v1/snapshot", {}, {"product_code": "ES", "limit": FUTURES_PROBE_CONTRACTS}
    ),
}
"""The snapshot each class's TICK host is checked against: the unified snapshot of two liquid
stocks, one contract of the SPY option chain, the S&P 500 index, and the E-mini S&P contracts.

The futures snapshot lists a product's contracts by ticker, so far-dated contracts that do not
trade yet come first (`ESH0`, `ESH1`, ...), and only a contract that trades carries a
`timeframe`: on 2026-11-02 the first two rows carried none and the third did. So the probe
reads up to `FUTURES_PROBE_CONTRACTS` rows in one request, and `recency_labels.read_labels`
takes the labels of every row that has them."""


_NO_TIMEFRAME: Final = "no timeframe in the {} snapshot"
"""The evidence of a decision whose snapshot carried no recency label, by its class."""


def unlabelled(evidence: str) -> bool:
    """Return whether a host decision's evidence is a snapshot that carried no recency label.

    Such a snapshot proves nothing about the plan: a base Business key's (without the trades
    and quotes expansion) carries none, and its plan may serve the class's streams on another
    host than the real-time one, so a refusal's message names `MassiveConfig.websocket_hosts`.

    Args:
        evidence: The decision's evidence, such as `"no timeframe in the stocks snapshot"`.

    Returns:
        True for the evidence of a snapshot without a `timeframe`, of any class.
    """
    return any(evidence == _NO_TIMEFRAME.format(asset_class) for asset_class in AssetClass)


_REFUSED_SNAPSHOT: Final = "the host check's snapshot ({}) was refused: {}"
"""The evidence of a decision whose snapshot Massive refused, by its path and Massive's text."""


def refused_snapshot(evidence: str) -> bool:
    """Return whether a host decision's evidence is a snapshot Massive refused.

    Massive refuses the snapshot on its free Basic plan, which has no live (WebSocket) data on
    either feed, so a stream refusal after it gains nothing from the delayed feed.

    Args:
        evidence: The decision's evidence, such as
            `"the host check's snapshot (/v3/snapshot) was refused: ..."`.

    Returns:
        True for the evidence of a refused snapshot, of any class.
    """
    return any(
        evidence.startswith(_REFUSED_SNAPSHOT.format(probe.path, ""))
        for probe in SNAPSHOT_PROBES.values()
    )


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


def _websocket_hosts_hint(asset_class: AssetClass) -> str:
    """Return the configuration that pins the class's host, for log messages.

    Args:
        asset_class: The class.

    Returns:
        For example `MassiveConfig(websocket_hosts={dm.AssetClass.STOCKS: "<host>"})`.
    """
    return f'MassiveConfig(websocket_hosts={{dm.AssetClass.{asset_class.name}: "<host>"}})'


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
        _websocket_hosts_hint(asset_class),
    )
    return HostDecision(REALTIME_HOST, Recency.REALTIME, evidence)


def _from_labels(
    asset_class: AssetClass, *, subscription: Subscription, labels: RecencyLabels
) -> HostDecision:
    """Decide the real-time feed of a subscription not listed as delayed, from its snapshot.

    Args:
        asset_class: The class (stocks, options, indices, or futures).
        subscription: Its subscription.
        labels: The snapshot's recency labels, read from its rows without the per-ticker error
            rows.

    Returns:
        The real-time host, `REALTIME`, with the label as evidence; a WARNING when the label
        is undocumented, or when an index snapshot has none.

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
    evidence = _NO_TIMEFRAME.format(asset_class)
    if asset_class is AssetClass.INDICES:  # the field is documented: its absence is anomalous
        return _unconfirmed_real_time(asset_class, evidence)
    return _real_time(asset_class, evidence)


def decide(
    asset_class: AssetClass,
    family: ChannelFamily,
    *,
    websocket_hosts: Mapping[AssetClass, str],
    delayed_subscriptions: AbstractSet[Subscription],
    fetch: Callable[[SnapshotProbe], SnapshotAnswer],
) -> HostDecision:
    """Decide the WebSocket host of an asset class and channel family.

    The rules, in order: the FMV family is `business.massive.com` (a real-time-only stream's
    own check is `require_real_time`); a TICK host pinned in
    `websocket_hosts` is used, with its host's recency; a subscription in `delayed_subscriptions`
    uses `delayed.massive.com` with no request; forex and crypto use `socket.massive.com` with no
    request; every other class reads its snapshot (`SNAPSHOT_PROBES`) once through `fetch`,
    drops the per-ticker rows that carry an `error` (a ticker the plan does not include counts
    as absent), and streams from the real-time feed unless a label says delayed
    (`_from_labels`). A refused snapshot (HTTP 403, `NOT_AUTHORIZED`, or rows that all carry
    an `error`, such as `NOT_ENTITLED`) says nothing certain
    about the WebSocket plan: the class streams from the real-time feed, a WARNING names
    Massive's text, and a refusal from the WebSocket server fails the refused streams.

    Runs on the calling thread; the only I/O is `fetch`, which blocks for one request.

    Args:
        asset_class: The class of the stream.
        family: Its channel family.
        websocket_hosts: `MassiveConfig.websocket_hosts`, the pinned TICK hosts.
        delayed_subscriptions: `MassiveConfig.delayed_subscriptions`.
        fetch: Makes the probe's request and returns its envelope status and raw JSON rows.
            It raises the public errors of the REST transport: `AuthenticationError`,
            `EntitlementError`, or `MassiveError`.

    Returns:
        The decision.

    Raises:
        DelayedDataError: If the subscription is not in `delayed_subscriptions` and Massive
            labels its snapshot delayed; the message names the label and the fix. It is an
            `EntitlementError`.
        AuthenticationError: If `fetch` raises it (a rejected key).
        MassiveError: If `fetch` fails otherwise (a transport failure, a server error).
    """
    subscription = subscription_of(asset_class)
    listed = subscription in delayed_subscriptions
    if family is ChannelFamily.FMV:
        return HostDecision(
            BUSINESS_HOST, Recency.REALTIME, "FMV is Business-only; fixed host (docs)"
        )
    pinned = websocket_hosts.get(asset_class)
    if pinned is not None:
        return HostDecision(pinned, recency_of(pinned), "websocket_hosts")
    if asset_class in (AssetClass.FOREX, AssetClass.CRYPTO):
        return HostDecision(
            REALTIME_HOST, Recency.REALTIME, "single documented host; Starter is real-time"
        )
    if listed:
        logger.info(
            "%s WebSocket host: %s (MassiveConfig.delayed_subscriptions)",
            asset_class.value,
            DELAYED_HOST,
        )
        return HostDecision(DELAYED_HOST, Recency.DELAYED, "MassiveConfig.delayed_subscriptions")
    return _from_snapshot(asset_class, SNAPSHOT_PROBES[asset_class], fetch)


def _from_snapshot(
    asset_class: AssetClass,
    probe: SnapshotProbe,
    fetch: Callable[[SnapshotProbe], SnapshotAnswer],
) -> HostDecision:
    """Decide a class's host from its snapshot: the last rule of `decide`.

    Args:
        asset_class: The class of the stream.
        probe: The class's snapshot request (`SNAPSHOT_PROBES`).
        fetch: Sends the request, as `decide` takes it.

    Returns:
        The real-time host with the refusal as its evidence when Massive refuses the snapshot
        (an `EntitlementError`, or rows that all carry an `error`); otherwise the decision of
        the labels of the rows that carry none (`_from_labels`).

    Raises:
        MassiveError: If `fetch` fails otherwise.
    """
    subscription = subscription_of(asset_class)
    try:
        answer = fetch(probe)
    except EntitlementError as error:
        return _unconfirmed_real_time(asset_class, _REFUSED_SNAPSHOT.format(probe.path, error))
    entitled = [row for row in answer.rows if not is_error_row(row)]
    if answer.rows and not entitled:
        # Massive can refuse a whole snapshot with HTTP 200 and an error on every row (a
        # Basic key's `NOT_ENTITLED`), which is a refusal like a 403.
        first = answer.rows[0]
        refusal = f"{first.get('error')}: {first.get('message', '')}".rstrip(": ")
        return _unconfirmed_real_time(asset_class, _REFUSED_SNAPSHOT.format(probe.path, refusal))
    labels = read_labels(answer.status, entitled)
    return _from_labels(asset_class, subscription=subscription, labels=labels)


def require_real_time(
    spec: DatasetSpec,
    resolve: Callable[[], HostDecision],
    *,
    delayed_subscriptions: AbstractSet[Subscription],
) -> HostDecision:
    """Decide a stream's host, refusing delayed data for a dataset that streams only in real time.

    A real-time-only dataset (`DatasetSpec.real_time_only`) has no delayed feed. Its class's
    decision is shared with the class's other streams and cached, so the refusal is made here,
    for the one stream, and never cached: the next call checks again. Any other dataset gets the
    decision, or the error, of `resolve` unchanged. Runs on the verb's thread; the only I/O is
    `resolve`'s.

    Args:
        spec: The stream's dataset.
        resolve: Decides the host of the dataset's class and family (the REST transport's
            `resolve_host`, cached per session).
        delayed_subscriptions: `MassiveConfig.delayed_subscriptions`.

    Returns:
        The decision.

    Raises:
        ValueError: If the dataset is real-time only and `delayed_subscriptions` lists its
            subscription, the one way a session reads it from Massive's delayed feed.
        EntitlementError: If the dataset is real-time only and Massive labels the key's data of
            its subscription delayed: the plan does not include the dataset. The message says
            where its plans are listed.

    `resolve` raises the `AuthenticationError`, `EntitlementError`, or `MassiveError` of the
    host decision.
    """
    if not spec.real_time_only:
        return resolve()
    asset_class = cast("AssetClass", spec.asset_class)
    subscription = subscription_of(asset_class)
    try:
        decision = resolve()
    except DelayedDataError as error:
        logger.debug("%s refused, real-time only: %s", spec.name, error)
        raise EntitlementError(
            f"{spec.name} streams only in real time, and Massive labels this key's "
            f"{subscription} data delayed, so its plan does not include it; "
            f"{real_time_note(spec)}"
        ) from error
    # The configuration pins a delayed host only for a listed subscription, so the list alone
    # says whether the session reads the subscription from the delayed feed.
    if subscription not in delayed_subscriptions:
        return decision
    # A refused argument is the caller's to fix and travels in the ValueError: DEBUG only.
    logger.debug("%s refused, real-time only: %s is delayed", spec.name, subscription)
    raise ValueError(
        f"{spec.name} streams only in real time, and Massive has no delayed feed of it, but "
        f"MassiveConfig.delayed_subscriptions lists dm.Subscription.{subscription.name}; "
        "stream it from a session whose delayed_subscriptions does not list it"
    )


def real_time_note(spec: DatasetSpec) -> str:
    """Say that a dataset streams only in real time, and where its plans are listed.

    A refusal of a real-time-only stream adds it, whether the library refuses the stream at the
    call (`require_real_time`) or Massive refuses its subscription, so the user learns which
    plans include the dataset rather than a fix of the feed that cannot apply.

    Args:
        spec: A real-time-only dataset.

    Returns:
        For example `"options.admin_messages streams only in real time, on the plans listed at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.admin_messages.md"`.
    """
    return f"{spec.name} streams only in real time, on the plans listed at {page_url(spec)}"
