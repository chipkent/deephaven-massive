"""Which WebSocket host serves an asset class and channel family: the user's choice, checked.

Massive serves each WebSocket cluster (`/stocks`, `/options`, ...) from several hosts, which
each channel's page lists (`_datasets.feeds`): `socket.massive.com`, the real-time feed;
`delayed.massive.com`, the delayed feed (15 minutes behind, 10 for futures); and, for the
fair-market-value (FMV) channels, the one host their pages list, `business.massive.com` today.
The user chooses the feed per subscription with `MassiveConfig.delayed_subscriptions`, and the
choice is deterministic: a stream never moves between feeds. This module decides the host once
per (asset class, channel family), before the first connection, and never by opening a
connection to try:

- the FMV family always uses the one host its class's FMV page lists (`feeds.fmv_host`);
- a TICK host pinned in `MassiveConfig.websocket_hosts` is used as is (the configuration has checked
  it against `delayed_subscriptions`);
- a listed subscription uses `delayed.massive.com`, with no request: the user asked for the
  delayed feed, so no answer from Massive changes the host;
- forex and crypto have one documented host, `socket.massive.com`, so no request is made;
- every other subscription uses the real-time feed. Its class's snapshot (`SNAPSHOT_PROBES`) is
  read first, and when Massive labels it delayed (`recency_labels.read_labels`: the envelope
  status, or a `timeframe` field), the verb raises `EntitlementError` naming the evidence and
  the exact fix rather than open a connection Massive would refuse. Without a delayed label
  nothing is guessed: a refused snapshot, an undocumented label, or no label at all streams
  from the real-time host, and a refusal from Massive fails the refused streams.

A dataset whose pages document one host (fair market value, the options admin messages, the
limit-up-limit-down bands; `feeds.dataset_hosts`) gets its family's decision like any other
stream: the FMV family's host is its page's, and the others follow their class's feed. Whether
Massive serves it there is Massive's answer, so a refused subscription fails the stream with
Massive's text and the documented host (`refusal_note`), and the library never refuses it first.

Recency follows the feed Massive's pages give the host (`recency_of`): a `Real-Time` or
`Business` host is shown real-time, a `Delayed` host delayed, and any other pinned host
`UNKNOWN`, which is never shown as real-time.

`decide` is pure apart from logging: the REST request itself is made by the caller's `fetch`
callable, which the REST transport (`_ingest/rest.py`) supplies, so every HTTP error has
already become its public exception there. A decision that raises is not cached, so the next
verb decides again and sees what Massive answers then. The REST transport holds a session's
decisions, single-flight (`_ingest/shared_lookup.py`), and writes the `m.connections()` rows
that record them. Nothing here starts a thread or calls Deephaven.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from deephaven_massive._config import DELAYED_HOST, DELAYED_PREFIX, REALTIME_HOST
from deephaven_massive._datasets.column_reference import page_url
from deephaven_massive._datasets.feeds import dataset_hosts, fmv_host, host_feed
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Recency,
    Subscription,
    subscription_of,
)
from deephaven_massive._errors import EntitlementError
from deephaven_massive._ingest.recency_labels import (
    RecencyLabels,
    delayed_refusal,
    read_labels,
)
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._massive import Feed
from deephaven_massive._tables.transports import HostDecision

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from collections.abc import Set as AbstractSet

    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = [
    "FEED_RECENCY",
    "FUTURES_PROBE_CONTRACTS",
    "ONE_FEED_NOTES",
    "SNAPSHOT_PROBES",
    "SnapshotAnswer",
    "SnapshotProbe",
    "decide",
    "recency_of",
    "refusal_note",
    "refused_snapshot",
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

    Such a snapshot says nothing about which host serves the key's streams: Massive may serve
    them on another host than the real-time one, so a refusal's message names
    `MassiveConfig.websocket_hosts`.

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

    A key Massive refuses the snapshot may have no live (WebSocket) data of the class on either
    feed, so a stream refusal after it quotes the snapshot's refusal and names the REST reads.

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


FEED_RECENCY: Final[Mapping[Feed, Recency]] = {
    Feed.REAL_TIME: Recency.REALTIME,
    Feed.DELAYED: Recency.DELAYED,
    # The fair-market-value channels' feed, which Massive's FMV pages document only in real
    # time. A host pinned to it for another channel is shown real-time too, though the label
    # does not say so: Massive's index value page lists only this feed, yet gives one plan
    # those values 15 minutes late, and such a key's pinned index values still show REALTIME.
    Feed.BUSINESS: Recency.REALTIME,
}
"""The recency a host's data is shown with, by the feed Massive's pages give the host."""


def recency_of(host: str) -> Recency:
    """Return the recency of a WebSocket host's data.

    Args:
        host: The hostname.

    Returns:
        The recency of the feed Massive's pages give the host (`FEED_RECENCY`); for a host
        no page lists, `DELAYED` for a delayed feed (a name that starts with `delayed.`) and
        `UNKNOWN` for any other (a pinned host the library does not know).
    """
    feed = host_feed(host)
    if feed is not None:
        return FEED_RECENCY[feed]
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

    The rules, in order: the FMV family uses the host its class's FMV page lists, whatever the
    settings; a TICK
    host pinned in
    `websocket_hosts` is used, with its host's recency; a subscription in `delayed_subscriptions`
    uses `delayed.massive.com` with no request; forex and crypto use `socket.massive.com` with no
    request; every other class reads its snapshot (`SNAPSHOT_PROBES`) once through `fetch`,
    drops the per-ticker rows that carry an `error` (a ticker Massive refuses counts as
    absent), and streams from the real-time feed unless a label says delayed
    (`_from_labels`). A refused snapshot (HTTP 403, `NOT_AUTHORIZED`, or rows that all carry
    an `error`, such as `NOT_ENTITLED`) says nothing certain
    about the WebSocket feeds: the class streams from the real-time feed, a WARNING names
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
            fmv_host(asset_class), Recency.REALTIME, "FMV channel's documented host"
        )
    pinned = websocket_hosts.get(asset_class)
    if pinned is not None:
        return HostDecision(pinned, recency_of(pinned), "websocket_hosts")
    if asset_class in (AssetClass.FOREX, AssetClass.CRYPTO):
        return HostDecision(REALTIME_HOST, Recency.REALTIME, "single documented host")
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
        # Massive can refuse a whole snapshot with HTTP 200 and an error on every row (such as
        # `NOT_ENTITLED`), which is a refusal like a 403.
        first = answer.rows[0]
        refusal = f"{first.get('error')}: {first.get('message', '')}".rstrip(": ")
        return _unconfirmed_real_time(asset_class, _REFUSED_SNAPSHOT.format(probe.path, refusal))
    labels = read_labels(answer.status, entitled)
    return _from_labels(asset_class, subscription=subscription, labels=labels)


ONE_FEED_NOTES: Final[Mapping[Feed, str]] = {
    Feed.REAL_TIME: "{name} streams only in real time, on the plans listed at {page}",
    Feed.DELAYED: "Massive documents {name} only on its Delayed feed, {host}; its plans are "
    "listed at {page}",
    Feed.BUSINESS: "Massive documents {name} only on its Business feed, {host}; its plans are "
    "listed at {page}",
}
"""What a refusal of a dataset documented on one feed adds, by that feed."""


def refusal_note(spec: DatasetSpec) -> str:
    """Say where Massive documents a dataset served on one feed, and where its plans are listed.

    Massive's refusal of such a stream's subscription adds it, so the user learns the one feed
    Massive's pages document for the dataset, and finds on its generated page what Massive's
    documentation says about its plans. Only a dataset whose pages document the real-time
    feed alone is said to stream only in real time: a page that lists another feed alone says
    where the channel is served, not how recent its data is.

    Args:
        spec: A WebSocket dataset.

    Returns:
        For example `"options.admin_messages streams only in real time, on the plans listed at
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/options.admin_messages.md"`,
        or `"Massive documents stocks.fmv only on its Business feed, business.massive.com; its
        plans are listed at ..."`; `""` for a dataset whose pages document several hosts.
    """
    hosts = dataset_hosts(spec)
    if len(hosts) != 1:
        return ""
    (only,) = hosts
    return ONE_FEED_NOTES[only.feed].format(name=spec.name, host=only.host, page=page_url(spec))
