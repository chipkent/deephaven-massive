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
  it against `delayed_subscriptions`), and its recency is what Massive says (`_pinned`);
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

Recency comes from Massive's answers and the user's settings, never from a host's name: the
real-time feed is shown real-time once the snapshot raises no delayed label, the delayed feed
delayed, and a pinned host as its class's snapshot labels the key's data. A pinned host whose
snapshot gives no documented label (no label, a refusal, a failed read, or a forex or crypto
class, whose snapshots carry none) is `UNKNOWN`, which is never shown as real-time.

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
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, assert_never

from deephaven_massive._config import DELAYED_HOST, REALTIME_HOST
from deephaven_massive._datasets.column_reference import page_url
from deephaven_massive._datasets.feeds import dataset_hosts, fmv_host
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Recency,
    Subscription,
    subscription_of,
)
from deephaven_massive._errors import EntitlementError, ErrorSource, MassiveError
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
    "FUTURES_PROBE_CONTRACTS",
    "ONE_FEED_NOTES",
    "SNAPSHOT_PROBES",
    "SnapshotAnswer",
    "SnapshotProbe",
    "decide",
    "pinned",
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


_PINNED: Final = "websocket_hosts"
"""The evidence of a pinned host's decision, alone or before what Massive's snapshot said."""


def pinned(evidence: str) -> bool:
    """Return whether a host decision's evidence is of a host pinned in `websocket_hosts`.

    A pinned decision's evidence is `websocket_hosts`, or `websocket_hosts, ` and what set its
    recency, so none of `unlabelled`, `refused_snapshot`, or `recency_labels.shows_real_time`
    matches it.

    Args:
        evidence: The decision's evidence, such as `"websocket_hosts, timeframe=DELAYED"`.

    Returns:
        True for the evidence of a pinned host.
    """
    return evidence == _PINNED or evidence.startswith(f"{_PINNED}, ")


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
    settings; a TICK host pinned in `websocket_hosts` is used, with the recency Massive's
    snapshot gives it (`_pinned`), and a failure of that snapshot at Massive or on the network
    leaves the recency `UNKNOWN` instead of raising, since the snapshot only labels a host the
    user already chose; a subscription in `delayed_subscriptions` uses `delayed.massive.com`
    with no request; forex and crypto use `socket.massive.com` with no
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
        DelayedDataError: If the subscription is not in `delayed_subscriptions`, no host is
            pinned for its class, and Massive labels its snapshot delayed; the message names
            the label and the fix. It is an `EntitlementError`.
        AuthenticationError: If `fetch` raises it (a rejected key) and no host is pinned.
        MassiveError: If `fetch` fails otherwise (a transport failure, a server error) and no
            host is pinned; for a pinned host, only a closed session, a library defect, or a
            rate-limit pause the caller gave up on (`_pinned_recency`).
    """
    subscription = subscription_of(asset_class)
    listed = subscription in delayed_subscriptions
    if family is ChannelFamily.FMV:
        return HostDecision(
            fmv_host(asset_class), Recency.REALTIME, "FMV channel's documented host"
        )
    host = websocket_hosts.get(asset_class)
    if host is not None:
        return _pinned(asset_class, host, listed=listed, fetch=fetch)
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
    labels = _labels(answer)
    if isinstance(labels, str):
        return _unconfirmed_real_time(asset_class, _REFUSED_SNAPSHOT.format(probe.path, labels))
    return _from_labels(asset_class, subscription=subscription, labels=labels)


def _labels(answer: SnapshotAnswer) -> RecencyLabels | str:
    """Read a snapshot's recency labels from the rows Massive did not refuse.

    Args:
        answer: The snapshot.

    Returns:
        The labels of the rows that carry no `error` (a ticker Massive refuses counts as
        absent); or, when every row carries one, Massive's refusal, such as
        `"NOT_ENTITLED: Not entitled to this data"`.
    """
    entitled = [row for row in answer.rows if not is_error_row(row)]
    if answer.rows and not entitled:
        # Massive can refuse a whole snapshot with HTTP 200 and an error on every row (such as
        # `NOT_ENTITLED`), which is a refusal like a 403.
        first = answer.rows[0]
        return f"{first.get('error')}: {first.get('message', '')}".rstrip(": ")
    return read_labels(answer.status, entitled)


_FAILED_SNAPSHOT: Final = "the host check's snapshot ({}) failed: {}"
"""The evidence of a pinned host whose snapshot read failed, by its path and the error."""


@dataclass(frozen=True, slots=True)
class _PinnedRecency:
    """What set a pinned host's recency (`_pinned_recency`).

    Attributes:
        recency: The recency.
        why: What set it, such as `"timeframe=DELAYED"`; the evidence after `websocket_hosts, `.
        warn: True when the snapshot gave no usable answer (an undocumented label, a refusal,
            or a failed read), which is logged at WARNING.
    """

    recency: Recency
    why: str
    warn: bool = False


def _pinned(
    asset_class: AssetClass,
    host: str,
    *,
    listed: bool,
    fetch: Callable[[SnapshotProbe], SnapshotAnswer],
) -> HostDecision:
    """Decide a TICK host pinned in `websocket_hosts`: the pinned host, with Massive's recency.

    The user chose the host, so it is used whatever Massive's snapshot says, and nothing here
    raises: a snapshot labelled delayed shows the stream `DELAYED` (no other host serves the
    data the user pinned, such as index values), and a snapshot Massive refuses, or whose
    read fails, leaves the recency `UNKNOWN`, as a refusal of the stream itself says what
    Massive serves there. The recency never comes from the host's name.

    Args:
        asset_class: The class of the stream.
        host: The pinned host.
        listed: Whether the class's subscription is in `delayed_subscriptions`.
        fetch: Sends the class's snapshot request, as `decide` takes it.

    Returns:
        The pinned host, with the recency `_pinned_recency` gives it and evidence
        `websocket_hosts, ` and what set it; logged at INFO, or at WARNING when the snapshot
        gave no usable answer.

    Raises:
        MassiveError: If the snapshot's read fails other than at Massive or on the network
            (`_pinned_recency`).
    """
    found = _pinned_recency(asset_class, listed=listed, fetch=fetch)
    evidence = f"{_PINNED}, {found.why}"
    if found.warn:
        logger.warning(
            "%s WebSocket host: %s (%s); streaming from the pinned host with its recency %s "
            "(a new session reads Massive's label again)",
            asset_class.value,
            host,
            evidence,
            found.recency,
        )
    else:
        logger.info(
            "%s WebSocket host: %s, %s (%s)", asset_class.value, host, found.recency, evidence
        )
    return HostDecision(host, found.recency, evidence)


def _pinned_recency(
    asset_class: AssetClass, *, listed: bool, fetch: Callable[[SnapshotProbe], SnapshotAnswer]
) -> _PinnedRecency:
    """Return a pinned host's recency, from the settings or from Massive's snapshot.

    Args:
        asset_class: The class of the stream.
        listed: Whether the class's subscription is in `delayed_subscriptions`, so the user
            asked for Massive's delayed data (the configuration has checked that the pin is a
            `delayed.` host).
        fetch: Sends the class's snapshot request.

    Returns:
        `DELAYED` for a listed subscription, with no request; `UNKNOWN` for forex and crypto,
        whose snapshots carry no recency label, with no request; otherwise what the class's
        snapshot (`SNAPSHOT_PROBES`) shows (`_pinned_labels`), or `UNKNOWN` when Massive
        refuses it or its read fails at Massive or on the network.

    Raises:
        MassiveError: If the read fails otherwise (`_answered_or_unreachable`): a closed
            session, a library defect, or a rate-limit pause the caller gave up on.
    """
    if listed:
        return _PinnedRecency(Recency.DELAYED, "MassiveConfig.delayed_subscriptions")
    if asset_class in (AssetClass.FOREX, AssetClass.CRYPTO):
        return _PinnedRecency(Recency.UNKNOWN, f"no {asset_class} snapshot labels recency")
    probe = SNAPSHOT_PROBES[asset_class]
    try:
        answer = fetch(probe)
    except EntitlementError as error:
        return _PinnedRecency(
            Recency.UNKNOWN, _REFUSED_SNAPSHOT.format(probe.path, error), warn=True
        )
    except MassiveError as error:
        if not _answered_or_unreachable(error):
            raise
        # Unlike the unpinned path, which propagates this error: the host is already chosen and
        # the snapshot only labels it, and a pinned stream never needed REST before, so a
        # failed read leaves the recency unknown, shown with the error, instead of failing it.
        return _PinnedRecency(
            Recency.UNKNOWN, _FAILED_SNAPSHOT.format(probe.path, error), warn=True
        )
    labels = _labels(answer)
    if isinstance(labels, str):
        return _PinnedRecency(
            Recency.UNKNOWN, _REFUSED_SNAPSHOT.format(probe.path, labels), warn=True
        )
    return _pinned_labels(asset_class, labels)


def _answered_or_unreachable(error: MassiveError) -> bool:
    """Return whether a failed snapshot read failed at Massive or on the network.

    Such a failure leaves a pinned host's recency unknown. Any other one propagates: a closed
    session or another stop of the caller's own (`ErrorSource.USER`), a library defect
    (`ErrorSource.LIBRARY`), and a rate-limit pause the caller's patience could not cover (a
    429, which the client raises only then), which `SharedLookup` treats as the caller giving
    up, so the next caller decides again.

    Args:
        error: The read's error.

    Returns:
        True for a `MASSIVE` or `NETWORK` error other than a 429.
    """
    source = error.source
    if source is ErrorSource.MASSIVE or source is ErrorSource.NETWORK:
        return error.status != HTTPStatus.TOO_MANY_REQUESTS
    if source is ErrorSource.USER or source is ErrorSource.LIBRARY:
        return False
    assert_never(source)  # a new source decides here whether it leaves the recency unknown


def _pinned_labels(asset_class: AssetClass, labels: RecencyLabels) -> _PinnedRecency:
    """Return the recency a snapshot's labels give a pinned host.

    Args:
        asset_class: The class, for the evidence of a snapshot without a label.
        labels: The snapshot's labels.

    Returns:
        `DELAYED` for any delayed label; `UNKNOWN` for an undocumented label, even beside a
        `REAL-TIME` one; `REALTIME` for a `REAL-TIME` label alone; `UNKNOWN` for no label.
    """
    if labels.delayed is not None:
        return _PinnedRecency(Recency.DELAYED, labels.delayed)
    if labels.unknown is not None:
        field, value = labels.unknown
        return _PinnedRecency(
            Recency.UNKNOWN,
            f"{field}={value!r}, neither DELAYED nor REAL-TIME, in the snapshot",
            warn=True,
        )
    if labels.real_time is not None:
        return _PinnedRecency(Recency.REALTIME, labels.real_time)
    return _PinnedRecency(Recency.UNKNOWN, _NO_TIMEFRAME.format(asset_class))


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
