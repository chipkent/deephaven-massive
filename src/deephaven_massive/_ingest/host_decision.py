"""Which WebSocket host serves an asset class and channel family, decided from REST evidence.

Massive serves each WebSocket cluster (`/stocks`, `/options`, ...) from several hosts:
`socket.massive.com` for real-time data, `delayed.massive.com` for 15-minute delayed data, and
`business.massive.com` for the Business plans' fair-market-value (FMV) channels. Which one a key
may use depends on its plan, which no API states outright. This module decides it once per
(asset class, channel family), before the first connection, from configuration or from one REST
snapshot request, and never by opening a connection to try:

- the FMV family is always `business.massive.com` (FMV is Business-only);
- a TICK host pinned in `MassiveConfig.ws_hosts` is used as is;
- forex and crypto have one documented host, `socket.massive.com`, with real-time data on every
  plan, so no request is made;
- stocks, options, indices, and futures read one snapshot (`SNAPSHOT_PROBES`) and classify the
  `timeframe` fields of its rows (`TIMEFRAME_FIELDS`): any `DELAYED` routes to the delayed host,
  else any `REAL-TIME` to the real-time host. When no row carries a timeframe, the class's own
  fallback applies (see `decide`).

When the snapshot gives no usable answer (Massive refuses it, or a timeframe holds a value other
than `DELAYED` or `REAL-TIME`), nothing is guessed from it: the class streams from the
real-time host, with `REALTIME` recency since that is the host's data, and a WARNING names the
evidence. Such a connection is never moved to the delayed host: if Massive refuses its topics,
their streams fail with Massive's text. No stream is ever sent to delayed data by inference,
and a plan the snapshot misjudges still gets every stream it is entitled to.

`decide` is pure apart from logging: the REST request itself is made by the caller's `fetch`
callable, which the REST transport (`_ingest/rest.py`) supplies, so every HTTP error has
already become its public exception there. `HostDecisionCache` holds a session's decisions,
single-flight, behind a leaf lock of its own; the REST transport fills it and writes the
`m.connections()` rows that record them. Nothing here starts a thread or calls Deephaven.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeVar

from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._errors import EntitlementError
from deephaven_massive._ingest.rest_rows import is_error_row
from deephaven_massive._log_limits import log_once
from deephaven_massive._tables.transports import HostDecision

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

__all__ = [
    "BUSINESS_HOST",
    "DELAYED_HOST",
    "REALTIME_HOST",
    "SNAPSHOT_PROBES",
    "TIMEFRAME_FIELDS",
    "HostDecisionCache",
    "SnapshotProbe",
    "decide",
    "succeeded_result",
]

logger = logging.getLogger(__name__)

REALTIME_HOST: Final = "socket.massive.com"
"""The host of real-time data, and the one documented host of forex and crypto."""

DELAYED_HOST: Final = "delayed.massive.com"
"""The host of 15-minute delayed data."""

BUSINESS_HOST: Final = "business.massive.com"
"""The host of the Business plans' fair-market-value channels."""

_DELAYED: Final = "DELAYED"
_REAL_TIME: Final = "REAL-TIME"


@dataclass(frozen=True, slots=True)
class SnapshotProbe:
    """The one REST request whose rows show an asset class's data recency.

    Attributes:
        path: The generated REST path template, such as `"/v3/snapshot"`.
        path_params: A value for each placeholder of the template.
        query: The query parameters, by wire name.
    """

    path: str
    path_params: Mapping[str, object]
    query: Mapping[str, object]


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
"""The snapshot each class's TICK host is decided from: the unified snapshot of two liquid
stocks, one contract of the SPY option chain, the S&P 500 index, and one E-mini S&P contract."""

TIMEFRAME_FIELDS: Final[Mapping[AssetClass, tuple[str, ...]]] = {
    AssetClass.STOCKS: ("last_trade.timeframe", "last_quote.timeframe"),
    AssetClass.OPTIONS: ("last_trade.timeframe", "last_quote.timeframe"),
    AssetClass.INDICES: ("timeframe",),
    AssetClass.FUTURES: (
        "last_trade.timeframe",
        "last_quote.timeframe",
        "last_minute.timeframe",
    ),
}
"""The dotted row fields, in reading order, whose `DELAYED` or `REAL-TIME` value states the
recency of a class's data. `underlying_asset.timeframe` is never read: the option-chain sample
shows the underlying delayed beside a real-time contract."""


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


def _ws_hosts_hint(asset_class: AssetClass) -> str:
    """Return the configuration that pins the class's host, for error messages.

    Args:
        asset_class: The class.

    Returns:
        For example `MassiveConfig(ws_hosts={dm.AssetClass.STOCKS: "<host>"})`.
    """
    return f'MassiveConfig(ws_hosts={{dm.AssetClass.{asset_class.name}: "<host>"}})'


def _classify(asset_class: AssetClass, rows: Sequence[Mapping[str, object]]) -> HostDecision | None:
    """Decide from the rows' timeframe fields, or return None when no row carries one.

    `DELAYED` wins a conflict, since routing a delayed plan to the real-time host fails every
    subscription, while the reverse only delays data. Without a `DELAYED`, a value other than
    `DELAYED` or `REAL-TIME` routes to the real-time host with `REALTIME` recency and a WARNING
    naming the field and the value: the library does not know what the value means, so it
    serves the real-time host's data and lets a refusal from Massive fail the refused streams
    (see the module docstring).

    Args:
        asset_class: The class whose fields are read.
        rows: The snapshot's rows, without the per-ticker error rows.

    Returns:
        The decision, or None when no timeframe was found.
    """
    delayed: str | None = None
    real_time: str | None = None
    unknown: tuple[str, object] | None = None
    for row in rows:
        for field in TIMEFRAME_FIELDS[asset_class]:
            value = _value(row, field)
            if value is None:
                continue
            if value == _DELAYED:
                delayed = delayed or field
            elif value == _REAL_TIME:
                real_time = real_time or field
            else:
                unknown = unknown or (field, value)
    if delayed is not None:
        logger.info("%s WebSocket host: %s=DELAYED", asset_class.value, delayed)
        return HostDecision(DELAYED_HOST, Recency.DELAYED, f"{delayed}=DELAYED")
    if unknown is not None:
        field, value = unknown
        return _unconfirmed_real_time(
            asset_class, f"{field}={value!r}, neither DELAYED nor REAL-TIME, in the snapshot"
        )
    if real_time is not None:
        logger.info("%s WebSocket host: %s=REAL-TIME", asset_class.value, real_time)
        return HostDecision(REALTIME_HOST, Recency.REALTIME, f"{real_time}=REAL-TIME")
    return None


def _unconfirmed_real_time(asset_class: AssetClass, evidence: str) -> HostDecision:
    """Route a class to the real-time host with `REALTIME` recency, logging one WARNING.

    Used when the snapshot gives no usable answer. The label is the host's: the connection is
    never moved to delayed data, and a refusal from Massive fails the refused streams with its
    text.

    Args:
        asset_class: The class.
        evidence: What the snapshot showed, for the log and the `m.connections()` row.

    Returns:
        The decision: `socket.massive.com`, `REALTIME`, with the evidence.
    """
    logger.warning(
        "%s WebSocket host: %s; streaming real-time data from %s, and a refusal from Massive "
        "fails the refused streams with its text (pin another host with %s)",
        asset_class.value,
        evidence,
        REALTIME_HOST,
        _ws_hosts_hint(asset_class),
    )
    return HostDecision(REALTIME_HOST, Recency.REALTIME, evidence)


def _without_timeframe(
    asset_class: AssetClass, rows: Sequence[Mapping[str, object]]
) -> HostDecision:
    """Decide for a snapshot whose rows carry no timeframe at all.

    Args:
        asset_class: The class (stocks, options, indices, or futures).
        rows: The snapshot's rows, without the per-ticker error rows.

    Returns:
        Stocks and options with a top-level `fmv` field (a Business key without a trades and
        quotes expansion): the Business host, logged once at WARNING. Other stocks and options:
        the delayed host, since `last_trade` and `last_quote` appear only on plans that include
        trades and quotes (logged at INFO). Indices: the delayed host with `UNKNOWN` recency and
        a WARNING, since the field is documented and its absence is anomalous. Futures: the
        real-time host with `UNKNOWN` recency, since the futures documentation lists no
        timeframe and its absence proves nothing; an explicit server verdict may still re-route
        the connection.
    """
    if asset_class in (AssetClass.STOCKS, AssetClass.OPTIONS):
        if any("fmv" in row for row in rows):
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
        logger.info(
            "%s WebSocket host: no trades or quotes in the snapshot; using %s",
            asset_class.value,
            DELAYED_HOST,
        )
        return HostDecision(
            DELAYED_HOST,
            Recency.DELAYED,
            "no trades/quotes in snapshot => below Developer (plan tables)",
        )
    if asset_class is AssetClass.INDICES:
        logger.warning(
            "indices WebSocket host: the snapshot has no timeframe; using %s (pin another with %s)",
            DELAYED_HOST,
            _ws_hosts_hint(asset_class),
        )
        return HostDecision(DELAYED_HOST, Recency.UNKNOWN, "no timeframe in indices snapshot")
    return HostDecision(REALTIME_HOST, Recency.UNKNOWN, "no timeframe in futures snapshot")


def decide(
    asset_class: AssetClass,
    family: ChannelFamily,
    *,
    ws_hosts: Mapping[AssetClass, str],
    fetch: Callable[[SnapshotProbe], Sequence[Mapping[str, object]]],
) -> HostDecision:
    """Decide the WebSocket host of an asset class and channel family.

    The rules, in order: the FMV family is `business.massive.com`; a TICK host pinned in
    `ws_hosts` is used with `UNKNOWN` recency; forex and crypto use `socket.massive.com`
    (real-time on every plan, no request); every other class reads its snapshot
    (`SNAPSHOT_PROBES`) once through `fetch`, drops the per-ticker rows that carry an `error`
    (a ticker the plan does not include counts as absent), and classifies the rest (`DELAYED`
    in any timeframe field wins, then `REAL-TIME`; see `_without_timeframe` when there is
    none).

    Runs on the calling thread; the only I/O is `fetch`, which blocks for one request.

    Args:
        asset_class: The class of the stream.
        family: Its channel family.
        ws_hosts: `MassiveConfig.ws_hosts`, the pinned TICK hosts.
        fetch: Makes the probe's request and returns its raw JSON rows. It raises the public
            errors of the REST transport: `AuthenticationError`, `EntitlementError`, or
            `MassiveError`.

    Returns:
        The decision.

    A refused snapshot (HTTP 403 or `NOT_AUTHORIZED`) and a timeframe that holds an
    undocumented value are not errors: the class streams from the real-time host with
    `REALTIME` recency, a WARNING names the evidence (Massive's text for a refusal), and a
    refusal from the WebSocket server fails the refused streams. A refused snapshot says
    nothing certain about the WebSocket plan, so no stream is refused on its account.

    Raises:
        AuthenticationError: If `fetch` raises it (a rejected key).
        MassiveError: If `fetch` fails otherwise (a transport failure, a server error).
    """
    if family is ChannelFamily.FMV:
        return HostDecision(
            BUSINESS_HOST, Recency.REALTIME, "FMV is Business-only; fixed host (docs)"
        )
    pinned = ws_hosts.get(asset_class)
    if pinned is not None:
        return HostDecision(pinned, Recency.UNKNOWN, "ws_hosts")
    if asset_class in (AssetClass.FOREX, AssetClass.CRYPTO):
        return HostDecision(
            REALTIME_HOST, Recency.REALTIME, "single documented host; Starter is real-time"
        )
    probe = SNAPSHOT_PROBES[asset_class]
    try:
        rows = fetch(probe)
    except EntitlementError as error:
        return _unconfirmed_real_time(
            asset_class, f"the host check's snapshot ({probe.path}) was refused: {error}"
        )
    entitled = [row for row in rows if not is_error_row(row)]
    decision = _classify(asset_class, entitled)
    return decision if decision is not None else _without_timeframe(asset_class, entitled)


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

    def replace_host(self, key: tuple[AssetClass, ChannelFamily], decision: HostDecision) -> None:
        """Cache a decision for a (class, family) in place of any earlier one.

        A caller already waiting on an earlier decision still gets that one; every later
        caller gets this one.

        Args:
            key: The asset class and channel family.
            decision: The new decision.
        """
        future: Future[HostDecision] = Future()
        future.set_result(decision)
        with self._lock:
            self._hosts[key] = future

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
