"""The subscription protocol of one WebSocket connection: topics and the reconcile loop.

A WebSocket stream wants *topics*: the channel's wildcard (`T.*`) for `dm.ALL`, or one topic
per symbol in the wire's spelling (`T.AAPL`, `C.EUR-USD`). Topics are exact strings: `T.*` and
`T.AAPL` are unrelated, and nothing matches one topic as a prefix of another. This module holds
the topic helpers and `Subscriptions`, the state machine that turns the topics a connection's
streams want into subscribe and unsubscribe frames, one step at a time, and keeps track of what
the server acknowledged and refused. `_ingest/ws.py` owns the connections that run it.

It makes no I/O and no Deephaven call, and holds no lock: a `Subscriptions` lives on its
connection's loop thread. It reaches the client only through the client's sending methods and
the loop's `call_later`, both passed in, so it is tested with a recording sender and a manual
timer queue.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING, Final, Protocol

from deephaven_massive._datasets.symbols import ws_symbol
from deephaven_massive._massive import StatusKind

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven_massive._enums import AssetClass
    from deephaven_massive._massive import Status

__all__ = [
    "Sender",
    "Subscriptions",
    "Timer",
    "channel_of",
    "is_wildcard",
    "stream_topics",
    "wildcard",
]

logger = logging.getLogger(__name__)

_WILDCARD: Final = "*"


def wildcard(channel: str) -> str:
    """Return a channel's wildcard topic, such as `T.*`.

    Args:
        channel: The channel.

    Returns:
        The topic.
    """
    return f"{channel}.{_WILDCARD}"


def is_wildcard(topic: str) -> bool:
    """Tell whether a topic is its channel's wildcard.

    Args:
        topic: An exact topic.

    Returns:
        True for `<channel>.*`.
    """
    return topic.partition(".")[2] == _WILDCARD


def channel_of(topic: str) -> str:
    """Return the channel of a topic.

    Args:
        topic: An exact topic, such as `T.AAPL`.

    Returns:
        The part before the first dot, such as `T`.
    """
    return topic.partition(".")[0]


def stream_topics(
    asset_class: AssetClass,
    channel: str,
    symbols: Iterable[str],
    *,
    pairs: Mapping[str, tuple[str, str]],
) -> frozenset[str]:
    """Return the topics of canonical symbols on a channel, in the wire's spelling.

    Args:
        asset_class: The symbols' class, whose WebSocket spelling the topics use
            (`ws_symbol`: `C:EURUSD` is `EUR-USD`, `X:BTCUSD` is `BTC-USD`).
        channel: The channel, such as `"T"`.
        symbols: Canonical symbols.
        pairs: The base and quote of each crypto symbol, from Massive's crypto ticker list;
            empty for a class other than crypto.

    Returns:
        The topics, such as `{"T.AAPL", "T.MSFT"}`.

    A crypto symbol not in `pairs` raises the `KeyError` of `ws_symbol`.
    """
    return frozenset(f"{channel}.{ws_symbol(asset_class, s, pairs)}" for s in symbols)


class Sender(Protocol):
    """What `Subscriptions` needs of a client: the loop-thread sending methods of `WsClient`."""

    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send subscribe frames for `topics` in the generation."""
        ...

    def unsubscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send unsubscribe frames for `topics` in the generation."""
        ...

    def reconnect(self) -> None:
        """Close the current socket so the client reconnects under its backoff rules."""
        ...


class Timer(Protocol):
    """A scheduled call that can be cancelled (`asyncio.TimerHandle`)."""

    def cancel(self) -> None:
        """Cancel the call if it has not run."""
        ...


class Subscriptions:
    """The subscription state of one connection, and the reconcile loop that drives it.

    Confined to the connection's loop thread: every method is called there, so it needs no
    lock. It keeps, per topic, how many streams want it (`add`, `remove`) and, per generation
    (one authenticated socket), what was sent, what the server acknowledged, and which topics
    reached `NO_ACK`. Its terms:

    - the *desired* set D: the topics some stream wants, minus refused ones and the options
      contracts deferred by the cap;
    - the *wire* set W: D, minus each channel's explicit topics while that channel's
      wildcard is in D and acknowledged, plus each wildcard that has left D but is still
      subscribed while an explicit topic of its channel in D is unresolved (neither
      acknowledged, refused, nor `NO_ACK`): transitions make before they break;
    - a *step*: one subscribe call for the topics in W but not sent, followed by one
      unsubscribe call for the topics sent but not in W. Only one step is in flight: from its
      send until each of its topics is acknowledged, refused, or `NO_ACK`. Unsubscriptions
      are not awaited.

    **The options cap.** With a `cap`, at most that many explicit topics are admitted to the
    wire; the rest are deferred, not failed. Topics of a static symbol list (`add(...,
    static=True)`) are admitted first, since their verb already checked them against the cap;
    a reactive table's topics then take the room left, the ones admitted before keeping their
    place and the new ones entering in sorted order. The admitted set outlives a generation,
    so a reconnect never reshuffles it. Every wanted explicit topic counts, also one covered by
    an acknowledged wildcard of its channel and so not on the wire meanwhile: the wildcard may
    go at any time, and a static contract must never be deferred then.

    Each subscribed topic has a watchdog bound to its generation. After `ack_timeout` without
    an acknowledgement it re-sends the topic alone; after another it marks the topic `NO_ACK`,
    logs at ERROR, and asks the client to reconnect if `first_no_ack(topic)` says this is the
    topic's first `NO_ACK` (the session keeps that record, so it survives this connection). A
    watchdog whose topic has left the wire or whose generation has ended does nothing more.

    Created by `Connection`, which passes the client, its loop's `call_later`, and its
    callbacks: `on_refused(topics, message)` for topics the server refused,
    `on_step_refused(status)` for a refusal naming no topic of a step none of whose topics was
    resolved (returning True when the connection handles it as the server's real-time
    verdict), `first_no_ack(topic)`, and `on_change()` after every change the connection's
    diagnostics row shows.

    Example:
        ::

            subscriptions = Subscriptions(
                client,
                call_later=client.loop.call_later,
                ack_timeout=5.0,
                cap=None,
                on_refused=lambda topics, message: None,
                on_step_refused=lambda status: False,
                first_no_ack=lambda topic: True,
                on_change=lambda: None,
            )
            subscriptions.add(["T.AAPL"])
            subscriptions.opened(1)  # subscribes T.AAPL
    """

    def __init__(
        self,
        sender: Sender,
        *,
        call_later: Callable[..., Timer],
        ack_timeout: float,
        cap: int | None,
        on_refused: Callable[[tuple[str, ...], str], None],
        on_step_refused: Callable[[Status], bool],
        first_no_ack: Callable[[str], bool],
        on_change: Callable[[], None],
    ) -> None:
        """Start with no topic wanted and no generation open.

        Args:
            sender: The client, for its sending methods.
            call_later: Schedules a watchdog: `call_later(delay, callback, *args)` returning a
                cancellable handle (the loop's `call_later`).
            ack_timeout: Seconds to wait for each acknowledgement (`MassiveConfig.ack_timeout`).
            cap: The most explicit topics on the wire (1,000 for `/options`), or None.
            on_refused: Called with refused topics and the server's message.
            on_step_refused: Called for a refusal of a whole step that names no topic.
            first_no_ack: Records a topic's `NO_ACK` for the session and tells whether it was
                the topic's first.
            on_change: Called after every change of the counts or the deferred topics.
        """
        self._sender = sender
        self._call_later = call_later
        self._ack_timeout = ack_timeout
        self._cap = cap
        self._on_refused = on_refused
        self._on_step_refused = on_step_refused
        self._first_no_ack = first_no_ack
        self._on_change = on_change
        self.generation: int | None = None
        self._refs: Counter[str] = Counter()
        self._static: Counter[str] = Counter()
        self._refused: set[str] = set()
        self._admitted: frozenset[str] = frozenset()
        self._deferred: frozenset[str] = frozenset()
        self._sent: set[str] = set()
        self._acked: set[str] = set()
        self._noack: set[str] = set()
        self._step: frozenset[str] = frozenset()
        self._pending: set[str] = set()
        self._watchdogs: dict[str, Timer] = {}
        self._warned_unparsed = False

    # --- demand --------------------------------------------------------------------------------

    def add(self, topics: Iterable[str], *, static: bool = False) -> None:
        """Count one more stream wanting each topic. A topic refused before is tried again.

        Call `reconcile` afterwards to send what changed.

        Args:
            topics: Exact topics.
            static: True for the topics of a static symbol list, which the options cap admits
                first.
        """
        for topic in topics:
            self._refs[topic] += 1
            if static:
                self._static[topic] += 1
            self._refused.discard(topic)

    def remove(self, topics: Iterable[str], *, static: bool = False) -> None:
        """Count one stream fewer wanting each topic; a topic nobody wants leaves D.

        Its watchdog is cancelled; its unsubscription is sent by the next step.

        Args:
            topics: Exact topics, each added before with the same `static`.
            static: As given to `add`.
        """
        for topic in topics:
            if static:
                self._static[topic] -= 1
                if not self._static[topic]:
                    del self._static[topic]
            count = self._refs[topic] - 1
            if count > 0:
                self._refs[topic] = count
                continue
            del self._refs[topic]
            self._refused.discard(topic)
            self._pending.discard(topic)
            self._cancel(topic)

    @property
    def wanted(self) -> bool:
        """True while any stream wants any topic."""
        return bool(self._refs)

    @property
    def admitted(self) -> frozenset[str]:
        """The explicit topics the options cap lets on the wire, as of the last reconcile."""
        return self._admitted

    @property
    def deferred(self) -> frozenset[str]:
        """The options contracts the cap keeps off the wire, as of the last reconcile."""
        return self._deferred

    @property
    def explicit(self) -> int:
        """How many explicit topics streams want and the server has not refused."""
        return sum(1 for t in self._refs if not is_wildcard(t) and t not in self._refused)

    def counts(self) -> tuple[int, int, int, int]:
        """Return the counts a connection's diagnostics row shows.

        Returns:
            `(topics, acked, pending, no_ack)`: the size of D, the topics acknowledged in the
            current generation, the topics sent and awaiting acknowledgement, and the topics
            in `NO_ACK`.
        """
        pending = len(self._sent - self._acked - self._noack)
        return len(self._desired()), len(self._acked), pending, len(self._noack)

    # --- generations ---------------------------------------------------------------------------

    def opened(self, generation: int) -> None:
        """Start a generation: nothing is subscribed on the new socket, so send all of W.

        Args:
            generation: The new generation's number, from `on_open`.
        """
        self.ended()
        self.generation = generation
        self.reconcile()

    def ended(self) -> None:
        """End the current generation: its watchdogs are cancelled and its state forgotten."""
        for timer in self._watchdogs.values():
            timer.cancel()
        self._watchdogs.clear()
        self.generation = None
        self._sent.clear()
        self._acked.clear()
        self._noack.clear()
        self._pending.clear()
        self._step = frozenset()

    # --- the reconcile loop --------------------------------------------------------------------

    def reconcile(self) -> None:
        """Apply the cap, then send the next step if a generation is open and none is in flight.

        The cap is applied even with no generation open, so `admitted` always counts the
        topics the streams have asked for.
        """
        self._defer()
        if self.generation is None or self._pending:
            return
        wire = self._wire()
        adds, removes = wire - self._sent, self._sent - wire
        generation = self.generation
        if adds:
            ordered = sorted(adds)
            self._sender.subscribe(generation, ordered)
            self._sent |= adds
            self._step = frozenset(adds)
            self._pending = set(adds)
            for topic in ordered:
                self._arm(topic, generation, attempt=1)
        if removes:
            self._sender.unsubscribe(generation, sorted(removes))
            self._sent -= removes
            self._acked -= removes
            self._noack -= removes
            for topic in removes:
                self._cancel(topic)
        self._on_change()

    def status(self, status: Status) -> None:
        """Apply one status object of the current generation, then reconcile.

        An acknowledgement resolves its topics (one that could not be parsed resolves the whole
        step in flight, with a WARNING once); a refusal naming topics refuses them; a refusal
        naming none applies to every unresolved topic of the step in flight. Other statuses
        change nothing.

        Args:
            status: The decoded status.
        """
        if status.kind is StatusKind.ACK:
            self._acknowledge(status.topics)
        elif status.kind is StatusKind.REFUSED and not self._refuse_status(status):
            return
        self.reconcile()

    def _acknowledge(self, topics: tuple[str, ...]) -> None:
        """Mark topics acknowledged in the current generation.

        Args:
            topics: The topics the status names; empty when its message could not be parsed,
                which acknowledges the whole step in flight.
        """
        if not topics:
            if not self._warned_unparsed:
                self._warned_unparsed = True
                logger.warning(
                    "an acknowledgement named no topic; treating it as acknowledging the "
                    "whole subscription in flight"
                )
            topics = tuple(self._pending)
        for topic in topics:
            if topic in self._sent:
                self._acked.add(topic)
                self._noack.discard(topic)
                self._pending.discard(topic)
                self._cancel(topic)

    def _refuse_status(self, status: Status) -> bool:
        """Apply a refusal.

        Args:
            status: A `REFUSED` status.

        Returns:
            False when the connection took the refusal as the server's real-time verdict and is
            moving to another host, so nothing more happens here; True otherwise.
        """
        if status.topics:
            self._refuse(tuple(t for t in status.topics if t in self._refs), status.message)
            return True
        if not self._pending:
            logger.warning(
                "a refusal named no topic and no subscription was in flight: %s", status.message
            )
            return True
        whole = self._pending == self._step
        if whole and self._on_step_refused(status):
            return False
        self._refuse(tuple(sorted(self._pending)), status.message)
        return True

    def _refuse(self, topics: tuple[str, ...], message: str) -> None:
        """Take refused topics out of D and report them.

        Args:
            topics: The refused topics that streams want.
            message: The server's message.
        """
        for topic in topics:
            self._refused.add(topic)
            self._sent.discard(topic)
            self._acked.discard(topic)
            self._noack.discard(topic)
            self._pending.discard(topic)
            self._cancel(topic)
        if topics:
            self._on_refused(topics, message)

    def _desired(self) -> set[str]:
        """Return D: the wanted topics, minus refused ones and deferred options contracts.

        Returns:
            The desired topics.
        """
        return {t for t in self._refs if t not in self._refused and t not in self._deferred}

    def _defer(self) -> None:
        """Recompute the explicit topics the cap admits and those it defers (see the class)."""
        if self._cap is None:
            return
        explicit = sorted(t for t in self._refs if not is_wildcard(t) and t not in self._refused)
        static = [t for t in explicit if t in self._static][: self._cap]
        room = self._cap - len(static)
        kept = [t for t in explicit if t in self._admitted and t not in self._static][:room]
        room -= len(kept)
        new = [t for t in explicit if t not in self._admitted and t not in self._static][:room]
        self._admitted = frozenset((*static, *kept, *new))
        self._deferred = frozenset(explicit) - self._admitted

    def _wire(self) -> set[str]:
        """Return W, the topics that should be subscribed now (see the class documentation).

        Returns:
            The topics.
        """
        desired = self._desired()
        covered = {channel_of(t) for t in desired if is_wildcard(t) and t in self._acked}
        wire = {t for t in desired if is_wildcard(t) or channel_of(t) not in covered}
        unresolved = {
            channel_of(t)
            for t in desired
            if not is_wildcard(t) and t not in self._acked and t not in self._noack
        }
        wire.update(
            t
            for t in self._sent
            if is_wildcard(t) and t not in desired and channel_of(t) in unresolved
        )
        return wire

    # --- watchdogs -----------------------------------------------------------------------------

    def _arm(self, topic: str, generation: int, *, attempt: int) -> None:
        """Start a topic's watchdog.

        Args:
            topic: The topic just sent.
            generation: The generation it was sent in.
            attempt: 1 after the step's send, 2 after the single-topic re-send.
        """
        self._cancel(topic)
        self._watchdogs[topic] = self._call_later(
            self._ack_timeout, self._fire, topic, generation, attempt
        )

    def _cancel(self, topic: str) -> None:
        """Cancel a topic's watchdog, if it has one.

        Args:
            topic: The topic.
        """
        timer = self._watchdogs.pop(topic, None)
        if timer is not None:
            timer.cancel()

    def _fire(self, topic: str, generation: int, attempt: int) -> None:
        """A topic's acknowledgement is overdue: re-send it once, then mark it `NO_ACK`.

        Args:
            topic: The topic.
            generation: The generation the watchdog is bound to.
            attempt: 1 for the first timeout, 2 for the second.
        """
        self._watchdogs.pop(topic, None)
        if generation != self.generation or topic not in self._sent or topic not in self._wire():
            self._pending.discard(topic)  # it left the wire: nothing to escalate
            self.reconcile()
            return
        if attempt == 1:
            self._sender.subscribe(generation, [topic])
            self._arm(topic, generation, attempt=2)
            return
        self._pending.discard(topic)
        self._noack.add(topic)
        first = self._first_no_ack(topic)
        logger.error(
            "no acknowledgement for %s after a re-send; %s",
            topic,
            "reconnecting" if first else "it stays wanted (it already caused a reconnect)",
        )
        if first:
            self._sender.reconnect()
        self.reconcile()
