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
import re
from collections import Counter
from typing import TYPE_CHECKING, Final, Protocol, assert_never

from deephaven_massive._config import NO_ACK_RESEND
from deephaven_massive._datasets.wire_symbols import ws_symbol
from deephaven_massive._log_limits import bounded_list
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

_NO_ACK_ATTEMPT: Final = 2
"""The watchdog attempt that marks a topic `NO_ACK`; attempt 1 re-sends it once alone."""


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


def _named(message: str, topics: Iterable[str]) -> list[str]:
    """Return the topics a server message names whole, not as part of a longer one.

    Args:
        message: The server's message.
        topics: The candidate topics, such as `T.1Y:SA 03M` and `T.1Y:SA 03M F6`.

    Returns:
        The topics, sorted, that appear with no ticker character (a letter, a digit, `.`,
        `:`, or `-`) on either side, a sentence's closing `.` after one still counting, at a
        place no longer candidate covers: a topic may hold a space, so `T.1Y:SA 03M F6`
        names itself, not `T.1Y:SA 03M`, and `T.CL:BF Q6-U6-V6` does not name `T.CL:BF Q6-U6`.
    """
    before, after = r"[A-Za-z0-9:.\-]", r"[A-Za-z0-9:\-]|\.\w"
    found = [
        (match.span(), topic)
        for topic in sorted(topics)
        for match in re.finditer(rf"(?<!{before}){re.escape(topic)}(?!{after})", message)
    ]
    return sorted(
        {
            topic
            for (start, end), topic in found
            if not any(s <= start and end <= e and e - s > end - start for (s, e), _ in found)
        }
    )


class Sender(Protocol):
    """What `Subscriptions` needs of a client: the loop-thread sending methods of `WsClient`."""

    def subscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send subscribe frames for `topics` in the generation."""
        ...

    def unsubscribe(self, generation: int, topics: Sequence[str]) -> None:
        """Send unsubscribe frames for `topics` in the generation."""
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
    wire; the rest are deferred, not failed. The cap counts topics, so a contract's trades and
    quotes are two: Massive documents "1,000 option contracts" per connection, but whether its
    server counts contracts or topics is not verified, and the stricter count never crosses
    it. Topics of a static symbol list (`add(..., static=True)`) are admitted first, since their
    verb already checked them against the cap; a reactive table's topics then take the room
    left, the ones admitted before keeping their place and the new ones entering in sorted
    order. The admitted set outlives a generation, so a reconnect never reshuffles it. Every
    wanted explicit topic counts, also one covered by an acknowledged wildcard of its channel
    and so not on the wire meanwhile: the wildcard may go at any time, and a static contract
    must never be deferred then.

    Each subscribed topic has a watchdog bound to its generation. After
    `subscription_confirmation_timeout` without an acknowledgement it re-sends the topic alone;
    after another it marks the topic `NO_ACK` (the topic's data may be missing; `no_ack` lists them,
    and the connection shows them in its streams' `Error`). Once the step in flight has resolved,
    one WARNING per step counts its `NO_ACK` topics. The topic stays wanted and on the wire, and
    every `NO_ACK_RESEND` seconds one round re-sends all `NO_ACK` topics together in one subscribe
    frame, until each is acknowledged, leaves the wire, or the generation ends. A step and a round
    never await answers together: a round waits while a step is in flight, and a step waits while a
    round awaits its answer (for `subscription_confirmation_timeout`). An acknowledgement naming no
    topic is credited to the step in flight, else to the round awaiting its answer, else to the
    latest step's `NO_ACK` topics, whose answer came after their watchdogs gave up. A refusal naming
    no topic fails streams, so it is credited only when exactly one topic on the connection awaits
    an answer (`_refuse_status`); any other is logged and fails nothing. An acknowledgement that
    arrives later clears `NO_ACK`. The connection is never reconnected for it: a reconnect
    would cut every other stream on the connection over one late answer. A watchdog whose
    topic has left the wire or whose generation has ended does nothing more.

    Created by `Connection`, which passes the client, its loop's `call_later`, and its
    callbacks: `on_refused(topics, message)` for topics the server refused, and `on_change()`
    after every change the connection's diagnostics row shows.

    Example:
        ::

            subscriptions = Subscriptions(
                client,
                call_later=client.loop.call_later,
                subscription_confirmation_timeout=5.0,
                cap=None,
                on_refused=lambda topics, message: None,
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
        subscription_confirmation_timeout: float,
        cap: int | None,
        on_refused: Callable[[tuple[str, ...], str], None],
        on_change: Callable[[], None],
    ) -> None:
        """Start with no topic wanted and no generation open.

        Args:
            sender: The client, for its sending methods.
            call_later: Schedules a watchdog: `call_later(delay, callback, *args)` returning a
                cancellable handle (the loop's `call_later`).
            subscription_confirmation_timeout: Seconds to wait for each acknowledgement
                (`MassiveConfig.subscription_confirmation_timeout`).
            cap: The most explicit topics on the wire (1,000 for `/options`), or None.
            on_refused: Called with refused topics and the server's message.
            on_change: Called after every change of the counts or the deferred topics.
        """
        self._sender = sender
        self._call_later = call_later
        self._subscription_confirmation_timeout = subscription_confirmation_timeout
        self._cap = cap
        self._on_refused = on_refused
        self._on_change = on_change
        self.generation: int | None = None
        self._refs: Counter[str] = Counter()
        self._static: Counter[str] = Counter()
        self._refused: set[str] = set()
        self._admitted: frozenset[str] = frozenset()
        self._deferred: frozenset[str] = frozenset()
        self._admission_stale = False
        self._sent: set[str] = set()
        self._acked: set[str] = set()
        self._noack: set[str] = set()
        self._unreported: list[str] = []  # `NO_ACK` topics of the step not logged yet
        self._pending: set[str] = set()
        # The topics of the latest step, which a late acknowledgement naming no topic is
        # credited to once the step has resolved, until a re-send goes out; the `NO_ACK` topics
        # of the re-send that awaits an answer, with the timer that ends the wait after
        # `subscription_confirmation_timeout`; and the timer of the next round of re-sends.
        self._step: frozenset[str] = frozenset()
        self._resend: frozenset[str] = frozenset()
        self._resend_timer: Timer | None = None
        self._round: Timer | None = None
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
        self._admission_stale = True

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
            self._resend_resolved(topic)
        self._admission_stale = True

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
        """The options topics the cap keeps off the wire, as of the last reconcile."""
        return self._deferred

    @property
    def no_ack(self) -> frozenset[str]:
        """The topics in `NO_ACK` in the current generation: re-sent, still unacknowledged."""
        return frozenset(self._noack)

    @property
    def acknowledged(self) -> frozenset[str]:
        """The topics acknowledged in the current generation, for the connection's summary."""
        return frozenset(self._acked)

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
        self._unreported.clear()
        self._pending.clear()
        self._step = frozenset()
        self._resend_resolved(*self._resend)
        if self._round is not None:
            self._round.cancel()
            self._round = None

    # --- the reconcile loop --------------------------------------------------------------------

    def reconcile(self) -> None:
        """Apply the cap, send the next step if a generation is open and none is in flight.

        The cap is applied even with no generation open, so `admitted` always counts the
        topics the streams have asked for. It is recomputed only after a change of what the
        streams want or of what the server refused: an acknowledgement or a `NO_ACK` changes
        neither, and a step of thousands of topics brings one acknowledgement per topic, so
        sorting every topic per acknowledgement would stall the loop for seconds.

        `on_change` is called every time, also while a step is in flight, so the connection's
        row shows each partial acknowledgement, refusal, and `NO_ACK` as it happens rather
        than when the whole step resolves.
        """
        if self._admission_stale:
            self._admission_stale = False
            self._defer()
        if self._unreported and not self._pending:
            self._report_no_ack()
        if self.generation is not None and not self._pending and not self._resend:
            self._send_step(self.generation)
        self._on_change()

    def _send_step(self, generation: int) -> None:
        """Send one step: subscribe what W adds to the sent topics, unsubscribe what it drops.

        Called by `reconcile` with no step in flight.

        Args:
            generation: The open generation.
        """
        wire = self._wire()
        adds, removes = wire - self._sent, self._sent - wire
        if (adds or removes) and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "generation %d: subscribe %s, unsubscribe %s (desired %d, acknowledged %d, "
                "deferred by the options cap %d)",
                generation,
                sorted(adds),
                sorted(removes),
                len(self._refs),
                len(self._acked - removes),
                len(self._deferred),
            )
        if adds:
            ordered = sorted(adds)
            self._sender.subscribe(generation, ordered)
            self._sent |= adds
            self._pending = set(adds)
            self._step = frozenset(adds)
            for topic in ordered:
                self._arm(topic, generation, attempt=1)
        if removes:
            self._sender.unsubscribe(generation, sorted(removes))
            self._sent -= removes
            self._acked -= removes
            self._noack -= removes
            for topic in removes:
                self._cancel(topic)

    def status(self, status: Status) -> None:
        """Apply one status object of the current generation, then reconcile.

        An acknowledgement resolves its topics; a refusal naming topics refuses them. An
        acknowledgement naming no topic (its message could not be parsed; a WARNING once) is
        credited to the topics it most likely answers (`_unanswered`); a refusal naming none
        only to topics it surely answers (`_refuse_status`). Other statuses change nothing.

        Args:
            status: The decoded status.
        """
        if status.kind is StatusKind.ACK:
            self._acknowledge(status.topics)
        elif status.kind is StatusKind.REFUSED:
            self._refuse_status(status)
        elif status.kind is StatusKind.MAX_CONNECTIONS or status.kind is StatusKind.OTHER:
            pass  # the client turns a lost slot into its TAKEN state; any other answers no topic
        else:
            assert_never(status.kind)  # a new kind needs its rule here
        self.reconcile()

    def _acknowledge(self, topics: tuple[str, ...]) -> None:
        """Mark topics acknowledged in the current generation.

        Topics in `NO_ACK` that are acknowledged now, late, are logged at INFO, the recovery
        of the WARNING `_report_no_ack` logged for them; one whose WARNING is still waiting for
        its step to resolve leaves that WARNING instead, so it is never reported as missing.

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
            topics = self._unanswered()
        late = []  # topics in `NO_ACK` that Massive acknowledges after all
        for topic in topics:
            if topic in self._sent:
                if topic in self._unreported:  # its WARNING was not logged yet: none is due
                    self._unreported.remove(topic)
                elif topic in self._noack:
                    late.append(topic)
                self._acked.add(topic)
                self._noack.discard(topic)
                self._pending.discard(topic)
                self._cancel(topic)
                self._resend_resolved(topic)
        if late:
            logger.info(
                "generation %d: Massive acknowledged %s after all; they leave NoAckTopics",
                self.generation,
                bounded_list(sorted(late)),
            )

    def _refuse_status(self, status: Status) -> None:
        """Apply a refusal: of the topics it names, or else of the one topic it surely answers.

        A refusal fails streams, so one naming no topic is credited only where it cannot be
        wrong: when exactly one topic on the connection awaits an answer, a single unresolved
        topic of the step in flight, with no topic in `NO_ACK` and no re-send round
        outstanding. Any other is logged at INFO and fails nothing: a late answer to an old
        topic could otherwise fail a healthy stream. The unanswered topics keep their notes,
        and a stream with rows still fails at `max_outage` after a reconnect, so nothing stale
        goes unmarked.

        A wanted topic with a space, such as a futures combo's `T.CL:BF Q6-U6-V6`, is never one
        word of the message, so it counts as named when the message holds it whole and no
        longer wanted topic holds that place.

        Args:
            status: A `REFUSED` status.
        """
        spaced = _named(status.message, (t for t in self._refs if " " in t))
        if status.topics or spaced:
            named = [t for t in status.topics if t in self._refs]
            self._refuse(tuple(dict.fromkeys([*named, *spaced])), status.message)
            return
        awaiting = len(self._pending) + len(self._noack) + len(self._resend)
        if len(self._pending) != 1 or awaiting != 1:
            logger.info(
                "a refusal named no topic while %d topics await an answer (%d in flight, %d "
                "never acknowledged, %d re-sent); it fails nothing: %s",
                awaiting,
                len(self._pending),
                len(self._noack),
                len(self._resend),
                status.message,
            )
            return
        self._refuse(tuple(self._pending), status.message)

    def _unanswered(self) -> tuple[str, ...]:
        """Return the topics an acknowledgement naming no topic is credited to, sorted.

        Answers come in the order of the frames they answer, and a step and a re-send never
        await answers together, so such an acknowledgement is for, in turn: the step in flight
        (its unresolved topics); else the `NO_ACK` topics of the re-send awaiting its answer;
        else the latest step's topics now in `NO_ACK`, an answer that came after their
        watchdogs gave up. A wrong credit only clears a `NO_ACK` note early.

        Returns:
            The topics; empty when nothing awaits an answer.
        """
        if self._pending:
            return tuple(sorted(self._pending))
        if self._resend:
            return tuple(sorted(self._resend))
        return tuple(sorted(self._step & self._noack))

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
            self._resend_resolved(topic)
        if topics:
            self._admission_stale = True
            self._on_refused(topics, message)

    def _desired(self) -> set[str]:
        """Return D: the wanted topics, minus refused ones and deferred options contracts.

        Returns:
            The desired topics.
        """
        return {t for t in self._refs if t not in self._refused and t not in self._deferred}

    def _defer(self) -> None:
        """Recompute the explicit topics the cap admits and those it defers (see the class).

        Called by `reconcile` after `add`, `remove`, or a refusal. Recomputing with nothing
        changed gives the same sets, since topics admitted before keep their place.
        """
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
        """Start a topic's watchdog, which fires after `subscription_confirmation_timeout`.

        Args:
            topic: The topic just sent.
            generation: The generation it was sent in.
            attempt: 1 after the step's send, 2 after the single-topic re-send.
        """
        self._cancel(topic)
        self._watchdogs[topic] = self._call_later(
            self._subscription_confirmation_timeout, self._fire, topic, generation, attempt
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
        """A topic's acknowledgement is overdue: re-send it alone, then mark it `NO_ACK`.

        The first timeout re-sends the topic alone, while its step is still in flight; the
        second marks it `NO_ACK` and makes sure a round of re-sends is scheduled
        (`_resend_round`). Nothing reconnects (see the class).

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
        if attempt < _NO_ACK_ATTEMPT:
            self._sender.subscribe(generation, [topic])
            self._arm(topic, generation, attempt=attempt + 1)
            return
        self._pending.discard(topic)
        self._noack.add(topic)
        self._unreported.append(topic)
        if self._round is None:
            self._round = self._call_later(NO_ACK_RESEND, self._resend_round, generation)
        self.reconcile()

    def _resend_round(self, generation: int) -> None:
        """Re-send every `NO_ACK` topic still on the wire, together in one subscribe frame.

        Runs every `NO_ACK_RESEND` seconds while any topic is in `NO_ACK`. While a step is in
        flight, or the previous round still awaits its answer, the round waits
        `subscription_confirmation_timeout` and tries again, so the two never await answers
        together. A sent round awaits its answer for `subscription_confirmation_timeout`
        (`_resend_unanswered`), during which no step goes out, and makes the latest step's topics no
        longer the target of a late acknowledgement naming no topic.

        Args:
            generation: The generation the round is bound to; a later one ignores it.
        """
        self._round = None
        if generation != self.generation:
            return
        wire = self._wire()
        due = sorted(t for t in self._noack if t in wire)
        if not due:
            return
        if self._pending or self._resend:
            logger.debug(
                "generation %d: the re-send of %d NO_ACK topics waits; a subscription step or "
                "the previous round awaits an answer",
                generation,
                len(due),
            )
            self._round = self._call_later(
                self._subscription_confirmation_timeout, self._resend_round, generation
            )
            return
        logger.debug(
            "generation %d: re-sending the subscribe of %s, still unacknowledged",
            generation,
            bounded_list(due),
        )
        self._sender.subscribe(generation, due)
        self._step = frozenset()
        self._resend = frozenset(due)
        self._resend_timer = self._call_later(
            self._subscription_confirmation_timeout, self._resend_unanswered
        )
        self._round = self._call_later(NO_ACK_RESEND, self._resend_round, generation)

    def _resend_unanswered(self) -> None:
        """Stop awaiting the answer to a round of re-sends that got none in time.

        It runs `subscription_confirmation_timeout` after the round was sent. Any answer that
        resolves the round's last topic cancels this timer, so it runs only while the round
        still awaits one. A step held back meanwhile goes out now.
        """
        self._resend_timer = None
        self._resend = frozenset()
        self.reconcile()

    def _resend_resolved(self, *topics: str) -> None:
        """Stop awaiting an answer for topics of the current round; its timer ends with its last.

        Args:
            topics: Topics acknowledged, refused, or no longer wanted.
        """
        if not self._resend:
            return
        self._resend = self._resend.difference(topics)
        if not self._resend and self._resend_timer is not None:
            self._resend_timer.cancel()
            self._resend_timer = None

    def _report_no_ack(self) -> None:
        """Log the `NO_ACK` topics of the step that just resolved, in one WARNING."""
        logger.warning(
            "generation %d: no acknowledgement from Massive after %.1f s and one re-send for "
            "the topics %s; they stay subscribed and are re-sent every %.0f s, they count in "
            "NoAckTopics of m.connections() and in their streams' Error, and the connection is "
            "not reconnected for them",
            self.generation,
            2 * self._subscription_confirmation_timeout,
            bounded_list(sorted(self._unreported)),
            NO_ACK_RESEND,
        )
        self._unreported.clear()
