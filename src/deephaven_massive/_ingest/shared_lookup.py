"""Shared lookups: a session's lookups that one caller makes while the others wait for it.

Some of a session's REST lookups serve every caller that needs them at once: the WebSocket host
decision of each (asset class, channel family), and Massive's list of crypto tickers with their
currencies. `SharedLookup` holds such lookups by key, single-flight: the first caller of a key
claims it (`claim_or_wait` returns a `Claim`) and makes it, every concurrent caller waits for
its outcome, and later callers get the value at once (`known`). The maker ends its claim with
`complete` or `fail`; a failed lookup is forgotten before its waiters hear of it, so the next
caller makes it again.

The lookup is shared, but its rate-limit pauses are not: a waiting caller charges each pause of
the lookup's governor bucket to its own patience (`RequestGovernor.charge_pause`), and gives up
once a pause outlasts what that patience still covers. So a console command, which waits out
90 s of pauses, never waits out the ten minutes of the caller it joined. When the maker stops
for a reason of its own (it gave up on a pause, was interrupted, or its background read was
stopped), its waiters do not fail with that reason: each tries again with the patience it has
left, and one of them makes the lookup. A failure from Massive reaches every waiter as a copy
of the maker's error.

The REST transport (`_ingest/rest.py`) owns the session's lookups and makes them; this module
holds no knowledge of what a lookup is. Nothing here starts a thread or calls Deephaven.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Generic, TypeVar

from deephaven_massive._errors import ErrorSource, MassiveError, copy_error, library_bug
from deephaven_massive._massive import WireError

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._massive import Cancel, Patience, RequestGovernor

__all__ = ["Claim", "SharedLookup"]

logger = logging.getLogger(__name__)

K = TypeVar("K")
"""The key of a shared lookup, such as an (asset class, channel family) pair."""
V = TypeVar("V")
"""The value of a shared lookup."""


def _monotonic() -> float:
    """Return the monotonic clock a lookup's age is measured by; tests replace it.

    Returns:
        Seconds, as `time.monotonic` gives them.
    """
    return time.monotonic()


class _AbandonedError(Exception):
    """What a shared lookup's future holds when the caller making it stopped for its own reason.

    That caller gave up on a pause its own patience could not cover, was interrupted (as by
    Ctrl-C), or its background read was stopped. None of these says anything about the lookup,
    so no caller waiting on it may fail with it: each tries again with the patience it has left
    (`SharedLookup.claim_or_wait`). Only ever set on a future, never raised; its text is the
    maker's error, for the log.
    """


def _shared_failure(error: BaseException, context: str, *, stopped: bool) -> BaseException:
    """Return what a failed shared lookup's future holds for the callers waiting on it.

    Args:
        error: What stopped the caller making the lookup: its public error, or the exception
            that interrupted it.
        context: What failed, for an unexpected error's message, such as `"the read failed"`.
        stopped: True when the maker's own read was stopped (a background read whose table
            was released) and its request ended without an answer from Massive.

    Returns:
        `_AbandonedError` when the maker stopped for its own reason: its read was stopped, a
        `MassiveError` with status 429 (its patience ran out; the client never gives up on a
        429 otherwise), or an interruption that is not an `Exception` (`KeyboardInterrupt`).
        Otherwise the `MassiveError` itself, or for any other error a `MassiveError` naming
        it, a defect of the library (`ErrorSource.LIBRARY`, with how to report it),
        which every waiter raises a copy of (`_outcome`).
    """
    if stopped:
        return _AbandonedError(f"its read was stopped: {error}")
    if isinstance(error, MassiveError):
        if error.status == HTTPStatus.TOO_MANY_REQUESTS:
            return _AbandonedError(str(error))
        return error
    if not isinstance(error, Exception):
        return _AbandonedError(repr(error))
    return MassiveError(library_bug(f"{context}: {error!r}"), source=ErrorSource.LIBRARY)


def _outcome(future: Future[V]) -> V:
    """Return a completed shared lookup's result, or raise a copy of its error.

    Args:
        future: A completed future that holds a result or a `MassiveError`.

    Returns:
        The result.

    A failed lookup raises a copy of the maker's `MassiveError` (its type, text, status,
    request id, source, `is_retryable`, and `is_bug`; `copy_error`), so that no exception
    object is raised on two threads at once.
    """
    try:
        return future.result()
    except MassiveError as error:
        copied = copy_error(error)
        raise copied from None


def _succeeded_result(future: Future[V] | None) -> V | None:
    """Return a shared lookup's result once it has succeeded, and None in every other state.

    Args:
        future: The lookup's future, or None when no lookup is claimed.

    Returns:
        The result; None when the lookup is not claimed, still running, or failed (its
        future then holds the error, which only `claim_or_wait` may act on).
    """
    if future is None or not future.done() or future.exception() is not None:
        return None
    return future.result()


@dataclass(frozen=True, eq=False, slots=True)
class Claim(Generic[K, V]):
    """A caller's claim on a shared lookup: that caller makes it and must end the claim.

    `SharedLookup.claim_or_wait` returns one to the first caller of a key. Its maker ends it
    exactly once, with `SharedLookup.complete` or `SharedLookup.fail`, whatever stops the
    making (an interrupt included): a claim left open hangs every later caller of the key.

    Attributes:
        key: The lookup's key.
        future: The lookup's outcome, which the waiting callers wait on.
    """

    key: K
    future: Future[V]


class SharedLookup(Generic[K, V]):
    """A session's lookups by key, each made by one caller while the others wait.

    The REST transport's `RestState` holds one per kind of lookup for the session's life.
    Thread-safe: the lookups sit behind one leaf lock, held only to read or change them; every
    caller waits outside it, on the lookup's future and the rate governor.

    Example:
        ::

            # In a REST reader: `state` is its `RestState`, `patience` its read's.
            lookup = SharedLookup[str, int](state.label, state.client.governor)
            got = lookup.claim_or_wait(
                "answer",
                bucket="other",
                patience=patience,
                what="the answer",
                public=reader.public_error,
            )
            if isinstance(got, Claim):  # this caller makes it, and must end the claim
                try:
                    value = 42  # the lookup's requests
                except BaseException as error:
                    lookup.fail(got, error, "the answer failed")
                    raise
                lookup.complete(got, value)
    """

    __slots__ = ("_claimed", "_futures", "_governor", "_label", "_lock")

    def __init__(self, label: str, governor: RequestGovernor) -> None:
        """Start with no lookup; nothing is requested.

        Args:
            label: The session's label, such as `"MassiveSession #1"`, for the log.
            governor: The session's rate governor, whose pauses a waiting caller charges to
                its own patience.
        """
        self._label = label
        self._governor = governor
        self._lock = threading.Lock()
        self._futures: dict[K, Future[V]] = {}
        self._claimed: dict[K, float] = {}  # when each lookup was claimed (`_monotonic`)

    def known(self, key: K) -> V | None:
        """Return a key's value if its lookup has succeeded, without claiming it.

        A failed lookup is removed before its future fails, but this check reads the future
        under the lock and looks at it after: its maker may forget and fail it in between. So
        only a value counts; a failed future gives None, and the caller then claims or waits
        as usual. The failed lookup is forgotten by then, so the caller claims a fresh one.

        Args:
            key: The lookup's key.

        Returns:
            The value, or None while no lookup is made, one is being made, or the one being
            made has just failed.
        """
        with self._lock:
            future = self._futures.get(key)
        return _succeeded_result(future)

    def _claim(self, key: K) -> tuple[Future[V], bool]:
        """Return a key's lookup, or claim it for the caller to make.

        Args:
            key: The lookup's key.

        Returns:
            The lookup's future and whether the caller created it.
        """
        with self._lock:
            future = self._futures.get(key)
            if future is not None:
                return future, False
            future = self._futures[key] = Future()
            self._claimed[key] = _monotonic()
            return future, True

    def claim_or_wait(
        self,
        key: K,
        *,
        bucket: str,
        patience: Patience,
        what: str,
        public: Callable[[WireError], MassiveError],
        cancel: Cancel | None = None,
    ) -> V | Claim[K, V]:
        """Claim a key's lookup for the caller to make, or wait for the caller making it.

        Take the patience before the call: a claim is ended only by the block that makes the
        lookup, so nothing between claiming and that block may raise.

        Args:
            key: The lookup's key.
            bucket: The governor bucket the lookup's requests are charged to.
            patience: This caller's patience; the pauses it waits out are charged to it.
            what: The lookup, for the log, such as `"the crypto ticker list"`.
            public: Turns a wire error into its public error, as the caller's reads do.
            cancel: This caller's stop token (a background read's): once it is set, the
                caller stops waiting. None when only the session's close should end the wait.

        Returns:
            A `Claim` the caller must end, when no other caller is making the lookup; else the
            value the other caller found.

        Blocks the calling thread while another caller makes the lookup; holds no lock
        meanwhile. A lookup that failed raises a copy of its maker's `MassiveError`. When the
        maker stopped for a reason of its own, this caller tries again, claiming the lookup
        or joining a new maker. A waiting caller that gives up raises the public error of
        `_wait`.
        """
        future, created = self._claim(key)
        while not created:
            if self._wait(
                future, bucket=bucket, patience=patience, what=what, public=public, cancel=cancel
            ):
                return _outcome(future)
            future, created = self._claim(key)
        return Claim(key, future)

    def complete(self, claim: Claim[K, V], value: V) -> None:
        """End a claim with its value: every waiting and later caller gets it.

        Args:
            claim: The caller's claim.
            value: The lookup's value.
        """
        claim.future.set_result(value)

    def fail(
        self, claim: Claim[K, V], error: BaseException, context: str, *, stopped: bool = False
    ) -> None:
        """End a claim that failed: forget the lookup, then tell its waiters.

        The lookup is forgotten before its future fails, so a waiter that tries again never
        finds it and claims a fresh one.

        Args:
            claim: The caller's claim.
            error: What stopped the maker: its public error, or the exception that interrupted
                it.
            context: What failed, for an unexpected error's message, such as `"the read
                failed"`.
            stopped: True when the maker's own read was stopped and its request ended without
                an answer from Massive: its waiters try again instead of copying the error.
        """
        with self._lock:
            if self._futures.get(claim.key) is claim.future:
                del self._futures[claim.key]
                del self._claimed[claim.key]
        claim.future.set_exception(_shared_failure(error, context, stopped=stopped))

    def retire(self, key: K, value: V, max_age: float) -> bool:
        """Forget a key's value when it is old, so the next caller makes the lookup again.

        A caller whose value lacks what it needs calls it before giving up: a value Massive
        has changed since is then found. The value is retired only when it is the one the
        caller holds and was claimed `max_age` seconds ago or more, so the lookup is made at
        most once per `max_age` however many callers miss, and callers that miss at once
        retire it once.

        Args:
            key: The lookup's key.
            value: The value the caller holds.
            max_age: The seconds after which the value may be retired.

        Returns:
            True when the caller should make the lookup again: the value was retired now, or
            another caller retired or replaced it meanwhile. False while it is the current
            value and younger than `max_age`.
        """
        with self._lock:
            future = self._futures.get(key)
            if future is not None and _succeeded_result(future) is value:
                if _monotonic() - self._claimed[key] < max_age:
                    return False
                del self._futures[key]
                del self._claimed[key]
            return True

    def _wait(
        self,
        future: Future[V],
        *,
        bucket: str,
        patience: Patience,
        what: str,
        public: Callable[[WireError], MassiveError],
        cancel: Cancel | None,
    ) -> bool:
        """Wait for a lookup another caller is making, within this caller's own patience.

        This caller charges each pause of the lookup's bucket to its own patience
        (`RequestGovernor.charge_pause`), when it starts waiting and after every change of
        that bucket, and stops waiting once a pause outlasts what its patience still covers;
        the lookup goes on for its maker and every other caller still waiting.

        Blocks the calling thread until the lookup ends, this caller gives up, or its stop
        token is set; holds no lock meanwhile. It always removes its governor and stop-token
        listeners; the callback it adds to the future cannot be removed, so it stays there,
        setting an event nobody waits on once this call returns (a future kept for the session
        keeps one per caller that waited on it). Logs the wait at DEBUG, a lookup abandoned by
        its maker at INFO, and giving up at DEBUG: the caller raises the error.

        Args:
            future: The lookup's future, which the caller making it completes.
            bucket: The governor bucket the lookup's requests are charged to.
            patience: This caller's patience.
            what: The lookup, for the log.
            public: Turns a wire error into its public error.
            cancel: This caller's stop token, or None.

        Returns:
            True when the future holds the lookup's outcome (`_outcome` returns or raises it);
            False when the caller making it stopped for a reason of its own
            (`_AbandonedError`), so this caller tries again.

        Once a pause of the lookup outlasts this caller's patience, it raises the public error
        of a request that gives up: `MassiveError("rate limited: ...")` with status 429, or
        `"... is closed"` if the session closed meanwhile. Once `cancel` is set, it raises
        `MassiveError("waiting for <what>: cancelled")`, or the closed session's error.
        """
        logger.debug(
            "%s: waits for %s another caller is making, through at most %.0f s of its pauses",
            self._label,
            what,
            patience.seconds,
        )
        governor = self._governor
        changed = threading.Event()
        future.add_done_callback(lambda _: changed.set())
        governor.subscribe_bucket(bucket, changed.set)
        forget_stop = None if cancel is None else cancel.on_set(changed.set)
        try:
            while True:
                changed.clear()  # before looking, so no change is missed
                if future.done():
                    abandoned = future.exception()
                    if isinstance(abandoned, _AbandonedError):
                        logger.info(
                            "%s: the caller making %s stopped (%s); this caller tries again",
                            self._label,
                            what,
                            abandoned,
                        )
                        return False
                    return True
                if cancel is not None and cancel.is_set():
                    stopped = public(WireError(f"waiting for {what}: cancelled"))
                    logger.debug(
                        "%s: stops waiting for %s, its read was stopped: %s",
                        self._label,
                        what,
                        stopped,
                    )
                    raise stopped
                failure = governor.charge_pause(bucket, patience)
                if failure is not None:
                    gave_up = public(failure)
                    logger.debug(
                        "%s: stops waiting for %s, which goes on for the caller making it: %s",
                        self._label,
                        what,
                        gave_up,
                    )
                    raise gave_up from failure
                governor.clock.wait(changed, math.inf)
        finally:
            governor.unsubscribe(changed.set)
            if forget_stop is not None:
                forget_stop()
