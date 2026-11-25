"""Finding which items of a refused request Massive refuses, by sending it again in halves.

Massive refuses a request of several items (a REST request of several symbols answered HTTP
403, a WebSocket subscribe frame of several topics answered "not authorized") without naming
the items it refuses. A refusal is final, so marking every item refused would leave an
entitled one out for good because of another. `RefusalSearch` decides which parts of the
refused request to send again, and reads the answers, until each item is answered, refused,
or failed for a reason that says nothing about entitlement:

1. The refused request splits by class first, one part per class, since a plan serves one
   class and not another. A part of one class splits into two halves by group count. A group
   is the items one request can't split apart (two symbols spelled alike on the wire).
2. An answered part is final: its items are served.
3. A part refused whole whose sibling half is refused whole too ends the search there: every
   item of both is refused, since Massive refusing both halves is the plan lacking the data,
   and halving further would cost two requests per level to learn the same. Otherwise a
   refused part of two or more groups splits again, and one of one group is refused.
4. A part that fails for another reason (a 5xx, a timeout, no answer) keeps that failure for
   its items and lets a refused sibling split.

Finding one refused item among `n` costs `2 * ceil(log2(n))` requests, and a refusal of every
item two. The search is pure: it sends nothing and keeps no clock. A REST read sends every
pending part of a level at once; a WebSocket connection sends one part at a time.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from deephaven_massive._errors import BrokenGuaranteeError, library_bug

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping, Sequence

__all__ = ["RefusalSearch"]

logger = logging.getLogger(__name__)

E = TypeVar("E")


class _Verdict(enum.Enum):
    """What Massive answered a part of the search."""

    ANSWERED = enum.auto()
    REFUSED = enum.auto()
    FAILED = enum.auto()
    DROPPED = enum.auto()  # not sent: the caller no longer wants any of its items


@dataclass(eq=False, slots=True)
class _Part:
    """A part of the refused request: a node of the search.

    Attributes:
        groups: Its groups, each a tuple of items sent together.
        sibling: The other half of the part it was split from, or None for a part of a
            class, or of the items an answered part refused.
        verdict: Massive's answer to it, once given.
        waiting: Refused whole, and waiting for its sibling's answer to decide whether it
            splits or is final.
    """

    groups: tuple[tuple[str, ...], ...]
    sibling: _Part | None = None
    verdict: _Verdict | None = None
    waiting: bool = False

    @property
    def items(self) -> tuple[str, ...]:
        """Its items, group by group."""
        return tuple(item for group in self.groups for item in group)


class RefusalSearch(Generic[E]):
    """The search for the items Massive refuses among those of one refused request.

    Not thread-safe: one caller (a read's thread, a connection's loop thread) owns it. The
    caller sends each part `pending()` returns, gives Massive's answer to each with
    `answered`, `refused`, `failed`, or `dropped`, and repeats until `done`.

    Example:
        ::

            search = RefusalSearch(["T.AAPL", "T.XYZ"], group=str, class_of=channel_of)
            [first, second] = search.pending()  # ("T.AAPL",), ("T.XYZ",)
            search.answered(first)
            search.refused(second)
            assert search.done and search.refused_items == ("T.XYZ",)
    """

    def __init__(
        self,
        items: Sequence[str],
        *,
        group: Callable[[str], str],
        class_of: Callable[[str], str],
    ) -> None:
        """Start the search of a request Massive refused whole.

        Args:
            items: The request's items, each once.
            group: The key of an item's group: items with one key are always sent together.
            class_of: An item's class; a group's class is its first item's.
        """
        by_key: dict[str, list[str]] = {}
        for item in items:
            by_key.setdefault(group(item), []).append(item)
        self._class_of = class_of
        self._pending: dict[tuple[str, ...], _Part] = {}
        self._refused: list[str] = []
        self._failed: dict[str, E] = {}
        self._requests = 0
        self._split(tuple(tuple(g) for g in by_key.values()))

    @property
    def done(self) -> bool:
        """True once every item has its answer: no part waits to be sent or answered."""
        return not self._pending

    @property
    def refused_items(self) -> tuple[str, ...]:
        """The items found refused, in the order they were found."""
        return tuple(self._refused)

    @property
    def failed_items(self) -> Mapping[str, E]:
        """Each item of a part that failed for a reason other than a refusal, with its error."""
        return dict(self._failed)

    @property
    def requests(self) -> int:
        """How many parts Massive answered, refused, or failed: the requests the search cost."""
        return self._requests

    def pending(self) -> list[tuple[str, ...]]:
        """Return the parts to send now, oldest first, each as its items.

        Returns:
            The parts with no answer yet. A part stays here until its answer is given, so a
            caller that sends one at a time sends the first.
        """
        return list(self._pending)

    def answered(self, part: Sequence[str], *, refused: Collection[str] = ()) -> None:
        """Take Massive's answer to a part: its items are served.

        Args:
            part: A part `pending()` returned.
            refused: Its items Massive refused in the same answer without naming them, such
                as the topics of a subscribe frame left unacknowledged beside a refusal; they
                are searched as a request of their own.
        """
        found = self._take(part, _Verdict.ANSWERED)
        if refused:
            kept = (tuple(i for i in g if i in refused) for g in found.groups)
            self._split(tuple(g for g in kept if g))
        self._decide_sibling(found)

    def refused(self, part: Sequence[str]) -> None:
        """Take Massive's refusal of a whole part.

        Args:
            part: A part `pending()` returned.
        """
        found = self._take(part, _Verdict.REFUSED)
        sibling = found.sibling
        if sibling is not None and sibling.verdict is _Verdict.REFUSED:
            # Both halves refused whole: the plan lacks the data, so every item of both is.
            if sibling.waiting:
                sibling.waiting = False
                self._refuse(sibling.items)
            self._refuse(found.items)
        elif sibling is not None and sibling.verdict is None and len(found.groups) > 1:
            found.waiting = True  # the sibling's answer decides whether it splits
        else:
            self._split(found.groups)

    def failed(self, part: Sequence[str], error: E) -> None:
        """Take a failure of a part that says nothing about entitlement, such as a timeout.

        Args:
            part: A part `pending()` returned.
            error: The failure, which each of its items keeps.
        """
        found = self._take(part, _Verdict.FAILED)
        for item in found.items:
            self._failed[item] = error
        self._decide_sibling(found)

    def dropped(self, part: Sequence[str]) -> None:
        """Take a part the caller did not send, since it no longer wants any of its items.

        Args:
            part: A part `pending()` returned.
        """
        self._decide_sibling(self._take(part, _Verdict.DROPPED))

    def _take(self, part: Sequence[str], verdict: _Verdict) -> _Part:
        """Remove a pending part and record its answer.

        Args:
            part: The part's items, as `pending()` returned them.
            verdict: Massive's answer.

        Returns:
            The part.

        Raises:
            BrokenGuaranteeError: `part` is not pending, a defect of the caller.
        """
        found = self._pending.pop(tuple(part), None)
        if found is None:
            raise BrokenGuaranteeError(library_bug(f"{list(part)} is no pending part"))
        found.verdict = verdict
        if verdict is not _Verdict.DROPPED:
            self._requests += 1
        logger.debug("refusal search: %s %s", verdict.name.lower(), list(part))
        return found

    def _decide_sibling(self, part: _Part) -> None:
        """Split a sibling that was refused whole, now that `part` was not.

        Args:
            part: A part just answered, failed, or dropped.
        """
        sibling = part.sibling
        if sibling is not None and sibling.waiting:
            sibling.waiting = False
            self._split(sibling.groups)

    def _split(self, groups: tuple[tuple[str, ...], ...]) -> None:
        """Split refused groups into parts to send: by class, else into halves.

        One group is refused as it is: nothing smaller can be sent.

        Args:
            groups: The groups of a refused request or part.
        """
        if len(groups) == 1:
            self._refuse(groups[0])
            return
        classes: dict[str, list[tuple[str, ...]]] = {}
        for g in groups:
            classes.setdefault(self._class_of(g[0]), []).append(g)
        if len(classes) > 1:
            parts = [_Part(tuple(c)) for c in classes.values()]
        else:
            half = len(groups) // 2
            first, second = _Part(groups[:half]), _Part(groups[half:])
            first.sibling, second.sibling = second, first
            parts = [first, second]
        for p in parts:
            self._pending[p.items] = p
        logger.debug("refusal search: split into %s", [list(p.items) for p in parts])

    def _refuse(self, items: tuple[str, ...]) -> None:
        """Record items as refused.

        Args:
            items: The items.
        """
        self._refused.extend(items)
        logger.debug("refusal search: refused %s", list(items))
