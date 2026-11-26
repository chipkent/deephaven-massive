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
3. When both halves of a part are refused whole and each holds two groups or more, one group
   of each is sent alone (a probe). Both probes refused end the search there: every item of
   both halves is refused, since Massive refusing all four is the plan lacking the data, and
   halving further would cost two requests per level to learn the same. Otherwise each half
   goes on without its probe: the rest of a half whose probe was answered is still refused,
   so it splits again, and the rest of one whose probe was refused or failed is sent again.
   A refused half of one group is refused, and its sibling splits on its own.
4. A part that fails for another reason (a 5xx, a timeout, no answer) keeps that failure for
   its items and lets a refused sibling split.
5. A refusal that names the items it refuses (`refused_by_name`) refuses exactly their groups,
   and the rest of the part is sent again.

Finding one refused item among `n` costs about `2 * ceil(log2(n))` requests, plus two for each
probe, and a refusal of every item of a class four. The search is pure: it sends nothing and
keeps no clock. A REST read sends every pending part of a level at once; a WebSocket
connection sends one part at a time.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, assert_never

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
    NAMED = enum.auto()  # refused, with the items it refuses named
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
        rest: For a probe (one group of a refused half), the half's other groups; None for
            any other part. A probe's sibling is the other half's probe.
    """

    groups: tuple[tuple[str, ...], ...]
    sibling: _Part | None = None
    verdict: _Verdict | None = None
    waiting: bool = False
    rest: tuple[tuple[str, ...], ...] | None = None

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
        if found.rest is not None:
            self._probed(found)
        elif sibling is None or sibling.verdict is not _Verdict.REFUSED:
            if sibling is not None and sibling.verdict is None and len(found.groups) > 1:
                found.waiting = True  # the sibling's answer decides whether it splits
            else:
                self._split(found.groups)
        elif sibling.waiting and len(found.groups) > 1:
            # Both halves refused whole: a probe of each tells the plan lacking the data
            # from a refused item in each half.
            sibling.waiting = False
            self._probe(sibling, found)
        else:
            # A half of one group, refused already or now: the other splits on its own.
            if sibling.waiting:
                sibling.waiting = False
                self._split(sibling.groups)
            self._split(found.groups)

    def refused_by_name(self, part: Sequence[str], refused: Collection[str]) -> tuple[str, ...]:
        """Take Massive's refusal of a part that names the items it refuses.

        The groups that hold a named item are refused, and the part's other groups are sent
        again as a part of their own. A refusal that names none of the part's items is taken
        as a refusal of the whole part (`refused`).

        Args:
            part: A part `pending()` returned.
            refused: The items the refusal names; it may name items of other parts.

        Returns:
            The items this refusal refuses: those of the named groups, or the whole part.
        """
        found = self._pending.get(tuple(part))
        named = () if found is None else tuple(g for g in found.groups if set(g) & set(refused))
        if found is None or not named:
            self.refused(part)  # raises for a part that is not pending
            return tuple(part)
        self._take(part, _Verdict.NAMED)
        items = tuple(item for group in named for item in group)
        self._refuse(items)
        rest = tuple(g for g in found.groups if g not in named)
        if rest:
            self._send(rest)
        self._decide_sibling(found)
        return items

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

    def _probe(self, first: _Part, second: _Part) -> None:
        """Send one group of each of two halves refused whole, each alone.

        Args:
            first: A refused half of two groups or more.
            second: Its sibling, refused too, of two groups or more.
        """
        probes = [_Part(half.groups[:1], rest=half.groups[1:]) for half in (first, second)]
        probes[0].sibling, probes[1].sibling = probes[1], probes[0]
        for p in probes:
            self._pending[p.items] = p
        logger.debug("refusal search: both halves refused; probing %s", [p.items for p in probes])

    def _probed(self, probe: _Part) -> None:
        """Decide two refused halves once both their probes have Massive's answer.

        Args:
            probe: A probe just answered, refused, refused by name, failed, or dropped.
        """
        other = probe.sibling
        assert other is not None  # a probe's sibling is the other probe
        assert other.rest is not None
        assert probe.rest is not None
        mine, theirs = probe.verdict, other.verdict
        assert mine is not None  # taken just now
        if theirs is None:
            return  # the other probe's answer decides
        halves = ((other, theirs, other.rest), (probe, mine, probe.rest))
        refused = {_Verdict.REFUSED, _Verdict.NAMED}
        if mine in refused and theirs in refused:
            # All four refused: the plan lacks the data, so every item of both halves is.
            for p, verdict, rest in halves:
                own = p.items if verdict is _Verdict.REFUSED else ()  # a named one's are refused
                self._refuse(own + tuple(item for group in rest for item in group))
            return
        for p, verdict, rest in halves:
            match verdict:
                case _Verdict.ANSWERED:
                    self._split(rest)  # the half was refused, so its rest still is
                case _Verdict.REFUSED:
                    self._refuse(p.items)
                    self._send(rest)  # the probe's refusal may be the half's only one
                case _Verdict.NAMED | _Verdict.FAILED | _Verdict.DROPPED:
                    self._send(rest)  # a named probe's items are refused when taken
                case _:
                    assert_never(verdict)

    def _send(self, groups: tuple[tuple[str, ...], ...]) -> None:
        """Send groups whose answer is not known again, as a part of their own.

        Args:
            groups: The groups.
        """
        part = _Part(groups)
        self._pending[part.items] = part
        logger.debug("refusal search: sending %s again", list(part.items))

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

        A probe's answer goes to the decision of its pair instead (`_probed`).

        Args:
            part: A part just answered, failed, refused by name, or dropped.
        """
        if part.rest is not None:
            self._probed(part)
            return
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
