"""Tests for the search for the items Massive refuses among those of one refused request."""

from __future__ import annotations

import itertools
import math
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._ingest.refusal_search import RefusalSearch

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from collections.abc import Set as AbstractSet


def _same(item: str) -> str:
    return item


def _one_class(_item: str) -> str:
    return "stocks"


def search_of(items: list[str]) -> RefusalSearch[str]:
    return RefusalSearch(items, group=_same, class_of=_one_class)


def answer(search: RefusalSearch[str], refused: set[str]) -> None:
    # Massive's answer to a REST level: every pending part at once, refused whole when it holds
    # a refused item.
    for part in search.pending():
        if refused & set(part):
            search.refused(part)
        else:
            search.answered(part)


def test_a_refused_root_splits_into_two_halves() -> None:
    search = search_of(["A", "B", "C", "D", "E"])
    assert search.pending() == [("A", "B"), ("C", "D", "E")]
    assert not search.done
    assert search.requests == 0


def test_a_root_of_one_group_is_refused_without_a_request() -> None:
    search: RefusalSearch[str] = RefusalSearch(
        ["I:DJT", "DJT"], group=lambda s: s.removeprefix("I:"), class_of=str
    )
    assert search.done
    assert search.refused_items == ("I:DJT", "DJT")
    assert search.requests == 0


def test_items_spelled_alike_on_the_wire_are_never_split_apart() -> None:
    search: RefusalSearch[str] = RefusalSearch(
        ["I:DJT", "AAPL", "DJT", "MSFT"],
        group=lambda s: s.removeprefix("I:"),
        class_of=_one_class,
    )
    assert search.pending() == [("I:DJT", "DJT"), ("AAPL", "MSFT")]
    answer(search, {"DJT"})
    assert search.done
    assert search.refused_items == ("I:DJT", "DJT")


def test_a_request_of_several_classes_splits_by_class_first() -> None:
    # Two classes refused whole are no "both halves refused": each is searched on its own.
    search: RefusalSearch[str] = RefusalSearch(
        ["Q.AAPL", "T.AAPL", "Q.MSFT", "T.MSFT"],
        group=_same,
        class_of=lambda t: t.split(".")[0],
    )
    assert search.pending() == [("Q.AAPL", "Q.MSFT"), ("T.AAPL", "T.MSFT")]
    answer(search, {"Q.AAPL", "T.MSFT"})
    assert search.pending() == [("Q.AAPL",), ("Q.MSFT",), ("T.AAPL",), ("T.MSFT",)]
    answer(search, {"Q.AAPL", "T.MSFT"})
    assert search.done
    assert sorted(search.refused_items) == ["Q.AAPL", "T.MSFT"]
    assert search.requests == 6


def test_a_transient_failure_of_a_half_keeps_its_items_and_lets_the_refused_half_split() -> None:
    search = search_of(["A", "B", "C", "D"])
    first, second = search.pending()
    search.refused(first)
    search.failed(second, "HTTP 503")
    assert search.failed_items == {"C": "HTTP 503", "D": "HTTP 503"}
    assert search.pending() == [("A",), ("B",)]
    answer(search, {"B"})
    assert search.done
    assert search.refused_items == ("B",)


def test_a_half_refused_first_waits_for_its_sibling_sent_one_at_a_time() -> None:
    # A WebSocket connection sends one part at a time: a refused half waits for the other.
    search = search_of(["A", "B", "C", "D"])
    search.refused(search.pending()[0])
    assert search.pending() == [("C", "D")]
    search.answered(("C", "D"))
    assert search.pending() == [("A",), ("B",)]
    search.refused(("A",))  # a part of one group is refused at once, its sibling unanswered
    assert search.refused_items == ("A",)
    search.refused(("B",))
    assert search.done
    assert list(search.refused_items) == ["A", "B"]


def test_an_answered_probe_splits_its_half_and_a_refused_one_sends_its_half_again() -> None:
    search = search_of(["A", "B", "C", "D", "E", "F"])
    search.refused(("A", "B", "C"))
    search.refused(("D", "E", "F"))
    search.answered(("A",))
    search.refused(("D",))
    # B and C hold a refusal still, so they split; E and F may not, so they go again.
    assert search.pending() == [("B",), ("C",), ("E", "F")]
    assert list(search.refused_items) == ["D"]
    search.answered(("B",))
    search.refused(("C",))
    search.answered(("E", "F"))
    assert search.done
    assert list(search.refused_items) == ["D", "C"]
    assert search.requests == 7


@pytest.mark.parametrize("other", ["failed", "dropped"])
def test_a_probe_that_failed_or_was_dropped_sends_its_half_again(other: str) -> None:
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.refused(("C", "D"))
    if other == "failed":
        search.failed(("C",), "timeout")
    else:
        search.dropped(("C",))
    search.answered(("A",))
    assert search.pending() == [("D",)]  # B is refused: its half was, and A was answered
    assert search.refused_items == ("B",)


def test_a_half_of_one_group_refused_lets_its_refused_sibling_split_with_a_request() -> None:
    # Three groups split one and two: the refused lone group says nothing of the other two.
    search = search_of(["A", "B", "C"])
    assert search.pending() == [("A",), ("B", "C")]
    search.refused(("A",))
    search.refused(("B", "C"))
    assert search.pending() == [("B",), ("C",)]
    search.answered(("B",))
    search.answered(("C",))
    assert search.refused_items == ("A",)
    search = search_of(["A", "B", "C"])
    search.refused(("B", "C"))  # refused first, it waits for its sibling
    search.refused(("A",))
    assert search.pending() == [("B",), ("C",)]


def test_a_refusal_that_names_its_items_refuses_their_groups_and_sends_the_rest_again() -> None:
    search = search_of(["A", "B", "C", "D"])
    assert search.refused_by_name(("A", "B"), {"A", "Z"}) == ("A",)
    assert search.refused_items == ("A",)
    assert search.pending() == [("C", "D"), ("B",)]
    search.answered(("C", "D"))
    search.answered(("B",))
    assert search.done


def test_a_named_refusal_of_none_of_the_part_refuses_it_whole() -> None:
    search = search_of(["A", "B", "C", "D"])
    assert search.refused_by_name(("A", "B"), {"Z"}) == ("A", "B")
    search.answered(("C", "D"))
    assert search.pending() == [("A",), ("B",)]


def test_a_named_probe_beside_a_refused_one_sends_every_other_group_alone() -> None:
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.refused(("C", "D"))
    assert search.refused_by_name(("A",), {"A"}) == ("A",)
    search.refused(("C",))
    assert search.pending() == [("B",), ("D",)]
    assert search.refused_items == ("A", "C")
    answer(search, {"D"})
    assert search.done
    assert list(search.refused_items) == ["A", "C", "D"]
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.refused(("C", "D"))
    search.refused_by_name(("A",), {"A"})
    search.answered(("C",))
    assert search.pending() == [("B",)]  # A's half may hold no other refusal
    assert search.refused_items == ("A", "D")


def test_an_answered_part_searches_the_items_it_refused_without_naming_them() -> None:
    search = search_of(["A", "B", "C", "D", "E", "F"])
    search.answered(("A", "B", "C"), refused={"B", "C"})
    assert search.pending() == [("D", "E", "F"), ("B",), ("C",)]
    search.answered(("D", "E", "F"))
    search.answered(("B",))
    search.refused(("C",))
    assert search.done
    assert search.refused_items == ("C",)
    search = search_of(["A", "B"])
    search.answered(("A",), refused={"A"})  # a lone item refused beside an answer is refused
    assert search.refused_items == ("A",)


def test_a_dropped_part_costs_no_request_and_lets_its_refused_sibling_split() -> None:
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.dropped(("C", "D"))
    assert search.requests == 1
    assert search.pending() == [("A",), ("B",)]


def test_an_answer_to_a_part_that_is_not_pending_is_a_defect() -> None:
    search = search_of(["A", "B"])
    with pytest.raises(BrokenGuaranteeError, match=r"^\['A', 'B'\] is no pending part\. "):
        search.answered(("A", "B"))
    with pytest.raises(BrokenGuaranteeError, match=r"^\['A', 'B'\] is no pending part\. "):
        search.refused_by_name(("A", "B"), {"A"})


def test_a_probe_verdict_no_case_names_is_a_defect() -> None:
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.refused(("C", "D"))
    search.answered(("A",))
    first_probe = search._pending[("C",)].sibling
    assert first_probe is not None
    first_probe.verdict = "SOMEHOW"  # type: ignore[assignment]  # the point: an unplanned verdict
    with pytest.raises(AssertionError):
        search.answered(("C",))


def test_two_refused_probes_send_every_other_group_alone_and_refuse_only_the_refused_ones() -> None:
    search = search_of(["A", "B", "C", "D", "E", "F"])
    search.refused(("A", "B", "C"))
    search.refused(("D", "E", "F"))
    assert search.pending() == [("A",), ("D",)]
    search.refused(("A",))
    assert search.pending() == [("D",)]  # the other probe's answer decides
    search.refused(("D",))
    assert search.pending() == [("B",), ("C",), ("E",), ("F",)]
    assert search.refused_items == ("A", "D")
    answer(search, {"E"})
    assert search.done
    assert list(search.refused_items) == ["A", "D", "E"]
    assert search.requests == 8


def _searched(
    items: Sequence[str], refused: AbstractSet[str], *, one_at_a_time: bool, name_every: int
) -> tuple[RefusalSearch[str], set[str]]:
    # Massive's answers to a whole search, every `name_every`-th refusal naming its refused
    # items (0: none does). Returns the search and every item Massive's answers prove refused:
    # one named, or one in a part refused whole whose every other item Massive served.
    search = search_of(list(items))
    named: set[str] = set()
    served: set[str] = set()
    refused_parts = [tuple(items)]  # the request the search starts from
    refusals = 0
    while not search.done:
        parts = search.pending()[:1] if one_at_a_time else search.pending()
        for part in parts:
            hit = refused & set(part)
            if not hit:
                served |= set(part)
                search.answered(part)
                continue
            refusals += 1
            if name_every and refusals % name_every == 0:
                named |= set(search.refused_by_name(part, hit))
            else:
                refused_parts.append(part)
                search.refused(part)
    proven = named | {
        item for part in refused_parts for item in part if set(part) - {item} <= served
    }
    return search, proven


def _cases() -> Iterator[tuple[list[str], frozenset[str]]]:
    # Every refused set of up to eight items, and for more items, refused sets of every
    # density, each in several orders.
    for n in range(1, 9):
        items = [f"S{i:02d}" for i in range(n)]
        for k in range(1, n + 1):
            for refused in itertools.combinations(items, k):
                yield items, frozenset(refused)
                yield items[::-1], frozenset(refused)
    for n in (16, 33, 64):
        items = [f"S{i:02d}" for i in range(n)]
        orders = [items, items[::-1], [items[i * 7 % n] for i in range(n)]]
        for step in range(1, 9):
            for offset in range(step):
                for order in orders:
                    yield order, frozenset(items[offset::step])
                    yield order, frozenset(items) - frozenset(items[offset::step])


@pytest.mark.parametrize(
    ("one_at_a_time", "name_every"), [(False, 0), (True, 0), (False, 3), (True, 5)]
)
def test_an_item_is_refused_only_by_a_refusal_of_its_group_alone_or_by_name(
    one_at_a_time: bool, name_every: int
) -> None:
    # Alone: in a part refused whole whose other groups Massive served, never beside a group
    # whose answer is unknown or refused, so the order of the items changes nothing.
    for items, refused in _cases():
        search, proven = _searched(
            items, refused, one_at_a_time=one_at_a_time, name_every=name_every
        )
        assert set(search.refused_items) <= proven, (items, refused)
        assert sorted(search.refused_items) == sorted(refused), (items, refused)
        assert len(search.refused_items) == len(refused)  # each refused once


@pytest.mark.parametrize("n", [4, 5, 64, 250])
def test_every_group_refused_costs_the_group_count_plus_two(n: int) -> None:
    items = [f"S{i:03d}" for i in range(n)]
    search = search_of(items)
    while not search.done:
        answer(search, set(items))
    # Two halves, a probe of each, then every other group alone.
    assert search.requests == n + 2
    assert sorted(search.refused_items) == items


@pytest.mark.parametrize("n", [2, 3, 5, 8, 9, 100])
def test_one_refused_group_costs_two_requests_per_level(n: int) -> None:
    items = [f"S{i:03d}" for i in range(n)]
    for refused in items:
        search = search_of(items)
        while not search.done:
            answer(search, {refused})
        assert search.refused_items == (refused,)
        assert search.requests <= 2 * math.ceil(math.log2(n))


def test_one_refused_group_among_250_costs_sixteen_requests() -> None:
    items = [f"S{i:03d}" for i in range(250)]
    costs = set()
    for refused in items:
        search = search_of(items)
        while not search.done:
            answer(search, {refused})
        assert search.refused_items == (refused,)
        costs.add(search.requests)
    assert max(costs) == 16  # two per level, ceil(log2(250)) = 8 levels


@pytest.mark.parametrize("one_at_a_time", [False, True])
def test_the_search_never_costs_two_times_the_groups(one_at_a_time: bool) -> None:
    for items, refused in _cases():
        search, _ = _searched(items, refused, one_at_a_time=one_at_a_time, name_every=0)
        assert search.requests <= 2 * len(items) - 2, (items, refused)


def test_the_order_of_items_never_changes_which_are_refused() -> None:
    for items in (["IBM", "AAPL", "XYZ", "MSFT"], ["AAPL", "IBM", "MSFT", "XYZ"]):
        search = search_of(items)
        while not search.done:
            answer(search, {"IBM", "XYZ"})
        assert sorted(search.refused_items) == ["IBM", "XYZ"]
