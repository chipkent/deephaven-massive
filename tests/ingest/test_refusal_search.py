"""Tests for the search for the items Massive refuses among those of one refused request."""

from __future__ import annotations

import pytest

from deephaven_massive._errors import BrokenGuaranteeError
from deephaven_massive._ingest.refusal_search import RefusalSearch


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


def test_two_refused_halves_end_the_search_with_every_item_refused() -> None:
    items = [f"S{i:03d}" for i in range(250)]
    search = search_of(items)
    answer(search, set(items))
    # The plan lacks the data: two requests, no further halving.
    assert search.done
    assert search.requests == 2
    assert sorted(search.refused_items) == items
    assert search.failed_items == {}


def test_a_refused_half_beside_an_answered_one_splits_down_to_one_item() -> None:
    items = [f"S{i:03d}" for i in range(250)]
    search = search_of(items)
    while not search.done:
        answer(search, {"S137"})
    assert search.refused_items == ("S137",)
    assert search.requests == 16  # two per level, ceil(log2(250)) = 8 levels


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


def test_a_half_refused_after_its_waiting_sibling_refuses_both() -> None:
    search = search_of(["A", "B", "C", "D"])
    search.refused(("A", "B"))
    search.refused(("C", "D"))
    assert search.done
    assert search.refused_items == ("A", "B", "C", "D")
    assert search.requests == 2


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
