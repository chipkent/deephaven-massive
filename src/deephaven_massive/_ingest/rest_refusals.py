"""Finding which symbols of a REST request of several symbols Massive refuses.

Massive answers a request that lists several symbols with one answer. When that answer is a
refusal, refusing every symbol it listed would leave an entitled symbol out for good because of
another, so a read by symbol (`RestReader.read_each`) sends the request again in parts:

- **A refusal that names its symbols** (`SymbolRefusalError`: rows of a page labelled delayed
  for a subscription the configuration does not list) refuses exactly those symbols, and the
  request is sent again for the others (`drop_named_refusals`). Symbols one wire spelling
  names (`wire_group`) are refused together, since no request can send them apart.
- **A refusal that names no symbol** (HTTP 403, a `NOT_AUTHORIZED` envelope) is searched
  (`search_refusals`, `refusal_search.RefusalSearch`): split by asset class, then into halves,
  the parts of every search sent together, one round per level, until each symbol is answered,
  refused, or failed for another reason.
- **A refusal of the feed** (`FeedRefusalError`) is neither: no symbol escapes it, so it fails
  the whole read wherever it comes, a part of a search included (`send` raises it).

Each part's answer is its own symbols' alone, so no symbol gets rows twice or loses them: the
refused request had none. The functions here decide what to send and read the answers; the
caller sends each round (`send`), so a failure of the whole read raises from there.
"""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from deephaven_massive._datasets.symbols import symbol_class
from deephaven_massive._errors import EntitlementError, MassiveError, SymbolRefusalError
from deephaven_massive._ingest.refusal_search import RefusalSearch
from deephaven_massive._ingest.rest_requests import listed_part, wire_group
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._tables.symbol_retry import is_symbol_refusal

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._ingest.symbol_reads import RestUnit

__all__ = ["drop_named_refusals", "search_refusals"]

logger = logging.getLogger(__name__)

Answer = TypeVar("Answer")
"""What an answered request gives: its rows (`rest._Outcome`)."""


def _class_name(symbol: str) -> str:
    """Return a symbol's asset class, by which a search splits a refused request first.

    Args:
        symbol: A canonical symbol.

    Returns:
        Its class's name (`symbol_class`), such as `"indices"`.
    """
    return symbol_class(symbol).value


def _named_split(
    spec: DatasetSpec, unit: RestUnit, error: SymbolRefusalError
) -> tuple[RestUnit, RestUnit] | None:
    """Split a request whose refusal names some of its symbols into the refused and the rest.

    Args:
        spec: The dataset.
        unit: A planned request and its path.
        error: Massive's refusal of it, which names the refused symbols.

    Returns:
        The request narrowed to the refused symbols, and to the others, each with its path;
        None when the request names one symbol, when every symbol it names shares a wire
        spelling with a refused one (nothing is left to send again), or when the refusal names
        none of them (the request would be sent again as it was).
    """
    rest_path, call = unit
    if call.symbol is not None:
        return None
    refused = {wire_group(spec, s) for s in call.symbols if s in error.symbols}
    rest = tuple(s for s in call.symbols if wire_group(spec, s) not in refused)
    if not rest or len(rest) == len(call.symbols):
        return None
    gone = tuple(s for s in call.symbols if s not in rest)
    return (rest_path, listed_part(spec, call, gone)), (rest_path, listed_part(spec, call, rest))


def drop_named_refusals(
    spec: DatasetSpec,
    units: Sequence[RestUnit],
    done: Sequence[Answer | MassiveError],
    *,
    send: Callable[[Sequence[RestUnit]], list[Answer | MassiveError]],
) -> tuple[list[RestUnit], list[Answer | MassiveError]]:
    """Refuse exactly the symbols a refusal names, and send each such request again without them.

    A request sent again may be refused for other symbols it names, so this repeats, one round
    each time, until no answer names a symbol of a request that it leaves served; each round
    drops at least one symbol, so it ends.

    Args:
        spec: The dataset.
        units: The requests sent.
        done: Each request's outcome or failure, in order.
        send: Sends one round of requests.

    Returns:
        The requests the outcomes answer, each in its place or replaced by the part refused
        (with the refusal) and the part sent again (with its answer).

    Errors are those of `send`: a failure of the whole read raises.
    """
    units, done = list(units), list(done)
    while True:
        splits = {
            index: split
            for index, (unit, outcome) in enumerate(zip(units, done, strict=True))
            if isinstance(outcome, SymbolRefusalError)
            and (split := _named_split(spec, unit, outcome)) is not None
        }
        if not splits:
            return units, done
        for index, (gone, rest) in splits.items():
            logger.debug(
                "%s: Massive refused %s in the request of %s for %s; sending it again for %s",
                spec.name,
                bounded_list(gone[1].symbols),
                rest[0].template,
                bounded_list(units[index][1].symbols),
                bounded_list(rest[1].symbols),
            )
        answers = iter(send([rest for _, rest in splits.values()]))
        next_units: list[RestUnit] = []
        next_done: list[Answer | MassiveError] = []
        for index, (unit, outcome) in enumerate(zip(units, done, strict=True)):
            if index in splits:
                gone, rest = splits[index]
                next_units.extend((gone, rest))
                next_done.extend((outcome, next(answers)))
            else:
                next_units.append(unit)
                next_done.append(outcome)
        units, done = next_units, next_done


@dataclass(slots=True)
class _SearchedRequest(Generic[Answer]):
    """A listed request Massive refused whole, and the search for the symbols it refuses.

    Attributes:
        unit: The refused request and its path.
        search: The search over its symbols (`RefusalSearch`).
        refusals: Each symbol's refusal so far: Massive's answer to the smallest refused part
            that held it, the request's own refusal until a part is refused.
        answered: Each part Massive answered, with its outcome, in the order answered.
    """

    unit: RestUnit
    search: RefusalSearch[MassiveError]
    refusals: dict[str, MassiveError]
    answered: list[tuple[RestUnit, Answer]]

    @classmethod
    def of(
        cls, spec: DatasetSpec, unit: RestUnit, outcome: Answer | MassiveError
    ) -> _SearchedRequest[Answer] | None:
        """Start the search of a request's refusal, when its symbols can be told apart.

        A refusal of the feed (`FeedRefusalError`) is never searched, since no symbol escapes
        it, nor one that names its symbols (`SymbolRefusalError`, `drop_named_refusals`), nor
        a request whose symbols can't be sent apart: a per-symbol request, or the symbols of
        one wire spelling (`wire_group`). A request of several wire spellings lists them
        (`listed_part`).

        Args:
            spec: The dataset.
            unit: A planned request and its path.
            outcome: Its outcome or failure.

        Returns:
            The search, or None when the outcome is not a refusal to search.
        """
        rest_path, call = unit
        if not isinstance(outcome, MassiveError) or not is_symbol_refusal(outcome):
            return None
        if isinstance(outcome, SymbolRefusalError):
            return None  # it names its symbols: nothing to search
        if call.symbol is not None or len({wire_group(spec, s) for s in call.symbols}) <= 1:
            return None  # a per-symbol request, or one wire spelling: nothing smaller to send
        logger.debug(
            "%s: Massive refused the request of %s for %s whole; sending it again in parts to "
            "find the symbols it refuses: %s",
            spec.name,
            rest_path.template,
            bounded_list(call.symbols),
            outcome,
        )
        search: RefusalSearch[MassiveError] = RefusalSearch(
            call.symbols, group=functools.partial(wire_group, spec), class_of=_class_name
        )
        return cls(unit, search, dict.fromkeys(call.symbols, outcome), [])

    def part_unit(self, spec: DatasetSpec, part: Sequence[str]) -> RestUnit:
        """Return the request for a part of the refused request's symbols.

        Args:
            spec: The dataset.
            part: A part the search has pending.

        Returns:
            The refused request narrowed to `part` (`listed_part`), with its path.
        """
        rest_path, call = self.unit
        return rest_path, listed_part(spec, call, part)

    def take(self, part: tuple[str, ...], unit: RestUnit, outcome: Answer | MassiveError) -> None:
        """Take Massive's answer to a part.

        Args:
            part: The part, as the search gave it.
            unit: The request sent for it.
            outcome: Its outcome or failure: rows are its symbols', a refusal that names its
                symbols refuses those and sends the rest again, any other refusal is searched
                further, and any other failure (a 5xx, a timeout) is its symbols' own. A
                refusal of the feed never comes here: `send` raises it.
        """
        if isinstance(outcome, SymbolRefusalError):
            # It names its symbols: refuse those, and send the rest of the part again.
            named = self.search.refused_by_name(part, outcome.symbols)
            self.refusals.update(dict.fromkeys(named, outcome))
        elif isinstance(outcome, EntitlementError):
            self.search.refused(part)
            self.refusals.update(dict.fromkeys(part, outcome))
        elif isinstance(outcome, MassiveError):
            self.search.failed(part, outcome)
        else:
            self.search.answered(part)
            self.answered.append((unit, outcome))

    def leaves(self, spec: DatasetSpec) -> list[tuple[RestUnit, Answer | MassiveError]]:
        """Return the parts that answer each symbol, once the search is done.

        Args:
            spec: The dataset.

        Returns:
            Each answered part with its outcome, then one part per failure, with the symbols
            that failure is theirs: each refused symbol with its refusal, each failed one with
            its error. Every symbol of the request is in exactly one.
        """
        failures = {
            **{item: self.refusals[item] for item in self.search.refused_items},
            **self.search.failed_items,
        }
        by_error: dict[int, tuple[MassiveError, list[str]]] = {}
        for item, error in failures.items():
            by_error.setdefault(id(error), (error, []))[1].append(item)
        rest_path, call = self.unit
        logger.debug(
            "%s: the search of the request of %s for %s cost %d more requests: %d symbols "
            "answered; refused: %s; failed otherwise: %s",
            spec.name,
            rest_path.template,
            bounded_list(call.symbols),
            self.search.requests,
            len(call.symbols) - len(failures),
            bounded_list(self.search.refused_items) if self.search.refused_items else "none",
            bounded_list(self.search.failed_items) if self.search.failed_items else "none",
        )
        answered: list[tuple[RestUnit, Answer | MassiveError]] = list(self.answered)
        return [
            *answered,
            *((self.part_unit(spec, items), error) for error, items in by_error.values()),
        ]


def search_refusals(
    spec: DatasetSpec,
    units: Sequence[RestUnit],
    done: Sequence[Answer | MassiveError],
    *,
    send: Callable[[Sequence[RestUnit]], list[Answer | MassiveError]],
) -> tuple[list[RestUnit], list[Answer | MassiveError]]:
    """Find which symbols of each refused listed request Massive refuses, by sending parts.

    Each request Massive refused whole without naming a symbol is searched
    (`_SearchedRequest`): split by asset class, then into halves, the parts of every search
    sent together, one round per level. Two refused halves of two groups or more are probed,
    one group of each sent alone, and two refused probes end the search with both halves
    refused (`refusal_search`), so a refusal of every symbol of a class costs four more
    requests, and finding one refused symbol among `n` about `2 * ceil(log2(n))` plus two
    per probe.

    Args:
        spec: The dataset.
        units: The requests sent.
        done: Each request's outcome or failure, in order.
        send: Sends one round of requests.

    Returns:
        The requests the outcomes answer, each in its place or, once searched, the parts that
        answered its symbols, with each one's outcome or failure: a refused part's error is
        Massive's refusal of the smallest part that held its symbols.

    Errors are those of `send`: a failure of the whole read raises.
    """
    searched = {
        index: found
        for index, (unit, outcome) in enumerate(zip(units, done, strict=True))
        if (found := _SearchedRequest.of(spec, unit, outcome)) is not None
    }
    while not all(one.search.done for one in searched.values()):
        sent = [
            (one, part, one.part_unit(spec, part))
            for one in searched.values()
            for part in one.search.pending()
        ]
        answers = send([unit for _, _, unit in sent])
        for (one, part, unit), outcome in zip(sent, answers, strict=True):
            one.take(part, unit, outcome)
    leaves: list[RestUnit] = []
    results: list[Answer | MassiveError] = []
    for index, (unit, outcome) in enumerate(zip(units, done, strict=True)):
        parts = searched[index].leaves(spec) if index in searched else [(unit, outcome)]
        leaves.extend(part for part, _ in parts)
        results.extend(answer for _, answer in parts)
    return leaves, results
