"""Regenerate the committed wire schemas from the vendored snapshot: the refresh's one entry.

`refresh()` is what `python -m deephaven_massive._massive.refresh` runs. It composes the
other modules and adds only the rule for writing:

1. `snapshot.Snapshot.load` reads the vendored snapshot and verifies it against its manifest;
2. `generate` runs the three generators, `rest_sources.generate_rest` (the OpenAPI specification),
   `ws_sources.generate_ws` (the WebSocket specification), and
   `flat_file_sources.generate_flat_files` (the flat-file pages), each checking the gates met per
   source; then the checks that span sources and transports (`gates`: stale hand entries,
   conversions no sample needs any more, one concept per column; `kinds`: one type per column,
   columns on two transports, the column order), and finally writes each source's memberships and
   orders its fields from its kind (`kinds.apply`), and writes the keys `IGNORED_KEYS` lists for
   it (`Source.ignored_keys`);
3. `render` writes the sources as the canonical text of `spec/rest.json`, `spec/ws.json`,
   `spec/flatfiles.json`, and `spec/descriptions.json` (which also holds each documenting
   page's plan tables, `plan_access`); each file is replaced only when every
   gate passed and its text changed, so a refresh that only rewords descriptions leaves the
   schema files untouched.

A stop at any step raises `gates.RefreshError` with every problem found, and nothing is
written: the per-source problems of all three generators together, or else every problem of
the checks that span them. Nothing here touches the network. The refresh runs from a source
checkout only: neither the refresh package nor the snapshot ships in a distribution.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from deephaven_massive._massive.refresh import gates, kinds, render
from deephaven_massive._massive.refresh.flat_file_sources import (
    FlatFileCoverage,
    generate_flat_files,
)
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem, RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.plan_access import plan_access
from deephaven_massive._massive.refresh.rest_sources import Coverage, generate_rest
from deephaven_massive._massive.refresh.snapshot import Snapshot
from deephaven_massive._massive.refresh.ws_sources import WsCoverage, generate_ws
from deephaven_massive._massive.spec.types import PlanAccess, RestPath, Source, Transport

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from pathlib import Path

# What a generator returns: its sources and its coverage.
_Result = TypeVar("_Result")

__all__ = ["Generated", "generate", "refresh"]


@dataclass(frozen=True, kw_only=True)
class Generated:
    """Every generated source of a snapshot, with how each transport's items are accounted for.

    Created by `generate`. Immutable.

    Attributes:
        rest: The REST paths, in specification order.
        ws: The WebSocket channels, in specification order.
        flat_files: The flat-file prefixes, in overview order.
        rest_coverage: How every path of the OpenAPI specification is accounted for.
        ws_coverage: How every channel of the WebSocket specification is accounted for.
        flat_file_coverage: How every documented flat-file prefix is accounted for.
        urls: Per transport, each generated source's vendor URLs: every documentation page
            that names it (for REST, every page whose endpoint line is its path; for the
            WebSocket, every page of its feed; for flat files, its dataset page, one per venue
            for a futures dataset).
        plans: What each of those pages says about Massive's plans, by the page's URL
            (`plan_access.plan_access`); a page without plan tables has no entry.
    """

    rest: tuple[RestPath, ...]
    ws: tuple[Source, ...]
    flat_files: tuple[Source, ...]
    rest_coverage: Coverage
    ws_coverage: WsCoverage
    flat_file_coverage: FlatFileCoverage
    urls: Mapping[Transport, Mapping[str, list[str]]]
    plans: Mapping[str, PlanAccess]


def generate(snapshot: Snapshot, hand: HandInput | None = None) -> Generated:
    """Generate every source of every transport and check every gate.

    A `RefreshError` propagates with every problem the generators found; or, when they found
    none, with every problem of the checks that span sources and transports.

    Args:
        snapshot: The vendored sources.
        hand: The hand input; the real tables when omitted.

    Returns:
        The generated sources, each with its memberships and its fields in its kind's order,
        their pages' URLs, and those pages' plan tables.

    Raises:
        DocsError: If a page that documents a generated source has plan sections of an
            unexpected shape (`plan_access.plan_access`).
    """
    hand = hand or HandInput()
    (paths, rest), (channels, ws), (files, flat) = _generate_each(snapshot, hand)
    found, orders = _check_kinds(
        hand, (*paths, *channels, *files), {**rest.uses, **ws.uses, **flat.uses}
    )
    rest_urls, ws_urls, file_urls = (
        snapshot.page_urls,
        snapshot.websocket_page_urls,
        snapshot.flat_file_page_urls,
    )
    plans: dict[str, PlanAccess] = {}
    for texts, page_urls, by_key, keys in (
        (snapshot.pages, rest_urls, rest.pages, [p.key for p in paths]),
        (snapshot.websocket_pages, ws_urls, ws.pages, [c.key for c in channels]),
        (snapshot.flat_file_pages, file_urls, flat.pages, [f.key for f in files]),
    ):
        for name in sorted({name for key in keys for name in by_key[key]}):
            page = plan_access(texts[name])
            if page is not None:
                plans[page_urls[name]] = page
    return Generated(
        rest=_ignoring(kinds.apply(paths, found, orders), hand),
        ws=_ignoring(kinds.apply(channels, found, orders), hand),
        flat_files=_ignoring(kinds.apply(files, found, orders), hand),
        rest_coverage=rest,
        ws_coverage=ws,
        flat_file_coverage=flat,
        urls={
            Transport.REST: {p.key: [rest_urls[n] for n in rest.pages[p.key]] for p in paths},
            Transport.WS: {c.key: [ws_urls[n] for n in ws.pages[c.key]] for c in channels},
            Transport.FLAT_FILES: {f.key: [file_urls[n] for n in flat.pages[f.key]] for f in files},
        },
        plans=plans,
    )


_S = TypeVar("_S", bound=Source)


def _ignoring(sources: Iterable[_S], hand: HandInput) -> tuple[_S, ...]:
    """Return the sources, each with the keys `IGNORED_KEYS` lists for it.

    Args:
        sources: Generated sources of one transport. A stale entry has already stopped the
            refresh (`gates.stale_entries`), so every entry names a key no source declares.
        hand: The hand input.

    Returns:
        The sources in order, each with its `ignored_keys`.
    """
    ignored: dict[str, set[str]] = {}
    for entry in hand.ignored_keys:
        ignored.setdefault(entry.source, set()).add(entry.wire_key)
    return tuple(
        dataclasses.replace(source, ignored_keys=frozenset(ignored[source.key]))
        if source.key in ignored
        else source
        for source in sources
    )


def _generate_each(
    snapshot: Snapshot, hand: HandInput
) -> tuple[
    tuple[tuple[RestPath, ...], Coverage],
    tuple[tuple[Source, ...], WsCoverage],
    tuple[tuple[Source, ...], FlatFileCoverage],
]:
    """Run the three generators, collecting the problems of all of them before stopping.

    Args:
        snapshot: The vendored sources.
        hand: The hand input.

    Returns:
        Each generator's sources and coverage: REST, WebSocket, flat files.

    Raises:
        RefreshError: With every problem of every generator, if any found one.
    """
    problems: list[Problem] = []
    rest = _attempt(generate_rest, snapshot, hand, problems=problems)
    ws = _attempt(generate_ws, snapshot, hand, problems=problems)
    flat_files = _attempt(generate_flat_files, snapshot, hand, problems=problems)
    if rest is None or ws is None or flat_files is None:
        raise RefreshError(problems)
    return rest, ws, flat_files


def _attempt(
    generator: Callable[[Snapshot, HandInput], _Result],
    snapshot: Snapshot,
    hand: HandInput,
    *,
    problems: list[Problem],
) -> _Result | None:
    """Run one generator, keeping its problems instead of stopping.

    Args:
        generator: The generator.
        snapshot: The vendored sources.
        hand: The hand input.
        problems: Where the generator's problems go when it stops.

    Returns:
        What the generator returns, or `None` when it stopped on a problem.
    """
    try:
        return generator(snapshot, hand)
    except RefreshError as error:
        problems += error.problems
        return None


def _check_kinds(
    hand: HandInput, sources: tuple[Source, ...], uses: Mapping[str, PathUse]
) -> tuple[list[kinds.Member], dict[str, tuple[str, ...]]]:
    """Run the checks that span sources and transports, and order the kinds' columns.

    Args:
        hand: The hand input.
        sources: Every generated source of every transport.
        uses: What generating each attempted source consumed, by key.

    Returns:
        The kinds' members and each reordered source's columns in order.

    Raises:
        RefreshError: With every problem found: two transports' sources sharing a key, a
            stale hand entry, a conversion no sample needs, two concepts in one column, a
            stale `KINDS` or `PROJECTIONS` entry, a type or transport disagreement, or a
            column order that cannot be taken from a kind.
    """
    problems: list[Problem] = []
    by_key: dict[str, Source] = {}
    for source in sources:
        if source.key in by_key:
            problems.append(
                Problem(
                    gate=Gate.SHAPE,
                    source=source.key,
                    key="",
                    message="two transports generate a source with this key",
                )
            )
        by_key[source.key] = source
    problems += gates.stale_entries(hand, uses)
    problems += gates.unneeded_type_fixes(hand, uses)
    problems += gates.unused_description_fixes(hand, uses)
    problems += gates.concept_collisions(hand, uses)
    found, member_problems = kinds.members(hand, by_key)
    problems += member_problems
    problems += kinds.type_agreement(found)
    problems += kinds.transport_agreement(hand, found)
    orders, order_problems = kinds.column_orders(found, uses)
    problems += order_problems
    if problems:
        raise RefreshError(problems)
    return found, orders


def refresh(
    snapshot_directory: Path | None = None,
    spec_directory: Path | None = None,
    hand: HandInput | None = None,
) -> list[Path]:
    """Regenerate `spec/*.json` from the snapshot.

    Each file is written only when every gate passes and its text changes. A `RefreshError`
    from loading the snapshot or from a gate propagates, and nothing is written.

    Args:
        snapshot_directory: The snapshot to read; `snapshot.DIRECTORY` when omitted.
        spec_directory: The directory to write the files to; `render.SPEC_DIRECTORY` when
            omitted. Both defaults are read when the function runs, so a test can point them
            elsewhere.
        hand: The hand input; the real tables when omitted.

    Returns:
        The files written (those whose text changed), in the order `rest.json`, `ws.json`,
        `flatfiles.json`, `descriptions.json`; empty when all were already current.
    """
    snapshot = Snapshot.load(snapshot_directory)
    directory = spec_directory or render.SPEC_DIRECTORY
    generated = generate(snapshot, hand)
    texts = {
        render.REST_FILE.name: render.render_rest(generated.rest),
        render.WS_FILE.name: render.render_sources(generated.ws),
        render.FLAT_FILES_FILE.name: render.render_sources(generated.flat_files),
        render.DESCRIPTIONS_FILE.name: render.render_descriptions(
            {
                Transport.REST: generated.rest,
                Transport.WS: generated.ws,
                Transport.FLAT_FILES: generated.flat_files,
            },
            fetched=snapshot.fetched,
            urls=generated.urls,
            plans=generated.plans,
        ),
    }
    written: list[Path] = []
    for name, text in texts.items():
        target = directory / name
        if target.is_file() and target.read_text(encoding="utf-8") == text:
            continue
        target.write_text(text, encoding="utf-8")
        written.append(target)
    return written
