"""Generate the WebSocket sources from the snapshot: every channel of the specification.

`generate_ws` reads the snapshot's WebSocket specification and WebSocket documentation pages
and returns one `Source` per generated channel, with an account of every channel
(`WsCoverage`), or raises `RefreshError` with every problem found. Nothing here touches the
network or a file.

**Which channels are generated.** The specification is an OpenAPI document whose paths are
the channels: `/<cluster>/<channel>` (`/stocks/T`), or `/business/<cluster>/<channel>` for the
fair-market-value feeds, which Massive serves from `business.massive.com`. Every path is
exactly one of:

- *deprecated* (`x-polygon-deprecation` on its operation, or `deprecated: true`): accounted
  for by the vendor and never generated;
- *excluded*: named, as the specification spells it, by an `exclusions.EXCLUSIONS` entry of
  transport `WS`, with a reason;
- *generated*: everything else.

**Keys.** A generated channel's `Source.key` is `"<cluster>/<channel>"` (`"stocks/T"`,
`"stocks/FMV"`), since stocks, options, and futures share channel names. A path of any other
shape, or two paths with one key, stops the refresh (`shape`).

**Documentation.** Each WebSocket page the index links names its feed: the cluster in its
connection URLs and the channel prefix. Every generated channel must be named by a page, and
every page that names a feed must name a channel of the specification (`docs`); overview and
quickstart pages name none. The pages are the source's vendor URLs.

**Fields** are the message's properties in specification order, after `$ref` and `allOf` are
resolved; the event-type key `ev`, which only says which channel the message is, is dropped.
`required` is copied where the vendor gives it. Names, types, units, conversions, and the
documented example are decided by the rules every transport shares (`fields`).

It imports the standard library and other `_massive` modules only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven_massive._massive.refresh import docs
from deephaven_massive._massive.refresh.fields import RawField, SourceFields
from deephaven_massive._massive.refresh.gates import Gate, PathUse, Problem, RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.openapi import OpenApi, properties, required
from deephaven_massive._massive.spec.types import Source, Transport

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deephaven_massive._massive.refresh.openapi import JsonObject
    from deephaven_massive._massive.refresh.snapshot import Snapshot

__all__ = ["WsCoverage", "channel_key", "generate_ws"]

# The event-type key of every message: it names the channel, never a field of the row.
_EVENT_KEY: Final = "ev"
# A specification path: an optional host segment, the cluster, and the channel.
_CHANNEL_PATH: Final = re.compile(
    r"/(?:(?P<host>[a-z]+)/)?(?P<cluster>[a-z]+)/(?P<channel>[A-Z0-9]+)"
)
# The path segments that may come before `<cluster>/<channel>`, each with what it means.
_HOST_SEGMENTS: Final = MappingProxyType(
    {
        "business": "The fair-market-value feeds, served from business.massive.com; the "
        "channel belongs to the cluster that follows.",
    }
)
# The vendor extensions a channel's operation may carry, each with what the generator does
# about it. Any other `x-polygon-*` key is a change at Massive the refresh stops on.
_OPERATION_EXTENSIONS: Final = MappingProxyType(
    {
        "x-polygon-deprecation": "The vendor is retiring the channel: never generated.",
        "x-polygon-entitlement-allowed-timeframes": "Which plans see real-time or delayed "
        "data; the library learns entitlements from the server, not from the specification.",
        "x-polygon-entitlement-data-type": "The plan family the channel belongs to; learned "
        "from the server, like every entitlement.",
        "x-polygon-entitlement-market-type": "The market the plan covers; learned from the "
        "server, like every entitlement.",
    }
)
# The documentation pages that describe an area rather than one feed: they name none.
_OVERVIEW_PAGES: Final = frozenset({"overview", "quickstart"})


@dataclass(frozen=True, kw_only=True)
class WsCoverage:
    """How every channel of the WebSocket specification is accounted for.

    Attributes:
        specified: Every path of the specification, as it spells it (`"/stocks/T"`).
        generated: The keys of the generated sources (`"stocks/T"`).
        excluded: The paths named by an exclusion.
        deprecated: The paths the specification marks deprecated.
        pages: Each source key a WebSocket page names, mapped to the names of the pages that
            name it, sorted.
        uses: What generating each attempted source consumed, by source key.
    """

    specified: frozenset[str]
    generated: frozenset[str]
    excluded: frozenset[str]
    deprecated: frozenset[str]
    pages: Mapping[str, tuple[str, ...]]
    uses: Mapping[str, PathUse]


def channel_key(path: str) -> str | None:
    """Return the source key of a WebSocket specification path.

    Args:
        path: A path of the specification, such as `"/stocks/T"` or `"/business/stocks/FMV"`.

    Returns:
        `"<cluster>/<channel>"` (`"stocks/T"`, `"stocks/FMV"`), or `None` for a path of any
        other shape.
    """
    found = _CHANNEL_PATH.fullmatch(path)
    if found is None or (found["host"] is not None and found["host"] not in _HOST_SEGMENTS):
        return None
    return f"{found['cluster']}/{found['channel']}"


class _ChannelGenerator(SourceFields):
    """Generate one channel's `Source`, collecting its problems instead of stopping."""

    def generate(self, item: JsonObject) -> Source | None:
        """Build the channel's source from its path item.

        A `RefreshError` from reading the specification on the way (a reference that cannot be
        followed, say) propagates; the problems recorded before it stay in `problems`.

        Args:
            item: The resolved path item.

        Returns:
            The source, or `None` when a problem prevents building it.
        """
        try:
            operation = self.spec.deref(item.get("get"), self.source)
            responses = self.spec.deref(operation.get("responses"), self.source)
            ok = self.spec.deref(responses.get("200"), self.source)
            content = self.spec.deref(ok.get("content"), self.source)
            media = self.spec.deref(content.get("application/json"), self.source)
            message = self.spec.schema(media.get("schema"), self.source)
        except RefreshError:
            self.problem(Gate.SHAPE, "", "no JSON message schema for the channel")
            return None
        found = properties(message)
        if found is None:
            self.problem(Gate.SHAPE, "", "the message is not an object")
            return None
        needed = required(message)
        raws = [
            RawField(name, self.spec.schema(schema, f"{self.source} {name}"), name in needed)
            for name, schema in found.items()
            if name != _EVENT_KEY
        ]
        fields = [f for raw in raws if (f := self.field(raw)) is not None]
        self.check_columns(fields)
        example = media.get("example")
        self.check_examples(fields, [example], documented=example is not None, where="")
        if self.problems:
            return None
        # The message's keys are distinct, and `check_columns` has passed, so the source's
        # invariants hold.
        return Source(key=self.source, transport=Transport.WS, fields=tuple(fields))


def _deprecated(item: JsonObject) -> bool:
    """Return whether the vendor marks a channel deprecated.

    Args:
        item: The resolved path item.

    Returns:
        True for `x-polygon-deprecation` or `deprecated: true` on its operation.
    """
    operation = item.get("get")
    return isinstance(operation, dict) and (
        "x-polygon-deprecation" in operation or operation.get("deprecated") is True
    )


def _check_extensions(spec: OpenApi, paths: JsonObject) -> Iterator[Problem]:
    """Report the vendor extensions on a channel or its operation that the refresh does not know.

    A channel's path item carries none; its operation carries only `_OPERATION_EXTENSIONS`.

    Args:
        spec: The specification.
        paths: Its `paths` object.

    Yields:
        One `shape` problem per unknown `x-polygon-*` key.
    """
    for path, node in paths.items():
        item = spec.deref(node, path)
        operation = item.get("get")
        found = {k for k in item if k.startswith("x-polygon-")}
        if isinstance(operation, dict):
            found |= {
                k
                for k in operation
                if k.startswith("x-polygon-") and k not in _OPERATION_EXTENSIONS
            }
        for key in sorted(found):
            yield Problem(gate=Gate.SHAPE, source=path, key=key, message="unknown vendor extension")


def _documented_feeds(snapshot: Snapshot, problems: list[Problem]) -> dict[str, tuple[str, ...]]:
    """Map each feed a WebSocket page names to every page that names it.

    Args:
        snapshot: The snapshot.
        problems: Where an unreadable page, or a page that is not an overview yet names no
            feed, is reported (`docs`).

    Returns:
        Source key (`"stocks/T"`) to page names, sorted.
    """
    feeds: dict[str, tuple[str, ...]] = {}
    for name, page in sorted(snapshot.websocket_pages.items()):
        try:
            found = docs.feed(page)
        except docs.DocsError as error:
            problems.append(Problem(gate=Gate.DOCS, source="", key=name, message=str(error)))
            continue
        if found is not None:
            key = "/".join(found)
            feeds[key] = (*feeds.get(key, ()), name)
        elif PurePosixPath(name).stem not in _OVERVIEW_PAGES:
            problems.append(
                Problem(
                    gate=Gate.DOCS,
                    source="",
                    key=name,
                    message="a WebSocket page that is not an overview names no feed",
                )
            )
    return feeds


def _check_exclusions(
    hand: HandInput, paths: Mapping[str, bool], problems: list[Problem]
) -> frozenset[str]:
    """Check each WebSocket exclusion against the specification.

    Args:
        hand: The hand input.
        paths: Each specification path, mapped to whether it is deprecated.
        problems: Where an exclusion of a path the specification lacks, or already marks
            deprecated, is reported (`stale-entry`).

    Returns:
        The excluded paths.
    """
    excluded: set[str] = set()
    for entry in hand.exclusions:
        if entry.kind is not Transport.WS:
            continue
        if entry.item not in paths:
            message = "EXCLUSIONS entry for a channel the specification lacks"
        elif paths[entry.item]:
            message = (
                "EXCLUSIONS entry for a channel the specification marks deprecated, which "
                "already accounts for it"
            )
        else:
            excluded.add(entry.item)
            continue
        problems.append(Problem(gate=Gate.STALE_ENTRY, source=entry.item, key="", message=message))
    return frozenset(excluded)


def generate_ws(
    snapshot: Snapshot, hand: HandInput | None = None
) -> tuple[tuple[Source, ...], WsCoverage]:
    """Generate every WebSocket source of the snapshot and check the gates met per channel.

    The checks that span sources (stale hand entries, the dataset kinds) run afterwards, over
    every transport, in `generate`.

    Args:
        snapshot: The vendored sources.
        hand: The hand input; the real tables when omitted.

    Returns:
        The generated sources in specification order, and how every channel is accounted for.

    Raises:
        RefreshError: With every problem found, if there is any.
    """
    hand = hand or HandInput()
    spec = OpenApi(snapshot.websocket)
    problems: list[Problem] = []
    feeds = _documented_feeds(snapshot, problems)
    paths = spec.deref(snapshot.websocket.get("paths", {}), "paths")
    deprecated = {path: _deprecated(spec.deref(item, path)) for path, item in paths.items()}
    problems += _check_extensions(spec, paths)
    excluded = _check_exclusions(hand, deprecated, problems)
    uses: dict[str, PathUse] = {}
    generated: list[Source] = []
    for path, item in paths.items():
        if deprecated[path] or path in excluded:
            continue
        key = channel_key(path)
        if key is None or key in uses:
            what = "a path that is not /<cluster>/<channel>" if key is None else "a second path"
            problems.append(
                Problem(gate=Gate.SHAPE, source=path, key="", message=f"{what} for one channel")
            )
            continue
        generator = _ChannelGenerator(spec, hand, key, origin=snapshot.origin)
        try:
            value = generator.generate(spec.deref(item, path))
        except RefreshError as error:
            problems += error.problems
            value = None
        # Kept even when the channel stopped, so its hand entries are not also reported stale.
        uses[key] = generator.use()
        problems += generator.problems
        if value is not None:
            generated.append(value)
        if key not in feeds:
            problems.append(
                Problem(
                    gate=Gate.DOCS,
                    source=key,
                    key="",
                    message="generated, but no WebSocket page names its feed",
                )
            )
    specified = {channel_key(path) for path in paths}
    problems += [
        Problem(
            gate=Gate.DOCS,
            source=key,
            key="",
            message=f"{', '.join(names)} documents a channel the specification lacks",
        )
        for key, names in feeds.items()
        if key not in specified
    ]
    if problems:
        raise RefreshError(problems)
    coverage = WsCoverage(
        specified=frozenset(paths),
        generated=frozenset(s.key for s in generated),
        excluded=excluded,
        deprecated=frozenset(p for p, d in deprecated.items() if d),
        pages=feeds,
        uses=uses,
    )
    return tuple(generated), coverage
