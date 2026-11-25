"""Which bare option underlyings are indices, from Massive's contracts reference.

Massive's options open interest and unusual trades name each contract's underlying bare (`SPX`,
`AAPL`), with no CFI code or market beside it, the greeks carry no code either, and the open
interest by underlying names the bare underlying alone, with no contract, so a row alone cannot say
whether `SPX` is an index (`I:SPX`) or a stock. Massive's contracts reference can: a contract's CFI
code (ISO 10962) marks an option on an index. `OptionRootKinds` answers by two keys. For a row with
a contract, by its OCC option root (`SPXW`, `AAPL1`), which names one underlying, so one contract
of a root answers for every contract of that root (`kind`). For a row with no contract, by the bare
underlying itself, from the first contract Massive's contracts list gives for it
(`underlying_kind`). Most underlyings are stocks, so both first read Massive's list of index
tickers, once per session (`/v3/reference/tickers?market=indices`): a bare ticker not on it
(`AAPL`) is a stock's or a fund's, with no further request. Only a listed index's ticker (`SPX`)
costs a request: the reference of a root's first contract
(`/v3/reference/options/contracts/{options_ticker}`), or the contracts list's first contract of the
underlying (`/v3/reference/options/contracts?underlying_ticker=SPX&limit=1`), as of that row's
trading day. Every answer but an unknown one, asked again after an hour, is kept for the session
and shared between every caller, so a stock and an index that share a bare ticker each get their
own root's answer; by bare underlying alone they share one, the first contract's. The pre-filter
assumes the underlying of every option on an index is a ticker on Massive's index list, active or
retired, which a live test checks; an index missing from it would keep its bare ticker with no
warning. The rows the reference can't classify are withheld as `unreadable_symbols`, since their
underlying may be an index's without its `I:` (`canonical_underlyings`, `canonical_symbols`).

The REST transport (`_ingest/rest.py`) holds the session's kinds in its `RestState` and reads them
through a `RestReader`, whose patience and error conversion the lookups use, and reports the
withheld rows as problems of its table. The single-flight sharing itself is `SharedLookup`'s
(`_ingest/shared_lookup.py`).
"""

from __future__ import annotations

import enum
import logging
from datetime import date
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, Protocol, assert_never, overload

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.symbols import option_roots
from deephaven_massive._datasets.wire_symbols import is_index_option
from deephaven_massive._errors import LIBRARY_BUG, ErrorSource, MassiveError, copy_error, source_of
from deephaven_massive._ingest.rate_limits import OTHER
from deephaven_massive._ingest.rest_requests import largest_page
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup
from deephaven_massive._massive import RowFault, WireError
from deephaven_massive._tables.withheld import Withheld, mark, with_marks, withhold_rows

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._ingest.crypto_tickers import TickerListReader
    from deephaven_massive._massive import Cancel, Page, Patience, RestClient

__all__ = [
    "REREAD_AFTER",
    "KindOf",
    "OptionRootKinds",
    "RootKind",
    "UnderlyingKindOf",
    "canonical_symbols",
    "canonical_underlyings",
]

logger = logging.getLogger(__name__)

_CONTRACT_PATH: Final = "/v3/reference/options/contracts/{options_ticker}"
"""The contracts reference of one contract, with its CFI code."""

_CONTRACTS_PATH: Final = "/v3/reference/options/contracts"
"""The contracts list, whose first contract of an underlying carries its CFI code."""

_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that lists Massive's tickers by market."""
_INDEX_LIST_KEY: Final = "index tickers"
"""The key of the session's one index ticker list in its `SharedLookup`."""
_INDICES_FAILED: Final = "Reading Massive's index ticker list failed"
"""What failed when the index ticker list's read fails, for its messages."""
_INDEX_PREFIX: Final = "I:"

REREAD_AFTER: Final = 3600.0
"""The seconds after which a root or an underlying the reference could not classify is asked
about again, so a contract Massive adds later is found without a restart, at most once an hour
per key."""


class RootKind(enum.Enum):
    """What Massive's contracts reference says a root's contracts are options on."""

    INDEX = "index"
    """An index: the underlying's canonical ticker has the `I:` prefix."""
    OTHER = "other"
    """Anything else, such as a stock or an ETF: the bare ticker is canonical."""
    UNKNOWN = "unknown"
    """The reference has no such contract, or gives it no CFI code."""


class KindOf(Protocol):
    """What `canonical_underlyings` asks: a root's kind (`RestReader.root_kind`)."""

    def __call__(self, root: str, *, underlying: str, contract: str, day: str) -> RootKind:
        """Return a root's kind, from the bare underlying, a contract, and its trading day."""
        ...


class UnderlyingKindOf(Protocol):
    """What `canonical_symbols` asks: a bare underlying's kind (`RestReader.underlying_kind`)."""

    def __call__(self, underlying: str, *, day: str) -> RootKind:
        """Return a bare underlying's kind, from the trading day of a row that names it."""
        ...


def _kind_of(cfi: object) -> RootKind:
    """Return the kind a contract's CFI code gives its underlying.

    Args:
        cfi: The code as Massive sent it, or None where the reference gave none.

    Returns:
        `INDEX` for an option on an index (`wire_symbols.is_index_option`), `OTHER` for any
        other code, `UNKNOWN` for no code.
    """
    # A code that is not text is no code the library can read, so it counts as absent.
    if not isinstance(cfi, str):
        return RootKind.UNKNOWN
    return RootKind.INDEX if is_index_option(cfi) else RootKind.OTHER


class OptionRootKinds:
    """A session's answers about option underlyings, asked once per key and shared by every caller.

    `RestState` creates one per session, with the session's REST client; nothing is read until
    the first `kind` or `underlying_kind`. Thread-safe: the answers are kept in a `SharedLookup`
    per key, which lets one caller ask while the others wait, and every caller waits outside
    any lock.

    Example:
        ::

            kinds = OptionRootKinds(client, "MassiveSession #1")
            kinds.kind("SPXW", contract="O:SPXW261218C06000000", day="2026-11-13", reader=reader)
            kinds.underlying_kind("SPX", day="2026-11-13", reader=reader)
    """

    __slots__ = ("_client", "_indices", "_label", "_lookup", "_underlyings")

    def __init__(self, client: RestClient, label: str) -> None:
        """Hold the session's client; no request is made.

        Args:
            client: The session's REST client, whose governor paces the lookups.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self._client = client
        self._label = label
        self._lookup: Final[SharedLookup[str, RootKind]] = SharedLookup(label, client.governor)
        self._underlyings: Final[SharedLookup[str, RootKind]] = SharedLookup(label, client.governor)
        self._indices: Final[SharedLookup[str, frozenset[str]]] = SharedLookup(
            label, client.governor
        )

    def kind(
        self,
        root: str,
        *,
        underlying: str,
        contract: str,
        day: str,
        reader: TickerListReader,
        patience: Patience,
        cancel: Cancel | None = None,
    ) -> RootKind:
        """Return what a root's contracts are options on, asking Massive on first need.

        A root whose bare `underlying` is not on Massive's index list, read once per session,
        is `OTHER` with no further request. For any other root, the first time a session meets
        it, it reads the contracts reference of `contract` as of `day` and keeps the answer:
        `INDEX` when its CFI code marks an option on an index (`wire_symbols.is_index_option`),
        `OTHER` for any other code. Each request counts against Massive's limit on reference
        data. Blocks the calling thread for them, or while another thread makes them, holding
        no lock meanwhile. A failed request is not kept, so the next call asks again. An answer
        of no contract (HTTP 404) or no CFI code is `UNKNOWN`, and is asked again once it is an
        hour old or more.

        Args:
            root: The contracts' OCC root, such as `"SPXW"`.
            underlying: The bare underlying Massive's row names, such as `"SPX"`.
            contract: A canonical options ticker of that root, such as
                `"O:SPXW261218C06000000"`.
            day: The trading day of the row that names the contract, as ISO text, for
                `as_of`, so an expired contract is still found.
            reader: The read the lookup serves, whose error conversion it uses.
            patience: The patience of the read, taken on its own thread (`RestReader.patience`),
                which the lookup's pauses are charged to.
            cancel: A background read's stop token, or None.

        Returns:
            The root's kind.

        A failed request raises its public error (`AuthenticationError`, `EntitlementError`,
        or `MassiveError`), its message naming the root and the contract.
        """
        return self._classify(
            self._lookup,
            root,
            underlying=underlying,
            subject=f"the option root {root!r}",
            source=f"Massive's Contract Overview of {contract}",
            ask=lambda: self._ask(
                root,
                contract=contract,
                day=day,
                reader=reader,
                patience=patience,
                cancel=cancel,
            ),
            reader=reader,
            patience=patience,
            cancel=cancel,
        )

    def underlying_kind(
        self,
        underlying: str,
        *,
        day: str,
        reader: TickerListReader,
        patience: Patience,
        cancel: Cancel | None = None,
    ) -> RootKind:
        """Return what a bare underlying's options are on, asking Massive on first need.

        For a row that names its underlying bare with no contract beside it. A ticker not on
        Massive's index list, read once per session, is `OTHER` with no further request. For
        any other ticker, the first time a session meets it, it reads the first contract of
        Massive's contracts list for that underlying as of `day` and keeps the answer: `INDEX`
        when the contract's CFI code marks an option on an index
        (`wire_symbols.is_index_option`), `OTHER` for any other code. A ticker that names both
        a stock and an index gets the first contract's answer. Each request counts against
        Massive's limit on reference data. Blocks the calling thread for them, or while another
        thread makes them, holding no lock meanwhile. A failed request is not kept, so the next
        call asks again. An answer of no contract or no CFI code is `UNKNOWN`, and is asked again
        once it is an hour old or more.

        Args:
            underlying: The bare underlying Massive's row names, such as `"SPX"`.
            day: The latest trading day among the page's rows that name it, as ISO text, sent
                as `as_of` (`canonical_symbols`).
            reader: The read the lookup serves, whose error conversion it uses.
            patience: The patience of the read, taken on its own thread (`RestReader.patience`),
                which the lookup's pauses are charged to.
            cancel: A background read's stop token, or None.

        Returns:
            The underlying's kind.

        A failed request raises its public error (`AuthenticationError`, `EntitlementError`,
        or `MassiveError`), its message naming the underlying.
        """
        return self._classify(
            self._underlyings,
            underlying,
            underlying=underlying,
            subject=f"the underlying {underlying!r}",
            source=f"Massive's contracts list for {underlying}",
            ask=lambda: self._ask_underlying(
                underlying, day=day, reader=reader, patience=patience, cancel=cancel
            ),
            reader=reader,
            patience=patience,
            cancel=cancel,
        )

    def _classify(
        self,
        lookup: SharedLookup[str, RootKind],
        key: str,
        *,
        underlying: str,
        subject: str,
        source: str,
        ask: Callable[[], RootKind],
        reader: TickerListReader,
        patience: Patience,
        cancel: Cancel | None,
    ) -> RootKind:
        """Answer one key from its lookup, the index list, or Massive, in that order.

        Args:
            lookup: The session's answers by this kind of key.
            key: The root or the bare underlying asked about.
            underlying: The bare underlying, checked against Massive's index list.
            subject: What is asked about, for the messages, such as `"the option root 'SPXW'"`.
            source: What `ask` reads, for the messages, such as `"Massive's Contract Overview
                of O:SPXW261218C06000000"`.
            ask: Reads Massive's answer for the key and keeps it in `lookup`.
            reader: The read the lookup serves.
            patience: The read's patience.
            cancel: The caller's stop token, or None.

        Returns:
            The key's kind.

        A failed read of the index list or of `ask` raises its public `MassiveError`, its
        message naming `subject` and what failed.
        """
        known = lookup.known(key)
        if known is RootKind.UNKNOWN and lookup.retire(key, known, REREAD_AFTER):
            logger.info(
                "%s: Massive's contracts reference could not classify %s; asking again, since "
                "the answer is an hour old or more",
                self._label,
                subject,
            )
            known = None
        if known is not None:
            return known
        try:
            indices = self._index_names(reader=reader, patience=patience, cancel=cancel)
        except MassiveError as public:
            named = copy_error(
                public,
                f"The library cannot tell whether {subject} is an index, since reading "
                f"Massive's index ticker list failed. {public}",
            )
            raise named from public
        if underlying not in indices:
            logger.debug(
                "%s: %s names %s, which is not on Massive's index ticker list, so its options "
                "are not options on an index; no request",
                self._label,
                subject,
                underlying,
            )
            return RootKind.OTHER
        try:
            return ask()
        except MassiveError as public:
            named = copy_error(
                public,
                f"The library cannot tell whether {subject} is an index, since reading "
                f"{source} failed. {public}",
            )
            raise named from public

    def _index_names(
        self, *, reader: TickerListReader, patience: Patience, cancel: Cancel | None
    ) -> frozenset[str]:
        """Return the bare tickers of Massive's indices, reading the list on first need.

        The first caller reads every page of `/v3/reference/tickers?market=indices`, first the
        active indices and then the inactive ones, so a past day's index that Massive has since
        retired is still found, at the largest page the path documents; a caller that arrives
        meanwhile waits for that read within its own patience. The list is kept for the whole
        session and never read again: almost every root misses it, so a re-read on a miss, as
        the crypto list has, would read it again for every stock. A failed read is not kept, so
        the next call reads it again.

        Args:
            reader: The read the lookup serves, whose error conversion it uses.
            patience: The read's patience.
            cancel: The caller's stop token, or None.

        Returns:
            Each listed index ticker without its `I:` prefix, such as `"SPX"`.

        A failed read raises its public `MassiveError`; `kind` names the root.
        """
        known = self._indices.known(_INDEX_LIST_KEY)
        if known is not None:
            return known
        claim = self._indices.claim_or_wait(
            _INDEX_LIST_KEY,
            bucket=OTHER,
            patience=patience,
            what="the index ticker list",
            public=reader.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        try:
            names = self._read_indices(reader, patience=patience, cancel=cancel)
        except WireError as error:
            public = reader.public_error(error)
            stopped = cancel is not None and cancel.is_set() and error.status is None
            logger.debug(
                "%s: reading the index ticker list failed (status %s, stop set %s); its "
                "waiters %s: %s",
                self._label,
                error.status,
                cancel is not None and cancel.is_set(),
                "try again" if stopped else "get a copy of the error",
                public,
            )
            self._indices.fail(claim, public, _INDICES_FAILED, stopped=stopped)
            raise public from error
        except BaseException as error:
            self._indices.fail(claim, error, _INDICES_FAILED)
            if source_of(error) is ErrorSource.LIBRARY:
                logger.exception("%s. %s", _INDICES_FAILED, LIBRARY_BUG)
            raise
        self._indices.complete(claim, names)
        return names

    def _read_indices(
        self, reader: TickerListReader, *, patience: Patience, cancel: Cancel | None
    ) -> frozenset[str]:
        """Read every page of Massive's lists of active and inactive indices.

        Args:
            reader: The read the lookup serves.
            patience: The reading caller's patience, for every page.
            cancel: The reading caller's stop token, or None.

        Returns:
            Each listed index ticker without its `I:` prefix.

        A failed request raises its `WireError`.
        """
        path = reader.rest_path(_TICKERS_PATH)
        names: set[str] = set()
        pages = 0
        for active in (True, False):
            query = {"market": "indices", "active": active, "limit": largest_page(path)}
            for page in self._client.pages(
                path, {}, query, bucket=OTHER, cancel=cancel, patience=patience
            ):
                pages += 1
                for row in page.rows:
                    ticker = row.get("ticker")
                    # A row without a ticker names no index; Massive marks every row's optional.
                    if isinstance(ticker, str):
                        names.add(ticker.removeprefix(_INDEX_PREFIX))
        if names:
            logger.debug(
                "%s: read Massive's index ticker list: %d indices in %d pages",
                self._label,
                len(names),
                pages,
            )
        else:
            logger.warning(
                "Massive's index ticker list, %s?market=indices, holds no index, so no option "
                "underlying in this session gets the I: prefix of an index.",
                _TICKERS_PATH,
            )
        return frozenset(names)

    def _ask(
        self,
        root: str,
        *,
        contract: str,
        day: str,
        reader: TickerListReader,
        patience: Patience,
        cancel: Cancel | None,
    ) -> RootKind:
        """Ask Massive about one root, single-flight, and keep the answer.

        Args:
            root: The root.
            contract: The contract whose reference is read.
            day: The `as_of` day.
            reader: The read the lookup serves.
            patience: The read's patience.
            cancel: The caller's stop token, or None.

        Returns:
            The root's kind.

        A failed request raises its public `MassiveError`, and every caller that waited on it a
        copy; an unexpected error of the reader, a defect, is logged at ERROR with how to report
        it and propagates.
        """
        claim = self._lookup.claim_or_wait(
            root,
            bucket=OTHER,
            patience=patience,
            what=f"the contracts reference of option root {root}",
            public=reader.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        page = self._read(
            self._lookup,
            claim,
            reader=reader,
            cancel=cancel,
            failed=f"Reading Massive's Contract Overview of {contract} failed",
            about=f"option root {root}",
            not_found=RootKind.UNKNOWN,
            request=lambda: self._client.get(
                reader.rest_path(_CONTRACT_PATH),
                {"options_ticker": contract},
                {"as_of": date.fromisoformat(day)},
                bucket=OTHER,
                cancel=cancel,
                patience=patience,
            ),
        )
        if isinstance(page, RootKind):
            logger.debug(
                "%s: Massive's Contract Overview has no %s as of %s (HTTP 404)",
                self._label,
                contract,
                day,
            )
            return page
        cfi = next((row.get("cfi") for row in page.rows), None)
        kind = _kind_of(cfi)
        logger.debug(
            "%s: option root %s is %s, by the CFI code %r of %s as of %s",
            self._label,
            root,
            kind.value,
            cfi,
            contract,
            day,
        )
        self._lookup.complete(claim, kind)
        return kind

    def _ask_underlying(
        self,
        underlying: str,
        *,
        day: str,
        reader: TickerListReader,
        patience: Patience,
        cancel: Cancel | None,
    ) -> RootKind:
        """Ask Massive's contracts list about one bare underlying, single-flight, and keep it.

        One page of one contract, `underlying_ticker=<bare>&limit=1&as_of=<day>`, classifies the
        underlying by that contract's CFI code; a page with no contract, or a contract with no
        code, is `UNKNOWN`.

        Args:
            underlying: The bare underlying, as Massive's row names it.
            day: The `as_of` day.
            reader: The read the lookup serves.
            patience: The read's patience.
            cancel: The caller's stop token, or None.

        Returns:
            The underlying's kind.

        A failed request raises its public `MassiveError`, and every caller that waited on it a
        copy; an unexpected error of the reader, a defect, is logged at ERROR with how to report
        it and propagates.
        """
        claim = self._underlyings.claim_or_wait(
            underlying,
            bucket=OTHER,
            patience=patience,
            what=f"the contracts list of the underlying {underlying}",
            public=reader.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        # The list answers an unknown underlying with no rows, so an HTTP 404 is a failure.
        page = self._read(
            self._underlyings,
            claim,
            reader=reader,
            cancel=cancel,
            failed=f"Reading Massive's contracts list for {underlying} failed",
            about=f"the underlying {underlying}",
            not_found=None,
            request=lambda: self._client.get(
                reader.rest_path(_CONTRACTS_PATH),
                {},
                {"underlying_ticker": underlying, "limit": 1, "as_of": date.fromisoformat(day)},
                bucket=OTHER,
                cancel=cancel,
                patience=patience,
            ),
        )
        first = next((row for row in page.rows), None)
        cfi = None if first is None else first.get("cfi")
        kind = _kind_of(cfi)
        logger.debug(
            "%s: the underlying %s is %s, by the CFI code %r of its first listed contract %s "
            "as of %s",
            self._label,
            underlying,
            kind.value,
            cfi,
            None if first is None else first.get("ticker"),
            day,
        )
        if kind is RootKind.OTHER:
            # Only a ticker on Massive's index list is asked about, so a stock's answer means
            # the ticker names both, and Massive's row for it cannot be told apart.
            logger.warning(
                "%s is on Massive's index list, and the first contract Massive lists for it "
                "is on a stock, so its rows keep the bare ticker %s. Massive's row may also "
                "count the options on %s%s, and the library can't separate them. Nothing to "
                "do: the row is Massive's total for the ticker.",
                underlying,
                underlying,
                _INDEX_PREFIX,
                underlying,
            )
        self._underlyings.complete(claim, kind)
        return kind

    @overload
    def _read(
        self,
        lookup: SharedLookup[str, RootKind],
        claim: Claim[str, RootKind],
        *,
        reader: TickerListReader,
        cancel: Cancel | None,
        failed: str,
        about: str,
        not_found: None,
        request: Callable[[], Page],
    ) -> Page: ...

    @overload
    def _read(
        self,
        lookup: SharedLookup[str, RootKind],
        claim: Claim[str, RootKind],
        *,
        reader: TickerListReader,
        cancel: Cancel | None,
        failed: str,
        about: str,
        not_found: RootKind,
        request: Callable[[], Page],
    ) -> Page | RootKind: ...

    def _read(
        self,
        lookup: SharedLookup[str, RootKind],
        claim: Claim[str, RootKind],
        *,
        reader: TickerListReader,
        cancel: Cancel | None,
        failed: str,
        about: str,
        not_found: RootKind | None,
        request: Callable[[], Page],
    ) -> Page | RootKind:
        """Make one lookup's request under its claim, which ends on any outcome but a page.

        Args:
            lookup: The lookup the claim is on.
            claim: The caller's claim, which the caller completes when this returns a page.
            reader: The read the lookup serves, whose error conversion it uses.
            cancel: The caller's stop token, or None.
            failed: What failed, for the waiters' messages and the defect log.
            about: What was asked about, for the log.
            not_found: What an HTTP 404 means, kept as the claim's answer: `UNKNOWN` for a
                reference that answers a missing item that way. None where the path never
                answers 404 for a missing item, so a 404 is a failure like any other.
            request: Sends the request.

        Returns:
            The page, or `not_found` for HTTP 404 where it is a kind.

        A failed request raises its public `MassiveError` after failing the claim, and an
        unexpected error of the reader, a defect, is logged at ERROR and propagates.
        """
        try:
            return request()
        except WireError as error:
            if error.status == HTTPStatus.NOT_FOUND and not_found is not None:
                lookup.complete(claim, not_found)
                return not_found
            public = reader.public_error(error)
            stopped = cancel is not None and cancel.is_set() and error.status is None
            logger.debug(
                "%s: asking Massive's contracts reference about %s failed (status %s, stop set "
                "%s); its waiters %s: %s",
                self._label,
                about,
                error.status,
                cancel is not None and cancel.is_set(),
                "try again" if stopped else "get a copy of the error",
                public,
            )
            lookup.fail(claim, public, failed, stopped=stopped)
            raise public from error
        except BaseException as error:
            lookup.fail(claim, error, failed)
            if source_of(error) is ErrorSource.LIBRARY:
                logger.exception("%s. %s", failed, LIBRARY_BUG)
            raise


_UNDERLYING: Final = "Underlying"
_ROOT: Final = "OptionRoot"
_SYMBOL: Final = "Symbol"
_DATE: Final = "Date"


def canonical_underlyings(
    batches: Sequence[pa.RecordBatch],
    kind: KindOf,
    *,
    dataset: str,
    symbol: str | None,
) -> Withheld:
    """Give the `Underlying` of each row on an index its `I:`, and withhold the rows it can't.

    For each root among a page's rows whose `Underlying` is bare, `kind` is asked once, with
    the first such row's contract and trading day; rows of an `INDEX` root get `I:`. Rows of
    a root Massive's reference could not classify (`UNKNOWN`) are withheld as
    `unreadable_symbols`, since their `Underlying` may be an index's without its `I:`, and the
    library publishes only canonical tickers. A row without Massive's `root` takes the root of
    its `Symbol`. A null `Underlying` stays null.

    Args:
        batches: A page's rows in the dataset's schema, with their `Date`; at least one.
        kind: Returns a root's kind (`RestReader.root_kind`).
        dataset: The dataset's name, for messages.
        symbol: The column the withheld rows are grouped by, for a read by symbol to tell
            whose they are (`withhold_rows`), or None.

    Returns:
        The rows to publish, each index underlying with its `I:`, and the withheld rows
        grouped by their underlying as Massive sent it and by `symbol`, for the reader to
        report.

    A failed lookup raises the `MassiveError` of `kind`.
    """
    marked = [_canonical(batch, kind, dataset=dataset) for batch in batches]
    return withhold_rows(pa.Table.from_batches(marked), symbol=symbol)


def _canonical(batch: pa.RecordBatch, kind: KindOf, *, dataset: str) -> pa.RecordBatch:
    """Give one batch's index underlyings their `I:`, and mark the rows of unclassified roots.

    Args:
        batch: The rows.
        kind: Returns a root's kind.
        dataset: The dataset's name.

    Returns:
        The rows with the mark columns (`withheld.with_marks`), each index underlying with
        its `I:` and each row of an unclassified root marked `UNREADABLE_SYMBOLS`.
    """
    underlyings = batch.column(_UNDERLYING)
    bare = pc.fill_null(pc.invert(pc.starts_with(underlyings, _INDEX_PREFIX)), fill_value=False)
    if not pc.any(bare).as_py():
        return with_marks(batch, {}, sent=underlyings)
    symbols = batch.column(_SYMBOL)
    roots = pc.coalesce(batch.column(_ROOT), option_roots(symbols))
    # Each bare root's first row that carries a `Date`, which dates its lookup. `Symbol` is a
    # required key of every dataset that asks, so it is text, and a bare `Underlying` is never
    # null; `Date` is not required, so a row Massive sends without one can't date a lookup.
    firsts: dict[str, tuple[str, str, str] | None] = {}
    for root, underlying, symbol, day, wanted in zip(
        roots.to_pylist(),
        underlyings.to_pylist(),
        symbols.to_pylist(),
        batch.column(_DATE).to_pylist(),
        bare.to_pylist(),
        strict=True,
    ):
        if wanted and isinstance(root, str) and firsts.get(root) is None:
            firsts[root] = None if day is None else (str(underlying), str(symbol), str(day))
    index_roots: list[str] = []
    unknown: dict[str, str] = {}
    for root, first in firsts.items():
        if first is None:
            unknown[root] = (
                f"no row of option root {root} carries a Date, so the library can't ask "
                f"Massive's Contract Overview as of a day whether its underlying is an index"
            )
            continue
        underlying, contract, day = first
        found = kind(root, underlying=underlying, contract=contract, day=day)
        match found:
            case RootKind.INDEX:
                index_roots.append(root)
            case RootKind.OTHER:
                pass
            case RootKind.UNKNOWN:
                unknown[root] = (
                    f"Massive's Contract Overview gives no CFI code for option root {root} "
                    f"(asked about {contract} as of {day}), so the library can't tell whether "
                    f"the underlying {underlying} is an index ({_INDEX_PREFIX}{underlying}) or "
                    "not"
                )
            case _:
                assert_never(found)
    return _prefix_and_mark(
        batch,
        column=_UNDERLYING,
        keys=roots,
        bare=bare,
        index_keys=index_roots,
        unknown=unknown,
        dataset=dataset,
        what="option roots",
    )


def _prefix_and_mark(
    batch: pa.RecordBatch,
    *,
    column: str,
    keys: pa.Array,
    bare: pa.Array,
    index_keys: list[str],
    unknown: dict[str, str],
    dataset: str,
    what: str,
) -> pa.RecordBatch:
    """Prefix a column's index values and mark the rows whose key could not be classified.

    Args:
        batch: The rows.
        column: The column to prefix: `Underlying`, or `Symbol` where it is the underlying.
        keys: Each row's lookup key, an option root or the bare underlying itself.
        bare: Which rows' `column` lacks the `I:` prefix.
        index_keys: The keys whose rows are an index's.
        unknown: The keys the reference could not classify, each with the reason.
        dataset: The dataset's name, for the log.
        what: What the keys are, for the log, such as `"option roots"`.

    Returns:
        The rows with the mark columns, each index value prefixed and each row of an
        unclassified key marked `UNREADABLE_SYMBOLS`.
    """
    values = batch.column(column)
    if index_keys:
        on_index = pc.and_(bare, pc.is_in(keys, value_set=pa.array(index_keys, pa.string())))
        prefixed = pc.binary_join_element_wise(_INDEX_PREFIX, values, "")
        logger.debug(
            "%s: %d rows of the index %s %s get the I: prefix",
            dataset,
            pc.sum(on_index).as_py() or 0,
            what,
            index_keys,
        )
        position = batch.schema.get_field_index(column)
        batch = batch.set_column(
            position, batch.schema.field(position), pc.if_else(on_index, prefixed, values)
        )
    marked = with_marks(batch, {}, sent=values)
    if not unknown:
        return marked
    of_unknown = pc.and_(bare, pc.is_in(keys, value_set=pa.array(list(unknown), pa.string())))
    logger.debug(
        "%s: %d rows of the %s %s, which Massive's contracts reference could not classify, "
        "are withheld",
        dataset,
        pc.sum(of_unknown).as_py() or 0,
        what,
        list(unknown),
    )
    names = keys.to_pylist()
    return mark(
        marked,
        RowFault.UNREADABLE_SYMBOLS,
        of_unknown,
        field=column,
        reason=lambda row: unknown[names[row]],
    )


def canonical_symbols(
    batches: Sequence[pa.RecordBatch],
    kind: UnderlyingKindOf,
    *,
    dataset: str,
    symbol: str | None,
) -> Withheld:
    """Give the `Symbol` of each row on an index its `I:`, and withhold the rows it can't.

    For a dataset whose `Symbol` is the bare underlying itself, with no contract beside it. For
    each bare `Symbol` among a page's rows, `kind` is asked once, with the latest trading day
    among its rows; the rows of an `INDEX` ticker get `I:`. The rows of a ticker Massive's
    contracts list could not classify (`UNKNOWN`) are withheld as `unreadable_symbols`, since
    the ticker may be an index's without its `I:`, and the library publishes only canonical
    tickers.

    Args:
        batches: A page's rows in the dataset's schema, with their `Date`; at least one.
        kind: Returns a bare underlying's kind (`RestReader.underlying_kind`).
        dataset: The dataset's name, for messages.
        symbol: The column the withheld rows are grouped by, for a read by symbol to tell
            whose they are (`withhold_rows`), or None.

    Returns:
        The rows to publish, each index with its `I:`, and the withheld rows grouped by the
        symbol as Massive sent it and by `symbol`, for the reader to report.

    A failed lookup raises the `MassiveError` of `kind`.
    """
    marked = [_canonical_symbol(batch, kind, dataset=dataset) for batch in batches]
    return withhold_rows(pa.Table.from_batches(marked), symbol=symbol)


def _canonical_symbol(
    batch: pa.RecordBatch, kind: UnderlyingKindOf, *, dataset: str
) -> pa.RecordBatch:
    """Give one batch's index symbols their `I:`, and mark the rows of unclassified tickers.

    Args:
        batch: The rows.
        kind: Returns a bare underlying's kind.
        dataset: The dataset's name.

    Returns:
        The rows with the mark columns (`withheld.with_marks`), each index symbol with its `I:`
        and each row of an unclassified ticker marked `UNREADABLE_SYMBOLS`.
    """
    symbols = batch.column(_SYMBOL)
    # `Symbol` is a required key of every dataset that asks, so no row's is null.
    bare = pc.invert(pc.starts_with(symbols, _INDEX_PREFIX))
    if not pc.any(bare).as_py():
        return with_marks(batch, {}, sent=symbols)
    # Each ticker's latest day on the page: the contracts list as of that day is the likeliest
    # to hold a contract, since a live index lists more as time passes and a retired one
    # listed its last on its last day. `Date` is not a required key, so a row Massive sends
    # without one dates nothing; a ticker with no dated row can't be asked about.
    latest: dict[str, str | None] = {}
    for symbol, day, wanted in zip(
        symbols.to_pylist(), batch.column(_DATE).to_pylist(), bare.to_pylist(), strict=True
    ):
        if not wanted:
            continue
        dated = None if day is None else str(day)
        known = latest.get(symbol)
        if known is None or (dated is not None and dated > known):
            latest[symbol] = dated
    indices: list[str] = []
    unknown: dict[str, str] = {}
    for symbol, day in latest.items():
        if day is None:
            unknown[symbol] = (
                f"no row of {symbol} carries a Date, so the library can't ask Massive's "
                f"contracts list as of a day whether {symbol} is an index "
                f"({_INDEX_PREFIX}{symbol}) or not"
            )
            continue
        found = kind(symbol, day=day)
        match found:
            case RootKind.INDEX:
                indices.append(symbol)
            case RootKind.OTHER:
                pass
            case RootKind.UNKNOWN:
                unknown[symbol] = (
                    f"Massive's contracts list has no contract with a CFI code for {symbol} as "
                    f"of {day}, so the library can't tell whether {symbol} is an index "
                    f"({_INDEX_PREFIX}{symbol}) or not"
                )
            case _:
                assert_never(found)
    return _prefix_and_mark(
        batch,
        column=_SYMBOL,
        keys=symbols,
        bare=bare,
        index_keys=indices,
        unknown=unknown,
        dataset=dataset,
        what="tickers",
    )
