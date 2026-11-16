"""The futures venues whose flat files S3 refused in one `history()` call, and what that does.

Massive publishes futures flat files per venue (exchange), and a plan may include only some
venues: S3 answers a file of a venue outside the plan with HTTP 403 (`WireEntitlementError`).
When REST cannot serve the call and its `Request.refused_venues` is `dm.OnError.LOG` or
`IGNORE` (always `LOG` when the call left `venues` out, so it reads every venue the plan
allows), the flat-file read skips such a venue instead of failing: `RefusedVenues` records each
venue's first refusal as the reader threads meet them, so the venue's other files are never
requested, and the read then drops every file of a refused venue, whatever its date, so the
table never holds a venue for some days of the range and not others. `report` logs the refused
venues in one record, each venue once per `history()` call (a WARNING under `LOG`, DEBUG under
`IGNORE`), with Massive's status and text, even when a table that follows a live symbol table
reads the files again: the call keeps a `LoggedKeys` of the venues it logged, held by its read or,
for a followed table, by its result, so the keys go when the call ends;
`every_venue_refused` is the `EntitlementError` raised when no requested venue is left, whatever
the setting. A static table's description names the skipped venues (`refused_note`); a
followed table's description is fixed before its reads, so each symbol's transport in
`m.symbol_reads()` names them instead (`refused_source`). A rejected credential
(`WireAuthError`) is no refusal of a venue: it fails the call under any setting.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from deephaven_massive._enums import FuturesVenue, OnError
from deephaven_massive._errors import EntitlementError
from deephaven_massive._on_error import effect_of

if TYPE_CHECKING:
    from deephaven_massive._log_limits import LoggedKeys
    from deephaven_massive._massive import WireEntitlementError

__all__ = ["RefusedVenues", "refused_note", "refused_source"]

logger = logging.getLogger(__name__)


class RefusedVenues:
    """The venues S3 refused in one flat-file read, each with its first refusal.

    Shared by the read's reader threads, so every access holds the instance's lock, which is a
    leaf: nothing else is called under it.
    """

    __slots__ = ("_lock", "_logged", "_refusals", "call", "policy")

    def __init__(self, policy: OnError, *, call: str, logged: LoggedKeys) -> None:
        """Start a read's record with no venue refused.

        Args:
            policy: The call's `Request.refused_venues`, `LOG` or `IGNORE`, which `report`
                follows; the read never makes a record under `RAISE`.
            call: The `history()` call the read serves, such as `"MassiveSession #1 call-9"`.
                A table that follows a live symbol table reads its files again as symbols
                join, and `report` logs each refused venue once per call, not per read.
            logged: The venues the call has logged (`LoggedKeys`), which the call keeps
                across its reads and drops when it ends.
        """
        self.policy = policy
        self.call = call
        self._logged = logged
        self._lock = threading.Lock()
        self._refusals: dict[FuturesVenue, WireEntitlementError] = {}

    def add(self, venue: FuturesVenue, error: WireEntitlementError) -> None:
        """Record a refusal of one of the venue's files; a venue keeps its first refusal.

        Args:
            venue: The file's venue.
            error: S3's refusal.
        """
        with self._lock:
            first = self._refusals.setdefault(venue, error)
        logger.debug(
            "S3 refused %s; skipping venue %s%s",
            error.message,
            venue.name,
            "" if first is error else " (already refused)",
        )

    def __contains__(self, venue: FuturesVenue | None) -> bool:
        """Tell whether S3 has refused one of the venue's files so far in the call.

        Args:
            venue: A file's venue; None for a file of a dataset without venues.

        Returns:
            True when the venue was refused.
        """
        with self._lock:
            return venue in self._refusals

    def venues(self) -> tuple[FuturesVenue, ...]:
        """Return the refused venues, in `FuturesVenue` order.

        Returns:
            The venues; empty when S3 refused none.
        """
        return tuple(venue for venue, _ in self._refused())

    def _refused(self) -> tuple[tuple[FuturesVenue, WireEntitlementError], ...]:
        """Return each refused venue with its first refusal, in `FuturesVenue` order.

        Returns:
            The pairs; empty when S3 refused no venue.
        """
        with self._lock:
            refusals = dict(self._refusals)
        return tuple((venue, refusals[venue]) for venue in FuturesVenue if venue in refusals)

    def report(self, dataset: str) -> None:
        """Log the venues the read skips in one record, each venue once per call.

        A WARNING under `LOG`, a DEBUG line under `IGNORE`. Called after the read; logs nothing
        when S3 refused no venue, or only venues an earlier read of the call logged.

        Args:
            dataset: The dataset's name, such as `"futures.trades"`.
        """
        refused = dict(self._refused())
        keys = {f"{dataset}\0refused venue {venue.name}": venue for venue in refused}
        new = tuple((keys[key], refused[keys[key]]) for key in self._logged.first_seen(keys))
        if not new:
            return
        logger.log(
            effect_of(self.policy).level,  # never RAISE: no RefusedVenues is made under it
            "%s history (%s): skipping the flat files of %s for the whole range, since Massive "
            "refused them; name only the exchanges the plan includes in venues= to request "
            "none of them. S3 refused: %s",
            dataset,
            self.call,
            _names(new),
            "; ".join(f"{venue.name}: {error.message}" for venue, error in new),
        )

    def every_venue_refused(self) -> EntitlementError:
        """Return the error of a call whose every requested venue S3 refused.

        Returns:
            An `EntitlementError` naming the venues and the first one's refusal, with its status
            and request id.
        """
        refused = self._refused()
        first = refused[0][1]
        return EntitlementError(
            f"the plan does not include the flat files of any venue requested ({_names(refused)}): "
            f"{first.message}; pass venues={{dm.FuturesVenue.<VENUE>, ...}} with only the venues "
            "the plan includes",
            status=first.status,
            request_id=first.request_id,
        )


def refused_note(venues: tuple[FuturesVenue, ...]) -> str | None:
    """Return the table description's sentence naming the venues a read skipped, or None.

    Args:
        venues: The venues S3 refused, in `FuturesVenue` order (`FlatFileRead.refused`); empty
            for none.

    Returns:
        The sentence, or None when no venue was refused.
    """
    if not venues:
        return None
    named = ", ".join(venue.name for venue in venues)
    return (
        f"Massive refused the flat files of {named}, so this table holds no rows of "
        f"{'that exchange' if len(venues) == 1 else 'those exchanges'}."
    )


def refused_source(source: str, venues: tuple[FuturesVenue, ...]) -> str:
    """Return a read by symbol's source, naming the venues it skipped.

    A table that follows a live symbol table fixes its description before any read, so
    `m.symbol_reads()` shows the skip in each symbol's `Transport`.

    Args:
        source: The source as users read it, such as `"flat files"`.
        venues: The venues S3 refused and the read skipped (`FlatFileRead.refused`); empty for
            none.

    Returns:
        `source` itself when no venue was skipped; otherwise, for example,
        `"flat files without NYMEX, COMEX (refused)"`.
    """
    if not venues:
        return source
    return f"{source} without {', '.join(venue.name for venue in venues)} (refused)"


def _names(refused: tuple[tuple[FuturesVenue, WireEntitlementError], ...]) -> str:
    """Name refused venues for a message, such as `"NYMEX, COMEX"`.

    Args:
        refused: The refused venues with their refusals.

    Returns:
        The venues' names, comma-separated.
    """
    return ", ".join(venue.name for venue, _ in refused)
