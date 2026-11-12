"""The futures venues whose flat files S3 refused in one `history()` call, and what that does.

Massive publishes futures flat files per venue (exchange), and a plan may include only some
venues: S3 answers a file of a venue outside the plan with HTTP 403 (`WireEntitlementError`).
When REST cannot serve the call and its `Request.refused_venues` is `dm.OnError.LOG` or
`IGNORE`, the flat-file read skips such a venue instead of failing: `RefusedVenues` records each
venue's first refusal as the reader threads meet them, so the venue's other files are never
requested, and the read then drops every file of a refused venue, whatever its date, so the
table never holds a venue for some days of the range and not others. `report` logs the refused
venues once per call (a WARNING under `LOG`, DEBUG under `IGNORE`), with Massive's status and
text, and `every_venue_refused` is the `EntitlementError` raised when no requested venue is
left, whatever the setting. A rejected credential (`WireAuthError`) is no refusal of a venue:
it fails the call under any setting.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from deephaven_massive._enums import FuturesVenue, OnError
from deephaven_massive._errors import EntitlementError

if TYPE_CHECKING:
    from deephaven_massive._massive import WireEntitlementError

__all__ = ["RefusedVenues", "refused_note"]

logger = logging.getLogger(__name__)


class RefusedVenues:
    """The venues S3 refused in one `history()` call, each with its first refusal.

    Shared by the call's reader threads, so every access holds the instance's lock, which is a
    leaf: nothing else is called under it.
    """

    __slots__ = ("_lock", "_refusals", "policy")

    def __init__(self, policy: OnError) -> None:
        """Start a call's record with no venue refused.

        Args:
            policy: The call's `Request.refused_venues`, `LOG` or `IGNORE`, which `report`
                follows; the read never makes a record under `RAISE`.
        """
        self.policy = policy
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
        """Log the venues the call skips: one WARNING under `LOG`, one DEBUG line under `IGNORE`.

        Called once per call, after the read; logs nothing when S3 refused no venue.

        Args:
            dataset: The dataset's name, such as `"futures.trades"`.
        """
        refused = self._refused()
        if not refused:
            return
        level = logging.WARNING if self.policy is OnError.LOG else logging.DEBUG
        logger.log(
            level,
            "%s history: skipping the flat files of %s for the whole range, as "
            "refused_venues=%s says; pass venues= with only the venues the plan includes to "
            "request none of them. S3 refused them: %s",
            dataset,
            _names(refused),
            self.policy.name,
            "; ".join(f"{venue.name}: {error.message}" for venue, error in refused),
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


def _names(refused: tuple[tuple[FuturesVenue, WireEntitlementError], ...]) -> str:
    """Name refused venues for a message, such as `"NYMEX, COMEX"`.

    Args:
        refused: The refused venues with their refusals.

    Returns:
        The venues' names, comma-separated.
    """
    return ", ".join(venue.name for venue, _ in refused)
