import logging

import pytest

from deephaven_massive._enums import FuturesVenue, OnError
from deephaven_massive._ingest import refused_venues
from deephaven_massive._ingest.refused_venues import RefusedVenues, refused_note, refused_source
from deephaven_massive._log_limits import LoggedKeys
from deephaven_massive._massive import WireEntitlementError
from deephaven_massive._tables.problem_events import (
    Problem,
    ProblemAction,
    ProblemCause,
    ProblemEvent,
)


def _refusal(venue: str, request_id: str) -> WireEntitlementError:
    return WireEntitlementError(f"{venue} denied.", status=403, request_id=request_id)


def test_a_venue_keeps_its_first_refusal_and_venues_come_in_enum_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    refused = RefusedVenues(OnError.LOG, call="MassiveSession #1 call-1", logged=LoggedKeys())
    assert refused.venues() == ()
    assert None not in refused
    with caplog.at_level(logging.DEBUG, logger=refused_venues.__name__):
        refused.add(FuturesVenue.COMEX, _refusal("comex-1", "r1"))
        refused.add(FuturesVenue.CME, _refusal("cme", "r2"))
        refused.add(FuturesVenue.COMEX, _refusal("comex-2", "r3"))
    assert refused.venues() == (FuturesVenue.CME, FuturesVenue.COMEX)
    assert FuturesVenue.COMEX in refused
    assert FuturesVenue.CBOT not in refused
    assert caplog.messages == [
        "skipping venue COMEX: comex-1 denied.",
        "skipping venue CME: cme denied.",
        "skipping venue COMEX (already refused): comex-2 denied.",
    ]
    error = refused.every_venue_refused("futures.trades")
    assert str(error) == (
        "futures.trades: Massive refused the flat files of every venue the read asked for, CME "
        "and COMEX. cme denied. To read from REST instead, pass source=dm.HistorySource.REST and "
        "contract tickers such as 'ESH5' in place of dm.ALL."
    )
    assert (error.status, error.request_id) == (403, "r2")


def test_the_note_names_one_venue_or_several() -> None:
    assert refused_note(()) is None
    assert refused_note((FuturesVenue.CBOT,)) == (
        "Massive refused the flat files of CBOT, so this table holds no rows of that exchange."
    )
    assert refused_note((FuturesVenue.CBOT, FuturesVenue.NYMEX)) == (
        "Massive refused the flat files of CBOT, NYMEX, so this table holds no rows of those "
        "exchanges."
    )


def test_a_source_names_the_skipped_venues() -> None:
    assert refused_source("flat files", ()) == "flat files"
    assert refused_source("flat files", (FuturesVenue.CME, FuturesVenue.COMEX)) == (
        "flat files without CME, COMEX (refused)"
    )


def test_a_report_gives_each_venue_once_for_the_call_that_keeps_the_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The record of what a call reported is the call's own: a second read of the call reports
    # only a venue new to it, and another call, with its own record, reports every venue again.
    call = LoggedKeys()
    problems: list[Problem] = []

    def report(logged: LoggedKeys, *venues: FuturesVenue) -> None:
        refused = RefusedVenues(OnError.IGNORE, call="MassiveSession #1 call-2", logged=logged)
        for venue in venues:
            refused.add(venue, _refusal(venue.name, "r"))
        refused.report("futures.trades", problems=problems.extend, stream_id=2)

    with caplog.at_level(logging.DEBUG, logger=refused_venues.__name__):
        report(call, FuturesVenue.CME)
        report(call, FuturesVenue.CME)
        report(call, FuturesVenue.CME, FuturesVenue.NYMEX)
        report(LoggedKeys(), FuturesVenue.CME)
    skipped = [r for r in caplog.records if "for the whole range" in r.getMessage()]
    assert [r.levelno for r in skipped] == [logging.DEBUG] * 3
    assert [r.getMessage() for r in skipped] == [
        "futures.trades history (MassiveSession #1 call-2): Massive refused the flat files of "
        f"{venue} for the whole range: {venue} denied"
        for venue in ("CME", "NYMEX", "CME")
    ]
    events = [p for p in problems if isinstance(p, ProblemEvent)]
    assert [(e.cause, e.action, e.stream_id, e.policy) for e in events] == [
        (ProblemCause.REFUSED_VENUES, ProblemAction.SKIPPED, 2, OnError.IGNORE)
    ] * 3
    # Each exchange is its own row of m.problems(): a later refusal never merges into another.
    assert [e.field for e in events] == ["CME", "NYMEX", "CME"]
    assert [e.detail for e in events] == [
        f"Massive refused the flat files of {venue} for the whole range: {venue} denied"
        for venue in ("CME", "NYMEX", "CME")
    ]
