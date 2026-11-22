from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from deephaven_massive._enums import AssetClass
from deephaven_massive._ingest import ws_silence

if TYPE_CHECKING:
    import pytest

_URL = "wss://socket.test/stocks"
_LAST = 1_800_000_000 * 10**9
"""The last frame's arrival, in nanoseconds since the epoch: 2027-01-15T08:00:00Z."""
_QUIET = _LAST + int(ws_silence.NO_FRAMES_WARNING * 1e9)
"""The first moment the connection has been silent long enough to warn."""


def _check(notes: list[str | None]) -> ws_silence.SilenceCheck:
    # A stocks connection's check; each note it shows on the streams goes to `notes`.
    def show() -> None:
        notes.append(check.note)

    check = ws_silence.SilenceCheck(_URL, AssetClass.STOCKS, on_note=show)
    return check


def _messages(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [(r.levelno, r.getMessage()) for r in caplog.records if r.name == ws_silence.__name__]


def test_a_silent_connection_warns_once_while_its_market_trades(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    trading = [False]
    monkeypatch.setattr(ws_silence, "in_trading_hours", lambda _asset_class, _moment: trading[0])
    notes: list[str | None] = []
    check = _check(notes)

    def at(now: int) -> None:
        check.check(now, connected=True, acknowledged=1, last_frame=_LAST)

    with caplog.at_level(logging.INFO, logger=ws_silence.__name__):
        at(_QUIET)  # silent long enough, but the market is closed
        trading[0] = True
        at(_QUIET - 10**9)  # not silent long enough yet
        at(_QUIET)
        at(_QUIET + 60 * 10**9)  # the same episode: logged once
        check.heard()  # a frame arrives: the recovery is logged at once
        check.heard()  # and only once
    assert _messages(caplog) == [
        (
            logging.WARNING,
            (
                f"{_URL}: Massive has sent nothing for 300 s during stocks trading hours, "
                "though it confirmed 1 subscription, so the live tables on this connection "
                "may be stale. The usual causes are a market holiday or symbols that are not "
                "trading. Check the connection's row in m.connections()."
            ),
        ),
        (logging.INFO, f"{_URL}: frames arrive again"),
    ]
    # The streams show the silence while it lasts, but keep running: nothing fails them.
    assert notes == [
        "Massive has sent nothing since 2027-01-15T08:00:00Z during stocks trading hours.",
        None,
    ]


def test_a_connection_with_nothing_acknowledged_is_never_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ws_silence, "in_trading_hours", lambda _asset_class, _moment: True)
    notes: list[str | None] = []
    with caplog.at_level(logging.INFO, logger=ws_silence.__name__):
        _check(notes).check(
            int(2 * ws_silence.NO_FRAMES_WARNING * 1e9),
            connected=True,
            acknowledged=0,
            last_frame=0,
        )
    assert not caplog.records
    assert notes == []


def test_a_connection_that_is_not_connected_is_the_outage_s(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ws_silence, "in_trading_hours", lambda _asset_class, _moment: True)
    notes: list[str | None] = []
    with caplog.at_level(logging.INFO, logger=ws_silence.__name__):
        _check(notes).check(_QUIET, connected=False, acknowledged=1, last_frame=_LAST)
    assert not caplog.records
    assert notes == []


def test_an_outage_ends_the_episode_silently_and_a_later_silence_warns_again(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The outage shows its own note, so ending the silence logs nothing and shows nothing;
    # the next silence is a new episode.
    monkeypatch.setattr(ws_silence, "in_trading_hours", lambda _asset_class, _moment: True)
    notes: list[str | None] = []
    check = _check(notes)
    with caplog.at_level(logging.INFO, logger=ws_silence.__name__):
        check.check(_QUIET, connected=True, acknowledged=1, last_frame=_LAST)
        check.end()
        assert check.note is None
        check.heard()  # no episode: no recovery line
        check.check(_QUIET, connected=True, acknowledged=1, last_frame=_LAST)
    assert [level for level, _ in _messages(caplog)] == [logging.WARNING, logging.WARNING]
    assert len(notes) == 2
    assert None not in notes
