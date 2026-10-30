from __future__ import annotations

import math
import threading
import time

import pytest

from deephaven_massive._massive.net import retry


def test_the_schedule_is_three_retries_of_half_one_and_two_seconds() -> None:
    assert retry.MAX_RETRIES == 3
    assert retry.TRANSIENT_WAITS == (0.5, 1.0, 2.0)
    assert frozenset({500, 502, 503, 504}) == retry.TRANSIENT_STATUSES


@pytest.mark.parametrize(("attempt", "base"), [(0, 0.5), (1, 1.0), (2, 2.0)])
def test_a_transient_wait_adds_up_to_twenty_percent_jitter(attempt: int, base: float) -> None:
    assert retry.transient_wait(attempt, 0.0) == base
    assert retry.transient_wait(attempt, 0.5) == pytest.approx(base * 1.1)
    assert retry.transient_wait(attempt, 0.999) == pytest.approx(base * 1.1998)


def test_there_is_no_fourth_transient_wait() -> None:
    with pytest.raises(IndexError):
        retry.transient_wait(3, 0.0)


@pytest.mark.parametrize(
    ("refusal", "wait"), [(1, 15.0), (2, 30.0), (3, 60.0), (4, 60.0), (9, 60.0)]
)
def test_a_rate_limit_without_retry_after_pauses_15_30_then_60(refusal: int, wait: float) -> None:
    assert retry.rate_limit_wait(refusal, None) == wait


def test_a_rate_limit_pauses_what_retry_after_asks() -> None:
    assert retry.rate_limit_wait(2, 7.0) == 7.0
    assert retry.rate_limit_wait(1, 0.0) == 0.0
    assert retry.rate_limit_wait(1, 3600.0) == 3600.0


@pytest.mark.parametrize(
    ("header", "seconds"),
    [("30", 30.0), (" 5 ", 5.0), ("0", 0.0), ("120", 120.0)],
)
def test_retry_after_in_seconds(header: str, seconds: float) -> None:
    assert retry.retry_after_seconds(header, now=0.0) == seconds


def test_retry_after_as_an_http_date_counts_from_now() -> None:
    # Wed, 21 Oct 2026 07:28:00 GMT is 1792567680 seconds after the epoch.
    header = "Wed, 21 Oct 2026 07:28:00 GMT"
    assert retry.retry_after_seconds(header, now=1_792_567_660.0) == 20.0


def test_retry_after_as_a_past_date_asks_for_no_wait() -> None:
    assert retry.retry_after_seconds("Wed, 21 Oct 2026 07:28:00 GMT", now=1_900_000_000.0) == 0.0


def test_an_absent_retry_after_is_none_and_silent(caplog: pytest.LogCaptureFixture) -> None:
    assert retry.retry_after_seconds(None, now=0.0) is None
    assert caplog.records == []


@pytest.mark.parametrize("header", ["", "soon", "-5", "1.5", "\u0663"])
def test_an_unusable_retry_after_is_none_and_logged(
    header: str, caplog: pytest.LogCaptureFixture
) -> None:
    assert retry.retry_after_seconds(header, now=0.0) is None
    [record] = caplog.records
    assert (record.name, record.levelname) == (retry.__name__, "WARNING")
    assert record.getMessage() == (
        f"ignoring a Retry-After that is neither seconds nor an HTTP date: {header!r}"
    )


def test_a_retry_after_of_more_seconds_than_a_float_holds_is_none_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Four hundred digits read as an infinite float: no pause could ever end.
    header = "9" * 400
    assert retry.retry_after_seconds(header, now=0.0) is None
    [record] = caplog.records
    assert (record.name, record.levelname) == (retry.__name__, "WARNING")
    assert record.getMessage() == (
        f"ignoring a Retry-After too large to be a number of seconds: {header!r}"
    )


def test_the_longest_retry_after_a_float_holds_is_honoured() -> None:
    header = "9" * 308
    assert retry.retry_after_seconds(header, now=0.0) == float(header)


@pytest.mark.parametrize(
    "header",
    [
        "Fri, 15 Jan 2027 08:00:00 GMT",
        "Fri, 15 Jan 2027 08:00:00 -0000",
    ],
)
def test_an_http_date_is_seconds_since_the_epoch_in_gmt(header: str) -> None:
    assert retry.http_date_seconds(header) == 1_800_000_000.0


@pytest.mark.parametrize("header", [None, "", "soon", "30"])
def test_what_is_not_an_http_date_is_none_and_silent(
    header: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    assert retry.http_date_seconds(header) is None
    assert caplog.records == []


def test_a_retry_after_date_without_a_zone_is_gmt() -> None:
    header = "Wed, 21 Oct 2026 07:28:00 -0000"
    assert retry.retry_after_seconds(header, now=1_792_567_660.0) == 20.0


def test_the_system_clock_reads_and_waits_on_real_time() -> None:
    clock = retry.SYSTEM_CLOCK
    assert isinstance(clock, retry.SystemClock)
    before = clock.monotonic()
    assert clock.wait(threading.Event(), 0.001) is False
    assert clock.monotonic() > before
    assert abs(clock.time() - time.time()) < 5


def test_the_system_clock_wait_ends_when_its_event_is_set() -> None:
    event = threading.Event()
    event.set()
    assert retry.SYSTEM_CLOCK.wait(event, 60.0) is True


def test_the_system_clock_waits_without_a_deadline_until_its_event_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `threading.Event.wait` refuses an infinite timeout; the clock waits for the event alone,
    # in short chunks. The event is set only once the waiter has been blocked for a while.
    monkeypatch.setattr(retry, "UNTIMED_CHUNK", 0.01)
    event = threading.Event()
    results: list[bool] = []
    waiter = threading.Thread(
        target=lambda: results.append(retry.SYSTEM_CLOCK.wait(event, math.inf)), daemon=True
    )
    waiter.start()
    waiter.join(0.2)
    assert waiter.is_alive()  # still waiting, across several chunks
    event.set()
    waiter.join(10)
    assert results == [True]
