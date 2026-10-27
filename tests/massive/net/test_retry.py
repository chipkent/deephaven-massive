from __future__ import annotations

import ssl
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


@pytest.mark.parametrize(("attempt", "wait"), [(0, 15.0), (1, 30.0), (2, 60.0)])
def test_a_rate_limit_without_retry_after_waits_15_30_then_60(attempt: int, wait: float) -> None:
    assert retry.rate_limit_wait(attempt, None) == wait


def test_a_rate_limit_waits_what_retry_after_asks() -> None:
    assert retry.rate_limit_wait(2, 7.0) == 7.0
    assert retry.rate_limit_wait(0, 0.0) == 0.0


def test_the_longest_honoured_retry_after_is_60_seconds() -> None:
    assert retry.MAX_RETRY_AFTER == 60.0


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


def test_a_retry_after_date_without_a_zone_is_gmt() -> None:
    header = "Wed, 21 Oct 2026 07:28:00 -0000"
    assert retry.retry_after_seconds(header, now=1_792_567_660.0) == 20.0


def test_a_certificate_failure_is_recognised_however_it_is_wrapped() -> None:
    cert = ssl.SSLCertVerificationError(1, "certificate verify failed")
    assert retry.is_tls_failure(cert)
    as_argument = OSError(cert)
    assert retry.is_tls_failure(as_argument)
    as_cause = ConnectionError("wrapped")
    as_cause.__cause__ = cert
    assert retry.is_tls_failure(as_cause)
    as_context = RuntimeError("during handling")
    as_context.__context__ = as_argument
    assert retry.is_tls_failure(as_context)


def test_other_failures_are_not_tls_failures() -> None:
    assert not retry.is_tls_failure(ConnectionResetError("reset"))
    assert not retry.is_tls_failure(ssl.SSLError("handshake failure"))
    assert not retry.is_tls_failure(ValueError(ValueError("nested"), "text"))


def test_a_cycle_of_exceptions_ends_the_search() -> None:
    first = ValueError("first")
    second = ValueError(first)
    first.__context__ = second
    assert not retry.is_tls_failure(first)


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
