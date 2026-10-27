import pytest

from deephaven_massive._massive.ws.backoff import JITTER, STABLE_UPTIME, Backoff


def _waits(backoff: Backoff, count: int) -> list[float]:
    return [backoff.next_wait(None) for _ in range(count)]


def test_without_jitter_the_wait_doubles_up_to_the_cap() -> None:
    assert _waits(Backoff(15.0, 120.0, uniform=lambda: 0.0), 6) == [15, 30, 60, 100, 100, 100]


def test_full_jitter_adds_20_percent_and_is_clamped_at_the_cap() -> None:
    assert _waits(Backoff(15.0, 120.0, uniform=lambda: 1.0), 5) == [18, 36, 72, 120, 120]


def test_at_the_cap_the_wait_is_drawn_from_the_top_sixth() -> None:
    low = Backoff(30.0, 120.0, uniform=lambda: 0.0)
    high = Backoff(30.0, 120.0, uniform=lambda: 0.999999)
    assert _waits(low, 3)[-1] == pytest.approx(120.0 / (1 + JITTER))
    assert _waits(high, 3)[-1] == pytest.approx(120.0, rel=1e-5)


def test_equal_bounds_always_wait_exactly_that_long() -> None:
    assert _waits(Backoff(30.0, 30.0, uniform=lambda: 0.7), 3) == [30.0, 30.0, 30.0]


def test_a_cap_close_to_the_initial_never_waits_less_than_the_initial() -> None:
    # 100 / 1.2 is below the initial 90, so the capped base stays at 90.
    waits = _waits(Backoff(90.0, 100.0, uniform=lambda: 0.0), 3)
    assert waits == [90.0, 90.0, 90.0]


def test_every_wait_lies_within_the_bounds() -> None:
    backoff = Backoff(15.0, 120.0)
    assert all(15.0 <= w <= 120.0 for w in _waits(backoff, 200))


def test_doubling_never_overflows() -> None:
    assert _waits(Backoff(15.0, 1e308, uniform=lambda: 0.0), 2000)[-1] < float("inf")


def test_only_a_stable_connection_resets_the_count() -> None:
    backoff = Backoff(15.0, 120.0, uniform=lambda: 0.0)
    _waits(backoff, 2)
    assert backoff.next_wait(uptime=STABLE_UPTIME - 0.001) == 60.0
    assert backoff.next_wait(uptime=STABLE_UPTIME) == 15.0
    assert backoff.next_wait(uptime=0.0) == 30.0


@pytest.mark.parametrize(
    ("initial", "maximum", "message"),
    [
        (0.0, 10.0, "initial backoff must be positive, not 0.0"),
        (-1.0, 10.0, "must be positive"),
        (float("nan"), 10.0, "must be positive"),
        (20.0, 10.0, "maximum backoff 10.0 is below the initial 20.0"),
    ],
)
def test_bounds_are_checked(initial: float, maximum: float, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Backoff(initial, maximum)
