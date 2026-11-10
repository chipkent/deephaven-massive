"""Tests of the Black-Scholes gamma that tells the unit of Massive's (`tests/_live/greeks.py`)."""

from __future__ import annotations

import pytest

from tests._live import greeks


def test_the_gamma_of_an_at_the_money_option_matches_the_textbook_value() -> None:
    # d1 = (0 + (0.05 + 0.02) * 1) / 0.2 = 0.35, and N'(0.35) / (100 * 0.2) = 0.0187620.
    gamma = greeks.black_scholes_gamma(
        spot=100.0, strike=100.0, years=1.0, volatility=0.2, rate=0.05
    )
    assert gamma == pytest.approx(0.0187620, rel=1e-5)


def test_a_dividend_yield_enters_d1_and_the_discount() -> None:
    paying = greeks.black_scholes_gamma(
        spot=100.0, strike=100.0, years=1.0, volatility=0.2, rate=0.05, dividend_yield=0.03
    )
    # d1 = (0 + (0.05 - 0.03 + 0.02) * 1) / 0.2 = 0.2, and e^-0.03 * N'(0.2) / 20 = 0.0189743.
    assert paying == pytest.approx(0.0189743, rel=1e-5)


@pytest.mark.parametrize(
    ("reported", "unit"),
    [
        (0.0187, greeks.PER_DOLLAR),
        (0.0187 * 1.24, greeks.PER_DOLLAR),
        (0.000187, greeks.PER_CENT),
        (0.000187 * 0.76, greeks.PER_CENT),
        (0.0187 * 1.3, greeks.NEITHER),
        (0.00187, greeks.NEITHER),
    ],
)
def test_the_unit_is_the_scale_the_reported_gamma_lies_near(reported: float, unit: str) -> None:
    assert greeks.gamma_unit(reported, 0.0187) == unit
