"""Black-Scholes gamma, to tell the unit of the gamma Massive's options snapshots carry.

Massive's description of `greeks.gamma` says it is the change in delta per $0.01 change in the
underlying's price, while the textbook gamma, and most vendors', is per $1. The two differ by a
factor of 100, so a gamma computed from the contract's own inputs tells them apart even when
Massive's rate, dividend, or day count differ from the check's by a few percent:

- `black_scholes_gamma` computes the textbook gamma, per $1, of a European option;
- `gamma_unit` names the unit a reported gamma matches.

Every function is pure.
"""

from __future__ import annotations

import math
from typing import Final

PER_DOLLAR: Final = "per $1"
PER_CENT: Final = "per $0.01"
NEITHER: Final = "neither"
"""The units `gamma_unit` names: a gamma that matches neither scale is `NEITHER`."""

TOLERANCE: Final = 0.25
"""How far, as a fraction, a reported gamma may stray from a scale and still match it: far
wider than the gap a different rate or day count makes near the money, and far narrower than
the factor of 100 between the scales."""


def black_scholes_gamma(
    *,
    spot: float,
    strike: float,
    years: float,
    volatility: float,
    rate: float,
    dividend_yield: float = 0.0,
) -> float:
    """Return the Black-Scholes gamma of a European option, per $1 move of the underlying.

    A call and a put of one strike and expiry share it.

    Args:
        spot: The underlying's price.
        strike: The strike price.
        years: The time to expiry, in years.
        volatility: The implied volatility, as a fraction a year, such as 0.2.
        rate: The risk-free rate, continuously compounded, as a fraction a year.
        dividend_yield: The underlying's dividend yield, as a fraction a year.

    Returns:
        The gamma.
    """
    root = volatility * math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate - dividend_yield + volatility**2 / 2) * years) / root
    density = math.exp(-(d1**2) / 2) / math.sqrt(2 * math.pi)
    return math.exp(-dividend_yield * years) * density / (spot * root)


def gamma_unit(reported: float, per_dollar: float) -> str:
    """Name the unit a reported gamma matches, against the textbook gamma of the same contract.

    Args:
        reported: The gamma Massive reported.
        per_dollar: The textbook gamma, per $1 (`black_scholes_gamma`).

    Returns:
        `PER_DOLLAR` when `reported` is within `TOLERANCE` of `per_dollar`, `PER_CENT` when it
        is within `TOLERANCE` of a hundredth of it, else `NEITHER`.
    """
    ratio = reported / per_dollar
    if abs(ratio - 1) <= TOLERANCE:
        return PER_DOLLAR
    if abs(ratio * 100 - 1) <= TOLERANCE:
        return PER_CENT
    return NEITHER
