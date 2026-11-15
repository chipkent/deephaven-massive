"""Live checks of Massive fields that may be one field under two names.

Each test runs one check of `tests/_live/same_field.py`, which reads the two fields from the
wire REST client back to back, records what Massive sent (`evidence`), and ends with one line
that says whether the two carried the same quantity within the stated tolerance (`SAME`), did
not (`DIFFERENT`), or could not be compared (`INCONCLUSIVE`). The tests are marked `settles`:
they assert only that the requests worked and returned rows, and a person reads the verdict.
How to run these tests is in `docs/live-tests.md`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests._live import same_field

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from deephaven_massive._massive import RestClient

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"


@pytest.mark.needs("stocks:starter")
@pytest.mark.settles("84")
@pytest.mark.requests(1)
@pytest.mark.timeout(60)
def test_todays_change_is_the_change_from_the_previous_close(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether the v2 snapshot's `todaysChange` is a price minus the previous day's close.

    Runs `same_field.todays_change` on five stocks.
    """
    assert all(same_field.todays_change(live_rest, evidence).rows)


@pytest.mark.needs("options:starter")
@pytest.mark.settles("84")
@pytest.mark.requests(1)
@pytest.mark.timeout(60)
def test_chain_day_change_is_the_change_from_the_previous_close(
    live_rest: RestClient, *, evidence: Evidence
) -> None:
    """Whether the option chain's `day.change` is `day.close` minus `day.previous_close`.

    Runs `same_field.chain_day_change` on 50 contracts of SPY's chain.
    """
    assert all(same_field.chain_day_change(live_rest, evidence).rows)


@pytest.mark.settles("85")
@pytest.mark.requests(2)
@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    ("v2_path", "tickers", "bucket"),
    [
        pytest.param(
            same_field.V2_STOCKS,
            same_field.STOCKS,
            "stocks",
            marks=pytest.mark.needs("stocks:starter"),
            id="stocks",
        ),
        pytest.param(
            same_field.V2_FOREX,
            same_field.FOREX_PAIRS,
            same_field.FX,
            marks=pytest.mark.needs("currencies:starter"),
            id="forex",
        ),
        pytest.param(
            same_field.V2_CRYPTO,
            same_field.CRYPTO_PAIRS,
            same_field.FX,
            marks=pytest.mark.needs("currencies:starter"),
            id="crypto",
        ),
    ],
)
def test_updated_is_the_unified_snapshot_s_last_updated(
    live_rest: RestClient,
    *,
    v2_path: str,
    tickers: tuple[str, ...],
    bucket: str,
    evidence: Evidence,
) -> None:
    """Whether the v2 snapshot's `updated` is the unified snapshot's `last_updated`.

    Runs `same_field.updated_times` on stocks, forex pairs, and crypto pairs. The evidence
    gives each ticker's times, then one verdict for each unified update time: the top-level
    `last_updated` Massive documents, and the nested ones its samples send instead.
    """
    outcome = same_field.updated_times(
        live_rest, evidence, v2_path=v2_path, tickers=tickers, bucket=bucket
    )
    assert all(outcome.rows)


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("86")
@pytest.mark.requests(24)
@pytest.mark.timeout(120)
@pytest.mark.parametrize(("base", "quote"), [("AUD", "USD"), ("USD", "AUD")])
def test_the_conversion_s_last_is_the_pair_s_last_quote(
    live_rest: RestClient, base: str, quote: str, *, evidence: Evidence
) -> None:
    """Whether the conversion's `last` object is the last quote of its `from`/`to` pair.

    Runs `same_field.conversion_quote` in both directions of AUD/USD. Massive's own samples
    suggest the answer is no: the conversion's sample for AUD to USD shows a bid of 1.3672596
    (US dollars to Australian), the last quote's for AUD/USD 0.73122.
    """
    outcome = same_field.conversion_quote(live_rest, evidence, base=base, quote=quote)
    assert all(outcome.rows)
