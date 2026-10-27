from datetime import date

import pytest

from deephaven_massive._massive.flatfiles.layout import object_key


@pytest.mark.parametrize(
    ("prefix", "day", "venue", "key"),
    [
        pytest.param(
            "us_stocks_sip/trades_v1",
            date(2025, 3, 3),
            None,
            "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz",
            id="plain-prefix",
        ),
        pytest.param(
            "global_crypto/trades_v1",
            date(2024, 12, 31),
            None,
            "global_crypto/trades_v1/2024/12/2024-12-31.csv.gz",
            id="two-digit-month",
        ),
        pytest.param(
            "us_futures_{venue}/trades_v1",
            date(2025, 3, 4),
            "nymex",
            "us_futures_nymex/trades_v1/2025/03/2025-03-04.csv.gz",
            id="venue-template",
        ),
    ],
)
def test_object_key(*, prefix: str, day: date, venue: str | None, key: str) -> None:
    assert object_key(prefix, day, venue) == key


def test_the_venue_defaults_to_none() -> None:
    assert object_key("us_indices/values_v1", date(2025, 3, 3)) == (
        "us_indices/values_v1/2025/03/2025-03-03.csv.gz"
    )
