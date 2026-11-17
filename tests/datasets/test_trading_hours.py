from __future__ import annotations

from datetime import UTC, datetime

import pytest

from deephaven_massive._datasets.dates import NEW_YORK
from deephaven_massive._datasets.futures_sessions import CHICAGO
from deephaven_massive._datasets.trading_hours import in_trading_hours
from deephaven_massive._enums import AssetClass

# 2026-10-26 is a Monday; 2026-10-30 a Friday, 2026-10-31 a Saturday, 2026-11-01 a Sunday.


@pytest.mark.parametrize("asset_class", [AssetClass.STOCKS, AssetClass.OPTIONS, AssetClass.INDICES])
@pytest.mark.parametrize(
    ("moment", "trading"),
    [
        (datetime(2026, 10, 26, 9, 29, tzinfo=NEW_YORK), False),
        (datetime(2026, 10, 26, 9, 30, tzinfo=NEW_YORK), True),
        (datetime(2026, 10, 30, 15, 59, tzinfo=NEW_YORK), True),
        (datetime(2026, 10, 30, 16, 0, tzinfo=NEW_YORK), False),
        (datetime(2026, 10, 31, 12, 0, tzinfo=NEW_YORK), False),
        (datetime(2026, 11, 1, 12, 0, tzinfo=NEW_YORK), False),
        # The extended hours and the overnight session, which starts on Sunday 2026-12-06.
        (datetime(2026, 12, 7, 4, 0, tzinfo=NEW_YORK), False),  # pre-market
        (datetime(2026, 12, 7, 19, 0, tzinfo=NEW_YORK), False),  # after hours
        (datetime(2026, 12, 7, 20, 30, tzinfo=NEW_YORK), False),  # the nightly pause
        (datetime(2026, 12, 6, 21, 30, tzinfo=NEW_YORK), False),  # Sunday overnight
        (datetime(2026, 12, 7, 22, 0, tzinfo=NEW_YORK), False),  # Monday overnight
        (datetime(2026, 12, 8, 2, 0, tzinfo=NEW_YORK), False),  # the same night, after midnight
    ],
)
def test_the_regular_session_is_weekdays_from_09_30_to_16_00_new_york(
    asset_class: AssetClass, moment: datetime, *, trading: bool
) -> None:
    assert in_trading_hours(asset_class, moment) is trading


@pytest.mark.parametrize(
    ("moment", "trading"),
    [
        (datetime(2026, 11, 1, 16, 59, tzinfo=CHICAGO), False),  # Sunday, before the open
        (datetime(2026, 11, 1, 17, 0, tzinfo=CHICAGO), True),
        (datetime(2026, 10, 27, 15, 59, tzinfo=CHICAGO), True),
        (datetime(2026, 10, 27, 16, 0, tzinfo=CHICAGO), False),  # the daily pause
        (datetime(2026, 10, 27, 17, 0, tzinfo=CHICAGO), True),
        (datetime(2026, 10, 30, 15, 59, tzinfo=CHICAGO), True),
        (datetime(2026, 10, 30, 17, 30, tzinfo=CHICAGO), False),  # Friday after the close
        (datetime(2026, 10, 31, 12, 0, tzinfo=CHICAGO), False),
    ],
)
def test_futures_trade_from_sunday_evening_to_friday_less_the_daily_pause(
    moment: datetime, *, trading: bool
) -> None:
    assert in_trading_hours(AssetClass.FUTURES, moment) is trading


@pytest.mark.parametrize(
    ("moment", "trading"),
    [
        (datetime(2026, 11, 1, 16, 59, tzinfo=NEW_YORK), False),
        (datetime(2026, 11, 1, 17, 0, tzinfo=NEW_YORK), True),
        (datetime(2026, 10, 28, 3, 0, tzinfo=NEW_YORK), True),
        (datetime(2026, 10, 30, 16, 59, tzinfo=NEW_YORK), True),
        (datetime(2026, 10, 30, 17, 0, tzinfo=NEW_YORK), False),
        (datetime(2026, 10, 31, 12, 0, tzinfo=NEW_YORK), False),
    ],
)
def test_forex_trades_from_sunday_to_friday_17_00_new_york(
    moment: datetime, *, trading: bool
) -> None:
    assert in_trading_hours(AssetClass.FOREX, moment) is trading


def test_crypto_always_trades() -> None:
    assert in_trading_hours(AssetClass.CRYPTO, datetime(2026, 10, 31, 3, 0, tzinfo=UTC))
