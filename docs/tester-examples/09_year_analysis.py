# Compare a year of two stocks: daily returns, growth, volatility, correlation, a chart, a file.
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven import agg, parquet
from deephaven.pandas import to_pandas
from deephaven.plot.figure import Figure
from deephaven.updateby import delta

import deephaven_massive as dm

m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))

year_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=365)
pair = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(year_ago)
returns = (
    pair.sort(["Symbol", "Date"])
    .update_by(ops=[delta(cols=["Change = Close"])], by=["Symbol"])
    .update(["Return = Change / (Close - Change)"])
)
first_close = pair.sort(["Symbol", "Date"]).first_by(["Symbol"]).view(
    ["Symbol", "FirstClose = Close"]
)
growth = returns.natural_join(first_close, on=["Symbol"], joins=["FirstClose"]).update(
    ["Growth = Close / FirstClose"]
)
summary_year = growth.agg_by(
    [
        agg.first("FirstClose = Close"),
        agg.last("LastClose = Close"),
        agg.std("DailyVolatility = Return"),
        agg.count_("TradingDays"),
    ],
    by=["Symbol"],
).update(
    [
        "TotalReturn = LastClose / FirstClose - 1",
        "AnnualVolatility = DailyVolatility * Math.sqrt(252)",
    ]
)

side_by_side = (
    returns.where("Symbol == `AAPL`")
    .view(["Date", "AAPL = Return"])
    .natural_join(
        returns.where("Symbol == `MSFT`").view(["Date", "MSFT = Return"]),
        on=["Date"],
        joins=["MSFT"],
    )
    .where(["!isNull(AAPL)", "!isNull(MSFT)"])
)
frame = to_pandas(side_by_side)
print(round(frame["AAPL"].corr(frame["MSFT"]), 2))

growth_chart = (
    Figure()
    .plot_xy(series_name="AAPL", t=growth.where("Symbol == `AAPL`"), x="TimestampStart", y="Growth")
    .plot_xy(series_name="MSFT", t=growth.where("Symbol == `MSFT`"), x="TimestampStart", y="Growth")
    .show()
)

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
parquet.write(growth, os.path.join(folder, "growth.parquet"))
