# Adds a column, keeps some rows, and summarizes by stock, with Deephaven's table operations.
# You should see three new tables: ranges, nvda (NVDA's rows only), and per_symbol (one row
# per stock).
from deephaven import agg

ranges = daily.update("Range = High - Low").view(["Symbol", "Date", "Close", "Range"])
nvda = ranges.where("Symbol == `NVDA`")
per_symbol = daily.agg_by(
    [agg.avg("AverageClose = Close"), agg.max_("HighestHigh = High"), agg.count_("Days")],
    by=["Symbol"],
)
