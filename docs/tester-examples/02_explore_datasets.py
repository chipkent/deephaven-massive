# List every dataset, and describe the columns of daily stock bars without a request.
import deephaven_massive as dm

m = dm.MassiveSession()

catalog = dm.datasets()
bars = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY)
print(bars)
bar_columns = bars.columns()
