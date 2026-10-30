# Lists every dataset, then describes the columns of one query without sending a request.
# You should see one line describing the query, and two new tables: catalog, one row per
# dataset, and bar_columns, one row per column of daily stock bars.
catalog = dm.datasets()

bars = m.stocks.bars(["AAPL", "MSFT", "NVDA"], dm.Timespan.DAY)
print(bars)
bar_columns = bars.columns()
