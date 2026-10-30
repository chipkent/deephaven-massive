# Starts a one-year download of two stocks in the background, so the console stays free.
# You should see a new table, year, with no rows at first; its row in streams reads
# LOADING, then DONE, and the rows appear all at once.
year = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.DAY).history(
    today - timedelta(days=365), wait=False
)
