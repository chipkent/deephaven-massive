import deephaven_massive as dm

m = dm.MassiveSession()
streams = m.streams()
connections = m.connections()

try:
    m.stocks.last_trade(["AAPL"]).fetch(wait=True)
    print("the latest trade: no error, so your plan includes it")
except dm.MassiveError as error:
    print(f"the latest trade: {type(error).__name__}: {error}")

try:
    live_minute_bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    print("live minute bars: opened; Massive's answer shows in the streams panel")
except dm.MassiveError as error:
    print(f"live minute bars: {type(error).__name__}: {error}")

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history("2015-01-05", "2015-01-09", wait=True)
    print("bars from 2015: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"bars from 2015: {type(error).__name__}: {error}")
