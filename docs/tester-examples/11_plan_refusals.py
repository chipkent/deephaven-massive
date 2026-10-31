# Asks for three things the free Basic plan does not include, and prints how each was
# refused. You should see three lines, each starting with what was asked. The first and the
# third show EntitlementError and the reason; the live minute bars open, and Massive's live
# connection refuses them a few seconds later, in the streams panel.
try:
    m.stocks.last_trade(["AAPL"]).fetch()
    print("the latest trade: no error, so your plan includes it")
except dm.MassiveError as error:
    print(f"the latest trade: {type(error).__name__}: {error}")

try:
    live_minute_bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    print("live minute bars: opened; Massive's answer shows in the streams panel")
except dm.MassiveError as error:
    print(f"live minute bars: {type(error).__name__}: {error}")

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history("2015-01-05", "2015-01-09")
    print("bars from 2015: no error, so your plan includes them")
except dm.MassiveError as error:
    print(f"bars from 2015: {type(error).__name__}: {error}")
