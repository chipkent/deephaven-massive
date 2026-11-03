# Closes the session, shows that a closed session refuses new work, and downloads once more
# in a with block, which closes its session by itself. You should see
# <MassiveSession #1 closed>, a MassiveError that says the session is closed, then
# <MassiveSession #2 closed> and a number between 3 and 5.
m.close()
print(m)

try:
    m.stocks.bars("AAPL", dm.Timespan.DAY).history(first_day, last_day)
except dm.MassiveError as error:
    print(f"MassiveError: {error}")

config = dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription))
with dm.MassiveSession(config) as m2:
    last_week = m2.stocks.bars("NVDA", dm.Timespan.DAY).history(
        last_day - timedelta(days=6), last_day
    )
print(m2, last_week.size)
