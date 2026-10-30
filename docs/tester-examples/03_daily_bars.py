# Downloads two weeks of daily price bars for three stocks: three requests to Massive.
# You should see a number between 24 and 30, and a new table named daily.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

today = datetime.now(ZoneInfo("America/New_York")).date()
last_day = today - timedelta(days=1)
first_day = last_day - timedelta(days=13)

daily = bars.history(first_day, last_day)
print(daily.size)
