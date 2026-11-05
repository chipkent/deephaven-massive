# Chart the gap between the 10-year and 2-year US Treasury yields over two years.
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from deephaven.plot.figure import Figure

import deephaven_massive as dm

m = dm.MassiveSession()

two_years_ago = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=730)
curve = (
    m.economy.treasury_yields(start=two_years_ago)
    .fetch()
    .update(["Spread = Yield10Year - Yield2Year", "Day = parseInstant(Date + `T00:00:00 ET`)"])
)
curve_chart = Figure().plot_xy(series_name="10y - 2y", t=curve, x="Day", y="Spread").show()
