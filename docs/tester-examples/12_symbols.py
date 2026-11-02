# Converts symbols written in other conventions into Massive's, one at a time and as a
# table. You should see C:EURUSD, X:BTCUSD, I:SPX, and BRK.B, then a ValueError that names
# the conventions to choose from, and a new table, converted, whose bad row is empty.
from deephaven import new_table
from deephaven.column import string_col

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))

try:
    dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS)
except ValueError as error:
    print(f"ValueError: {error}")

watchlist = new_table([string_col("Symbol", ["BRK-B", "AAPL", "not a ticker"])])
converted = dm.canonicalize_symbols(
    watchlist,
    dm.AssetClass.STOCKS,
    convention=dm.SymbolConvention.YAHOO,
    on_error=dm.OnError.LOG,
)

# A watchlist you can edit: an input table. A fetch() of it follows it, reading each symbol
# you add. You should see a new table, dividends, with AAPL's dividends, then MSFT's join it
# a few seconds after the second add.
from deephaven import dtypes as dht
from deephaven import input_table

names = input_table({"Symbol": dht.string}, key_cols="Symbol")
names.add(new_table([string_col("Symbol", ["AAPL"])]))
dividends = m.stocks.dividends(names).fetch()
names.add(new_table([string_col("Symbol", ["MSFT"])]))
