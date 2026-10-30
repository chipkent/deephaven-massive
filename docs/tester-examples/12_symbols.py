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
