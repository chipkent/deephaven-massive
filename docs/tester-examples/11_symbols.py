# Convert symbols written in other conventions into Massive's, one at a time and as a table.
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))

try:
    dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS)
except ValueError as error:
    print(f"ValueError: {error}")

yahoo_symbols = new_table([string_col("Symbol", ["BRK-B", "AAPL", "not a ticker"])])
converted = dm.canonicalize_symbols(
    yahoo_symbols, dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
)
