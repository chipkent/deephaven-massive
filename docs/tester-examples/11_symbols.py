from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm

print(dm.canonical_symbol("EUR/USD", dm.AssetClass.FOREX))
print(dm.canonical_symbol("btc-usd", dm.AssetClass.CRYPTO))
print(dm.canonical_symbol("SPX", dm.AssetClass.INDICES))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO))
print(dm.canonical_symbol("BRK-B", dm.AssetClass.STOCKS))

yahoo_symbols = new_table([string_col("Symbol", ["BRK-B", "AAPL", "VOD.L"])])
converted = dm.canonicalize_symbols(
    yahoo_symbols, dm.AssetClass.STOCKS, convention=dm.SymbolConvention.YAHOO
)
