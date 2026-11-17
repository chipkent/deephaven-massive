import os

from deephaven.pandas import to_pandas

diagnostics = os.path.expanduser("~/massive-test/diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {
    "problems": m.problems(),
    "streams": m.streams(),
    "connections": m.connections(),
    "rate_limits": m.rate_limits(),
    "symbol_reads": m.symbol_reads(),
}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
