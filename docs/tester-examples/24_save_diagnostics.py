# Save the diagnostics tables of the session in m, the one the last example opened.
import os

from deephaven.pandas import to_pandas

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
diagnostics = os.path.join(folder, "diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {
    "streams": m.streams(),
    "connections": m.connections(),
    "rate_limits": m.rate_limits(),
    "symbol_reads": m.symbol_reads(),
}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
