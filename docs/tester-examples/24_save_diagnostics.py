# Saves the session's diagnostics tables as files for a bug report. Run it right after a
# problem, before you close the session. You should see the folder the files went to.
import os

from deephaven.pandas import to_pandas

FOLDER = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
diagnostics = os.path.join(FOLDER, "diagnostics")
os.makedirs(diagnostics, exist_ok=True)
tables = {"streams": m.streams(), "connections": m.connections(), "rate_limits": m.rate_limits()}
for name, table in tables.items():
    to_pandas(table.snapshot()).to_csv(os.path.join(diagnostics, f"{name}.csv"), index=False)
print("saved to", diagnostics)
