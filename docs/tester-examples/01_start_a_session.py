import os

import deephaven_massive as dm

dm.enable_debug_logging(os.path.expanduser("~/massive-test/massive-debug.log"))

m = dm.MassiveSession()
print(dm.__version__)
print(m)
