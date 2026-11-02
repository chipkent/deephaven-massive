# Turns on the debug log and opens a session on your Massive key. You should see the
# library's version, such as 0.1.0rc1, then <MassiveSession #1 open>.
import os

import deephaven_massive as dm

FOLDER = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
os.makedirs(FOLDER, exist_ok=True)
LOG = os.path.join(FOLDER, "massive-debug.log")
dm.enable_debug_logging(LOG)

# The free plan's session. With a paid Stocks plan, use the line the guide gives instead.
m = dm.MassiveSession(dm.MassiveConfig(rest_basic_subscriptions=set(dm.Subscription)))
print(dm.__version__)
print(m)
