import os

import deephaven_massive as dm

# tester.py and the Docker setup name the test folder in MASSIVE_TEST_FOLDER.
folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
os.makedirs(folder, exist_ok=True)
dm.enable_debug_logging(os.path.join(folder, "massive-debug.log"))

m = dm.MassiveSession()
print(dm.__version__)
print(m)
