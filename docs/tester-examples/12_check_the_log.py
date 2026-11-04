# Count the debug log's lines, its warnings and errors, and any copy of your key.
import os

folder = os.environ.get("MASSIVE_TEST_FOLDER", os.path.expanduser("~/massive-test"))
with open(os.path.join(folder, "massive-debug.log"), encoding="utf-8") as log_file:
    text = log_file.read()
lines = text.splitlines()
print(len(lines), "lines")
print(sum(" WARNING " in line or " ERROR " in line for line in lines), "warnings and errors")
print(text.count(os.environ["MASSIVE_API_KEY"]), "copies of the key")
