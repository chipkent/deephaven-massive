import os

with open(os.path.expanduser("~/massive-test/massive-debug.log"), encoding="utf-8") as log_file:
    text = log_file.read()
lines = text.splitlines()
print(len(lines), "lines")
print(sum(" WARNING " in line or " ERROR " in line for line in lines), "warnings and errors")
print(text.count(os.environ["MASSIVE_API_KEY"]), "copies of the key")
