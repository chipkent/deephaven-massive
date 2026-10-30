"""A fake child process of the live-run program: pytest or a built-in group, scripted by a test.

The live-run program (`tests/_live/live_run.py`) runs every group in a child process: pytest
with the outcomes plugin, or its own script's `_group` command. Its tests must never run the
real live suites, which would call Massive, so they point the program's interpreter at a
wrapper that runs this file instead, with the same arguments. It does what the test scripts in
the JSON of the `FAKE_LIVE_CHILD` environment variable, in this order:

- `print`: lines to print (a test plants secrets in them to see the program clean its logs);
- `evidence`: text appended to `live-evidence.log` in the working directory, as the live
  tests' evidence recorder does (never while collecting);
- `collection`: with `--collect-only`, the object written to `MASSIVE_LIVE_COLLECTION`;
- `outcomes`: for each node id on the command line, its phase records, appended to
  `MASSIVE_LIVE_OUTCOMES` as the outcomes plugin would;
- `results`: for `_group`, the lines appended to the attempt's `results.jsonl`;
- `prompt`: for `_group`, a line printed before the results, after which the child reads one
  line from its standard input (the upgrade check's Enter);
- `hang`: wait for a signal instead of exiting (never while collecting); `ignore_interrupt`
  also ignores SIGINT, so only SIGKILL ends it;
- `exit`: the exit status, 0 by default.

It also writes its command line to `argv.json` beside the outcomes or results, so a test can
read what the program ran. It imports only the standard library, and runs under the test's
own interpreter.
"""

from __future__ import annotations

import json
import os
import signal
import sys
from pathlib import Path


def _option(argv: list[str], name: str) -> str:
    """Return the value after an option on a command line, or `""`."""
    return argv[argv.index(name) + 1] if name in argv else ""


def main() -> int:
    """Act as the scripted child.

    Returns:
        The scripted exit status.
    """
    argv = sys.argv[1:]
    script = json.loads(os.environ.get("FAKE_LIVE_CHILD", "{}"))
    for line in script.get("print", []):
        sys.stdout.write(line + "\n")
    sys.stdout.flush()
    if "evidence" in script and "--collect-only" not in argv:
        with Path("live-evidence.log").open("a", encoding="utf-8") as file:
            file.write(script["evidence"])
    collection = os.environ.get("MASSIVE_LIVE_COLLECTION", "")
    outcomes = os.environ.get("MASSIVE_LIVE_OUTCOMES", "")
    if "--collect-only" in argv and collection and "collection" in script:
        Path(collection).write_text(json.dumps(script["collection"]), encoding="utf-8")
    elif outcomes:
        Path(outcomes).with_name("argv.json").write_text(json.dumps(argv), encoding="utf-8")
        with Path(outcomes).open("a", encoding="utf-8") as file:
            for nodeid in argv:
                file.writelines(
                    json.dumps({"nodeid": nodeid, **record}) + "\n"
                    for record in script.get("outcomes", {}).get(nodeid, [])
                )
    elif "_group" in argv:
        folder = Path(_option(argv, "--out")) / _option(argv, "--attempt")
        (folder / "argv.json").write_text(json.dumps(argv), encoding="utf-8")
        if "prompt" in script:
            sys.stdout.write(script["prompt"] + "\n")
            sys.stdout.flush()
            sys.stdin.readline()
        with (folder / "results.jsonl").open("a", encoding="utf-8") as file:
            for line in script.get("results", []):
                file.write(json.dumps(line) + "\n")
    if script.get("hang") and "--collect-only" not in argv:
        if script.get("ignore_interrupt"):
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        sys.stdout.write("hanging\n")
        sys.stdout.flush()
        while True:
            signal.pause()
    return int(script.get("exit", 0))


if __name__ == "__main__":
    sys.exit(main())
