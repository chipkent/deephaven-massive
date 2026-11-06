"""Check settings-file answers: load each with `MassiveConfig.from_file` and compare it.

The checker of the `configuration-page-sufficiency` eval case.

Usage, from the repository root:

    skill=.claude/skills/writing-user-docs
    uv run python $skill/scripts/compare_settings_answers.py \
        $skill/evals/configuration-page-sufficiency/expected/settings.yaml <answers directory>

An answer is `<answers directory>/<task>.yaml`; a task with no file is read as no settings, every
field at its default. The expected settings file holds one mapping of settings per task. An
answer is right when it loads and gives the same configuration as its task's mapping. The
script prints one line per task and a total, and exits 0 only when every task is right.

It starts an embedded Deephaven server first, because the library imports Deephaven, and leaves
through `os._exit`, because a normal interpreter shutdown after the JVM has run can crash.
"""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile

import yaml
from deephaven_server import Server

Server(port=0, jvm_args=["-Xmx1g"]).start()

import deephaven_massive as dm  # noqa: E402 - the library imports Deephaven, so after the start

os.environ["MASSIVE_API_KEY"] = "key"
os.environ.pop("MASSIVE_S3_ACCESS_KEY_ID", None)
os.environ.pop("MASSIVE_S3_SECRET_ACCESS_KEY", None)


def load(text: str) -> dm.MassiveConfig:
    """Return the configuration a settings file's text gives; no settings gives the defaults."""
    if yaml.safe_load(text) in (None, {}):
        return dm.MassiveConfig()
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as file:
        file.write(text)
    return dm.MassiveConfig.from_file(file.name)


def main() -> int:
    """Compare every task's answer with its expected settings and print the results.

    Returns:
        The exit status: 0 when every task is right, else 1.
    """
    expected = yaml.safe_load(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    answers = pathlib.Path(sys.argv[2])
    right = 0
    for task, settings in expected.items():
        want = load(yaml.safe_dump(settings) if settings else "")
        answer = answers / f"{task}.yaml"
        try:
            got = load(answer.read_text(encoding="utf-8") if answer.exists() else "")
        except (ValueError, OSError) as error:
            sys.stdout.write(f"task {task}: refused: {error}\n")
            continue
        wrong = [
            f"{name} is {getattr(got, name)!r}, expected {getattr(want, name)!r}"
            for name in dm.MassiveConfig.__dataclass_fields__
            if getattr(got, name) != getattr(want, name)
        ]
        right += not wrong
        sys.stdout.write(f"task {task}: {'; '.join(wrong) or 'right'}\n")
    sys.stdout.write(f"{right} of {len(expected)} right\n")
    return 0 if right == len(expected) else 1


if __name__ == "__main__":
    status = main()
    sys.stdout.flush()
    os._exit(status)
