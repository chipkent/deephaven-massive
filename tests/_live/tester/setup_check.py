"""The tester's setup check: both installs, the same commit in each, and a key that works.

`docs/tester-guide.md` has the tester build two setups from one pinned version:

- **the console install**, the library inside a Deephaven server, for the exercises in the
  web console: a Python virtual environment with `deephaven-server` and the library installed
  by pip from the repository's tag (`~/massive-test/dh-venv` by default), or, as the
  alternative, a Docker image built from `ghcr.io/deephaven/server` with the library
  installed from the checkout and the commit in its `deephaven-massive.commit` label;
- **the source checkout** of the same tag, with `uv` and JDK 17, for the live-run program.

`check_setup` checks both, read only: the live-run program's machine checks
(`machine_checks.check_machine`: Java, `uv`, the commit and whether the tree is clean, the
secrets, the disk, the memory, and a way to keep the machine awake); which tag the checkout
is on; the console install's versions and the commit pip installed it from
(`INSPECT_CONSOLE`, run by the console's own Python or inside the image); that this commit is
the checkout's; and, through the live-run program's probe (`run_probe.probe_key`), whether
Massive accepts the key and which plan each subscription is on. Each item is `OK`, `WARN`, or
`FAIL`, and a failed item says how to fix it.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING, Final

from tests._live.live_run.machine_checks import FAIL, OK, WARN, Item, check_machine

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from tests._live.live_run.run_probe import ProbeResult

INSPECT_CONSOLE: Final = """\
import importlib.metadata as metadata
import json

found = {}
for name in ("deephaven-massive", "deephaven-server", "deephaven-core"):
    try:
        found[name] = metadata.version(name)
    except metadata.PackageNotFoundError:
        found[name] = None
try:
    origin = metadata.distribution("deephaven-massive").read_text("direct_url.json")
except metadata.PackageNotFoundError:
    origin = None
found["commit"] = json.loads(origin or "{}").get("vcs_info", {}).get("commit_id", "")
print(json.dumps(found))
"""
"""What the console's Python runs to describe its install: one line of JSON with the versions
of `deephaven-massive`, `deephaven-server`, and `deephaven-core` (None when missing), and the
commit pip installed the library from (`""` when it came from a folder)."""

COMMIT_LABEL: Final = "deephaven-massive.commit"
"""The Docker image label that holds the commit its library was installed from."""


def tag_item(root: Path, run: Callable[[list[str]], str]) -> Item:
    """Name the tag the checkout is on: the tester installs a pinned version, not `main`.

    Args:
        root: The repository root.
        run: Runs a command and returns its output, `""` when it fails.

    Returns:
        `OK` with the tag, or `WARN` when the checkout is on no tag.
    """
    git = shutil.which("git") or "git"
    tag = run([git, "-C", str(root), "describe", "--tags", "--exact-match", "HEAD"]).strip()
    if tag:
        return Item("version", OK, f"the checkout is on {tag}")
    return Item("version", WARN, "the checkout is on no tag: check out the version the owner named")


def console_command(console_python: Path | None, docker_image: str) -> list[str]:
    """Return the command that runs `INSPECT_CONSOLE` in the console install.

    Args:
        console_python: The console's Python, such as `~/massive-test/dh-venv/bin/python`.
        docker_image: The Docker image instead, or `""`.

    Returns:
        The command line.
    """
    if docker_image:
        docker = shutil.which("docker") or "docker"
        return [
            docker,
            "run",
            "--rm",
            "--entrypoint",
            "python",
            docker_image,
            "-c",
            INSPECT_CONSOLE,
        ]
    return [str(console_python), "-c", INSPECT_CONSOLE]


def image_commit(docker_image: str, run: Callable[[list[str]], str]) -> str:
    """Read the commit a Docker image's library was installed from, from the image's label.

    Args:
        docker_image: The image.
        run: Runs a command and returns its output.

    Returns:
        The commit, or `""` when the label is missing.
    """
    docker = shutil.which("docker") or "docker"
    template = "{{ index .Config.Labels " + json.dumps(COMMIT_LABEL) + " }}"
    text = run([docker, "image", "inspect", "--format", template, docker_image]).strip()
    return "" if text == "<no value>" else text


def console_items(
    found: Mapping[str, object] | None, *, checkout_commit: str, where: str
) -> list[Item]:
    """Judge the console install against the checkout.

    Args:
        found: What `INSPECT_CONSOLE` printed, parsed, with the image's commit for Docker;
            None when it could not run.
        checkout_commit: The source checkout's commit.
        where: The console's Python or image, for the messages.

    Returns:
        The `console` item (the versions), and the `same commit` item.
    """
    if found is None:
        return [
            Item(
                "console",
                FAIL,
                f"could not run {where}: install the console setup as the guide says",
            )
        ]
    library, server = found.get("deephaven-massive"), found.get("deephaven-server")
    core = found.get("deephaven-core")
    if not library or not (server or core):
        missing = "deephaven-massive" if not library else "the Deephaven server"
        return [Item("console", FAIL, f"{where} lacks {missing}: install it as the guide says")]
    versions = f"deephaven-massive {library}, deephaven-core {core}, in {where}"
    commit = str(found.get("commit") or "")
    if not commit:
        same = Item("same commit", WARN, "the console's library came from no commit pip records")
    elif commit == checkout_commit:
        same = Item("same commit", OK, f"both are {commit}")
    else:
        same = Item(
            "same commit",
            FAIL,
            f"the console has {commit} and the checkout {checkout_commit}: install the same "
            "version in both",
        )
    return [Item("console", OK, versions), same]


def probe_items(result: ProbeResult) -> list[Item]:
    """Describe the key's probe: accepted or not, and each subscription's plan.

    Args:
        result: The live-run program's probe of the key.

    Returns:
        The `key` item, and one `plan` item per subscription when the probe could tell.
    """
    if result.key_rejected:
        return [Item("key", FAIL, "Massive rejected the API key: copy it again from the dashboard")]
    if result.tiers is None:
        return [Item("key", WARN, f"could not tell the plans ({result.error}); try again soon")]
    items = [Item("key", OK, "Massive accepted the API key")]
    for name, tier in result.tiers.items():
        history = result.history_start.get(name, "")
        items.append(Item(f"plan {name}", OK, f"{tier}, history from {history}"))
    flat = ", ".join(result.flat_files) or "none"
    items.append(Item("flat files", OK, flat))
    return items


def check_setup(
    *,
    root: Path,
    folder: Path,
    environ: Mapping[str, str],
    console_python: Path | None,
    docker_image: str,
    run: Callable[[list[str]], str],
    probe: Callable[[], ProbeResult],
) -> list[Item]:
    """Check both setups and the key.

    Args:
        root: The repository root of the source checkout.
        folder: The tester's folder, for the free disk.
        environ: The environment, for the secrets.
        console_python: The console install's Python, or None with `docker_image`.
        docker_image: The console install's Docker image, or `""`.
        run: Runs a command and returns its output, `""` when it fails.
        probe: Probes the key (`run_probe.probe_key` on the environment's credentials).

    Returns:
        Every item, the machine's first.
    """
    result = probe()
    items, environment = check_machine(
        root=root, disk_path=folder, environ=environ, clock_offset=result.clock_offset
    )
    items.append(tag_item(root, run))
    where = docker_image or str(console_python)
    text = run(console_command(console_python, docker_image)).strip()
    found: dict[str, object] | None = None
    if text:
        found = json.loads(text.splitlines()[-1])
        if docker_image:
            found["commit"] = image_commit(docker_image, run)
    items.extend(console_items(found, checkout_commit=str(environment["commit"]), where=where))
    items.extend(probe_items(result))
    return items
