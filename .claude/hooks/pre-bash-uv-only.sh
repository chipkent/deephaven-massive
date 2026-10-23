#!/usr/bin/env bash
# Claude Code PreToolUse hook for Bash.
#
# Blocks a command that runs python, pytest, ruff, mypy, or pip directly instead of through
# `uv run`, so every tool runs from the project's locked environment. Exit code 2 blocks the
# call and returns the reason on stderr to the agent.
set -euo pipefail

# The check runs on the system python3 in isolated mode (-I), not `uv run`: it needs only the
# standard library, runs before every Bash call, and must not itself be a `uv run` command.
exec python3 -I -c '
import json
import re
import shlex
import sys

BARE = re.compile(r"(python[0-9.]*t?|pytest|py\.test|ruff|mypy|pip[0-9.]*)")
SEPARATORS = {";", "&", "&&", "|", "||", "|&", "(", ")", "{", "}"}
# Wrappers that run another command, with the options that take a separate argument. Their
# options (for example `env -i` or `nice -n 5`) are skipped so the wrapped command is checked.
WRAPPERS = {
    "builtin": set(),
    "command": set(),
    "env": {"-u", "--unset", "-C", "--chdir", "-S", "--split-string"},
    "exec": {"-a"},
    "nice": {"-n", "--adjustment"},
    "nohup": set(),
    "sudo": {"-u", "--user", "-g", "--group", "-C", "-D", "-h", "-p", "-r", "-t", "-U"},
    "time": {"-f", "--format", "-o", "--output"},
}
ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=.*")

command = json.load(sys.stdin).get("tool_input", {}).get("command", "")
# Newlines, backticks, and command substitution also start a new command.
text = re.sub(r"\n|`|\$\(", " ; ", command)
try:
    lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    tokens = list(lexer)
except ValueError:
    tokens = text.split()

at_start = True
wrapper_options = None  # options of the wrapper just seen, while its options are being skipped
skip_argument = False
for token in tokens:
    if token in SEPARATORS:
        at_start, wrapper_options, skip_argument = True, None, False
        continue
    if not at_start:
        continue
    if skip_argument:
        skip_argument = False
        continue
    if wrapper_options is not None and token.startswith("-"):
        if token == "--":
            wrapper_options = None
        elif token in wrapper_options:
            skip_argument = True
        continue
    if ASSIGNMENT.fullmatch(token):
        continue
    if token in WRAPPERS:
        wrapper_options = WRAPPERS[token]
        continue
    at_start = False
    name = token.rsplit("/", 1)[-1]
    if BARE.fullmatch(name):
        print(
            f"Blocked: run `uv run {name} ...` instead of bare `{name}`, so the locked "
            "project environment is used (see AGENTS.md).",
            file=sys.stderr,
        )
        sys.exit(2)
'
