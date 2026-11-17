#!/usr/bin/env bash
# Claude Code PreToolUse hook for Bash.
#
# Blocks a command that would merge a pull request or push to `main`: `gh pr merge` (with any
# flags, `--auto` too), a `gh api` call to a pull request's merge endpoint, the `merges`
# endpoint, or the GraphQL `mergePullRequest` and `enablePullRequestAutoMerge` mutations, and a
# `git push` that names `main` as a destination or pushes every branch. Only the owner merges.
# Exit code 2 blocks the call and returns the reason on stderr to the agent.
set -euo pipefail

# The check runs on the system python3 in isolated mode (-I), not `uv run`: it needs only the
# standard library and runs before every Bash call.
exec python3 -I -c '
import json
import re
import shlex
import sys

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
# Shells whose `-c` string is itself a command line, checked the same way, as is `eval`.
SHELLS = {"bash", "sh", "zsh", "dash", "ksh"}
ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=.*")
# git options before the subcommand that take a separate argument.
GIT_GLOBAL_ARGUMENT = {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path"}
# git push options that take a separate argument.
PUSH_ARGUMENT = {"-o", "--push-option", "--repo", "--receive-pack", "--exec"}
# gh options before the subcommand that take a separate argument.
GH_GLOBAL_ARGUMENT = {"-R", "--repo", "--hostname"}
MAIN_REFS = {"main", "refs/heads/main"}
# A REST endpoint that merges: `pulls/<number>/merge` or the repository `merges` endpoint.
MERGE_ENDPOINT = re.compile(r"(?:^|/)pulls/[^/\s]+/merge(?:[?/]|$)|(?:^|/)merges(?:\?|$)")
MERGE_MUTATION = re.compile(r"\b(?:mergePullRequest|enablePullRequestAutoMerge)\b")
# gh api options that set the HTTP method, and those that add a field (which makes it POST).
GH_METHOD = {"-X", "--method"}
GH_FIELD = {"-f", "-F", "--field", "--raw-field", "--input"}
# Shell keywords that may start a command before the command itself.
KEYWORDS = {"if", "then", "else", "elif", "while", "until", "do", "!"}
# Marks a command substitution, so a quoted one is still checked as a command.
SUBSTITUTION = "\x1f"


def block(what):
    print(
        f"Blocked: {what}. AGENTS.md, Limits, \"Agents never merge\": never commit or push to "
        "`main`, and never merge a PR in any way. The owner merges; tell them the PR is ready.",
        file=sys.stderr,
    )
    sys.exit(2)


def tokenize(command):
    # Newlines and command substitution also start a new command. A substitution inside double
    # quotes stays in its argument, so it is marked for check() to read that argument too.
    text = re.sub(r"`|\$\(", f" ; {SUBSTITUTION} ", command.replace("\n", " ; "))
    try:
        lexer = shlex.shlex(text, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        return list(lexer)
    except ValueError:
        return text.split()


def commands(tokens):
    """Yield each simple command as its argument list, wrappers and assignments removed."""
    argv = []
    wrapper_options = None
    skip_argument = False
    for token in tokens:
        if token in SEPARATORS:
            if argv:
                yield argv
            argv, wrapper_options, skip_argument = [], None, False
            continue
        if argv:
            argv.append(token)
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
        if ASSIGNMENT.fullmatch(token) or token in KEYWORDS or token == SUBSTITUTION:
            continue
        if token in WRAPPERS:
            wrapper_options = WRAPPERS[token]
            continue
        argv.append(token)
    if argv:
        yield argv


def positionals(arguments, takes_argument):
    """Return the non-option arguments, skipping the argument an option takes."""
    found = []
    skip = False
    for argument in arguments:
        if skip:
            skip = False
        elif argument.startswith("-"):
            skip = argument in takes_argument
        else:
            found.append(argument)
    return found


def check_git(arguments):
    rest = list(arguments)
    while rest and rest[0].startswith("-"):
        option = rest.pop(0)
        if option in GIT_GLOBAL_ARGUMENT and rest:
            rest.pop(0)
    if not rest or rest[0] != "push":
        return
    options = rest[1:]
    if "--all" in options or "--mirror" in options or "--branches" in options:
        block("`git push` of every branch pushes `main`")
    words = positionals(options, PUSH_ARGUMENT)
    # The first positional is the repository unless `--repo` names it.
    if not any(option == "--repo" or option.startswith("--repo=") for option in options):
        words = words[1:]
    for refspec in words:
        destination = refspec.lstrip("+").rsplit(":", 1)[-1]
        if destination in MAIN_REFS:
            block(f"`git push` to `main` (refspec `{refspec}`)")


def check_gh(arguments):
    words = positionals(arguments, GH_GLOBAL_ARGUMENT)
    if words[:2] == ["pr", "merge"]:
        block("`gh pr merge` merges a pull request")
    if words[:1] == ["api"]:
        # gh api sends GET unless a method is given or a field makes it POST; GET only reads.
        method = "POST" if any(a.split("=", 1)[0] in GH_FIELD for a in arguments) else "GET"
        for option, value in zip(arguments, arguments[1:]):
            if option in GH_METHOD:
                method = value.upper()
        for argument in arguments:
            if argument.startswith(("--method=", "-X")) and argument not in GH_METHOD:
                method = argument.split("=", 1)[-1].removeprefix("-X").upper()
        for argument in arguments:
            if MERGE_ENDPOINT.search(argument) and method != "GET":
                block(f"`gh api` call to a merge endpoint (`{argument}`)")
            if MERGE_MUTATION.search(argument):
                block("`gh api graphql` call to a merge mutation")


def check(command):
    for argv in commands(tokenize(command)):
        for argument in argv:
            if SUBSTITUTION in argument:
                check(argument)
        name = argv[0].rsplit("/", 1)[-1]
        if name == "git":
            check_git(argv[1:])
        elif name == "gh":
            check_gh(argv[1:])
        elif name == "eval":
            check(" ".join(argv[1:]))
        elif name in SHELLS:
            # The string after the first short-option group holding `c` (`-c`, `-lc`).
            for index, argument in enumerate(argv[1:-1], start=1):
                if re.fullmatch(r"-[a-zA-Z]*c[a-zA-Z]*", argument):
                    check(argv[index + 1])
                    break


check(json.load(sys.stdin).get("tool_input", {}).get("command", ""))
'
