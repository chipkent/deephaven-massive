# 102. From `0.1.0`, a breaking change needs the owner's permission and names the next version

Date: 2026-11-11

Status: accepted

The library had no users before its first release, so nothing was kept, aliased, or shimmed for
compatibility and a rename was an ordinary edit. Once `0.1.0` is on PyPI, users' code and settings
files depend on the documented contract. A change breaks when code written to the last release's
documented contract stops working or gets a different documented result. From `0.1.0`, such a change
needs the owner's permission; when the owner requests a public change, the agent says whether it
breaks code and, if so, which version comes next, before making it. Versions follow
`docs/dev/releasing.md`, "Which number to raise": pre-releases before `0.1.0` owe no compatibility;
in `0.x` a break raises the minor and anything else the patch; from `1.0.0`, standard Semantic
Versioning.
