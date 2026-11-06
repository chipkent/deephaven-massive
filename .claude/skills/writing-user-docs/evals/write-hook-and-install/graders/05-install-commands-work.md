---
type: llm
focus: trace
weight: 1
---

hook-install.md's Install offers venv or uv as a choice stated before either block, looks up the newest release tag in the install command with the repository name written out (no constant shell variable), sets MASSIVE_API_KEY and the two optional S3 keys in one step, and starts `deephaven server` from the activated environment.
