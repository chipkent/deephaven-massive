# Vendored third-party content

`massive/` holds a pinned snapshot of Massive's published specifications and documentation:
the OpenAPI specification (`openapi.json`), the documentation index (`llms.txt`), the REST
documentation pages it links (`rest/`), and `manifest.json`, which records each file's URL and
SHA-256 hashes.

These files are Massive's published content, vendored only as input to the schema generator.
They are not covered by this repository's license and are not distributed in the wheel or the
sdist. The library reads none of them at run time; it loads the schemas generated from them
(`src/deephaven_massive/_massive/spec/*.json`).

Never edit the files by hand: the generator verifies every file against the manifest and stops
on any difference, and on any file the manifest does not list. The only exceptions are the
files an operating system's file browser leaves in a folder it shows (`.DS_Store`, `._*`,
`Thumbs.db`, `desktop.ini`): no vendor file can have such a name, so they are ignored. To take a vendor change, from the repository root:

```bash
uv run python -m deephaven_massive._massive.refresh --fetch   # replace the snapshot with the live sources
uv run python -m deephaven_massive._massive.refresh           # regenerate spec/*.json from it
```

Review the snapshot diff and the generated-schema diff together in one pull request. This
README sits beside the snapshot rather than inside it, because the snapshot directory holds
exactly the files its manifest lists.
