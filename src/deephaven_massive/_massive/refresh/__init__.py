"""The build-time refresh of the wire schemas: vendored snapshot in, `spec/*.json` out.

`python -m deephaven_massive._massive.refresh` regenerates the committed wire schemas from a
pinned snapshot of Massive's published specifications, and `--fetch` replaces that snapshot
with the live sources. The package runs from a source checkout only: the wheel excludes all
of it, the snapshot included, and ships only the generated `spec/*.json`. It may import any
part of `_massive`, and nothing but itself and its own tests imports it.

Modules:
    - `__main__`: the command line.
    - `fetch`: downloads the vendor sources and stores them canonically, with a manifest of
      their SHA-256 hashes.
    - `docs`: reads the documentation index (`llms.txt`) and the REST pages it links.
    - `generate`: turns the snapshot into `spec/rest.json`, applying the representation rules,
      the naming rule, and the refresh gates.
    - `lexicon`, `overrides`, `exclusions`: every piece of hand input, each entry with its
      reason. Nothing else in the refresh is written by hand.
    - `snapshot/`: the vendored sources (not a module).

The package defines no names of its own.
"""
