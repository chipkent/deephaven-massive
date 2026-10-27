"""The build-time refresh of the wire schemas: vendored snapshot in, `spec/*.json` out.

`python -m deephaven_massive._massive.refresh` regenerates the committed wire schemas from a
pinned snapshot of Massive's published specifications, and `--fetch` replaces that snapshot
with the live sources. The snapshot lives outside the package, in `vendor/massive/` at the
top of the source checkout: it is Massive's published content, vendored only as generation
input, not covered by the repository's license, and in neither the wheel nor the sdist. So the
package runs from a source checkout only; the wheel excludes all of it and ships only the
generated `spec/*.json`. It may import any part of `_massive`, and nothing but itself and its
own tests imports it.

Modules:
    - `__main__`: the command line.
    - `generate`: the refresh itself, composing the modules below and writing the result.
    - `snapshot`: the snapshot's location, layout, and manifest; storing and verified loading.
    - `fetch`: downloads the vendor sources and puts them in canonical form.
    - `docs`: reads the documentation index (`llms.txt`) and the REST pages it links.
    - `openapi`: resolves references and merged schemas in the OpenAPI specification.
    - `rest`: turns the specification into REST sources by the representation, naming, and
      parameter rules.
    - `naming`: the naming policy shared by every transport.
    - `gates`: the refresh gates, the problems they report, and the checks that span paths.
    - `render`: writes `spec/rest.json` in its canonical text.
    - `hand_input`: bundles the hand input for the generators.
    - `lexicon`, `overrides`, `exclusions`: every piece of hand input, each entry with its
      reason. Nothing else in the refresh is written by hand.

The package defines no names of its own.
"""
