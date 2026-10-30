"""The build-time refresh of the wire schemas: vendored snapshot in, `spec/*.json` out.

`python -m deephaven_massive._massive.refresh` regenerates the committed wire schemas from a
pinned snapshot of Massive's published specifications, `--fetch` replaces that snapshot with
the live sources, and `--check` reports how the live sources differ from it. The snapshot
lives outside the package, in `vendor/massive/` at the top of the source checkout: it is
Massive's published content, vendored only as generation input, not covered by the
repository's license, and in neither the wheel nor the sdist. So the package runs from a
source checkout only; the wheel excludes all of it and ships only the generated
`spec/*.json`. It may import any part of `_massive`, and nothing but itself and its
own tests imports it.

Modules:
    - `__main__`: the command line.
    - `generate`: the refresh itself, composing the modules below and writing the result.
    - `snapshot`: the snapshot's location, layout, and manifest; storing and verified loading.
    - `fetch`: downloads the vendor sources and puts them in canonical form.
    - `drift`: the drift check: compares the live vendor sources with the snapshot, the
      committed schemas, and the flat-file bucket, and reports what changed.
    - `spec_diff`: reads each path or channel of a specification as the drift check compares
      it, and describes how two copies differ.
    - `docs`: reads the documentation index (`llms.txt`) and the REST, WebSocket, and
      flat-file pages it links.
    - `knowledge_base`: the knowledge-base articles the library relies on, and the Markdown
      extracted from each article's HTML page.
    - `openapi`: resolves references and merged schemas in the OpenAPI specifications.
    - `fields`: the field rules every generator shares: names, representation, units,
      conversions, and documented examples.
    - `rest_sources`: turns the OpenAPI specification into REST sources, with their result keys and
      parameters.
    - `ws_sources`: turns the WebSocket specification into WebSocket sources.
    - `flat_file_sources`: turns the flat-file pages into flat-file sources.
    - `kinds`: the dataset kinds across transports: memberships, the type and transport
      agreement, and the kind column order.
    - `naming`: the naming policy shared by every transport.
    - `gates`: the refresh gates, the problems they report, and the checks that span sources.
    - `plain_text`: the fixed rules that turn a vendor's field description into plain text.
    - `render`: writes `spec/rest.json`, `spec/ws.json`, `spec/flatfiles.json`, and
      `spec/descriptions.json` in their canonical text.
    - `hand_input`: bundles the hand input for the generators.
    - `lexicon`, `overrides`, `exclusions`: every piece of hand input, each entry with its
      reason. Nothing else in the refresh is written by hand.

The package defines no names of its own.
"""
