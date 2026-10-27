"""The run-time wire vocabulary: what each Massive REST path, channel, and flat file carries.

`spec` describes Massive's wire format as plain, frozen values, so that every client decodes
against the same description and no schema is written by hand:

- `spec.types` defines the value types: `Transport`, `Source` and `RestPath` (one REST path,
  WebSocket channel, or flat-file prefix), `Field` (one wire key and its long-form column),
  `Param` and `ParamKind` (REST parameters), `Membership` (a source's place in a dataset kind),
  `WireType` and `WireUnit` (a field's type and timestamp unit), and `DriftReport` (what a
  payload had that its source did not, and the reverse).
- `spec.convert` checks one decoded JSON value against its `WireType` and converts it
  exactly (an ISO instant to integer nanoseconds, for example), for every JSON decoder.
- The generated `rest.json`, `ws.json`, and `flatfiles.json`, and the loaders that read them
  once into those types, are added beside it by the refresh.

`spec` is a bottom layer of `_massive`: it imports only the standard library, never another
part of `_massive`, so the REST, WebSocket, and flat-file clients can all depend on it.
"""
