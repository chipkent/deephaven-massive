# 26. One wire package

Date: 2026-10-26

Status: accepted

`_massive` owns all wire access, the vendored snapshot, and the generated schemas; it imports
nothing from Deephaven or the rest of the library, and an AST test enforces the boundary.
