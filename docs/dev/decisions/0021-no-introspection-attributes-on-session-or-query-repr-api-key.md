# 21. Sessions and queries expose no introspection attributes; their reprs are defined

Date: 2026-10-23

Status: accepted

A session has no `name` or `config` and a query no `dataset`; the label shows only in reprs,
warnings, and errors. The public surface stays the verbs, `columns()`, and the namespaces, and the
API key stays out of objects users leave in console globals.
