"""The dataset namespaces of a session: `m.stocks`, `m.options`, ..., `m.partners`.

- `query_building`: `Namespace`, the base class of every namespace class, and the shared
  dataset factory (`build_query`), which returns the query class `_query.py` chooses for the
  dataset (`query_class`, `bar_query_class`);
- `stocks`, `options`, `futures`, `indices`, `forex`, `crypto`, `reference`, `indicators`,
  `economy`, `partners`: one namespace class each, and that namespace's `DatasetSpec`s
  (`DATASETS`), which `_session.py` composes into the process's dataset catalog.

Layer: API, the top layer with `__init__.py`, `_session.py`, and `_query.py`; it may import
every layer below except the ingest code (`_ingest`), which only `_session.py` imports. These
are plain modules, not a namespace package. The package defines no names of its own: import
the module that owns a name.
"""
