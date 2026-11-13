# 41. Test layout mirrors the package

Date: 2026-10-26

Status: accepted

Test layout mirrors the package, one leading underscore dropped per path component; `__init__.py`
maps to `test_init.py`, `__main__.py` to `test_main.py`, and `_` directories under `tests/` are
helpers.
