# 1. No SDK: the library owns its REST and WebSocket clients

Date: 2026-10-26

Status: accepted

It speaks Massive's documented protocols on `urllib3` and `websockets`. Massive's SDK needed about
fourteen workaround layers and kept defects (dropped wildcard siblings, instant reconnects, silently
dropped fields), and a fork that fixed them could not be a PyPI dependency.
