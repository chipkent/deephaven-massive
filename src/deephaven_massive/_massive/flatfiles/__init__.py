"""The flat-file (S3) client of the Massive wire package (a marker for now, with no contents).

It is the home of the client for Massive's S3 flat files: object keys, the bucket client, the
S3 error mapping, and CSV decoding into Arrow batches. It may import only `net` and `spec` from
`_massive`. PEP 8 forbids an underscore in a package name, so it is spelled `flatfiles`; the
library module that uses it is `_flat_files.py`.
"""
