"""The flat-file (S3) client of the Massive wire package.

Massive publishes one gzip CSV file per dataset and day in an S3 bucket. This package reads
them, and knows nothing of datasets, sessions, or Deephaven:

- `layout`: `object_key`, where a dataset's file for a day lives in the bucket.
- `client`: `FlatFileClient`, the bucket's boto3 client, its S3 error mapping to the wire errors,
  and its close.
- `reader`: `FlatFile`, one object's body decoded block by block into Arrow batches, filtered
  and renamed as a `ReadPlan` of `ReadColumn`s says.

It imports only `net` from `_massive`. PEP 8 forbids an underscore in a package name, so it is
spelled `flatfiles`; the library module that uses it is `_flat_files.py`, which builds the read
plans and owns the missing-file policy, the reader pool, and cancellation.
"""

from __future__ import annotations

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.layout import object_key
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan

__all__ = ["FlatFile", "FlatFileClient", "ReadColumn", "ReadPlan", "object_key"]
