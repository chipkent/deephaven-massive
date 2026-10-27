"""The flat-file (S3) client of the Massive wire package.

Massive publishes one gzip CSV file per dataset and day in an S3 bucket. This package reads
them, and knows nothing of datasets, sessions, or Deephaven:

- `layout`: `object_key`, where a dataset's file for a day lives in the bucket.
- `client`: `FlatFileClient`, the bucket's boto3 client, its S3 error mapping to the wire errors,
  and its close.
- `reader`: `FlatFile`, one object's body decoded block by block into Arrow batches, filtered
  and renamed as a `ReadPlan` of `ReadColumn`s says.

The package defines and imports nothing itself: `deephaven_massive._massive` maps each of these
names straight to its submodule, so using `object_key` or `ReadPlan` never imports `client`,
and with it boto3. It imports only `net` from `_massive`. The library module that uses it is
`_ingest/flatfiles.py`, which builds the read plans and owns the missing-file policy, the reader
pool, and cancellation.
"""
