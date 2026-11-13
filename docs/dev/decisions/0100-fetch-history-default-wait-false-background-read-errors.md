# 100. `fetch()` and `history()` read in the background by default: `wait=False`

Date: 2026-11-11

Status: accepted

"Yes, go with wait=False as the default. Waiting is dumb because it blocks all massive downloads and
makes startup slow." Like every other verb, the call returns a table at once: "If there is no data
in the table, it should return with no rows. It is up to a user to choose when they want the data to
show up and how they want the program to wait." `wait=True` keeps the blocking call and its static
table. What raises at the call, whatever `wait` is: an argument error (`TypeError`, `ValueError`), a
closed session, and, as decisions 66 and 81 and `poll()` and the streams already have it, a crypto
pair passed in code that Massive's crypto ticker list does not hold, or a failed read of that list,
for the paths that take a pair as its two currencies (`fetch(wait=False)` failed its table instead,
a defect). What Massive answers later fails the table, logged and shown in `m.streams()` `Error`, as
for `poll()` and the streams. No `with`-block waiting: `close()` and `__exit__` keep their behavior,
so a read still loading when its session closes fails. Made consistent on the way, since the default
path now runs them: every background and watchlist read runs on the session's read pool of
`max_concurrency` threads, where each background read had an unbounded thread of its own; a
background flat-file `history()` under `AUTO` checks the REST parameters of the days it may read
from REST at the call, as a call that waits raises them, even when the files serve every day; and a
watchlist read that does not wait fails its table when every symbol it held at the call ends
`FAILED`, such as on a plan without the dataset (decision 11), as a call that waits raises then. A
failure that may pass counts once its retries run out; a symbol with rows, or deleted before it
failed, settles the check, and a symbol added later fails alone. Accepted consequences: a script
that reads rows on the next line gets none (pass `wait=True`, or let Deephaven update what is built
on the table); a `try`/`except` around a default call no longer catches Massive's answers; the `call
N start:` line and the table description change (`fetch()` is the background call,
`fetch(wait=True)` the blocking one); the order of a `merge` of a background read and a stream
follows which lands first.
