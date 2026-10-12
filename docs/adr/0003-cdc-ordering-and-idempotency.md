# ADR-0003: Source log position as the CDC ordering key

- **Status:** Accepted
- **Hard problem:** 1 of 3 — see `src/dataplatform/cdc_apply.py`

## Context

The obvious CDC implementation reads a micro-batch, MERGEs it, then commits
offsets — two writes to two systems with no transaction spanning them. If the
process dies in between, the batch is re-applied, and whether that is harmless
depends entirely on whether the apply is idempotent.

A "last row wins" MERGE is not idempotent. A replay can overwrite a *newer*
value that landed in between with an older one. The table stays queryable,
nothing errors, nothing pages — the balance is simply wrong, and stays wrong
until someone reconciles against the source months later.

## Decision

**Order by the source database's own log position**, carried as `_lsn`, and gate
the update on it:

```sql
WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET <guarded columns>
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

That single predicate is both the ordering guarantee and the idempotency
guarantee: a replay carries old positions, so it matches no update and inserts
nothing, whether the replay is one second or ten batches later. `>` not `>=`, so
re-applying an identical version does not rewrite the file it lives in.

**Not `source.ts_ms`.** Host clocks are not monotonic — a failover or NTP
correction moves time backwards — and two changes inside one transaction can
share a millisecond, so timestamps *tie*. A log position cannot tie.

**Deletes are soft** — a versioned row with `_tombstone = true` — so a late stale
insert for a deleted key is rejected by the same comparison rather than
resurrecting the row, and auditors see the deletion event rather than an absence.
Consumers read a view that filters tombstones, so no individual query can forget
to.

**Columns the source did not send are not written.** An UPDATE that does not
touch a TOASTed Postgres column makes Postgres omit it from the WAL record, and
Debezium substitutes a sentinel *string* — not a null. Written verbatim it looks
like data. So the batch collapse carries the most recent *real* value forward
**per column**, and the MERGE guards each business column with a changed-flag.
A column genuinely set to NULL still writes NULL, which is why the carry-forward
wraps values in a struct rather than filtering nulls.

**The per-batch marker is an optimisation, not the guarantee.** A sha256 of
topic plus sorted offset ranges, appended to `platform.cdc_applied_batches`
*after* the data commit and probed before it. Marker after the data, never
before: a crash in between costs one redundant MERGE, where the reverse order
would skip a batch that never landed and lose data.

## Alternatives considered

**The batch marker as the correctness mechanism.** Wrong twice over: the marker
column is mutable, so its absence proves nothing, and the probe is not
metadata-pruned — a sha256 is uniformly distributed and defeats min/max file
pruning, so that lookup scans. The LSN predicate carries correctness.

**Offsets in the Iceberg snapshot summary** via `snapshot-property.*` — atomic
with the data commit, strictly better than a second table. Rejected because it
is reachable only from `DataFrameWriterV2` and the apply is SQL `MERGE`.

**Batch extracts instead of log-based CDC.** Rejected on three counts: a nightly
`WHERE updated_at > yesterday` cannot see deletes, misses every intermediate
state, and loads the production database.

**Hard deletes.** Rejected because a stale insert arriving after the delete
would have no row to compare against and would resurrect the key.

## Consequences

- Tombstones accumulate, so a retention job removes them once older than any
  consumer's replay window. Deliberately separate from the CDC path, because
  purging is policy and should not be entangled with correctness.
- The MERGE cannot use `SET *` / `INSERT *`, so the column list is explicit and
  must track schema evolution. Mitigated by deriving it from
  `CdcTable.target_columns` rather than hardcoding.
- Depends on the source exposing a monotonic position. Postgres gives an LSN,
  MySQL a binlog file plus position, Oracle an SCN, MongoDB a resume token. A
  source with none falls back to (transaction id, row order, timestamp) with an
  explicitly weaker guarantee, written down against that source rather than
  silently degraded.
- One partition-pruned lookup per micro-batch against the marker table, on a
  7-day retention.
