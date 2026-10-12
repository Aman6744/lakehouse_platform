# ADR-0006: Merge-on-read for CDC targets, copy-on-write for marts

- **Status:** Accepted

## Context

Parquet files are immutable, so updating a few rows inside one has two possible
strategies:

- **Copy-on-write** rewrites the whole file with the change applied. Reads stay
  fast — one clean file, no reconciliation — and writes are expensive in
  proportion to file size, not to rows changed.
- **Merge-on-read** leaves the data file alone and writes a small delete file
  recording which rows are superseded. Writes are cheap; reads must reconcile
  data against delete files, and compaction periodically folds them back in.

Iceberg defaults all three write modes (`merge`, `update`, `delete`) to
copy-on-write. The CDC applier (ADR-0003) runs a `MERGE` every minute against
tables up to 50M rows, with updates scattered across the key space because
customers transact independently. Under copy-on-write that rewrites every data
file containing any changed row — potentially a large fraction of the table —
**every minute**.

The easy mistake is to leave the write modes unset and assume the Iceberg
default is the safe one. It is not: the default is tuned for read-heavy tables
that change rarely, the opposite of a CDC target. On the arithmetic, this is the
strongest candidate for the first thing on the platform to buckle under load.

## Decision

CDC target tables are created merge-on-read, as a property set the applier owns
in code rather than prose in a document:

```python
CDC_TARGET_PROPERTIES = {
    "format-version": "2",
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
    "write.distribution-mode": "hash",
}
```

`cdc_apply.tblproperties_clause()` renders it into DDL and
`cdc_apply.assert_target_is_merge_on_read()` is a startup check on the streaming
job, so a target that silently reverted to copy-on-write — recreated by hand,
restored from backup, cloned from a template — fails loudly instead of quietly
costing many times as much. `test_target_is_configured_for_merge_on_read`
asserts both directions. `write.distribution-mode = hash` clusters incoming
writes by the merge key so a given MERGE touches fewer files.

**Gold marts keep the copy-on-write default.** Written once per partition per day
through the publish gate and read constantly, so the trade-off inverts: no
write-amplification problem to solve, and read reconciliation would be a
permanent tax on every consumer query.

## Alternatives considered

**Copy-on-write everywhere** — the Iceberg default. Rejected on the write cost
above. It is not *wrong* — the data would be correct — which is precisely why it
is dangerous: the failure is a bill, not an error.

**Merge-on-read everywhere, including marts.** Rejected because it degrades
every analyst query to buy nothing on the write side for tables written once a
day.

**A longer streaming trigger** (say 15 minutes) to reduce rewrite frequency while
keeping copy-on-write. A real option, and cheaper to implement than either.
Rejected because it trades freshness — the one thing the micro-batch choice in
ADR-0002 was meant to preserve — to work around a configuration default, and it
moves the problem by an order of magnitude rather than removing it.

## Consequences

- **Read amplification between compactions.** Delete files accumulate for up to a
  compaction cycle, and queries pay reconciliation cost meanwhile. This ADR holds
  only if compaction keeps pace; it is not a standalone decision.
- Compaction becomes load-bearing rather than housekeeping, with its own capacity
  and its own alert. It competes with ingestion for the same pool, which is the
  failure mode named in the design document's "where it breaks first" — a
  maintenance backlog degrades reads slowly enough that nobody notices until
  queries are already slow.
- Two different configurations to keep straight across layers. Mitigated by the
  property set living in code with an assertion, rather than in prose.
- **Not measured.** The claim that write amplification beats the catalog as the
  first bottleneck is arithmetic, not a benchmark. Measuring actual rewrite
  volume per micro-batch against a real table is the first thing I would do with
  production data.
