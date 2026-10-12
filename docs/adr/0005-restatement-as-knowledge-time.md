# ADR-0005: Corrections are new knowledge versions, not edits

- **Status:** Accepted
- **Hard problem:** 3 of 3 — see `src/dataplatform/restatement.py`

## Context

The first two hard problems are about getting data in correctly. This one is
about **having been wrong in public.**

A settlement file for the 3rd arrives on the 7th with different totals. Finance
closed the month on the 5th using the old numbers. An auditor later asks why the
closed report and the live table disagree. Both answers have to remain
available, and the difference has to be explainable.

Every row carries five distinct notions of time, and most subtle bugs in a
platform like this come from collapsing two of them: `event_time`, `ingest_time`,
`business_date`, `valid_time`, and `knowledge_time` (what the platform believed
as of a given moment). A restatement moves along the knowledge-time axis, and an
auditor's question is phrased in it.

## Decision

**The realisation that made this tractable: an Iceberg table's snapshot history
already *is* a knowledge-time axis.** Every commit records what the platform
believed at that instant, for free. There is no need to build bitemporal
versioning — only to stop discarding it and name the moments that matter.

So: ordinary snapshots expire after 7 days to keep metadata small, and a **tag**
is cut at every finance close and every *published* restatement, retained five
years:

```sql
SELECT * FROM table VERSION AS OF 'close_2026_03_31'
```

An ordinary SQL read an auditor can run themselves. The same mechanism gives
data scientists leakage-free training sets with no second system.

**Classification is by content hash, never filename.** Partners rename, re-zip
and re-send constantly; only the bytes are evidence. Unseen bytes for this date →
`NEW`; these exact bytes → `DUPLICATE`, a no-op; this date but different bytes →
`RESTATEMENT`. Treating the last as an append is the default behaviour of every
naive file loader and the one that silently doubles a day's settlement totals.

**The ledger lookup considers `CLAIMED` rows as well as `COMPLETE` ones.**
Restricting it to complete rows classifies a redelivery arriving while the first
load is still in flight as brand new, and loads the same file twice. `COMPLETE`
is the intuitive filter to write here, which is exactly why it is dangerous.

**Claiming is a MERGE with a retry loop, and ownership is read back.** Iceberg
gives conflict *detection*, not automatic re-execution — a losing commit raises
rather than transparently replanning — so the retry loop is load-bearing. And
"my MERGE did not raise" is not the same statement as "I own this claim", so the
winner is established by reading its own `worker_id` back.

**The correction replaces the partition in one commit** via
`DataFrameWriterV2.overwrite(condition)`, detaching old files as it attaches new
ones, so no reader sees the partition empty or doubled. It writes to the publish
gate's candidate branch, so **a correction failing its own P0 checks is held like
any other candidate — a correction is not automatically an improvement.**

**Tag and invalidation fire from the publish path, never from the write.** Both
are claims about published state; raising them before the gate passes means a
rebuild picks up old or new numbers depending on timing.

## Alternatives considered

**Append every version with a version column and let readers take the latest.**
Arguably purer — full bitemporal, nothing overwritten. Rejected on the consumer
side: every query must then remember to filter to the latest knowledge version,
and a plain `SUM(amount)` silently double-counts the first time someone forgets.
Same class of trap as filename deduplication — correct if everyone is careful,
wrong the first time they are not. Better to push the complexity into the write
path, where it is written once and tested. The cost is a dependency on the table
format for recovering old versions.

**`INSERT INTO … REPLACE WHERE`.** Reads naturally, and is Delta Lake syntax
Iceberg does not have. Caught by checking the documentation rather than trusting
a draft.

**`DELETE` then `INSERT`.** Rejected: two commits, so it has exactly the visible
intermediate state this must avoid.

**Row-by-row comparison to detect a correction.** Correct, and requires reading
500M rows to answer a question a 32-byte hash answers — and the hash answers the
stronger question, "are these the exact bytes I already processed".

## Consequences

- **Tags pin data files, so superseded partition versions stay on disk for five
  years** — a single-digit percentage of storage, and a bargain against not
  being able to answer the auditor.
- **The sharper cost: pinning files for five years collides directly with a DPDP
  erasure request.** You cannot delete rows a tag is holding. ADR-0007's
  per-subject key destruction is the answer, and it is a direction rather than a
  finished design. The constraint most likely to force a redesign here.
- A few thousand refs per table after five years (≈1,825 daily closes plus one
  per restatement). Fine for the ref list itself; the cost is the pinned files,
  and the metadata overhead of that many refs on a real catalog is unmeasured.
- `dataset_valid_through` reads the *latest* decision per business date, so a
  date published Monday and held Tuesday stops being served rather than staying
  visible because it was once fine.
- Invalidation emits rows rather than calling a scheduler directly, so the
  propagation is itself auditable — if a mart is wrong, there is a record of
  whether it was ever told to rebuild.
