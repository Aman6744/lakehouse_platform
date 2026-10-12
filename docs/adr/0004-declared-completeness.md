# ADR-0004: Completeness is declared per source, never inferred

- **Status:** Accepted
- **Hard problem:** 2 of 3 — see `src/dataplatform/publish_gate.py`

## Context

"Is this dataset complete?" is not answerable by looking at the dataset. You
cannot know whether more data is coming for a business date — only whether
everything you were promised has arrived.

The failure this prevents is *plausible*, which is what makes it dangerous. A
mart joins loans to repayments. The loan feed is healthy; the repayment feed
stalled three days ago. The joined table is queryable, non-empty, internally
consistent — and wrong: loans genuinely appear to have no repayments. No null
check fires, no referential check fires, because every loan legitimately *may*
have zero repayments. Someone reports that repayments collapsed.

The only thing that catches it is refusing to publish past the slowest input.

## Decision

**Completeness is declared in an arrival contract per source** and decided
against, never inferred from the data. The signal comes from the ingest ledger —
a source reports business date D complete — not from `max(business_date)` in the
table, which answers a much weaker question.

**Two rules, because there are two ways to get the watermark wrong:**

1. A dataset's effective watermark is the **minimum** across its upstreams. A
   maximum ignores the laggard — the obvious bug.
2. Each upstream's own watermark is **the newest date with an unbroken run
   behind it.** A maximum *within* one upstream ignores holes: a source that
   reported D-3 and D but never D-2 says D, and the gate waves through a close
   built on a missing day. The subtler of the two, and the one most likely to
   survive a code review unnoticed — the function reads as if it were already
   correct.

**A missing signal is unknown, not optimistic.** `None` from any upstream
collapses the whole result to `None`. An upstream we know nothing about is not
the same as one that is up to date.

**Publication is write-audit-publish on Iceberg branches.** The candidate goes to
a per-business-date branch no consumer reads; graded P0/P1 checks (design
document §7) run against those exact bytes, with real cardinalities and real
skew; only a clean run is fast-forwarded onto `main` in one atomic catalog call.
A failure drops the branch and records `HELD`. `main` never moved, so
**consumers see stale-but-correct, never fresh-but-wrong.**

**The escape hatch is designed, not stubbed.** An `Override` carries an approver,
a ticket and a reason, publishes as `OVERRIDDEN` rather than `PUBLISHED` so
consumers and auditors can tell afterwards, and **can waive the completeness
hold but never a P0 check.** Incompleteness is a judgement call; a negative money
column is not.

**Every gate decision is recorded** — publish, hold, override, failure — with a
machine-readable `valid_through`. "We looked at this partition and refused it,
for these reasons, at this time" is the evidence an auditor actually asks for.

## Alternatives considered

**Infer completeness from the data** (`max(business_date)`, row-count
thresholds). Rejected: see the two watermark rules. One late file makes the data
look complete when it is not.

**Publish optimistically and retract later.** Rejected because a number that has
been reported on cannot be un-reported — exactly ADR-0005's problem. For lending
and settlement data a late number is recoverable and a wrong number is not.

**One shared `publish_candidate` branch** instead of per-business-date. Rejected:
a backfill and the daily run would collide, and whichever held first would drop
the other's candidate out from under it.

**Run checks before the write, against a sample.** Rejected: a sample has
neither the real cardinalities, the real skew, nor the real join behaviour. The
branch lets the truth be materialised without being exposed.

## Consequences

- **A stalled upstream holds the whole dependent tree past its freshness SLA and
  pages someone.** Intended, and also the gate's largest risk — not technically
  but organisationally: people start asking for overrides routinely, and a
  routine override is a disabled gate with extra paperwork. Override rate per
  dataset is therefore a reported metric.
- The contiguity rule assumes every calendar date is owed by every source. A
  source idle at weekends over-holds. `_expected_dates` is the named seam for a
  per-source business calendar, and it is unimplemented.
- The ledger records *reports*, not rows, which is what makes a holiday safe: a
  complete day with zero rows is a valid report. Absence of a report blocks;
  absence of rows does not.
- `fast_forward` requires `main` to be an ancestor of the branch, so a concurrent
  write to `main` makes the publish fail rather than silently clobber. Recorded
  as `FAILED` with the branch left for inspection — an unrecorded gate crash is
  indistinguishable from a gate that never ran.
- **Only one table at a time.** There is no multi-table atomic publish, so two
  marts that must agree can be momentarily inconsistent. Catalog-level branching
  (ADR-0001) would close this.
