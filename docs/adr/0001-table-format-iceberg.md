# ADR-0001: Apache Iceberg as the table format

- **Status:** Accepted

## Context

A "table" in object storage is just files; a table format is the metadata layer
that says which files constitute the table right now. That indirection buys
atomic commits, so readers never see half a write; retained older file lists, so
time travel exists; and schema evolution.

Two of the three hard problems need more than that. The publish gate (ADR-0004)
must materialise a candidate somewhere no consumer reads, validate it, then make
it visible in one operation. The audit trail (ADR-0005) needs named, long-lived
references to past states.

## Decision

**Apache Iceberg**, on S3 with Parquet. Three reasons, in the order they
mattered:

1. **Branches and tags are first-class.** The one that decided it. A branch is
   where the gate stages a candidate; `fast_forward` is the single atomic
   catalog operation that publishes it; a tag is how "as reported on 31 March"
   becomes an ordinary SQL read. ADR-0004 and ADR-0005 lean on this directly
   rather than approximating it.
2. **Genuine engine neutrality.** Spark writes it, Trino reads it, neither is
   privileged — load-bearing given that applications, analysts and data
   scientists all want different engines.
3. **Hidden partitioning.** Consumers filter `WHERE business_date = …` without
   knowing the physical layout, and the layout can change without rewriting
   their queries.

## Alternatives considered

**Delta Lake.** Honestly the nicer developer experience and better Spark
ergonomics. Rejected because its branching story (shallow clones) is weaker than
Iceberg refs for write-audit-publish, and its gravity pulls toward one vendor's
engine. Worth conceding that Delta would have been faster to build on.

**Apache Hudi.** Arguably the best of the three at exactly the CDC upsert problem
in ADR-0003, with a record-level index that would reduce the write amplification
ADR-0006 manages by hand. Rejected because the operational surface is larger —
table services, timeline management, several table types — and a small team would
run this. A team-size argument, not a claim of technical superiority.

**Plain Parquet in Hive-style directories.** Rejected: no atomic commits, so the
gate's central guarantee cannot be built at all.

## Consequences

- Iceberg's SQL surface is thinner. Branch and tag operations are DDL rather
  than a convenient API, `MERGE` lacks some of Delta's convenience clauses, and
  a few operations are reachable only from the DataFrame API — which forced a
  real decision in ADR-0005's partition overwrite.
- Write amplification on CDC targets must be managed explicitly rather than by a
  record index. That is ADR-0006.
- **Catalog.** An Iceberg REST catalog (Polaris or Nessie) rather than Hive
  Metastore — the interface Iceberg itself defines, so every engine speaks it
  without a vendor bridge. It is also the component I expect to break second,
  and the one piece here I have no hands-on operating experience with. Nessie's
  catalog-level multi-table branching would close the single-table publish limit
  noted in ADR-0004, and this choice positions the platform to adopt it later
  without a format migration.
