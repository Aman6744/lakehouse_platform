# Architecture Decision Records

One record per decision that was genuinely contested — what was chosen, what was
rejected, and what the choice costs. The design document states the decisions
and the reasoning behind the three hard problems; these records carry the
alternatives and the trade-off detail, so the design document can stay the
length the brief asks for without the reasoning being lost.

Deliberately a short list. A record exists here only where a competent engineer
could reasonably have chosen differently; decisions with an obvious answer are
stated in the design document and left there.

Format is a trimmed [MADR](https://adr.github.io/madr/): context, decision,
alternatives considered, consequences. Status is `Accepted` unless noted.

## Index

| # | Decision | Status |
|---|---|---|
| [0001](0001-table-format-iceberg.md) | Apache Iceberg as the table format | Accepted |
| [0002](0002-spark-only-no-flink.md) | Spark for both batch and streaming; no Flink | Accepted |
| [0003](0003-cdc-ordering-and-idempotency.md) | Source log position as the CDC ordering key | Accepted |
| [0004](0004-declared-completeness.md) | Completeness is declared per source, never inferred | Accepted |
| [0005](0005-restatement-as-knowledge-time.md) | Corrections are new knowledge versions, not edits | Accepted |
| [0006](0006-merge-on-read-for-cdc-targets.md) | Merge-on-read for CDC targets, copy-on-write for marts | Accepted |
| [0007](0007-physical-storage-compression-and-encryption.md) | Physical storage — compression and encryption | Accepted |

## Reading order

[0003](0003-cdc-ordering-and-idempotency.md),
[0004](0004-declared-completeness.md) and
[0005](0005-restatement-as-knowledge-time.md) are the three hard problems, and
are the three to read if you only read three.
[0006](0006-merge-on-read-for-cdc-targets.md) is the one with the largest cost
consequence attached to getting it wrong.

Decisions that were made but not contested enough to warrant a record of their
own are in the design document: the Iceberg REST catalog and Dagster
orchestration are covered under their chosen options in §2 and in ADR-0001 and
ADR-0002 respectively, the serving split in §8, late-arriving events in §3.1,
and graded data quality checks with their alerting policy in §7.
