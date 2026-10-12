# ADR-0002: Spark for both batch and streaming; no Flink

- **Status:** Accepted

## Context

The platform needs a streaming path (CDC and event ingestion) and a batch path
(mart builds, backfills, compaction). The default senior-engineer instinct is
Flink for the former and Spark for the latter, so the question is whether the
latency requirement justifies two engines.

The arithmetic settles it:

- 10,000 events/sec at peak × ~500 bytes ≈ **5 MB/s**.
- One minute of peak traffic is ~600,000 events per micro-batch.
- Nothing downstream needs sub-second data: applications read a KV store,
  analysts work in minutes, finance works in days.

Neither number is large. Volume is not the constraint anywhere in this design.

## Decision

**Spark only** — batch plus Structured Streaming, micro-batches on a 1-minute
trigger. One engine, one set of transformation code, one deployment path, one
on-call skill set.

Offsets are owned by the Structured Streaming checkpoint; there is no second
offset store and no manual commit. The window the applier must survive is
therefore the framework's own at-least-once retry, which is exactly what
ADR-0003 is built for.

## Alternatives considered

**Flink for the streaming half.** Genuinely the better streaming engine: true
event-time windowing, sub-second latency, a more natural state model. Rejected
because a second engine is a large permanent tax — two sets of transformation
code, two deployment pipelines, two failure modes, two skill sets — to buy
latency nobody asked for. If a fraud or real-time-decisioning use case appears,
that decision gets **revisited rather than tuned**, because the requirement has
changed.

**Continuous processing instead of micro-batch.** Rejected: narrower operator
support, buying sub-second latency this design has no consumer for.

**A warehouse (Snowflake, BigQuery) instead of a lakehouse.** Simpler and
probably cheaper to operate at this size. The brief specifies a lakehouse, but
the honest technical argument is the data scientists: they want open-format file
access with their own tools, and a warehouse makes that awkward and expensive.
If pushed that a warehouse would be better here, that is a reasonable position
worth conceding rather than fighting.

## Consequences

- A latency floor of roughly 30–60 seconds end to end. Accepted, and stated as a
  choice rather than a limitation.
- One commit per table per minute, which is the source of both metadata
  accumulation on the catalog and the small-file pressure that makes compaction
  load-bearing in ADR-0006.
- No true event-time windowing primitive, so late-event handling is built on
  partition semantics and recompute (design document §3.1) rather than on
  watermark operators.
- Backfill and streaming share one code path — the main practical benefit, and
  the reason bronze stays immutable: recovering from bad transformation logic is
  a recompute, not an apology.
- **Orchestration is Dagster**, for the same reason the engine is singular: the
  real scheduling condition here is "has business date D arrived from every
  upstream", not "is it 2am". Dagster's partition-aware assets express that
  natively, where a time-triggered DAG has to rebuild it in sensor code.
