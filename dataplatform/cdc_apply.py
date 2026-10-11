"""Hard problem 1 of 3 -- applying database CDC to Iceberg safely under replay.

Guarantees this module owns
--------------------------
G1  Replay safety. Re-applying the same Kafka offset range leaves the table in
    exactly the state it was in after the first application. Enforced by the
    `s._lsn > t._lsn` predicate in the MERGE, *not* by the batch marker: a
    replayed batch whose rows are all at or below the target's current `_lsn`
    matches no UPDATE and inserts nothing, whether the replay is immediate or
    ten batches later. `cdc_applied_batches` only makes that cheap, by skipping
    a MERGE that was already provably inert -- so it cannot drift from the table
    in any way that matters. See design document section 6.1.
G2  Total ordering. A row version derived from an older source log position can
    never overwrite a newer one, regardless of batch arrival order.
G3  Ordered deletes. A delete is another version of a row, so it obeys G2
    rather than sitting outside it.

Plumbing deliberately stubbed: Spark session construction, Kafka security
config, the schema registry lookup.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dataplatform.sqlutil import catalog_of, sql_ident, sql_str

# Debezium op codes. 'r' (snapshot read) is treated exactly like an insert, so
# the snapshot-to-stream cutover needs no special case.
_OP_DELETE = "d"


@dataclass(frozen=True)
class CdcTable:
    """Configuration for one replicated source table."""

    source: str  # e.g. "lending_core"
    topic: str  # e.g. "lending_core.public.loan"
    target: str  # e.g. "lakehouse.silver.loan"
    primary_key: tuple[str, ...]  # e.g. ("loan_id",)
    payload_columns: tuple[str, ...]  # business columns, registry-resolved

    def __post_init__(self) -> None:
        sql_ident(self.target)
        if not self.primary_key:
            raise ValueError(f"{self.target}: a CDC target needs a primary key")

    @property
    def pk_predicate(self) -> str:
        return " AND ".join(f"t.{sql_ident(c)} = s.{sql_ident(c)}" for c in self.primary_key)

    @property
    def applied_batches(self) -> str:
        return f"{catalog_of(self.target)}.platform.cdc_applied_batches"


@dataclass(frozen=True)
class OffsetRange:
    """The exact Kafka coordinates a micro-batch was read from."""

    topic: str
    ranges: tuple[tuple[int, int, int], ...]  # (partition, start_offset, end_offset)

    @property
    def is_empty(self) -> bool:
        return not self.ranges or all(end <= start for _p, start, end in self.ranges)

    def batch_id(self) -> str:
        canonical = json.dumps(
            {"topic": self.topic, "ranges": sorted(self.ranges)},
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:32]


def already_applied(spark: SparkSession, cfg: CdcTable, batch_id: str) -> bool:
    probe = spark.sql(
        f"SELECT 1 FROM {cfg.applied_batches} "
        f"WHERE topic = {sql_str(cfg.topic)} AND batch_id = {sql_str(batch_id)} LIMIT 1"
    )
    return len(probe.take(1)) > 0


def record_applied(spark: SparkSession, cfg: CdcTable, batch_id: str, rows: int) -> None:
    spark.sql(
        f"INSERT INTO {cfg.applied_batches} (topic, batch_id, row_count, applied_at) "
        f"VALUES ({sql_str(cfg.topic)}, {sql_str(batch_id)}, {int(rows)}, current_timestamp())"
    )


def decode_envelope(raw: DataFrame, cfg: CdcTable) -> DataFrame:
    payload = F.from_json(F.col("value").cast("string"), _envelope_schema(cfg))
    ev = raw.select(
        payload.alias("e"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
    )

    # For a delete, Debezium populates `before` and leaves `after` null.
    state = F.when(F.col("e.op") == _OP_DELETE, F.col("e.before")).otherwise(F.col("e.after"))

    return ev.select(
        *[state.getField(c).alias(c) for c in cfg.payload_columns],
        F.col("e.op").alias("_op"),
        (F.col("e.op") == _OP_DELETE).alias("_deleted"),
        F.coalesce(F.col("e.source.lsn"), F.col("e.source.pos")).cast("long").alias("_lsn"),
        F.col("e.source.ts_ms").cast("long").alias("_source_ts_ms"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
    )


def collapse_to_latest(events: DataFrame, cfg: CdcTable) -> DataFrame:
    newest_first = Window.partitionBy(*cfg.primary_key).orderBy(
        F.col("_lsn").desc(), F.col("_kafka_partition").desc(), F.col("_kafka_offset").desc()
    )
    return (
        events.withColumn("_rn", F.row_number().over(newest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _merge_statement(cfg: CdcTable, staged_view: str) -> str:
    return f"""
        MERGE INTO {cfg.target} AS t
        USING {sql_ident(staged_view)} AS s
        ON {cfg.pk_predicate}
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """


def apply_micro_batch(
    batch: DataFrame,
    cfg: CdcTable,
    offsets: OffsetRange,
    mark_applied: Callable[[SparkSession, CdcTable, str, int], None] = record_applied,
) -> bool:
    if offsets.is_empty:
        # An empty range hashes to a constant, so every empty batch would share
        # one marker. Nothing to apply, so return before that can matter.
        return False

    spark = batch.sparkSession
    batch_id = offsets.batch_id()

    if already_applied(spark, cfg, batch_id):
        return False

    staged = (
        collapse_to_latest(decode_envelope(batch, cfg), cfg)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit(cfg.source))
        # Cached because the count below and the MERGE would otherwise each
        # recompute the decode and the collapse window -- two shuffles over
        # ~600k rows per batch at peak, for one number.
        .cache()
    )

    view = f"staged_{cfg.source}_{batch_id[:8]}"
    staged.createOrReplaceTempView(view)
    try:
        staged_rows = staged.count()
        spark.sql(_merge_statement(cfg, view))
    finally:
        spark.catalog.dropTempView(view)
        staged.unpersist()

    mark_applied(spark, cfg, batch_id, staged_rows)
    return True


def current_state_view(spark: SparkSession, cfg: CdcTable, view: str) -> None:
    """The only shape a consumer is allowed to read.

    Soft deletes mean the raw silver table contains tombstones. Every serving
    path -- Trino, the KV-store feed, downstream marts -- reads this view, so
    forgetting the `_deleted` filter is not something an individual query can
    get wrong.
    """
    spark.sql(
        f"CREATE OR REPLACE VIEW {sql_ident(view)} AS "
        f"SELECT * EXCEPT (_deleted) FROM {cfg.target} WHERE _deleted = false"
    )


def run_stream(spark: SparkSession, cfg: CdcTable, checkpoint: str):
    """Wire the applier into Structured Streaming.

    Offsets are owned by the checkpoint -- there is no second offset store and no
    manual commit. The window this module has to survive is therefore the
    framework's own at-least-once retry: a batch whose data commit succeeded but
    whose epoch was not marked complete is re-executed with an identical offset
    range, which is exactly what `apply_micro_batch` is built for.

    The 1-minute trigger is a deliberate choice, not a limitation; see design
    document sections 2 and 8 for what it buys and what it costs the catalog.
    """

    def handler(batch: DataFrame, _epoch: int) -> None:
        apply_micro_batch(batch, cfg, _offsets_of(batch, cfg.topic))

    return (
        spark.readStream.format("kafka")
        .options(**_kafka_options(cfg.topic))  # stub: brokers, SASL, TLS
        .load()
        .writeStream.foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="1 minute")
        .start()
    )


# --- stubbed plumbing -------------------------------------------------------
def _envelope_schema(cfg: CdcTable): ...  # resolved from the schema registry
def _kafka_options(topic: str) -> dict: ...
def _offsets_of(batch: DataFrame, topic: str) -> OffsetRange: ...
