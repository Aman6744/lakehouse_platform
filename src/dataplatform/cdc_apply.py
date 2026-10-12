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
G4  No fabricated values. A column the source did not actually send -- a
    TOASTed Postgres column omitted from the WAL record, for which Debezium
    substitutes a sentinel string -- never overwrites the value already in the
    table, and a column genuinely set to NULL still writes NULL. Enforced by
    the per-column carry-forward in `collapse_to_latest` and the changed-flag
    guard in `_merge_statement`. See design document section 6.1.

Plumbing deliberately stubbed: Spark session construction, Kafka security
config, the schema registry lookup.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dataplatform.sqlutil import catalog_of, sql_ident, sql_str

# Debezium op codes. 'r' (snapshot read) is treated exactly like an insert, so
# the snapshot-to-stream cutover needs no special case.
_OP_DELETE = "d"

# Debezium's sentinel for a TOASTed Postgres column that the triggering
# statement did not modify, and which Postgres therefore omits from the WAL
# record. It is a real string in the payload, not a null, which is what makes it
# dangerous: written verbatim it looks like data. See `collapse_to_latest`.
# https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-toasted-values
TOAST_PLACEHOLDER = "__debezium_unavailable_value"

# Prefix for the per-column "this batch knows a real value for this column"
# flags that `collapse_to_latest` emits and `_merge_statement` consumes. They
# exist only on the staged frame and are never written to the target.
_CHANGED_PREFIX = "_chg__"

# Table properties a CDC target must be created with.
#
# Iceberg defaults every write mode to copy-on-write, which rewrites each data
# file containing a changed row. With a 1-minute trigger against a 50M-row
# table and updates scattered across the key space, that is a large fraction of
# the table rewritten every minute -- it would dominate the platform's compute
# cost and is a stronger candidate for "first thing to break" than the catalog.
# Merge-on-read writes a small delete file instead and defers the rewrite to
# compaction, which is schedulable and can be given its own capacity.
#
# The trade-off is read amplification between compactions, so this pairs with
# the maintenance job rather than standing alone. Gold marts deliberately keep
# the copy-on-write default: written once per partition per day and read
# constantly, the trade-off inverts. See docs/adr/0006.
CDC_TARGET_PROPERTIES: dict[str, str] = {
    "format-version": "2",
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
    # Cluster incoming writes by the merge key so a MERGE touches fewer files.
    "write.distribution-mode": "hash",
}

# Columns the applier maintains itself, as opposed to the source's payload.
_BOOKKEEPING = (
    "_op",
    "_tombstone",
    "_lsn",
    "_source_ts_ms",
    "_kafka_partition",
    "_kafka_offset",
    "_batch_id",
    "_ingest_ts",
    "_source_system",
)


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
    def business_columns(self) -> tuple[str, ...]:
        """Payload columns that are not part of the key.

        These are the only columns that can arrive TOASTed, and therefore the
        only ones needing the changed-flag treatment in the MERGE.
        """
        return tuple(c for c in self.payload_columns if c not in self.primary_key)

    @property
    def target_columns(self) -> tuple[str, ...]:
        """Every column the applier writes, in a fixed order.

        Stated explicitly because the MERGE can no longer use `SET *` / `INSERT *`
        -- the staged frame carries changed-flags that have no place in the
        target. See `_merge_statement`.
        """
        return tuple(self.primary_key) + self.business_columns + _BOOKKEEPING

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
        (F.col("e.op") == _OP_DELETE).alias("_tombstone"),
        F.coalesce(F.col("e.source.lsn"), F.col("e.source.pos")).cast("long").alias("_lsn"),
        F.col("e.source.ts_ms").cast("long").alias("_source_ts_ms"),
        F.col("_kafka_partition"),
        F.col("_kafka_offset"),
    )


def collapse_to_latest(events: DataFrame, cfg: CdcTable) -> DataFrame:
    """Reduce many changes for one key inside a batch down to one row.

    Without this, MERGE raises on multiple source rows matching one target row.

    The reduction is **per column, not per row**, and that distinction is the
    whole point. An UPDATE that does not touch a TOASTed column makes Postgres
    omit it from the WAL record, and Debezium substitutes
    `TOAST_PLACEHOLDER` -- a real string, not a null. So the row with the
    highest `_lsn` for a key is not necessarily the row holding that key's most
    recent *real* value for every column.

    Concretely: `_lsn` 10 sets `address = 'MG Road'`; 11 and 12 both change
    `balance` only and both carry the placeholder for `address`. Picking the
    max-`_lsn` row wholesale takes 12's row -- placeholder and all -- and the
    MERGE writes "__debezium_unavailable_value" into `address` over a real
    value. Nothing errors, nothing pages, and the column is quietly wrong until
    somebody reads it.

    So: treat the placeholder as absent, carry the last non-null value forward
    per column over the key's history in this batch, and take `_op`/`_lsn` and
    the Kafka coordinates from the genuinely newest row. Ordering is ascending
    here because `last(..., ignorenulls=True)` over the default frame
    (unbounded preceding .. current row) is what "most recent real value" means.
    """
    oldest_first = Window.partitionBy(*cfg.primary_key).orderBy(
        F.col("_lsn").asc(), F.col("_kafka_partition").asc(), F.col("_kafka_offset").asc()
    )
    newest_first = Window.partitionBy(*cfg.primary_key).orderBy(
        F.col("_lsn").desc(), F.col("_kafka_partition").desc(), F.col("_kafka_offset").desc()
    )

    carried, flags = [], []
    for col in cfg.business_columns:
        # Wrapping the value in a struct is what keeps a *genuine* null
        # distinguishable from an unchanged TOAST. A placeholder collapses the
        # whole struct to null and is skipped by `ignorenulls`; a real null
        # yields a non-null struct whose field happens to be null, so it is
        # carried like any other value. Comparing with `eqNullSafe` matters --
        # a plain `== placeholder` against a null column is null, not false,
        # and would send genuine nulls down the placeholder branch.
        observed = F.when(F.col(col).eqNullSafe(F.lit(TOAST_PLACEHOLDER)), F.lit(None)).otherwise(
            F.struct(F.col(col).alias("v"))
        )
        latest = F.last(observed, ignorenulls=True).over(oldest_first)

        carried.append(latest.getField("v").alias(col))
        # False when every event for this key in this batch left the column
        # TOASTed, i.e. the batch has no opinion about it and the MERGE must
        # keep whatever the target already holds.
        flags.append(latest.isNotNull().alias(f"{_CHANGED_PREFIX}{col}"))

    return (
        events.select(
            *cfg.primary_key,
            *carried,
            *flags,
            *(c for c in _BOOKKEEPING if c in events.columns),
        )
        .withColumn("_rn", F.row_number().over(newest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def tblproperties_clause(properties: dict[str, str] = CDC_TARGET_PROPERTIES) -> str:
    """Render table properties as a `TBLPROPERTIES (...)` clause.

    Exists so the required merge-on-read settings are a value this module owns
    and tests assert against, rather than prose in a design document that a
    hand-written `CREATE TABLE` can silently disagree with.
    """
    inner = ", ".join(f"{sql_str(k)} = {sql_str(v)}" for k, v in sorted(properties.items()))
    return f"TBLPROPERTIES ({inner})"


def assert_target_is_merge_on_read(spark: SparkSession, cfg: CdcTable) -> None:
    """Fail loudly if a CDC target is not configured for merge-on-read.

    Intended as a startup check on the streaming job. A target that silently
    reverted to copy-on-write -- recreated by hand, restored from a backup,
    cloned from a template -- still produces correct data, just at many times
    the write cost, which makes it exactly the kind of regression nobody
    notices until the bill arrives.
    """
    rows = spark.sql(f"SHOW TBLPROPERTIES {cfg.target}").collect()
    actual = {r["key"]: r["value"] for r in rows}
    wrong = {
        k: (v, actual.get(k))
        for k, v in CDC_TARGET_PROPERTIES.items()
        if k.startswith("write.") and actual.get(k) != v
    }
    if wrong:
        detail = ", ".join(
            f"{k}: expected {exp!r}, found {got!r}" for k, (exp, got) in wrong.items()
        )
        raise ValueError(f"{cfg.target} is not configured for merge-on-read -- {detail}")


def _merge_statement(cfg: CdcTable, staged_view: str) -> str:
    """The MERGE that enforces G1, G2, G3 and G4.

    `s._lsn > t._lsn` on the MATCHED clause is the ordering and the idempotency
    guarantee. Drop it and a replayed or delayed batch silently reverts rows to
    older values -- the failure that is almost impossible to spot in production,
    because the table stays queryable and merely tells the wrong story.
    `>` and not `>=`: re-applying an identical version is a no-op, so skipping
    it avoids rewriting the file it lives in.

    The SET and INSERT lists are explicit rather than `SET *` / `INSERT *`, for
    two reasons. The staged frame carries changed-flags that must not be written
    to the target; and each business column has to be guarded by its flag, so a
    column this batch only ever saw TOASTed keeps the value the target already
    holds instead of being overwritten (G4). `SET *` cannot express that, which
    is precisely how the placeholder reaches the table if you let it.
    """
    guarded = ", ".join(
        f"{sql_ident(c)} = CASE WHEN s.{sql_ident(_CHANGED_PREFIX + c)} "
        f"THEN s.{sql_ident(c)} ELSE t.{sql_ident(c)} END"
        for c in cfg.business_columns
    )
    bookkeeping = ", ".join(f"{sql_ident(c)} = s.{sql_ident(c)}" for c in _BOOKKEEPING)
    set_clause = ", ".join(part for part in (guarded, bookkeeping) if part)

    insert_cols = ", ".join(sql_ident(c) for c in cfg.target_columns)
    insert_vals = ", ".join(f"s.{sql_ident(c)}" for c in cfg.target_columns)

    return f"""
        MERGE INTO {cfg.target} AS t
        USING {sql_ident(staged_view)} AS s
        ON {cfg.pk_predicate}
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
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
    forgetting the `_tombstone` filter is not something an individual query can
    get wrong.
    """
    spark.sql(
        f"CREATE OR REPLACE VIEW {sql_ident(view)} AS "
        f"SELECT * EXCEPT (_tombstone) FROM {cfg.target} WHERE _tombstone = false"
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
