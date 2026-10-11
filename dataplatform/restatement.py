"""Hard problem 3 of 3 -- absorbing corrections that arrive after a number has
already been published, without lying about what we used to believe.

Guarantees this module owns
--------------------------
G7  A file delivered twice is ingested once. A file redelivered with different
    content is treated as a correction, never as an append.
G8  A restated partition is a new version of our knowledge, not an edit of
    history: "as we reported it on 31 March" stays answerable forever.
G9  Every dataset derived from a restated partition is marked stale, so a
    correction cannot stop halfway down the graph -- and it is marked only once
    the correction has actually published.

The realisation that made this tractable: Iceberg snapshots already *are* a
knowledge-time axis, so I do not need to build bitemporal versioning -- I need to
stop throwing it away and name the points that matter. Ordinary snapshots expire
after 7 days; a tag is cut at every finance close and every restatement and
retained for five years. "As reported on 31 March" becomes
`VERSION AS OF 'close_2026_03_31'`, a normal SQL read an auditor can run
themselves, and the same mechanism gives data scientists leakage-free training
sets for free.

Two costs, both real, both discussed in design document section 6.3: tags pin
data files, so superseded partition versions stay on disk for five years; and
that pinning is in direct tension with a DPDP erasure request, since you cannot
delete rows a tag is holding. The answer there is to destroy a per-subject
encryption key rather than the rows, and it is the constraint most likely to
force a redesign of this module.

Plumbing deliberately stubbed: object-store listing, checksum streaming, the
downstream scheduler that consumes invalidation rows, the lineage graph.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from enum import Enum

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from dataplatform.publish_gate import ArrivalContract, Decision, publish_partition
from dataplatform.sqlutil import catalog_of, sql_ident, sql_str

_CLAIM_ATTEMPTS = 5


class Disposition(str, Enum):
    NEW = "NEW"  # first sight of this business date
    DUPLICATE = "DUPLICATE"  # byte-identical redelivery; ignore
    RESTATEMENT = "RESTATEMENT"  # same date, different content; correct it


@dataclass(frozen=True)
class Delivery:
    source: str
    business_date: date
    file_hash: str  # sha256 of the file body
    uri: str
    row_count: int


@dataclass(frozen=True)
class Claim:
    """The outcome of trying to take ownership of a delivery."""

    knowledge_version: int
    owned_by_us: bool  # False if another worker won the race


def ledger(table: str) -> str:
    return f"{catalog_of(table)}.platform.ingest_ledger"


def invalidations(table: str) -> str:
    return f"{catalog_of(table)}.platform.invalidations"


def classify(spark: SparkSession, d: Delivery, table: str) -> Disposition:
    """Decide what a delivery means before touching any data.

    Keying on content hash rather than filename is the whole trick. Partners
    rename, re-zip and re-send constantly; the only thing that reliably says
    "you have seen this exact content before" is the bytes. Conversely a new
    hash for a date already on record is unambiguously a correction, and
    treating it as an append -- the default behaviour of every naive file loader
    -- silently doubles the day's settlement totals.

    Deliberately looks at CLAIMED rows as well as COMPLETE ones. Filtering to
    COMPLETE would classify a redelivery that arrives while the first load is
    still in flight as NEW, and load the same file twice.
    """
    prior = spark.sql(
        f"""
        SELECT file_hash
        FROM {ledger(table)}
        WHERE source = {sql_str(d.source)}
          AND business_date = DATE {sql_str(d.business_date)}
          AND status IN ('CLAIMED', 'COMPLETE')
        """
    ).collect()

    if not prior:
        return Disposition.NEW
    if any(r["file_hash"] == d.file_hash for r in prior):
        return Disposition.DUPLICATE
    return Disposition.RESTATEMENT


def next_version(spark: SparkSession, d: Delivery, table: str) -> int:
    row = spark.sql(
        f"""
        SELECT coalesce(max(knowledge_version), 0) + 1 AS v
        FROM {ledger(table)}
        WHERE source = {sql_str(d.source)}
          AND business_date = DATE {sql_str(d.business_date)}
        """
    ).first()
    return int(row["v"])


def claim(spark: SparkSession, d: Delivery, table: str, worker_id: str) -> Claim:
    """Take an exclusive claim on this delivery and return its knowledge version.

    Two workers can see the same drop. Iceberg gives us *conflict detection*,
    not automatic re-execution: a losing MERGE raises rather than transparently
    re-planning against the winner's snapshot, so the retry loop here is not
    optional decoration -- it is the part that makes the exclusion work. On
    retry the row exists, `WHEN NOT MATCHED` no longer matches, and the second
    worker's MERGE becomes a genuine no-op.

    We then read the claim back and compare `worker_id`, because "my MERGE did
    not raise" is not the same statement as "I own this claim". Only the worker
    that reads its own id back proceeds to load; the other returns
    `owned_by_us=False` and does nothing. Leaning on the table format this way
    avoids a lock service that could fail independently of the data.
    """
    last_error: Exception | None = None
    for _attempt in range(_CLAIM_ATTEMPTS):
        version = next_version(spark, d, table)
        try:
            spark.sql(
                f"""
                MERGE INTO {ledger(table)} AS t
                USING (SELECT {sql_str(d.source)} AS source,
                              DATE {sql_str(d.business_date)} AS business_date,
                              {sql_str(d.file_hash)} AS file_hash,
                              {int(version)} AS knowledge_version,
                              {sql_str(d.uri)} AS uri,
                              {int(d.row_count)} AS row_count,
                              {sql_str(worker_id)} AS worker_id) AS s
                ON t.source = s.source AND t.business_date = s.business_date
                   AND t.file_hash = s.file_hash
                WHEN NOT MATCHED THEN INSERT (source, business_date, file_hash,
                     knowledge_version, uri, row_count, worker_id, status, claimed_at)
                VALUES (s.source, s.business_date, s.file_hash, s.knowledge_version,
                        s.uri, s.row_count, s.worker_id, 'CLAIMED', current_timestamp())
                """
            )
        except Py4JJavaError as exc:  # ValidationException / CommitFailedException
            last_error = exc
            continue

        owner = spark.sql(
            f"""
            SELECT worker_id, knowledge_version
            FROM {ledger(table)}
            WHERE source = {sql_str(d.source)}
              AND business_date = DATE {sql_str(d.business_date)}
              AND file_hash = {sql_str(d.file_hash)}
            """
        ).first()
        if owner is None:
            continue
        return Claim(
            knowledge_version=int(owner["knowledge_version"]),
            owned_by_us=owner["worker_id"] == worker_id,
        )

    raise RuntimeError(
        f"could not claim {d.source}/{d.business_date} after {_CLAIM_ATTEMPTS} attempts"
    ) from last_error


def write_partition(
    spark: SparkSession,
    d: Delivery,
    table: str,
    version: int,
    branch: str,
) -> None:
    """Replace one partition's contents and record it as a new knowledge version.

    `DataFrameWriterV2.overwrite(condition)` is one Iceberg commit that detaches
    the old files as it attaches the new ones, so no reader sees the partition
    empty or doubled, and it does not require the partition spec to line up with
    the predicate. Rejected: `INSERT INTO ... REPLACE WHERE`, which reads
    naturally and is Delta Lake syntax Iceberg does not have; and `DELETE` then
    `INSERT`, which is two commits and so has exactly the visible intermediate
    state this must avoid.

    Writes land on the publish-gate candidate branch, so a correction that fails
    its own P0 checks is held like any other candidate.
    """
    corrected = (
        _read_and_conform(spark, d)
        .withColumn("_knowledge_version", F.lit(int(version)))
        .withColumn("_source_file_hash", F.lit(d.file_hash))
    )
    previous_branch = spark.conf.get("spark.wap.branch", None)
    spark.conf.set("spark.wap.branch", sql_ident(branch))
    try:
        corrected.writeTo(table).overwrite(
            F.col("business_date") == F.lit(d.business_date)
        )
    finally:
        if previous_branch is None:
            spark.conf.unset("spark.wap.branch")
        else:
            spark.conf.set("spark.wap.branch", previous_branch)


def ingest_delivery(
    spark: SparkSession, d: Delivery, table: str, branch: str
) -> Disposition:
    """The whole path for one file drop. Idempotent by construction.

    Returns the disposition actually acted on. A DUPLICATE, or a claim another
    worker owns, is a no-op that touches no data.
    """
    disposition = classify(spark, d, table)
    if disposition is Disposition.DUPLICATE:
        return disposition

    taken = claim(spark, d, table, worker_id=str(uuid.uuid4()))
    if not taken.owned_by_us:
        return Disposition.DUPLICATE

    write_partition(spark, d, table, taken.knowledge_version, branch)
    _mark_complete(spark, d, table)
    return disposition


def publish_restatement(
    spark: SparkSession,
    contract: ArrivalContract,
    d: Delivery,
    version: int,
) -> Row:
    """Gate the correction, and only then tag it and propagate it.

    This is the composition G8 and G9 actually depend on, and it is the reason
    `invalidate_downstream` is not called from `write_partition`: both the tag
    and the invalidation are claims about published state, so neither may fire
    until the gate has said yes. A held correction leaves downstream marts
    reading the previous version, which is correct -- they are stale, not wrong.
    """
    row = publish_partition(spark, contract, d.business_date)
    if row["status"] not in (Decision.PUBLISHED.value, Decision.OVERRIDDEN.value):
        return row

    tag_close(
        spark, contract.dataset,
        f"restatement_{d.business_date:%Y%m%d}_v{version}",
    )
    invalidate_downstream(spark, contract.dataset, d.business_date, version)
    return row


def tag_close(spark: SparkSession, table: str, label: str, retain_days: int = 5 * 365) -> None:
    """Freeze a named, long-lived reference to the current state.

    Called at every finance close and after every restatement *publishes*. This
    is the line that turns Iceberg's incidental version history into a
    deliberate, queryable audit record.

    DDL, not a procedure: Iceberg has `ALTER TABLE ... CREATE TAG`, and no
    `system.create_tag`. `RETAIN` carries an explicit unit, so the tag outlives
    ordinary snapshot expiry by construction rather than by a comment.
    """
    spark.sql(
        f"ALTER TABLE {sql_ident(table)} CREATE OR REPLACE TAG `{sql_ident(label)}` "
        f"RETAIN {int(retain_days)} DAYS"
    )


def invalidate_downstream(
    spark: SparkSession, table: str, business_date: date, version: int
) -> None:
    """G9. Mark every dependent dataset stale for this business date.

    Called by the publish path *after* the correction has published, never from
    `write_partition`: emitting at write time tells marts to rebuild from a
    partition the gate might still hold, so the rebuild picks up the old or new
    numbers depending on timing.

    Rows rather than a direct scheduler call, so the propagation is itself
    auditable -- if a mart is wrong, there is a record of whether it was ever
    told to rebuild. Edges come from the lineage graph derived from the SQL; a
    hand-maintained dependency list is exactly what goes stale and breaks G9
    without anyone noticing.
    """
    dependents = _lineage_dependents(spark, table)
    if not dependents:
        return
    values = ", ".join(
        f"({sql_str(dep)}, DATE {sql_str(business_date)}, {int(version)}, "
        f"'STALE', current_timestamp())"
        for dep in dependents
    )
    spark.sql(
        f"INSERT INTO {invalidations(table)} (dataset, business_date, knowledge_version, "
        f"status, raised_at) VALUES {values}"
    )


# --- stubbed plumbing -------------------------------------------------------
def _read_and_conform(spark: SparkSession, d: Delivery) -> DataFrame: ...
def _mark_complete(spark: SparkSession, d: Delivery, table: str) -> None: ...
def _lineage_dependents(spark: SparkSession, table: str) -> list[str]: ...
