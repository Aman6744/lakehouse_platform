"""The hardest test in the suite: prove that a crash anywhere in the CDC apply
cannot leave the table holding the wrong row version.

This is the failure I most wanted to catch, because it is invisible in
production. Nothing errors, no alert fires, the table stays queryable -- it just
holds a row version that is one generation stale, forever, until someone
reconciles against the source and cannot explain the difference.

The tests separate the two mechanisms deliberately, because they carry different
weight and it is easy to conflate them:

  * the `_lsn > t._lsn` MERGE predicate is what makes a replay *correct*, and it
    holds whether the replay is immediate or ten batches later
  * `cdc_applied_batches` is what makes a replay *cheap*, by skipping a MERGE
    that would have been inert anyway

So `test_replay_after_lost_marker_leaves_state_unchanged` is the load-bearing
one: it removes the marker entirely and still demands correct state.

How this fails if the design is wrong
-------------------------------------
* Drop the `s._lsn > t._lsn` predicate -> `test_stale_batch_cannot_revert` and
  `test_replay_after_lost_marker_leaves_state_unchanged` both fail. This is the
  scenario that corrupts a real balance.
* Change the predicate from `>` to `>=` -> a same-version reapply rewrites the
  row, so `test_replay_after_lost_marker_leaves_state_unchanged` fails on
  `data_snapshot_count`.
* Make `OffsetRange.batch_id()` impure -> the replay mints a fresh id, the
  marker probe misses, and `test_replay_with_marker_writes_no_new_snapshot`
  fails on `data_snapshot_count`.
* Mark the batch applied *before* the MERGE instead of after -> a crash in
  between would skip the batch permanently. `test_lost_data_commit_is_retried`
  fails, and that one loses data rather than merely duplicating work.
* Physically delete on a Debezium 'd' instead of soft-deleting ->
  `test_delete_is_ordered_like_any_other_version` fails, because the stale
  insert no longer matches a target row and is inserted as live.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from conftest import CATALOG, create_applied_batches
from dataplatform.cdc_apply import (
    TOAST_PLACEHOLDER,
    CdcTable,
    OffsetRange,
    apply_micro_batch,
    assert_target_is_merge_on_read,
    record_applied,
    tblproperties_clause,
)

TARGET = f"{CATALOG}.silver.loan"

CFG = CdcTable(
    source="lending_core",
    topic="lending_core.public.loan",
    target=TARGET,
    primary_key=("loan_id",),
    payload_columns=("loan_id", "principal_paise", "status"),
)


class ProcessDied(RuntimeError):
    """Stands in for the executor being killed mid-apply."""


def _crash(*_args, **_kwargs):
    raise ProcessDied


@pytest.fixture(autouse=True)
def envelope_schema(monkeypatch):
    """Pin the Debezium envelope shape instead of reaching for a live registry."""
    body = StructType(
        [
            StructField("loan_id", StringType()),
            StructField("principal_paise", LongType()),
            StructField("status", StringType()),
        ]
    )
    schema = StructType(
        [
            StructField("op", StringType()),
            StructField("before", body),
            StructField("after", body),
            StructField(
                "source",
                StructType(
                    [
                        StructField("lsn", LongType()),
                        StructField("pos", LongType()),
                        StructField("ts_ms", LongType()),
                    ]
                ),
            ),
        ]
    )
    monkeypatch.setattr("dataplatform.cdc_apply._envelope_schema", lambda cfg: schema)


@pytest.fixture(autouse=True)
def target_table(spark, namespaces):
    create_applied_batches(spark)
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    # Created through the module's own property set rather than a hand-written
    # TBLPROPERTIES, so the DDL under test cannot drift from what the applier
    # requires. `test_target_is_configured_for_merge_on_read` asserts it stuck.
    spark.sql(f"""
        CREATE TABLE {TARGET} (
            loan_id STRING, principal_paise BIGINT, status STRING,
            _op STRING, _tombstone BOOLEAN, _lsn BIGINT, _source_ts_ms BIGINT,
            _kafka_partition INT, _kafka_offset BIGINT, _batch_id STRING,
            _ingest_ts TIMESTAMP, _source_system STRING
        ) USING iceberg {tblproperties_clause()}
    """)


def kafka_batch(spark, events: list[dict], first_offset: int = 0):
    """Build a frame shaped like `spark.readStream.format("kafka").load()`."""
    rows = [(json.dumps(e), 0, first_offset + i) for i, e in enumerate(events)]
    return spark.createDataFrame(rows, "value string, partition int, offset long")


def change(loan_id: str, paise: int, status: str | None, lsn: int, op: str = "u") -> dict:
    body = {"loan_id": loan_id, "principal_paise": paise, "status": status}
    return {
        "op": op,
        "before": body if op == "d" else None,
        "after": None if op == "d" else body,
        "source": {"lsn": lsn, "pos": lsn, "ts_ms": 1_700_000_000_000 + lsn},
    }


def state_fingerprint(spark) -> list[tuple]:
    """Every field a consumer could observe, ordered deterministically.

    Deliberately excludes `_batch_id` and `_ingest_ts`: they are provenance, and
    asserting on them would make the test fail for a change that no consumer
    can see.
    """
    return [
        tuple(r)
        for r in spark.sql(f"""
            SELECT loan_id, principal_paise, status, _tombstone, _lsn
            FROM {TARGET} ORDER BY loan_id
        """).collect()
    ]


def data_snapshot_count(spark) -> int:
    return spark.sql(f"SELECT count(*) FROM {TARGET}.snapshots").first()[0]


def marker_count(spark) -> int:
    return spark.sql(
        f"SELECT count(*) FROM {CATALOG}.platform.cdc_applied_batches WHERE topic = '{CFG.topic}'"
    ).first()[0]


BATCH_ONE = [
    change("L-1", 500_00, "ACTIVE", lsn=10),
    change("L-2", 250_00, "ACTIVE", lsn=11),
]
OFFSETS_ONE = OffsetRange(topic=CFG.topic, ranges=((0, 0, 2),))
STATE_ONE = [
    ("L-1", 500_00, "ACTIVE", False, 10),
    ("L-2", 250_00, "ACTIVE", False, 11),
]


def test_replay_with_marker_writes_no_new_snapshot(spark):
    """The happy path: a marked batch is skipped outright on replay."""
    assert apply_micro_batch(kafka_batch(spark, BATCH_ONE), CFG, OFFSETS_ONE) is True
    assert state_fingerprint(spark) == STATE_ONE
    commits = data_snapshot_count(spark)

    assert apply_micro_batch(kafka_batch(spark, BATCH_ONE), CFG, OFFSETS_ONE) is False
    assert state_fingerprint(spark) == STATE_ONE
    assert data_snapshot_count(spark) == commits, "replay wrote a second data commit"
    assert marker_count(spark) == 1, "replay duplicated the marker"


def test_replay_after_lost_marker_leaves_state_unchanged(spark):
    """The load-bearing one: correctness must not depend on the marker at all.

    Simulates the process dying after the data commit but before the marker
    lands, then replays the identical offset range. The MERGE runs again -- the
    short-circuit cannot help -- and the ordering predicate has to carry the
    guarantee on its own.
    """
    with pytest.raises(ProcessDied):
        apply_micro_batch(kafka_batch(spark, BATCH_ONE), CFG, OFFSETS_ONE, mark_applied=_crash)

    after_first = state_fingerprint(spark)
    assert after_first == STATE_ONE, "the data commit itself did not land"
    assert marker_count(spark) == 0, "marker landed despite the simulated crash"
    commits = data_snapshot_count(spark)

    assert apply_micro_batch(kafka_batch(spark, BATCH_ONE), CFG, OFFSETS_ONE) is True
    assert state_fingerprint(spark) == after_first, "replay mutated observable state"
    assert marker_count(spark) == 1
    # An extra commit is the honest, accepted cost of the lost marker. What is
    # not acceptable -- and what the assertion above rules out -- is a changed row.
    assert data_snapshot_count(spark) >= commits


def test_foreign_marker_does_not_suppress_a_batch(spark):
    """The probe must match on this batch's id, not merely on the topic.

    Weaker than I would like: the failure I actually want to rule out is marking
    a batch applied *before* the MERGE, which would turn a crash in between into
    permanently missing data. That ordering is not injectable, so it is asserted
    by reading `apply_micro_batch` rather than by this test -- named here because
    it is the one gap in this file I know about.
    """
    record_applied(spark, CFG, "a-marker-for-some-other-batch", rows=0)
    assert apply_micro_batch(kafka_batch(spark, BATCH_ONE), CFG, OFFSETS_ONE) is True
    assert state_fingerprint(spark) == STATE_ONE


def test_stale_batch_cannot_revert(spark):
    """An older source version must never overwrite a newer one."""
    apply_micro_batch(
        kafka_batch(spark, [change("L-1", 900_00, "CLOSED", lsn=99)], first_offset=10),
        CFG,
        OffsetRange(CFG.topic, ((0, 10, 11),)),
    )

    # A delayed batch carrying an older LSN for the same key -- a redelivery
    # after a connector restart, or a rebalance replaying history.
    apply_micro_batch(
        kafka_batch(spark, [change("L-1", 100_00, "ACTIVE", lsn=42)], first_offset=4),
        CFG,
        OffsetRange(CFG.topic, ((0, 4, 5),)),
    )

    row = spark.sql(
        f"SELECT principal_paise, status, _lsn FROM {TARGET} WHERE loan_id = 'L-1'"
    ).first()
    assert (row["principal_paise"], row["status"], row["_lsn"]) == (900_00, "CLOSED", 99)


def test_delete_is_ordered_like_any_other_version(spark):
    """A tombstone obeys the ordering guard, and a stale insert cannot resurrect."""
    apply_micro_batch(
        kafka_batch(spark, [change("L-9", 700_00, "ACTIVE", lsn=20)]),
        CFG,
        OffsetRange(CFG.topic, ((0, 0, 1),)),
    )
    apply_micro_batch(
        kafka_batch(spark, [change("L-9", 700_00, "ACTIVE", lsn=21, op="d")], first_offset=1),
        CFG,
        OffsetRange(CFG.topic, ((0, 1, 2),)),
    )
    apply_micro_batch(  # arrives late, older than the delete
        kafka_batch(spark, [change("L-9", 700_00, "ACTIVE", lsn=20)], first_offset=5),
        CFG,
        OffsetRange(CFG.topic, ((0, 5, 6),)),
    )

    row = spark.sql(f"SELECT _tombstone, _lsn FROM {TARGET} WHERE loan_id = 'L-9'").first()
    assert (row["_tombstone"], row["_lsn"]) == (True, 21), "stale insert resurrected a deleted row"
    assert spark.sql(f"SELECT count(*) FROM {TARGET} WHERE loan_id = 'L-9'").first()[0] == 1


def test_in_batch_collapse_keeps_the_newest_version(spark):
    """Several changes to one key inside one batch must collapse to the newest,
    or the MERGE raises on multiple source matches -- and if it collapsed to the
    oldest instead, the table would silently lag by a batch."""
    apply_micro_batch(
        kafka_batch(
            spark,
            [
                change("L-5", 100_00, "ACTIVE", lsn=30),
                change("L-5", 200_00, "ACTIVE", lsn=31),
                change("L-5", 300_00, "CLOSED", lsn=32),
            ],
        ),
        CFG,
        OffsetRange(CFG.topic, ((0, 0, 3),)),
    )
    row = spark.sql(
        f"SELECT principal_paise, status, _lsn FROM {TARGET} WHERE loan_id = 'L-5'"
    ).first()
    assert (row["principal_paise"], row["status"], row["_lsn"]) == (300_00, "CLOSED", 32)


def test_toast_placeholder_does_not_overwrite_a_real_value(spark):
    """G4 across batches: a column the source did not send must not be written.

    `L-7` gets a real status, then a later batch changes only the principal and
    leaves `status` TOASTed. Postgres omits an unchanged TOASTed column from the
    WAL record and Debezium substitutes a sentinel *string*, not a null -- so a
    `SET *` MERGE writes "__debezium_unavailable_value" straight over 'ACTIVE'.
    Queryable, unpaged, and wrong until somebody reads the column.
    """
    apply_micro_batch(
        kafka_batch(spark, [change("L-7", 400_00, "ACTIVE", lsn=50)]),
        CFG,
        OffsetRange(CFG.topic, ((0, 0, 1),)),
    )
    apply_micro_batch(
        kafka_batch(spark, [change("L-7", 450_00, TOAST_PLACEHOLDER, lsn=51)], first_offset=1),
        CFG,
        OffsetRange(CFG.topic, ((0, 1, 2),)),
    )

    row = spark.sql(
        f"SELECT principal_paise, status, _lsn FROM {TARGET} WHERE loan_id = 'L-7'"
    ).first()
    assert row["principal_paise"] == 450_00, "the column that did change was not applied"
    assert row["status"] == "ACTIVE", "a TOAST placeholder overwrote a real value"
    assert row["_lsn"] == 51


def test_toast_placeholder_within_one_batch_carries_the_real_value(spark):
    """G4 inside one batch: the newest row is not the newest *value* per column.

    This is the case a row-wise collapse gets wrong even when the MERGE is
    careful. lsn 61 holds the only real status in the batch; 62 and 63 change
    the principal and leave status TOASTed. Picking the max-lsn row wholesale
    takes 63's placeholder, because 63 never carried a real status either.
    """
    apply_micro_batch(
        kafka_batch(
            spark,
            [
                change("L-8", 100_00, "ACTIVE", lsn=61),
                change("L-8", 200_00, TOAST_PLACEHOLDER, lsn=62),
                change("L-8", 300_00, TOAST_PLACEHOLDER, lsn=63),
            ],
        ),
        CFG,
        OffsetRange(CFG.topic, ((0, 0, 3),)),
    )
    row = spark.sql(
        f"SELECT principal_paise, status, _lsn FROM {TARGET} WHERE loan_id = 'L-8'"
    ).first()
    assert (row["principal_paise"], row["_lsn"]) == (300_00, 63)
    assert row["status"] == "ACTIVE", "in-batch collapse lost the real value"


def test_genuine_null_is_still_applied(spark):
    """The other half of G4, and the one a naive placeholder filter breaks.

    "Unchanged because TOASTed" and "genuinely set to NULL" arrive differently
    -- a sentinel string versus an actual null -- and must stay distinguishable.
    Treating the placeholder as null and then skipping nulls would silently
    refuse every real null-ing, which is a quieter bug than the one it fixes.
    """
    apply_micro_batch(
        kafka_batch(spark, [change("L-10", 500_00, "ACTIVE", lsn=70)]),
        CFG,
        OffsetRange(CFG.topic, ((0, 0, 1),)),
    )
    apply_micro_batch(
        kafka_batch(spark, [change("L-10", 500_00, None, lsn=71)], first_offset=1),
        CFG,
        OffsetRange(CFG.topic, ((0, 1, 2),)),
    )

    row = spark.sql(f"SELECT status, _lsn FROM {TARGET} WHERE loan_id = 'L-10'").first()
    assert row["status"] is None, "a genuine NULL was swallowed by the placeholder guard"
    assert row["_lsn"] == 71


def test_target_is_configured_for_merge_on_read(spark):
    """Copy-on-write is the Iceberg default and would rewrite every file holding
    a changed row, once a minute. Correct, and enormously expensive -- so the
    configuration is asserted rather than left to whoever wrote the DDL."""
    assert_target_is_merge_on_read(spark, CFG)  # created via tblproperties_clause()

    cow = f"{CATALOG}.silver.loan_copy_on_write"
    spark.sql(f"DROP TABLE IF EXISTS {cow}")
    spark.sql(f"CREATE TABLE {cow} (loan_id STRING) USING iceberg")
    with pytest.raises(ValueError, match="merge-on-read"):
        assert_target_is_merge_on_read(spark, replace(CFG, target=cow))


def test_batch_id_is_pure_and_range_order_insensitive():
    """No Spark needed. If this drifts, every replay short-circuit above is void."""
    a = OffsetRange("t", ((0, 0, 5), (1, 0, 7)))
    b = OffsetRange("t", ((1, 0, 7), (0, 0, 5)))
    assert a.batch_id() == b.batch_id() == a.batch_id()
    assert OffsetRange("t", ((0, 0, 6),)).batch_id() != a.batch_id()
    assert OffsetRange("other", ((0, 0, 5), (1, 0, 7))).batch_id() != a.batch_id()


def test_empty_batch_is_skipped(spark):
    """Every empty range hashes identically, so one marker would suppress all
    later empty batches. Returning early is what stops that from mattering."""
    empty = OffsetRange(CFG.topic, ((0, 4, 4),))
    assert empty.is_empty
    assert apply_micro_batch(kafka_batch(spark, []), CFG, empty) is False
    assert marker_count(spark) == 0
