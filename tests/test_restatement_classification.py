"""Hard problem 3's tests: prove that a redelivered file is never appended, and
that a correction is never silently absorbed as one.

Previously this problem had no test file at all, on the argument that the two
others were harder. That was the wrong thing to skip. `classify` is the cheapest
possible thing to test -- a pure ledger read, no branches, no Spark streaming --
and it is the single decision that stands between a re-sent settlement file and
a doubled day of revenue. The genuinely hard property here (that a reader never
observes the partition empty or doubled mid-overwrite) is still not tested, and
is still named in the honesty section.

How this fails if the design is wrong
-------------------------------------
* Key the ledger on filename instead of content hash ->
  `test_renamed_but_identical_file_is_a_duplicate` fails, and in production the
  day is counted twice.
* Filter the ledger lookup to `status = 'COMPLETE'` ->
  `test_redelivery_while_first_load_is_in_flight_is_a_duplicate` fails. This is
  the bug the earlier version of `classify` actually had: a redelivery arriving
  while the first load was still CLAIMED looked brand new.
* Treat a new hash for a known date as an append instead of a restatement ->
  `test_new_content_for_a_known_date_is_a_restatement` fails.
* Trust "my MERGE did not raise" as proof of ownership ->
  `test_second_worker_does_not_own_the_claim` fails, and two workers load the
  same file concurrently.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from conftest import CATALOG, create_ingest_ledger
from dataplatform.restatement import (
    Delivery,
    Disposition,
    claim,
    classify,
    next_version,
    tag_close,
)

D = date(2026, 4, 3)
TABLE = f"{CATALOG}.silver.settlement"
HASH_A, HASH_B, HASH_C = "a" * 64, "b" * 64, "c" * 64

FIRST = Delivery(
    source="partner_acme",
    business_date=D,
    file_hash=HASH_A,
    uri="s3://drops/acme/2026-04-03/settlement.csv.gz",
    row_count=1_200_000,
)


def redelivery(**overrides) -> Delivery:
    return replace(FIRST, **overrides)


@pytest.fixture(autouse=True)
def ledger_table(spark, namespaces):
    create_ingest_ledger(spark)
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"""
        CREATE TABLE {TABLE} (
            business_date DATE, txn_id STRING, amount_paise BIGINT,
            _knowledge_version INT, _source_file_hash STRING
        ) USING iceberg
        PARTITIONED BY (days(business_date))
        TBLPROPERTIES ('write.wap.enabled' = 'true')
    """)


def test_first_sight_of_a_business_date_is_new(spark):
    assert classify(spark, FIRST, TABLE) is Disposition.NEW


def test_renamed_but_identical_file_is_a_duplicate(spark):
    """Partners rename and re-zip constantly. Only the bytes are evidence."""
    claim(spark, FIRST, TABLE, worker_id="w1")
    same_bytes_new_name = redelivery(
        uri="s3://drops/acme/resent/SETTLEMENT_FINAL_v2.csv.gz"
    )
    assert classify(spark, same_bytes_new_name, TABLE) is Disposition.DUPLICATE


def test_redelivery_while_first_load_is_in_flight_is_a_duplicate(spark):
    """The claim is CLAIMED, not yet COMPLETE, and must still suppress a retry.

    A lookup restricted to COMPLETE rows returns nothing here and the delivery
    reads as NEW -- so the same file loads twice, concurrently, into the same
    partition.
    """
    claim(spark, FIRST, TABLE, worker_id="w1")
    assert spark.sql(
        f"SELECT status FROM {CATALOG}.platform.ingest_ledger"
    ).first()["status"] == "CLAIMED"
    assert classify(spark, FIRST, TABLE) is Disposition.DUPLICATE


def test_new_content_for_a_known_date_is_a_restatement(spark):
    """Same date, different bytes: a correction, never an append."""
    claim(spark, FIRST, TABLE, worker_id="w1")
    corrected = redelivery(file_hash=HASH_B, row_count=1_200_004)
    assert classify(spark, corrected, TABLE) is Disposition.RESTATEMENT


def test_restatement_takes_the_next_knowledge_version(spark):
    claim(spark, FIRST, TABLE, worker_id="w1")
    corrected = redelivery(file_hash=HASH_B)
    assert next_version(spark, corrected, TABLE) == 2

    taken = claim(spark, corrected, TABLE, worker_id="w2")
    assert (taken.knowledge_version, taken.owned_by_us) == (2, True)
    # A third correction keeps counting, rather than resetting or colliding.
    assert next_version(spark, redelivery(file_hash=HASH_C), TABLE) == 3


def test_second_worker_does_not_own_the_claim(spark):
    """Two workers see the same drop; exactly one is allowed to load it.

    The losing worker's MERGE is a legitimate no-op -- `WHEN NOT MATCHED` no
    longer matches -- so it does not raise, and "did not raise" is therefore
    worthless as proof. Ownership is established by reading the `worker_id` back.
    """
    first = claim(spark, FIRST, TABLE, worker_id="w1")
    second = claim(spark, FIRST, TABLE, worker_id="w2")

    assert first.owned_by_us is True
    assert second.owned_by_us is False
    assert second.knowledge_version == first.knowledge_version
    assert spark.sql(
        f"SELECT count(*) FROM {CATALOG}.platform.ingest_ledger "
        f"WHERE source = '{FIRST.source}' AND business_date = DATE '{D}'"
    ).first()[0] == 1, "the same delivery was claimed twice"


def test_tag_survives_ordinary_snapshot_expiry(spark):
    """G8's mechanism, end to end: a tag is a five-year named reference.

    Cutting the tag, then expiring every ordinary snapshot, then reading the tag
    is the whole audit story -- if `RETAIN` were missing or the wrong unit, the
    expiry below would drop the snapshot the tag points at and the final read
    would fail rather than returning what we believed in March.
    """
    spark.sql(
        f"INSERT INTO {TABLE} VALUES (DATE '{D}', 'T-1', 500, 1, '{HASH_A}')"
    )
    tag_close(spark, TABLE, "close_2026_03_31")

    spark.sql(f"INSERT INTO {TABLE} VALUES (DATE '{D}', 'T-2', 700, 1, '{HASH_A}')")
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots("
        f"table => 'silver.settlement', older_than => TIMESTAMP '2099-01-01 00:00:00', "
        f"retain_last => 1)"
    )

    as_of_close = spark.sql(
        f"SELECT count(*) FROM {TABLE} VERSION AS OF 'close_2026_03_31'"
    ).first()[0]
    assert as_of_close == 1, "the tagged snapshot was expired away"
    assert spark.table(TABLE).count() == 2, "current state should still hold both rows"

