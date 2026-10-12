"""The second hardest test: prove that an upstream which is not provably
complete stops a joined dataset from publishing, even though the data is sitting
there looking perfectly reasonable.

This is the failure mode I care most about after replay safety, because the
broken output is *plausible*. If the repayment feed stalls and the loan feed does
not, a loans-with-repayments mart publishes rows in which loans genuinely appear
to have no repayments. Row counts look sane, no null check fires, no referential
check fires -- every loan legitimately may have zero repayments. The only thing
that catches it is refusing to publish past the slowest input.

Two ways to get that wrong, and this file asserts both:

  * `max` across upstreams instead of `min` -- the laggard is ignored
  * `max` *within* one upstream -- a hole in its history is ignored, which is
    the subtler of the two and the one a suite written against the obvious bug
    would miss

How this fails if the design is wrong
-------------------------------------
* `effective_watermark` takes `max(...)` instead of `min(...)` ->
  `test_stalled_upstream_is_held` fails at the HELD assertion.
* `source_watermark` reverts to `max(business_date) WHERE status='COMPLETE'` ->
  `test_hole_in_upstream_history_is_held` fails, and that is the one that would
  publish a month-end close built on a missing day.
* A missing upstream watermark is treated optimistically ->
  `test_unknown_upstream_is_not_treated_as_current` fails.
* Checks run against `main` instead of the candidate branch, or `fast_forward`
  happens before the checks -> `test_p0_failure_holds_and_main_is_untouched`
  fails, which is the assertion protecting "stale but correct".
* An override is allowed to waive a P0 -> `test_override_cannot_waive_a_p0`
  fails. That distinction is a policy decision, so it gets its own test rather
  than living in a comment.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from conftest import CATALOG, create_dataset_manifest, create_ingest_ledger
from dataplatform.publish_gate import (
    ArrivalContract,
    Check,
    Decision,
    Override,
    Severity,
    dataset_valid_through,
    effective_watermark,
    publish_partition,
)

D = date(2026, 3, 31)
MART = f"{CATALOG}.gold.loan_repayment_daily"

NON_NEGATIVE_PRINCIPAL = Check(
    name="principal_is_non_negative",
    severity=Severity.P0,
    violation_sql=(
        "SELECT count(*) FROM {branch} "
        "WHERE business_date = DATE '{business_date}' AND principal_paise < 0"
    ),
    rationale="Money is integer paise; a negative principal means a bad cast upstream.",
)

CONTRACT = ArrivalContract(
    dataset=MART,
    upstreams=("lending_core", "payments_core"),
    due_after_close=timedelta(hours=6),
    checks=(NON_NEGATIVE_PRINCIPAL,),
)


@pytest.fixture(autouse=True)
def platform_tables(spark, namespaces, monkeypatch):
    monkeypatch.setattr("dataplatform.publish_gate._page", lambda *a, **k: None)
    monkeypatch.setattr("dataplatform.publish_gate._raise_warning", lambda *a, **k: None)

    create_ingest_ledger(spark)
    create_dataset_manifest(spark)
    spark.sql(f"DROP TABLE IF EXISTS {MART}")
    spark.sql(f"""
        CREATE TABLE {MART} (
            business_date DATE, loan_id STRING, principal_paise BIGINT, repaid_paise BIGINT
        ) USING iceberg
        PARTITIONED BY (days(business_date))
        TBLPROPERTIES ('write.wap.enabled' = 'true')
    """)
    # Seed an earlier partition so the table has a snapshot to branch from --
    # Iceberg cannot create a branch on a table with no commits -- and so the
    # "main is untouched" assertions run against a non-empty baseline rather
    # than against zero, where any bug that writes nothing would also pass.
    spark.sql(f"INSERT INTO {MART} VALUES (DATE '{D - timedelta(days=10)}', 'L-OLD', 10000, 10000)")


def mark_complete(spark, source: str, dates) -> None:
    """Declare an upstream complete for an explicit set of business dates.

    Explicit rather than a range, because the interesting test is the one where
    the set has a hole in it.
    """
    for d in dates:
        # Every column is listed, including the ones an arrival signal has no
        # opinion about. The ledger carries two kinds of row by design -- an
        # upstream declaring a business date complete, and a file claim from the
        # restatement path -- so file_hash, uri and worker_id are genuinely null
        # here. They are spelled out because Spark's v2 write path requires a
        # value for every output column and Iceberg declares no defaults: a
        # partial column list fails with CANNOT_FIND_DATA rather than filling
        # the gaps. Casts are explicit so the nulls carry the column's type.
        spark.sql(
            f"INSERT INTO {CATALOG}.platform.ingest_ledger "
            f"(source, business_date, file_hash, knowledge_version, uri, "
            f"row_count, worker_id, status, claimed_at) VALUES ("
            f"'{source}', DATE '{d}', CAST(NULL AS STRING), CAST(NULL AS INT), "
            f"CAST(NULL AS STRING), CAST(NULL AS BIGINT), CAST(NULL AS STRING), "
            f"'COMPLETE', CAST(NULL AS TIMESTAMP))"
        )


def days_through(through: date, span: int = 5):
    start = D - timedelta(days=span)
    return [start + timedelta(days=i) for i in range((through - start).days + 1)]


def stage_candidate(spark, rows: list[tuple], business_date: date = D) -> None:
    """Write the candidate partition to the WAP branch, as the mart job would.

    Branch creation is DDL -- `ALTER TABLE ... CREATE BRANCH`. Iceberg has no
    `system.create_branch` procedure, and asserting against the real DDL is the
    entire point of running this on a live catalog rather than a mock.
    """
    branch = CONTRACT.candidate_branch(business_date)
    spark.sql(f"ALTER TABLE {MART} CREATE OR REPLACE BRANCH {branch}")
    spark.conf.set("spark.wap.branch", branch)
    try:
        spark.createDataFrame(
            rows,
            "business_date date, loan_id string, principal_paise bigint, repaid_paise bigint",
        ).writeTo(MART).append()
    finally:
        spark.conf.unset("spark.wap.branch")


def latest_decision(spark, business_date: date = D) -> str:
    """Ordered by `decision_seq`, not by timestamp: two decisions can share a
    millisecond during a retry, and this assertion has to be unambiguous."""
    return spark.sql(
        f"SELECT status FROM {CATALOG}.platform.dataset_manifest "
        f"WHERE dataset = '{MART}' AND business_date = DATE '{business_date}' "
        f"ORDER BY decision_seq DESC LIMIT 1"
    ).first()["status"]


def branch_exists(spark, business_date: date = D) -> bool:
    branch = CONTRACT.candidate_branch(business_date)
    return spark.sql(f"SELECT count(*) FROM {MART}.refs WHERE name = '{branch}'").first()[0] > 0


def test_stalled_upstream_is_held(spark):
    """Healthy loan feed, repayment feed three days behind -> must not publish."""
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D - timedelta(days=3)))

    assert effective_watermark(spark, CONTRACT, D) == D - timedelta(days=3)

    stage_candidate(spark, [(D, "L-1", 500_00, 0)])
    assert publish_partition(spark, CONTRACT, D)["status"] == Decision.HELD.value
    assert latest_decision(spark) == "HELD"
    assert not branch_exists(spark), "held candidate branch was left behind"


def test_hole_in_upstream_history_is_held(spark):
    """A gap behind the newest date is still incompleteness.

    payments_core reported everything up to D *except* D-2. `max(business_date)`
    says D and waves the partition through; the watermark has to say D-3, which
    is the newest date with an unbroken run behind it.
    """
    mark_complete(spark, "lending_core", days_through(D))
    with_hole = [d for d in days_through(D) if d != D - timedelta(days=2)]
    mark_complete(spark, "payments_core", with_hole)

    assert effective_watermark(spark, CONTRACT, D) == D - timedelta(days=3)

    stage_candidate(spark, [(D, "L-1", 500_00, 250_00)])
    publish_partition(spark, CONTRACT, D)
    assert latest_decision(spark) == "HELD"


def test_unknown_upstream_is_not_treated_as_current(spark):
    """Silence from an upstream is not the same as being up to date."""
    mark_complete(spark, "lending_core", days_through(D))
    # payments_core has never reported at all.

    assert effective_watermark(spark, CONTRACT, D) is None

    stage_candidate(spark, [(D, "L-1", 500_00, 0)])
    publish_partition(spark, CONTRACT, D)
    assert latest_decision(spark) == "HELD"


def test_p0_failure_holds_and_main_is_untouched(spark):
    """Every upstream complete, but the data itself violates a P0.

    The gate has to get past the completeness check to reach the check suite at
    all, which is why this test exists separately -- the earlier suite held on
    the watermark in every case and never once executed a check.
    """
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D))

    before = spark.table(MART).count()
    stage_candidate(spark, [(D, "L-1", -1, 0)])  # negative paise: a bad cast upstream
    row = publish_partition(spark, CONTRACT, D)

    assert row["status"] == Decision.HELD.value
    assert row["reason"] == "P0 check failure"
    assert (
        row["findings"] == '{"principal_is_non_negative": 1}'
    ), "findings must be JSON -- str(dict) emits single quotes and corrupts the literal"
    assert spark.table(MART).count() == before, "held partition leaked onto main"
    assert row["valid_through"] is None


def test_publishes_once_the_laggard_catches_up(spark):
    """The gate must not be a ratchet -- recovery has to actually work."""
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D))

    stage_candidate(spark, [(D, "L-1", 500_00, 250_00)])
    row = publish_partition(spark, CONTRACT, D)

    assert row["status"] == Decision.PUBLISHED.value
    assert row["valid_through"] == D
    assert (
        spark.sql(f"SELECT count(*) FROM {MART} WHERE business_date = DATE '{D}'").first()[0] == 1
    )
    assert dataset_valid_through(spark, CONTRACT) == D
    assert not branch_exists(spark), "published candidate branch was not cleaned up"


def test_override_publishes_incomplete_data_but_says_so(spark):
    """Finance would sometimes rather have a partial close than none.

    The escape hatch exists, but it is recorded as OVERRIDDEN with an approver
    and a ticket -- not as PUBLISHED -- so a consumer and an auditor can both
    tell the difference later.
    """
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D - timedelta(days=3)))

    stage_candidate(spark, [(D, "L-1", 500_00, 0)])
    row = publish_partition(
        spark,
        CONTRACT,
        D,
        override=Override(
            approver="cfo@example.com", ticket="FIN-9912", reason="statutory filing deadline"
        ),
    )

    assert row["status"] == Decision.OVERRIDDEN.value
    assert "FIN-9912" in row["reason"]
    assert (
        spark.sql(f"SELECT count(*) FROM {MART} WHERE business_date = DATE '{D}'").first()[0] == 1
    )


def test_override_cannot_waive_a_p0(spark):
    """Incompleteness is a judgement call. A failed P0 is not."""
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D - timedelta(days=3)))

    before = spark.table(MART).count()
    stage_candidate(spark, [(D, "L-1", -1, 0)])
    row = publish_partition(
        spark,
        CONTRACT,
        D,
        override=Override(approver="cfo@example.com", ticket="FIN-9912", reason="deadline"),
    )

    assert row["status"] == Decision.HELD.value
    assert row["reason"] == "P0 check failure"
    assert spark.table(MART).count() == before


def test_valid_through_drops_back_when_a_published_date_is_later_held(spark):
    """A restatement can make a previously good partition bad again.

    `dataset_valid_through` reads the *latest* decision per business date, so a
    partition that publishes on Monday and is held on Tuesday stops being
    served -- rather than staying visible because it was once fine.
    """
    mark_complete(spark, "lending_core", days_through(D))
    mark_complete(spark, "payments_core", days_through(D))
    stage_candidate(spark, [(D, "L-1", 500_00, 250_00)])
    publish_partition(spark, CONTRACT, D)
    assert dataset_valid_through(spark, CONTRACT) == D

    stage_candidate(spark, [(D, "L-1", -1, 0)])  # the correction is itself broken
    publish_partition(spark, CONTRACT, D)

    assert latest_decision(spark) == "HELD"
    assert dataset_valid_through(spark, CONTRACT) is None
