"""Shared test fixtures.

`pythonpath = ["src", "tests"]` in pyproject.toml resolves both
`import dataplatform...` and `from conftest import ...`, so `pytest` works
straight from a clone with no install step. The package is called
`dataplatform` rather than `code` deliberately -- `code` is a standard library
module, and shadowing it works only by accident of `sys.path` order.

One Spark session is shared across the whole run: starting a JVM per module
costs more than every assertion in this suite combined.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

# Pinned rather than floating: branch and tag DDL, and the fast_forward
# procedure, are version-sensitive and a silent upgrade would change what these
# tests are actually asserting about.
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
CATALOG = "local"


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-guarantees")
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(warehouse))
        .config("spark.sql.shuffle.partitions", "4")
        # The design turns on a business_date derived from a 23:00 IST settlement
        # cutover, so DATE and TIMESTAMP handling is load-bearing. Pinning the
        # session to UTC keeps these assertions deterministic; left at the
        # machine's local zone, a test could pass here and fail on a reviewer's
        # laptop for reasons that have nothing to do with the design.
        .config("spark.sql.session.timeZone", "UTC")
        # No UI port to bind, and no hostname resolution on the way up -- both
        # are pure startup cost for a test run, and an unresolvable hostname
        # stalls session creation for seconds on some laptops.
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    # Spark logs at INFO by default, which buries the assertion that actually
    # failed under several screens of scheduler output.
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def namespaces(spark):
    for ns in ("platform", "bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")


def create_ingest_ledger(spark) -> None:
    """The completeness signal the publish gate reads, and the claim table the
    restatement path writes. One table, two readers, by design: a delivery is
    not complete until the loader says so, and the gate must not be able to
    disagree with the loader about what arrived."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.platform.ingest_ledger")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.platform.ingest_ledger (
            source STRING, business_date DATE, file_hash STRING,
            knowledge_version INT, uri STRING, row_count BIGINT,
            worker_id STRING, status STRING, claimed_at TIMESTAMP
        ) USING iceberg
    """)


def create_dataset_manifest(spark) -> None:
    """Every gate decision, including refusals. Append-only: "we looked at this
    partition and refused it, for these reasons, at this time" is the evidence
    an auditor asks for six months later."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.platform.dataset_manifest")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.platform.dataset_manifest (
            dataset STRING, business_date DATE, decision_seq BIGINT, status STRING,
            valid_through DATE, published_at TIMESTAMP, findings STRING, reason STRING
        ) USING iceberg
    """)


def create_applied_batches(spark) -> None:
    """The CDC replay short-circuit. Partitioned by topic so the probe prunes to
    a single partition rather than scanning every batch the platform has ever
    applied."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.platform.cdc_applied_batches")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.platform.cdc_applied_batches (
            topic STRING, batch_id STRING, row_count BIGINT, applied_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (topic)
    """)


def create_invalidations(spark) -> None:
    """Downstream staleness raised by a published restatement.

    Rows rather than a direct scheduler call, so the propagation is itself
    auditable -- if a mart is wrong, there is a record of whether it was ever
    told to rebuild.
    """
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.platform.invalidations")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.platform.invalidations (
            dataset STRING, business_date DATE, knowledge_version INT,
            status STRING, raised_at TIMESTAMP
        ) USING iceberg
    """)
