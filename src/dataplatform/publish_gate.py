"""Hard problem 2 of 3 -- deciding a dataset is complete, and refusing to
publish it when it is not.

Guarantees this module owns
--------------------------
G4  Nothing becomes visible to a consumer until every P0 check has passed on
    the exact bytes that will be published.
G5  A derived dataset never claims to be fresher than its least-fresh input,
    and "least fresh" means the newest date with no holes behind it, not the
    newest date present.
G6  Every gate decision -- publish, hold, or failure -- is recorded with a
    machine-readable `valid_through`, so a consumer can answer "is this
    current?" without asking a human.

Completeness is not observable: you cannot know whether more data is coming for
a business date, only whether everything you were promised has arrived. So it
has to be declared -- an arrival contract per source -- and then decided against.
G5 has two failure modes, not one: a maximum *across* upstreams ignores the
laggard, and a maximum *within* one upstream ignores holes. Both are handled in
`source_watermark` and `effective_watermark` below. Mechanism, trade-offs and
the override policy: design document section 6.2.

Plumbing deliberately stubbed: alerting, the approval workflow that mints an
Override, the per-source business calendar.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum

from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException
from pyspark.sql import Row, SparkSession

from dataplatform.sqlutil import catalog_of, relative_to_catalog, sql_ident, sql_str


class Severity(str, Enum):
    P0 = "P0"  # blocks publication, and no override can waive it
    P1 = "P1"  # publishes, but raises and is tracked


class Decision(str, Enum):
    PUBLISHED = "PUBLISHED"
    HELD = "HELD"
    OVERRIDDEN = "OVERRIDDEN"  # published with an incomplete upstream, on record
    FAILED = "FAILED"  # the gate itself could not complete; main untouched


@dataclass(frozen=True)
class Check:
    """One assertion, expressed as SQL that counts violations.

    Counting rather than returning a boolean matters: whoever is paged at 03:00
    needs the blast radius before deciding whether to escalate, and "1 row"
    versus "4.2M rows" are very different pages.

    `violation_sql` may reference `{branch}` and `{business_date}`; substitution
    is literal, so SQL containing other braces (a JSON path, say) is safe.
    """

    name: str
    severity: Severity
    violation_sql: str
    rationale: str

    def render(self, branch_table: str, business_date: date) -> str:
        return self.violation_sql.replace("{branch}", branch_table).replace(
            "{business_date}", business_date.isoformat()
        )


@dataclass(frozen=True)
class Override:
    """An audited waiver of the completeness hold. Never of a P0 check."""

    approver: str
    ticket: str
    reason: str


@dataclass(frozen=True)
class ArrivalContract:
    """What a dataset is promised, and by when."""

    dataset: str
    upstreams: tuple[str, ...]
    # Offset from the close of the business date by which all inputs must land.
    # Settlement feeds cut over at 23:00 IST, not midnight, so a partition for
    # business date D legitimately keeps filling until D+1.
    due_after_close: timedelta = timedelta(hours=6)
    # How far back `source_watermark` looks for holes. Bounded because the
    # alternative is rescanning five years of ledger on every gate run.
    hole_lookback: timedelta = timedelta(days=30)
    checks: tuple[Check, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        sql_ident(self.dataset)
        if not self.upstreams:
            # A dataset with no declared upstreams can never satisfy the gate,
            # so it would hold forever and look like a stuck job rather than a
            # misconfiguration. A source-shaped dataset lists itself.
            raise ValueError(f"{self.dataset}: contract declares no upstreams")

    @property
    def catalog(self) -> str:
        return catalog_of(self.dataset)

    @property
    def manifest(self) -> str:
        return f"{self.catalog}.platform.dataset_manifest"

    @property
    def ingest_ledger(self) -> str:
        return f"{self.catalog}.platform.ingest_ledger"

    def candidate_branch(self, business_date: date) -> str:
        """One branch per business date.

        A single shared `publish_candidate` branch would make a backfill and the
        daily run collide: whichever held first would drop the other's
        candidate out from under it.
        """
        return f"publish_candidate_{business_date:%Y%m%d}"

    def branch_table(self, business_date: date) -> str:
        return f"{self.dataset}.branch_{self.candidate_branch(business_date)}"


def source_watermark(
    spark: SparkSession,
    contract: ArrivalContract,
    source: str,
    business_date: date,
) -> date | None:
    """The latest business date for which `source` is provably complete.

    Read from the ingest ledger, not from the data: asking the data "what is the
    max date present?" answers a much weaker question, since one late file makes
    the data look complete when it is not.

    Returns the newest date with an *unbroken run of COMPLETE dates behind it*,
    anchored at the earliest COMPLETE date inside the lookback window (so a
    source that only started reporting mid-window is not penalised for the dates
    before it existed). A hole anywhere after that anchor caps the watermark
    below it, which is the point.

    Assumption stated: every calendar date in the window is expected to have
    data. For a source that is idle at weekends this over-holds, and the fix is
    a per-source business calendar -- `_expected_dates` is the seam for it.
    """
    floor = business_date - contract.hole_lookback
    rows = spark.sql(
        f"""
        SELECT DISTINCT business_date
        FROM {contract.ingest_ledger}
        WHERE source = {sql_str(source)}
          AND status = 'COMPLETE'
          AND business_date BETWEEN DATE {sql_str(floor)} AND DATE {sql_str(business_date)}
        ORDER BY business_date
        """
    ).collect()
    complete = [r["business_date"] for r in rows]
    if not complete:
        return None

    expected = _expected_dates(source, complete[0], business_date)
    watermark = complete[0]
    present = set(complete)
    for d in expected:
        if d <= watermark:
            continue
        if d not in present:
            break
        watermark = d
    return watermark


def effective_watermark(
    spark: SparkSession, contract: ArrivalContract, business_date: date
) -> date | None:
    """G5. The minimum watermark across every upstream.

    `None` from any single upstream collapses the whole result to `None`. An
    upstream we know nothing about is not the same as an upstream that is up to
    date, and treating a missing signal as an optimistic one is how silent
    incompleteness gets published.
    """
    marks = [source_watermark(spark, contract, u, business_date) for u in contract.upstreams]
    if any(m is None for m in marks):
        return None
    return min(marks)


def run_checks(
    spark: SparkSession, contract: ArrivalContract, business_date: date
) -> list[tuple[Check, int]]:
    """Evaluate every check against the candidate branch, not against `main`.

    Reading `<table>.branch_<candidate>` is what makes this a real audit rather
    than a dry run: the checks run over the exact bytes that will be published,
    with real cardinalities and real skew.
    """
    branch_table = contract.branch_table(business_date)
    return [
        (check, int(spark.sql(check.render(branch_table, business_date)).first()[0]))
        for check in contract.checks
    ]


def publish_partition(
    spark: SparkSession,
    contract: ArrivalContract,
    business_date: date,
    override: Override | None = None,
) -> Row:
    """Decide, then act. Records the gate report on every path.

    "We looked at this partition and refused it, for these reasons, at this
    time" is exactly the evidence an auditor asks for six months later, so the
    manifest is written whether we publish, hold, or fail.
    """
    watermark = effective_watermark(spark, contract, business_date)
    incomplete = watermark is None or watermark < business_date

    if incomplete and override is None:
        return _hold(
            spark,
            contract,
            business_date,
            reason=f"incomplete upstreams; effective watermark = {watermark}",
            findings=[],
        )

    results = run_checks(spark, contract, business_date)
    blocking = [(c, n) for c, n in results if n > 0 and c.severity is Severity.P0]
    warnings = [(c, n) for c, n in results if n > 0 and c.severity is Severity.P1]

    if blocking:
        # Deliberately checked after the override branch: an override waives
        # incompleteness, never a P0 violation.
        return _hold(
            spark,
            contract,
            business_date,
            reason="P0 check failure",
            findings=blocking,
        )

    branch = contract.candidate_branch(business_date)
    try:
        # Atomic publish: one catalog operation moves `main` to the validated
        # snapshot. There is no window in which a consumer sees half a partition.
        spark.sql(
            f"CALL {contract.catalog}.system.fast_forward("
            f"table => {sql_str(relative_to_catalog(contract.dataset))}, "
            f"branch => 'main', to => {sql_str(branch)})"
        )
    except (AnalysisException, Py4JJavaError) as exc:
        # Most likely cause: `main` advanced since the branch was cut, so the
        # move is not a fast-forward. Recording FAILED matters more than the
        # exception -- an unrecorded gate crash is indistinguishable from a gate
        # that was never run, and `main` is still serving the last good state.
        # The candidate branch is deliberately left in place so the divergence
        # can be inspected before the next run overwrites it.
        return _record_and_return(
            spark,
            contract,
            business_date,
            Decision.FAILED,
            None,
            [],
            reason=f"fast_forward failed: {type(exc).__name__}: {exc}",
        )

    decision = Decision.OVERRIDDEN if incomplete else Decision.PUBLISHED
    reason = (
        f"override by {override.approver} ({override.ticket}): {override.reason}"
        if override
        else ""
    )
    row = _record_and_return(
        spark, contract, business_date, decision, watermark, warnings, reason=reason
    )
    _drop_candidate(spark, contract, business_date)
    for check, count in warnings:
        _raise_warning(contract.dataset, business_date, check, count)
    return row


def _hold(
    spark: SparkSession,
    contract: ArrivalContract,
    business_date: date,
    reason: str,
    findings: list[tuple[Check, int]],
) -> Row:
    """Discard the candidate and leave `main` serving the last good state."""
    row = _record_and_return(
        spark, contract, business_date, Decision.HELD, None, findings, reason=reason
    )
    _drop_candidate(spark, contract, business_date)
    _page(contract.dataset, business_date, reason, findings)
    return row


def _drop_candidate(spark: SparkSession, contract: ArrivalContract, business_date: date) -> None:
    """`IF EXISTS` because the completeness hold can fire before the mart job
    has staged anything, and a gate that crashes while cleaning up is worse than
    a stale branch that the next run overwrites."""
    spark.sql(
        f"ALTER TABLE {contract.dataset} "
        f"DROP BRANCH IF EXISTS {sql_ident(contract.candidate_branch(business_date))}"
    )


# --- the manifest is the consumer-facing contract ---------------------------
def _record_and_return(
    spark: SparkSession,
    contract: ArrivalContract,
    business_date: date,
    decision: Decision,
    valid_through: date | None,
    findings: list[tuple[Check, int]],
    reason: str = "",
) -> Row:
    """Append one immutable row per gate decision, then return it.

    `decision_seq` is a per-(dataset, business_date) counter rather than a
    timestamp: two decisions inside the same millisecond are entirely possible
    during a retry storm, and "latest decision" has to be unambiguous for the
    serving views built on top of this.

    Findings are serialised with `json.dumps`, not `str(dict)` -- Python's repr
    uses single quotes, which would terminate the surrounding SQL literal and
    corrupt every row that had something to report.
    """
    findings_json = json.dumps({c.name: n for c, n in findings}, sort_keys=True)
    seq = spark.sql(
        f"SELECT coalesce(max(decision_seq), 0) + 1 AS n FROM {contract.manifest} "
        f"WHERE dataset = {sql_str(contract.dataset)} "
        f"AND business_date = DATE {sql_str(business_date)}"
    ).first()["n"]

    spark.sql(
        f"""
        INSERT INTO {contract.manifest}
        SELECT {sql_str(contract.dataset)}, DATE {sql_str(business_date)}, {int(seq)},
               {sql_str(decision.value)},
               {f"DATE {sql_str(valid_through)}" if valid_through else "CAST(NULL AS DATE)"},
               current_timestamp(), {sql_str(findings_json)}, {sql_str(reason)}
        """
    )
    return spark.sql(
        f"SELECT * FROM {contract.manifest} "
        f"WHERE dataset = {sql_str(contract.dataset)} "
        f"AND business_date = DATE {sql_str(business_date)} AND decision_seq = {int(seq)}"
    ).first()


def dataset_valid_through(spark: SparkSession, contract: ArrivalContract) -> date | None:
    """G6. The single number every consumer reads.

    Trino views over gold filter on this, so a dashboard physically cannot show
    a partition the gate refused -- the trust boundary is enforced in the serving
    path, not in a runbook people are asked to remember. It is the newest
    business date whose *latest* decision was a publish, which is why the
    `decision_seq` window matters: a partition published on Monday and
    subsequently held after a restatement must drop back out.
    """
    row = spark.sql(
        f"""
        WITH latest AS (
            SELECT business_date, status,
                   row_number() OVER (PARTITION BY business_date
                                      ORDER BY decision_seq DESC) AS rn
            FROM {contract.manifest}
            WHERE dataset = {sql_str(contract.dataset)}
        )
        SELECT max(business_date) AS vt FROM latest
        WHERE rn = 1 AND status IN ('PUBLISHED', 'OVERRIDDEN')
        """
    ).first()
    return row["vt"] if row else None


# --- stubbed plumbing -------------------------------------------------------
def _expected_dates(source: str, start: date, end: date) -> list[date]:
    """Every business date `source` owes us in [start, end].

    Defaults to every calendar date. The seam exists so a source that is idle at
    weekends or on settlement holidays can supply its own calendar instead of
    over-holding; see the honesty section.
    """
    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _page(dataset, business_date, reason, findings) -> None: ...
def _raise_warning(dataset, business_date, check, count) -> None: ...
