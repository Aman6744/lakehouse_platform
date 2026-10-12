# Lakehouse Platform — Lending, Insurance and Recharge

Three businesses, each with its own OLTP systems, event streams, partner files,
third-party APIs and hand-maintained spreadsheets. That is a normal way for a company
to arrive at the stated symptom: nobody can say with confidence whether a given table is
current or correct. Everything here exists to make that sentence false.

"Current or correct" reads as two separable and separately hard questions. *Correct* is
the transformation being right — same raw fact in, same number out, deterministically.
*Current* is honesty about time: the platform knowing what it does not yet know, and
saying so, rather than publishing a plausible number built on an incomplete input.

None of the hard problems is about volume. 10k events/sec is roughly 5 MB/s. They are
trust problems that happen to occur at a size where you cannot fix them by hand.

## Start here

| Document | What it covers |
|---|---|
| [`docs/design/Design_Document.pdf`](docs/design/Design_Document.pdf) | The design: architecture with chosen/rejected/cost, ingestion per source, physical layout, capacity, the three hard problems, serving, operations, and the honesty section |
| [`docs/design/Test_Plan.pdf`](docs/design/Test_Plan.pdf) | What the suite asserts, and how each test fails if the design is wrong |
| [`docs/adr/`](docs/adr/) | Seven architecture decision records — the alternatives and trade-off detail behind each genuinely contested call |
| [`docs/architecture/`](docs/architecture/) | Platform overview (SVG) plus one diagram per hard problem |

Both documents ship as `.docx` and `.pdf`. The `.pdf` is the one to read; the `.docx`
is there so the content can be quoted or diffed without a convertor.

## Stack

| Component | Choice | What it buys here | ADR |
|---|---|---|---|
| Table format | Apache Iceberg on S3/Parquet | Branches and tags are first-class — the publish gate and the audit trail both lean on them directly | [0001](docs/adr/0001-table-format-iceberg.md) |
| Catalog | Iceberg REST (Polaris / Nessie) | The interface Iceberg defines, so every engine speaks it without a vendor bridge | [0001](docs/adr/0001-table-format-iceberg.md) |
| Processing | Spark — batch and Structured Streaming | One engine for both; nothing downstream needs sub-second freshness | [0002](docs/adr/0002-spark-only-no-flink.md) |
| Orchestration | Dagster, partition-aware assets | The real scheduling condition is "has business date D arrived", not "is it 2am" | [0002](docs/adr/0002-spark-only-no-flink.md) |
| CDC write mode | Merge-on-read; copy-on-write for marts | A 1-minute MERGE under the copy-on-write default rewrites a large fraction of the table every minute | [0006](docs/adr/0006-merge-on-read-for-cdc-targets.md) |
| Compression | Zstandard level 3 | Ratio dominates over five years of retention, and object storage charges for scanned bytes too | [0007](docs/adr/0007-physical-storage-compression-and-encryption.md) |
| Encryption | SSE-KMS per zone, mTLS, per-subject keys for PII | Erasure becomes key destruction rather than rewriting five years of pinned files | [0007](docs/adr/0007-physical-storage-compression-and-encryption.md) |
| Serving | Trino for analytics, a KV store for applications | Four consumers want genuinely different shapes; one API over one table would serve all four badly | design doc §8 |

## Repository layout

```
.
├── src/dataplatform/           the three guarantees, as code
│   ├── cdc_apply.py            CDC applied safely under replay          (G1–G4)
│   ├── publish_gate.py         completeness, and refusing to publish    (G5–G7)
│   ├── restatement.py          corrections without rewriting history    (G8–G10)
│   └── sqlutil.py              the only sanctioned way to build SQL
│
├── tests/                      pytest against a live in-process Iceberg catalog
│   ├── conftest.py             Spark local[2], hadoop catalog, tmp warehouse
│   └── test_*.py               one file per hard problem
│
├── docs/
│   ├── design/                 Design_Document + Test_Plan (.docx and .pdf)
│   ├── architecture/           platform-overview.svg + hardproblem{1,2,3}.png
│   └── adr/                    7 architecture decision records
│
├── .github/workflows/ci.yml    lint · compile · tests · secret scan
└── pyproject.toml              packaging, pytest and ruff configuration
```

## Running it

```bash
pip install -e ".[dev]"
pytest
```

`pytest` works straight from a clone — `pyproject.toml` puts `src` and `tests` on the
path, so no install step is needed just to read a test run. The suite runs against a
**live in-process Iceberg catalog**: Spark `local[2]`, hadoop catalog type, a temporary
warehouse, the Iceberg runtime pinned in `tests/conftest.py`. Assertions are against
real `MERGE`, real branch DDL and real `fast_forward` semantics rather than mocks, which
is the only way a test catches a wrong API rather than just a wrong idea. A first run
needs network access to Maven Central to resolve the Iceberg runtime, or a populated
`~/.ivy2` cache.

Nothing needs to run for review.

## Assumptions

- **Settlement cutover.** Lending and payments settle at 23:00 IST, not midnight, so
  `business_date` derives from that cutover rather than the calendar date. I do not have
  the real cutover times for insurance or recharge and did not invent them; the design
  differentiates those two on behaviour I *can* reason about — restatement tail, volume,
  latency tolerance.
- **Catalog.** An Iceberg REST catalog rather than Hive Metastore — a greenfield choice
  on merits, and the component I expect to break second.
- **Latency.** Nothing downstream needs sub-second freshness, which is what justifies
  one engine instead of adding Flink. If a fraud use case later needs it, that decision
  gets revisited rather than tuned.
- **`due_after_close` of 6 hours** is a reasonable-sounding default, not something
  load-tested against real arrival patterns — almost certainly too generous for recharge
  and too tight for insurance.
- **Watermark contiguity** assumes every calendar date is owed by every source. A source
  idle at weekends would over-hold; `_expected_dates` is the named seam for a per-source
  business calendar.

## Code size

`src/dataplatform/` is roughly 1,200 lines across four modules, about 600 of them
executable once docstrings and comments are stripped, against a suggested 200–400. It is
over, and I would rather say that plainly than derive a number that makes it look
compliant.

The overage is almost entirely prose, and deliberate: each module header states the
guarantees it owns, and the reasoning for a decision sits next to the line implementing
it — the argument for `s._lsn > t._lsn`, or for marking a batch applied *after* the data
commit rather than before, belongs at those lines. The brief says it reads code as code,
because that is where a guarantee either holds or does not. Strip the prose and the
mechanisms are compact; the prose is there because the mechanisms are the part worth
defending. Tests are a further ~900 lines including fixtures.

## AI disclosure

I used AI to draft and iterate the code, tests and documents against a design I had
already decided on, to render the diagrams from a layout I specified box by box, and as
an adversarial reviewer of my own drafts.

Everything it produced I verified rather than trusted, and that is worth being specific
about, because the errors a fluent draft produces are exactly the ones that read as
correct:

- **Iceberg APIs that do not exist.** An `INSERT INTO … REPLACE WHERE` that reads
  perfectly naturally and is Delta Lake syntax Iceberg does not have, plus three
  `system.*` procedure calls for creating and dropping branches and tags — those are DDL
  in Iceberg, not procedures. Every Iceberg API in this repo was checked against the
  documentation.
- **The TOAST sentinel.** Debezium sends `__debezium_unavailable_value` for a TOASTed
  column the source did not send, and the obvious implementation — collapse to the whole
  max-`_lsn` row, `MERGE … SET *` — writes that string over real data. Silent, unpaged,
  and wrong until somebody reads that column. Caught by reading the applier against the
  Debezium documentation, which is a thing no test would have told me.

Three places in this design have an implementation that looks right and is not, and they
are the places to push on: the per-batch marker is an optimisation and not the
idempotency guarantee; a completeness watermark must not take a maximum *within* an
upstream, or a hole in its history passes the gate the gate exists to guard; and `SET *`
cannot express a per-column TOAST guard.

Problem selection, the mechanisms, and the framing are mine: which three problems to go
deep on, the min-not-max and no-holes-behind-it watermark rules, treating a missing
signal as unknown rather than optimistic, the LSN predicate as the guarantee with the
marker demoted, content-hash classification, tags-as-knowledge-time, the line that an
override waives completeness but never a P0, and the five-clocks framing that ties the
three together. Full detail in the design document's honesty section.
