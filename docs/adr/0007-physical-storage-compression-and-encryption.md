# ADR-0007: Physical storage — compression and encryption

- **Status:** Accepted

Two decisions about the bytes on disk. They belong together because they are
both properties of the table rather than of whichever job wrote it, and because
the second one collides with ADR-0005.

## Context

Roughly 500M rows/day from files and ~216M events/day from streams, held five
years, for three regulated consumer businesses holding 50M customers' personal
and financial data in India under the DPDP Act.

**Compression** affects three budgets at once: storage (bytes × 5 years), scan
cost (bytes read per query, which on object storage is also request cost), and
ingest CPU (competing with the merge and compaction for the same pool).

**Encryption** has three requirements that get conflated if not named apart:
at rest (bytes unreadable without going through the platform), in transit (not
readable on the wire), and **erasure** — a data subject can require deletion,
which collides directly with ADR-0005 pinning files for five years so an auditor
can reproduce a closed report. You cannot delete rows a tag holds. That third
one is the hard one.

## Decision

**Compression: Zstandard at level 3** for all Parquet, set as a table property:

```
write.parquet.compression-codec = zstd
write.parquet.compression-level  = 3
```

Dictionary encoding stays on (the Parquet default) — it does most of the real
work on the columns that dominate these tables (`status`, `source_system`,
`business_line`, currency codes), and no general-purpose codec beats a dictionary
on low-cardinality strings. Level 3 rather than the library default of 1, and
well below maximum: the ratio curve flattens sharply after ~3 while CPU keeps
climbing, so levels in the teens buy a few percent of storage for a large share
of ingest CPU on a path that runs every minute.

**At rest: SSE-KMS with a customer-managed key per zone.** Separate keys for
bronze, silver and gold, so a compromise or revocation is scoped to a layer and
key usage is auditable through KMS access logs. Server-side rather than
client-side because Spark, Trino and compaction all read the same files, and
client-side would force every engine to share key material.

**In transit: TLS everywhere, mTLS between platform services.** Kerberos for
Hadoop-protocol access where it exists, OIDC for human access to Trino, Ranger
for authorisation once authenticated.

**For PII: per-subject envelope encryption, and erasure by key destruction.**
Subject-identifying columns are encrypted under a per-subject data key, itself
wrapped by a KMS master key. An erasure request destroys the subject's data key
rather than rewriting five years of Parquet. Tagged historical reads still
resolve — files, row counts and every aggregate remain exactly as reported — but
the personal columns return ciphertext nobody can decrypt. The audit trail keeps
its shape without keeping the data.

## Alternatives considered

**Snappy** — the historical Parquet default, fastest to decompress. Rejected
because zstd-3 compresses meaningfully better at broadly comparable decompression
speed; over five years that ratio difference is the dominant term, and the
scan-cost saving compounds on every query rather than being paid once at write
time. **Gzip** — better ratio than Snappy, but substantially slower to
decompress, and read latency is paid by every analyst query forever. **LZ4** —
faster still, rejected more strongly: at ~5 MB/s this platform is not
write-CPU-bound, so trading ratio for compression speed optimises a resource that
is not scarce. **Uncompressed** — object storage charges for bytes stored *and*
scanned, so compression is not a space/time trade-off here; it is cheaper on
both.

**Row deletion on erasure request.** The obvious reading of the regulation,
rejected on mechanics: tags pin the data files a closed-period read depends on,
so deleting rows either breaks that read or requires dropping the tag, which
destroys the guarantee that justified tags. Also unbounded — one request rewrites
every partition the subject appears in across five years.

**Masking or tokenising at the serving layer only.** Cheap and reversible.
Rejected as insufficient for erasure: the plaintext still exists at rest, so
nothing has been erased. An access-control measure wearing a privacy-control
costume.

**Column-level encryption for every column.** Disproportionate: it puts a KMS
call on the read path for columns with no privacy value, and read-path KMS
latency is the main cost of this approach, so it should be spent only where it
buys something.

**Iceberg native table encryption.** Attractive — encryption as a table property,
managed by the format. Rejected because support across Spark and Trino is less
mature than SSE-KMS, and the catalog (ADR-0001) is already the component this
design expects to break first; a second immature dependency on the same critical
path compounds that.

## Consequences

- Storage lands around 25–40GB/day compressed from files, ~12–15TB/year, so
  roughly 60–75TB over five years before tag-pinned restatement versions.
- Ingest CPU is higher than Snappy. Accepted because the streaming path has
  headroom; it would need revisiting if peak throughput grew by an order of
  magnitude. Engines without zstd support cannot read these files — not a
  constraint for Spark 3.5 or Trino.
- A KMS dependency on the read path for PII columns. Latency and request cost
  scale with how often those columns are read, which argues for keeping them out
  of wide marts that get scanned routinely.
- Key management becomes a real system at 50M subjects, not a configuration line:
  rotation, caching, KMS request rate under a large analytical scan, and what a
  tagged read does when its key was destroyed mid-query. **Designed to the level
  of a direction, not an implementation.**
- **Crypto-shredding may not satisfy a regulator.** That destroying the key
  constitutes erasure is reasonable and widely argued, but not settled, and I
  would want legal in the room before committing. The single constraint most
  likely to force a redesign of ADR-0005.
- **Compression ratios are not measured.** These are published, widely
  reproduced codec characteristics, not a benchmark against this data. Column
  cardinality drives real-world ratio more than codec choice does, so measuring
  per-column is the first thing worth doing with production data — it may move
  the level rather than the codec.
