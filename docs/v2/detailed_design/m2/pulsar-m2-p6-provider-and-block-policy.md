---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M2-P6 provider and block-policy evidence

P6 turns the P0 capability seam into a bounded S3 production adapter and executes the ADR 0056/0057 candidate
protocol before any provider or class is promoted. `S3PulsarOffloadObjectStoreV1` admits only a 4-GiB NPD1/NPO1
Object envelope even when the remote provider supports more. It uses `If-None-Match: *`, a declared-length streaming
request body, Nereus SHA-256 metadata, exact bounded Range GETs, delete followed by absence proof, and abort/relist
multipart cleanup under the attempt prefix. SDK status and client failures are mapped into the closed provider failure
model. The adapter executor and client are Cell-session resources and reject calls after close.

The P6 S3-compatible execution uses `localstack/localstack:4.14.0`. This proves the adapter request/response behavior,
canonical NPD1/NPO1 publication, targeted reads, conditional conflict, deletion, and multipart cleanup; it is not an
Amazon S3 latency, durability, availability, or service-endorsement claim. Published S3 protocol limits remain a
separate capability-source attachment.

## Candidate and resolution contract

The evidence matrix retains exactly the 1/4/8/16-MiB targets and both `FIXED_NONE` and `ZSTD_IF_SMALLER`. Eligible ZSTD
persists the actual `NONE` or `ZSTD` family independently in every sparse row and never creates a RAW class. The
selection candidate is limited to `latency-1mib`, `balanced-4mib`, and `scan-8mib`; it becomes authoritative only
when the source-qualified P6 receipt and ADR/open-question updates land together. The initial Deployment default
candidate is `balanced-4mib`.

Resolution is strictly Deployment base, then Namespace override, then Topic override. The Cell may reject a resolved
class against its admitted set or target cap, and the host may reject it against a decoded-block memory ceiling. Neither
may replace or relabel the semantic result. The native attempt persists class ID, exact target, and compression policy;
reads and source-deletion revalidation use the persisted values after failover. Unsupported IDs, target mismatch, or
policy drift fail before provider I/O.

The proposed selected hard envelope is 4 GiB per data Object, 1,024 multipart parts, 64 MiB per entry and decoded
block, and 65,536 entries per block. P6 must cover all four target candidates, 100-byte/50,000-entry ledgers, a 20-MiB
medium-entry scan ledger, the stock 5-MiB message case, a dedicated near-64-MiB entry, random and sequential reads,
provider latency/request/byte counts,
AEAD/decode CPU, compression ratio, observed heap/direct memory, concurrency, and the pinned native Pulsar 1-MiB read
buffer baseline.

## Promotion boundary

`v2M2PulsarP6Check` must require zero skipped Docker/provider tests, a clean exact Pulsar source tuple, canonical receipt
validation, and all preceding P0-P5 gates. P6 does not wire a broker process or NAR, promote Pulsar Final scenarios,
prove Amazon S3 production performance, or establish global M2 PASS. M6 still owns process activation.
