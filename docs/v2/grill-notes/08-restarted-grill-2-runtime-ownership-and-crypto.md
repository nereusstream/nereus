---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 6: runtime ownership, block format, and crypto

Date: 2026-08-09

The user confirmed every round-5 recommendation. ADRs 0033 through 0041 own those contracts. This record preserves the
next independent decision frontier that was presented to the user. The user subsequently confirmed all seven round-6
recommendations; ADRs 0042 through 0048 now own the accepted contracts. This session record is not runtime evidence.

## Source facts used for recommendations

- Kafka research checkout `76f62f3b83e882105219b6c7687dbde594a8b8a2` currently writes `TopicRecord` before
  `PartitionRecord`s in `TopicImage`, removes the whole topic image through `RemoveTopicRecord`, and has no generated
  Nereus aggregate metadata record in `MetadataDelta`. The metadata loader publishes controller batches and snapshots
  only after replay, so V2 can validate the complete topic image at those publication cuts rather than exposing a
  half-created topic.
- Pulsar research checkout `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` caches one selected physical `ReadHandle`
  per ledger in `ManagedLedgerImpl.getLedgerHandle`. It does not own a composite Object/BookKeeper handle or a
  source-specific read pin. Native trim sets `bookkeeperDeleted=true` in metadata before issuing the physical
  BookKeeper delete, and a later delete failure is only logged.
- Stock jcloud offload blocks use a 128-byte header followed by repeated
  `[entryLength, entryId, entryBytes]` values and padding. The stock reader seeks from a sparse entry and scans forward;
  the format has no NPO1-bound per-block digest, independent compression/AEAD boundary, or bounded entry directory.
- Current Nereus `WAL_OBJECT_V1` rejects every non-zero encryption envelope length and has no V2 WalRun/root/key
  hierarchy. ADR 0040 requires independent frame AEAD, but key generation, KMS frequency, nonce uniqueness, directory
  authentication, and rotation remain unset.
- ADR 0039 deliberately leaves the WalRun Root's physical home and seal/successor publication form open. Making the
  root a provider object would add provider discovery before append; mutating an allegedly immutable root to seal it
  would create a second state authority.
- ADR 0041 fixes one immutable aligned slice per Pulsar Protocol Cell but explicitly leaves resize and additional
  slices downstream. Those choices must close before allocator/ledger-chain epoch layers and retirement proof can be
  frozen.

These are pinned-source capabilities and constraints, not V2 implementation evidence.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-KAF-META-02` |
| Q2 | `V2-OPEN-PUL-META-01` |
| Q3 | `V2-OPEN-BK-09` |
| Q4 | `V2-OPEN-BK-10` |
| Q5 | `V2-OPEN-OBJ-15` |
| Q6 | `V2-OPEN-OBJ-16` |
| Q7 | `V2-OPEN-PUL-OBJ-07` |

❓ **Q1** - **Kafka aggregate record and image ownership**: At feature level 2, should the aggregate remain an opaque
controller-side attachment, live in a parallel image, or become a first-class KRaft metadata record owned by each
topic image? What is the replay/snapshot/delete cut?

➡️ Recommend one new generated `TopicBindingAggregateRecord` API record at wire v0 with explicit typed fields rather
than an opaque byte blob or attributes map. `TopicImage` owns exactly one validated aggregate beside its partitions.
The canonical snapshot order is feature records, then for each topic `TopicRecord -> TopicBindingAggregateRecord ->
PartitionRecord*`. `RemoveTopicRecord(topicId)` removes the aggregate with the topic; no separate aggregate-delete
record exists. At level 2, duplicate/unknown/invalid aggregates fail immediately, and every live topic must have exactly
one aggregate at the atomic controller-batch boundary or completed-snapshot boundary. A transient `TopicRecord` without
its aggregate may exist only inside the unpublished batch/snapshot replay. The tradeoff is a Kafka fork change across
generated metadata, image, delta, snapshot, and tooling, in return for native topicId ownership, deterministic replay,
and no parallel lifecycle authority.

❓ **Q2** - **Pulsar aggregate deletion and same-name recreation**: Must every full immutable aggregate remain forever,
or may it be retired without reopening an old generation after delete/recreate?

➡️ Recommend two permanent fences. A name-scoped `PulsarTopicGenerationSelector` is the compact ABA authority: its
generation increases monotonically, `DELETED(g)` is durable, and overflow fails closed. The full incarnation-scoped
aggregate remains immutable while the incarnation is live, deleting, readable, or referenced by any epoch, extent,
cursor/projection, task, handle, or audit grace. Only after exact reference-free retirement proof may one exact-version
CAS replace it, last, with a compact `RetiredTopicIncarnationTombstone` at the same authority key. That tombstone binds
protocol/incarnation, generation, original aggregate SHA, and retirement-proof digest; the key is never deleted or
reused, so a delayed `putIfAbsent` cannot resurrect it. A later generation uses a new key and does not wait for old
compaction. Hard lifetime count/byte admission includes every tombstone. The tradeoff is one small permanent row per
historical incarnation, in return for releasing the full payload without a delete/retry ABA window.

❓ **Q3** - **Sealed-ledger data Object block contract**: NPO1 now freezes the root, but what physical unit does its
`SPARSE_INDEX` authorize inside the data Object? Should reads scan stock-style padded blocks, use one entry per range,
or use independently verifiable multi-entry blocks?

➡️ Recommend a distinct `NPD1` data format made of ordered, gap-free, independently decodable blocks. Each NPO1 sparse
row binds exactly one block ordinal, contiguous entry-ID range, byte offset/encoded length, decoded length, codec and
encryption family, and SHA-256 of the exact encoded block. Each block contains a bounded entry directory followed by
the exact ManagedLedger entry bytes; directory rows bind entry ID, offset, and length. One entry never crosses a block;
an entry larger than the target block size gets one dedicated oversize block subject to the hard maximum. Compression,
AEAD, and integrity reset per block, with no padding or cross-block state. The tradeoff is that a single-entry read pays
one block range GET and decode, while multi-entry blocks preserve compression/request efficiency and make every range
independently verifiable without a whole-data-object GET.

❓ **Q4** - **ManagedLedger dual-source handle and read-pin owner**: Which layer owns Object/BK fallback and prevents
native deletion from racing an already admitted BookKeeper range read?

➡️ Recommend one ManagedLedger-owned `DualSourceReadHandle` per ledger, cached instead of one selected physical handle.
It lazily owns Object and BookKeeper child handles, applies ADR 0036's source/error policy, and acquires a
source-specific range pin tied to the exact native ledger-metadata version and offload attempt. A BookKeeper deletion
cut first fences new BK pins, waits boundedly for admitted BK pins to drain, performs final Object revalidation, then
CASes `bookkeeperDeleted=true`; only afterward does it invalidate/close the BK child and issue physical deletion.
Fallback releases all partial entries and the primary pin before rechecking eligibility and pinning the secondary.
Composite close stops admission, drains both sources, and closes both children exactly once. The tradeoff is additional
per-ledger state, handles, and possible delete delay, in return for a real source-purity/deletion concurrency proof
rather than a cache invalidation race.

❓ **Q5** - **NWG1 key hierarchy, AEAD, and authenticated directory**: Is encryption keyed per group, per run, or per
topic, and how can per-frame nonce uniqueness and range-readable directory authentication avoid a KMS call per PUT?

➡️ Recommend mandatory `AES-256-GCM/HKDF-SHA-256 v1` for 0.2 NWG1. A random 256-bit WalRun data key is wrapped once
under the immutable Cell KMS key/version recorded in the root. Each ObjectExtent derives a unique object key from that
run key plus shard/run epoch/sequence using a domain-separated HKDF; sequences and run epochs are never reused. Within
the object, fixed 96-bit nonces encode a domain plus ordinal: one domain for the encrypted/authenticated
`BindingContextTable + AppendUnitDirectory`, another for frame ordinals. The fixed header, root SHA, and exact key-
envelope identity are AAD; compression happens before per-frame AEAD, and CRC32C is checked only after
decrypt/decompress. KMS unwrap and run-key caching are run-scoped, with key rotation only at rollover. The tradeoff is a
run-sized key compromise radius and mandatory crypto CPU, in return for run-scoped rather than per-object KMS
operations, deterministic nonce uniqueness, authenticated range planning, and independent frame reads. Per-object KMS
wrapping is rejected from the cost-first hot path.

❓ **Q6** - **WalRun Root physical home and seal/successor publication**: Should the root be a provider object or
control-metadata record, and should sealing mutate it or publish a separate immutable fact?

➡️ Recommend a bounded immutable `WalRunRootRecord` in the protocol Cell's control-metadata backend, addressed by key
and canonical SHA; `walRunRootKey` therefore names a metadata key, not an Object key. Root creation is `putIfAbsent`
with exact-reread response-loss recovery. Sealing never mutates the root: after stopping admission and reconciling the
tail, publish one immutable `WalRunSealRecord` binding root SHA, terminal extent sequence, and exact typed terminal
coverage. Then create the successor root referencing predecessor root+seal SHA and CAS
`CurrentWalRunPointer` from the old exact tuple to the successor. If a crash leaves the pointer on a sealed root,
recovery deterministically finishes or adopts the successor; it never reopens the sealed run. The tradeoff is two
immutable records plus one low-frequency CAS per rollover, in return for no provider-root discovery, no mutable-root
ambiguity, and zero normal-append metadata I/O.

❓ **Q7** - **Virtual-ledger slice expansion policy**: Once a Cell reaches the end of its fixed `2^k` slice, may 0.2
resize, relocate, extend, or attach a second slice?

➡️ Recommend that 0.2 forbids all four. Bounds and `sliceAssignmentId` never change, a Cell owns exactly one slice, and
exhaustion fails closed before allocating another ledger ID. Additional capacity requires a new Pulsar Protocol Cell ID
and new slice; moving an existing Topic Incarnation or ManagedLedger requires a future explicit migration contract.
Therefore exact `k` and deployment admission must cover the supported lifetime rather than assume online expansion.
The tradeoff is conservative sizing and an unavailable exhausted Cell, in return for one interval, one allocator, one
never-reuse proof, and no hidden multi-slice ordering or recovery semantics in 0.2.

## Deferred descendants

- Kafka generated field/API IDs, exact snapshot golden vectors, and feature-level-2 image validation hooks depend on
  Q1. Pulsar selector/tombstone wire, retirement receipt, reference domains, and replacement crash cuts depend on Q2.
- NPD1 fixed header/entry-directory field IDs, hard block/entry limits, codecs, AEAD envelope, and golden vectors depend
  on Q3. Persisted physical-delete intent/fact state and restart reconciliation depend on Q4.
- Exact NWG1 fixed header/directory bytes, HKDF info framing, nonce bytes, KMS envelope caps, and crypto golden vectors
  depend on Q5. Checkpoint-page authority, handoff, retirement frontier, and GC order depend on Q6.
- Virtual-ledger registry/allocator/chain epoch layers, exact `k`, allocation response loss, ledger-head publication,
  rollover/takeover, and `RETIRING -> RETIRED` proof depend on Q7.
- `V2-OPEN-OBJ-01`, `V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01` remain executable evidence gates rather than prose
  decisions. KoP remains documented and deferred outside the 0.2 runtime.

## Confirmed answer and authoritative synchronization

The user answered: “全部按推荐确认”. The decisions were synchronized as follows:

- Q1 / `V2-OPEN-KAF-META-02` →
  [ADR 0042](../../decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md): a generated typed
  wire-v0 record is owned by `TopicImage`, ordered canonically in snapshots, and removed with the topic;
- Q2 / `V2-OPEN-PUL-META-01` →
  [ADR 0043](../../decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md): the permanent
  name/generation selector and same-key retired tombstone close recreation ABA without retaining the full aggregate;
- Q3 / `V2-OPEN-BK-09` →
  [ADR 0044](../../decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md): NPD1 uses ordered, gap-free,
  independently verifiable multi-entry blocks with bounded directories;
- Q4 / `V2-OPEN-BK-10` →
  [ADR 0045](../../decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md): ManagedLedger owns one composite
  dual-source handle and drains source-specific BookKeeper pins before its native deletion CAS;
- Q5 / `V2-OPEN-OBJ-15` →
  [ADR 0046](../../decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md): mandatory
  AES-256-GCM/HKDF-SHA-256 v1 uses one wrapped run key, derived Object keys, and disjoint directory/frame nonce domains;
- Q6 / `V2-OPEN-OBJ-16` →
  [ADR 0047](../../decisions/0047-v2-walrun-root-seal-and-successor-publication.md): immutable Root and Seal records
  live in Cell control metadata and one exact pointer CAS publishes the successor;
- Q7 / `V2-OPEN-PUL-OBJ-07` →
  [ADR 0048](../../decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md): 0.2 forbids every slice geometry
  change or second slice and fails closed at exhaustion.

Implementation and executable evidence remain NotStarted.
