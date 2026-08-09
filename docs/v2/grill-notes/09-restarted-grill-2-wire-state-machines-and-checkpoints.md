---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 7: wire, state machines, and checkpoints

Date: 2026-08-09

The user confirmed every round-6 recommendation. ADRs 0042 through 0048 now own those accepted contracts. This record
contains only the next independent decision frontier. None of the recommendations below is normative until the user
explicitly confirms it.

## Source facts used for recommendations

- Kafka research checkout `76f62f3b83e882105219b6c7687dbde594a8b8a2` currently defines metadata record IDs
  `0..28`; metadata type/version values are unsigned varints decoded into positive Java `short` values and therefore
  cannot exceed 32,767. `TopicImage.write` emits `TopicRecord` then partitions, `TopicsDelta` owns create/remove, and
  `MetadataDelta`/`MetadataBatchLoader` require explicit cases for every generated record type.
- The pinned Oxia 0.9.0 client hard-codes `maxBatchSize=131072` bytes. Its public API has single-key versioned puts and
  generated sequence keys, but no all-conditions-or-no-writes multi-key transaction. The local capability spike proves
  fixed-width sequence suffixes and the absence of a public multi-key conditional API. A one-record registry must
  therefore stay materially below 128 KiB including request overhead.
- Pulsar research checkout `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` caches one selected `ReadHandle` per
  ledger. Native source trim marks `bookkeeperDeleted=true` before physical deletion and only logs a later delete
  failure; it has no attempt-scoped persisted physical-delete intent/fact state.
- Stock jcloud blocks use a 128-byte header plus repeated entry headers/payloads and padding. The broker default
  `maxMessageSize` is 5 MiB but is configurable as an `int`; a V2 data-format cap must be checked against the exact
  native entry limit at profile admission rather than silently reducing supported message size.
- ADR 0046 fixes the NWG1 algorithm family but not byte framing. ADR 0047 fixes Root/Seal/pointer publication, while
  ADRs 0030 and 0039 still require strong LIST for an ACKed open tail when asynchronous checkpoint coverage is absent
  or invalid.
- The reserved virtual-ledger domain with `k=40` yields 1,099,511,627,776 IDs per Cell and 4,194,303 full numeric
  slices. Numeric capacity is therefore not the limiting factor if the permanent single-record registry is capped far
  below the Oxia batch limit.

These are pinned-source capabilities and constraints, not V2 implementation evidence.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-KAF-META-03` |
| Q2 | `V2-OPEN-PUL-META-02` |
| Q3 | `V2-OPEN-BK-11` |
| Q4 | `V2-OPEN-BK-12` |
| Q5 | `V2-OPEN-OBJ-17` |
| Q6 | `V2-OPEN-OBJ-18` |
| Q7 | `V2-OPEN-PUL-OBJ-08` |
| Q8 | `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **Kafka aggregate API key and generated wire**: Should the fork take the next upstream-sequential metadata
ID, reserve an extension band, or encode the aggregate inside another record? Which layer rejects an incomplete final
image without rejecting the transient replay state inside one atomic batch?

➡️ Recommend reserving the high positive-short band `32000..32767` for Nereus KRaft extensions and assigning
`TopicBindingAggregateRecord.apiKey=32000`, `validVersions=0`, non-flexible wire v0. The generated JSON record contains
explicit fields for topic ID/canonical name, aggregate schema version, 32-byte binding/epoch IDs and back-reference,
Cell/protocol identity, every closed binding/epoch discriminator and version, ordinal zero, profile/origin, and nullable
sealed end; it contains no aggregate blob, attributes map, lifecycle state, or retry field. Fixed SHA IDs are exactly 32
bytes, enum/version values are fixed-width, and every canonical name/topic-ID/back-reference is cross-checked.
`TopicDelta` accumulates the record during replay; completed controller-batch `apply` and snapshot `finishSnapshot`
perform the exactly-one final validation after the finalized feature image is known. Controller replay, image writer,
JSON tools/redactor, and metadata dump all get explicit generated cases. The tradeoff is a permanent fork-reserved ID
band and a deliberately strict v0 that requires a new record version for evolution, in return for low upstream ID
collision risk, inspectable fields, and no opaque compatibility bypass.

❓ **Q2** - **Pulsar selector create/delete/recreate state machine**: With selector and aggregate on separate Oxia keys,
how does creation remain fail-closed without claiming a transaction that Oxia does not provide?

➡️ Recommend bounded `PulsarTopicGenerationSelectorV1` states
`RESERVED -> ACTIVE -> DELETING -> DELETED`, with only exact forward CAS transitions. First use is
`putIfAbsent(RESERVED(1, aggregateKey, aggregateSha))`; recreation is
`CAS(DELETED(g) -> RESERVED(g+1,...))`, with overflow rejected. The creator then `putIfAbsent`s the exact immutable
aggregate and CASes the same reservation to ACTIVE. Open/append/read admission requires ACTIVE plus exact selector /
aggregate identity; RESERVED is a selector recovery state, not aggregate `CREATING`, and is never a visible topic.
Delete first CASes ACTIVE→DELETING to fence authority, completes native topic deletion, then CASes DELETING→DELETED;
old readable physical data and the old aggregate may retire later. Every lost response rereads the exact key/value and
either resumes the one legal next step or fails on a conflicting winner. The incarnation record later becomes
`RetiredTopicIncarnationTombstoneV1` by the ADR 0043 same-key CAS, never deletion. The tradeoff is three low-frequency
selector/aggregate operations on create and two selector transitions on delete, in return for explicit partial-state
recovery and no false multi-key atomicity or same-name ABA.

❓ **Q3** - **NPD1 exact block wire, limits, codec, and crypto**: How should NPD1 remain independently range-readable
without fixing a hard block cap below Pulsar's configured native entry size?

➡️ Recommend big-endian NPD1 v1 with a 32-byte data-object header, followed by blocks with a fixed 64-byte `NPB1`
header and a canonical 24-byte row per entry (`entryId`, decoded offset, length, flags). NPO1 remains the external block
range/SHA authority. Hard limits are 65,536 blocks, 65,536 entries per block, a 2 MiB directory, and encoded/decoded
block lengths below `2^31`; the persisted attempt-specific max must cover the broker's exact admitted native entry size
plus envelope overhead or `BOOKKEEPER_WAL_ASYNC_OBJECT` admission fails before writing. An 8 MiB decoded target is
policy, not format identity; a larger admitted entry receives one dedicated block. The only v1 codecs are `NONE` and
`ZSTD`; use ZSTD only when the encoded result is smaller. One random attempt key is KMS-wrapped once in NPO1 ATTEMPT
facts, HKDF derives a block key by attempt UUID+ordinal, and AES-256-GCM uses a fixed domain+ordinal nonce over the
directory and compressed payload. The tradeoff is one KMS envelope per offload attempt and whole-block authentication/
decode for a single entry, in return for native-size-aware admission, bounded parsing, compression, and independent
block proof without per-block KMS calls.

❓ **Q4** - **Persisted BookKeeper physical-delete intent and fact**: After the native deletion cut makes reads
Object-only, how does restart distinguish “delete not issued”, “issued but response lost”, and “confirmed absent”?

➡️ Recommend replacing an unqualified boolean transition with an attempt-scoped native metadata state
`BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`; `bookkeeperDeleted` is true exactly for INTENT/DONE and
inconsistent combinations fail closed. After ADR 0045 pin drain and final Object revalidation, one CAS writes INTENT
plus deterministic delete operation ID derived from ledger ID, attempt UUID, and NPO1 root SHA. Reads become Object-only
at that CAS. The executor performs idempotent BookKeeper deletion; success or native `NoSuchLedger` proof CASes the same
INTENT to DONE. Restart always retries/reconciles INTENT and never makes BookKeeper eligible again; permanent failure
retains an alerting physical residue but cannot roll logical state back. Ledger/offload retirement requires DONE. The
tradeoff is one extra native metadata write and potentially long-lived leaked physical bytes during an outage, in
return for deterministic restart convergence without double authority or silent delete failure.

❓ **Q5** - **NWG1 exact cryptographic framing**: Which bytes enter HKDF, nonce, and AAD, and how can the directory be
range-read before any frame ciphertext is trusted?

➡️ Recommend a fixed 256-byte big-endian clear NWG1 v1 header containing bounded counts/offsets/lengths, hashed Cell /
shard/run/session identities, run epoch/sequence, Root SHA-256, wrapped-key-envelope SHA-256, algorithm IDs, and a
CRC32C computed with its CRC field zero. The encrypted `BindingContextTable + AppendUnitDirectory` AEAD unit follows
immediately; frame ciphertext ranges follow without padding. Derive a 32-byte Object key with
`HKDF-Extract(salt=rootSha256, IKM=walRunDataKey)` then `HKDF-Expand` over a length-framed UTF-8
`nereus/nwg1/object-key/v1` domain plus Cell/shard/run-epoch/sequence. A 96-bit nonce is `uint32 domain || uint64
ordinal`, big-endian: directory domain `0x4e444952` (`NDIR`) at ordinal zero and frame domain `0x4e46524d` (`NFRM`) at
the corresponding frame ordinal. Directory AAD is the finalized header; frame AAD is the header SHA, Root/envelope SHAs,
and its canonical authenticated directory row. All limits are checked before KMS/decryption and golden vectors freeze
header CRC, HKDF, nonce, AAD, tag, decompression, and payload CRC ordering. The tradeoff is a fixed 256-byte header and
major-version bump for incompatible growth, in return for exact cross-language crypto, one initial directory range GET,
and no trust in unauthenticated frame bounds.

❓ **Q6** - **WalRun checkpoint pages and open-tail handoff**: Are checkpoint pages merely hints forever, or can a
completed sealed run use them as its canonical inventory without moving metadata publication into the ACK path?

➡️ Recommend immutable control-metadata `WalRunCheckpointPageV1` values, each covering at most 256 contiguous extents
and 64 KiB canonical bytes. A page binds Root SHA, page ordinal, exact sequence interval, predecessor-page SHA, every
content-addressed extent descriptor, and per-binding typed coverage. Pages and one CAS checkpoint head publish
asynchronously after ACK; an open-run recovery validates the contiguous page chain, then still uses bounded strong LIST
for the uncovered tail, and falls back to full bounded LIST if the chain is missing or invalid. Rollover flushes a final
gap-free page and `WalRunSealRecord` binds the final checkpoint-head SHA before the successor pointer advances. Only
that sealed page chain becomes the canonical extent inventory; provider bytes/digests still require validation. Handoff
may use the validated head to reduce work but cannot skip LIST for an uncovered ACKed tail. The tradeoff is asynchronous
metadata volume plus seal delay, in return for fast normal takeover and complete sealed inventory without per-group ACK
metadata I/O. Retirement frontier and GC ordering remain a later descendant of this checkpoint decision.

❓ **Q7** - **Virtual-ledger exponent and registry lifetime caps**: What fixed geometry fits both practical lifetime
rollover and Oxia 0.9.0's 128 KiB batch ceiling?

➡️ Recommend deployment-format constant `k=40`: each Cell receives `2^40` IDs and numeric space permits 4,194,303
full slices. Freeze canonical `maxRegistryBytes=64 KiB`, `maxAssignmentsEver=256`, and a fixed maximum 192-byte
assignment row; all headers plus 256 rows must fit the byte cap, and every retired row counts forever. Registry writes
are rejected before either count or canonical-byte limit, leaving large safety margin below Oxia's 131,072-byte batch
limit. At even 1,000 virtual-ledger rollovers/second, one Cell's numeric slice lasts about 34.8 years; deployments needing
more than 256 lifetime Protocol Cells require a new reservation domain/deployment rather than mutating this registry.
The tradeoff is a deliberately finite 0.2 deployment lifetime and substantial unused numeric space, in return for a
small single-CAS record, simple capacity math, and no provider-limit ambiguity.

❓ **Q8** - **Virtual-ledger allocation and Ledger Chain head publication**: How does a Cell allocate a globally unique
ID and publish one ManagedLedger head after response loss without a multi-key transaction or per-entry metadata?

➡️ Recommend one Cell-scoped, single-key CAS `VirtualLedgerAllocatorRecordV1` with phases `IDLE` and `RESERVED`. An
IDLE→RESERVED CAS stores and burns the candidate ID, increments `nextLedgerId`, and records a deterministic request ID
derived from ManagedLedger incarnation, expected head/node SHA, and next ledger ordinal. While RESERVED is unresolved,
that Cell serializes further rollovers. Recovery then `putIfAbsent`s one immutable `VirtualLedgerNodeRecord` at the
candidate ID and CASes that ManagedLedger's `VirtualLedgerHeadRecord` from the exact predecessor to the new node. Only
after head publication may entry zero be allocated; the allocator then CAS-clears the reservation. Any conflict leaves
the candidate as a permanent gap/unreferenced receipt, never a chain member. Every uncertain write converges by exact
reread; no step is retried under a different request ID, and normal entry append performs zero metadata I/O. The
tradeoff is serialized Cell-level ledger rollovers and three control-plane writes per successful rollover, in return for
bounded single-key recovery, explicit chain order, permitted gaps, and no dependence on an unbounded sequence scan.

## Deferred descendants

- Kafka byte-level schema/golden vectors and exact fork hook locations become implementation work after Q1. Selector
  retirement-receipt fields and exhaustive reference proof depend on Q2 plus physical-delete/virtual-ledger lifecycle.
- NPO1 section field offsets for the NPD1 envelope, range coalescing, and codec/crypto golden vectors depend on Q3.
  Delete retry policy, operator repair, and residue metrics depend on Q4.
- NWG1 directory/frame row offsets, KMS cache erasure lifecycle, and crypto golden vectors depend on Q5. WalRun
  retirement frontier, compact tombstone, and provider/metadata GC order depend on Q6.
- Registry wire rows depend on Q7. `RETIRING -> RETIRED` proof, allocator takeover, chain trim/compaction, cursor
  recovery, replication, and transaction behavior depend on Q8.
- `V2-OPEN-OBJ-01`, `V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01` remain executable evidence gates rather than prose
  decisions. KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 7 recommendation above has been accepted. Confirmed answers must be synchronized to new ADRs and normative
contracts before any descendant becomes the next frontier.
