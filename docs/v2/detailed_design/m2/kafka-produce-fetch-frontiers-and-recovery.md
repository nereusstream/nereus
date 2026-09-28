---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: DocumentationOnly
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M2 Kafka Produce/Fetch frontiers and protocol recovery design

## Delivery boundary

This design implements [ADR 0087](../../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) on
top of [the ADR 0086 run/range-index design](kafka-bookkeeper-offset-range-index.md), under
[the M2-K0 implementation-input closure](kafka-m2-k0-implementation-input-closure.md). M2 owns the engine primitives
and fault harness; M6 connects them to Kafka leadership, replicas, purgatory, errors, transactions, and broker lifecycle.
M3 implements the Object-WAL `NWKCP1` carrier while keeping physical checkpoint/Seal unchanged. M4/M5 connect
immutable read generations and source retirement. Existing V1 partition storage, reservation,
producer-recovery, and leader-epoch code is evidence only and is replaced rather than wrapped or dual-written.

The current normative contract is amended by [NSIP-1](../../NSIP/nsip-1.md) and remains `InProgress`.
`DocumentationOnly` means this revised contract has no current-source full acceptance receipt; the focused implementation
results below do not promote it. The [historical M2 Final receipt](../../evidence/v2-m2/kafka/k10/kafka-final.json)
binds the earlier contract and source and does not certify NSIP-1 changes.
The NSIP-1 BK first batch implements coherent shared commit, closed-history recovery and the native Controller/Broker
activation path. Native transaction, disconnected-response retry, group-offset and superseded-recovery evidence is
focused; the normal final-source real-cluster task passes. The component contracts below cover both this implemented BK
slice and later profile/checkpoint/read-generation work:

1. partition frontier state and one coherent publication cell;
2. pre-offset admission plus speculative producer/transaction deltas;
3. profile durability resolution and ordered multi-state commit;
4. shared BK commit plus closed Owner admission, exact run discovery and cold takeover;
5. profile-neutral producer, transaction, range-index, and leader-epoch checkpoint vector;
6. immutable Fetch capture, isolation bounds, delayed-wakeup seam, and compaction-gap lookup;
7. random targeted reads, sequential cursor validation, and source-generation pinning;
8. deterministic failure cuts followed by real BookKeeper and native Kafka integration evidence.

## Partition state model

The owner keeps one partition-local state root. Logical fields are shown below; implementation may pack hot fields and
use immutable references, but it may not split publication into independently visible authorities.

```text
KafkaPartitionProtocolStateV1
  bindingId
  topicIncarnation
  partitionId
  bindingGeneration
  storageEpochId
  ownerEpoch
  kafkaLeaderEpoch
  stateVersion
  trimStartOffset
  allocatedEndOffset
  durableEndOffset
  readableEndOffset             // Kafka LEO
  highWatermark
  lastStableOffset
  runTableRef
  activeTailRef
  sourceMapRef
  committedProducerStateRef
  speculativeProducerQueueRef
  transactionIndexRef
  leaderEpochIndexRef
  checkpointVectorRef
```

`stateVersion` and the frontier tuple are captured coherently through a single immutable reference or seqlock-backed
publication cell. A reader may not combine the LEO from one version with the transaction index or Source Map from
another. Large maps are immutable/versioned references; capture does not copy them.

Checked validation enforces:

```text
trimStart <= LSO <= HW <= LEO/readable <= durable <= allocated
```

The component checkpoint vector is:

```text
KafkaRecoveryCheckpointVectorV1
  rangeIndexCoveredThrough
  producerStateCoveredThrough
  txnIndexCoveredThrough
  leaderEpochCoveredThrough
  compatibleRunIdentity
  bindingId
  storageEpochId
  creatorOwnerEpoch
  kafkaLeaderEpochAtWrite
```

Only a single compound checkpoint or the minimum mutually compatible component boundary seeds suffix recovery.

## Pre-offset admission

The admission method is side-effect ordered:

```text
validate request/batches/fences/producer/transaction
  -> reserve tracker + locator + request bytes + pending entries + provider permits
  -> allocate Kafka Offset Range
  -> reserve contiguous DATA entry IDs / Object group membership
  -> install PendingCommitSet and speculative protocol deltas
  -> submit storage I/O
```

One reservation object owns every permit and either transfers them into the commit set or releases them. Failure before
offset allocation consumes no position. Failure after allocation enters the ordered resolver; arbitrary cancellation
cannot delete the slot and let a successor publish around it.

Logical offset count is checked `lastOffsetDelta + 1` from assigned Kafka batch headers. Record count is never used.
Every batch in one partition storage append is included in one `KafkaAppendCommitSet` with one Kafka range.

## Producer and storage identities

The engine exposes separate closed identities:

```text
KafkaBatchDuplicateIdentityV1
  producerId
  producerEpoch
  baseSequence
  lastSequence

KafkaCommitSetIdentityVectorV1
  orderedBatchIdentities[]

IngressRequestDigestV1             // ephemeral; same explicit in-process request only
StoredAssignedBatchDigestV1        // final assigned bytes; storage recovery only

StorageAttemptIdentityV1
  appendAttemptId
  assignedOffsetRange
  storedAssignedBatchDigest
  physicalExtentIdentity
```

The native duplicate identity is per RecordBatch; one commit set binds an ordered vector and may not assume a single
producer ID. Admission consults a producer-state view composed from committed state and ordered speculative deltas.
After native parsing/CRC/error-precedence checks, the pending partition-subrequest table returns exactly one of:

- `NEW_VALID`: allocate and submit;
- `PENDING_SAME_REQUEST_INSTANCE`: join only the same explicit in-process request future;
- `NATIVE_COMMITTED_DUPLICATE`: return the native prior result for matching PID/epoch/sequence;
- `OUT_OF_ORDER_SEQUENCE` or another native Kafka rejection;
- `INDETERMINATE`: fail closed until recovery resolves the predecessor.

Nereus never returns a new payload-digest conflict for a native duplicate. `IngressRequestDigestV1` cannot provide
protocol deduplication and only guards the same request-instance join. `StoredAssignedBatchDigestV1` is calculated
after broker offset/leader-epoch/timestamp/header/CRC rewrites and is used only to reconcile storage response loss.
`NO_PRODUCER_ID` requests have no protocol retry guarantee; equal bytes do not return an earlier offset and a network
retry may append again. A new internal attempt ID never changes the native producer-state result.

## Storage completion and ordered commit queue

Each pending slot progresses locally through closed states such as:

```text
ADMITTED -> IO_IN_FLIGHT -> DURABLE_EXACT -> PUBLISHED
                         -> DEFINITIVE_FAILURE
                         -> INDETERMINATE
```

State names are implementation-local; the semantic cuts are not. Storage callbacks validate exact run/Object,
entry/group, identity, length, digest, owner/storage fence, and profile proof. Completion order may differ from slot
order. Only the queue head may publish.

One serialized publication applies the complete delta:

```text
hidden locator ranges
+ committed producer delta
+ transaction/open-abort-complete delta
+ leader-epoch observation
+ append result/original offsets
+ durable/readable frontier
```

Before replacement, the publisher compares exact Binding/incarnation/generation, Storage Epoch, Owner Epoch, Kafka
leader epoch, and predecessor `stateVersion` under one state-root CAS or equivalent serialized partition lock. Owner,
leadership, and Storage-Epoch transitions compete on this same cut. Only a successful fenced replacement makes the
delta visible; then it wakes local Fetch/replica waiters. An optional response-time fence check decides success versus
outcome-unknown/native fenced response, but does not retroactively invalidate a legal publication. A crash cannot
expose a locator/frontier while retaining old producer or transaction state, and a stale callback cannot publish at
all.

If the head definitively fails, the writer fences new admission and resolves every successor as inert physical tail or
unacknowledged protocol work. It never publishes a later range. If the head is outcome-unknown, the queue remains
blocked under a hard age/count/bytes deadline until exact reconciliation or run fencing/recovery.

## Profile durability resolver

The ordered queue consumes a closed `ProfileDurabilityProof` whose type is fixed by the Storage Epoch:

- `OBJECT_WAL`: exact provider-resolved immutable Object/group identity plus complete commit-set membership and the
  selected per-Binding/partition durable authorization receipt;
- `BOOKKEEPER_WAL_ONLY`: every DATA member and terminal descriptor at BookKeeper quorum;
- `BOOKKEEPER_WAL_ASYNC_OBJECT`: the same BookKeeper proof; Object materialization is not in the Produce ACK cut.

BK resolution performs no per-append Oxia/KRaft mutation. Object performs the synchronous authorization Head CAS
after exact upload; asynchronous physical checkpoint pages and long-lived manifest handoff cannot substitute for it.
The local readable publication is still mandatory.

## Acknowledgement and shared commit

Kafka RF=1/minISR=1 is the current contract. `acks=0` sends no response; `acks=1` and `acks=all` wait for
complete profile durability, legal qualification and coherent protocol publication. BK ordered commit advances
Durable/Readable/HW to the same continuous commit end, with LSO calculated from the transaction state in that root.
Allocated remains separate while successors wait. UNKNOWN/failed predecessors fence later publication; a later
physical completion cannot cross the unresolved gap. The implemented BK in-flight duplicate path shares the original
assigned commit/result; recovery reconstructs the bounded completed duplicate results.

`OxiaKafkaRunRootAuthorityV2` now uses one native Owner head to order run attachment and closure. The old mandatory
Observation Journal/Applied eligibility does not qualify BK commit or cold takeover. GC/native physical tickets,
reader/source protection and compatible checkpoint/tail recovery retain their separate duties. Ordinary BK append
has no per-batch remote control mutation. Object uses NSIP-1 section 6.4's synchronous per-Binding/partition Head CAS
after exact authenticated upload; the same key orders grant, common checkpoint selection and Owner closure. Tracker
and coherent publication require its exact grant receipt. Physical existence/LIST cannot extend the closed prefix.
Object tests establish recovered producer duplicate state and exact authorization-candidate retry reuse. The native
Object Produce duplicate-return path is not implemented or validated in this core batch.

## Transaction and leader-epoch state

Partition transaction state includes ongoing transactions, first unstable offset, completed/aborted ranges, control
markers, and the aborted-transaction index used to construct native `read_committed` Fetch responses. It advances only in the ordered publication cut. Cross-
partition commit/abort remains Transaction Coordinator authority.

The leader-epoch index stores Kafka `leaderEpoch -> startOffset`. Owner Epoch and Storage Epoch never substitute.
`OffsetForLeaderEpoch`, follower truncation, leader failover, and client epoch validation use the Kafka index.

A logical `KafkaProtocolCheckpointStore` publishes producer, transaction/aborted, and leader-epoch components and
recovers one compatible vector. BookKeeper implements it with `NBKE2` control entries inserted only between complete
commit sets by the ledger-entry sequencer. Object WAL implements it with a distinct bounded content-addressed
`NWKCP1` protocol-checkpoint object family under an exact WalRun Root sub-prefix. Each Object row binds Binding /
incarnation, partition, Storage Epoch, Owner Epoch, Kafka leader epoch, covered-through, producer state, transaction /
aborted state, and leader-epoch index. Rows may be batched only under bounded canonical directory and recovery limits.

The Object physical extent checkpoint pages and physical Seal stay physical-only; `NWKCP1` cannot authorize ACK,
physical-recovery omission, frontier advance, source protection release, or GC. Missing/corrupt checkpoint state falls
back to bounded NWG1 suffix replay. Checkpoint cadence is operational, but aggregate uncovered entries/bytes/age/time
and a terminal compatible vector at run close are hard contracts.

Object WAL selects checkpoints through one independent Root-bound `KafkaProtocolCheckpointHeadV1`. Its logical state
contains Root identity, fenced publisher epoch, `OPEN|TERMINAL`, ordinal, predecessor digest, exact checkpoint object
key/length/digest, and the covered-through vector. Publication is:

```text
conditional-create content-addressed NWKCP1
  -> complete object verification
  -> CAS Head from exact predecessor and publisher epoch
```

Ordinals advance by one, vectors never regress, one publisher has at most one unresolved candidate, and unknown
responses converge by exact object/Head reread. Takeover changes only publisher epoch while preserving the selected
Head. After admission stops and the final vector exists, `OPEN -> TERMINAL` is an irreversible same-Head CAS. A
successor Root binds the exact terminal Head key/canonical-value digest separately from the physical Root/Seal lineage.
LIST discovery alone never selects a checkpoint or proves terminal closure.

Selected NWKCP1 Objects and the Head remain while any successor, manifest, recovery, retention, or source dependency
references the run. Checkpoint deletion cannot precede the WAL/source required by its replay semantics. Unselected
content-addressed residue needs bounded authoritative non-reference proof. Exact wire, vector/key caps, and backend
mapping are M3 outputs; this lifecycle authority does not enter append ACK and never authorizes source GC.

## Takeover recovery

The current BK request binds a closed `KafkaOwnerAdmissionV1`, the exact selected `KafkaRunRootRecordV2`, the
ledger handle and a new native fence. It validates scope/membership/identity before I/O. The caller validates the
current Controller assignment, closes old run admission, discovers the entire fixed legal set, then fences every
ledger that can still accept old writes. Attachment and closure use the same Owner-head CAS; response loss cannot
hide a legally admitted root. Immutable closure archives preserve the set across repeated takeover.

`KafkaBookKeeperTakeoverRecoveryV1` uses the recovered ledger's LAC, validates compatible compound checkpoint or
RUN_HEADER, and scans the complete suffix in physical order within the cumulative entries/bytes/time envelope.
It accepts only complete contiguous commit sets with exact member identity/digest and reconstructable protocol
state. Partial groups/gaps stop adoption. Sealed root/end conflicts fail closed. Complete unacknowledged batches
inside the legal history are adopted, preserving original duplicate results after response loss. A predecessor
protocol state may seed the next run at the same covered boundary; it is rebound to that run without changing
producer/transaction/leader-epoch state. Metadata closure alone is not BK fencing.

The installed BK HW equals the recovered legal prefix end; LSO is derived from recovered transaction state.
There is no Follower Applied/election-adoptable cap. Fresh run qualification and native generation revalidation
must precede ready. Recovery locators, full multi-run composition, obsolete callback rejection, and native
Controller/Broker acceptance are still being integrated; the local kernel alone is not that acceptance.

## Fetch capture

The logical capture is:

```text
KafkaReadSnapshotV1
  binding/incarnation/generation
  ownerEpoch
  kafkaLeaderEpoch
  storageEpochId
  stateVersion
  runTableRef
  activeTailRef
  sourceMapRef
  logStartOffset
  logEndOffset                 // readableEndOffset
  highWatermark
  lastStableOffset
  committedProducerStateRef
  transactionIndexRef
  leaderEpochIndexRef
  sourceProtectionGeneration
  readViewPin
```

It uses the ADR 0069/0070 allocation-free generation pin/hazard contract for one partition read batch. No remote
metadata read occurs. Replica/read-uncommitted/read-committed upper bounds select LEO/HW/LSO respectively. The read
planner refuses any locator whose coverage crosses the captured bound.

For `read_committed`, the source reader returns protocol-native batches through LSO plus the native aborted-
transactions metadata needed by Kafka Fetch response construction. It does not silently remove aborted DATA/control
batches as a storage-only filter.

## Delayed Fetch

M2 exposes frontier-versioned waiter registration; M6 adapts it to Kafka delayed-operation purgatory. A waiter binds
partition, isolation, requested offset, minimum bytes, deadline, captured leader/state version, and cancellation.
Registration and frontier publication use a lost-wakeup-safe sequence:

```text
capture state/version
  -> evaluate
  -> register against exact version
  -> re-evaluate version/frontier
  -> sleep only if unchanged and insufficient
```

LEO wakes replica waiters, HW wakes read-uncommitted waiters, and LSO wakes read-committed waiters. Log-start movement,
leader/owner change, read-view/source change, offline/delete, and timeout wake every affected class. Wakeup only
re-evaluates local state; it never polls BookKeeper, Object Storage, or Oxia.

## Locator lookup and read planning

The lookup kernel is coverage-aware:

```text
candidate = floor(requestedOffset)
if candidate != null && candidate.lastOffset >= requestedOffset:
    use candidate
else:
    candidate = successor(requestedOffset) across block/run
```

It rejects a candidate beyond the snapshot upper bound and returns offset-out-of-range against the captured Log Start /
end rules. Locator coverage is checked against the raw assigned RecordBatch header. A complete batch deleted by
compaction creates a gap and is skipped via successor; sparse offsets inside a surviving batch remain covered by
`lastOffsetDelta + 1`.

Random reads target the one DATA entry. Sequential reads may use a packed cursor:

```text
run identity + source generation + index block identity + locator ordinal
+ next entry ID + next Kafka offset + snapshot state version
```

Every new Fetch captures and pins a fresh snapshot, then accepts the cursor only on exact identity/version agreement.
The cursor is discarded on compaction, trim, source generation, leader/owner, run, or index change. It never pins a
generation across requests. Adjacent entries are coalesced by byte budget, not fixed entry count. A RecordBatch is
never split; native first-oversized-batch behavior applies.

The compactor is not a byte-preserving materializer. It may remove part of a batch, create a sparse/empty batch or new
batch boundaries, and rewrite CRC/timestamp fields, but it preserves logical offsets, producer sequence recovery,
control markers/coordinator epochs, transaction and aborted-range semantics, tombstone retention, and native
`ListOffsets`/timestamp behavior. Every compacted generation rebuilds its range, producer, transaction/aborted,
leader-epoch, and timestamp indexes. Whole-batch deletion continues to use floor+coverage+successor.

## Object materialization and pinned source plans

One captured Fetch may plan an Object prefix and BookKeeper tail, or another set of non-overlapping Source Map ranges.
Generation change after capture does not alter that plan. Each atomic commit set and each whole-range fallback is
source-pure. Existing ADR 0069/0071-0080 pin, protection, handoff, and GC contracts apply.

Non-compacting materialization must reproduce raw RecordBatch bytes and verified range, producer,
transaction/aborted, and leader-epoch indexes before publishing the successor generation. A compaction generation is
the protocol-semantic rewrite defined above and is not required to preserve exact raw bytes.
`ObjectMaterializedFrontier` is a routing optimization only.

## Internal-topic initial policy

The versioned Kafka internal-topic Deployment policy resolves `__consumer_offsets` and `__transaction_state` to
`BOOKKEEPER_WAL_ONLY` in 0.2. They do not inherit tenant defaults. `__share_group_state` remains fail-closed until its
own explicit policy is frozen; this design does not silently infer one. Changing either selected topic to async Object
requires a later policy version plus compaction, checkpoint, coordinator restart, fallback, BK-GC, and marker-parity
evidence.

## Implementation cuts

These cuts start only after non-promotable `v2M2KafkaInputsCheck` proves the K0 inputs. Recommended reviewable cuts are:

1. frontier/value validators and coherent publication cell;
2. producer identity, speculative delta queue, duplicate joining, and deterministic unit cuts;
3. profile durability proof and ordered multi-state commit queue;
4. transaction/leader-epoch indexes plus checkpoint vector and bounded recovery;
5. Fetch snapshot/isolation, coverage-aware floor+successor, and targeted/sequential reader;
6. lost-wakeup-safe delayed-Fetch seam;
7. closed native Owner admission, complete legal run discovery, shared HW/LSO and cold takeover;
8. real BookKeeper plus native Kafka Produce/Fetch/transaction/failover integration;
9. performance/scale receipts and remaining obsolete library removal. The native runtime/checkpoint sources replaced
   by the first BK slice are already removed; unconnected historical replication fixtures do not provide a product mode.

BK append adds no per-append control metadata; Object requires durable grant metadata after upload. Neither profile
adds dual-write compatibility, a second offset authority, or a second assignment authority.

## Evidence gates

`V2-KAF-DATA-001..022` are mandatory. M2 receipts measure at least Produce p50/p99, pipeline depth, allocation bytes,
duplicate-join cost, active-tail/index bytes, recovery entries/bytes/time, targeted/random and sequential read
amplification, admission/closure cost, waiter registration/wakeup cost, and zero normal-path metadata
calls. M6 receipts add native Kafka error
codes, RF=1/minISR=1/HW behavior, transaction coordinator/control-marker flows, full client retries, broker restart,
leader transfer, Fetch purgatory, and comparison with the pinned Kafka baseline.

An M2 receipt may promote only rows whose milestone is exactly M2. Mixed M2/M3/M4/M5/M6 rows remain `PLANNED` until
every named owner supplies evidence; `v2M2KafkaFinalCheck` is not the global `v2M2Check`.

Persisted encodings and parser caps froze through K0 before their first durable use. K9 selected the operational queue,
checkpoint, cursor, lag, and performance defaults in the canonical defaults projection consumed by Kafka Final. Topic
policy cannot enlarge hard correctness/recovery bounds; Cell/host pressure may backpressure or seal early without
changing persisted bytes or Kafka visibility semantics.
