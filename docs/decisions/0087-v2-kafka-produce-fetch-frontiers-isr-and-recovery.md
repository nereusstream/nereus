# ADR 0087: V2 Kafka Produce/Fetch frontiers, single-owner shared commit, and protocol recovery

## Status

Accepted, amended by [NSIP-1](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/NSIP/nsip-1.md).
The current Kafka contract is RF=1/minISR=1, one native Controller-selected Owner, shared durable commit,
and cold takeover from closed legal history. This revision replaces the previous mandatory
Observation Journal/Applied/election-adoption contract; it does not introduce a second product mode.
Existing M1–M4 receipts remain historical evidence for their original source and semantics.

BK first-batch implementation has Owner-head admission/closure, shared HW/LSO, duplicate joins and ordered
completion. Real BK/Oxia tests cover closed multi-run native transaction replay, repeated history, crash-run successor
writes, incomplete terminal groups and rejection of the old fenced writer. An actual 3-Broker/1-Controller case covers
ongoing transaction/COMMIT, disconnected Produce response and same-offset native idempotent retry, durable group
offset, recovery superseded by another assignment, and two cold sole-replica relocations with continued
Produce/read_committed Fetch. The obsolete native V1 runtime/checkpoint dependencies have been removed; current
focused native tests, Checkstyle and SpotBugs pass. The normal dedicated native real-cluster task passes on the final
implementation; 94 stock Partition regressions also pass. Wider repository `check` is blocked by unpublished Pulsar
managed-ledger/testmocks 5.0.0-M1-SNAPSHOT inputs; this is not a whole-repository CI claim.
Object core commit authorization/closure is implemented through NSIP-1's per-Binding/partition Head CAS and verified
with eight real MinIO/Oxia cases, including shared-member closure, exact UNKNOWN retries and repeated cold
partition-state recovery. The native Object Broker profile remains fail-closed. Full Kafka compatibility and M5/M6
completion are not claimed.

## Context

ADR 0086 fixes the physical Kafka mapping:

```text
Kafka Offset Range
  -> BookKeeper run
  -> RangeIndexBlock
  -> BatchLocator
  -> complete raw Kafka RecordBatch
```

That mapping deliberately separates one partition append, one RecordBatch lookup unit, and one sealed ledger-run
lifecycle unit. It does not by itself define when data is the Kafka Log End Offset, High Watermark, or Last Stable
Offset. A single `committedEndOffset` would incorrectly collapse primary-WAL durability, owner-local readability,
legal shared commit, and transaction visibility.

BookKeeper quorum describes physical redundancy, not Kafka replica count. Kafka RF and minISR are both 1.
A complete durable commit set in legally admitted BK history may advance HW only through contiguous,
fenced coherent publication of locators and recoverable producer/transaction/leader-epoch state.
Physical completion alone is insufficient. KRaft remains the native assignment authority.

## Decision

### Partition frontier model

One Kafka Topic Partition owns these distinct half-open boundaries:

```text
KafkaPartitionFrontiers
  trimStartOffset
  allocatedEndOffset
  durableEndOffset
  readableEndOffset
  highWatermark
  lastStableOffset
```

Their meanings are:

- `trimStartOffset`: Kafka Log Start Offset and the first logically retained offset;
- `allocatedEndOffset`: next owner-local speculative offset after admission; it may include unresolved writes;
- `durableEndOffset`: greatest contiguous prefix with the selected profile's primary-WAL durability proof;
- `readableEndOffset`: greatest contiguous prefix whose locators, committed producer state, partition transaction
  state/index, and leader-epoch state were coherently published; this is Kafka LEO;
- `highWatermark`: end of the continuous, complete, legal shared commit prefix coherently published by the Owner;
- `lastStableOffset`: Kafka LSO derived from HW and the first unstable transaction offset.

The invariant is:

```text
trimStartOffset <= lastStableOffset <= highWatermark
                <= readableEndOffset <= durableEndOffset <= allocatedEndOffset
```

Offsets from different Topic Incarnations, partitions, or Position Domains are not comparable. `ownerEpoch`, Kafka
`leaderEpoch`, and `storageEpochId`/binding generation are separate fences and never substitute for one another.

Materialization and recovery checkpoint state do not enter this ordering chain:

- `ObjectMaterializedFrontier` is only a derived greatest contiguous prefix with a verified preferred Object source;
  exact source coverage remains in the pinned Source Map;
- checkpoint coverage is a component vector, not one ambiguous `checkpointEndOffset`:
  `rangeIndexCoveredThrough`, `producerStateCoveredThrough`, `txnIndexCoveredThrough`, and
  `leaderEpochCoveredThrough`;
- `recoveryCoveredThrough` is the minimum compatible component boundary unless one atomic compound checkpoint proves
  the same cut.

Neither materialization nor a checkpoint can advance LEO, HW, or LSO.

### Persisted Kafka leader-epoch boundary

Kafka leader epoch, Nereus Owner Epoch, and Storage Epoch are independent. A BookKeeper run is bound to exactly one
Kafka leader epoch; an epoch change stops/seals/reconciles the old ACTIVE run and opens a fresh run before admission,
even when the same broker remains owner. `KafkaBookKeeperRunV1`, `RUN_HEADER`, every `NBKE2 DATA`/commit descriptor,
range-index anchor, protocol checkpoint, and `RUN_FOOTER` bind or exactly derive that leader epoch.

An Object WalRun may mix partitions and therefore never stores one singular Kafka leader epoch in its Root. Each Kafka
append-unit directory row/context binds its exact partition and Kafka leader epoch, and every commit-set member must
cross-check it. The raw assigned RecordBatch leader-epoch field remains protocol-native authority where applicable.

### Produce admission and storage pipeline

For one partition subrequest, the owner performs:

1. native protocol parsing, authorization, quota, request-size, record-batch CRC, topic/partition, leader, Binding,
   Owner Epoch, and Storage Epoch validation;
2. Kafka producer ID/epoch/sequence and transaction validation;
3. reservation of completion tracker, active-tail locator, request bytes, BookKeeper pending entries, Object builder,
   and provider/concurrency capacity;
4. ordered assignment of one exact half-open Offset Range;
5. creation of one `KafkaAppendCommitSet` and one independent storage-attempt identity;
6. bounded overlapping BookKeeper/Object I/O in admission order;
7. exact profile durability resolution;
8. ordered publication through the greatest contiguous successful prefix;
9. one fence-protected partition-local coherent publication of locators, producer state, transaction state/index,
   leader-epoch state, append result, `durableEndOffset`, and `readableEndOffset`;
10. local Fetch-waiter wakeup and Kafka replica/HW progression;
11. completion according to the requested `acks` mode.

Capacity is reserved before offset assignment. A request rejected by size, quota, memory, entry count, builder space,
or provider queue consumes no offset. `allocatedEndOffset` is owner-local speculative state and is not mutated through
one remote metadata CAS per Produce.

Offset admission is serialized, storage I/O is bounded and concurrent, and protocol publication is ordered. If B and
C become durable while predecessor A remains unresolved, neither B nor C becomes readable, advances LEO, or returns a
success ACK. A definitive A failure or non-converging gap fences the writer/run; later physical entries are inert
orphan tail. Recovery publishes only the greatest gap-free prefix and starts fresh admission under a new fence/run.

Speculative offsets may be reused only after fenced recovery proves they were never readable, HW-covered, or otherwise
committed and atomically rolls back locator, producer, transaction, and leader-epoch speculative state. An ambiguous
outcome remains fail-closed and is never reused merely because the client timed out.

### Fenced coherent publication

Storage durability does not authorize a stale callback to publish Kafka state. Before anything becomes visible, the
ordered queue enters one partition publication cut and compares at least the exact Binding ID, Topic Incarnation,
Binding generation, Storage Epoch ID, Owner Epoch, Kafka leader epoch, and predecessor publication `stateVersion`.
The implementation uses either one state-root compare-and-set or one serialized publication lock with an equivalent
checked state-root replacement. Kafka leadership, ownership, and Storage-Epoch transitions compete on that same cut.

The linearization rules are:

- if the append publication wins first, the complete commit set legally belongs to the old leader epoch;
- if a fence/leadership transition wins first, the stale append cannot advance Durable/Readable/LEO, install protocol
  state, wake a Fetch/replica waiter, or produce a success ACK;
- a transition after successful publication does not undo legal old-epoch data. A final response-time fence check may
  withhold the response and report an outcome-unknown/native fenced error, but it cannot make the publication illegal.

Therefore `release-publish -> final fence check` is not the publication protocol. The mandatory order is `profile
durability -> fenced coherent publication -> waiter/progress notification -> optional response fence check -> ACK`.

### Kafka idempotency and speculative producer state

Kafka protocol idempotency and storage retry identity are different:

```text
KafkaBatchDuplicateIdentity = producerId + producerEpoch + baseSequence + lastSequence
StorageAttemptIdentity = appendAttemptId + assignedOffsetRange + storedAssignedBatchDigest + physicalExtentIdentity
```

The Kafka identity is per RecordBatch and is validated before assigning a new offset. Native Kafka producer-state
validation alone decides whether PID/epoch/sequence is a duplicate; Nereus does not add a payload-digest-based
`DUPLICATE_CONFLICT`. A native committed duplicate returns the original result/Offset Range even when a second valid
request carries different application payload bytes under the same native duplicate tuple. All native parsing, CRC,
epoch, sequence, and error-precedence rules still run. One multi-batch commit set binds the complete ordered native
identity/result vector; it does not assume all batches have one producer ID.

Two payload digests have deliberately narrower scopes:

- `IngressRequestDigest` covers canonical unassigned client bytes and may only protect joining the same explicit
  in-process request instance to its pending future. It never creates protocol-level deduplication;
- `StoredAssignedBatchDigest` covers final persisted bytes after broker-assigned offsets and any permitted timestamp,
  leader-epoch, header, or CRC rewrite. It is storage response-loss/recovery evidence only.

An internal storage attempt proven aborted and never visible may retry with a fresh attempt ID without changing the
Kafka request identity. A non-idempotent request with `NO_PRODUCER_ID` has no protocol duplicate guarantee: timeout
and retry may append twice, and equal payload digest never returns an earlier offset. A pending join for such a request
is legal only for the same explicit in-process request instance. Mixed duplicate/new-batch behavior follows the native
Kafka batch validator and cannot be collapsed into one synthetic producer or storage-digest rule.

One producer may have multiple admitted commit sets in flight. Admission validates a new sequence against committed
producer state plus ordered speculative producer deltas, not committed state alone. The same ordered publication cut
that makes bytes readable commits the corresponding producer and transaction deltas. It is forbidden to publish data
without producer state or producer state without data.

The raw assigned Kafka RecordBatch remains producer/sequence/transaction payload authority. `NBKE2 DATA` may repeat
selected fields for bounded recovery and defensive validation only when exact equality with the raw batch is required;
those repeated fields never become a second Kafka authority. A commit-set descriptor binds the full Offset Range,
member ordinals/count, physical DATA range, identities, epochs, and aggregate payload digest. The common single-batch
path may carry its descriptor in that DATA entry; no extra control entry is required solely for that case.

### `acks`, RF=1, and shared High Watermark

Kafka replication factor and min.insync.replicas must both resolve to 1. Explicit unsupported values are rejected
at topic creation, expansion/configuration and replica reassignment; they are not translated into BK quorum.
Broker defaults and `__consumer_offsets`/`__transaction_state` replication configuration follow the same rule.
Admin assignment/leader/ISR reports the real sole hosting Broker. A failed sole replica needs a native Controller
assignment change to a new eligible Broker; it cannot be elected merely because storage is shared.

- `acks=0` sends no Produce success response; its admitted work follows the same correctness path.
- `acks=1` and `acks=all` both await complete profile durability, legal commit qualification and coherent publication.
- For BK, the ordered publication advances readable/durable/HW to the same complete contiguous commit end.
  LSO is `min(HW, firstUnstableOffset)` from the transaction state in that publication.
- Later physical completions wait behind unresolved predecessors. A failed/UNKNOWN predecessor fences the pipeline;
  successors cannot independently publish. Recovery may adopt a complete legal prefix after the old history is closed.
- Timeout/response loss is UNKNOWN. In the implemented BK path, PID/epoch/sequence retries join the original in-flight result or return its
  recovered bounded duplicate result, without another offset allocation.

Mandatory follower observation, journaling and Applied progress no longer qualify BK ACK/HW or takeover.
Their old source-retention obligation is replaced by complete cold-recovery sources; reader pins, recovery roots,
transaction indexes and shared-member lifecycle protection remain required.

BK normal append adds no per-batch remote offset allocation or commit-metadata mutation. Low-frequency ownership
and run admission bind the data path to legal history. Object separately establishes its durable per-Binding grant
after exact physical persistence; necessary authorization I/O belongs to its ACK cost.

### Transaction visibility and recovery

A `KafkaAppendCommitSet` is partition-local. It does not make a Kafka transaction atomic across partitions. The
Transaction Coordinator decides commit/abort, and each involved partition persists native transactional batches plus
COMMIT/ABORT control batches.

Each partition maintains at least:

- producer epoch/sequence and recent-batch state;
- marker producer/coordinator epochs and last marker offset; native control batches have no producer sequence;
- ongoing transaction first offsets and first unstable offset;
- completed and aborted transaction ranges/control markers;
- the transaction index required by `read_committed`;
- Kafka leader-epoch start offsets.

A profile-neutral logical `KafkaProtocolCheckpointStore` publishes/recover-checks producer, transaction/aborted, and
leader-epoch components at one compatible covered-through vector. The BookKeeper implementation uses authenticated
`NBKE2` control entries between complete commit sets. The Object-WAL implementation uses a distinct bounded,
Root-bound, content-addressed `NWKCP1` protocol-state checkpoint Object family. `NWKCP1` may batch bounded partition
rows, but each row binds Binding/incarnation, partition, Storage Epoch, Owner Epoch, Kafka leader epoch,
`coveredThrough`, producer snapshot, transaction/aborted snapshot, and leader-epoch index with canonical integrity.

Physical Object checkpoint pages and `WalRunSealRecord` remain physical-only. They never contain producer/transaction
state and cannot authorize a protocol checkpoint, ACK, omission of physical recovery, frontier advance, protection
release, or GC. `NWKCP1` is stored under a separate exact WalRun Root sub-prefix, selected only by the protocol Head,
and charged to the run's cumulative metadata-read/GET/decode/time recovery envelope. Missing/corrupt Head or Object
state falls back to bounded NWG1 suffix replay; exhaustion fails closed.

The latest selected Object checkpoint is owned by one low-frequency, Root-bound
`KafkaProtocolCheckpointHeadV1`, independent of the physical checkpoint head and Seal:

```text
KafkaProtocolCheckpointHeadV1
  walRunRootIdentity
  publisherEpoch
  state = OPEN | TERMINAL
  checkpointOrdinal
  predecessorCheckpointDigest
  checkpointObjectKey
  checkpointObjectLength
  checkpointObjectDigest
  coveredThroughVector
```

Publication first conditionally creates and completely verifies the content-addressed `NWKCP1`, then CASes the Head
from the exact predecessor. Ordinal advances by exactly one, the predecessor digest must match the selected object,
and every covered-through component is non-regressing. At most one candidate may remain unresolved per publisher;
response loss converges only through exact reread of the object and Head. A publisher change CASes only the fenced
publisher epoch while preserving the selected checkpoint. A fork, predecessor mismatch, regression, or stale
publisher fails closed and can never be chosen by LIST order.

After run admission stops and the final compatible checkpoint exists, an irreversible CAS changes the same Head from
`OPEN` to `TERMINAL` and binds the terminal vector. That terminal Head value is the durable protocol-closure fact; the
physical Seal remains a separate physical fact. A successor WalRun Root must bind both the predecessor physical
Root/Seal identities and the exact terminal protocol-Head key plus canonical value digest before accepting Kafka
append admission. It may not infer closure from a discoverable `NWKCP1` Object or from the physical Seal alone.

Selected checkpoint Objects and their Head remain until the entire run has no successor, manifest, recovery,
retention, or source dependency. Their deletion cannot precede retirement/deletion of the WAL/source on which their
replay semantics depend. Unselected content-addressed residue may be reclaimed only after bounded authoritative
non-reference proof. Neither terminal state nor deletion eligibility grants ACK, physical-recovery omission, source
protection release, or source GC. Exact Head wire, vector caps, key grammar, and backend binding remain M3 evidence.

Async creation is allowed, but finite aggregate uncovered entries/bytes/age/time are mandatory. Exhaustion causes
checkpoint, backpressure, or rollover before ACKed recovery becomes unbounded.

### Closed-history BK takeover

1. Validate the current native Controller assignment and close old run admission before fencing any ledger.
2. Freeze the full legal run set through the same persistent head CAS that orders run attachment.
3. Fence/recover every admitted ledger that could still accept old writes. A locally fenced current ledger alone
   does not stop the old Owner from trying rollover; the closed head rejects its late root attachment.
4. Recover the complete contiguous commit prefix, using compatible compound checkpoint plus tail replay.
   Reconstruct bounded producer duplicate results, ongoing/completed/aborted transactions, marker indexes and
   leader-epoch history together. A partial group/gap stops the prefix; do not select a maximum physical offset.
5. Open the successor history and activate only if the native assignment/leader/Broker generation is still current.
   Internal-topic coordinator loading starts after this exact partition recovery is installed.

`OxiaKafkaRunRootAuthorityV2` implements one `owner-admission-v1` head per physical-namespace/Binding/partition/
StorageEpoch scope. It records native Owner identity and at most 1024 exact initial-root links. Attachment and
`OPEN -> CLOSED` use that head's version CAS. The winner determines membership even when its response or root
promotion is lost. CLOSED bytes are archived immutably before a newer Owner replaces the head; exact close retries
and later takeovers read the same set. Full admission stops at capacity. GC tickets/native ledger verification remain
separate requirements and do not grant protocol Owner admission. `openOwner` callers must validate native assignment
and finish predecessor recovery; the low-level metadata transport is not a Controller identity verifier.

BK recovery validates closed membership before I/O, then fences the exact ledger. It adopts complete unacknowledged
batches too, so response-loss retries can return the original offsets. Sealed root/end conflicts fail closed. HW is the
recovered legal prefix end; LSO derives from that HW and recovered transaction state. Coordinator authority in
`__transaction_state` is separate from partition marker/index state.

The Object core implements NSIP-1 section 6.4's durable grant/closing protocol. Both operations compare and replace
the same selected Binding/partition Head; closure fixes the complete legal prefix and successor Owner carries its
sources and checkpoint debt. Physical Object existence, LIST, local Owner checks, and asynchronous checkpoint/Seal
do not independently grant legal commit qualification.

### Coherent Fetch snapshot and isolation

Every Fetch partition read captures one allocation-free, immutable logical snapshot/reference set containing at least:

```text
Binding/incarnation and generation
Owner Epoch, Kafka leader epoch, Storage Epoch
run table, active-tail view, and source-map generation
logStartOffset, LEO/readableEndOffset, HW, and LSO
committed producer-state generation
transaction/aborted-index generation
leader-epoch-index generation
source-protection/read-pin generation
```

This is a coherent publication cell plus immutable references, not a deep copy of producer maps or one heap object per
record. Ordinary Fetch performs no synchronous Oxia, Object metadata, or manifest-authority read.

The read upper bound is:

- replica Fetch: `readableEndOffset` / LEO;
- consumer `read_uncommitted`: HW;
- consumer `read_committed`: LSO.

Primary-WAL durable end, Object materialization, or latest index coverage never substitutes for these upper bounds.
For `read_committed`, storage returns the exact protocol-native batch stream up to LSO plus the native aborted-
transaction metadata required by the Fetch response. Nereus does not silently rewrite this into a storage-only filter
that deletes aborted batches or control markers before Kafka response construction.

### Delayed Fetch

When the selected upper bound does not pass the requested offset or available bytes do not satisfy `fetch.min.bytes`,
the broker registers a bounded local delayed-Fetch waiter/purgatory operation. It never polls Oxia, BookKeeper, or
Object Storage for progress.

Wakeup sources are isolation-specific LEO/HW/LSO advancement and also Log Start Offset movement, owner/leader epoch
change, source-view/readability change, partition offline/delete, and timeout. Multi-partition Fetch retains Kafka's
request-level byte/deadline behavior: a relevant partition event reevaluates the request, and completion occurs when
the native aggregate condition is satisfied.

### Offset lookup, compaction gaps, and sequential reads

Random lookup is not `floor(offset)` alone. It is:

1. floor-search the run and index/active-tail directory;
2. accept the floor locator only when its assigned `[baseOffset,lastOffset]` covers the request;
3. otherwise select the first successor surviving batch, possibly in the next index block or run;
4. read the target DATA entry and validate BookKeeper digest, `NBKE2` CRC, and raw Kafka batch header/CRC.

`lastOffsetDelta + 1`, rather than record count, represents coverage inside a sparse/compressed batch. Successor
lookup handles whole batches removed by log compaction. Missing/deleted offsets therefore advance to the first
surviving batch whose `lastOffset >= requestedOffset`, subject to the captured Fetch upper bound.

Byte-preserving materialization and Kafka compaction are separate operations. Materialization copies exact raw
RecordBatch bytes. Compaction may remove only some records, create sparse/empty batches or new batch boundaries, and
rewrite Kafka CRC/timestamp structures, but it must preserve logical offsets, producer sequence recovery, control
batch/coordinator-epoch semantics, transaction markers and aborted ranges, tombstone retention, and native
`ListOffsets`/timestamp behavior. It rebuilds the range, producer, transaction/aborted, leader-epoch, and timestamp
side indexes for the new generation. Exact-byte requirements do not apply to a compaction rewrite.

A complete Kafka RecordBatch is the minimum physical/response boundary; it is never split because a byte limit lands
inside it. Native Kafka first-oversized-batch behavior is retained. Random seek uses indexed targeted reads. Sequential
Fetch may retain a compact disposable cursor over run, index block, locator ordinal, next entry, and next Kafka offset,
then coalesce a byte-bounded adjacent entry range. A cursor never holds a source-generation pin across requests: every
Fetch captures a new coherent view and revalidates the cursor's exact identities/version, otherwise discarding it.

The current BK-only native timestamp lookup bounds one scan by configured recovery-chunk records/bytes and Fetch
timeout. It scans only the captured shared HW prefix; a missing match or maximum-timestamp result requires reaching
that complete prefix. Capacity/timeout failure returns a native error instead of an incomplete empty result.

### Materialization and source selection

For `BOOKKEEPER_WAL_ASYNC_OBJECT`, source selection may be:

```text
[logStartOffset, ObjectMaterializedFrontier) -> Object preferred, protected BK fallback
[ObjectMaterializedFrontier, LEO)            -> BookKeeper active/sealed tail
```

One Fetch snapshot may intentionally plan disjoint non-overlapping Object and BookKeeper ranges. It must not replan
because a newer generation appears after capture. Source purity is required for each atomic append unit and each
declared whole-range fallback, not for an entire multi-range Fetch response.

Non-compacting materialization preserves exact raw RecordBatch bytes plus range directory, producer-state checkpoint, transaction /
aborted index, leader-epoch index, and integrity roots. Publication requires exact coverage and side-index validation,
then durable generation publication and local view installation. Old BookKeeper extents remain protected until all old
view pins drain and source-protection/retirement/GC contracts permit deletion. Source switching never changes Kafka
offsets, LEO, HW, LSO, or consumer-group commits.

For `OBJECT_WAL`, one immutable Object group may contain multiple partition-local commit sets. ACK requires provider-
resolved immutable bytes, reconstructible Root/key identity, complete local protocol-state/locator publication, and
the existing bounded LIST/checkpoint recovery contract. It does **not** reintroduce one remote metadata mutation per
commit set or require an async checkpoint page in the ACK cut. Low-frequency manifest generations select long-lived
read views/materialized sources; they are not the normal per-group append linearization point.

### Consumer-group offsets, internal topics, and profile boundary

Consumer-group committed offsets remain `(groupId, topic, partition, logicalOffset, metadata)` and contain no ledger,
entry, Object key, or byte position. Fetch resolves that logical offset through the same run/directory/locator path.

The 0.2 Kafka internal-topic Deployment policy fixes `__consumer_offsets` and `__transaction_state` to
`BOOKKEEPER_WAL_ONLY`. These low-latency, small-record, compaction-heavy coordinator topics do not initially pay Object
group linger or Object-recovery risk. A future change to `BOOKKEEPER_WAL_ASYNC_OBJECT` requires its own versioned policy
revision after internal-topic compaction generation, protocol checkpoint, Object fallback, coordinator restart, BK GC,
and marker parity evidence. No default for `__share_group_state` is inferred by this ADR; its explicit internal-topic
policy remains a fail-closed release gate rather than inheriting a user-topic default.

Profile durability differs; protocol publication does not:

| Profile | `durableEndOffset` proof | `readableEndOffset` publication | Default physical source |
| --- | --- | --- | --- |
| `OBJECT_WAL` | exact immutable persistence plus a per-Binding durable Head CAS grant | locator plus producer/txn/leader state | Object |
| `BOOKKEEPER_WAL_ONLY` | complete commit set at BK quorum in admitted, fenceable Owner history | locator plus producer/txn/leader state | BookKeeper |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | same as BK-only | same as BK-only | BookKeeper tail; Object preferred after generation handoff |

## Consequences and tradeoffs

- Kafka offsets, bounded duplicate results, transaction visibility and leader-epoch history retain distinct semantics.
- Shared BK commit removes mandatory logical follower work; cold recovery and Controller reassignment now carry
  the takeover responsibility. Storage redundancy remains independently configured and has a correlated failure domain.
- Ordinary BK append has no remote control-metadata I/O. Checkpoint debt, run enumeration, recovery memory and
  concurrency must remain bounded; fixed RTO requires measurement with storage/control availability assumptions.
- Object authorization cost cannot be hidden as asynchronous checkpoint work. No performance improvement is claimed
  without a comparable measured baseline.

## Evidence and implementation boundary

M2 owns the storage-engine/frontier/producer/transaction/index/checkpoint/read primitives and deterministic fault
harness. The NSIP-1 first batch brings forward native RF=1 Controller reassignment and recovery-gated activation. M6 still owns remaining broker/purgatory/coordinator integration, exact error mapping, client-compatible
Produce/Fetch/transaction/leader-failover behavior, and full-process restart evidence. M4/M5 own materialized read-view
handoff and source retirement integration. No milestone may claim Kafka parity or superiority from document status.
The code-level cuts are the
[M2-K0 input closure](../v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) and the
[M2 Kafka Produce/Fetch detailed design](../v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md).

Required scenarios are `V2-KAF-DATA-001..022`. They cover out-of-order durability, predecessor failure, response-loss
retry, speculative producer state, checkpoint crash recovery, LEO/HW/LSO isolation, abort filtering, RF=1 reassignment,
delayed Fetch, compaction gaps, pinned source generation, Object/BK fallback and GC, pre-admission rejection,
leader-epoch recovery, random/sequential full-batch reads, fence/publication races, closed legal-history recovery,
old-owner admission closure, native duplicate identity, Object protocol checkpoints, and partial-batch compaction.
An M2 receipt may promote only scenario claims owned exactly by M2; rows shared with M3/M4/M5/M6 remain `PLANNED`
until every named milestone supplies its evidence. Kafka `v2M2KafkaFinalCheck` is not global `v2M2Check`.

This ADR refines ADRs 0009, 0011, 0031, 0067, 0069, and 0086. M2 production inputs and evidence-derived numeric
defaults remain under `V2-OPEN-BK-02`; exact `NWKCP1` is M3. None is reopened as a protocol semantic choice.

NSIP-1 T4 implements Object core grants in `KafkaObjectAuthorizationV1`. A complete authenticated NWG1 member obtains
an exact receipt only after its Owner/predecessor/source descriptor CAS succeeds. The completion tracker and coherent
publication both require that receipt. Object shared-commit HW equals the granted continuous end; LSO derives from the
same transaction state. Closing orders on the same Head and fixes an immutable legal prefix. Common NWKCP1 state plus
only the selected authorized tail can restore a fresh partition root; bare physical replay cannot enable the new commit
model. Source references and recovery debt survive Owner/run changes; only Head-selected complete checkpoint coverage
releases uncovered debt. This core does not enable the native Object Broker profile or extend M4/M5/Final receipts.
Object tests query producer duplicate state after recovery and retry the original grant candidate. They do not exercise
native Object Produce retries returning the prior result; `KafkaCoherentCommitCoordinatorV1.findDuplicate` still
admits only BOOKKEEPER, and that end-to-end behavior remains part of native Object integration.
