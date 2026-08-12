---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m1
---

# Correctness and append

## Authorities

V2 separates five authorities:

| Authority | Owns | Does not own |
| --- | --- | --- |
| Native Write Authority | the only protocol allowed to allocate positions for one Topic Incarnation | physical placement or another protocol's positions |
| ownership authority | current binding/incarnation, Owner Epoch, admission | committed bytes or protocol kind |
| primary WAL | durable Physical Extents and binding-scoped Protocol Coverage/frontier | Topic Protocol Binding or protocol coordinator state |
| manifest authority | one typed logical read view over immutable physical generations | active-tail append linearization |
| protocol authority | Kafka/Pulsar coordinator, cursor, transaction, and visibility semantics | physical object retirement by itself |

Control metadata selects an owner and stores low-frequency roots. It is not consulted on every normal append.

## Append state machine

For one Topic Protocol Binding, Topic Incarnation, Storage Epoch, and Owner Epoch:

1. the protocol-native owner resolves one immutable `TopicBindingAggregateRecord` containing the complete Topic
   Protocol Binding, typed protocol-native Topic Incarnation Identity, and initial Storage Epoch;
2. a serialized writer lane validates the current ownership token and atomically reserves both completion-tracker and
   active-tail-locator capacity;
3. only after that reservation succeeds, it allocates positions through the binding's Position Domain;
4. the selected WAL accepts protocol-native frames carrying binding/incarnation, Storage Epoch, Owner Epoch, typed
   Protocol Coverage, commit-set membership where applicable, length, and checksum;
5. Object WAL first resolves physical extent outcome independently from protocol order; BookKeeper uses its native
   durable ledger outcome;
6. validated append units enter only their binding's contiguous typed durable frontier;
7. the protocol response succeeds only when the complete returned Protocol Coverage is durable and readable;
8. background workers later publish sealed/read-optimized generations.

Kafka allocation yields a half-open Kafka Offset Range. Pulsar allocation yields Pulsar Positions whose adjacency and
cross-ledger order are proven by the Ledger Chain and represented as ledger-keyed Pulsar Coverage. On Pulsar Object WAL,
the serialized writer allocates entry IDs inside the current virtual ledger while a cell-owned MetadataStore/Oxia
authority publishes low-frequency ledger identity and chain changes. Neither path allocates or persists a
cross-protocol `long logicalOffset`.

Kafka makes all RecordBatch Frames from one partition storage append durable and visible as one all-or-none Append
Commit Set. Pulsar makes one ManagedLedger entry one frame/commit set. An Object group may contain several such units,
but its physical PUT boundary cannot weaken either protocol's append atomicity.

On Kafka BookKeeper-primary profiles, offset/entry admission remains ordered while a bounded set of BookKeeper writes
may be in flight. Each complete append commit set stores durable group evidence in the ledger. Completion enters an
ordered queue, and Durable/Readable/ACK publication advances only through the greatest contiguous Kafka prefix. Packed
range-index checkpoints are asynchronous; pre-reserved owner-local locators make an ACKed tail readable meanwhile.
No normal append creates a remote mapping/reservation record, and an earlier failed/unknown group fences recovery
rather than allowing a later durable group to commit around the gap. The same coherent publication cut installs
producer, transaction/aborted, and Kafka leader-epoch state; a storage-only `committedEndOffset` is forbidden. ADRs
0086 and 0087 are authoritative.

The owner must not acknowledge coverage because a local future completed if its Owner Epoch or Storage Epoch authority
was fenced before the durability completion was validated.

On Pulsar, admitted work captures the complete local atomic ACTIVE-fence word before position allocation and rechecks
exact equality before every success completion, duplicate/transaction fast-path response, or ACK. A changed/invalid
word permits only a fenced or uncertain failure, never a success ACK, even when bytes became durable. The check is
local; ordinary append performs no ownership or selector metadata read.

## Physical extent resolution versus binding durability

For `OBJECT_WAL`, `LaneExtentResolvedThrough` and `BindingDurableFrontier` are distinct:

- the former is a per-WalRun/lane contiguous sequence of verified provider-resolved Objects and is used for physical
  PUT ordering, checkpoint, Seal, and recovery;
- the latter is a binding-scoped Position Domain frontier and alone decides protocol durability/ACK eligibility.

One provider-resolved shared Object may enter checkpoint while binding A waits for typed predecessor coverage and
binding B advances. The lane barrier waits only for an earlier Object outcome that remains unknown or could be absent;
it does not wait for every member's protocol ACK.

Binding completion uses owner-local, lazy, reconstructible state keyed by Protocol Cell, binding, incarnation, Storage
Epoch, and Position Domain identity/version. Owner Epoch is an O(1) cached completion fence rather than durable frontier
identity; admitted completion performs no remote metadata read. One complete Kafka append commit set or Pulsar entry
receives one checked 64-bit local `CompletionTicket` after capacity and exact coverage allocation. Full ticket equality
fences ring-slot ABA, while exact typed coverage and Position Domain adjacency remain ordering authority. The ticket is
not product wire/API/config or persistent state. Normal completion uses a bounded ring/window; recovery defaults to
bounded collect/sort and fresh local tickets. Runtime gaps/futures/tickets are not persisted.

## Typed protocol frontiers

Each Position Domain supplies comparison, adjacency, coverage, and frontier operations for:

- `AllocatedFrontier`: the next boundary reserved by the active writer lane;
- `DurableFrontier`: the highest contiguous boundary proven by the primary WAL;
- `ReadableFrontier`: the highest boundary exposed through the current logical read view;
- `TrimFrontier`: the lowest logically retained boundary.

Required ordering is `TrimFrontier <= ReadableFrontier <= DurableFrontier <= AllocatedFrontier`, where `<=` is
defined only by that binding's Position Domain. A normal successful append closes only when its returned coverage is
inside Readable and Durable coverage. A failed or uncertain append may leave allocation ahead of durability only until
owner-local resolution or recovery. Recovery never exposes an unproven gap.

For Kafka, the generic frontiers are refined rather than renamed into one committed end:

```text
TrimFrontier / Log Start <= LastStableOffset <= HighWatermark
                         <= ReadableFrontier / LEO <= DurableFrontier <= AllocatedFrontier
```

`AllocatedFrontier` is speculative owner-local admission. `DurableFrontier` is the greatest contiguous profile proof.
`ReadableFrontier` becomes Kafka LEO only after locators plus producer/transaction/leader-epoch state publish
coherently. Kafka ISR owns HW and transaction stability owns LSO. `acks=1` waits for Readable/LEO; `acks=all` obeys
native ISR/minISR admission and waits for HW. Consumer `read_uncommitted` stops at HW, `read_committed` stops at LSO,
and replica Fetch may read through LEO. Object materialization and checkpoint coverage never advance those bounds.

Kafka producer identity `(producerId,producerEpoch,baseSequence,lastSequence)` is per RecordBatch and distinct from the
storage attempt identity; a multi-batch commit set binds the ordered identity vector. Native Kafka duplicate lookup is
not strengthened with a payload-digest conflict. Only the same explicit in-process request instance may join a pending
future; a native committed duplicate returns its original Offset Range. Non-idempotent retries may duplicate.
Admission validates multiple in-flight
batches from one producer against committed state plus ordered speculative deltas before assigning new offsets. A
timeout is outcome-unknown; speculative positions may be reused only after
fenced recovery proves they never became readable/HW-covered and rolls back every coupled speculative state.

Frontiers from different Topic Protocol Bindings, Topic Incarnations, or Position Domains are not comparable. Pulsar
cross-ledger ordering comes from the authoritative Ledger Chain; V2 does not derive a permanent
`ledgerBase + entryId` coordinate.

After provider resolution, Object request/payload/ciphertext/compression buffers are released. A pending typed gap
retains only coverage, idempotency identity, an authenticated descriptor reference, and an owner-local waiting future.
Per-binding count/descriptor-byte/future/age bounds and aggregate shard tracker bytes stop that binding before new
position allocation; ordinary typed gaps do not fence the binding or roll the WalRun, and unrelated bindings continue.

## Active-tail readable publication

Object-WAL ACK requires a derived owner-local active-tail read view before manifest publication. Logical isolation is
per Binding, but physical storage may use shard-owned segmented, Kafka-offset-range, and Pulsar-ledger/entry-range
indexes. A generic `ProtocolCoverage` TreeMap is forbidden on the normal append/ACK hot path; no heavy object per
Binding/unit is required.

Each shared Object's digest/header/directory/AEAD validation produces one reusable `VerifiedExtent`; member publication
adds no HEAD/GET, KMS, metadata call, whole-Object verification, or repeated directory decryption. Locator budget is
part of the pre-position reservation. A serialized binding cut installs the next contiguous range locators hidden and,
for Kafka, checks exact Binding/incarnation/Storage/Owner/Kafka-leader/predecessor-state fences before one coherent
state-root replacement publishes protocol state plus Readable/Durable/LEO. Leadership and ownership transitions
compete on that same cut. A stale callback cannot publish then discover fence loss. Only a legal publication may wake
readers and proceed to optional response fencing/ACK. A locator behind a typed gap remains invisible even when already
installed.

Takeover rebuilds Root/checkpoint/LIST physical inventory first and then publishes each Binding view independently.
Manifest replacement must cover the same typed range and satisfy source-protection/read-pin conditions before active
locators retire. This correctness path cannot be disabled by Topic policy.

Append and source handoff have different publication lifetimes. Normal append installs hidden locators, performs the
fenced Readable/Durable publication, and ACKs without creating a snapshot object. Low-frequency manifest
handoff publishes a source-selection generation. A logical `BindingReadViewSnapshot` captures Binding/incarnation,
Storage Epoch/Position Domain version, owner fence, Readable Frontier, active-tail version, manifest generation, and
source-protection generation for one Binding-scoped protocol read batch. Allocation-free local pins are bounded; one
record/message/frame, a whole connection, and an unbounded stream are not pin units.

Each concurrently unfinished batch exclusively reserves one slot from a bounded sharded cross-Binding pool before
source I/O. It publishes `{Binding,G}`, establishes StoreLoad ordering, revalidates G, and only then captures a stable
generation-tagged `{sourceGenerationId, ReadableFrontier, activeTailViewVersion}`. A mismatched/torn cell retries
without dereference. One atomic `SlotLeaseWord` is `FREE` or `PINNED(generation)`; ordinary callbacks equality-check it
and only the unique terminal drain CAS may clear it. Cancellation stops new source use but the slot remains until every
provider I/O, retry, fallback, decode, and source-backed-buffer use completes. Nonresponsive work is quarantined under
a hard capacity bound rather than force-cleared.

The same snapshot may serve disjoint manifest and active-tail ranges, but one atomic append unit and every explicitly
declared whole-range fallback stay source-pure. Locator retirement follows old-view pin drain. One selector CAS
atomically selects `PREFERRED_ONLY`, closes E, grants same-owner no-fallback E+1, and persists E's closure anchor;
takeover competes on the same exact predecessor tuple. Only `ADMITTING/STOPPED` exist, STOPPED recovery uses a fresh
epoch, and response unknown forbids further E admission until exact reread. One small inline canonical unresolved-
anchor set remains selector-owned, and every `ADMITTING` admission reserves the complete emergency STOPPED envelope.

Source protection remains until current fallback-bearing pins drain and source i's `[first_i,sharedLast]` interval has
contiguous source-independent proof before its exact release CAS. Batch minimum is summary only; N members retain up to
N release CAS operations and bounded O(N) reconciliation. One quarantined member blocks full-batch retirement and
capacity but not eligible sibling release. Each proof follows an asynchronous irreversible terminal cut binding the
closure anchor and immutable capability evidence. A hint, ordinary owner fence, cross-key reread, current backend
configuration, session loss, or host timer is insufficient. Pressure may STOP admission but never authorizes early
reclaim, frontier advance, metadata compaction as source GC, or physical GC. Terminal correctness comes from one closed
verifier over the exact immutable candidate; a non-transactional current-owner check is only ACL/rate/audit. Eligible
anchors prune asynchronously in batches. A completed full batch may only perform the irreversible exact-version same-
key `FULL_V1 -> RETIRED_V1` compaction; the compact tombstone remains permanent in 0.2 and authorizes no protection
release or source deletion.

## Uncertain append

A timeout, connection reset, or lost provider response yields an uncertain result. Resolution preserves the original
protocol idempotency identity and verifies:

- Topic Protocol Binding, Topic Incarnation, Storage Epoch, and Owner Epoch;
- typed Protocol Coverage and frame/entry count;
- object/ledger identity;
- exact byte length, SHA-256/v1 `ObjectExtentDigest`, CRC32C/v1 `FramePayloadChecksum`, and provider proof in their
  declared domains where applicable;
- contiguous predecessor coverage.

A retry may return the original success or fail closed. It may not allocate different successful protocol positions for
the same idempotency identity. For Object WAL after process loss, a provider-present group is verified and reconciled;
a conclusively absent never-ACKed group fences the old run at its proven frontier and may be rebuilt only in a fresh run.
Unknown presence remains fail-closed. V2 does not pretend a deterministic nonce recreates lost ciphertext.

For Kafka, response reconciliation also restores producer sequence, partition transaction/aborted state, and Kafka
leader-epoch state at the same contiguous cut as locators and LEO. It may not decide a duplicate from storage attempt ID
alone or expose bytes while producer state remains behind.

## Hot-path contract

Normal append must report both of these as zero:

- remote control-metadata reads;
- remote control-metadata mutations.

The complete active Topic Binding Aggregate and ownership are cached only after an explicit open/acquire operation.
Cache misses stop admission and reload before allocating positions; they do not insert metadata access into the admitted
append path.

Object-WAL admission also proves every still-recoverable run remains within the cumulative worst-case recovery
envelope. Approaching a run or recovery bound triggers rollover/backpressure before another ACK; fallback never resets
the envelope. This correctness-driven availability cost is explicit and does not permit a per-group metadata mutation.

Relevant tradeoffs: `T-APPEND-01` and `T-POSITION-01`. Required scenarios: `V2-APP-001`, `V2-APP-002`,
`V2-APP-003`, `V2-POSITION-001..007`, `V2-META-002..004`, `V2-KAF-META-001`,
`V2-OBJ-002/004..012/020..024`, and `V2-READ-003..015`.
