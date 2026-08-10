# Shared Storage Context

The Shared Storage Context describes how protocol-owned data is covered, placed, transformed, and retired without
creating a third cross-protocol position truth.

## Fabric and binding

**Storage Fabric**:
A Nereus deployment boundary that may serve multiple Kafka and Pulsar Protocol Cells over shared storage and lifecycle
services.
_Avoid_: Single-protocol cluster, universal log

**Protocol Cell**:
One Kafka or Pulsar control-and-runtime domain whose protocol authority is independent from other cells sharing the
Storage Fabric.
_Avoid_: Storage tenant, broker group

**Provider Infrastructure**:
An external Object Storage service/account or BookKeeper cluster that may be used by multiple Protocol Cells. Sharing it
is a deployment choice and may create a common physical failure domain.
_Avoid_: Cell Provider Session, logical isolation guarantee

**Cell Provider Scope**:
The Protocol-Cell-owned binding of provider endpoint identity, exclusive namespace, credential/security scope, allowed
encryption/KMS scope, admission/quota scope, operator owner, and physical-delete capability. Stable secret references or
identity versions may be bound; secret values are not persisted in the scope.
_Avoid_: Shared credential context, provider class name

**Cell Provider Session**:
A process-local, independently drainable/closeable provider adapter for one Cell Provider Scope. It owns cell-local
admission, retry/circuit-breaker state, open groups, in-flight accounting, and metrics. It may borrow compatible
lower-level transport, but that transport owns no protocol or lifecycle authority.
_Avoid_: Cross-cell batching authority, shared correctness state

**Topic Protocol Binding**:
The immutable association between one Topic Incarnation, one Protocol Cell, one Position Domain, one payload mapping,
and one Native Write Authority kind. The current leader/broker holder is represented separately by an Owner Epoch.
_Avoid_: Topic storage profile, mutable topic binding

**Topic Incarnation**:
One lifetime of a durable topic identity; deleting and recreating a topic creates a different incarnation even when the
external name is reused.
_Avoid_: Topic name, stream name

**Topic Incarnation Identity**:
The protocol-discriminated ABA fence for one Topic Incarnation: Kafka topic UUID plus canonical name, or Pulsar
canonical persistence/name facts plus binding generation. It deterministically derives aggregate authority and IDs.
_Avoid_: Name-only key, random durable topic ID, backend version

**Position Domain**:
The protocol-specific rules that define valid positions, ordering, adjacency, and frontiers for a Topic Protocol
Binding.
_Avoid_: Global offset, universal logical offset

**Native Write Authority**:
The single protocol authority permitted to allocate new positions for a Topic Incarnation at a given time.
_Avoid_: Dual writer, projection writer

## Coverage and placement

**Protocol Coverage**:
A Topic-Protocol-Binding-scoped description of which protocol positions are represented by a storage source or output.
_Avoid_: Storage range, byte range, global logical range

**Physical Extent**:
An immutable locator and integrity identity for bytes stored in Object Storage or BookKeeper.
_Avoid_: Protocol position, MessageId, Kafka offset

**Object Extent**:
A Physical Extent identified by an immutable object key/version, byte interval where applicable, length, and integrity
metadata.
_Avoid_: Kafka Offset Range, Pulsar Coverage

**BookKeeper Extent**:
A Physical Extent identified by ledger/entry coordinates and integrity metadata; those coordinates are placement facts,
not automatically protocol positions.
_Avoid_: Kafka Offset, universal ledger position

**Protocol Frontier**:
A Position-Domain-typed boundary between covered and uncovered protocol positions.
_Avoid_: Global end offset, untyped watermark

**Storage Epoch**:
One immutable interval in a Topic Protocol Binding's append-only storage history, with one profile and protocol-native
start/sealed-end frontiers. Lifecycle changes are append-only state history. In 0.2 one initial epoch is created per
Topic Incarnation and no online transition runtime exists; the exact future transition vocabulary is deferred.
_Avoid_: Mutable profile, storage mode flag

**Resolved Policy Class**:
A closed typed Topic/Tenant-or-Namespace policy plus quantized values whose effective budgets are capped by Protocol
Cell/shard and host/process limits. It cannot enlarge format/parser caps. Any value affecting bytes or recovery is
persisted at its Storage Epoch, hard-recovery WalRun Root, Object group, or offload attempt, and one policy identity
cannot cross those lifecycles. Product/Deployment owns the base semantic default; Cell/host never replaces it.
_Avoid_: Arbitrary per-topic flags, Root-level soft packing identity, Cell-selected default, host-selected durable
format, configurable correctness

**Topic Binding Aggregate**:
The atomically visible create/open unit whose one immutable logical schema v1 contains a Topic Protocol Binding and its
ordinal-zero initial Storage Epoch. Canonical bytes persist only independent semantics: protocol/profile-derived
position, payload, authority, WAL, checksum, and crypto views plus the logical binding back-reference are not repeated
as separately selectable fields. Neither component has an independently writable authority; ACTIVE is derived.
_Avoid_: Default epoch, partially visible topic, separately mutable binding/epoch, duplicate derived wire authority

**Object Extent Digest**:
Integrity over the exact canonical Object-provider request body after Nereus compression and client-side encryption.
It proves stored-object bytes and cannot replace a frame payload checksum.
_Avoid_: ETag, decoded payload checksum

**Frame Payload Checksum**:
Integrity over the exact protocol-native Kafka batch or Pulsar entry bytes after the outer Object envelope is decoded.
It cannot prove the exact Object-provider request body.
_Avoid_: Application-record reserialization checksum, Object extent digest

**Provider Object Proof**:
Provider-bound evidence joining one immutable object version, exact canonical-body length, full-object checksum
algorithm/type, and value. It is distinct from Nereus user metadata and the expected Object Extent Digest descriptor,
and is not a prerequisite for each routine authenticated frame range.
_Avoid_: ETag proof, user-metadata checksum echo, composite checksum, full GET before every random read

**WalRun Root**:
The immutable Cell control-metadata authority for one Object-WAL shard run. It fixes scope, prefix, run/session
identity, epoch-validation rules, format families, wrapped run-key identity, lane-sequence contract, and aggregate
bounded LIST/recovery budgets; per-group descriptors are reconstructed from leaf keys and verified headers. It does
not carry one Topic-specific soft packing class or a per-lane copy of the recovery budget.
_Avoid_: Per-group metadata commit, Root-level packing class, pointer per class, sealed-run-only discovery, unbounded
prefix scan

**WalRun Scheduling Lane**:
One of at most three lazily instantiated packing-class lanes under a single WalRun Root/pointer. It owns a stable
one-digit class/lane ID, lane-local extent-resolution barrier, bounded builder, and in-flight limit; all run/recovery
budgets remain aggregate and checkpoint publication uses one vector chain. IDs are permanently
`0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, and `2=OBJECT_COST`; sequence allocates after immutable group-plan admission
and before HKDF/encryption/final-body seal.
_Avoid_: Eager target-sized buffers, lane-specific Root/pointer, protocol ACK as physical barrier, lane-local checkpoint
chain

**Provider Resolved Extent**:
An immutable Object whose conditional PUT outcome, exact identity/body proof, Root-bound header/directory, and lane
sequence have converged so it can no longer become provider-absent. It is eligible for physical checkpoint regardless
of whether every member has advanced its binding frontier.
_Avoid_: All-members-ACK prerequisite, checkpoint as protocol ACK fact, payload retention after resolution

**Lane Extent Resolved Through**:
The per-WalRun/lane greatest contiguous provider-resolved sequence. It drives physical checkpoint/Seal/recovery and is
never compared with Kafka Offset or Pulsar Position.
_Avoid_: Binding Durable Frontier alias, Object order as protocol order, typed gap consuming uncovered-tail budget

**Binding Completion Tracker**:
Owner-local reconstructible completion state that advances one Binding's Position-Domain Durable Frontier. Normal work
uses a bounded ring/window; recovery defaults to bounded collect/sort and fresh tickets. Tracker slot and active-tail
locator budget reserve together before protocol position allocation. It is not persisted and retains no payload in gap
entries.
_Avoid_: Per-topic permanent TreeMap, remote completion metadata read, persisted runtime gap/ticket map, Owner Epoch in
logical frontier identity

**Completion Ticket**:
A checked 64-bit owner-local ring identity allocated after combined capacity reservation and exact Protocol Coverage.
One complete Kafka append commit set or Pulsar entry receives one ticket; full equality fences slot ABA while Position
Domain coverage/adjacency remains authority. Takeover discards it.
_Avoid_: Product wire/API/config field, persistent append ordinal, separate ticket generation, ticket-based recovery
order

**Verified Extent**:
One owner-local result proving the shared Object digest, KMS envelope, fixed header, directory parse, and directory AEAD
have validated. All member bindings reuse it for completion and active-tail publication without repeated remote or
cryptographic setup work.
_Avoid_: HEAD/GET/KMS per binding, repeated whole-directory decryption, provider proof as frame-offset authority

**Active-Tail Read View**:
The owner-local derived view that makes unmanifested acknowledged coverage readable. Logical isolation is per Binding;
physical storage may use one shard-owned segmented Kafka-offset/Pulsar-ledger-entry range index. Locators install hidden
before Readable/Durable frontiers publish and ACK; entries behind a gap remain invisible.
_Avoid_: Disable switch, one heavy object per Binding/unit, generic ProtocolCoverage TreeMap on the hot path, ACK before
locator publish

**Binding Read View Snapshot**:
The logical state captured for one Binding-scoped protocol read batch. It binds incarnation, epoch/Position Domain,
owner fence, Readable Frontier, active-tail view, manifest generation, and source-protection generation, but need not be
a heap object. Append release-publishes frontiers without creating generations; source handoff publishes and drains
low-frequency pinned generations.
_Avoid_: Snapshot object per ACK/read, record/message/frame pin, connection-lifetime pin, remote metadata read, one
process-global refcount

**Read View Pin**:
An allocation-free claim by one unfinished Binding-scoped protocol read batch on one exact source-selection generation.
It lasts through every source I/O, retry, fallback, decode, and source-backed-buffer use; capacity pressure never
authorizes early clear or protection release.
_Avoid_: Binding x event-loop fixed slot, shared concurrent-read slot, connection pin, timeout-based force clear

**Slot Lease Word**:
The single owner-local atomic identity proving that one read batch, and no earlier reuse of the same slot, owns a Read
View Pin until complete source drain.
_Avoid_: Async lifecycle state machine, split ticket/state atomics, timeout-clear token, durable quiescence proof

**Generation-Tagged Read Publication**:
The coherent owner-local tuple binding one source-generation identity to the Readable Frontier and active-tail view
version captured by a pinned read batch. A mismatched or torn tuple is unusable.
_Avoid_: Generation-only pin, independently sampled frontier, per-ACK snapshot object

**Owner Read Quiescence Proof**:
Source-independent durable evidence that one exact Read Admission Epoch can no longer access fallback-bearing read
views through a stated generation. It derives from authoritative planned drain or qualified read-admission expiry.
_Avoid_: Source-specific owner row, latest-owner-only proof, ordinary owner fence, distributed per-read refcount

**Read Admission Epoch**:
The monotonic, never-reused Binding-incarnation order in which an owner becomes authorized to admit reads. It becomes
atomically visible with that authority and is distinct from an unqualified backend Owner Epoch.
_Avoid_: Host session counter, source generation, inferred takeover order, reusable owner number

**Binding Read Selector**:
The single Binding-incarnation authority that atomically selects source view, Owner Epoch, Read Admission Epoch, and
closed `ADMITTING/STOPPED` state, so takeover and fused fallback-removal/E+1 closure cannot both commit from the same
predecessor.
_Avoid_: CLOSING/DRAINING reader state, manifest-only pointer, topic-generation selector, cross-key reread

**Read Admission Closure Anchor**:
The immutable predecessor/successor selector and transition digest that proves one Read Admission Epoch lost admission
authority. The selector logically owns a small bounded inline canonical unresolved set and preserves a dedicated
emergency STOPPED envelope until each fallback-relevant epoch gains a terminal cut. Membership-neutral transitions
copy validated canonical bytes; 0.2 has no anchor page/index/chain.
_Avoid_: Backend history assumption, remote anchor lookup, borrowed STOPPED reserve, watch event, host-local CAS receipt

**Read Admission Epoch Terminal Cut**:
The immutable asynchronous proof identity that one exact Read Admission Epoch can never admit another read and binds
its selector-carried closure anchor plus last admitted/drained read-view cut to planned closure or qualified authority
expiry. Planned and expiry variants use one closed verifier; immutable candidate facts remain the safety proof when a
backend cannot atomically check current owner authority. Eligible anchors prune asynchronously in batches.
_Avoid_: Role-name authority, proof key existence, mutable closed flag, per-terminal prune CAS, local timeout

**Source Retirement Batch**:
One transition-exact immutable bounded fallback set with a shared last epoch. Every source/protection row retains its
own inherited first epoch and releases against `[first_i, sharedLast]`; the batch minimum is summary only. After every
exact protection and reference retires, the same key may move irreversibly from `FULL_V1` to compact `RETIRED_V1`.
The permanent 0.2 tombstone proves metadata compaction only.
_Avoid_: Mutable per-owner accumulator, `RETIRED_V1 -> FULL_V1`, tombstone deletion, source-GC inference

**Quiescence Proof Window**:
The bounded Binding authority that proves contiguous Owner Read Quiescence Proof coverage for reusable epoch intervals;
a gap affects only source-retirement intervals `[first_i, sharedLast]` that contain it.
_Avoid_: Owner x source matrix, per-batch CAS accumulator, gap-skipping frontier

**Quiescence Capability Evidence**:
The immutable Protocol Cell/backend admission-generation evidence that defines which planned-drain or authority-expiry
proof verifier may interpret historical Read Admission Epochs.
_Avoid_: Current backend configuration, generic lease flag, Topic capability switch, copied conformance report

**Current WalRun Pointer**:
The one low-frequency per-shard CAS authority binding the current WalRun Root key/SHA and shard run epoch. It anchors a
bounded predecessor lineage; normal admitted group append does not mutate it.
_Avoid_: Root-prefix LIST, per-group pointer update, locally merged lineage

**WalRun Seal**:
The immutable Cell control-metadata record that binds one Root to its terminal lane-sequence vector, one final
checkpoint-head key/SHA, and minimum aggregate extent-count/body-byte completeness facts. It contains no
binding/read frontier, ACK, gap, or per-binding coverage. A successor Root references both predecessor Root and Seal
before the current pointer advances.
_Avoid_: Mutating the Root to seal, reopening a sealed run, pointer advance before successor publication

**WalRun Checkpoint Page**:
An asynchronous immutable page in the one run-wide predecessor chain, with at most 256 aggregate descriptors/64 KiB
and a per-lane provider-resolved `coveredThrough` vector. One publisher-epoch-fenced combiner admits one candidate at a
time. Root identity appears once in the page header; physical rows do not repeat it or carry binding logical state. It
may advance any subset of lanes contiguously; open uncovered tails still require LIST and the Seal requires one final
gap-free vector chain.
_Avoid_: Per-row Root SHA, per-topic checkpoint switch, ACK dependency, binding frontier/coverage row, one chain/head
per lane, checkpoint overriding provider bytes

**Checkpoint Provider Proof Mode**:
The Root-fixed `NONE` default or conditionally admitted version-bound FULL_OBJECT SHA-256 proof family. Root fixes
Provider adapter/canonicalizer and token cap; a row carries only proof tag, token length, and bounded canonical binary
version bytes. An absent, oversized, or incomplete candidate becomes `NONE` before row seal; malformed persisted wire
fails closed. `NONE` does not trigger routine whole-Object recovery.
_Avoid_: Topic switch, String normalization, ETag, header/SDK blob, repeated body length/SHA/scope, proof as offset or
ACK authority

**Directory Prefix Hint**:
The exclusive `directoryPrefixEnd19` embedded in every NWG1 leaf key. It plans a bounded prefix GET under the exact
Root/key identity but does not enter the body digest, prove durability, authenticate the directory, or authorize frame
offsets. Structured descriptors reconstruct the key from the Root prefix rather than repeat it.
_Avoid_: Provider proof prerequisite, manifest-only hint, full key per checkpoint row, authenticated offset authority

**Binding Context Table**:
The bounded NWG1 table that binds frames to exact Topic Incarnation, binding, Storage Epoch, and Owner Epoch authority
inside a multi-binding ObjectExtent. The WalRun Root does not carry one singular topic epoch.
_Avoid_: Group shard epoch as topic authority, untyped binding summary

**Append Unit Directory**:
The authoritative bounded NWG1 in-body directory for frame ranges, context references, Kafka commit-set membership,
and Pulsar entry units. Prefix bytes are hard-capped before frame count is derived; sidecars, manifests, and checkpoints
are accelerators only. Root-bound AEAD authorizes routine local ranges, not a ProviderObjectProof.
_Avoid_: Footer-only authority, paginated/second directory authority, unbounded prefix, provider proof per random read,
commit set spanning ObjectExtents, record-count-derived coverage

**NWG1 Run Key**:
The random 256-bit WalRun data key wrapped once under the immutable Cell KMS key/version. HKDF derives per-Object keys;
directory and frame AEAD use disjoint fixed nonce domains, and derivation binds lane ID plus lane-local sequence.
_Avoid_: Per-PUT KMS wrap, topic-wide key, reused run epoch/lane/sequence/nonce

**Recovery Envelope**:
The cumulative worst-case bound over all work required to recover admitted Object-WAL state. Normal ACK/admission must
preserve it; fallback cannot reset it.
0.2 performs conservative bounded prefix recovery for current-run candidates and has no partial-run omission vector;
an authoritative whole-WalRun retirement frontier may exclude only the retired run as a whole.
_Avoid_: `FullyManifestCoveredThrough`, manifest/lane inference as skip proof, fallback counter reset, partial
certificate without M3/M7 evidence, takeover timeout only, per-run counter reset, partial recovery success

## Projection and migration

**Access Projection**:
A read/access relationship that exposes data governed by one Native Write Authority through another protocol without
granting the target protocol position-allocation authority.
_Avoid_: Dual-native topic, shared writer

**Projection Map**:
A future durable mapping between source and target Protocol Coverage for an Access Projection or migration. The term is
retained in 0.2, but no Projection Map store/runtime is shipped.
_Avoid_: Global offset map, per-message control-plane commit

**Migration Link**:
The explicit authority-transfer relationship between a source Topic Protocol Binding and a target Topic Protocol
Binding.
_Avoid_: Storage Epoch, profile switch

## Provider and failure boundary

Protocol Cell is the minimum logical failure-attribution and provider-authorization boundary. Provider Infrastructure,
worker processes, executors, and observability may be shared, but sessions, namespaces, admission, retry/circuit-breaker
state, task/cache roots, and GC authorization remain cell-scoped. Object WAL groups do not cross cells in 0.2.

Shared physical infrastructure may still fail all attached cells. Dedicated provider infrastructure is an optional
deployment topology for stronger SLO, compliance, or physical-failure isolation. Tenant policies may further subdivide
a Protocol Cell; a Cell is not redefined as a storage tenant.
