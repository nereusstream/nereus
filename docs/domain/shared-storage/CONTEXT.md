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

**Topic Binding Aggregate**:
The atomically visible create/open unit whose one immutable logical schema v1 contains a Topic Protocol Binding and its
ordinal-zero initial Storage Epoch. Neither component has an independently writable authority; ACTIVE is derived.
_Avoid_: Default epoch, partially visible topic, separately mutable binding/epoch

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
algorithm/type, and value. It is distinct from Nereus user metadata and the expected Object Extent Digest descriptor.
_Avoid_: ETag proof, user-metadata checksum echo, composite checksum

**WalRun Root**:
The immutable pre-append authority for one Object-WAL shard run. It fixes scope, prefix, run/session identity,
epoch-validation rules, format families, initial sequence, and bounded LIST recovery budgets; per-group descriptors are
reconstructed from content-addressed leaf keys and verified headers.
_Avoid_: Per-group metadata commit, sealed-run-only discovery, unbounded prefix scan

**Current WalRun Pointer**:
The one low-frequency per-shard CAS authority binding the current WalRun Root key/SHA and shard run epoch. It anchors a
bounded predecessor lineage; normal admitted group append does not mutate it.
_Avoid_: Root-prefix LIST, per-group pointer update, locally merged lineage

**Binding Context Table**:
The bounded NWG1 table that binds frames to exact Topic Incarnation, binding, Storage Epoch, and Owner Epoch authority
inside a multi-binding ObjectExtent. The WalRun Root does not carry one singular topic epoch.
_Avoid_: Group shard epoch as topic authority, untyped binding summary

**Append Unit Directory**:
The authoritative bounded NWG1 in-body directory for frame ranges, context references, Kafka commit-set membership,
and Pulsar entry units. Sidecars, manifests, and checkpoints are accelerators only.
_Avoid_: Footer-only authority, commit set spanning ObjectExtents, record-count-derived coverage

**Recovery Envelope**:
The cumulative worst-case bound over all work required to recover admitted Object-WAL state. Normal ACK/admission must
preserve it; fallback cannot reset it.
_Avoid_: Takeover timeout only, per-run counter reset, partial recovery success

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
