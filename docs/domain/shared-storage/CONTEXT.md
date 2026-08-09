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
The atomically visible create/open unit containing one immutable Topic Protocol Binding and its one initial Storage
Epoch. A `CREATING` or internally incomplete aggregate admits no read, append, or ownership.
_Avoid_: Default epoch, partially visible topic

**Object Extent Digest**:
Integrity over the exact canonical Object-provider request body after Nereus compression and client-side encryption.
It proves stored-object bytes and cannot replace a frame payload checksum.
_Avoid_: ETag, decoded payload checksum

**Frame Payload Checksum**:
Integrity over the canonical decoded protocol payload/record bytes defined by the binding payload mapping. It cannot
prove the exact Object-provider request body.
_Avoid_: Object extent digest, provider version

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
