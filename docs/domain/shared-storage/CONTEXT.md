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
start/sealed-end frontiers. Lifecycle changes are append-only state history; the exact state vocabulary is open.
_Avoid_: Mutable profile, storage mode flag

## Projection and migration

**Access Projection**:
A read/access relationship that exposes data governed by one Native Write Authority through another protocol without
granting the target protocol position-allocation authority.
_Avoid_: Dual-native topic, shared writer

**Projection Map**:
A durable mapping between source and target Protocol Coverage for an Access Projection or migration.
_Avoid_: Global offset map, per-message control-plane commit

**Migration Link**:
The explicit authority-transfer relationship between a source Topic Protocol Binding and a target Topic Protocol
Binding.
_Avoid_: Storage Epoch, profile switch

## Open question

The minimum hard resource/failure-isolation boundary for Protocol Cells is not yet frozen. Namespace, quota,
credentials, encryption, noisy-neighbor containment, and operator ownership remain tracked by
[`V2-OPEN-FABRIC-01`](../../v2/open-questions.md).
