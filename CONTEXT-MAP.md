# Nereus V2 Context Map

## Contexts

- [Shared Storage](./docs/domain/shared-storage/CONTEXT.md) — owns storage-profile epochs, typed protocol coverage,
  physical extents, manifests, materialization, and safe physical lifecycle.
- [Kafka](./docs/domain/kafka/CONTEXT.md) — owns Kafka topic-partition identity, offsets, protocol state, and Kafka-native
  write authority.
- [Pulsar](./docs/domain/pulsar/CONTEXT.md) — owns Pulsar topic/ManagedLedger identity, positions, MessageIds, cursors,
  and Pulsar-native write authority.

KoP is outside the current V2 Kafka/Pulsar design session. Its existing design remains retained but deferred.

## Relationships

- **Kafka → Shared Storage**: a Kafka Topic Protocol Binding selects the Kafka Position Domain; Kafka publishes
  binding-scoped Kafka Offset Range coverage over physical extents. Object WAL preserves assigned RecordBatch Frames
  and makes one partition storage append visible as an all-or-none Kafka Append Commit Set.
- **Pulsar → Shared Storage**: a Pulsar Topic Protocol Binding selects the Pulsar Position Domain; Pulsar publishes
  binding-scoped Pulsar Coverage over physical extents. In Object WAL mode, a Pulsar-cell virtual-ledger authority owns
  ledger IDs and explicit Ledger Chain order under one deployment-wide reservation registry; Object identity remains
  physical only. One ManagedLedger entry remains one Pulsar Frame. In async-offload mode, one native sealed-ledger
  attempt maps to one deterministic Sealed-Ledger Object Pair with a bounded root.
- **Kafka ↔ Pulsar**: neither context compares or allocates the other's positions. Shared access uses an Access
  Projection with one Native Write Authority; an authority transfer uses a separate Migration Link. ADR 0016 retains
  these boundaries but excludes their runtime from 0.2.
- **Protocol Cells ↔ Storage Fabric**: multiple Kafka and Pulsar Protocol Cells may share external provider
  infrastructure and capacity pools, but each cell owns a separate Cell Provider Scope/session, resource accounting,
  task/cache namespace, and GC authority; they never share a position domain or write authority.
