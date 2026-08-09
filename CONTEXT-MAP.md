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
  and makes one partition storage append visible as an all-or-none Kafka Append Commit Set co-located in one NWG1
  ObjectExtent. KRaft activates aggregate schema v1 only at fresh-bootstrap feature level 2; the generated aggregate
  record is owned, snapshotted, and removed with its native TopicImage.
- **Pulsar → Shared Storage**: a Pulsar Topic Protocol Binding selects the Pulsar Position Domain; Pulsar publishes
  binding-scoped Pulsar Coverage over physical extents. In Object WAL mode, a Pulsar-cell virtual-ledger authority owns
  ledger IDs and explicit Ledger Chain order under one deployment-wide reservation registry with permanent fixed-slice
  assignments that cannot be resized or extended; a RANGE candidate preserves an incarnation-owned installed grant
  across owner fencing and never burns its whole tail merely on takeover. Object identity remains physical only. One
  ManagedLedger entry remains one Pulsar Frame. In async-offload mode, one native sealed-ledger attempt maps to one
  deterministic NPD1-data/NPO1 pair with a ManagedLedger-owned dual-source handle, whole-range fallback, BK-pin drain,
  and final Object revalidation before BookKeeper-source deletion.
- **Kafka ↔ Pulsar**: neither context compares or allocates the other's positions. Shared access uses an Access
  Projection with one Native Write Authority; an authority transfer uses a separate Migration Link. ADR 0016 retains
  these boundaries but excludes their runtime from 0.2.
- **Protocol Cells ↔ Storage Fabric**: multiple Kafka and Pulsar Protocol Cells may share external provider
  infrastructure and capacity pools, but each cell owns a separate Cell Provider Scope/session, resource accounting,
  task/cache namespace, and GC authority; they never share a position domain or write authority.
- **Policy ↔ durable state**: Topic/Tenant-or-Namespace typed classes express SLO/cost intent, Protocol Cell/shard
  policy owns shared scheduling and recovery budgets, and host/process configuration only caps resources. Correctness
  and parser caps are never switches; Product/Deployment owns the base semantic default, while Cell/host cannot replace
  it. Durable choices are persisted at their Storage Epoch, hard-recovery WalRun Root, Object-group, or offload-attempt
  boundary, one identity never spans those lifecycles, and effective budgets are the minimum across scopes. Object WAL
  maps compatible classes to at most three lazy lanes under one Root/pointer and one vector checkpoint chain; all hard
  recovery/resource budgets remain aggregate. Provider-resolved lane extent order is separate from each binding's
  typed Durable Frontier, so physical checkpoint progress never waits for every member's protocol ACK. Before position
  allocation, tracker and active-tail locator capacity reserve together; one shared verified extent feeds compact
  protocol-specific locator ranges, which publish before Readable/Durable frontiers and ACK. The mechanism and hard
  caps cannot be disabled by Topic policy. Provider-proof mode/canonicalizer/token cap belong to the WalRun Root and
  default to `NONE`. A logical Binding read snapshot separates allocation-free high-frequency frontier publication
  from low-frequency source-selection generations and bounded read pins. Generation-tagged hazard publication prevents
  pin-after-retire/torn-frontier reads; durable no-fallback selection plus capability-qualified current/old-owner
  quiescence precedes exact protection release. Neither becomes a Topic switch or remote per-read refcount.
