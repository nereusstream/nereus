# ADR 0011: V2 position domains and multi-protocol Storage Fabric

## Status

Accepted for the V2 design. Implementation and runtime evidence are not started.

## Context

Kafka Offset and Pulsar Position have different ordering, adjacency, rollover, batch, cursor, and recovery semantics.
Shared storage lifecycle code still needs a common way to describe which protocol-owned data occupies which physical
bytes. Treating both as one `long logicalOffset` would create a second Pulsar position truth and leak physical
BookKeeper/Object coordinates into protocol contracts.

## Decision

Kafka and Pulsar do not share a universal numeric position. A Kafka Topic Partition owns `KafkaOffset`; a Pulsar
Managed Ledger owns `PulsarPosition(ledgerId, entryId)`. Every position and comparison is scoped by Topic Protocol
Binding and Topic Incarnation, so equal numeric values from different topics or incarnations are unrelated.

Nereus may nevertheless run multiple Kafka and Pulsar Protocol Cells in one Storage Fabric. The Shared Storage Context
describes data with a typed `ProtocolCoverage`—`KafkaOffsetRange` or ledger-keyed `PulsarCoverage`—and separately
describes placement with `PhysicalExtent`—Object or BookKeeper. BookKeeper entry coordinates do not become Kafka
offsets, and Object keys do not become Pulsar positions.

ADR 0014 defines the sharing boundary: cells may use the same external Provider Infrastructure, but each cell keeps a
distinct Cell Provider Scope and independently owned sessions, queues, cache/task namespaces, and GC authority. Shared
infrastructure does not imply shared runtime correctness state or cross-cell Object groups.

The valid typed combinations are:

| Protocol/storage path | Protocol Coverage | Physical Extent |
| --- | --- | --- |
| Kafka / Object WAL | `KafkaOffsetRange` | `ObjectExtent` |
| Kafka / BookKeeper | `KafkaOffsetRange` | `BookKeeperExtent` |
| Pulsar / BookKeeper | `PulsarCoverage` | `BookKeeperExtent` |
| Pulsar / Object WAL | `PulsarCoverage` | `ObjectExtent` |

A Pulsar range crossing ledgers is a collection of per-ledger entry ranges whose order is proven by the ManagedLedger
ledger chain. For Pulsar Object WAL, ADR 0022 assigns virtual-ledger ID allocation and explicit chain order to a
Pulsar-cell MetadataStore/Oxia authority; Object identity remains only a Physical Extent.
V2 does not persist `ledgerBase + entryId` as a second Pulsar position truth.

## Consequences

- Shared manifests, handoff, trim, recovery, and GC carry binding-scoped typed coverage and frontiers.
- Shared algorithms must dispatch ordering/adjacency through the binding's Position Domain.
- Multiple protocol cells may share physical Object Storage or BookKeeper infrastructure, compatible transport pools,
  worker processes, and observability while retaining cell-scoped sessions, resource accounting, cache/task roots, and
  physical-delete authority.
- A Topic Incarnation has exactly one Position Domain and one Native Write Authority at a time.

This refines ADR 0007 and ADR 0009 and is further refined by ADR 0014 and ADR 0022. It is tracked by
`T-POSITION-01`, `T-MULTIPROTOCOL-01`, `T-FABRIC-01`, `V2-POSITION-001..002`, `V2-MULTIPROTOCOL-001`, and
`V2-FABRIC-001..003`.
