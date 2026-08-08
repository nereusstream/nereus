# ADR 0009: V2 protocol-native data paths

## Status

Accepted for the V2 design and refined by ADR 0011 and ADR 0013. Implementation and runtime evidence are not started
at M0.

## Context

Kafka and Pulsar have different durable positions, control planes, recovery rules, and feature semantics. A thick generic
append facade would either leak protocol details into the shared core or weaken the native runtime path.

## Decision

V2 shares storage lifecycle concepts but keeps protocol-native execution paths:

- Kafka retains KRaft partition leadership, `UnifiedLog` behavior, producer state, transactions, high watermark,
  leader epochs, and timestamp lookup semantics. Its durable control records use the KRaft backend.
- Pulsar BookKeeper profiles retain native ManagedLedger positions, cursor/retention semantics, and BookKeeper lifecycle.
  Their durable control records use Pulsar MetadataStore/Oxia.
- Pulsar `OBJECT_WAL` uses an ObjectManagedLedger projection whose MessageId and cursor rules are defined explicitly.
- Shared code owns binding-scoped Protocol Coverage, immutable Physical Extent descriptors, manifest generation rules,
  checksums, cache policy, task identity, materialization, and physical-GC proof.

One Storage Fabric may host multiple independent Kafka and Pulsar Protocol Cells. Sharing providers or lifecycle workers
does not permit cross-cell position comparison or grant another cell Native Write Authority. Secondary protocol access
uses an Access Projection; protocol-authority transfer uses a Migration Link.

The control plane is split by capability rather than represented by one universal metadata interface:

- topic binding;
- ownership authority;
- manifest publication;
- logical trim;
- background-work coordination.

KRaft stores durable, low-churn roots. Ephemeral worker coordination must not create an unbounded high-churn KRaft log.
Fast handoff records are hints and never replace the durable primary WAL plus ownership authority.

## Consequences

- Kafka can be optimized against AutoMQ without forcing Kafka abstractions through Pulsar/Oxia.
- Pulsar BookKeeper profiles can preserve the native hot path rather than paying a generic translation penalty.
- Cross-backend conformance tests verify shared invariants, not identical storage operations or lease semantics.
- Some logic is deliberately duplicated at the protocol boundary to preserve native behavior.
- The Shared Storage Context cannot require every protocol to expose a common numeric range.

This decision is tracked by `T-PROTOCOL-01`, `T-META-01`, `T-MULTIPROTOCOL-01`, and scenarios
`V2-META-001`, `V2-MULTIPROTOCOL-001`, `V2-KAF-001`, and `V2-PUL-001`.
