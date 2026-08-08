# ADR 0013: V2 cross-protocol projection and migration boundary

## Status

Accepted as an architectural boundary. ADR 0016 excludes Kafka/Pulsar projection and migration runtime from 0.2.

## Context

The same business data may need secondary access or a later authority transfer between Kafka and Pulsar. Modeling that as
one shared position domain or as a Storage Epoch change would obscure protocol-owned offsets/positions and could admit
two control planes as concurrent writers. Position mapping also does not by itself translate cursor/group, batch,
transaction, compaction, delivery, schema, or deduplication semantics.

## Decision

The same business data may be exposed through Kafka and Pulsar only with one Native Write Authority. An
`AccessProjection` maps source Protocol Coverage to target Protocol Coverage through a durable `ProjectionMap`; it
does not let the target protocol allocate positions into the source Topic Incarnation.

Changing the native protocol is not a Storage Epoch transition. It uses a separate `MigrationLink` between a source
and target Topic Protocol Binding, with an explicit authority-transfer cut and projection checkpoint. Two protocols may
never be simultaneous native writers for one Topic Incarnation; that would require a cross-control-plane sequencer and
reintroduce per-append coordination.

Mapping granularity, cursor/group transfer, batches, transactions, compaction, delayed delivery, Key_Shared, schema,
headers/properties, and producer deduplication remain open questions. A universal logical offset would not solve those
semantic translations.

## Consequences

- Shared read access and authority migration are distinct domain concepts.
- A Storage Epoch changes physical placement under one protocol; a Migration Link changes protocol authority.
- Projection-map granularity, format, and lifecycle require a later accepted decision.
- KoP remains outside the current session and 0.2 runtime scope.

This decision is refined by [ADR 0016](0016-v2-0.2-cross-protocol-runtime-scope.md) and tracked by
`T-PROJECTION-01`, `V2-PROJECTION-001`, and the [V2 open questions](../v2/open-questions.md).
