# ADR 0012: V2 storage epochs and profile evolution

## Status

Accepted as the V2 domain model. ADR 0015 fixes the 0.2 runtime scope to one initial epoch per Topic Incarnation and no
online transition API/state machine.

## Context

Topic-level profiles are required, but binding a profile permanently to an entire Topic Incarnation prevents explicit
same-protocol evolution between cost-first and performance-first paths. Treating profile as a mutable flag would instead
hide a distributed cutover, recovery, read, retention, and GC protocol. Protocol identity must remain stable while
physical placement changes at an auditable native frontier.

## Decision

V2 separates immutable protocol identity from storage placement:

- `TopicProtocolBinding` is immutable for one Topic Incarnation and fixes Protocol Cell, Position Domain, payload
  mapping, and Native Write Authority.
- `StorageEpoch` is one immutable interval in that binding's append-only storage history and fixes its profile, format,
  checksum/encryption family, and protocol-native start/end frontiers.

A binding may contain a chain of Storage Epochs. At most one epoch admits new positions at a time. A transition must
durably establish one exact protocol-native cutover frontier and prevent overlapping epoch write authority; it does not
require dual write or a cross-protocol global offset. The exact order of drain, seal, activation, failure recovery, and
rollback remains open. Readers resolve historical coverage through the epoch chain and its physical extents.

This decision permits future profile evolution. ADR 0015 explicitly does not activate it in 0.2: the runtime creates one
initial epoch and rejects same-incarnation profile transition. Exact future states, rollback boundaries, transition
matrix, and historical-data policy remain deferred in [V2 open questions](../v2/open-questions.md).

## Consequences

- ADR 0010's immutable profile binding is superseded; only Topic Protocol Binding remains immutable.
- A profile is immutable within one Storage Epoch, not forever within a Topic Incarnation.
- Physical history may be materialized or retired after cutover only through normal reader, retention, projection, task,
  and GC proofs.
- Pulsar BookKeeper/Object transitions cannot be assumed to work inside one native ManagedLedger until their dedicated
  open gate is closed.

This decision is refined by [ADR 0015](0015-v2-0.2-storage-epoch-runtime-scope.md) and tracked by `T-PROFILE-01`,
`T-MIGRATION-01`, `V2-PROFILE-001`, and `V2-MIGRATION-001`.
