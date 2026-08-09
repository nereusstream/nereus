# ADR 0023: V2 Topic Binding Aggregate physical record

## Status

Accepted for 0.2 topic creation. Implementation and runtime evidence are not started at M0.

## Context

ADR 0019 requires one atomically visible Topic Binding Aggregate but permits several physical representations. The
Kafka controller can append a bounded atomic record batch, while the pinned MetadataStore/Oxia public API provides
single-key conditional writes rather than an all-conditions-or-no-writes multi-key transaction. Persisting binding and
initial epoch separately would therefore require a backend-specific create saga even though 0.2 never mutates either
component after creation.

## Decision

0.2 uses one immutable `TopicBindingAggregateRecord` as the physical authority for the complete Topic Protocol Binding
and its one initial Storage Epoch:

- Kafka appends that composite record inside the existing atomic `CreateTopics` controller result. The binding and
  epoch do not have separately authoritative controller records.
- MetadataStore/Oxia creates one aggregate key with `putIfAbsent`. A lost create response is resolved by rereading the
  same key and requiring exact aggregate equality; a mismatch is a conflict and fails closed.
- `TopicProtocolBindingStore` and `StorageEpochStore` expose typed logical views projected from this one record. A
  projected view cannot be independently created, replaced, or activated.
- The record is immutable for one Topic Incarnation. Repeated create with the same identity and exact value converges;
  a different value cannot reuse the record identity.
- The `CREATING` intent path from ADR 0019 is not used by the 0.2 composite representation. A future requirement to
  split authoritative state across keys/records requires a new accepted contract before activating that path.
- Topic open caches the complete record after validation. Normal admitted append performs no remote aggregate read or
  mutation.

Topic incarnation, authority-key, and deterministic-ID rules are refined by ADR 0028. Exact aggregate field encoding,
schema evolution, deletion/recreation retirement, and snapshot/replay conformance remain downstream implementation
gates.

## Consequences

- `V2-OPEN-META-02` is resolved.
- Both metadata backends implement the same visible aggregate without requiring Oxia multi-key transactions.
- Whole-record versioning and equality replace partial-record repair in 0.2.
- M1 must prove atomic Kafka replay/snapshot publication, Oxia create-response-loss convergence, conflicting retry
  rejection, typed-view equality, and absence of independently writable child records.

This decision is refined by [ADR 0028](0028-v2-topic-incarnation-keys-and-deterministic-ids.md), refines ADR 0019, and is
tracked by `T-META-01`, `V2-PROFILE-001`, `V2-META-002`, and `V2-META-003`.
