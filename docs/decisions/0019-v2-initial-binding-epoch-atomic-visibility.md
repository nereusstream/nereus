# ADR 0019: V2 initial binding and Storage Epoch atomic visibility

## Status

Accepted for 0.2 topic creation. Implementation and runtime evidence are not started at M0.

## Context

ADR 0015 permits exactly one initial Storage Epoch for a new Topic Incarnation in 0.2. Topic open cannot safely choose a
primary WAL, Position Domain contract, format, checksum, or encryption family if the immutable Topic Protocol Binding
and initial epoch become visible independently. A timeout, retry, controller/store failover, or concurrent open must not
invent a default epoch or admit I/O against a half-created topic.

Kafka KRaft and Pulsar MetadataStore/Oxia have different mutation APIs, so the contract must define atomic visibility
rather than require identical backend primitives.

## Decision

The Topic Protocol Binding and its initial Storage Epoch form one visible `TopicBindingAggregate`:

- A backend that can publish both records in one replay-atomic transaction/batch does so and exposes the aggregate only
  after the whole batch is durable.
- Otherwise the backend first creates one deterministic `CREATING` intent that binds the topic incarnation, binding ID,
  initial Storage Epoch ID, profile, format family, Object-extent digest family, Frame-payload checksum family,
  encryption family, and create-attempt identity. Retries reuse that identity and idempotently complete the immutable
  records before one fenced transition publishes `ACTIVE`.
- Open, ownership acquisition, append, and read admit only an internally complete `ACTIVE` aggregate. `CREATING`, a
  missing half, mismatched identities, unknown versions, or an uncertain activation outcome triggers bounded recovery
  or fail-closed rejection; no caller derives a default binding or epoch.
- Recovery may finish or safely abandon an unexposed create attempt, but it cannot replace one component while retaining
  the other or publish a second initial epoch for the same Topic Incarnation.
- Aggregate creation/open is low-frequency control-plane work. It adds no remote metadata read or mutation to normal
  admitted append.

The backend representation may be one aggregate record, multiple immutable records behind one active root, or one
replay-atomic record batch, provided the externally observable and recovery semantics above are identical.

## Consequences

- `V2-OPEN-META-01` is resolved.
- Non-transactional backends pay for a durable intent and recovery path instead of exposing partial state.
- Topic create may remain uncertain or retriable longer, but append/read never guess around that uncertainty.
- M1 conformance must cover duplicate create, lost create/activation response, half-written records, conflicting retry,
  backend failover, concurrent open, and cleanup of an unexposed attempt.

This decision refines ADR 0015 and is tracked by `T-META-01`, `V2-PROFILE-001`, `V2-META-001`, and `V2-META-002`.
