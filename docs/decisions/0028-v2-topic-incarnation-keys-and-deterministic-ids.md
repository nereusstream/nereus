# ADR 0028: V2 Topic incarnation keys and deterministic aggregate IDs

## Status

Accepted for 0.2 topic identity and aggregate publication. Implementation and runtime evidence are not started at M0.

## Context

ADR 0023 selects one immutable physical Topic Binding Aggregate record but leaves its ABA fence, authority key, and
binding/epoch ID generation open. Kafka has a native UUID topic incarnation. Pulsar has no equivalent topic UUID, while
the pinned Nereus selector already has a canonical persistence name, a monotonically increasing binding generation, and
a durable deleted state. Random or time/backend-dependent IDs would prevent an identical retry from converging after a
lost create response.

## Decision

0.2 uses a discriminated `TopicIncarnationIdentity`:

- Kafka: `{topicId: UUID, canonicalTopicName}`. The native `topicId` is the authority and ABA fence; the name is retained
  only for canonical cross-checking. Recreating a deleted name receives a new topic ID and therefore a new incarnation.
- Pulsar: `{canonicalPersistenceName, canonicalTopicName, bindingGeneration > 0}`. A generation belongs permanently to
  that name/incarnation. Only the native selector's durable `DELETED(g)` state may authorize creation of a later
  generation; an old generation is never overwritten or reused.

Aggregate authority keys include the protocol discriminator and typed incarnation. Kafka uses the canonical topic UUID
as its leaf component. Pulsar uses a domain-separated digest of the canonical persistence name followed by a
fixed-width generation component. The value repeats the complete typed incarnation; every read rederives the expected
key and fails closed on key/value, protocol, name, topic-ID, or generation disagreement.

`bindingId` is a domain-separated SHA-256 derivation over canonical length-framed Cell identity plus the complete typed
incarnation. The initial `storageEpochId` is a separate domain-separated SHA-256 derivation over that binding ID plus
epoch ordinal zero. Neither derivation may contain random attempt IDs, wall-clock time, controller offsets, backend
versions, or another retry-dependent fact. Exact retries therefore reproduce the same aggregate bytes and IDs.

The aggregate remains immutable. Enumeration indexes may be added only as repairable hints; a name index or second key
cannot participate in aggregate visibility or override the incarnation-scoped authority.

## Consequences

- `V2-OPEN-META-03` is resolved.
- Kafka delete/recreate ABA is fenced by its native topic UUID; Pulsar pays for an explicit generation selector and
  retained non-reuse evidence.
- Logical schema/versioning and Kafka feature activation are refined by ADRs 0033 and 0034. Kafka image ownership and
  Pulsar deletion/retirement ABA are refined by ADRs 0042 and 0043; exact selector/wire and executable vectors remain
  downstream gates.
- M1 must prove deterministic retry bytes, Kafka same-name/new-topic-ID isolation, Pulsar generation non-reuse,
  key/value collision checks, protocol discriminator checks, and rejection of retry-dependent IDs.

This decision is refined by [ADRs 0033](0033-v2-topic-binding-aggregate-logical-schema-v1.md),
[0034](0034-v2-kafka-feature-level-2-bootstrap-activation.md),
[0042](0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md), and
[0043](0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md), refines ADR 0023, and is tracked by
`T-META-01`, `V2-META-002..005`, and `V2-KAF-META-001..002`.
