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

Bootstrap `deploymentId`, `reservationDomainId`, protocol-specific `CellId`, and Kafka `topicId` are create-only,
non-zero 16-byte identities. They are never derived from names/configuration; rebuilding a Cell creates a new Cell ID.
`bindingId` and `storageEpochId` are fixed 32-byte SHA-256 outputs. Kafka topic UUIDs are encoded as raw 16 bytes and
never as strings.

The exact outer derivation preimages are:

```text
NTB1 || u32be(cellLength) || cellBytes
     || u32be(incarnationLength) || incarnationBytes

NSE1 || bindingId[32] || u64be(epochOrdinal)
```

M1 accepts only ordinal zero. `cellBytes` and `incarnationBytes` use independent stable canonical domain sub-encodings;
they cannot depend on Oxia/Kafka physical wire, Java serialization, runtime configuration, or retry state. Neither
derivation may contain random attempt IDs, wall-clock time, controller offsets, backend versions, or another retry-
dependent fact. Exact retries reproduce the same IDs, and derivation runs only on create/replay rather than append/read.

The aggregate remains immutable. Enumeration indexes may be added only as repairable hints; a name index or second key
cannot participate in aggregate visibility or override the incarnation-scoped authority.

## Consequences

- `V2-OPEN-META-03` is resolved.
- Kafka delete/recreate ABA is fenced by its native topic UUID; Pulsar pays for an explicit generation selector and
  retained non-reuse evidence.
- Logical schema/versioning and Kafka feature activation are refined by ADRs 0033 and 0034. Kafka image ownership and
  Pulsar deletion/retirement ABA are refined by ADRs 0042 and 0043. Kafka physical wire/publication validation and the
  Pulsar exact selector/cache state machine are refined by ADRs 0050 and 0051; executable vectors remain downstream.
- M1 must prove exact NTB1/NSE1 golden vectors and lengths, raw UUID encoding, deterministic retry bytes, bootstrap-ID
  non-zero/create-only behavior, Kafka same-name/new-topic-ID isolation, Pulsar generation non-reuse, key/value
  collision checks, protocol discriminator checks, and rejection of retry-dependent IDs.

This decision is refined by [ADRs 0033](0033-v2-topic-binding-aggregate-logical-schema-v1.md),
[0034](0034-v2-kafka-feature-level-2-bootstrap-activation.md),
[0042](0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md), and
[0043](0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md),
[0050](0050-v2-kafka-aggregate-wire-and-publication-validation.md), and
[0051](0051-v2-pulsar-selector-state-machine-and-cached-fence.md), and
[0082](0082-v2-m1-domain-and-control-authority-contracts.md), refines ADR 0023, and is tracked by `T-META-01`,
`V2-META-002..007`, and `V2-KAF-META-001..005`.
