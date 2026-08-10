# ADR 0033: V2 Topic Binding Aggregate logical schema v1

## Status

Accepted for the 0.2 metadata model. Implementation and runtime evidence are not started at M0.

## Context

ADRs 0023 and 0028 make the complete binding plus initial Storage Epoch one immutable, incarnation-scoped record, but
leave logical schema evolution and the distinction between immutable identity and mutable lifecycle state open. Giving
the binding and epoch independent reader versions would create combinations that every backend, snapshot, and retry
path must understand. Retry-dependent operational fields would also break exact whole-record equality after a lost
create response.

## Decision

0.2 has one logical compatibility axis: `aggregateSchemaVersion=1`. `TopicBindingAggregateV1` contains exactly one
complete binding and its ordinal-zero initial epoch.

The binding payload contains:

- deterministic binding ID, binding/schema version, Protocol Cell identity, and protocol kind;
- the complete typed `TopicIncarnationIdentity` from ADR 0028;
- closed kind/version discriminators for Position Domain, payload mapping, and Native Write Authority.

The initial epoch payload contains:

- deterministic Storage Epoch ID, binding-ID back-reference, and ordinal zero;
- selected profile, typed origin, and the source-qualified policy/catalog version used for resolution;
- an absent sealed end;
- closed kind/version discriminators for WAL, payload, checksum, compression, and encryption families.

Every discriminator is a closed wire value. A semantically inapplicable value uses an explicit `NONE`; unknown values,
unknown required versions, illegal combinations, missing required fields, non-zero initial ordinal, a present initial
sealed end, or a binding/epoch back-reference mismatch fail closed.

`ACTIVE` is the derived visibility result of successfully publishing and validating the complete aggregate. It is not a
stored field. `TopicBindingAggregateV1` does not store `CREATING`, deletion/lifecycle state, owner state, timestamps,
random attempts, controller offsets, backend versions, or an untyped attributes map.

`NTA1` is the canonical domain aggregate payload. MetadataStore/Oxia's normal envelope with `schemaVersion=1` and
`minReaderSchemaVersion=1` only wraps those bytes. Kafka physical controller-record wire version 0 maps its generated
fields directly to the logical object and validator; it neither embeds nor constructs temporary NTA1 bytes. Identity
preimages use their own stable canonical sub-encodings and depend on neither physical representation.

NTA1 freezes field order, enum codes, presence bytes, `u32be` lengths/counts, and fixed-array widths. Strict UTF-8
encoding/decoding rejects malformed or unmappable input without NFC, lowercasing, or replacement. Decoders reject
unknown discriminators, illegal presence, overflow, non-canonical values, and trailing bytes. Version 1 has no unknown
optional-field extension path; evolution requires NTA2/logical schema v2 and a new Kafka wire version. Parser caps are
derived once from pinned protocol limits, then frozen as v1 format constants. Deployment may lower only new-write or
admission ceilings; reading persisted NTA1 always uses the full fixed v1 parser cap. Runtime cannot enlarge, recompute,
or silently lower that decoder contract. A read followed by canonical logical re-encode preserves exact aggregate
equality.

## Consequences

- `V2-OPEN-META-04` is resolved.
- Whole-record schema evolution is heavier than independently versioning children, but eliminates reader-version
  combination explosion and preserves exact retry equality.
- Kafka image ownership/snapshot order and Pulsar deletion/retirement are refined by ADRs 0042/0043. Kafka physical
  field/API IDs and publication validation plus the exact Pulsar selector/cache state machine are refined by ADRs
  0050/0051; executable byte vectors and a future schema-v2 evolution protocol remain downstream gates.
- M1 must prove NTA1 byte/golden vectors, strict UTF-8, every length/count/overflow/trailing-byte boundary, closed-
  discriminator rejection, required/`NONE` combinations, ordinal/back-reference validation, backend-to-logical
  equality without Kafka temporary NTA1 allocation, deterministic re-encode, fixed parser caps, and exclusion of every
  mutable/retry-dependent field.

This decision is refined by ADRs 0042/0043, 0050/0051, and 0082, refines ADRs 0023/0028, and is tracked by `T-META-01`,
`V2-META-002..007`, and `V2-KAF-META-002..005`.
