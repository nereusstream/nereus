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
- selected profile and typed origin;
- an absent sealed end;
- closed kind/version discriminators for WAL, payload, checksum, compression, and encryption families.

Every discriminator is a closed wire value. A semantically inapplicable value uses an explicit `NONE`; unknown values,
unknown required versions, illegal combinations, missing required fields, non-zero initial ordinal, a present initial
sealed end, or a binding/epoch back-reference mismatch fail closed.

`ACTIVE` is the derived visibility result of successfully publishing and validating the complete aggregate. It is not a
stored field. `TopicBindingAggregateV1` does not store `CREATING`, deletion/lifecycle state, owner state, timestamps,
random attempts, controller offsets, backend versions, or an untyped attributes map.

MetadataStore/Oxia uses its normal envelope with `schemaVersion=1` and `minReaderSchemaVersion=1`. Kafka's physical
controller record wire version 0 maps to the same logical v1. The two physical encodings need not have identical bytes,
but they use one logical validator and shared golden semantic vectors. A read followed by canonical logical re-encode
must preserve exact aggregate equality.

## Consequences

- `V2-OPEN-META-04` is resolved.
- Whole-record schema evolution is heavier than independently versioning children, but eliminates reader-version
  combination explosion and preserves exact retry equality.
- Kafka image ownership/snapshot order and Pulsar deletion/retirement are refined by ADRs 0042/0043. Kafka physical
  field/API IDs and publication validation plus the exact Pulsar selector/cache state machine are refined by ADRs
  0050/0051; executable byte vectors and a future schema-v2 evolution protocol remain downstream gates.
- M1 must prove closed-discriminator rejection, required/`NONE` combinations, ordinal/back-reference validation,
  backend-to-logical equality, deterministic re-encode, and exclusion of every mutable/retry-dependent field.

This decision is refined by ADRs 0042/0043 and 0050/0051, refines ADRs 0023/0028, and is tracked by `T-META-01`,
`V2-META-002..006`, and `V2-KAF-META-002..003`.
