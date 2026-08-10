# ADR 0050: V2 Kafka aggregate wire and publication validation

## Status

Accepted for the 0.2 Kafka metadata integration. Implementation and runtime evidence are not started at M0.

## Context

ADR 0042 makes the aggregate a generated TopicImage-owned record, but the physical API key and validation cost still
matter. Validating every live topic after each raw Raft batch would become `O(all topics)` even when a transaction spans
several batches or only one topic changed. Validating too early would also reject a legal transient state inside an
unpublished atomic metadata transaction.

## Decision

The Kafka fork reserves positive metadata API-key band `32000..32767` for Nereus extensions.
`TopicBindingAggregateRecord` uses `apiKey=32000`, `validVersions=0`, and strict non-flexible wire v0. Its generated
fields explicitly encode the closed logical aggregate from ADR 0033, including topic identity/back-reference, schema
version, fixed binding/epoch IDs, Cell/protocol identity, closed format discriminators, profile/origin, ordinal zero,
and nullable sealed end. It has no opaque aggregate blob, untyped attributes, mutable lifecycle, or retry fields.

Exactly-one validation is mandatory and runs at the actual `MetadataLoader` image-publication boundary, after the
finalized feature image for that publication is known:

- initial record replay and CreateTopics mapping perform complete semantic validation once;
- ordinary log-delta publication validates only touched topics' incremental identity/back-reference/version invariants
  against the resulting image; it does not rescan all topics or recompute canonical aggregate SHA values, and removal
  also proves no aggregate residue remains;
- replay-local duplicate, unknown-topic, illegal-version, identity, and schema errors fail the candidate publication;
- snapshot load, fresh bootstrap, and an equivalent complete catch-up boundary call `finishSnapshot` and then scan every
  live topic exactly once before publishing the complete image.

CreateTopics computes the complete `TopicRecord + TopicBindingAggregateRecord + PartitionRecord*` record count and
encoded-size admission before returning an atomic result; it does not discover an atomic-batch limit only after append.
No candidate image is available to publishers or readers before the applicable validation succeeds.

No raw batch boundary is treated as a publication boundary when Kafka metadata transactions span batches. No ordinary
delta performs a full live-topic scan. Validation failure is fatal/fail-closed and cannot be configured off. Generated
serde, controller replay, delta/image ownership, snapshot writer, JSON/redaction, metadata dump, and golden-vector
tooling all handle the record explicitly.

## Consequences

- `V2-OPEN-KAF-META-03` is resolved.
- A permanent fork extension band and strict v0 evolution cost are accepted for inspectable metadata and predictable
  upstream collision handling.
- Normal publication work is proportional to touched topics; full `O(all topics)` validation is limited to
  snapshot/bootstrap.
- M1 proves record/image authority, aggregate batch pre-admission, multi-batch transactions, touched-topic tracking,
  removal, snapshot/bootstrap scans, feature ordering, every invalid record class, non-disableability, and no ordinary
  full-image scan or SHA recomputation. M6 proves the corresponding complete-process restart/catch-up behavior.

This decision refines ADRs 0033, 0034, and 0042 and is tracked by `T-META-01`, `T-POLICY-01`,
`V2-KAF-META-002..005`.
