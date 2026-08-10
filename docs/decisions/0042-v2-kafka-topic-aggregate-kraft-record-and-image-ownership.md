# ADR 0042: V2 Kafka topic aggregate KRaft record and image ownership

## Status

Accepted for the 0.2 Kafka metadata integration. Implementation and runtime evidence are not started at M0.

## Context

ADRs 0033 and 0034 freeze the logical aggregate schema and bootstrap-only Kafka feature level, but do not decide
whether the physical aggregate is an opaque controller attachment, a parallel image, or native topic metadata. A
parallel lifecycle authority would have to reproduce topic replay, snapshot, and deletion ordering and could expose a
half-created topic at an externally visible publication cut.

## Decision

At finalized `nereus.storage.version=2`, Kafka uses one generated, explicitly typed
`TopicBindingAggregateRecord` at physical wire version 0. It is not an opaque byte blob or attributes map.

`TopicImage` owns exactly one validated aggregate beside the topic's partition images. The canonical snapshot order is:

1. finalized feature records;
2. for each topic, `TopicRecord`;
3. that topic's `TopicBindingAggregateRecord`;
4. that topic's `PartitionRecord` values.

`RemoveTopicRecord(topicId)` removes the complete active topic image, including the aggregate. 0.2 has no separate
aggregate-delete record and no parallel aggregate image with independent lifecycle.

At the actual MetadataLoader image-publication boundary, every topic in `TopicImage`, including internal topics, has
exactly one aggregate whose topic ID and logical schema validate against its `TopicRecord`. Topic names cannot exempt a
topic; the KRaft metadata log itself is not a TopicImage topic. Missing, duplicate, unknown-topic, unknown-version, or
invalid aggregates fail closed. A transient replay state containing a `TopicRecord` without its aggregate may exist
only inside unpublished delta/snapshot construction; it is never published as a usable metadata image. ADR 0050 refines
ordinary publication to validate only touched topics and reserves full live-topic scans for snapshot/bootstrap.

## Consequences

- `V2-OPEN-KAF-META-02` is resolved.
- The Kafka fork must change generated metadata APIs, replay delta/image ownership, snapshot writing, removal, dump
  tooling, and tests.
- Native topic-ID ownership and one publication boundary replace a parallel lifecycle authority.
- API key, validation scope, and publication cut are refined by ADR 0050. Generated schema implementation and
  byte-level snapshot golden vectors remain downstream implementation gates.
- M1 must prove atomic batch visibility, canonical snapshot order, topic-cascaded removal, duplicate/unknown rejection,
  and that no completed image exposes a live Nereus topic without exactly one aggregate.

This decision is refined by ADRs 0050/0082/0083, refines ADRs 0023, 0033, and 0034, and is tracked by `T-META-01`,
`V2-META-002..004`, and `V2-KAF-META-001..005`.
