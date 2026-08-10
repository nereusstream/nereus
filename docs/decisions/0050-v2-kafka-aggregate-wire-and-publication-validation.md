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

M1 exposes only the input-only pseudo-config `nereus.storage.profile` with the three closed profile names. User topics
inherit one versioned Deployment default when absent; topic names do not infer a Namespace. Resolution removes only
that exact key from ordinary `ConfigRecord` changes. Key/value matching is case-sensitive with no trim; null, empty, or
unknown value for the exact key is `INVALID_CONFIG`, while unknown `nereus.*` keys retain stock validation/error
precedence.
Resolved value, origin, and policy/catalog version live only in the aggregate. DescribeConfigs may synthesize an
aggregate projection that is `readOnly=true`, has no synonyms, reports explicit input as `DYNAMIC_TOPIC_CONFIG` and
inherited input as `DEFAULT_CONFIG`, and preserves the creation-time default origin. Both
AlterConfigs APIs reject SET/DELETE/APPEND/SUBTRACT, including same-value SET. Thus Aggregate and ConfigRecord cannot
become competing authorities.

Classifier v1 uses the pinned `Topic.isInternal` set: `__consumer_offsets`, `__transaction_state`, and
`__share_group_state`. Those topics reject explicit profile input and use only the versioned internal Deployment
policy. Streams, Connect, MM2, `__remote_log_metadata`, and other application/Admin-created topics follow the user-
topic path; `__cluster_metadata` is not a TopicImage topic. This classification does not exempt stock tiered-storage
bootstrap from replication/minISR admission; M6 must disable it or prove compatible explicit settings. Every live entry
point capable of creating a TopicImage topic uses the same resolution and aggregate kernel; replay/snapshot validates
persisted pairs rather than rerunning policy resolution.

Exactly-one validation is mandatory and runs at the actual `MetadataLoader` image-publication boundary, after the
finalized feature image for that publication is known:

- initial record replay and CreateTopics mapping perform complete semantic validation once;
- ordinary log-delta publication validates only touched topics' incremental identity/back-reference/version invariants
  against the resulting image; it does not rescan all topics or recompute canonical aggregate SHA values, and removal
  also proves no aggregate residue remains;
- replay-local duplicate, unknown-topic, illegal-version, identity, and schema errors fail the candidate publication;
- snapshot load, fresh bootstrap, and an equivalent complete catch-up boundary call `finishSnapshot` and then scan every
  live topic exactly once before publishing the complete image.

CreateTopics and `validateOnly` compute the exact generated count and serialized size of the final cumulative atomic
controller batch, including
`TopicRecord + TopicBindingAggregateRecord + native configuration-derived records* + PartitionRecord* + record/batch
overhead` before returning. Native configuration-derived records currently include `ConfigRecord` plus any applicable
`ClearElrRecord`. An isolated per-topic estimate is insufficient. The stock request-wide 10,000-partition cap and
`POLICY_VIOLATION` remain unchanged. Aggregate-expanded record-count/byte overflow is also per-topic
`POLICY_VIOLATION`. Request-order greedy admission may reject one candidate and continue with a later smaller one; it
does not reorder, backtrack, or solve a knapsack. Request order is the controller-received collection's insertion order;
an unordered caller obtains no stronger winner-order promise. All admitted topics still publish in one atomic
controller batch.

Each topic first becomes an externally side-effect-free `TopicCreateCandidate`. Candidate construction may generate
the UUID required by the complete aggregate and exact sizing, but only admission commits or exposes it. Rejection leaves
no quota, success-map, topic-ID, or record residue. `ConfigRecord` records sort by config name and `PartitionRecord`
records by partition ID; companion configuration-derived records retain their required semantic order. Duplicate
pseudo-config semantics remain an explicit pre-implementation descendant. A pure incremental sizer extracts Raft's
exact `BatchBuilder` record-size logic, uses `MetadataRecordSerde` plus one serialization cache, and consumes the same
effective controller count/byte limits,
including test injection. The existing fit-oriented builder API is not misused as a pure estimator. It sizes every
record once and accumulates with checked arithmetic. Prefix reserialization and maximum-batch buffer
allocation for sizing are forbidden, so work is `O(total generated records)`. The final Raft guard remains defense in
depth rather than the first normal oversized-request detector. No candidate image is available to publishers or
readers before the applicable validation succeeds.

When the current accumulator batch cannot fit a candidate, the final append path recomputes exact size as the first
records of a fresh batch with reset offset deltas before it may report `RecordBatchTooLargeException`. M1 tests that
current-batch-versus-fresh-batch seam so the defense-in-depth guard cannot be the first discoverer or a false reject.

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
- M1 proves record/image authority, pseudo-config removal/read-only projection/AlterConfigs rejection, explicit internal-
  topic policy, exact serialized batch pre-admission, validate-only equality, native partial success, multi-batch
  transactions, touched-topic tracking, removal, snapshot/bootstrap scans, feature ordering, every invalid record class,
  non-disableability, and no ordinary full-image scan or SHA recomputation. M6 proves complete-process behavior.

This decision is refined by ADRs 0082/0083, refines ADRs 0033, 0034, and 0042 and is tracked by `T-META-01`, `T-POLICY-01`,
`V2-KAF-META-002..005`.
