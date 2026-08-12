---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Topic Protocol Binding, Storage Epochs, and profiles

## Immutable protocol binding

A Topic Incarnation is created with one durable protocol binding:

```text
TopicProtocolBinding {
  bindingId
  bindingVersion
  protocolCellId
  protocolKind
  topicIncarnationIdentity
  positionDomain
  payloadMapping
  nativeWriteAuthorityKind
}
```

The binding is resolved before I/O and is immutable for the incarnation. It fixes protocol identity and position
semantics, not physical placement. The current leader/broker holding the authority changes through Owner Epochs, while
the binding's Native Write Authority kind remains Kafka or Pulsar.

Missing, unknown, V1, or internally inconsistent values fail before append or read admission. A Topic Incarnation has
one Position Domain and one Native Write Authority at a time. Positions from another binding or incarnation are not
comparable.

`TopicIncarnationIdentity` is protocol-discriminated. Kafka uses its native topic UUID as authority and retains the
canonical topic name only for cross-checking. Pulsar uses canonical persistence/name facts plus a positive binding
generation, whose prior generation must be durably deleted before a new incarnation is created. Topic names alone are
never authority keys.

## Atomic initial aggregate

The Topic Protocol Binding and its one initial Storage Epoch are physically stored as one immutable
`TopicBindingAggregateRecord`. Kafka appends it inside the existing atomic `CreateTopics` controller result;
MetadataStore/Oxia creates one aggregate key with `putIfAbsent`. One decode yields one
`VersionedAggregateSnapshot`; logical binding and ordinal-zero epoch are domain projections, not child stores, keys,
writes, caches, watches, lists, or future-epoch mutation authorities.

Aggregate authority keys include the protocol kind and complete typed incarnation: Kafka keys use the canonical topic
UUID, while Pulsar keys use a domain-separated canonical-persistence-name digest plus fixed-width generation. Values
repeat the identity and are validated against the key. Bootstrap deployment/reservation-domain/Cell IDs and Kafka
topic UUIDs are create-only, non-zero 16-byte values; `bindingId` and ordinal-zero `storageEpochId` are 32-byte SHA-256
values. They use ADR 0082's exact `NTB1 || u32be(cellLength) || cellBytes || u32be(incarnationLength) ||
incarnationBytes` and `NSE1 || bindingId[32] || u64be(epochOrdinal)` preimages. ADR 0083 fixes NPC1/NTI1 variant layouts:
Pulsar Cell bytes include reservation-domain identity, while compatibility namespace/provider/broker facts do not.
Kafka UUID is raw 16 bytes and rejects ZERO/ONE; Kafka name is at most 249 ASCII bytes; Pulsar generation is positive
signed-long with overflow rejection. Protocol codes are `KAFKA=1`, `PULSAR=2`; zero and all other v1 values fail.
Pulsar uses ADR 0084's single NPN1 persistence-name digest under distinct selector/aggregate leaf prefixes and a
19-digit generation. M1 accepts only ordinal zero, and random attempts, runtime configuration, time, log offsets, and
backend versions cannot influence IDs. NTA1 v1 accepts only classic `persistent://` Pulsar names, caps canonical
persistence/topic names at 4,096 strict UTF-8 bytes each, and requires exact mutual rederivation.

The one logical compatibility axis is `aggregateSchemaVersion=1`. Canonical `NTA1` persists only independent
semantics: protocol, one binding ID, length-framed Cell/incarnation, one ordinal-zero Storage Epoch ID, resolved
profile/origin/catalog SHA, one `FrameEncodingPolicy` pair, and absent sealed end. Position/payload/native authority are
derived from protocol; primary WAL/digest/checksum/encryption are derived from profile plus fixed format contracts; the
binding back-reference is the Aggregate's one binding ID. They are domain views, not repeated wire authorities.
NTA1 is flat/sequential with no TLV/map/self-digest/tail and permits only `0x00` for initial sealed-end presence. Pure
closed enums use one `u16`; only independently evolving typed payloads use kind/version. NTA1 v1 accepts exactly
`NONE={0,0,empty}` and `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`. Kafka/Pulsar `OBJECT_WAL` requires the latter;
`BOOKKEEPER_WAL_ONLY` and `BOOKKEEPER_WAL_ASYNC_OBJECT` require NONE. The exact checked decoder caps are
`maxCellBytes=54`, `maxIncarnationBytes=8,214`, and `maxNta1Bytes=8,397`; the 12.5-percent policy, 16-KiB names, and
64-KiB rounded total are rejected for v1. Deployment may lower only new-write/admission ceilings, never persisted-v1
decoding. Unknown/illegal combinations fail closed. `ACTIVE` is derived only
after complete publication/validation and is not stored. The v1 aggregate excludes `CREATING`, delete/lifecycle/owner
state, timestamps, attempts, controller offsets, backend versions, and untyped attributes. Oxia envelope schema 1 wraps
NTA1; Kafka wire v0 maps generated fields directly to the domain validator without constructing temporary NTA1 bytes.

M1.1a-A now implements the additive modules, frozen identity/ID codecs, minimal independent domain values and direct
foundation validator, and four closed metadata capabilities. Its executable gate owns the production classpath/API
boundary and local goldens. The separate continuity/backend/runtime work has not started. The M1.1b production
encoding/decoding and exact goldens are authorized by the accepted table but remain a separate slice and cannot be
claimed by the foundation.

At Kafka feature level 2, that physical record is one generated, typed, non-flexible
`TopicBindingAggregateRecord(apiKey=32000, wireVersion=0)` owned by `TopicImage`, not an opaque attachment or parallel
image. Completed snapshots write feature records and then, per topic,
`TopicRecord -> TopicBindingAggregateRecord -> PartitionRecord*`; topic removal cascades through the existing
`RemoveTopicRecord`. At actual MetadataLoader publication, ordinary deltas validate only touched/created/removed
topics, while snapshot/bootstrap scans every live topic. Missing, duplicate, unknown-topic, or invalid records fail
closed; a partial topic may exist only inside unpublished replay construction. Validation cannot be disabled.

Pulsar additionally retains one permanent name-scoped `PulsarTopicGenerationSelector`. Its generation is monotonic and
`DELETED(generation)` never becomes absent. A full incarnation aggregate remains immutable until exact reference-free
retirement; then one exact-version CAS may replace it, at the same never-reused key, with a compact permanent
`RetiredTopicIncarnationTombstone` binding the original aggregate and retirement-proof digests. Later generations use
new keys, and lifetime metadata admission counts every selector and tombstone. Selector publication uses only
`RESERVED -> ACTIVE -> DELETING -> DELETED`; exact ACTIVE/aggregate identity is validated and cached at topic
open/ownership/version change, while normal append/read checks the local versioned fence with zero per-access Oxia I/O.

A lost create response rereads the same record and requires exact equality. Missing, mismatched, unknown, or conflicting
aggregate state fails closed and never selects a default epoch. The generic `CREATING` fallback in ADR 0019 is not used
by this 0.2 single-record representation. Normal admitted append does not read or mutate the aggregate remotely. ADRs
0019, 0023, 0028, 0033, 0034, 0042, and 0043 are authoritative. Kafka activates this schema only at fresh-bootstrap finalized
`nereus.storage.version=2`; level-1 replay and runtime upgrade/downgrade are rejected.

## Append-only Storage Epoch chain

Physical placement evolves through an append-only chain:

```text
StorageEpoch {
  storageEpochId
  bindingId
  storageProfile
  startProtocolFrontier
  sealedEndProtocolFrontier?
  primaryWalFormat
  payloadFormat
  objectExtentDigestFamily
  framePayloadChecksumFamily
  encryptionFamily
  lifecycleState/history
}
```

The profile and formats are immutable within one Storage Epoch. Its start frontier is binding-scoped and typed; a sealed
end frontier is assigned once and cannot be moved. Epoch coverage is ordered by the binding's Position Domain, not by a
universal offset. At most one epoch may admit newly allocated positions at a time, and a cutover may not create
overlapping Native Write Authorities or require synchronous dual write.

ADR 0015 fixes the 0.2 runtime to exactly one initial Storage Epoch per Topic Incarnation. The release exposes no online
profile-transition API/state machine and may not create a second epoch for an existing incarnation. The append-only
chain shape and typed-cut/single-admitting-epoch invariants remain durable model contracts for future evolution; they do
not advertise a 0.2 transition feature. Exact future transitions remain deferred in
[V2 open questions](open-questions.md).

## Operational policy

Operational policy is versioned separately and may change only at an explicit activation boundary:

- group linger and target bytes;
- per-topic and per-tenant admission budgets;
- cache and read-ahead limits;
- materialization concurrency and lag thresholds;
- retention duration and compaction cadence;
- observability sampling.

Correctness, recovery, fencing, security/parser hard caps, and durable compatibility are never switches.
Topic/Tenant-or-Namespace policy uses a small closed set of typed classes and cannot enlarge a format cap; Protocol
Cell/shard policy owns shared checkpoint/allocator/group/recovery budgets; host/process configuration supplies only
resource ceilings and may cause backpressure or early seal. Effective numeric budgets are
`min(topic/tenant-or-namespace request, Cell/shard budget, host capacity)`. A resolved value that affects bytes or
recovery is persisted at its Storage Epoch, hard-recovery WalRun Root, Object-group descriptor/header, or offload
attempt and cannot drift after failover. Product/Deployment owns the admitted catalog and base semantic default;
Namespace may inherit/override, Topic may explicitly override, Cell admits/caps, and host only ceilings. Cross-topic
batching requires compatible resolved classes rather than arbitrary per-topic flag combinations. One policy identity
cannot combine Storage-Epoch encoding, Object-group packing, sealed-ledger offload-attempt policy, and host capacity
because those values activate and change at different lifecycle boundaries. Topic-specific soft packing is not one
WalRun Root identity. At most three lazily instantiated lanes share one Root/pointer and aggregate budgets; a binding
moves lane only after its prior lane work converges. Permanent IDs `0/1/2` mean
`OBJECT_LATENCY/OBJECT_BALANCED/OBJECT_COST`; evidence-selected target/linger/quantized changes use
`packingPolicyVersion`, and a group requires identical class/version/resolved policy.

Active-tail readability and combined tracker/locator reservation are non-disableable correctness behavior. A
Binding/tenant may request only a conservative active-tail soft share; Protocol Cell x shard owns recovery concurrency,
and shard/Cell/host owns hard memory ceilings/materialization-pressure triggers. Topic policy cannot enlarge the hard
cap or turn readable-before-ACK off.

Checkpoint provider-proof semantics are also not a Topic policy. Each WalRun Root fixes `NONE` or the admitted
version-bound proof mode plus Provider adapter/canonicalizer version and token hard cap; `NONE` is the default and a
future Root is required for a mode/cap change. Read-view pin and retired-generation hard bounds cannot be disabled or
enlarged by Topic policy; exact evidence-selected ceilings remain Cell/host admission inputs.

Read hazard ordering and complete source-access lifetime are correctness, not policy. Slot-pool/retained-source hard
ceilings belong to Cell/host capacity. Whether unplanned takeover can prove old-reader expiry is a versioned Protocol
Cell/backend admission capability; a Topic cannot enable it, weaken it, or choose deletion despite missing proof.

A policy change that affects a primary WAL profile, format, Object-extent digest family, Frame-payload checksum family,
or encryption family requires a new Storage Epoch at an exact Protocol Frontier. A materialization-only format or index
policy may create a new immutable generation without changing the append epoch. Neither operation rewrites the Topic
Protocol Binding.

## Profile contracts

For Kafka, all three profiles share one Kafka Offset Domain. Profile selection changes the primary-WAL ACK boundary,
source-local physical index, preferred read source, and retirement eligibility; it never creates a profile-specific
offset or consumer-group cursor. Object generations use an authenticated Object directory. BookKeeper-primary
generations use ADR 0086's per-partition ledger run and packed in-ledger RecordBatch range index. A physical generation
switch preserves exact Kafka coverage. ADR 0087 further separates Allocated, profile-Durable, Readable/LEO, HW, and
LSO. Profile durability may advance before Kafka visibility; Object materialization never advances LEO/HW/LSO.

### `OBJECT_WAL`

The Object group is the primary durable WAL. ACK waits for verified provider durability of the complete typed Protocol
Coverage plus owner-local locator and Kafka producer/transaction/leader-epoch publication. The WalRun Root/key/LIST
contract makes the immutable group recoverable; no per-commit-set manifest mutation or async checkpoint page is added
to the ACK cut.
The profile accepts batching latency in exchange for lower storage and request cost. Post-ack materialization remains
asynchronous and cannot weaken readability of acknowledged WAL ranges.

### `BOOKKEEPER_WAL_ONLY`

ACK waits for the configured BookKeeper quorum and coherent owner-local protocol publication. Object storage is not
required for append, recovery, read, or retention.
The profile explicitly accepts BookKeeper cost in exchange for low-latency quorum writes.

### `BOOKKEEPER_WAL_ASYNC_OBJECT`

ACK has the same BookKeeper and protocol-publication boundary as the WAL-only profile. Sealed Protocol Coverage is asynchronously materialized
to Object storage. Lag policy may throttle or stop admission, but it never changes a completed ACK into a synchronous
Object wait.
For Kafka, BookKeeper source deletion requires the Nereus manifest and source-protection proof. For Pulsar, native
ManagedLedger ledger/offload metadata is the sole offload and deletion-eligibility authority; a Nereus manifest is
derived and cannot independently delete a native ledger. See [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md).

## System topics

Kafka classifier-v1 built-ins and Pulsar system topics use explicit initial-epoch profile policy; they never inherit a
tenant default without validation. Kafka exposes only exact, case-sensitive, no-trim `nereus.storage.profile` with the
three profile names. It is removed from native `ConfigRecord` changes, the resolved value/origin/catalog-policy version
lives only in the aggregate, DescribeConfigs synthesizes a read-only/no-synonym projection, and every AlterConfigs
operation is rejected. The three pinned built-ins reject explicit input; Streams/Connect/MM2,
`__remote_log_metadata`, and other topics use the Deployment user-topic default/explicit-input path without name-
inferred Namespace. Duplicate exact pseudo-keys are last-wins and the native `CreateTopicPolicy` sees only the view
after pseudo removal. V2 admission requires `remote.log.storage.system.enable=false`; M1 proves the fail-closed
interlock and M6 proves RLMM remains inactive. Every successful TopicImage topic owns an aggregate;
the KRaft metadata log is not such a topic. A protocol adapter may
restrict the allowed initial profile set when its recovery or transaction authority cannot satisfy the contract. No
adapter exposes an online transition set in 0.2.

Relevant tradeoffs: `T-PROFILE-01`, `T-MIGRATION-01`, `T-POLICY-01`, `T-OBJECT-01`, and `T-BK-01`. Required scenarios:
`V2-PROFILE-001`, `V2-POLICY-001..002`, `V2-MIGRATION-001`, `V2-META-002..007`, and
`V2-KAF-META-001..005`. See
[ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md),
[ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md),
[ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md),
[ADR 0033](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md),
[ADR 0034](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md),
[ADR 0042](../decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md),
[ADR 0043](../decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md),
[ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md),
[ADR 0050](../decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md), and
[ADR 0051](../decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md), with NPD1 policy authority refined
by [ADR 0057](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md) and Object-WAL lanes by
[ADRs 0060](../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md) and
[0062](../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md), and M1 exact domain/control boundaries by
[ADRs 0082](../decisions/0082-v2-m1-domain-and-control-authority-contracts.md) and
[0083](../decisions/0083-v2-m1-wire-control-and-evidence-bounds.md).
