---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: ImplementationDesign
sourceTuple: v2-m0
receipt: docs/v2/evidence/v2-m1/k1/k1-focused.json
---

# K1 Kafka KRaft metadata authority

## Status and authority boundary

This is the code-level design for K1. It translates accepted ADRs 0033, 0034, 0042, 0050, 0081, and 0083 into one
reviewable Kafka-fork implementation. Those ADRs remain normative and win on conflict. K1 implements only Kafka's
metadata authority: feature bootstrap, generated record wire, aggregate construction and replay, image publication,
snapshot/removal, CreateTopics admission, and the read-only Admin projection. Produce, Fetch, replica protocol,
BookKeeper/Object data paths, broker storage activation, and end-to-end process evidence remain M6 or later.

The implementation base is the clean source-locked Kafka fork
`76f62f3b83e882105219b6c7687dbde594a8b8a2`. K1 consumes only the immutable N1 domain artifact version
`0.2.0-n1.330aaec349c51fb2ace52b1085e8a9e5a60b5e3e`; Kafka `:metadata` imports `nereus-domain` and does not import
`nereus-metadata-spi`, Oxia, a Nereus composite build, a changing dependency, or a SNAPSHOT. K1 evidence remains
focused and non-promotable until N2/N3.

## Activatable source tuple

The following changes are dormant or active as one coherent source tuple:

1. `nereus.storage.version=2` support and fresh-format-only activation;
2. generated API-key-32000 `TopicBindingAggregateRecord` wire v0;
3. direct generated-record/domain mapping and validation;
4. controller replay and `TopicImage`/`TopicDelta`/`TopicsDelta` ownership;
5. snapshot order, removal, and publication-boundary validation;
6. CreateTopics aggregate resolution and exact cumulative batch admission;
7. DescribeConfigs projection and both AlterConfigs rejection paths.

The fork must not advertise, format, or emit feature 2 until all seven are present. Level 1 remains V1 and is rejected.
Missing feature metadata is stock/disabled state; runtime transitions to or from level 2 are rejected even through an
unsafe-update option. Only fresh KRaft formatting may establish level 2.

## Immutable artifact boundary

The Kafka repository contains a source-qualified copy of the exact N1 Maven layout under:

```text
gradle/locked-artifacts/nereus-n1/
  330aaec349c51fb2ace52b1085e8a9e5a60b5e3e/m2/
```

An exclusive repository rule admits only `com.nereusstream:nereus-domain` at the exact source-qualified version.
The `:metadata` runtime and test classpaths must contain exactly one matching component and JAR SHA
`2c605ef675c388953f3d2046e02f17bff6b7273a04e4ab8d09cf60be59095600`. The gate also checks the POM SHA
`39ca614a8be63e1ef737e808d8ed886fb023f63943fdc537f3660ef030644e75`, manifest SHA
`9058ff01f9029f12d9fd2d0a7bc0456322bd5b2d19223a3961ee2201a07b91bb`, source commit, and absence of SPI/Oxia.

## Generated physical record

`metadata/src/main/resources/common/metadata/TopicBindingAggregateRecord.json` owns the physical Kafka wire:

```text
apiKey                         32000
validVersions                  0
flexibleVersions               none

topicId                        UUID
topicName                      STRING
aggregateSchemaVersion         INT16
protocolKind                   INT16
bindingId                      BYTES, exactly 32
deploymentId                   UUID
kafkaCellId                    UUID
storageEpochId                 BYTES, exactly 32
epochOrdinal                   INT64, exactly 0
storageProfile                 INT16
profileOrigin                  INT16
policyCatalogDigest            BYTES, exactly 32
frameEncodingPolicyKind        INT16
frameEncodingPolicyVersion     INT16
frameEncodingPolicyPayload     BYTES, exactly empty in v0
initialSealedEnd               nullable BYTES, exactly absent in v0
```

The record has no NTA1 blob, timestamps, lifecycle, backend version, retry identity, attributes map, flexible tags, or
extension tail. `KafkaTopicBindingAggregateMapperV1` maps fields directly to N1 domain values, invokes
`TopicBindingAggregateValidatorV1`, rederives NTB1/NSE1 IDs, and checks the `TopicRecord` ID/name back-reference. It
does not encode or decode temporary NTA1 bytes. Unknown record versions or any illegal field fail closed.

## Feature and bootstrap implementation

`NereusStorageVersion` exposes only V2 level `2` as the supported production level. `QuorumFeatures` advertises range
`[2,2]` only when Nereus mode is explicitly enabled. Storage formatting accepts exact level 2 only on a fresh metadata
directory and emits it with the other bootstrap feature records. Feature replay rejects level 1 because it lies outside
the local range. `FeatureControlManager` rejects every runtime mutation whose feature name is
`nereus.storage.version`; no generic upgrade/downgrade compatibility path can activate V2.

Focused tests cover missing/disabled state, fresh format, V1 replay, unsafe update, downgrade, exact same-value update,
unsupported record version, and a node that has the generated class but has not enabled the complete tuple.

## Aggregate construction and policy input

K1 introduces one immutable `NereusKafkaMetadataPolicyV1`, supplied to the controller builder and snapshotted for the
controller lifetime. It contains only:

- non-zero `DeploymentId` and `KafkaCellId`;
- the closed policy-catalog SHA-256;
- the user-topic Deployment default profile;
- the versioned built-in internal-topic mapping;
- the admission fact that `remote.log.storage.system.enable=false`.

It has no per-host fallback or dynamic default lookup. Feature 2 with an absent/invalid policy fails controller
construction or fresh format before topic creation. M6 may map product configuration into this typed input, but cannot
change the K1 resolution semantics.

The sole input-only pseudo-config is `nereus.storage.profile`. A single insertion-order pass preserves Kafka's
last-wins duplicate behavior, removes only that exact key before native config validation and `CreateTopicPolicy`, and
parses the closed case-sensitive values `OBJECT_WAL`, `BOOKKEEPER_WAL_ONLY`, and
`BOOKKEEPER_WAL_ASYNC_OBJECT`. Null, empty, trimmed variants, or unknown values are `INVALID_CONFIG`. Unknown
`nereus.*` names are not intercepted and retain production native validator precedence.

User topics use explicit `TOPIC_EXPLICIT` or the versioned `DEPLOYMENT_USER_DEFAULT`. The exact pinned
`Topic.isInternal` set uses `DEPLOYMENT_INTERNAL`: `__consumer_offsets` and `__transaction_state` select
`BOOKKEEPER_WAL_ONLY`, while `__share_group_state` fails closed. Those built-ins reject an explicit pseudo-config.
Streams, Connect, MM2, and `__remote_log_metadata` follow the user path. Stock remote-log-system activation is rejected
before K1 admission; K1 neither silently changes that config nor creates a special remote-log profile.

Candidate construction creates the topic UUID locally, derives binding and ordinal-zero epoch IDs, chooses the
profile-required frame policy, validates the full aggregate once, and prepares response plus records without mutating
quota, success maps, controller timelines, or publication state. A rejected candidate's UUID is never exposed.

## CreateTopics and exact atomic-batch admission

`TopicCreateCandidateV1` contains the native validated configuration-derived records, assignment, response projection,
generated topic ID, validated aggregate, and the final ordered candidate records:

```text
TopicRecord
TopicBindingAggregateRecord
native configuration-derived records in semantic order
PartitionRecord values ordered by partition ID
```

The request preserves stock request-wide partition validation and per-topic partial success. Candidate construction
continues through name/existing/config/assignment/policy/quota preparation without externally committing side effects.
Request-order greedy admission evaluates the cumulative accepted record list; a rejected candidate yields
`POLICY_VIOLATION` and a later smaller candidate may still fit. There is no sorting, backtracking, or knapsack.

`MetadataRecordBatchSizer` extracts the same pure record-size calculation used by Raft `BatchBuilder`, uses
`MetadataRecordSerde` and one `ObjectSerializationCache`, and computes exact fresh-batch bytes and record count with
checked arithmetic. It receives the same effective limits as `QuorumController`, including test injection. The final
Raft append guard recomputes from a reset offset delta when a candidate cannot fit the current accumulator batch; it is
defense in depth rather than the first ordinary oversize detector. Tests cover `ConfigRecord`, applicable
`ClearElrRecord`, varint boundaries, current-versus-fresh accumulator batches, 9,999/10,000 partition seams, greedy
partial success, and an injected smaller count/byte limit.

`validateOnly` runs the identical resolution, native validation, aggregate validation, assignment, and batch-admission
kernel but emits no records and commits no quota/result/timeline state. Production tests use
`ControllerConfigurationValidator`; a `NO_OP` unit fixture cannot prove unknown-config behavior.

## Replay, image ownership, and publication

`TopicImage` owns one validated `TopicBindingAggregateV1` alongside the name, topic ID, and partitions when the
finalized image has feature 2. Replay may temporarily hold a topic without an aggregate only inside an unpublished
delta. `TopicBindingAggregateRecord` replay rejects unknown topic IDs, duplicate aggregate records, identity/name
mismatch, invalid domain values, and unsupported versions. `RemoveTopicRecord` removes the complete topic image and
leaves no independent aggregate residue.

Snapshot order is finalized feature records, then for each topic `TopicRecord`, its aggregate record, and sorted
partitions. A stock/feature-disabled image continues to write no aggregate. Snapshot load and bootstrap call
`finishSnapshot` then validate all live topics exactly once. Ordinary publication tracks only touched/created/removed
topic IDs and validates the resulting identities and exactly-one invariant without scanning all topics or recomputing a
canonical SHA. Validation runs at the actual `MetadataLoader` candidate-image publication boundary; an invalid
candidate is never exposed to publishers/readers and `ImageWriter` loss handling cannot discard the error.

Controller state also retains the validated generated record so controller snapshots and replay use the same
authority. No Kafka component implements the Nereus metadata SPI.

## Admin projection

DescribeConfigs synthesizes `nereus.storage.profile` from the aggregate only. It is read-only, non-sensitive, has no
synonyms, and reports `DYNAMIC_TOPIC_CONFIG` for `TOPIC_EXPLICIT` and `DEFAULT_CONFIG` for inherited/internal origin.
Both legacy AlterConfigs and IncrementalAlterConfigs reject every operation on that exact key, including same-value SET
and DELETE/APPEND/SUBTRACT. No `ConfigRecord` stores it. Config requests for a stock/feature-disabled topic follow
native behavior; feature-2 topics fail closed if their aggregate is unavailable.

## Package and source ownership

The new code is limited to generated Kafka metadata plus small `org.apache.kafka.*` V2 classes:

```text
metadata/.../common/metadata/TopicBindingAggregateRecord.json
metadata/.../nereus/KafkaTopicBindingAggregateMapperV1.java
metadata/.../nereus/NereusKafkaMetadataPolicyV1.java
metadata/.../nereus/NereusTopicProfileResolverV1.java
metadata/.../nereus/TopicCreateCandidateV1.java
metadata/.../nereus/MetadataRecordBatchSizer.java
metadata/.../image/TopicImage.java, TopicDelta.java, TopicsDelta.java
metadata/.../controller/FeatureControlManager.java
metadata/.../controller/ReplicationControlManager.java
metadata/.../controller/QuorumController.java
metadata/.../loader/MetadataLoader.java
server-common/.../NereusStorageVersion.java
core/.../StorageTool.scala and focused Admin projection seams
```

Existing V1 runtime packages are not reused as K1 authorities. They remain residue until the separately reviewed graph
cut and mechanical prune. K1 introduces no Produce/Fetch hook and no compatibility facade.

## Deterministic test and gate matrix

The Kafka focused gate must discover and execute non-zero tests with zero failure/error/skip and cover:

- strict generated wire v0 goldens, all fixed widths, corruption, trailing bytes, and unknown version;
- direct domain mapping, ID rederivation, no NTA1 temporary encoding, and no SPI/Oxia dependency;
- fresh feature-2 format plus every forbidden runtime/V1 transition;
- CreateTopics pseudo-config precedence, explicit/default/internal policy, production validator, validate-only, native
  partial success, response loss/replay, exact count/bytes, and final Raft fresh-batch seam;
- replay order, duplicate/unknown/missing/invalid aggregate, touched-only publication validation, full
  snapshot/bootstrap scan, canonical snapshot order, and remove cascade;
- DescribeConfigs projection, both AlterConfigs families, `__share_group_state`, and remote-log interlock;
- unchanged stock behavior when feature 2 is absent.

The repository-side K1 checker verifies exact Kafka before/after commit and clean state, exact N1 artifact hashes,
focused XML test accounting, generated schema/API inventory, no metadata SPI/Oxia/dynamic dependency, and no
Produce/Fetch/V1-prune/scenario-promotion claim. Its receipt is `K1_FOCUSED_ONLY` with
`promotionEligible=false`; N2 later binds it into the exact source tuple.

## Implemented focused evidence

K1 is implemented at clean, pushed Kafka fork commit
`8afbc425660f3466bdc3255e3dd4eb43f8685af1`, descended from the immutable implementation base
`76f62f3b83e882105219b6c7687dbde594a8b8a2`. The seven reviewable commits cover the locked N1 dependency, feature and
generated record, image authority, CreateTopics admission, Admin projection, and exact boundary tests.

`v2M1K1FocusedCheck` executes 39 exact tests in 16 suites with zero failure, error, or skip and validates the generated
schema, clean before/after source, remote branch, immutable N1 JAR/POM/manifest identities, dependency boundary, and
absence of Produce/Fetch integration. The committed [receipt](../../evidence/v2-m1/k1/README.md) is
`K1_FOCUSED_ONLY` and `promotionEligible=false`. K1 therefore completes the Kafka M1 metadata authority but does not
complete P1/R1/G1, prune V1, activate broker data paths, promote scenarios, or claim M1 PASS.

## Commit sequence and stop conditions

K1 is delivered in reviewable commits, each pushed before the next:

1. this detailed design and locked N1 dependency input;
2. generated record, direct mapper, and feature-2 bootstrap/update boundary;
3. image/delta/replay/snapshot/removal and publication validation;
4. CreateTopics resolver/candidate/exact sizer and native error behavior;
5. Admin/internal/remote-log behavior, focused gate, and evidence binding.

Stop rather than weaken the contract if implementation requires a dynamic artifact, per-topic full-image scan,
temporary NTA1 round-trip, generic runtime feature activation, late-only Raft size rejection, per-topic aggregate
side authority, V1 compatibility shim, or Produce/Fetch integration. A focused K1 pass does not promote a scenario or
complete M1.
