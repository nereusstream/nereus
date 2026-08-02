# 04 — Oxia Binding, Leader Session, Checkpoint and Lifecycle

> 状态：F9-M2 implementation complete；ordinary and direct real-service gates pass；aggregate final blocked only by
> inherited Pulsar source-lock drift；F9-M4 all seven canonical payload codecs/full composition and Object-WAL
> exact-reference durable checkpoint quarantine partial slices implemented
> 2026-07-29 状态增量：real-Oxia provider preemption 之外，真实 two-release-process/KRaft singleton
> reassignment、Object/BookKeeper in-flight cuts、three-profile handoff、three-voter ACTIVE failover、activation store/proof
> cuts 与 real Oxia transport reset 均通过；native DeleteRecords 的 rooted NKC1 publication、durable trim、forced process
> death、pre-trim checkpoint hydration/current-trim pruning 与 continued IO 也已通过；completed group/transaction
> internal-topic migration 已由 product `7c25d2e` 的 live two-broker gate 闭合，ongoing/aborted coordinator 和 broader
> chaos cuts 仍 open
> 2026-07-29 ongoing transaction migration 增量：product `efe782d` 在两个 live Object-WAL brokers 间双向迁移 user 与
`__transaction_state-0`，OPEN transactions 分别跨 handoff COMMIT/ABORT；exact ownership、LSO convergence、same-ID
> continuation 与 aborted filtering 均通过，剩余边界为 injected resolution failure、mandatory NTC2、profile expansion 与
> broader chaos
> 2026-07-29 mandatory NTC2 deterministic 增量：product `b6b02f4` 从 exact binding root 解析 activated generation-set
> digest，fork `89b66ab03b` 在 internal coordinator storage installation 前等待每个未 trim generation 的 constrained
> probe；failure preserves the binding、cancels local pending epoch and resigns storage。Physical repair evidence is recorded
> in the next increment
> 2026-07-29 mandatory NTC2 真实修复增量（覆盖上一行末尾）：product `0ae8ca9` 让 read-failure quarantine 持久化 prior index
> version/SHA，允许且仅允许 exact `QUARANTINED -> COMMITTED` generation 与 `QUARANTINED -> ACTIVE` physical-root
> repair；resolver 校验恢复对象的 HEAD/full-read identity 后执行 bounded CAS repair，再以 `REPLACE` 递增 coverage activation
> epoch。真实 Object-WAL delete/corrupt 两轮 repair/re-election 通过；本增量当时未覆盖的 profile expansion 由下一行
`4676c12` 闭合
> 2026-07-29 mandatory NTC2 五 profile 增量（覆盖上一行末尾）：product `4676c12` repeats both physical failure/repair
> cycles for Object async and BookKeeper WAL-only/async/sync；with the fresh Object-sync gate this is five profiles and ten
> real scenarios。`BOOKKEEPER_WAL_ONLY` is the only registration-free compaction authority：the
> resolver/committer/activation guard all revalidate the projection-free L0 stream identity and reject an F4
> registration，while ordinary materialization still requires one
> 2026-07-30 M6 aggregate 增量：new-leader recovered-state fencing is now installed before the Kafka partition state lock
> exposes the leader；the real ongoing transaction migration no longer has a transient LSO=0 window。The same fresh 94/94 M6
> aggregate replays all five-profile transaction-resolution and mandatory-NTC2 repair matrices，plus checkpoint/trim
> response-loss and activation failover。This is current M6 process evidence，not M7 completion
> Durable rule：KRaft owns protocol leadership，stream head owns data commit，one Oxia partition root owns
> mapping/lifecycle
> 禁止：跨 shard atomicity 假设、topic-name identity、checkpoint-as-log、TTL-only leader fencing

## 1. Truth hierarchy

F9 持久状态分为三类，读取/修复时按固定优先级解释：

1. **KRaft metadata image/log**：topic ID、partition、replica assignment、leader、leader epoch、topic config；
2. **Nereus stream head + reachable commit chain**：committed bytes、stable end、trim offset、active append session；
3. **Kafka partition binding root**：`topicId + partition` 到 stream 的 identity、lifecycle、checkpoint refs；
4. **immutable checkpoint/generation objects**：可验证的 derived acceleration；
5. **registry、observed offsets、local cache**：repair hints only。

若 observed binding offset 与 stream head 不同，以 head 为准并修复 observed field。若 binding identity 与 KRaft
topic ID 冲突，fail closed；不得按 topic name“修正”为另一个 stream。

## 2. Keyspace

目标 class：
`nereus-metadata-oxia/.../KafkaPartitionKeyspace.java`

所有 component 使用 `KeyComponentCodec` canonical encoding。`kafkaClusterId` 是 Kafka KRaft cluster ID，
`nereusCluster` 仍是 Nereus namespace，两者不能混用。

```text
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/activation
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/capabilities/{brokerId}/{brokerEpoch}
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/readiness

/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/partitions/{topicId}/{partition}/root
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/partitions/{topicId}/{partition}/compaction-plans/{materializationTaskId}
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/partitions/{topicId}/{partition}/checkpoint-failures/{partitionIncarnation}/{objectId}
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/registry/{shard}/{topicId}/{partition}
```

Partition key：

```text
binding/registry = sha256(kafkaClusterId || 0x00 || topicId || 0x00 || partition)
activation       = sha256(kafkaClusterId || 0x00 || "activation")
```

binding 与其 immutable compaction-plan/checkpoint-failure children 使用同一 deterministic partition key；registry hint
使用其 shard
partition key。实现仍不能依赖 multi-key
transaction。stream head 使用既有 `streamPartitionKey(streamId)`，通常与 binding 不同 shard。
V1 activation、capability 与 readiness 三类 control-plane key 均使用 `activation` partition key；这是 deterministic
routing，不是 multi-key transaction 承诺。`KafkaPartitionKeyspace` 已实现 capability key strict round-trip parser，拒绝
alternate decimal、wrong depth 与 wrong cluster。

`partition` key component 是 fixed-width non-negative integer；parser 必须 round-trip canonical full path，拒绝
额外 slash、alternate decimal、wrong cluster 和 unknown depth。

## 3. Partition root record

### 3.1 Classes

```text
records/KafkaPartitionBindingRecord.java
records/KafkaPartitionLifecycle.java
records/KafkaPayloadMapping.java
records/KafkaCheckpointReferenceRecord.java
records/KafkaCompactionCoverageRecord.java
records/KafkaPartitionPendingOperationRecord.java
records/KafkaPartitionRegistryRecord.java
records/KafkaCompactionPlanRecord.java
codec/KafkaPartitionBindingRecordCodecV1.java
codec/KafkaPartitionRegistryRecordCodecV1.java
codec/KafkaCompactionPlanRecordCodecV1.java
KafkaPartitionMetadataTransitions.java
KafkaPartitionMetadataStore.java
KafkaCompactionPlanMetadataStore.java
OxiaJavaKafkaPartitionMetadataStore.java
```

codec 使用显式 closed field order/wire IDs，不用 Java serialization、enum ordinal 或 reflection-derived component
order。record envelope 仍使用仓库统一 magic/type/schema/min-reader/encoding framing。

### 3.2 `KafkaPartitionBindingRecord` field order

V1 target field order：

| #  | Field                       | Type          | Invariant                                                 |
|----|-----------------------------|---------------|-----------------------------------------------------------|
| 1  | `formatVersion`             | int           | exactly 1                                                 |
| 2  | `kafkaClusterId`            | string        | non-blank canonical ID                                    |
| 3  | `topicId`                   | string        | non-zero Kafka UUID canonical text                        |
| 4  | `partitionId`               | int           | non-negative                                              |
| 5  | `observedTopicName`         | string        | non-blank advisory；never identity                         |
| 6  | `incarnation`               | long          | exactly 1 initially；positive                              |
| 7  | `streamName`                | string        | deterministic exact name or empty only while CREATING     |
| 8  | `streamId`                  | string        | non-empty from ACTIVE onward                              |
| 9  | `payloadMappingId`          | int           | `KAFKA_RECORD_BATCH_V1 = 1`                               |
| 10 | `storageProfile`            | string        | immutable executable profile                              |
| 11 | `lifecycleId`               | int           | closed wire enum                                          |
| 12 | `bindingEpoch`              | long          | starts 1；increments every successful root transition      |
| 13 | `createdMetadataOffset`     | long          | KRaft offset that first proved identity                   |
| 14 | `lastAppliedMetadataOffset` | long          | monotonic，>= created                                      |
| 15 | `observedLeaderId`          | int           | `-1` or broker ID；advisory                                |
| 16 | `observedLeaderEpoch`       | int           | `-1` or non-negative；monotonic per topic incarnation      |
| 17 | `observedBrokerEpoch`       | long          | `-1` or KRaft broker registration epoch                   |
| 18 | `observedLogStartOffset`    | long          | advisory；non-negative                                     |
| 19 | `observedStableEndOffset`   | long          | advisory；>= observed log start                            |
| 20 | `compactionCoverage`        | nested record | irreversible client-visible NTC2 coverage；EMPTY initially |
| 21 | `checkpointReferences`      | list          | 0..3，descending checkpoint offset，closed nested record    |
| 22 | `pendingOperation`          | nested record | EMPTY or lifecycle-compatible exact attempt               |
| 23 | `createdAtMillis`           | long          | positive                                                  |
| 24 | `updatedAtMillis`           | long          | >= created；audit only                                     |
| 25 | `metadataVersion`           | long          | hydrated Oxia version，not trusted from encoded payload    |

`metadataVersion` follows current store convention：writer encodes zero/canonical value；store read hydrates actual Oxia
version。CAS requires exact hydrated version。

### 3.3 Wire enums

`KafkaPartitionLifecycle`：

| ID | State      | Meaning                                               |
|----|------------|-------------------------------------------------------|
| 1  | `CREATING` | root reserved；stream not yet durably bound            |
| 2  | `ACTIVE`   | identity/lifecycle usable；leadership is separate      |
| 3  | `DELETING` | KRaft deletion proven；no new leader/open              |
| 4  | `DELETED`  | stream logical deletion verified；long-lived tombstone |
| 5  | `CORRUPT`  | durable invariant failed；operator/repair required     |

Leader open/recovery states are process-local and not encoded as lifecycle.Unknown IDs fail closed。

`KafkaPayloadMapping`：

```text
1 = KAFKA_RECORD_BATCH_V1
```

mapping immutable after root creation。未来 mapping 需要新 topic/stream or explicit migration protocol；不能 mutate
ACTIVE root in place。

### 3.4 Checkpoint reference nested record

Field order：

```text
referenceVersion:int=1
objectId:string
objectKey:string
objectLength:long
objectSha256:32-byte binary
checkpointOffset:long
logStartOffsetAtCheckpoint:long
sourceCommitVersion:long
sourceHeadSha256:32-byte binary
writerBuild:string
createdAtMillis:long
```

`checkpointOffset` means all committed Kafka batches with end offset `<= checkpointOffset` have been applied to the
snapshot；tail replay starts exactly there。It must be an entry boundary，`logStart <= checkpointOffset <= stableEnd`。

The list retains current plus up to two fallback refs.Offsets strictly descend and object IDs differ。Root CAS adding a
new ref
does not immediately release the displaced oldest object；GC follows reference/pin grace protocol。

### 3.5 Compaction coverage nested record

```text
coverageVersion:int                 1, or 0 only for EMPTY
startOffset:long
endOffset:long                      mandatory half-open client-visible coverage
activationEpoch:long                0 for EMPTY, otherwise positive/monotonic
generationSetSha256:32-byte binary  empty only for EMPTY
policySha256:32-byte binary         empty only for EMPTY
activatedAtMillis:long              0 for EMPTY, otherwise positive
```

This is correctness state, not an observed cache.Once a range becomes mandatory `TOPIC_COMPACTED` visibility, readers
cannot
fall back to lossless COMMITTED bytes and resurrect removed records。Coverage extension/replacement rules and the
generation-
publication handshake are defined in document 05 §11。Trim may advance `startOffset`; `endOffset` never decreases。

#### 3.5.1 Exact quarantined-generation repair

Mandatory compacted-read failure first uses the ordinary read-failure handler to quarantine both the physical root and
the
generation index。The index `stateReason` appends:

```text
|prior-index-version=<activated metadata version>
|prior-index-sha256=<activated durable record SHA-256>
```

These fields are not diagnostics-only：the binding `generationSetSha256` names the historical wrapper identity, while an
Oxia CAS changes the current wrapper version/SHA。`KafkaActivatedGenerationSetResolver.repairIfQuarantined(...)`
therefore
reconstructs exactly one gap-free historical path from `COMMITTED` members plus quarantined members carrying those prior
identities；zero or multiple digest matches fail closed。It then performs, in order:

1. decode only an `ObjectSliceReadTarget` for `STREAM_COMPACTED_OBJECT` /
   `PARQUET_V2_TOPIC_COMPACTED` / `KAFKA_RECORD_BATCH`；
2. require the current physical root to retain the same object key/id、full-object offset/length and storage checksum；
3. `headObject(timeout)` and compare length、CRC32C and non-empty ETag；
4. full `readRange` with the expected CRC and compare the durable content SHA-256；
5. CAS `PhysicalObjectLifecycle.QUARANTINED -> ACTIVE` with `lifecycleEpoch + 1`；
6. CAS only the same publication identity `GenerationLifecycle.QUARANTINED -> COMMITTED`，clearing `stateReason`；
7. recompute the generation-set digest from current COMMITTED wrapper versions/SHAs and call
   `activateCompactionCoverage(..., REPLACE, ...)`，which increments `activationEpoch`。

Each metadata transition retries only `F4MetadataConditionFailedException` and is capped at eight attempts。A concurrent
binding change、different repaired bytes/root identity、missing prior identity、non-COMMITTED unchanged path member or
another publication identity aborts repair。Already repaired coverage is idempotent；there is no COMMITTED read fallback
and no operator edit of the binding digest。

### 3.6 Pending operation nested record

```text
operationTypeId:int       NONE=0, CREATE=1, DELETE=2, REPAIR=3
attemptId:string          empty iff NONE
ownerId:string            broker/worker runtime identity
ownerEpoch:long           positive iff non-NONE
leaseExpiresAtMillis:long positive iff non-NONE
targetMetadataOffset:long non-negative
startedAtMillis:long      positive iff non-NONE
lastErrorCode:string      advisory, bounded, empty allowed
```

Lease 只协调 worker，不授予 Kafka append authority。过期 worker 的 late CAS 因 root version/bindingEpoch 不匹配而
失败。DELETE/CREATE operations are idempotent by deterministic attempt ID。

### 3.7 Immutable compaction-plan attachment

KCP1 is larger and more Kafka-specific than the partition root，so it is an immutable child rather than a mutable nested
binding field。`KafkaCompactionPlanRecord` field order：

```text
formatVersion:int=1
kafkaClusterId/topicId/partitionId
streamId:string
planId:string                         exact kcp1-<sha256-base32lower>
materializationTaskId:string
outputStartOffset/outputEndOffset:long
decisionEndOffset:long
planSha256:32-byte binary
planBytes:bytes                       1..60 KiB canonical KCP1
createdAtMillis:long
metadataVersion:long                  zero on write, hydrated on read
```

Record construction verifies `sha256(planBytes)` and canonical identity/range/size bounds。The adapter
`KafkaCompactionPlanRecordMapper` then decodes KCP1 and cross-checks stream、plan/task IDs and both ranges；the metadata
module
does not interpret Kafka transaction facts or depend back on the adapter。

The child key uses `materializationTaskId`，not `planId`，so a generic worker can resolve KCP1 directly from its durable
task
without a prefix scan。The first plan for one output task becomes authoritative；a later decision-horizon replan must
create a
new output task/source identity rather than mutate that child。

`putCompactionPlanIfAbsent` converges only when the existing exact record bytes are identical；same task ID with
different
plan bytes is an invariant failure。Read verifies key/value partition + task identity。Terminal cleanup uses exact Oxia
version；
a stale delete cannot remove a replacement。The real-Oxia F9 metadata gate includes create/idempotent restart read/exact
delete for this child。

`KafkaCompactionPlanCoordinator` owns the non-atomic KCP1/task-root convergence boundary。The two roots may hash to
different Oxia partitions，so the implementation uses plan-first ordering rather than claiming a cross-partition
transaction：

```text
converge(partition, outputTask, frozenFacts, authorityGuard):
  plan      = KafkaCompactionPlan.create(outputTask, frozenFacts)
  requested = KafkaCompactionPlanRecordMapper.toRecord(partition, plan, clock.millis())
  authorityGuard.revalidate()
  durablePlan = plans.putCompactionPlanIfAbsent(requested)
  durableTask = tasks.create(outputTask, guard = () -> {
      authorityGuard.revalidate()
      current = plans.getCompactionPlan(partition, outputTask.taskId()).orElse(fail)
      require current == durablePlan
      require current.value.withMetadataVersion(0) == requested
      require mapper.fromRecord(current.value) == plan
  })
  require durableTask == outputTask
```

`MaterializationTaskStore.create` invokes that inner guard after revalidating every exact source generation and
immediately
before the task mutation。Therefore a visible compaction task has passed a final authority check and an exact KCP1
reread；
a crash after KCP1 create but before task create leaves only a harmless immutable orphan。A retry converges on identical
KCP1/task bytes。A missing or changed plan before task admission is a non-retryable metadata invariant violation。

Worker restart starts from the already loaded durable `MaterializationTask` and calls
`recover(partition, outputTask)`。The coordinator performs a direct child lookup by `outputTask.taskId()`，decodes the
SHA-verified KCP1 and runs `plan.requireMaterializationTask(outputTask)` before returning any frozen transaction/source
facts。A task without its KCP1 attachment fails closed with retryable `METADATA_CONDITION_FAILED`；execution never
rebuilds
the decision horizon from the current log。

`KafkaCompactionTerminalRetirer` now owns terminal task/KCP1 dual-root deletion。Its production entry point requires the
exact `VersionedMaterializationTask` and exact `VersionedKafkaCompactionPlan` already observed by recovery；passing only
a
logical task ID is insufficient。The accepted task lifecycle set is closed to
`PUBLISHED/CANCELLED/TERMINAL_FAILED`。Before metadata IO it decodes the task snapshot、decodes KCP1、checks
partition/metadata-version consistency and runs `plan.requireMaterializationTask(task)`。The deletion protocol is：

```text
retire(partition, expectedTerminalTask, expectedPlan, stableAuthorityGuard):
  require expectedTerminalTask.lifecycle is terminal
  require decode(expectedPlan).materializationTask == decode(expectedTerminalTask)
  stableAuthorityGuard.revalidate()
  currentPlan = plans.get(partition, taskId)
  if currentPlan is absent:
    require tasks.get(stream, taskId) is absent
    return idempotent success
  require currentPlan == expectedPlan

  currentTask = tasks.get(stream, taskId)
  if currentTask is present:
    require currentTask == expectedTerminalTask
    tasks.delete(currentTask)                 # exact stream/id/version delete
    on any uncertain response:
      reload exact task
      absent          -> deletion applied
      equal           -> retry, maximum 8 attempts
      changed         -> invariant failure

  stableAuthorityGuard.revalidate()
  require tasks.get(stream, taskId) is absent
  require plans.get(partition, taskId) == expectedPlan
  plans.delete(expectedPlan)                  # exact KCP1 version delete
  on any uncertain response:
    reload exact plan
    absent          -> deletion applied
    equal           -> retry, maximum 8 attempts
    changed         -> invariant failure
```

Task-first ordering exposes at worst a harmless plan-only orphan and never deliberately exposes a visible task without
its
restart image。`MaterializationTaskStore.delete(expected)` is deliberately proof-free：it only preserves exact
stream/task/version identity，while lifecycle/reference/authority proofs remain with the retirer。The guard contract is a
stable partition authority fence and must also reject concurrent task admission；a transient “check only” callback is not
sufficient to prevent recreation between the two non-atomic roots。`KafkaCompactionTerminalRetirerTest` covers normal
ordering、both delete-response-loss cuts、non-terminal rejection、pre-delete root change and a root change after an
uncertain
delete。Plan-only orphan prefix scanning after a process crash remains a separate bounded scanner slice。

## 4. Deterministic identity

```text
KafkaPartitionId = kafkaClusterId/topicId/partition
StreamName        = kafka/{kafkaClusterId}/{topicId}/{partition}/incarnation-1
createAttemptId   = sha256("create-v1" || identity || createdMetadataOffset)
deleteAttemptId   = sha256("delete-v1" || identity || deleteMetadataOffset)
```

`StreamName` passed through existing `StreamName` canonical validation。stream attributes at first create：

```text
nereus.protocol.owner=kafka-native
nereus.kafka.cluster.id=<id>
nereus.kafka.topic.id=<uuid>
nereus.kafka.partition=<canonical int>
nereus.kafka.payload.mapping=KAFKA_RECORD_BATCH_V1
nereus.append.authority.mode=EXTERNAL_MONOTONIC_TERM_V1
```

这些 attributes immutable；profile first-create binding 也 immutable。topic name intentionally absent as identity。

## 5. Protocol-neutral leader authority

### 5.1 Public API additions

不改变现有 public record constructors。目标新增：

```java
// target
public record AppendAuthority(
        String authorityType,
        String authorityId,
        long authorityEpoch,
        String ownerId,
        long ownerEpoch) { ... }

public record AppendSessionRequest(
        AppendSessionOptions options,
        Optional<AppendAuthority> authority) { ... }

public record AcquiredAppendSession(
        AppendSession session,
        Optional<AppendAuthority> authority) { ... }
```

`StreamStorage` 新 binary-safe default overload：

```java
default CompletableFuture<AcquiredAppendSession> acquireAppendSession(
        StreamId streamId, AppendSessionRequest request);
```

legacy-equivalent empty authority delegates existing method；non-empty on unupgraded provider 返回
`UNSUPPORTED_APPEND_AUTHORITY`（追加 ErrorCode）。

Kafka mapping：

```text
authorityType  = "kafka-partition-leader-v1"
authorityId    = kafkaClusterId/topicId/partition
authorityEpoch = KRaft partition leaderEpoch
ownerId        = decimal brokerId
ownerEpoch     = KRaft broker registration epoch
```

writerId remains one process-run identity configured in `StreamStorageConfig`，not Kafka broker ID。

### 5.2 Durable session snapshot evolution

现有 `AppendSessionSnapshotRecord` 嵌在 `StreamHeadRecord`，必须继续在同一 head CAS 内 fencing。target 增加：

```text
authorityType:string     empty for legacy
authorityId:string       empty for legacy
authorityEpoch:long      0 for legacy
authorityOwnerId:string  empty for legacy
authorityOwnerEpoch:long 0 for legacy
```

这要求 Phase 1 metadata envelope/codec V2：

- V1 reader path decodes old session and fills empty authority；
- V2 writer uses explicit `StreamHeadRecordCodecV2`，不再依赖 reflection-derived record component layout；
- old broker cannot read V2 and therefore must be excluded by F9 activation before first authority-bound head write；
- ordinary non-authority streams dual-read V1/V2，new writes may migrate envelope without changing logical facts；
- all current Phase 1 golden V1 bytes remain readable and unchanged。

不要把 authority 放到独立 key；append commit只验证 head中的 session即可，无跨-key race。

### 5.3 Authority comparison

For same `authorityType + authorityId`：

```text
new.authorityEpoch > current.authorityEpoch
    => preempt immediately, regardless of TTL/writer

new.authorityEpoch == current.authorityEpoch
  and new.ownerId == current.ownerId
  and new.ownerEpoch > current.ownerEpoch
    => same Kafka leader identity restarted with newer broker epoch; preempt

all term fields equal and writerId == current.writerId and session live
    => renew/reacquire same token/epoch with leaseVersion+1

all other equal/lower/conflicting-owner cases
    => FENCED_APPEND
```

Higher `ownerEpoch` cannot preempt a different owner under same leader epoch；KRaft must bump leader epoch when leader
broker changes。authority type/ID mismatch is `METADATA_INVARIANT_VIOLATION`，not retriable steal。

New preemption atomically writes：new writerId、session epoch `current+1`、new random token、leaseVersion `current+1`、
authority tuple、expiry。Old writer head CAS/protection validation immediately fails even if its wall-clock lease remained
live。

### 5.4 Stream authority mode

Stream attribute controls admissible calls：

| Mode                         | Legacy acquire    | Authority acquire                  |
|------------------------------|-------------------|------------------------------------|
| absent / `LEASE_V1`          | existing behavior | rejected unless separate migration |
| `EXTERNAL_MONOTONIC_TERM_V1` | rejected          | required and compared as above     |

This prevents a legacy auto-acquire caller from waiting out TTL and stealing a Kafka stream。Pulsar streams retain
current
behavior。

### 5.5 Exact stable-head snapshot API（implemented 2026-07-23）

`StreamStorage.getStableHeadSnapshot(StreamId)` is a binary-safe default that unsupported providers fail closed；
`DefaultStreamStorage` delegates to the metadata store exact head read。`StableStreamHeadSnapshot` exposes one atomic
observation of stream state/profile、trim、committed end/cumulative size/commit version/last commit ID、optional exact
`AcquiredAppendSession`（including authority）、metadata version and SHA-256 of the canonical durable `StreamHeadRecord`
envelope。The digest is computed from metadata-version-zero durable bytes，not reconstructed from public metadata views。

Production Oxia and `FakeOxiaMetadataStore` use the same `StableStreamHeadSnapshots` mapper。Authority renewal changes
the
digest and metadata version；empty streams are represented canonically by end/size/commitVersion `0` and empty
lastCommitId。
`StableStreamHeadSnapshot.commitAnchor()` returns a canonical `StreamCommitAnchor`；
`StreamStorage.isCommitReachable(descendant,id,version)` walks backward from that exact immutable descendant through
mixed
legacy/generic commit records。Missing/mismatched ancestors return false；broken chains fail as metadata invariant and the
configured scan budget fails retriably instead of returning a false proof。Production Oxia and the fake share these
semantics。
This supplies the concrete validator without exposing Oxia records through the adapter；same-version digest equality is
never
used to validate an older checkpoint against a newer head。

### 5.6 Public exact-session renewal API（implemented 2026-07-23）

`StreamStorage.renewAppendSession(AppendSession, Duration)` is a binary-safe default。Null、non-positive、sub-millisecond
or millisecond-overflow TTL returns `INVALID_ARGUMENT`；a legacy provider returns `UNSUPPORTED_APPEND_AUTHORITY` without
receiving an acquire fallback。`DefaultStreamStorage` delegates through `AppendSessionManager` only when the session
writer
matches `StreamStorageConfig.writerId`，then production/fake Oxia performs the existing exact stream/writer/epoch/token
CAS。
The returned session retains authority identity、epoch and fencing token while strictly advancing lease
version/expiry；the
head digest/version changes because the renewed session is durable state。

## 6. Binding creation state machine

```text
ABSENT
  --putIfAbsent deterministic root--> CREATING
CREATING
  --createOrGet deterministic stream--> CREATING(stream facts prepared locally)
  --CAS exact root + verify stream attrs/profile--> ACTIVE
ACTIVE
  --idempotent reconcile--> ACTIVE
any invariant mismatch
  --CAS/audit--> CORRUPT
```

Algorithm `KafkaPartitionLifecycleCoordinator.ensureBinding`：

1. validate KRaft topicId/partition and storage activation；
2. read root；ABSENT 时 `putIfAbsent(CREATING, deterministic attempt)`；
3. claim/reclaim pending CREATE lease；
4. call `createOrGetStream(deterministic StreamName, immutable profile/attributes)`；
5. read stream metadata and require exact name/profile/attributes；
6. CAS root CREATING → ACTIVE with stream ID，clear pending operation；
7. put/update registry hint only after ACTIVE；
8. response loss reloads root and converges；never calls create with a new name/attempt。

Two brokers may race steps 2–6；deterministic name and conditional roots converge to one stream。The loser verifies winner
facts。
Partial CREATING roots are recovered by registry/KRaft reconciliation。

## 7. Leader open/recovery state machine

Process-local `KafkaPartitionState`：

```text
NEW
  -> BINDING
  -> ACQUIRING_AUTHORITY
  -> LOADING_HEAD
  -> LOADING_CHECKPOINT
  -> REPLAYING
  -> VALIDATING
  -> LEADER_WRITABLE

LEADER_WRITABLE
  -> WRITE_FENCED_RECOVERY_REQUIRED   unknown append / lost authority
  -> RESIGNING                       KRaft change/shutdown
  -> CORRUPT_OFFLINE                 checksum/invariant failure

WRITE_FENCED_RECOVERY_REQUIRED
  -> REPLAYING (same/current authority only)
  -> RESIGNING

RESIGNING -> CLOSED
```

Open algorithm：

1. require binding ACTIVE and exact KRaft identity；
2. acquire authority-bound append session using leader/broker epochs；
3. read stream head after acquisition；freeze `trimOffset/endOffset/commitVersion/lastCommitId/session`；
4. select newest valid checkpoint with `logStartAtCheckpoint <= current trim` and
   `checkpointOffset between current trim and end`；
5. pin/read/verify checkpoint；if unusable try at most two older refs；
6. hydrate fresh Kafka state objects，不复用 prior leader instances；
7. replay committed entries from checkpoint offset to frozen end；
8. validate Kafka batch CRC/ranges、producer/transaction/epoch/segment invariants；
9. compare current head/session to frozen snapshot；any change restarts or fences；
10. in one short Kafka partition critical section publish log/state and mark writable；
11. schedule checkpoint if replay exceeded threshold。

No user append is admitted during steps 1–10。Materialization generation changes do not change head and are harmless。

### 7.1 Current manager/open-plan boundary（2026-07-23）

`DefaultKafkaPartitionStorageManager` now owns steps 1 and the process-local publication envelope：it completes
`KafkaPartitionLifecycleCoordinator.ensureBinding` first，freezes the ACTIVE stream identity and exact storage-profile
policy
into `KafkaPartitionOpenPlan`，then delegates steps 2–10 to one `KafkaPartitionOpener` operation。The plan carries the
remaining
deadline；same authority may share an open only when stream ID/name and profile policy are identical。Delete/shutdown
remove the
desired local term before any late opener result can install。`DefaultKafkaPartitionOpener` now acquires the exact
authority
session、uses `DefaultKafkaCheckpointSourceValidator` to freeze ACTIVE profile/head/session facts、launches one fresh
existing
checkpoint/replay coordinator under the remaining deadline、validates the returned frozen range and constructs
`DefaultKafkaPartitionStorage`。The opener also injects the runtime-owned renewal scheduler、session TTL and interval；the
storage renews only its current exact recovered token。A renewal failure or non-monotonic/mismatched result immediately
removes
write admission，resets speculative admission to the last stable end and publishes `LEADERSHIP_LOST`。An
already-dispatched
append is not cancelled because it may commit；after its exact completion no queued successor is dispatched。`resign()`
cancels
the pending timer but does not close the shared scheduler。

### 7.2 Current live two-broker provider boundary（2026-07-28）

`f9MultiBrokerTakeoverProviderIntegrationTest` now runs two independently owned
`NereusKafkaObjectWalRuntimeFactory` graphs against the same real four-shard Oxia authority and the same Object-WAL
root。
The activation proof contains two exact `(brokerId, brokerEpoch)` capability records with one compatibility/provider
digest
and one broker-set digest；each runtime owns a separate Oxia client/runtime、ObjectStore instance、partition manager、
callback executor and renewal scheduler。The test executes this exact sequence：

1. broker A opens `(leaderId=1, leaderEpoch=7, brokerEpoch=31)` and stably commits Kafka batch `[0,1)`；
2. while broker A and its 30-second append session remain live，broker B opens
   `(leaderId=2, leaderEpoch=8, brokerEpoch=41)`；
3. B's `acquireAppendSession` atomically preempts the head session because `leaderEpoch=8` dominates，without waiting for
   lease expiry；B then freezes the new head，replays A's exact committed RecordBatch into a fresh recovery state and
   installs
   writable end `1`；
4. A submits offset `1` with its old token。The durable head CAS returns `FENCED_APPEND` even though the provider can
   prove
   `AppendOutcome.KNOWN_NOT_COMMITTED`；
5. `DefaultKafkaPartitionStorage.completeHeadAppend` treats `FENCED_APPEND`、`APPEND_SESSION_EXPIRED` and
   `OFFSET_CONFLICT` as authority/head conflicts before applying the generic safe-retry rule。Therefore A transitions to
   `WRITE_FENCED_RECOVERY_REQUIRED` and cannot dispatch another append；an unrelated explicit known-not-committed timeout
   still resets to the stable end and remains writable；
6. B appends `[1,2)` under leader epoch `8` and Fetches the byte-exact concatenation of A and B batches。

The focused deterministic regression is
`DefaultKafkaPartitionStorageTest.knownNotCommittedAuthorityOrHeadFailureStillFencesTheOldLeader`。The real gate is wired
into
`phase9M3ProviderCheck` as `:nereus-kafka-adapter:f9MultiBrokerTakeoverProviderIntegrationTest`。This closes an R-tier
two-runtime Object-provider live-preemption slice for KF-META-007/KF-META-012；it is not yet a two Kafka-process/KRaft
failover、BookKeeper-profile takeover or multi-controller proof。The separate sections 7.3–7.7 process gates now supply
the Object-WAL post-handoff/old in-flight cuts、the BookKeeper three-profile P/C boundaries and ACTIVE controller
failover。

### 7.3 Release-process handoff and binding-preservation boundary（2026-07-28）

`f9MultiBrokerTakeoverProcessIntegrationTest` now closes the next boundary with real release binaries：one combined
controller/broker node and one broker-only node share one KRaft cluster ID、controller quorum、Nereus cluster、
four-shard Oxia authority and LocalStack Object root while retaining separate metadata/log/cache directories。The test
commits `[0,1)` on assignment `[1]`，starts broker 2，Admin-reassigns the partition to singleton `[2]`，requires exact
`leader=2, replicas=[2], ISR=[2]` and an empty reassignment listing while broker 1 remains alive，then requires broker 2
to
recover `[0,1)` and commit/read `[1,2)`。

The durable lifecycle rule exposed by this gate is：

```text
previous exact identity = oldImage(topicName, topicId, partition)
new exact identity      = newImage(topicName, topicId, partition)

new exact identity exists:
    local replica removed from this broker
    -> resign(previous identity, new leader epoch)
    -> keep binding/root/head/checkpoint/materialization state

new exact identity absent or topicId changed:
    durable partition deletion or same-name recreation
    -> delete(previous identity, metadata offset)
    -> serialize later open of the new identity after delete
```

`TopicsDelta.localChanges(brokerId).deletes()` cannot distinguish these cases by itself。Calling durable delete for the
first
case makes the departing broker a cluster-wide deletion owner and races the new leader's recovery；the initial process
run
reproduced exactly this failure as `Kafka partition binding is deleted or deleting`。
`NereusTopicDeltaLifecycleTest.testLocalReplicaRemovalResignsWithoutDeletingSharedBinding` locks the corrected decision，
while the existing previous-topic-ID and same-name-recreation tests lock the true-delete branches。

The matching controller rule is one atomic KRaft record that changes replicas、ISR and leader to the active target
singleton，with empty adding/removing lists；no transitional RF2 or follower ISR may become visible。This lets the new
broker
acquire a higher stream-head authority only after KRaft ownership changes，while the old broker's metadata callback
merely
resigns its local runtime。Fresh process execution passed 73/73 actionable tasks。This first gate supplies post-handoff
recovery/continuation；section 7.4 supplies the KF-APP-014 already-dispatched append cut。

### 7.4 Already-dispatched append takeover boundary（2026-07-28）

`f9InFlightTakeoverProcessIntegrationTest` keeps control-plane liveness independent from the old data leader：

```text
node 3 = combined controller/broker, sole controller voter
node 1 = broker-only, initial RF1 owner
node 2 = broker-only, takeover target
all nodes = same cluster ID + Nereus cluster + Oxia + bucket + proxied S3 endpoint
```

After offset 0 is stable，the harness installs a downstream timeout toxic and starts a single-attempt
`retries=0, enable.idempotence=false` Produce for offset 1。It does not infer “in flight” from elapsed time：
`${java.home}/bin/jcmd <pid> Thread.print -l` must capture the broker-1 storage worker inside
`NereusUnifiedLog.appendStable` and `CompletableFuture.get` while the client future is incomplete。Only then may the
harness freeze broker 1 with `SIGSTOP`。

The toxic is removed while broker 1 is frozen。Kafka's broker-endpoint `DescribeCluster.controller` is a live broker
chosen
as an Admin forwarding target，not necessarily the KRaft controller；therefore an Admin created immediately after
`SIGSTOP` can remain pinned to frozen broker 1。`awaitTakeoverAdmin` repeatedly creates an Admin from broker 2/node 3
broker
listeners until the non-fenced broker list excludes node 1、the forwarding ID is not 1 and an empty
`listPartitionReassignments` request completes。Only that returned, already-probed Admin may install singleton `[2]`。
Before resuming the old process，broker 2 must recover exact `[0,1)` and report earliest/latest `0/1`。When broker 1
receives
`SIGCONT`，the guarded object path re-runs `revalidateAppendSession` against the durable head and observes the newer
session；
the old future terminates with `FencedLeaderEpochException` and the exact message
`append session changed before guarded object upload`。Because rejection occurs before upload，the raw WAL key set must
remain equal to its pre-fault snapshot。The old JVM must remain alive，durable latest must remain 1，and broker 2 must be
the
only process able to commit `[1,2)`。

This proves two independent safety layers：the process-local storage call already entered the provider future，but durable
session revalidation still prevents stale physical publication；KRaft handoff recovers only the old stable head and does
not
consume a future/in-memory end offset。The current proof is Object-WAL P/C evidence；section 7.6 supplies the BookKeeper
provider-applied counterpart。`KF-APP-014` remains `PLANNED` until the owning milestone/final aggregate policy advances
it，
not because this provider cut is still absent。

### 7.5 BookKeeper three-profile post-handoff boundary（2026-07-29）

`f9BookKeeperProfileTakeoverProcessIntegrationTest` reuses one real stock ZooKeeper long-hierarchical metadata service
and
two Bookies，but creates a separate Kafka cluster ID、Nereus cluster、bucket、F1-BK namespace reservation and ACTIVE exact
publication for each profile：

```text
BOOKKEEPER_WAL_ONLY
BOOKKEEPER_WAL_ASYNC_OBJECT
BOOKKEEPER_WAL_SYNC_OBJECT
```

Each iteration starts node 1 combined controller/broker and node 2 broker-only concurrently so both exact readiness
identities can participate before provider construction。Node 1 owns singleton `[1]` and commits/fetches `[0,1)`；
Admin then installs singleton `[2]` and the harness requires exact `leader=2, replicas=[2], ISR=[2]`、empty
`listPartitionReassignments`、earliest/latest `0/1` and a still-live old JVM。Node 2 must recover the original Kafka
RecordBatch from shared BookKeeper authority，commit/fetch `[1,2)` and expose earliest/latest `0/2`。

The profile invariants are not inferred from the selected enum。WAL-only must leave its S3 bucket empty both before and
after
handoff；async and sync must expose at least one real NCP2 object before takeover and after continuation。The async
operator-seeded activation is built with the same one-entry rollover/physical-deletion BookKeeper configuration as both
broker
processes；a mismatched compatibility digest is expected to fail closed before storage I/O。Fresh execution passes 64/64
actionable tasks in 2m17s and is aggregated by `phase9M6KafkaProcessCheck` and
`phase9M6KafkaBookKeeperProcessCheck`。

This is P-tier post-handoff evidence：it proves all three provider graphs can resign/open/recover/continue without
deleting
shared authority while both JVMs remain live。It does not itself hold a BookKeeper append after provider dispatch；the
following independent C-tier gate does so on the shared appender boundary before the three profiles diverge into their
materialization completion policies。

### 7.6 BookKeeper provider-applied/pre-publication C boundary（2026-07-29）

`f9BookKeeperInFlightTakeoverProcessIntegrationTest` creates an observable cut inside the real BookKeeper write without
a
production hook。The `f9BookKeeperFaultAgent` source set has only Byte Buddy plus JDK dependencies and is packaged as
`nereus-f9-bookkeeper-fault-agent.jar` with `Premain-Class`。Only broker 1 receives
`KAFKA_OPTS=-javaagent:<jar>=arm=...,captured=...,applied=...,release=...,installed=...`；the artifact is neither
published
as a Nereus module nor copied into the Kafka release distribution。

The instrumented method is exactly：

```java
DefaultBookKeeperClientOperations.write(
        WriteAdvHandle handle,
        long entryId,
        ByteBuf entry,
        BookKeeperOperationDeadline deadline)
```

The advice preserves the real provider future returned after `handle.writeAsync(entryId, transmitted)` and substitutes a
second `CompletableFuture<Long>` toward `BookKeeperPrimaryWalAppender`。When the provider future succeeds，the callback
writes
the exact `entryId` to the applied marker and waits on the release marker before completing the substituted future。Thus
BookKeeper has acknowledged bytes while the production pipeline is still between
`casReservation(..., WRITING)` and `casReservation(..., DURABLE)`。

The process gate rejects a timing-only observation。Before takeover it requires all of：

1. agent installed/captured/applied markers；
2. an incomplete retries-disabled Produce and a `jcmd Thread.print -l` stack containing
   `NereusUnifiedLog.appendStable` plus `CompletableFuture.get`；
3. `BookKeeperWriterStateRecord.activeReservationId` resolving to the exact
   `BookKeeperAppendReservationRecord` with `WRITING`、matching stream、ledger、entry and `entryCount=1`；
4. a separately constructed stock BookKeeper client opening that ledger without recovery and reading the same
   `(ledgerId, entryId)` via `readUnconfirmed` with positive length；
5. durable earliest/latest still `0/1`。

Broker 1 is then `SIGSTOP`ped；the common broker-endpoint takeover helper first waits for KRaft heartbeat fencing to
remove
broker 1 from both the live broker set and Admin forwarding target，probes the surviving forwarding path，then atomically
installs `leader=2, replicas=[2], ISR=[2]`。Because the BookKeeper writer is lazy，broker 2's offset-1 Produce is the
explicit recovery
trigger。`BookKeeperLedgerRecovery` must abandon the captured `WRITING` reservation and seal its root before allocating
the
new writer ledger；the new Produce returns offset 1 and reads byte-exactly。Only after those metadata facts are observed
does
the test create the release marker and `SIGCONT` broker 1。The stale pipeline may receive its provider success but its
metadata CAS must fail；the old process stays alive、WAL-only has zero S3 objects and final earliest/latest remains `0/2`。

This single C cut covers all three BookKeeper profiles because `BOOKKEEPER_WAL_ONLY`、
`BOOKKEEPER_WAL_ASYNC_OBJECT` and `BOOKKEEPER_WAL_SYNC_OBJECT` all execute the same
`BookKeeperPrimaryWalAppender` through `WRITING -> provider write -> DURABLE`；their NCP2 behavior starts only after the
captured boundary。The held Produce uses 120-second request/130-second delivery timeouts，must remain incomplete through
reassignment and may not end with Kafka `TimeoutException`。Section 7.5 separately proves each profile's post-handoff
composition。A fresh exact run passes 66/66 actionable tasks in 1m32s；the hardened Object + BookKeeper pair passes 76/76
in 2m40s and this task belongs to both `phase9M6KafkaProcessCheck` and
`phase9M6KafkaBookKeeperProcessCheck`。

### 7.7 ACTIVE controller failover metadata boundary（2026-07-29）

`f9MultiControllerFailoverProcessIntegrationTest` runs three combined `broker,controller` release processes with one
static
voter set。The data partition is deliberately assigned to a combined node other than the current controller leader，so
killing the controller does not also remove the RF1 data owner。Before and after the kill，the harness opens its own
`SharedOxiaClientRuntime` and `KafkaStorageActivationMetadataStore` rather than trusting only Kafka logs。

The accepted metadata state before the fault is：

```text
activation.lifecycle == ACTIVE
activation.kafkaClusterId == formatted Kafka cluster ID
readiness.kafkaClusterId == formatted Kafka cluster ID
sorted(readiness.brokers.brokerId) == [1, 2, 3]
readiness.readinessEpoch >= activation.activationEpoch
readiness.expiresAtMillis > now
readiness.capabilitySha256 == activation.requiredCapabilitySha256
```

After the active controller process is forcibly terminated，`describeMetadataQuorum` must expose a different leader ID
and
strictly larger leader epoch while still returning voters `[1,2,3]` and a non-negative high watermark。The replacement
controller must complete its own activation reconciliation for that exact epoch。The second Oxia read then requires
`replacement.activation().equals(initial.activation())` and
`replacement.readiness().readinessEpoch() >= initial.readiness().readinessEpoch()`。This freezes the one-way-state rule：
controller replacement may refresh/reconcile readiness but may not recreate、downgrade or rewrite the ACTIVE activation
authority。

This cut does not persist controller epoch inside
`KafkaStorageProtocolActivationRecord`。The fork's process-local reconciliation marker proves the selected leader ran the
coordinator；the existing Oxia CAS records remain the durable safety authority。Therefore the gate supplies ACTIVE
steady-state
P/C evidence only；the following independent gate owns the provider-applied publication cuts。

### 7.8 Activation store publication-boundary cuts（2026-07-29）

`f9ActivationCutFailoverProcessIntegrationTest` separates control-plane failure from broker-set drift by running three
dedicated controllers plus one dedicated broker。Every controller receives a separate test-only completion-gate agent and
marker set，but the harness arms only the current controller leader after discovering it through direct controller Admin。
The agent captures exactly one invocation and supports two explicit phases：`before-provider` skips the real store method
and returns an incomplete future；`after-provider` leaves the real
`OxiaJavaKafkaStorageActivationMetadataStore` future untouched，requires it to complete successfully and write an
`applied` marker，then keeps the replacement future incomplete until the process is killed。

The test runs six isolated clusters：

| Cut                         | Phase and intercepted store method                         | Durable state before kill                                               | Required replacement action                                        |
|-----------------------------|------------------------------------------------------------|-------------------------------------------------------------------------|--------------------------------------------------------------------|
| `READINESS_BEFORE_PROVIDER` | before `createReadiness(record)`                           | activation and readiness are absent after the first proof               | repeat the proof，create readiness/PREPARED and publish ACTIVE      |
| `READINESS_APPLIED`         | after `createReadiness(record)` succeeds                   | activation absent；exact readiness brokers are `[4]`                     | reuse the readiness tuple，create PREPARED and publish ACTIVE       |
| `PREPARED_BEFORE_PROVIDER`  | before `createActivation(record)`                          | activation absent；readiness brokers are `[4]`                           | reuse the exact readiness tuple，create PREPARED and publish ACTIVE |
| `PREPARED_APPLIED`          | after `createActivation(record)` succeeds                  | exact PREPARED record exists；readiness brokers are `[4]`                | resume the same prepared tuple and publish ACTIVE                  |
| `ACTIVE_BEFORE_PROVIDER`    | before `compareAndSetActivation(expected, active)`         | exact PREPARED record exists after the second empty-cluster proof       | publish ACTIVE from the same prepared tuple                        |
| `ACTIVE_APPLIED`            | after `compareAndSetActivation(expected, active)` succeeds | exact ACTIVE record exists but old coordinator has not observed success | treat ACTIVE as the winner；do not rewrite it                       |

Before killing the gated leader，the harness uses `bootstrap.controllers` to freeze exact controller ID/epoch/voters and
a
direct Oxia client to freeze activation/readiness。A before-provider cut requires `blocked` and forbids `applied`；an
after-provider cut requires `applied`。It also rejects a false-positive cut if the old leader already emitted the
reconciliation-success marker。After `destroyForcibly()`，a different controller at a strictly higher epoch must emit
its own success marker。For an empty control plane，the replacement creates fresh readiness after revalidation；for
readiness-only state，the replacement binds PREPARED to the existing readiness epoch and `kraftMetadataOffset` even when
its
local KRaft image has advanced；for durable PREPARED，all immutable facts are preserved into ACTIVE；for durable ACTIVE，the
complete activation record compares equal。All six paths require broker admission only after recovery、RF1 native offset-0
Produce/Fetch、
earliest/latest `0/1` and a positive Object count。

The first before-provider run exposed a production recovery defect：a replacement controller could reuse valid readiness
at offset `r` but build PREPARED from its newer snapshot offset `s`，then reject the tuple because `s != r`。
`KafkaStorageFirstActivationCoordinator.createPrepared` now takes `preparedAtMetadataOffset` from the durable readiness
record；`resumesAbsentActivationFromExistingReadinessAfterControllerFailure` fixes this contract with a deterministic
regression。

This supplies process P/C evidence for the complete three-operation store-publication boundary matrix without adding
controller epoch or test markers to durable schemas。The following independent gates own initial empty-cluster
snapshot/capability aggregation and actual Oxia transport failure。Accordingly KF-OPS-005 remains `PLANNED` until the
final
aggregate closes。

### 7.9 Initial snapshot-proof and capability-aggregation cuts（2026-07-29）

`f9ActivationProofCutFailoverProcessIntegrationTest` reuses the three dedicated controller + broker `[4]` topology，exact
current-leader discovery and per-controller marker set。The test-only agent changes target from the Oxia store to
`KafkaStorageFirstActivationCoordinator` and runs four isolated cuts：

| Cut                            | Intercepted method/phase                    | Completed fact before kill                                                                         | Durable state               |
|--------------------------------|---------------------------------------------|----------------------------------------------------------------------------------------------------|-----------------------------|
| `SNAPSHOT_BEFORE_PROVIDER`     | before `currentSnapshot()`                  | no snapshot proof invoked                                                                          | readiness/activation absent |
| `SNAPSHOT_APPLIED`             | after `currentSnapshot()` succeeds          | fork KRaft/local-log fact plus all 64 binding-registry shard scans                                 | readiness/activation absent |
| `CAPABILITIES_BEFORE_PROVIDER` | before `loadCapabilities(snapshot)`         | snapshot proof completed；capability aggregation not invoked                                        | readiness/activation absent |
| `CAPABILITIES_APPLIED`         | after `loadCapabilities(snapshot)` succeeds | broker `[4]` identity/epoch/expiry、five-profile compatibility and provider-scope digests validated | readiness/activation absent |

Before-provider skips the method and returns an incomplete stage。After-provider wraps armed attempts but exceptional
completions propagate unchanged and do not consume the one-shot capture；the first successful completion atomically
writes
`applied` and remains incomplete to the coordinator。This is necessary because a controller may attempt activation while
the Oxia client or broker capability is still converging，and a failed attempt is not evidence that the proof boundary
was
crossed。

After direct Oxia confirms the empty durable state，the harness forcibly kills the exact gated leader。The surviving
quorum
must elect a different controller ID at a strictly higher epoch，repeat the entire snapshot/capability proof，emit its
exact
reconciliation marker and publish ACTIVE/readiness `[4]`。Only then may broker 4 pass native RF1 offset-0
Produce/Fetch/ListOffsets `0/1` and positive Object count。Fresh execution passes 66/66 actionable tasks in 1m49s；failure
evidence is isolated under `build/f9-kafka-activation-proof-cut-evidence` and the task belongs to
`phase9M6KafkaProcessCheck`。

### 7.10 Actual Oxia transport failure and same-epoch retry（2026-07-29）

`OxiaJavaKafkaStorageActivationMetadataStore` is the abstraction boundary between Oxia client failures and the
controller /
broker activation state machines。Before this checkpoint，its read methods returned raw exceptional futures and
`metadataFailure` returned any `RuntimeException` unchanged。A gRPC/Oxia transport exception was therefore not a
`NereusException(retriable=true)`；the fork's `NereusControllerStorageRuntime` classified it as durable，set
`terminalFailure=true` for the current controller epoch and stopped scheduling activation attempts even after transport
recovered。

The store now applies one explicit failure contract：

| Boundary                                           | Condition/invariant failure                                                                   | Unknown provider/transport failure                                          |
|----------------------------------------------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `getActivation` / `getCapability` / `getReadiness` | preserve an existing typed `NereusException`                                                  | wrap as `METADATA_UNAVAILABLE`, `retriable=true`, retaining the exact cause |
| create/CAS invocation                              | preserve condition failures for winner reload                                                 | convert synchronous provider throws to failed futures before recovery       |
| create/CAS response-loss recovery                  | accept a byte-equivalent applied winner；otherwise preserve typed condition/invariant failures | wrap the original non-Nereus failure as retriable metadata unavailable      |

`invoke(Supplier<CompletableFuture<T>>)` ensures a provider that throws before returning a future still obeys the
asynchronous
store contract。`metadataRead(...)` performs the read-side normalization。`metadataFailure(...)` preserves only an
existing
typed `NereusException`; an arbitrary runtime exception is no longer treated as proof of a durable contradiction。
`KafkaStorageActivationMetadataStoreContractTest.normalizesRawTransportFailuresAsRetriableMetadataUnavailable` first
reproduced the raw exception escape and now freezes both read and non-applied write behavior；the complete store contract
still passes its create/CAS monotonicity and applied-response-loss cases。

`:nereus-kafka-adapter:f9ActivationTransportRecoveryProcessIntegrationTest --rerun-tasks` supplies real P/C evidence。It
formats one dedicated controller (node 1) and one dedicated broker (node 2) against one single-voter KRaft quorum、shared
LocalStack S3 and an Oxia endpoint routed through Toxiproxy。The controller first reaches a healthy KRaft leader epoch
while
no broker capability exists。The harness then installs a downstream `reset_peer` toxic and starts the broker；for a four
second fault window both JVMs must remain alive and direct, non-proxied Oxia reads must show no readiness or activation。
After removing the toxic，the same controller ID and epoch—not a leadership replacement—must emit its exact
reconciliation
marker，Oxia must expose ACTIVE plus readiness brokers `[2]`，and the broker must pass RF1 offset-0
Produce/Fetch/ListOffsets `0/1` with a positive Object count。The controller log must never contain the durable-failure
fault
message。

Fresh execution passes 73/73 actionable tasks in 1m10s；the JUnit scenario itself completes in 36.512s with zero
failure/error and is included in `phase9M6KafkaProcessCheck`。This closes actual Oxia connection-reset recovery during
first
activation。It does not claim arbitrary provider failures、rolling capability mismatch or the M7 chaos aggregate。

### 7.11 Durable trim response-loss reconciliation（2026-07-29）

The durable stream head、checkpoint references、Kafka binding observation and fork-local log start are deliberately
different publication domains。For DeleteRecords target `T`，`StreamStorage.trim(T)` may be durably visible while its
completion is lost before `KafkaRetentionDurableTrimListener` advances the binding and before the fork publishes local
`UnifiedLog.logStartOffset`。That state is legal and is recovered as follows：

```text
stream.trimOffset >= T
binding.observedLogStartOffset < T
local process has no successful DeleteRecords completion
  -> forced process loss
  -> new opener reads stream head as authority
  -> checkpoint hydration uses its captured historical window
  -> canonical/local state is pruned to stream.trimOffset
  -> binding observation is monotonically advanced
  -> retry target <= durable trim returns without another mutation
```

`f9TrimResponseLossProcessIntegrationTest` makes this exact state externally observable with a one-shot completion-loss
agent。Before kill it requires stream `trim/end=3/6`、rooted NKC1、binding observed start/end `0/6` and a pending native
DeleteRecords process。After fresh-process open，the same target `3` is an idempotent success only if no stream-head
metadata version、binding metadata version、checkpoint-reference set or NKC1 object-key set changes。This freezes two
ownership
rules：the stream head is recovery truth when binding lags；an already-satisfied target must not republish either durable
domain。Binding may lag the stream head, but it may never lead it。

`f9TrimProfileMatrixProcessIntegrationTest` then applies the same state transition and identity comparison to Object
async
plus BookKeeper WAL-only/async/sync。Together with the Object-sync task，all five profile factories cross the same durable
stream/binding/checkpoint ownership boundary。The BookKeeper profiles use one real two-bookie service with isolated
reservation/activation scopes；the matrix passes 75/75 tasks in 3m23s and is part of both M6 process aggregates。

### 7.12 Recovery component ownership

The executable recovery boundary is now split by resource ownership：

1. `NereusKafkaObjectWalRuntimeFactory` owns `DefaultObjectReadPinManager`、`KafkaCheckpointReader`、
   `KafkaCheckpointVerifier`、`KafkaCheckpointRecoveryCoordinator` and
   `DefaultKafkaPartitionRecoveryLauncher` because those components require the concrete
   ObjectStore/Oxia/physical-reference
   graph；
2. `DefaultKafkaRecoveryBatchSource` translates each bounded `StreamStorage.read` into one exact dense page using
   `COMMITTED + EXACT_START + ALLOW_FIRST_ENTRY_OVERFLOW`，requires
   `PayloadFormat.KAFKA_RECORD_BATCH`，never advances on an empty page and never returns a batch past the frozen end；
3. `KafkaPartitionRecoveryCoordinator` runs page-by-page on the owned callback executor under one wall deadline，hydrates
   only
   a fresh state，checks exact progress/contiguity，revalidates the current source after replay，publishes through a short
   critical-section callback，then revalidates again before returning writable state。A page-read failure is retried only
   when the unwrapped failure is a retriable `NereusException`；retry delay starts at 10 ms、doubles with saturation at
   250 ms and is capped by the remaining original deadline。The exact same start/end offsets are retried，so no failed
   page is
   applied，no partial state is published and no coordinator-ready callback occurs before the complete frozen range
   succeeds。
   Non-retriable failures and deadline expiry still fail closed；ordinary Fetch keeps its existing fail-fast backpressure
   contract；
4. the Kafka fork supplies only `KafkaRecoveryStateFactory`，which creates a fresh stock-RecordBatch-derived codec and an
   exact
   `Partition` publisher after ReplicaManager exists。The published state implements stock
   `LeaderEpochAwareRecoveryState`，so `Partition` preserves exact identity/epoch/frozen validation without an
   artifact-only compile dependency。The one-time bridge fails retriably before binding。

The Object-WAL composition now owns an independent `KafkaCheckpointFailureMetadataStore` and installs
`DurableKafkaCheckpointFailureQuarantine` into both recovery and retention。It does not rewrite the binding root or make
quarantine a deletion authority。A new Object-WAL runtime reloads the immutable record before object I/O，and a newly
confirmed
permanent failure must durably create/reconcile that record before fallback。Neither the product launcher nor the fork
publisher
may bypass opener/source revalidation。The optional `BOOKKEEPER_WAL_ONLY` composition uses the same checkpoint reader、
quarantine、source revalidation and recovery coordinator while replacing generation-zero
append/read/physical-reference/profile
resolution with the provider-neutral BookKeeper runtime。Async/sync Object materialization profiles must explicitly
compose
the same contract when implemented；the transient observer adapter remains test-only。

If no checkpoint：

- `trimOffset == 0`：full replay from 0 allowed；
- `trimOffset > 0`：cannot reconstruct producer/transaction history safely，mark CORRUPT unless a separate verified
  recovery checkpoint anchored at/after trim exists。

## 8. Unknown append recovery

When `NereusException.appendOutcome` is `MAY_HAVE_COMMITTED` or `KNOWN_COMMITTED` with lost response：

1. CAS local state to `WRITE_FENCED_RECOVERY_REQUIRED` before completing Kafka error；
2. keep exact `AppendAttemptId` if present；
3. current runtime calls `recoverAppend(streamId, attemptId, ...)` on recovery executor；
4. KNOWN_COMMITTED result is exact-validated then full Kafka state is rebuilt/replayed from last published checkpoint；
5. KNOWN_NOT_COMMITTED permits reopen at unchanged end；
6. still uncertain repeats bounded/backoff recovery，never appends a new attempt；
7. if process dies，new leader session fences old and uses head + committed bytes as truth；lost in-memory attempt is not
   required to interpret committed data。

Binding `observedStableEndOffset` may be stale throughout；it is updated only after successful reopen/checkpoint and
never
decides outcome。

### 8.1 Trim-aware recovery anchor

Checkpoint publication、recovery 和 trim revalidation 必须区分三类会同时变化的 durable facts：

| Fact                      | Authority                                                                        | Recovery rule                                         |
|---------------------------|----------------------------------------------------------------------------------|-------------------------------------------------------|
| committed append identity | stream head `commitVersion + lastCommitId + committedEndOffset + cumulativeSize` | 相同 commit version 必须完全相等；更高 commit version 只允许单调增长    |
| current visibility window | stream head `trimOffset..committedEndOffset`                                     | restart/fetch/ListOffsets 的权威边界                       |
| advisory observation      | binding `observedLogStartOffset/observedStableEndOffset`                         | 允许落后；任一值领先 current stream head 都是 invariant violation |

`durableHeadSha256` 不是 committed append identity：trim、append-session renewal 或其他 metadata-only transition
可以在不新增 commit 的情况下改变它。因此 `KafkaCheckpointPublicationCoordinator`、
`KafkaCheckpointRecoveryCoordinator` 与 `KafkaTrimBarrier.requireSameAuthority` 在相同 commit version 下都使用
`lastCommitId + committedEndOffset + cumulativeSize`，不能要求旧/new durable-head digest 相等。

`DefaultKafkaPartitionOpener` 在 session acquire 后先读取 current stream source，再从 Oxia 重载 binding；
binding identity/incarnation/stream/profile 必须不变，observed leader/broker epoch 不得领先或冲突，observed
offsets 不得领先 source。Recovery 成功后才以 bounded CAS loop 调用
`KafkaPartitionMetadataTransitions.observe(...)` 发布 exact
`topicName/leaderId/leaderEpoch/brokerEpoch/currentTrim/currentEnd`。CAS condition failure 可在同一 open deadline
内重读重试；metadata invariant、fencing 或 deadline failure 直接关闭 open，禁止把 stale observation 当成成功。

读取 NKC1 时，object/read-pin operation 的 TTL 必须完整覆盖 provider operation 加最大时钟偏差：

```text
pendingProtectionDuration - maximumClockSkew >= operationTtl
```

`NereusKafkaObjectWalRuntimeConfiguration` 在构造期验证该不等式；不满足时在创建 provider graph 前失败。这样
checkpoint verification、trim barrier 与 fresh-process recovery 不会拿到“业务 timeout 尚未结束但保护已被
另一节点判定过期”的窗口。

## 9. Checkpoint container `NKC1`

### 9.1 Object identity

```text
object type = KAFKA_PARTITION_CHECKPOINT
magic       = ASCII "NKC1"
version     = 1
key         = {cluster}/kafka/checkpoints/v1/{kafkaCluster}/{topicId}/{partition}/
              {checkpointOffset}/{attemptId}.nkc
attemptId   = sha256(identity + streamId + checkpointOffset + sourceCommitVersion + contentPolicyDigest)
```

`ObjectType` 增加显式 wire value；old readers fail closed。Existing physical-object root/protection/reader-pin/GC
infrastructure must add `KAFKA_PARTITION_CHECKPOINT` domain before activation。

### 9.2 Binary layout

All integers big-endian；strings are unsigned-32 length + strict UTF-8；no Java serialization。

```text
Header:
  magic[4]                         "NKC1"
  formatVersion:u16                1
  minReaderVersion:u16             1
  flags:u32                        known bits only
  headerLength:u32
  kafkaClusterId:string
  topicId:16 bytes
  partitionId:i32
  incarnation:i64
  streamId:string
  payloadMappingId:i32             1
  leaderEpoch:i32
  checkpointOffset:i64
  logStartOffset:i64
  stableEndOffset:i64
  sourceCommitVersion:i64
  sourceLastCommitId:string
  sourceHeadSha256[32]
  sectionCount:u32

Section repeated:
  sectionType:u16
  sectionVersion:u16
  sectionFlags:u32
  payloadLength:u64
  payloadCrc32c:u32
  payloadSha256[32]
  payload[payloadLength]

Trailer:
  contentLength:u64                 header + sections
  contentSha256[32]
  trailerCrc32c:u32                 all trailer fields except itself
```

Hard limits：header 1 MiB、section count 16、each section 256 MiB、whole checkpoint 1 GiB、strings 64 KiB unless field
has tighter bound。Decoder checks lengths before allocation、no overflow、known flags、unique required sections、EOF exact。

### 9.3 Section wire IDs

| ID | Section                     | Required      |
|----|-----------------------------|---------------|
| 1  | producer state snapshot     | yes           |
| 2  | aborted transaction index   | yes，may empty |
| 3  | leader epoch ranges         | yes           |
| 4  | virtual segment descriptors | yes           |
| 5  | time index                  | yes，may empty |
| 6  | logical byte position index | yes           |
| 7  | open transaction summary    | yes，may empty |

每 section 的 canonical fields 见文档 05。Unknown optional section 仅当 header forward-compatible flag允许时可跳过；
unknown required section flag fail closed。

Current implementation note（2026-07-27）：`nereus-kafka-adapter` 已实现全部 required section 1–7 的
Kafka-artifact-neutral models 和 strict V1 codecs。Decoder 在 allocation 前验证 unsigned count/remaining bytes，
并校验 exact outer required/version/flags、payload version、排序、cross-section equivalence、checkpoint offset 和
EOF；section 3 另外交叉校验 logStart/stableEnd，限制最多一条首位 pre-logStart carried-forward range，并允许
stableEnd 上的当前空 epoch；section 5/6 共用同一个 logStart/stableEnd-bounded canonical image，time segment
必须引用 logical-byte segment，entry/sample offset 严格递增，timestamp 非递减，cumulative logical bytes
严格递增且小于 segment logical bytes，stableEnd 上的 segment 必须为空；section 4 冻结 dense virtual
segment ranges、lifecycle、roll jitter/reason、cumulative bytes 和 bounded effective config history。
`KafkaCanonicalCheckpointStateCodecV1` 按 type 1–7 产生 deterministic section order，decode 对 section order
不敏感，并交叉校验 `checkpointOffset=stableEnd`、virtual/logical segment set、logical bytes、time/sample
segment bounds 和 max timestamp。encode→decode→encode 由 frozen digests 与每类 200 轮固定种子随机状态覆盖。
该切片只接受 normal checkpoint
barrier；section 7 的 completed-but-not-finalized entry 在定义并实现显式 section flag 前必须 fail closed。
Full canonical composition 与 publication-request factory 已实现；publication coordinator 的 runtime-owned
object round trip 以及 fork checkpoint capture/export handoff 仍是后续切片；fork import/replay 已在 local
`ec7f0db991` 接入。Product
`KafkaCanonicalCheckpointPublicationFactory` 已完成前半段：
它只接受同一 capture 中 `checkpointOffset=stableEnd=source.end`、`logStart=source.trim`、无 in-flight append、
state-map end exact、ACTIVE binding 和 exact leader authority 的 canonical image，然后产生 header、type 1–7
sections 与 immutable write request。Runtime-owned private staging/writer、trigger/coalescing 和真实 object
round trip 仍待接线。

## 10. Checkpoint publication

`KafkaCheckpointPublicationCoordinator.publish`：

1. under partition snapshot lock capture authority tuple、binding version、stream head、Kafka state at exact
   `checkpointOffset=stableEnd`；
2. reject if append in-flight or state map end differs；
3. register physical root/protection intent before PUT；
4. encode deterministic NKC1 to private staging；compute length/SHA；
5. guarded immutable PUT；response loss exact HEAD/read hash reconciliation；
6. full decoder/verifier reads object；
7. reload binding + stream head，require identity/lifecycle/session and source head facts unchanged or still safely
   superseding the captured checkpoint；
8. CAS binding root to prepend checkpoint reference；
9. activate permanent reference/protection，release task intent；
10. retire displaced fourth ref only after root CAS proof、reader pins and configured grace。

If stream advanced after capture，checkpoint remains valid if `checkpointOffset <= new end`、source commit anchor is
reachable and same authority/session has not been fenced；root may publish it as a stale-but-useful checkpoint。If trim
advanced
past checkpoint, do not publish。

No producer append waits for checkpoint object upload。Retention may wait for a sufficiently new checkpoint before trim。

## 11. Checkpoint recovery/fallback

For each root reference newest-first：

1. acquire durable reader pin keyed by exact object identity/ref version；
2. GET exact object key，never LIST to discover；
3. verify provider length/checksum、root SHA、NKC1 trailer、section checksums；
4. verify cluster/topic/partition/incarnation/stream/mapping；
5. require `logStartAtCheckpoint <= currentTrim <= checkpointOffset <= currentEnd`；
6. require source commit anchor/recovery checkpoint proves checkpoint offset committed；
7. decode canonical Kafka state；
8. revalidate root still references object or pin is otherwise protected；
9. use；release pin after state copied。

Missing/corrupt newest ref is quarantined/audited and next ref tried。Object LIST cannot resurrect an unreferenced
checkpoint。

### 11.1 Exact-reference durable quarantine record（implemented 2026-07-28）

The authoritative Java surfaces are：

```text
records/KafkaCheckpointFailureRecord.java
codec/KafkaCheckpointFailureRecordCodecV1.java
KafkaCheckpointFailureMetadataStore.java
OxiaJavaKafkaCheckpointFailureMetadataStore.java
KafkaCheckpointFailureQuarantine.java
DurableKafkaCheckpointFailureQuarantine.java
```

The key is scoped by exact Kafka partition identity、positive binding `partitionIncarnation` and canonical encoded
`objectId`。It uses `KafkaPartitionKeyspace.bindingPartitionKey(identity)`，so the store never infers identity from LIST
and
rejects wrong depth、alternate encoding、wrong cluster or non-positive incarnation。

The closed V1 payload order is：

```text
formatVersion:i32 = 1
kafkaClusterId:string
topicId:string
partitionId:i32
partitionIncarnation:i64
objectId:string
referenceSha256[32]
sourceId:i32                 1=RECOVERY, 2=RETENTION
failureCode:string
failureSha256[32]
quarantinedAtMillis:i64
metadataVersion:i64          0 on create, replaced by Oxia version on read
```

`referenceSha256` is domain-separated
`SHA-256("nereus-kafka-checkpoint-reference-v1\0" || canonical length-prefixed reference fields)` over every
`KafkaCheckpointReferenceRecord` field，not just object ID/key/SHA。`failureSha256` is separately domain-separated over
exception class、stable `ErrorCode`、retriable flag and length-prefixed message；the raw message is never persisted。
Eligible codes are the closed set `OBJECT_NOT_FOUND`、`OBJECT_CHECKSUM_MISMATCH`、`UNSUPPORTED_FORMAT` and
`METADATA_INVARIANT_VIOLATION`。Transient metadata/provider errors are never converted into quarantine。

`putIfAbsent` is immutable first-writer-wins。A concurrent recovery/retention classifier may preserve the first
source/code/time
when both writers identify the same exact `referenceSha256`；the stored record is still the authoritative first-failure
audit。
The same key with a different reference digest is `METADATA_INVARIANT_VIOLATION`。A failed create always reloads the
exact key：
an applied-but-response-lost write reconciles to the stored winner，absence/read failure fails closed。There is no update、
clear or fallback-on-store-error path in this milestone。

Recovery and retention execute the same ordering：

1. query the exact quarantine key before acquiring an object read；
2. if an exact record exists，skip object I/O and try the next rooted reference；
3. otherwise verify the current reference；
4. on an eligible permanent failure，await immutable quarantine create/reconciliation；
5. only after that future succeeds may the older rooted reference run；
6. quarantine read/write/collision failure completes the whole recovery/retention operation exceptionally。

The binding root still owns reference reachability and retention/physical-GC decisions。Quarantine does not
remove、reorder or
mutate `checkpointRefs`，and same-name/new-topic or new partition incarnation cannot inherit the old audit。

## 12. Topic deletion

KRaft topic ID deletion is the only normal delete authority。State machine：

```text
ACTIVE
  --CAS exact KRaft deletion proof / DELETE attempt--> DELETING
DELETING
  -> stop/fence broker admission
  -> acquire/recover operation owner
  -> seal stream
  -> logical delete stream
  -> mark checkpoint/generation refs for protected retirement
  -> verify stream state DELETED
  --CAS--> DELETED
DELETED
  -> retain tombstone/audit through configured grace
  -> registry retirement after no KRaft identity/ref/late PUT
```

Deletion proof includes KRaft metadata offset and topic ID；a transient missing name lookup is insufficient。All brokers
see
metadata delta，but a background scanner with current metadata image handles partitions that had no leader。

Response loss at every step reloads root/stream/object state and converges。Late old leader cannot append because
DELETING
blocks open and session/head becomes sealed/deleted；late checkpoint PUT remains unreferenced and is collected through
physical
root intent rules。

`DELETED` root remains long enough to prevent delayed event from recreating same topic ID。Same-name new topic has
different ID,
different key and stream。

## 13. Registry/scanner

64 shards：

```text
shard = first 6 bits sha256(kafkaClusterId/topicId/partition)
```

Registry record contains only identity、binding root key/hash、last observed lifecycle/version/update time。It is
hint-only：
scanner must load root and KRaft image before mutation。Page size default 256，hard 1,024；continuation key must strictly
advance，empty page with continuation is invariant failure。

Scanner responsibilities：

- recover CREATING/DELETING leased operations；
- refresh advisory observed offsets；
- identify KRaft-deleted ACTIVE bindings；
- retire old checkpoint refs/root tombstones after proofs；
- report CORRUPT roots；
- never acquire append authority or publish a leader。

## 14. No cross-shard atomicity

| Partial cut                             | Durable observation               | Repair                               |
|-----------------------------------------|-----------------------------------|--------------------------------------|
| root CREATING before stream             | CREATING + no stream              | deterministic create                 |
| stream created before ACTIVE root CAS   | deterministic stream exists       | verify and CAS ACTIVE                |
| ACTIVE root CAS response lost           | reload same root                  | accept exact winner                  |
| authority head CAS before local install | head has new term                 | new opener resumes；old writer fenced |
| checkpoint PUT before root ref          | object intent/output unreferenced | retry exact publication or GC        |
| root ref CAS before response            | reload contains exact ref         | activate protection/return success   |
| DELETING before seal                    | root blocks open                  | scanner resumes seal                 |
| stream deleted before root DELETED      | stream truth deleted              | CAS DELETED                          |
| registry update missing                 | root authoritative                | backfill scanner                     |

No repair path chooses “latest object by name/time”。

## 15. Metadata-store method contract

```java
public interface KafkaPartitionMetadataStore {
    CompletionStage<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id);
    CompletionStage<VersionedKafkaPartitionBinding> putCreatingIfAbsent(KafkaPartitionBindingRecord value);
    CompletionStage<VersionedKafkaPartitionBinding> compareAndSet(
        VersionedKafkaPartitionBinding expected, KafkaPartitionBindingRecord update);
    CompletionStage<VersionedKafkaPartitionBinding> claimOperation(...);
    CompletionStage<VersionedKafkaPartitionBinding> clearOperation(...);
    CompletionStage<Void> putRegistryHint(KafkaPartitionRegistryRecord value);
    CompletionStage<KafkaPartitionScanPage> scanRegistry(int shard, Optional<String> continuation, int limit);
}
```

All returned records hydrated with exact metadata version。CAS condition failure returns a typed condition exception
carrying no
payload bytes；caller reloads and revalidates。Retry budget bounded，deadline propagated，close rejects new calls and drains
in-flight futures。

## 16. Tests and gates

### 16.1 Codec/golden

- every lifecycle/payload mapping/operation wire ID；unknown ID/field/version；
- full binding and min/max checkpoint refs；canonical key parse round trip；
- checkpoint-failure key/record/envelope round trip、unknown source/version、reference collision and redacted durable
  bytes；
- StreamHead V1 golden remains decodable；V2 authority golden and V1→V2 rewrite；old reader rejection；
- NKC1 each section/limit/checksum/truncation/duplicate/unknown-required fixture。

### 16.2 Deterministic state machines

- two creator convergence at every response-loss cut；
- authority lower/equal/higher leader and broker epochs；old in-flight head CAS fenced；
- open with current/stale/corrupt/missing checkpoints；trimmed stream without checkpoint fails；
- exact quarantine restart lookup、response-loss reconciliation、no object I/O for a quarantined ref and no fallback
  before
  durable audit completion；
- unknown append exact recovery；
- delete/create/open races；same-name new topic isolation；
- scanner pagination/lease takeover/registry hint loss。

### 16.3 Real services

- independent runtimes against real Oxia and configured Object store；
- broker A leader → broker B higher epoch immediate takeover before TTL；
- same broker restart higher broker epoch；
- object PUT/HEAD/root CAS response loss across fresh processes；
- 16,384 bindings across all shards、hot shard pagination、three checkpoint refs each；
- process kill during every create/checkpoint/delete cut。

F9-M2 final gate proves metadata/session/checkpoint primitives only；native Kafka compatibility remains F9-M3+。

### 16.4 Current implementation evidence（2026-07-23）

- public `AppendAuthority`、`AppendSessionRequest`、`AcquiredAppendSession` and the binary-safe `StreamStorage` overload
  are implemented；a legacy provider delegates only the empty-authority request and otherwise fails closed with
  `UNSUPPORTED_APPEND_AUTHORITY`；
- `AppendSessionSnapshotRecord` and `AppendSessionRecord` carry the exact authority tuple while retaining their old
  constructors；explicit dual V1/V2 codecs preserve every frozen Phase 1 V1 golden envelope byte；
- both fake and production Oxia metadata stores execute authority comparison inside the existing stream-head CAS；renewal
  preserves authority and a legacy acquisition is rejected for `EXTERNAL_MONOTONIC_TERM_V1` streams even after expiry；
- public `StreamStorage.renewAppendSession` and `AppendSessionManager.renew` expose that exact CAS without leaking Oxia；
  `DefaultKafkaPartitionStorageTest` proves monotonic token installation、later append propagation、renewal-failure
  fencing、
  leadership-loss publication and queued-append drain while preserving the real outcome of the already-dispatched
  append；
- `StreamHeadV2CodecTest`、`KafkaLeaderAuthorityPropertyTest` and `KafkaLeaderAuthorityIntegrationTest` prove V1 decode,
  V2 round trip, schema mismatch rejection, leader/broker term ordering, immediate live-session preemption and
  old-session
  fencing；
- `f9MultiBrokerTakeoverProviderIntegrationTest` starts two independent activated Object-WAL runtime graphs against real
  Oxia/shared provider state，preempts broker A before its session TTL with broker B's higher leader epoch，replays A's
  exact
  committed batch、fences A's next durable append and lets B continue at the recovered end。The companion partition test
  locks
  the rule that a known-not-committed authority/head conflict is still a recovery-required fence；
- `f9InFlightTakeoverProcessIntegrationTest` starts a controller JVM plus two broker JVMs over one real KRaft/Oxia/S3
  identity，then installs a Toxiproxy downstream timeout before a retries-disabled Produce。A JDK `jcmd Thread.print -l`
  sample must show the broker-1 storage worker inside `NereusUnifiedLog.appendStable` waiting on
  `CompletableFuture.get` before the harness may freeze it。After atomic reassignment to broker 2 and exact recovery at
  stable end 1，resuming broker 1 must surface `append session changed before guarded object upload`；the stale future
  fails、the process survives、the pre-takeover WAL key set is unchanged and only broker 2 may publish `[1,2)`。This is the
  concrete process implementation of “old in-flight head CAS/upload must not outlive authority” for Object-WAL；
- `f9BookKeeperProfileTakeoverProcessIntegrationTest` uses real stock ZooKeeper metadata、two Bookies and two Kafka
  release
  JVMs per profile to prove exact `[1] -> [2]` singleton handoff、live old-owner resign、shared committed recovery and
  continuation for WAL-only/async/sync。It also requires zero objects for WAL-only and real NCP2 objects for both Object
  profiles。This is BookKeeper post-handoff P evidence；
- `f9BookKeeperInFlightTakeoverProcessIntegrationTest` instruments only the test process's common BookKeeper client
  write，
  proves Bookie-applied bytes plus an Oxia `WRITING` reservation before takeover，then requires new-owner recovery to
  publish
  exact `ABANDONED`/`SEALED` metadata and rejects the resumed old completion without moving LEO。This is the shared
  BookKeeper C evidence and introduces no production hook；
- config-free Kafka identity/domain values、the `nereus-kafka-adapter` module skeleton、canonical binding/registry keys、
  all 25 binding-root fields、closed lifecycle/mapping/operation wire IDs and explicit V1 codecs are implemented；
- frozen Kafka metadata envelope SHA-256 values are binding
  `c196685df742d8ff9528bfa5eb4fa7e3c7a9ec8b7077818a19d100a4050ba578` and registry
  `8919c79ce1e19e4128ef905b78d18e45ec49d1df4a2f2a582e2e183f249a3b55`；
- fake and real Oxia stores enforce exact single-key CAS and bounded per-shard continuation；the registry remains a hint，
  and `KafkaPartitionRegistryScanner` reloads each authoritative root across all 64 shards；
- `KafkaPartitionLifecycleCoordinator` implements deterministic CREATING → ACTIVE and ACTIVE → DELETING → DELETED，
  exact stream profile/attribute verification，post-ACTIVE hint publication，response-loss convergence and same-name/new-
  topic-ID isolation；
- `KafkaCheckpointWriter` encodes to bounded private staging and runs the protection guard before immutable PUT；only
  PUT/verify failures enter exact-key response-loss reconciliation，so a failed guard can never be bypassed by an old
  object；
- NKC1 uses seven closed required sections、whole-object CRC32C/SHA-256 and deterministic object identity；the frozen
  full-object SHA-256 is `c6d8848d7e946917e649b0fb0679f390ce76c8660a88bf447c797581285ce91c`；
- `KafkaCheckpointPublicationCoordinator` performs pending protection → immutable PUT/full verify → source
  revalidation →
  binding CAS → permanent root protection → pending release；idempotent retries converge without an unprotected PUT
  window；
- `KafkaCheckpointRecoveryCoordinator` reads referenced keys newest-first under durable reader pins，falls back only for
  object-local missing/corrupt/invariant failures，persists/reconciles the exact immutable failure audit before fallback，
  skips a previously quarantined exact ref without object I/O，and fails closed when trim is non-zero without a usable
  checkpoint；
- `KafkaPartitionRecoveryCoordinator` hydrates only a fresh state instance，requires exact contiguous committed batch
  coverage to the frozen stable end across bounded pages，retries retriable page-read failures at the same cursor with
  bounded
  10–250 ms exponential backoff under the original deadline，revalidates session/head before and after non-writable state
  installation，and fences instead of enabling writes if the head changes during replay/publication；
- `DefaultKafkaRecoveryBatchSourceTest` proves the exact COMMITTED/EXACT_START request, configured record/byte bounds,
  source-fact matching and fail-closed empty/non-Kafka pages；`KafkaCheckpointPublicationRecoveryIntegrationTest` proves
  multi-page replay to the frozen end、transient read-budget rejection followed by exactly one complete publication and
  restart-stable quarantine lookup；
- `KafkaCheckpointFailureMetadataStoreContractTest` covers immutable winner、reference collision and
  applied-response-loss reconciliation；`DurableKafkaCheckpointFailureQuarantineTest` covers exact-reference hashing、
  redaction、restart lookup and rejection of transient failure classes；the real-Oxia integration gate writes the audit、
  closes/reopens the client/runtime and reloads identical durable bytes；
- fork `NereusKafkaRecoveryStateCodecTest` proves exact magic-v2 single-batch parsing、CRC、compressed and uncompressed
  dense
  record replay、timestamps/leader-epoch ranges、trailing/source mismatch rejection and M3 fail-closed
  producer/transaction/NKC1 behavior；`NereusKafkaRecoveryStateFactoryTest` proves exact current-Partition publication
  and
  stale epoch rejection；
- `:nereus-metadata-oxia:f9MetadataTest`、`:nereus-metadata-oxia:f9OxiaIntegrationTest`、
  `:nereus-object-store:kafkaCheckpointTest`、`:nereus-object-store:kafkaCheckpointS3IntegrationTest`、
  `:nereus-kafka-adapter:f9M2Test` and `:nereus-kafka-adapter:f9M2IntegrationTest` pass on current source；
- `phase9M2Check --rerun-tasks` passes on current source；`phase9M2FinalCheck --rerun-tasks` reaches the inherited
  `checkPulsarSourceLock` and stops because the local Pulsar checkout is `5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e`
  while the repository requires `2f9c1eb93be96e2036fbdc8c5e39545f21fa6200`。
- `phase9M6CheckpointQuarantineCheck --rerun-tasks` passes 146/146 scenario-manifest validation、metadata contracts、
  real-Oxia close/reconnect and recovery/retention runtime composition on 2026-07-28。This is a focused partial gate，not
  F9-M4/M5/M6 or Phase 9 completion。
