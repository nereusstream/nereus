# 04 — Oxia Binding, Leader Session, Checkpoint and Lifecycle

> 状态：F9-M2 implementation complete；ordinary and direct real-service gates pass；aggregate final blocked only by inherited Pulsar source-lock drift
> Durable rule：KRaft owns protocol leadership，stream head owns data commit，one Oxia partition root owns mapping/lifecycle
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
/nereus/clusters/{nereusCluster}/kafka/{kafkaClusterId}/registry/{shard}/{topicId}/{partition}
```

Partition key：

```text
binding/registry = sha256(kafkaClusterId || 0x00 || topicId || 0x00 || partition)
activation       = sha256(kafkaClusterId || 0x00 || "activation")
```

binding 与其 registry hint 使用相同 Oxia partition key where provider permits，但实现不能依赖 multi-key
transaction。stream head 使用既有 `streamPartitionKey(streamId)`，通常与 binding 不同 shard。

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
codec/KafkaPartitionBindingRecordCodecV1.java
codec/KafkaPartitionRegistryRecordCodecV1.java
KafkaPartitionMetadataTransitions.java
KafkaPartitionMetadataStore.java
OxiaJavaKafkaPartitionMetadataStore.java
```

codec 使用显式 closed field order/wire IDs，不用 Java serialization、enum ordinal 或 reflection-derived component
order。record envelope 仍使用仓库统一 magic/type/schema/min-reader/encoding framing。

### 3.2 `KafkaPartitionBindingRecord` field order

V1 target field order：

| # | Field | Type | Invariant |
| --- | --- | --- | --- |
| 1 | `formatVersion` | int | exactly 1 |
| 2 | `kafkaClusterId` | string | non-blank canonical ID |
| 3 | `topicId` | string | non-zero Kafka UUID canonical text |
| 4 | `partitionId` | int | non-negative |
| 5 | `observedTopicName` | string | non-blank advisory；never identity |
| 6 | `incarnation` | long | exactly 1 initially；positive |
| 7 | `streamName` | string | deterministic exact name or empty only while CREATING |
| 8 | `streamId` | string | non-empty from ACTIVE onward |
| 9 | `payloadMappingId` | int | `KAFKA_RECORD_BATCH_V1 = 1` |
| 10 | `storageProfile` | string | immutable executable profile |
| 11 | `lifecycleId` | int | closed wire enum |
| 12 | `bindingEpoch` | long | starts 1；increments every successful root transition |
| 13 | `createdMetadataOffset` | long | KRaft offset that first proved identity |
| 14 | `lastAppliedMetadataOffset` | long | monotonic，>= created |
| 15 | `observedLeaderId` | int | `-1` or broker ID；advisory |
| 16 | `observedLeaderEpoch` | int | `-1` or non-negative；monotonic per topic incarnation |
| 17 | `observedBrokerEpoch` | long | `-1` or KRaft broker registration epoch |
| 18 | `observedLogStartOffset` | long | advisory；non-negative |
| 19 | `observedStableEndOffset` | long | advisory；>= observed log start |
| 20 | `compactionCoverage` | nested record | irreversible client-visible NTC2 coverage；EMPTY initially |
| 21 | `checkpointReferences` | list | 0..3，descending checkpoint offset，closed nested record |
| 22 | `pendingOperation` | nested record | EMPTY or lifecycle-compatible exact attempt |
| 23 | `createdAtMillis` | long | positive |
| 24 | `updatedAtMillis` | long | >= created；audit only |
| 25 | `metadataVersion` | long | hydrated Oxia version，not trusted from encoded payload |

`metadataVersion` follows current store convention：writer encodes zero/canonical value；store read hydrates actual Oxia
version。CAS requires exact hydrated version。

### 3.3 Wire enums

`KafkaPartitionLifecycle`：

| ID | State | Meaning |
| --- | --- | --- |
| 1 | `CREATING` | root reserved；stream not yet durably bound |
| 2 | `ACTIVE` | identity/lifecycle usable；leadership is separate |
| 3 | `DELETING` | KRaft deletion proven；no new leader/open |
| 4 | `DELETED` | stream logical deletion verified；long-lived tombstone |
| 5 | `CORRUPT` | durable invariant failed；operator/repair required |

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

The list retains current plus up to two fallback refs.Offsets strictly descend and object IDs differ。Root CAS adding a new ref
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

This is correctness state, not an observed cache.Once a range becomes mandatory `TOPIC_COMPACTED` visibility, readers cannot
fall back to lossless COMMITTED bytes and resurrect removed records。Coverage extension/replacement rules and the generation-
publication handshake are defined in document 05 §11。Trim may advance `startOffset`; `endOffset` never decreases。

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
authority tuple、expiry。Old writer head CAS/protection validation immediately fails even if its wall-clock lease remained live。

### 5.4 Stream authority mode

Stream attribute controls admissible calls：

| Mode | Legacy acquire | Authority acquire |
| --- | --- | --- |
| absent / `LEASE_V1` | existing behavior | rejected unless separate migration |
| `EXTERNAL_MONOTONIC_TERM_V1` | rejected | required and compared as above |

This prevents a legacy auto-acquire caller from waiting out TTL and stealing a Kafka stream。Pulsar streams retain current
behavior。

### 5.5 Exact stable-head snapshot API（implemented 2026-07-23）

`StreamStorage.getStableHeadSnapshot(StreamId)` is a binary-safe default that unsupported providers fail closed；
`DefaultStreamStorage` delegates to the metadata store exact head read。`StableStreamHeadSnapshot` exposes one atomic
observation of stream state/profile、trim、committed end/cumulative size/commit version/last commit ID、optional exact
`AcquiredAppendSession`（including authority）、metadata version and SHA-256 of the canonical durable `StreamHeadRecord`
envelope。The digest is computed from metadata-version-zero durable bytes，not reconstructed from public metadata views。

Production Oxia and `FakeOxiaMetadataStore` use the same `StableStreamHeadSnapshots` mapper。Authority renewal changes the
digest and metadata version；empty streams are represented canonically by end/size/commitVersion `0` and empty lastCommitId。
`StableStreamHeadSnapshot.commitAnchor()` returns a canonical `StreamCommitAnchor`；
`StreamStorage.isCommitReachable(descendant,id,version)` walks backward from that exact immutable descendant through mixed
legacy/generic commit records。Missing/mismatched ancestors return false；broken chains fail as metadata invariant and the
configured scan budget fails retriably instead of returning a false proof。Production Oxia and the fake share these semantics。
This supplies the concrete validator without exposing Oxia records through the adapter；same-version digest equality is never
used to validate an older checkpoint against a newer head。

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

Two brokers may race steps 2–6；deterministic name and conditional roots converge to one stream。The loser verifies winner facts。
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
`KafkaPartitionLifecycleCoordinator.ensureBinding` first，freezes the ACTIVE stream identity and exact storage-profile policy
into `KafkaPartitionOpenPlan`，then delegates steps 2–10 to one `KafkaPartitionOpener` operation。The plan carries the remaining
deadline；same authority may share an open only when stream ID/name and profile policy are identical。Delete/shutdown remove the
desired local term before any late opener result can install。`DefaultKafkaPartitionOpener` now acquires the exact authority
session、uses `DefaultKafkaCheckpointSourceValidator` to freeze ACTIVE profile/head/session facts、launches one fresh existing
checkpoint/replay coordinator under the remaining deadline、validates the returned frozen range and constructs
`DefaultKafkaPartitionStorage`。The launcher remains the Kafka-fork state hydration/publication seam。Periodic session renewal
and the fork callback wiring remain open；neither may bypass opener/source revalidation。

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

Binding `observedStableEndOffset` may be stale throughout；it is updated only after successful reopen/checkpoint and never
decides outcome。

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

| ID | Section | Required |
| --- | --- | --- |
| 1 | producer state snapshot | yes |
| 2 | aborted transaction index | yes，may empty |
| 3 | leader epoch ranges | yes |
| 4 | virtual segment descriptors | yes |
| 5 | time index | yes，may empty |
| 6 | logical byte position index | yes |
| 7 | open transaction summary | yes，may empty |

每 section 的 canonical fields 见文档 05。Unknown optional section 仅当 header forward-compatible flag允许时可跳过；
unknown required section flag fail closed。

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
reachable and same authority/session has not been fenced；root may publish it as a stale-but-useful checkpoint。If trim advanced
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

Missing/corrupt newest ref is quarantined/audited and next ref tried。Object LIST cannot resurrect an unreferenced checkpoint。

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

Deletion proof includes KRaft metadata offset and topic ID；a transient missing name lookup is insufficient。All brokers see
metadata delta，but a background scanner with current metadata image handles partitions that had no leader。

Response loss at every step reloads root/stream/object state and converges。Late old leader cannot append because DELETING
blocks open and session/head becomes sealed/deleted；late checkpoint PUT remains unreferenced and is collected through physical
root intent rules。

`DELETED` root remains long enough to prevent delayed event from recreating same topic ID。Same-name new topic has different ID,
different key and stream。

## 13. Registry/scanner

64 shards：

```text
shard = first 6 bits sha256(kafkaClusterId/topicId/partition)
```

Registry record contains only identity、binding root key/hash、last observed lifecycle/version/update time。It is hint-only：
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

| Partial cut | Durable observation | Repair |
| --- | --- | --- |
| root CREATING before stream | CREATING + no stream | deterministic create |
| stream created before ACTIVE root CAS | deterministic stream exists | verify and CAS ACTIVE |
| ACTIVE root CAS response lost | reload same root | accept exact winner |
| authority head CAS before local install | head has new term | new opener resumes；old writer fenced |
| checkpoint PUT before root ref | object intent/output unreferenced | retry exact publication or GC |
| root ref CAS before response | reload contains exact ref | activate protection/return success |
| DELETING before seal | root blocks open | scanner resumes seal |
| stream deleted before root DELETED | stream truth deleted | CAS DELETED |
| registry update missing | root authoritative | backfill scanner |

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

All returned records hydrated with exact metadata version。CAS condition failure returns a typed condition exception carrying no
payload bytes；caller reloads and revalidates。Retry budget bounded，deadline propagated，close rejects new calls and drains
in-flight futures。

## 16. Tests and gates

### 16.1 Codec/golden

- every lifecycle/payload mapping/operation wire ID；unknown ID/field/version；
- full binding and min/max checkpoint refs；canonical key parse round trip；
- StreamHead V1 golden remains decodable；V2 authority golden and V1→V2 rewrite；old reader rejection；
- NKC1 each section/limit/checksum/truncation/duplicate/unknown-required fixture。

### 16.2 Deterministic state machines

- two creator convergence at every response-loss cut；
- authority lower/equal/higher leader and broker epochs；old in-flight head CAS fenced；
- open with current/stale/corrupt/missing checkpoints；trimmed stream without checkpoint fails；
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
- `StreamHeadV2CodecTest`、`KafkaLeaderAuthorityPropertyTest` and `KafkaLeaderAuthorityIntegrationTest` prove V1 decode,
  V2 round trip, schema mismatch rejection, leader/broker term ordering, immediate live-session preemption and old-session
  fencing；
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
  PUT/verify failures enter exact-key response-loss reconciliation，so a failed guard can never be bypassed by an old object；
- NKC1 uses seven closed required sections、whole-object CRC32C/SHA-256 and deterministic object identity；the frozen
  full-object SHA-256 is `c6d8848d7e946917e649b0fb0679f390ce76c8660a88bf447c797581285ce91c`；
- `KafkaCheckpointPublicationCoordinator` performs pending protection → immutable PUT/full verify → source revalidation →
  binding CAS → permanent root protection → pending release；idempotent retries converge without an unprotected PUT window；
- `KafkaCheckpointRecoveryCoordinator` reads referenced keys newest-first under durable reader pins，falls back only for
  object-local missing/corrupt/invariant failures，and fails closed when trim is non-zero without a usable checkpoint；
- `KafkaPartitionRecoveryCoordinator` hydrates only a fresh state instance，requires exact contiguous committed batch
  coverage to the frozen stable end，revalidates session/head before and after non-writable state installation，and fences
  instead of enabling writes if the head changes during replay/publication；
- `:nereus-metadata-oxia:f9MetadataTest`、`:nereus-metadata-oxia:f9OxiaIntegrationTest`、
  `:nereus-object-store:kafkaCheckpointTest`、`:nereus-object-store:kafkaCheckpointS3IntegrationTest`、
  `:nereus-kafka-adapter:f9M2Test` and `:nereus-kafka-adapter:f9M2IntegrationTest` pass on current source；
- `phase9M2Check --rerun-tasks` passes on current source；`phase9M2FinalCheck --rerun-tasks` reaches the inherited
  `checkPulsarSourceLock` and stops because the local Pulsar checkout is `5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e`
  while the repository requires `2f9c1eb93be96e2036fbdc8c5e39545f21fa6200`。
