# 08 — Scenario and Evidence Matrix

> 状态：Active scenario contract；146-row JSON manifest synchronized；rows remain `PLANNED` until owning milestone evidence
> 规则：一个 requirement 至少一个稳定 ID；release report 必须给每个 ID 一个实际执行结果
> 当前 F9-M1 implementation、deterministic gate 与真实 LocalStack NCP2/NTC2 已通过；inherited final gate 因本地 Pulsar checkout 偏离锁定提交而未通过，因此 M1 rows 暂不升级为 `PASSED_CURRENT_SOURCE`

## 1. Evidence tiers

| Tier | Meaning |
| --- | --- |
| `D` | deterministic unit/contract/golden test，no external service |
| `M` | randomized/property/model test with reproducible seed |
| `R` | real Oxia/Object/BookKeeper/KRaft service，may be one JVM |
| `P` | independent broker/runtime processes，fresh restart/kill capable |
| `K` | Kafka fork/upstream focused compatibility suite |
| `C` | named failure cut/response loss/network/process chaos |
| `A` | clean aggregate report consuming all predecessors |

`R/P/C` rows cannot be satisfied by fake stores or mocks。A row may require multiple tiers。

## 2. Machine-readable manifest target

F9-M1 implementation start 已创建 `docs/phase-9-kafka-native-storage/f9-scenarios.json`，每个 Markdown row 对应
一个 object；`checkPhase9ScenarioManifest` 当前验证 146/146 ID、required fields、status vocabulary 和 planned
method uniqueness：

```json
{
  "id": "KF-APP-NNN",
  "milestone": "F9-M3",
  "task": "phase9M3FinalCheck",
  "testClass": "...KafkaNativeAppendRecoveryIntegrationTest",
  "testMethod": "responseLossAfterHeadCasFencesUntilExactRecovery",
  "evidenceTier": ["P", "C"],
  "requiredServices": ["kraft", "oxia", "bookkeeper", "object-store"],
  "sourceLocks": ["nereus", "kafka"],
  "status": "PLANNED"
}
```

Aggregator validates unique ID、exact class/method、executed-not-skipped status、source commits、service fixture and artifact
hash。Markdown/JSON ID sets must match。

## 3. Source, scope and compatibility

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-SRC-001 | AutoMQ checkout HEAD/version equals locked `1c648d...` / `3.9.0-SNAPSHOT` | `Phase9SourceLockTest` | D | M0/M3 |
| KF-SRC-002 | every AutoMQ reference file blob matches document 01 | `Phase9SourceLockTest` | D | M0/M3 |
| KF-SRC-003 | Nereus locked API/reader/materialization blobs drift is reported，not silently accepted | `Phase9SourceLockTest` | D | M1 |
| KF-SRC-004 | production Kafka fork exact upstream/fork commits and relevant signatures match reviewed lock | fork `NereusSourceLockTest` | D,K | M3 |
| KF-SRC-005 | Nereus inject markers are balanced/narrow and no reflection bypass exists | fork `NereusForkMarkerTest` | D,K | M3 |
| KF-SRC-006 | F5 KoP payload/binding/coordinator records cannot be opened as F9 | `KafkaTrackIsolationTest` | D | M2 |
| KF-SRC-007 | disabled mode loads no Nereus runtime/classes with side effects and passes stock focused suite | fork `NereusDisabledCompatibilityTest` | K | M3/M7 |
| KF-SRC-008 | AutoMQ `elasticstream.enable=true` and F9 enabled are rejected as conflicting modes | fork `NereusKafkaConfigTest` | D,K | M6 |

## 4. Ranged append/read and physical formats

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-API-001 | legacy OPAQUE one-entry/one-offset constructors/results remain exact | `RangedAppendApiTest` | D | M1 |
| KF-API-002 | Kafka ranged entries accept positive counts and exact sum/count/order | `RangedAppendApiTest` | D,M | M1 |
| KF-API-003 | zero/negative/overflow/mismatched record counts fail before IO | `RangedAppendApiTest` | D,M | M1 |
| KF-API-004 | concatenated payload CRC32C covers exact entry order and detects mutation | `RangedAppendApiTest` | D,M | M1 |
| KF-API-005 | old `append` delegates none precondition；old provider rejects non-empty new semantics explicitly | `StreamStorageCompatibilityTest` | D | M1 |
| KF-API-006 | expected start equal succeeds；lower/higher produces OFFSET_CONFLICT with no physical append | `ConditionalAppendContractTest` | D,R | M1 |
| KF-API-007 | exact result matches requested range/count/bytes across every storage profile | `ConditionalAppendContractTest` | D,R | M1 |
| KF-API-008 | EXACT_START at entry start works；inside ranged entry fails，never skips to next | `RangedReadContractTest` | D,M | M1 |
| KF-API-009 | CONTAINING_ENTRY at first/middle/last returns full exact range/payload | `RangedReadContractTest` | D,M | M1 |
| KF-API-010 | first-entry overflow can exceed records/bytes exactly once；second entry cannot | `RangedReadContractTest` | D,M | M1 |
| KF-API-011 | containing semantics propagate across resolved slices and only global first entry overflows | `RangedReadContractTest` | D,R | M1 |
| KF-API-012 | WAL writer/reader preserves mixed entry counts and Kafka payload format | `WalRangedEntryRoundTripTest` | D,R | M1 |
| KF-API-013 | BookKeeper reader has exact parity with Object WAL for boundaries/limits | `BookKeeperRangedReadIntegrationTest` | R | M1 |
| KF-API-014 | NCP2 dense rows preserve start/count/payload/CRC and reject every schema/count corruption | `Ncp2GoldenAndCorruptionTest` | D,R | M1 |
| KF-API-015 | NTC2 sparse rows/coverage/key tags/control dispositions round-trip and reject corruption | `Ntc2GoldenAndCorruptionTest` | D,R | M1 |
| KF-API-016 | V1 bytes remain readable；V1/V2 exact dispatch never guesses/reinterprets | `CompactedFormatCompatibilityTest` | D,R | M1 |

## 5. Binding, metadata, authority and checkpoint

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-META-001 | Kafka key builders/parsers canonical round-trip and reject wrong cluster/depth/encoding | `KafkaPartitionKeyspaceTest` | D,M | M2 |
| KF-META-002 | topic name reuse with new topicId creates a different root/stream；old data cannot alias | `KafkaBindingLifecycleTest` | D,R | M2 |
| KF-META-003 | two brokers racing absent binding converge to one deterministic stream | `KafkaBindingRaceIntegrationTest` | R,C | M2 |
| KF-META-004 | every create response-loss cut reloads/converges without extra stream | `KafkaBindingRaceIntegrationTest` | R,C | M2 |
| KF-META-005 | binding profile/mapping/authority attributes are immutable and mismatch becomes CORRUPT | `KafkaBindingTransitionTest` | D,R | M2 |
| KF-META-006 | lower KRaft leader epoch cannot acquire/renew session | `KafkaLeaderAuthorityPropertyTest` | D,M,R | M2 |
| KF-META-007 | higher leader epoch immediately preempts live old session before TTL | `KafkaLeaderAuthorityIntegrationTest` | R,P,C | M2 |
| KF-META-008 | same leader/owner term renews only exact writer/token；different owner is fenced | `KafkaLeaderAuthorityPropertyTest` | D,M | M2 |
| KF-META-009 | same broker restart with higher broker epoch preempts same leader term | `KafkaLeaderAuthorityIntegrationTest` | R,P | M2 |
| KF-META-010 | higher broker epoch from different owner at same leader epoch is rejected | `KafkaLeaderAuthorityPropertyTest` | D,M | M2 |
| KF-META-011 | legacy caller cannot acquire authority-required Kafka stream after lease expiry | `KafkaLeaderAuthorityIntegrationTest` | D,R | M2 |
| KF-META-012 | old writer primary/protection/head CAS fails after authority preemption | `KafkaLeaderAuthorityIntegrationTest` | R,P,C | M2 |
| KF-META-013 | StreamHead V1 goldens decode to empty authority；V2 round-trip and old reader rejects | `StreamHeadV2CodecTest` | D | M2 |
| KF-META-014 | NKC1 all required sections/golden SHA/CRC/EOF round-trip deterministically | `KafkaCheckpointFormatTest` | D | M2 |
| KF-META-015 | NKC1 length/flag/section/checksum/duplicate/unknown-required corruption fails before unsafe allocation | `KafkaCheckpointCorruptionTest` | D,M | M2 |
| KF-META-016 | checkpoint PUT/HEAD/root-CAS response loss converges across fresh runtime | `KafkaCheckpointPublicationIntegrationTest` | R,P,C | M2 |
| KF-META-017 | newest corrupt/missing checkpoint falls back to referenced older checkpoint only | `KafkaCheckpointRecoveryIntegrationTest` | R,P,C | M2 |
| KF-META-018 | trim>0 with no sufficient checkpoint fails closed；trim=0 permits full replay | `KafkaCheckpointRecoveryIntegrationTest` | D,R | M2 |
| KF-META-019 | topic delete without leader progresses ACTIVE→DELETING→DELETED and survives every cut | `KafkaBindingDeleteIntegrationTest` | R,P,C | M2/M6 |
| KF-META-020 | 64-shard registry scanner pages strictly、recovers leases and treats hints as non-authoritative | `KafkaRegistryScannerIntegrationTest` | D,R,M | M2/M7 |
| KF-META-021 | compaction coverage root never decreases and rejects unverified generation digest | `KafkaCompactionCoverageTransitionTest` | D,M,R | M5 |
| KF-META-022 | binding/root observed offsets never override a newer stream head/trim | `KafkaBindingReconciliationTest` | D,R | M2 |

## 6. Produce, state publication and recovery

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-APP-001 | stock LogValidator assigns offsets/leader epoch before Nereus encoder | fork `NereusUnifiedLogTest` | D,K | M3 |
| KF-APP-002 | one MemoryRecords/multiple RecordBatches maps one AppendBatch/one entry per batch | `KafkaAppendBatchEncoderTest` | D,M | M3 |
| KF-APP-003 | raw batch bytes/CRC/compression/producer fields identical before and after append/read | `KafkaBatchExactBytesIntegrationTest` | D,R,K | M3 |
| KF-APP-004 | gzip/snappy/lz4/zstd/uncompressed supported formats pass exact round trip | `KafkaBatchExactBytesIntegrationTest` | R,K | M3 |
| KF-APP-005 | acks=1 response occurs only after stable head and Kafka derived state update | `KafkaNativeProduceIntegrationTest` | R,P,C | M3 |
| KF-APP-006 | acks=-1 RF1 response follows stable HW；acks=0 still commits/updates exactly once | `KafkaNativeProduceIntegrationTest` | R,P | M3 |
| KF-APP-007 | response loss after head CAS fences until exact recovery；no successor append | `KafkaNativeAppendRecoveryIntegrationTest` | P,C | M3 |
| KF-APP-008 | known-not-committed timeout leaves LEO/producer/HW unchanged and permits safe retry | `KafkaNativeAppendRecoveryIntegrationTest` | R,C | M3 |
| KF-APP-009 | post-stable producer/txn/segment derived update failure becomes known-committed fence/replay | `KafkaPostCommitFailureTest` | D,R,C | M3/M4 |
| KF-APP-010 | duplicate idempotent request returns original offsets without second StreamStorage append | `KafkaNativeIdempotenceIntegrationTest` | R,K | M4 |
| KF-APP-011 | append executor queue/byte rejection occurs before IO and releases owned buffer | fork `NereusProduceBufferTest` | D,M | M3 |
| KF-APP-012 | client disconnect/cancel after enqueue cannot cancel uncertain append；callback/buffer terminal once | fork `NereusProduceBufferTest` | D,R,C | M3 |
| KF-APP-013 | different partitions run concurrently，same partition never reorders under saturation | `KafkaAppendExecutorIntegrationTest` | D,R,M | M3 |
| KF-APP-014 | leader takeover during old in-flight append leaves only current-term publication | `KafkaNativeLeaderTakeoverIntegrationTest` | P,C | M3 |
| KF-APP-015 | broker kill before/after each append cut recovers exact LEO/HW/bytes | `KafkaNativeProcessCutIntegrationTest` | P,C | M3/M7 |
| KF-APP-016 | each claimed Nereus storage profile passes identical Produce correctness contract | `KafkaNativeProfileMatrixIntegrationTest` | R,P | M3/M7 |

## 7. Fetch, views and offset APIs

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-FET-001 | committed Fetch returns exact Kafka batches and respects response/partition byte budgets | `KafkaNativeFetchIntegrationTest` | R,K | M3 |
| KF-FET-002 | fetch offset inside compressed batch returns containing batch without split | `KafkaNativeFetchIntegrationTest` | D,R,K | M3 |
| KF-FET-003 | first batch greater than max bytes progresses only when Kafka minOneMessage allows | `KafkaNativeFetchLimitTest` | D,R,K | M3 |
| KF-FET-004 | minBytes waits on actual bytes，stable append wakes，maxWait returns once | fork `NereusFetchOperationTest` | D,R,M | M3 |
| KF-FET-005 | multi-partition fetch coalesces events，has at most one read in flight and callback once | fork `NereusFetchOperationTest` | D,M,R | M3 |
| KF-FET-006 | READ_UNCOMMITTED upper bound HW and READ_COMMITTED upper bound LSO | `KafkaNativeIsolationIntegrationTest` | R,K | M4 |
| KF-FET-007 | aborted transaction list/filter matches Kafka baseline | `KafkaNativeIsolationIntegrationTest` | R,K | M4 |
| KF-FET-008 | request below durable logStart is out-of-range；mid-batch trim hides prefix | `KafkaNativeDeleteRecordsIntegrationTest` | R,K | M5 |
| KF-FET-009 | ListOffsets earliest/latest/max timestamp/timestamp lookup match baseline | `KafkaNativeListOffsetsIntegrationTest` | R,K | M3/M4 |
| KF-FET-010 | leader-epoch end-offset lookup survives checkpoint/restart/takeover | `KafkaNativeLeaderEpochIntegrationTest` | R,P,K | M4 |
| KF-FET-011 | compacted sparse holes advance by source coverage without loop/phantom row | `KafkaCompactedFetchIntegrationTest` | D,R | M5 |
| KF-FET-012 | mandatory compacted prefix switches exactly once to committed tail at coverage end | `KafkaCompactedFetchIntegrationTest` | D,R,K | M5 |
| KF-FET-013 | missing/corrupt newest NTC2 uses verified same-view fallback only | `KafkaCompactedNoResurrectionIntegrationTest` | R,P,C | M5 |
| KF-FET-014 | no healthy mandatory NTC2 fails closed and never exposes COMMITTED source | `KafkaCompactedNoResurrectionIntegrationTest` | R,P,C | M5 |
| KF-FET-015 | NCP2 may replace COMMITTED primary for recovery/fetch without changing Kafka bytes/offsets | `KafkaNcp2CommittedViewIntegrationTest` | R,P | M5 |
| KF-FET-016 | broker shutdown cancels/wakes fetch operations，unregisters listeners and releases buffers | fork `NereusFetchOperationTest` | D,P,C | M6 |

## 8. Producer, transactions and coordinators

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-TXN-001 | producer epoch/sequence validation matches selected Kafka baseline | fork `NereusProducerStateCompatibilityTest` | M,K | M4 |
| KF-TXN-002 | sequence wrap and retained duplicate-batch window survive checkpoint/replay | `KafkaProducerStatePropertyTest` | M,R,K | M4 |
| KF-TXN-003 | canonical producer snapshot encode/decode/replay equals full replay state | `KafkaProducerStatePropertyTest` | D,M | M4 |
| KF-TXN-004 | producer expiration and checkpoint-before-trim preserve subsequent validation | `KafkaProducerTrimIntegrationTest` | R,K | M4/M5 |
| KF-TXN-005 | open transaction crossing checkpoint restores first unstable offset/LSO | `KafkaTransactionRecoveryIntegrationTest` | R,P | M4 |
| KF-TXN-006 | commit marker stable append advances LSO in stock order | `KafkaTransactionRecoveryIntegrationTest` | R,K | M4 |
| KF-TXN-007 | abort marker builds exact aborted index and read-committed filtering | `KafkaTransactionRecoveryIntegrationTest` | R,K | M4 |
| KF-TXN-008 | crash after transactional data/marker commit before derived update replays correctly | `KafkaTransactionProcessCutIntegrationTest` | P,C | M4 |
| KF-TXN-009 | transaction spanning virtual segments/checkpoint/takeover remains atomic to consumers | `KafkaTransactionProcessCutIntegrationTest` | P,K | M4 |
| KF-TXN-010 | transaction verification guard survives async executor handoff without request-thread local use | fork `NereusTransactionVerificationTest` | D,R,K | M4 |
| KF-TXN-011 | `__consumer_offsets` opens/replays before group coordinator election | `KafkaInternalTopicOrderingIntegrationTest` | P,C | M4 |
| KF-TXN-012 | `__transaction_state` opens/replays before transaction coordinator election | `KafkaInternalTopicOrderingIntegrationTest` | P,C | M4 |
| KF-TXN-013 | group commit/rebalance/restart/takeover works with native internal topic | `KafkaGroupCoordinatorIntegrationTest` | P,K | M4 |
| KF-TXN-014 | ongoing transaction coordinator failover resolves from internal topic | `KafkaTransactionCoordinatorIntegrationTest` | P,K | M4 |
| KF-TXN-015 | group offset lag does not protect user-topic retention；client observes normal reset/out-of-range | `KafkaGroupRetentionIndependenceTest` | R,P,K | M5 |
| KF-TXN-016 | mandatory internal-topic NTC2 unavailable blocks coordinator election，no full-source fallback | `KafkaInternalTopicNoResurrectionTest` | P,C,K | M5 |

## 9. Retention and DeleteRecords

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-RET-001 | time retention over virtual closed segments matches stock predicate | `KafkaRetentionOracleTest` | D,M,K | M5 |
| KF-RET-002 | size retention uses exact Kafka logical bytes and matches stock predicate | `KafkaRetentionOracleTest` | D,M,K | M5 |
| KF-RET-003 | combined policies choose monotonic next-segment boundary，never active segment | `KafkaRetentionOracleTest` | D,M | M5 |
| KF-RET-004 | insufficient checkpoint blocks trim；new checkpoint then permits exact candidate | `KafkaRetentionBarrierIntegrationTest` | R,C | M5 |
| KF-RET-005 | trim response loss reloads durable head and completes idempotently | `KafkaRetentionBarrierIntegrationTest` | R,P,C | M5 |
| KF-RET-006 | DeleteRecords at batch start/middle/end/HW maps durable logStart/low watermark correctly | `KafkaNativeDeleteRecordsIntegrationTest` | R,K | M5 |
| KF-RET-007 | retention/config/new-append races revalidate and never over-trim | `KafkaRetentionRacePropertyTest` | M,R,C | M5 |
| KF-RET-008 | compact+delete preserves compacted visibility until logical trim passes range | `KafkaCompactionRetentionIntegrationTest` | R,K | M5 |
| KF-RET-009 | logical trim success is independent of delayed protected physical GC | `KafkaRetentionPhysicalGcIntegrationTest` | R,C | M5 |
| KF-RET-010 | all storage profiles obey same checkpoint barrier and trim semantics | `KafkaRetentionProfileMatrixTest` | R | M5/M7 |

## 10. Kafka compaction

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-CMP-001 | latest keyed value survives；older values removed at same offsets as oracle | `KafkaCompactionOraclePropertyTest` | M,K | M5 |
| KF-CMP-002 | empty key is keyed；null key is uniquely retained；encodings cannot collide | `KafkaCompactionKeyEncodingTest` | D,M | M5 |
| KF-CMP-003 | tombstone retention/drop boundary matches Kafka delete-retention oracle | `KafkaCompactionOraclePropertyTest` | M,K | M5 |
| KF-CMP-004 | newer key in decision-horizon tail removes eligible older output record | `KafkaCompactionHorizonTest` | D,M | M5 |
| KF-CMP-005 | append after frozen horizon can leave extra old record but never drops newest incorrectly | `KafkaCompactionHorizonTest` | D,M,C | M5 |
| KF-CMP-006 | spill/no-spill/restart produce deterministic same NTC2 SHA/rows | `KafkaCompactionSpillPropertyTest` | M,R,C | M5 |
| KF-CMP-007 | all compression formats、headers、timestamps rewrite to equivalent valid records | `KafkaCompactionRewriteTest` | D,M,K | M5 |
| KF-CMP-008 | idempotent/transactional/control/open/aborted traces match stock cleaner views | `KafkaCompactionTransactionOracleTest` | M,K | M5 |
| KF-CMP-009 | decode ratio/key/task limits abort without publication or lost source refs | `KafkaCompactionResourceLimitTest` | D,M,R | M5 |
| KF-CMP-010 | generation commit before coverage CAS is not client-visible mandatory compaction | `KafkaCompactionActivationCutTest` | R,C | M5 |
| KF-CMP-011 | coverage CAS response loss reloads exact activation and never double-advances epoch | `KafkaCompactionActivationCutTest` | R,P,C | M5 |
| KF-CMP-012 | compact→delete preserves old mandatory coverage；delete→compact activates only after verified output | `KafkaCompactionPolicyTransitionTest` | R,K,C | M5 |
| KF-CMP-013 | same-range NTC2 replacement protects readers and retires old generation after proof | `KafkaCompactionReplacementIntegrationTest` | R,C | M5 |
| KF-CMP-014 | user and both internal compacted topics pass differential/restart/takeover suite | `KafkaNativeCompactionEndToEndTest` | P,K,C | M5/M7 |

## 11. Configuration, activation, controller and operations

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-OPS-001 | every config default/bound/static rule and secret redaction is executable | `NereusKafkaStorageConfigTest` | D,M | M6 |
| KF-OPS-002 | KRaft-only、remote-log/cleaner/conflicting mode/message-limit violations reject before IO | fork `NereusKafkaConfigTest` | D,K | M6 |
| KF-OPS-003 | empty cluster first activation PREPARED→ACTIVE succeeds with exact brokers/digests | `KafkaActivationIntegrationTest` | R,P,C | M6 |
| KF-OPS-004 | any topic/internal topic/local authoritative log/binding makes first activation fail non-destructively | `KafkaActivationIntegrationTest` | R,P | M6 |
| KF-OPS-005 | controller failover at every activation cut preserves one-way state | `KafkaActivationControllerFailoverTest` | P,C,K | M6 |
| KF-OPS-006 | controller enforces RF=1/minISR=1 for create/create-partitions/manual assignment | fork `NereusReplicationControlManagerTest` | D,K | M6 |
| KF-OPS-007 | ISR/reassignment/directory APIs cannot create follower/local-placement semantics | fork `NereusReplicationControlManagerTest` | D,K | M6 |
| KF-OPS-008 | missing/mismatched/expired broker capability excludes leader ownership | `KafkaCapabilityReadinessIntegrationTest` | R,P,C | M6 |
| KF-OPS-009 | compatible rolling restart/new broker epoch reacquires readiness/leadership | `KafkaRollingRestartIntegrationTest` | P,K | M6 |
| KF-OPS-010 | unsupported binary rollback or locally disabled broker remains fenced | `KafkaRollingRestartIntegrationTest` | P,C | M6 |
| KF-OPS-011 | wrong Kafka cluster/provider scope/profile digest blocks readiness before partition IO | `KafkaCapabilityReadinessIntegrationTest` | R,P | M6 |
| KF-OPS-012 | startup failure closes only owned resources in reverse order；borrowed stay open | `NereusKafkaRuntimeLifecycleTest` | D,M | M6 |
| KF-OPS-013 | graceful/timeout/kill shutdown classifies appends，drains/cancels and next leader recovers | `KafkaRuntimeShutdownIntegrationTest` | P,C | M6 |
| KF-OPS-014 | priority/backpressure budgets pause background first and never allocate unbounded | `KafkaStorageAdmissionStressTest` | D,M,R | M6 |
| KF-OPS-015 | metrics have bounded labels，logs/admin redact payload/token/secrets，callbacks cannot reclassify IO | `KafkaObservabilitySecurityTest` | D,M | M6 |
| KF-OPS-016 | admin mutations require exact binding/leader/broker guard；read-only verify is bounded | `NereusKafkaStorageAdminTest` | D,R,C | M6 |
| KF-OPS-017 | metadata publisher waits partition recovery before internal coordinator callback | fork `NereusBrokerMetadataPublisherTest` | D,P,C,K | M6 |
| KF-OPS-018 | registry/retention/checkpoint/compaction schedulers stop and remove deadlines on close | `NereusKafkaRuntimeLifecycleTest` | D,M | M6 |

## 12. Scale, chaos and aggregate

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-SCL-001 | 16,384 bindings across 64 shards plus hot-shard pagination/restart complete without omission | `KafkaBindingScaleIntegrationTest` | R,P | M7 |
| KF-SCL-002 | 10,000 active partition state/open/close/checkpoint scheduling stays bounded | `KafkaPartitionScaleIntegrationTest` | R,P | M7 |
| KF-SCL-003 | 1,000 concurrent Produce/Fetch operations obey queue/byte/thread limits and make progress | `KafkaIoConcurrencyStressTest` | R,P,M | M7 |
| KF-SCL-004 | ranged count near Integer.MAX_VALUE uses checked metadata math without per-record allocation | `KafkaRangedCountLimitTest` | D,M | M7 |
| KF-SCL-005 | 128-source/million-record NCP2/NTC2 task respects memory/spill/source protection | `KafkaMaterializationScaleIntegrationTest` | R,C | M7 |
| KF-SCL-006 | repeated leader churn across three brokers never accepts stale-term write/publication | `KafkaLeaderChurnChaosTest` | P,C | M7 |
| KF-SCL-007 | Oxia/Object/BK/network response-loss matrix converges after fresh process restart | `KafkaProviderChaosIntegrationTest` | P,C | M7 |
| KF-SCL-008 | supported Kafka client/protocol versions pass Produce/Fetch/group/txn/admin compatibility | `KafkaClientCompatibilitySuite` | P,K | M7 |
| KF-SCL-009 | performance report records per-profile latency/throughput/recovery/resource baselines without skipped samples | `KafkaNativePerformanceGate` | P,A | M7 |
| KF-SCL-010 | clean `phase9FinalCheck --rerun-tasks` maps every Markdown/JSON ID to one passing result and exact sources | `Phase9EvidenceAggregatorTest` | A | M7 |

## 13. Coverage audit

The aggregate must additionally prove：

- every public/durable class in documents 02–06 appears in at least one test owner；
- every lifecycle transition has success、condition-lost、response-lost and restart evidence where applicable；
- every ErrorCode/AppendOutcome mapping has an assertion at Kafka response and partition-state levels；
- every physical format/metadata codec has golden + corruption fixture；
- every claimed profile/provider has at least one real-service multi-process path；
- every stock file injection has disabled-mode and enabled-mode focused coverage；
- no scenario is marked passed by a test disabled through assumption/environment absence。

## 14. Planned row count and status transition

This design defines 146 planned IDs：8 source、16 API、22 metadata、16 append、16 fetch、16 transaction、10 retention、
14 compaction、18 operations and 10 scale/aggregate。Before implementation，a link/audit script must compute the count from
the actual tables rather than trusting this sentence；if IDs change，both count and manifest change in the same commit。

Status progression：

```text
PLANNED -> IMPLEMENTED_NOT_RUN -> PASSED_CURRENT_SOURCE
                         \-----> FAILED / BLOCKED_ENVIRONMENT
```

Only `PASSED_CURRENT_SOURCE` from the clean aggregate satisfies release。Historical passing evidence with a different source
lock remains audit history，not current gate。
