# Phase 9 — Native Kafka Shared-Storage Code-Level Target

> 状态：In progress；F9-M1 public API/default-overload slice implemented；无 native Kafka runtime
> Future：F9 Native Kafka Shared Storage
> 目标日期基线：2026-07-23
> AutoMQ 参考锁：`1c648d84819d5c3fef2af585f02149c397584870`（`3.9.0-SNAPSHOT`）
> F9 implementation base：`main@112c459`

本目录是原生 Kafka 与 Nereus 集成的代码级 target contract。这里的 class、method、record、key、状态机和
gate 是实现约束。当前仅 `nereus-api` ranged values、Kafka batch validation、binary-safe default overloads 与
scenario manifest/check gate 已落地；core provider 仍会 fail closed，Kafka broker capability 仍不存在。若以后
实现与本文不同，必须先更新合同、版本和兼容性分析，不能让代码静默改变 durable bytes 或 correctness owner。

## 1. 设计文档

| 文档 | 权威范围 |
| --- | --- |
| `01-current-contract-and-automq-source-audit.md` | 本地 Nereus/AutoMQ 源码事实、可复用边界、gap inventory |
| `02-ranged-entry-api-and-object-format.md` | protocol-neutral API、ranged entry、read boundary、NCP2/NTC2 bytes |
| `03-kafka-fork-log-and-broker-integration.md` | Kafka fork classes/methods、Produce/Fetch、LEO/HW/LSO、error mapping |
| `04-oxia-binding-session-checkpoint-and-lifecycle.md` | keyspace、records/codecs、leader authority、partition lifecycle、recovery |
| `05-producer-state-transactions-compaction-and-retention.md` | producer state、transaction、internal topics、virtual segment、retention/cleaner |
| `06-runtime-configuration-rollout-and-observability.md` | config、runtime ownership、activation、upgrade、metrics/alerts/runbook |
| `07-implementation-plan-and-gates.md` | package/file ownership、milestone DAG、build/test gates、definition of done |
| `08-scenario-evidence-matrix.md` | requirement-to-test traceability、failure cuts、scale/compatibility aggregate |
| `09-f9-m0-design-review-2026-07-23.md` | dated M0 coverage/status/scope audit；非新的运行时合同 |

North-star 摘要见 `../design/nereus-future9-kafka-native-storage.md`。发生冲突时：已实现代码/测试优先于
代码级合同；代码级合同优先于 Future 摘要；本地锁定源码优先于对 AutoMQ 或 Kafka 的记忆。

## 2. F9 与 F5 的不可合并边界

F5 和 F9 是两个独立 consumer：

```text
F5: Kafka client -> KoP/Pulsar facade -> Nereus
F9: Kafka client -> native Kafka fork -> Nereus
```

禁止事项：

- F9 不读写 F5 的 `KAFKA_RECORD_V1` projection record；
- F9 不把 native group/transaction state 存成 F5 Oxia coordinator record；
- F5 不按 F9 ranged `RecordBatch` 解释已有 one-record-per-offset payload；
- 任一 track 若要读取另一 track 的 stream，必须新增显式 mapping/migration version；
- `nereus-kop-adapter` 与计划中的 `nereus-kafka-adapter` 不能互相依赖。

## 3. 目标模块依赖

未来只新增一个 Kafka-aware Nereus module：

```text
nereus-kafka-adapter
  -> nereus-api
  -> nereus-core
  -> nereus-metadata-oxia
  -> nereus-object-store
  -> nereus-materialization
  -> Kafka server/common dependencies (module-local)
```

`nereus-api`、`nereus-core`、`nereus-metadata-oxia` 和 object formats 只接受 protocol-neutral evolution。
Kafka `TopicPartition`、`MemoryRecords`、`RecordBatch`、`Errors` 等类型只存在于 adapter 或 Kafka fork。

计划 package：

| Package | Owner |
| --- | --- |
| `com.nereusstream.kafka.config` | typed Kafka/Nereus config and validation |
| `com.nereusstream.kafka.runtime` | process runtime、owned/borrowed resources、activation |
| `com.nereusstream.kafka.partition` | partition identity、binding、storage facade、state machine |
| `com.nereusstream.kafka.codec` | Kafka batch validation/append/fetch mapping |
| `com.nereusstream.kafka.metadata` | Oxia keyspace、records、codecs、store、scanner |
| `com.nereusstream.kafka.checkpoint` | immutable checkpoint format/store/publication |
| `com.nereusstream.kafka.recovery` | open/replay/unknown-outcome recovery |
| `com.nereusstream.kafka.retention` | log-start、segment retention、DeleteRecords |
| `com.nereusstream.kafka.compaction` | F4 topic codec/strategy and NTC2 rewrite |

## 4. Target call paths

### 4.1 Broker boot

```text
BrokerServer.startup
  -> NereusKafkaRuntimeFactory.create(KafkaConfig)
  -> validate cluster-wide mode / dependency connectivity
  -> advertise exact capability
  -> wait for ACTIVE protocol record
  -> construct NereusLogManager / NereusReplicaManager
  -> accept metadata images
  -> admit client traffic only after readiness
```

### 4.2 Partition leader open

```text
BrokerMetadataPublisher delta
  -> KafkaPartitionStorageManager.onMetadataDelta
  -> resolve/create KafkaPartitionBindingRecord(topicId, partition)
  -> acquire append session with leader authority epoch
  -> load stable stream head + selected checkpoint
  -> replay committed Kafka batches to head
  -> rebuild ProducerStateManager / txn / epochs / indexes
  -> NereusUnifiedLog.publishWritable
  -> Partition becomes leader
```

### 4.3 Produce

```text
KafkaApis.handleProduceRequest
  -> ReplicaManager.appendRecords
  -> Partition.appendRecordsToLeader
  -> UnifiedLog.append (stock validation + offset assignment)
  -> NereusLocalLog.append
  -> KafkaAppendBatchEncoder (one entry per RecordBatch)
  -> StreamStorage.append(expectedStartOffset)
  -> stable AppendResult exact-range validation
  -> Kafka state update / HW = stable end
  -> ProduceResponse
```

### 4.4 Fetch

```text
KafkaApis.handleFetchRequest
  -> ReplicaManager.fetchMessages
  -> Partition.readRecords
  -> NereusLogRecords.read
  -> StreamStorage read(COMMITTED or TOPIC_COMPACTED, CONTAINING_ENTRY)
  -> KafkaFetchAssembler
  -> apply logStart/HW/LSO/isolation/aborted-txn bounds
  -> FetchDataInfo / FetchResponse
```

### 4.5 Delete/retention

```text
KRaft topic delete or DeleteRecords/retention
  -> KafkaPartitionLifecycleCoordinator / KafkaRetentionCoordinator
  -> guarded binding lifecycle or log-start transition
  -> StreamStorage.seal/delete or trim
  -> verify durable state
  -> checkpoint/index cleanup
  -> delayed physical GC through existing reference fences
```

## 5. Correctness owners

| Concern | Single owner | Forbidden alternate owner |
| --- | --- | --- |
| data commit | Nereus stream head + reachable commit | local LEO、object list、Kafka checkpoint file |
| offset allocation | Kafka validation under partition append lock + Nereus expected-start CAS | adapter-side retry with new offset |
| leader fencing | KRaft leader epoch bound into durable append authority | broker routing/cache only |
| stream identity | Oxia binding keyed by cluster/topicId/partition | topic name / log directory name |
| recovery state | committed bytes replayed from verified checkpoint | local disk snapshot alone |
| group/txn coordinator | native Kafka internal topics | F9-specific Oxia coordinator tree |
| compacted visibility | F4 committed generation | cleaner local swap or task output existence |
| logical retention | Kafka policy -> Nereus trim | consumer group offset floor |

## 6. Code-level invariants

### 6.1 Append

- entries ordered by Kafka base offset；ranges dense inside one request；
- `AppendBatch.recordCount == sum(AppendEntry.recordCount)`；
- `expectedStartOffset == first RecordBatch.baseOffset == current committedEndOffset`；
- success result start/end/count exactly equals request；
- payload bytes are read-only snapshots before async handoff；
- no retry creates a new physical attempt while previous completion is unknown；
- partition append lane remains closed until exact recovery converges。

### 6.2 Read

- storage may return one full entry whose range contains requested start；
- after the containing first entry，all ranges are dense for `COMMITTED`；
- `TOPIC_COMPACTED` rows may be sparse，coverage rather than next row proves skipped holes；
- an indivisible first Kafka batch may exceed both maxRecords and maxBytes once；
- fetch assembler never mutates source payload and never exposes offsets below log start or above isolation bound；
- corrupted CRC/format stops the partition；it is not silently skipped。

### 6.3 Metadata

- all durable formats begin with magic/version or equivalent codec discriminator；
- records reject unknown mandatory fields and invalid enum ordinals；
- every mutating CAS guards partition identity、incarnation、lifecycle、authority epoch and root version；
- no correctness transition requires atomic writes across Oxia shards；
- derived observed offsets in binding/checkpoint never override current stream head。

### 6.4 Lifecycle

- same topic ID/partition cannot bind two live streams；
- same-name recreate cannot reuse deleted binding；
- only current KRaft leader epoch obtains writable session；
- losing leadership fences admission before releasing resources；
- delete is metadata-first and restart-safe；late object PUT cannot resurrect a deleted partition；
- close is idempotent and distinguishes owned from borrowed resources。

## 7. Initial compatibility envelope

| Dimension | Initial target |
| --- | --- |
| Kafka mode | KRaft only |
| Kafka fork baseline | version aligned with locked integration branch；initial audit uses AutoMQ 3.9 fork |
| Kafka replication factor | exactly 1 in Nereus mode |
| Nereus storage profile | immutable per stream；all activated executable profiles；default BK async object |
| topic cleanup policy | `delete`、`compact`、`compact,delete` after F9-M5 |
| compression | exact batch bytes；all Kafka-supported codecs present in locked fork |
| message formats | magic versions explicitly allowed by locked fork/config；no implicit downgrade |
| transactions | required for compatibility claim after F9-M4 |
| tiered storage | stock Kafka remote log disabled for Nereus partitions |
| local log migration | unsupported |
| mixed F9/non-F9 broker | forbidden after activation |
| mixed storage topics | unsupported in first release |

## 8. F9 milestone DAG

```text
F9-M0 design/source lock
  -> F9-M1 ranged API + formats
      -> F9-M2 metadata/session/checkpoint
          -> F9-M3 produce/fetch
              -> F9-M4 producer/txn/internal topics
              -> F9-M5 retention/compaction
                  -> F9-M6 activation/multi-broker rollout
                      -> F9-M7 scale/chaos/compatibility aggregate
```

M4 与 M5 可以在 M3 后并行开发，但 M6 writable rollout 必须消费两者的能力版本；M7 必须执行全部前驱，
不能用 mock-only suite 代替真实 Oxia、BookKeeper/Object store 与多 broker process evidence。

## 9. 文档完成门禁（F9-M0）

F9-M0 只有在以下条件全部满足时完成：

- [x] AutoMQ source commit、version、关键 file blob hash 全部锁定；
- [x] Nereus 当前 API/reader/materialization 的 ranged-entry gap 有具体 class/method 证据；
- [x] F5/F9 ownership 与 coordinator-state 差异写入 roadmap/index；
- [x] public API 变更有 source/binary compatibility 策略；
- [x] NCP2/NTC2 byte layout、limits、golden/corruption tests 完整；
- [x] Kafka fork 每个 stock file 的 inject point、subclass 与 fallback path 列出；
- [x] Oxia keys、records、codec field order、CAS guards、scanner bounds 完整；
- [x] append/leader/delete/recovery/checkpoint/retention/compaction 状态机完整；
- [x] config default、validation、activation、shutdown、metrics、alerts 和 runbooks 完整；
- [x] 每个 MUST invariant 映射到 scenario ID 和计划测试 class；
- [x] 文档内部链接和状态词审计通过；
- [x] 没有提交任何 production implementation code。

勾选只代表 design gate，不代表 F9-M1 之后的能力已实现。

## 10. 评审顺序

1. 先读 `01`，确认事实与参考边界；
2. 再读 `02`，确认所有上层工作都建立在可表达 ranged Kafka batch 的中立合同上；
3. 联合评审 `03` 和 `04`，确认 Kafka state 与 Nereus state 没有双写 truth；
4. 评审 `05`，重点检查 internal topic、transaction、retention 与 compaction；
5. 评审 `06` 的 activation、failure mapping 与 runbook；
6. 用 `07`/`08` 逐项反查每个 class、transition 和 failure cut 是否有实现 owner 与 gate；
7. 用 `09` 查看 F9-M0 的 dated 审计结果、未实现边界与下一里程碑入口条件。
