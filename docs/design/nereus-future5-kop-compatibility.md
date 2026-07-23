# 技术细节：KoP/Kafka Compatibility on Nereus Storage

> 状态：Designed；`nereus-kop-adapter` 当前只有 marker class
> 前置：Future 1 stable append/read、Future 2 projection boundary、Future 4 generation reader

本文定义 KoP/Kafka 协议在 Nereus shared storage 上的映射、风险和兼容性边界。它仍是 Future 5
的方向设计；在实现前还必须冻结 canonical payload mapping，不能把 F2
`PULSAR_ENTRY_V1` 的 entry offset 当成 Kafka batch 内每条 record 的 offset。

## 1. 目标

总体架构以 Pulsar native 为主，Kafka 通过 KoP 进入同一套 stream storage。本文要保证：

- Kafka offset 与 internal stream offset 一致；
- Kafka record batch offset 能从 Oxia commit result 正确生成；
- consumer group metadata 与 offset commit 存入 Oxia；
- Kafka transactions 映射到 Pulsar/Oxia transaction state；
- Kafka topic compaction 复用 shared compaction service；
- KoP 不引入第二套 durable log。

当前 Phase 1 只接受 one-record-per-entry `OPAQUE_RECORD_BATCH`，没有 Kafka producer sequence、
transaction marker 或 record-batch decoder。Future 2 又明确以一条完整 Pulsar Entry 占一个 L0 offset。
Future 5 实现必须增加明确的 payload/projection contract，使 Kafka batch 中每条 record
占一个 L0 offset，或显式拒绝不兼容的 stream；不能假设当前 L0 已能直接持久化
任意 Kafka record batch。

> F5/F9 boundary：本文只约束 `Kafka client -> KoP -> Pulsar facade -> Nereus`。原生 KRaft Kafka fork 直接
> 使用 Nereus 的路径属于独立 Future 9，见 `nereus-future9-kafka-native-storage.md` 和
> `../phase-9-kafka-native-storage/README.md`。F9 不复用本文的 payload mapping、partition identity、group/txn
> Oxia records 或 retention floor；两条路径若未来互读，必须先定义新的 durable migration/mapping version。

## 2. 坐标映射

| Kafka 概念 | Nereus 概念 |
| --- | --- |
| Topic partition | Stream |
| Kafka offset | `streamId + recordOffset` |
| Log end offset | `committedEndOffset` |
| Fetch offset | offset index lookup |
| Consumer committed offset | Oxia group offset state |
| Record batch base offset | Oxia commit result base offset |
| Transaction marker | stream transaction marker / txn state |

Kafka offset 直接等于 stream record offset，但前提是 stream 的 mapping version 保证一条 Kafka
record 消耗一个 L0 offset。Pulsar MessageId 是另一个 projection，不能影响 Kafka offset。
F2 `PULSAR_ENTRY_V1` 将 batch 内所有 message 放在同一 offset，因此不满足该前提。

Future 5 必须在打开 partition 前读取并校验 L0 的 `nereus.payloadMapping` stream attribute：

| Mapping | KoP admission |
| --- | --- |
| `KAFKA_RECORD_V1` or reviewed canonical mapping | allow |
| `PULSAR_ENTRY_V1` with only single-message entries | reject by default；不依赖历史数据扫描猜测 |
| `PULSAR_ENTRY_V1` with batched entries | reject |
| missing/unknown mapping | reject |

即使历史上恰好都是 single-message entry，mapping attribute 也是持久化协议；不能靠扫描样本
动态猜测兼容性。

## 3. Produce Mapping

KoP produce path：

1. KoP 接收 Kafka ProduceRequest。
2. Broker 把 Kafka records 归一化为 stream append batch；`recordCount` 等于 Kafka record 数，
   decoder/index 保留每条 record 的边界。
3. Primary WAL 写入时可先使用 relative offsets。
4. Stream commit intent + head CAS 固化 `[baseOffset, endOffset)`；generation-0 offset index 可在 strict
   boundary 前物化，或由 resolver 从 reachable commit repair。
5. KoP 生成 ProduceResponse，返回 base offset。
6. 如果需要保留 Kafka record batch header 的 base offset，reader 在 fetch 时基于
   offset index result 重写或补齐。

不变量：

- ProduceResponse 的 base offset 必须来自 stable stream-head commit result；
- broker 不能提前返回本地 offset；
- failed slice 不返回成功 ProduceResponse；
- retry 通过 Kafka producer id / sequence 去重；
- one batch 的 committed range 必须是 `[baseOffset, baseOffset + recordCount)`，不能是 F2 的
  one-entry/one-offset 编码。

## 4. Fetch Mapping

Fetch path：

1. Kafka fetch offset -> `streamId + offset`。
2. 查询/修复 Oxia generation-aware offset index。
3. 若 index 指向 WAL object，读取 row-based payload 并生成 Kafka record batch。
4. 若 index 指向 compacted Parquet object，读取 row group 并投影成 Kafka record batch。
5. 应用 transaction visibility 和 topic compaction visibility。
6. 返回 records 和 high watermark。

High watermark：

```text
highWatermark = stream.committedEndOffset
```

Kafka leader epoch 必须由 routing/stream epoch 的显式 projection 定义；不能直接把 compaction
generation 当成 leader epoch。

## 5. Consumer Group

Group metadata 存入 Oxia：

```text
/kafka/groups/{groupId}/state
/kafka/groups/{groupId}/members/{memberId}
/kafka/groups/{groupId}/offsets/{topic}/{partition}
```

Group coordinator assignment：

- 使用 `hash(groupId, zone)` 选择 preferred coordinator；
- coordinator 是 locality role，不是 durable owner；
- group state CAS 存入 Oxia；
- coordinator failover 后新 broker 从 Oxia 恢复 group state。

Offset commit：

```json
{
  "groupId": "g1",
  "topic": "t",
  "partition": 0,
  "committedOffset": 1048576,
  "metadata": "...",
  "generationId": 12,
  "commitVersion": 88
}
```

Kafka committed offset 与 Pulsar cursor 可以共享 stream offset truth，但不能互相覆盖。
Kafka group offset 是 Kafka group 语义；Pulsar subscription cursor 是 Pulsar 语义。

## 6. Kafka Transactions

Kafka transaction state 映射：

```text
/kafka/transactions/{transactionalId}/producer-state
/kafka/transactions/{transactionalId}/txn/{producerEpoch}/{sequence}
```

规则：

- producer id / epoch 由 KoP coordinator 管理，durable state 在 Oxia；
- transaction records 写入 stream，但对 `read_committed` fetch 不可见；
- commit marker 发布后 records 可见；
- abort marker 发布后 records 被 abort index 跳过；
- transaction offset commits 与 transaction state 一起进入 Oxia state machine。

Future 5 必须显式覆盖：

- idempotent producer sequence；
- transaction fencing；
- transaction timeout；
- read_committed/read_uncommitted；
- transaction offset commit。

## 7. Topic Compaction

Kafka topic compaction 复用 distributed compaction service：

1. Planner 读取 stream offset index。
2. Worker 扫描 uncompacted ranges。
3. Worker 对 key 保留最高 offset record。
4. Worker 写 compacted object。
5. Committer 发布更高 generation offset index。
6. Fetch 读取最高 generation。

被丢弃 record 的 offset 仍属于 compacted range。Fetch 不返回旧 value，但 offset
coverage 不出现 gap。

## 8. Record Batch Offset Rewrite

Object WAL 可以在写入时使用 relative offsets。Fetch 时必须保证 Kafka client 看到正确
base offset。

两种策略：

| 策略 | 说明 |
| --- | --- |
| Read-time rewrite | 根据 Oxia offset index result 重写 record batch base offset |
| Finalized segment rewrite | compaction 时写入最终 base offset 到 compacted object |

默认：

- tail read 使用 read-time rewrite；
- compacted object 使用 finalized offsets；
- checksum 在 rewrite 前后要分层处理：object checksum 保护 stored bytes，Kafka batch
  checksum 保护返回给 client 的 batch。

## 9. 与 Pulsar 语义隔离

只有当一个 reviewed canonical mapping 同时定义 Pulsar 和 Kafka 投影时，同一个 stream 才可以有
两类外部位置：

- Pulsar MessageId；
- Kafka offset。

隔离规则：

- open/load 先校验 `nereus.payloadMapping`；不兼容时在任何 append/fetch IO 前拒绝；
- Kafka group offset 不更新 Pulsar subscription cursor；
- Pulsar cursor 不更新 Kafka group offset；
- retention/GC 使用所有 cursor/group offset 的 low watermark；
- transaction visibility 使用同一 stream txn/abort metadata；
- schema id 可以共享，但 schema registry API 语义要隔离。

## 10. Future 5 Design Gate

| Design question | Required answer |
| --- | --- |
| Produce offset | ProduceResponse base offset 必须来自 stable stream-head commit result |
| Fetch from WAL object | read-time offset rewrite 必须有 checksum 分层策略 |
| Fetch from compacted object | finalized offset 必须与 stream offset 一致 |
| Consumer group failover | group coordinator 是 locality role，group state 从 Oxia 恢复 |
| Offset commit CAS | stale generation 必须被拒绝，retry 必须幂等 |
| Idempotent producer retry | producer id / sequence 不得重复推进 committed end offset |
| Kafka transaction commit | `read_committed` 只能看到 committed transaction records |
| Kafka transaction abort | aborted records and offset commits 必须不可见 |
| Topic compaction | latest value by key，offset coverage 不出现 gap |
| Payload mapping | every Kafka record consumes exactly one L0 offset and mapping version is durable |
| Mixed Pulsar/Kafka access | only a reviewed canonical mapping is admitted；`PULSAR_ENTRY_V1` is rejected |

## 11. 与 Ursa 的目标设计对齐

目标设计已覆盖：

- Kafka offset 使用 stream offset；
- broker 不维护 Kafka local log；
- consumer group metadata 可放 Oxia；
- topic compaction 可由 compaction service 实现；
- primary WAL durable + stable head commit 才能返回 produce ack；strict profile 还等待 generation-0
  index confirmation。

本设计增强点：

- 同一 stream 同时支持 Pulsar MessageId projection；
- Kafka group offset 与 Pulsar cursor 共用 retention low-watermark 计算；
- KoP 可复用 Pulsar auth/multi-tenancy/schema 能力。

仍弱于 Ursa：

- Ursa 是 Kafka-compatible engine，本设计依赖 KoP 适配层；
- Kafka protocol 边界受 KoP 当前实现影响，Nereus 需要把 KoP coordinator state 明确迁入 Oxia；
- record batch offset rewrite 会引入额外 CPU/checksum 路径，设计上需要保留 finalized segment rewrite 选项；
- Kafka ecosystem compatibility 不是 L0 能力，必须在 Future 5 作为独立兼容层处理。

## 12. 参考

- 总体架构：`nereus-overall-architecture.md`
- Commit protocol：`nereus-commit-protocol.md`
- Ursa VLDB paper: <https://www.vldb.org/pvldb/vol18/p5184-guo.pdf>
