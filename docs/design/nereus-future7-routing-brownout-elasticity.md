# 技术细节：Nereus Zone-aware Routing and Brown-out Handling

> 状态：Designed；routing/broker-session implementation 尚未存在
> 前置：Future 1 append-session/head-CAS semantics；Future 2/5 lookup projections

本文定义 ownership-decoupled broker serving 下的 zone-aware routing、preferred broker、brown-out
摘除和恢复。`leaderless` 指 broker 不持有 durable partition data，不表示同一 stream 可以绕过
append session/CAS 无序提交。

## 1. 目标

Ursa 的公开设计强调 stateless/leaderless broker、zone-aware routing 和 brown-out 快速
摘除。本设计需要达到同类能力：

- 任意 broker 可服务任意 stream；
- preferred broker 只负责 locality、batching、cache，不负责 correctness；
- client 尽量连接同 AZ broker，降低 cross-AZ traffic；
- broker crash/brown-out 不触发数据搬迁；
- routing 变化通过 Oxia notification 和 client retry 收敛；
- append correctness 仍由 Oxia session fencing、commit intent 和 stream-head CAS 保证。

当前实现边界：Phase 1 只有 stream-scoped append-session snapshot 和 fake metadata semantics；broker
membership、routing ring、health model、Pulsar lookup/Kafka metadata projection 都是 target design。

## 2. 核心概念

| 概念 | 含义 |
| --- | --- |
| Broker session | broker 在 Oxia 中的 ephemeral session |
| Broker zone | broker 所在 AZ/zone |
| Preferred broker | 某 stream 在某 zone 下的推荐服务 broker |
| Append session | Oxia-fenced append token |
| Routing ring | zone-local consistent hash ring |
| Brown-out | broker 未死亡但持续高延迟/低吞吐/错误率异常 |
| Degraded broker | 被 routing 暂时摘除但 session 可能仍存在 |

## 3. Oxia Routing Metadata

```text
/clusters/{cluster}/brokers/{brokerId}/session
/clusters/{cluster}/brokers/{brokerId}/load
/clusters/{cluster}/brokers/{brokerId}/health
/clusters/{cluster}/zones/{zone}/brokers/{brokerId}

/routing/rings/{zone}/version
/routing/rings/{zone}/members/{brokerId}
/routing/streams/{streamId}/preferred/{zone}
/routing/degraded/{brokerId}
```

Broker registration：

```json
{
  "brokerId": "broker-7",
  "zone": "us-east-1a",
  "host": "10.0.1.7",
  "protocols": ["pulsar", "kafka"],
  "capacity": {
    "maxIngressBytesPerSec": 1073741824,
    "maxConnections": 100000
  },
  "sessionTtlMs": 30000
}
```

## 4. Zone-aware Preferred Routing

Preferred broker 计算：

```text
preferredBroker = hash(streamId, zone, ringVersion) over healthy brokers in zone
```

规则：

- same-zone broker 优先；
- zone 内无健康 broker 时才跨 zone；
- preferred broker 变化不影响 stream correctness；
- lookup response 携带 routing version；
- broker 发现 routing version 过期时刷新 ring cache。

Pulsar lookup：

```json
{
  "brokerServiceUrl": "pulsar://broker-7:6650",
  "nativeUrl": "pulsar://broker-7:6650",
  "kafkaUrl": "broker-7:9092",
  "routingVersion": 128,
  "zone": "us-east-1a"
}
```

Kafka metadata response：

- Kafka protocol 仍需要返回 leader field；
- 该 leader 是 protocol projection，实际是 preferred broker；
- correctness 不依赖该 broker 持久拥有 partition。

## 5. Append Session 与 Preferred Broker 的关系

Preferred broker 不等于 append owner。

| 场景 | 行为 |
| --- | --- |
| preferred broker 有 append session | 正常写入，batch/cache locality 最佳 |
| non-preferred broker 收到 produce | 可以转发，也可以获取 append session 后直接写 |
| preferred broker brown-out | routing ring 摘除，client retry 到新 preferred broker |
| append session stale | stream-head commit validation rejects the writer |

实现策略：

- 默认同 zone preferred broker 直接处理 produce；
- 对低延迟 topic，允许 non-preferred broker 转发到 preferred broker；
- 对 cost profile，允许任意 broker 直接 append，减少跨 AZ hop；
- append session renew 失败后 broker 必须停止 ack 新 writes。

## 6. Brown-out Detection

Broker 定期上报 health/load：

```json
{
  "brokerId": "broker-7",
  "timestamp": 1783036800000,
  "publishLatencyP99Ms": 450,
  "fetchLatencyP99Ms": 300,
  "objectPutLatencyP99Ms": 500,
  "oxiaCommitLatencyP99Ms": 80,
  "walQueueBytes": 1073741824,
  "requestErrorRate": 0.05,
  "eventLoopDelayMs": 200,
  "cpuUsage": 0.92,
  "directMemoryUsage": 0.88
}
```

Degraded 判定：

```text
degraded if any condition lasts for degradationWindow:
  publishLatencyP99 > baselineP99 + 3 * stddev
  fetchLatencyP99 > baselineP99 + 3 * stddev
  requestErrorRate > threshold
  eventLoopDelay > threshold
  walQueueBytes > threshold
  objectPutLatencyP99 > threshold
  oxiaCommitLatencyP99 > threshold
```

推荐默认：

```text
degradationWindow = 30s
reAdmissionWindow = 60s
```

## 7. Brown-out Handling

当 broker 被标记 degraded：

1. 写 `/routing/degraded/{brokerId}`。
2. 从 zone ring active members 中临时移除。
3. 增加 ring version。
4. 发送 Oxia routing notification。
5. lookup/metadata response 返回新 preferred broker。
6. client 在 retry 或 metadata refresh 时切换。
7. 原 broker 停止 acquire 新 append sessions，但可以完成已进入 commit 阶段的 slice。

不变量：

- degraded broker 不删除 durable data，因为 broker 无 durable data；
- no partition rebalance copy；
- stale append commit 由 Oxia fencing 拒绝；
- acked records 仍由 stream head/reachable commits 解释，并通过 generation index 或 repair 读取。

## 8. Re-admission

Broker 恢复条件：

```text
all health metrics inside nominal envelope for reAdmissionWindow
and session is valid
and object/Oxia connectivity healthy
```

恢复步骤：

1. 删除 `/routing/degraded/{brokerId}`。
2. 加回 zone ring。
3. 增加 ring version。
4. 通知 broker/lookup refresh。
5. broker 逐步 warm cache，不立即承接过多 hot streams。

Warmup：

- 限制新连接速率；
- 预取 hot stream offset index；
- 延迟承接 compaction-heavy streams；
- 根据 cache hit ratio 逐步加权。

## 9. Client Retry

Pulsar client：

- lookup 返回 preferred broker；
- connection failure 或 `NotOwner/RetryLater` 后重新 lookup；
- producer sequence/dedup 避免 retry 重复可见；
- consumer cursor state 在 Oxia，换 broker 后恢复。

Kafka client through KoP：

- MetadataResponse leader field 返回 preferred broker；
- broker degraded 后 metadata version 更新；
- client retry 到新 leader projection；
- group coordinator 使用 `hash(groupId, zone)` 选择 preferred coordinator。

## 10. Cross-zone 策略

目标：正常情况下 client 只访问本 zone broker。

允许跨 zone：

- 本 zone 无健康 broker；
- topic policy 要求跨 zone durability/availability；
- client 未提供 zone；
- admin/read-only/historical read broker 策略选择跨 zone。

跨 zone routing 必须计入 metrics：

```text
pulsar_nereus_cross_zone_produce_bytes
pulsar_nereus_cross_zone_fetch_bytes
pulsar_nereus_cross_zone_lookup_total
```

## 11. Metrics

| Metric | 含义 |
| --- | --- |
| `pulsar_nereus_routing_ring_version` | zone ring version |
| `pulsar_nereus_broker_degraded` | broker degraded 状态 |
| `pulsar_nereus_broker_degraded_total` | degraded 次数 |
| `pulsar_nereus_broker_readmitted_total` | re-admission 次数 |
| `pulsar_nereus_routing_changes_total` | routing ring 变化次数 |
| `pulsar_nereus_preferred_broker_cache_hit_ratio` | request 命中 preferred broker 比例 |
| `pulsar_nereus_cross_zone_produce_bytes` | 跨 zone produce bytes |
| `pulsar_nereus_cross_zone_fetch_bytes` | 跨 zone fetch bytes |
| `pulsar_nereus_append_session_transfer_total` | append session 切换次数 |
| `pulsar_nereus_client_retry_after_routing_change_total` | routing 变化导致的 client retry |

## 12. Future 7 Design Gate

| Design question | Required answer |
| --- | --- |
| Broker crash remap | 无数据搬迁，新 broker 从 stream head/session 和 read index 继续 |
| Brown-out evict | degraded broker 从 active ring 移除，ring version 单调递增 |
| Re-admission warmup | 恢复 broker 渐进承接流量，不立即接管 hot stream |
| Same-zone routing | client 优先连本 zone broker |
| Cross-zone fallback | 本 zone 无健康 broker 时仍可服务 |
| Stale append session | degraded/stale broker commit 被 Oxia fencing 拒绝 |
| KoP metadata refresh | Kafka leader projection 从同一 routing state 派生 |
| Pulsar lookup refresh | Pulsar lookup 从同一 routing state 派生 |
| Group coordinator remap | coordinator 是 locality role，group state 从 Oxia 恢复 |

## 13. 与 Ursa 的目标设计对齐

目标设计已覆盖：

- broker stateless/leaderless；
- preferred broker 只做 locality；
- zone-aware routing 降低 cross-AZ traffic；
- broker crash/brown-out 不触发数据搬迁；
- metadata service/Oxia 决定 routing state。

增强点：

- 同时定义 Pulsar lookup 和 Kafka metadata projection；
- 将 append session fencing 与 preferred broker 解耦；
- 将 cache warmup、cursor recovery、group coordinator remap 纳入策略。

仍弱于 Ursa：

- Ursa 的 broker/routing 面向 Kafka-compatible engine 原生设计，Nereus 需要同时投影 Pulsar lookup 和 Kafka MetadataResponse；
- degraded 阈值和 readmission 策略在设计上只能给出状态机，实际阈值需要在后续产品 profile 中校准；
- Preferred broker 与 append session 解耦会让实现复杂度高于单协议 broker；
- KoP metadata projection 依赖 Kafka client 的 leader/coordinator 心智，错误码和 retry 语义需要在 Future 5/7 联合收敛。

## 14. 参考

- 总体架构：`nereus-overall-architecture.md`
- Commit protocol：`nereus-commit-protocol.md`
- KoP compatibility：`nereus-future5-kop-compatibility.md`
- Ursa VLDB paper: <https://www.vldb.org/pvldb/vol18/p5184-guo.pdf>
- StreamNative Lakestream architecture:
  <https://docs.streamnative.io/cloud/overview/lakestream-overview>
