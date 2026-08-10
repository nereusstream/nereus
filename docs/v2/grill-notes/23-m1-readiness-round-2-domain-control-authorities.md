---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# M1 Readiness Grill round 2: domain and control authorities

Date: 2026-08-10

Round 2 descends from ADR 0081 into the exact M1 domain identity, aggregate encoding, production metadata capability,
Kafka topic-create input, Pulsar ownership witness, and virtual-ledger Registry admission contracts. The answer below
was given against clean Nereus `main` at `9145b69ffb2c84e4df2fec80866efa5ab07df0ca`; no M1 implementation or runtime
evidence existed before this documentation update.

The normative result is [ADR 0082](../../decisions/0082-v2-m1-domain-and-control-authority-contracts.md), with local
refinements in the earlier identity, aggregate, Kafka, Pulsar, Registry, allocator-evidence, plan, and scenario owners.
This record preserves the complete adjusted answer. It is not implementation, source promotion, allocator selection,
or PASS evidence.

## Adjusted answer preserved verbatim

结论：Q1–Q6 都建议“调整后确认”。方向整体正确，主要缺口在 Q3 的 Registry SPI、Q5 的 stale-install 竞态，以及 Q6 的 writer-set 全集证明。

### Q1 — 调整后确认

- 只有 `deploymentId/reservationDomainId/*CellId/topic UUID` 是 16-byte；`bindingId/storageEpochId` 是 32-byte SHA-256。
- bootstrap ID 必须 create-only、非零、不可由名称或配置重算；Cell 重建使用新 ID。
- 精确 preimage 冻结为：

```text
NTB1 || u32be(cellLength) || cellBytes
     || u32be(incarnationLength) || incarnationBytes

NSE1 || bindingId[32] || u64be(epochOrdinal)
```

M1 只允许 ordinal 0。Kafka UUID 必须编码为原始 16 bytes，禁止字符串形式。ID 计算只发生在 create/replay，不进入热路径。

### Q2 — 调整后确认

接受“一个逻辑模型、两个物理 encoding、无通用扩展字段”，但补充：

- `NTA1` 是 domain canonical aggregate payload，Oxia envelope 只负责包裹；Kafka generated record 直接映射 domain，不生成临时 NTA1 bytes。
- ID preimage 使用独立、稳定的 canonical 子编码，不能依赖 Oxia envelope 或 Kafka wire。
- 冻结字段顺序、enum code、presence byte、`u32be` 长度/count、固定数组宽度，并拒绝 trailing bytes、overflow、unknown discriminator。
- UTF-8 必须严格编码/解码，malformed/unmappable 直接失败；domain 不做 NFC、lowercase 或 replacement。
- parser cap 应从 pinned 协议边界推导后固化为 v1 format constants。Deployment 只能降低，不能运行时放大或重新推导。
- v1 不追加 unknown optional fields；演进使用 NTA2/logical schema v2/Kafka 新 wire version。

### Q3 — 实质调整后确认

原列表缺少 M1 已要求的生产 authority，必须补：

- `PulsarVirtualLedgerNamespaceRegistryStore`

同时不建议把 Binding/Epoch 做成两个真正的 backend store。优先采用：

```text
TopicBindingAggregatePublisher
TopicBindingAggregateReader
PulsarTopicGenerationSelectorStore
PulsarVirtualLedgerNamespaceRegistryStore
```

Binding 与 initial Epoch 由同一个 `VersionedAggregateSnapshot` 在 domain 内投影。若保留原两个接口名，也必须保证一次 aggregate read/decode、多视图复用，禁止 child key、child write、独立 cache/watch、list 或未来 epoch mutation。

所有 conditional mutation 使用封闭结果：

- create：`CREATED | EXISTING_EXACT | DEFINITIVE_CONFLICT | INDETERMINATE`
- CAS：`APPLIED_EXACT | PREDECESSOR_UNCHANGED | DEFINITIVE_CONFLICT | INDETERMINATE`

“EXACT”必须匹配 key、schema、digest 和 canonical stored bytes，不能只比较部分逻辑字段。Kafka KRaft 不实现这些 SPI；`OwnershipWitnessProvider` 继续留在 Pulsar native integration。Allocator candidate SPI 仍严格 evidence-only。

### Q4 — 调整后确认

所有成功进入 `TopicImage` 的 topic 都必须有 Aggregate，不按名称豁免；KRaft metadata log 不属于这一集合。

推荐把 Nereus 配置做成 CreateTopics 的 input-only pseudo-config：

- 解析并 resolution 后从普通 ConfigRecord changes 中移除；
- resolved value、origin 和 policy/catalog version 只进入 Aggregate；
- DescribeConfigs 如需展示，从 Aggregate 合成 read-only projection；
- AlterConfigs 的 change/delete 全部明确拒绝。

这样不会形成 Aggregate 与 ConfigRecord 双权威。

Internal topic 也进入 Nereus authority，但使用显式、版本化的 Kafka internal-topic Deployment policy，不继承 tenant default，也不必与普通用户 topic 默认完全相同。

`validateOnly` 使用同一纯 resolution、domain validation 和 batch admission。完整公式必须包含：

```text
TopicRecord
+ AggregateRecord
+ native ConfigRecord*
+ PartitionRecord*
+ record/batch serialization overhead
```

继续保留 Kafka 原生 per-topic partial-success 语义；“atomic”只指每个成功 topic 的全部记录在同一 batch 发布。

### Q5 — 调整后确认

128-bit `acquisitionId` 必须在第一次 conditional acquire 前生成；同一次响应未知重试复用，真正的 reacquire、transfer、forced takeover、missing-record recreate 必须换新。

严格来说随机 128-bit 是 collision-resistant，不是数学上的绝对 never-reuse。应要求 CSPRNG、全零非法、进程内重复检测；若合同要求绝对证明，还需绑定 backend creation/session revision。

还必须补：

- 冻结每种 native ownership transition 是继承还是生成新 acquisitionId。
- A/B 必须是 authoritative read；ELM 本地 eventual TableView 不合格。
- A/B 后安装缓存时还要 CAS 校验 ownership/selector watcher sequence，防止“失效先到、旧 installer 后写回 valid”。
- 热路径使用一个原子 fence word，而不是可撕裂的 `generation + valid` 两字段；admission 捕获，完成/ACK 前再次全等检查。
- V2 强制的是 witness capability，不必强迫所有第三方 backend 实现；未实现的 backend fail closed。Legacy/ELM 只有通过 capability gate 才能宣称支持。

成本是控制路径两次 ownership read；可按 service unit 有界合并多个 topic 的验证。普通读写仍只有本地原子读取。

### Q6 — 实质调整后确认

`deploymentId + reservationDomainId` 还不足以隔离 Registry。Registry authority 必须首先绑定实际共享 ledger-ID 空间的 immutable `ledgerIdCompatibilityNamespaceId`。0.2 最简单的边界是一个 compatibility namespace 只允许一个 admitted reservation Registry。

writer-set digest 本身不能证明“没有漏掉 writer”。需要：

- bounded canonical writer set 直接进入 Registry；或
- immutable、content-addressed writer-set snapshot，并由 Registry 绑定其 exact key/version/length/SHA。

同时必须有 ACL、credential 或 deployment admission interlock，确保未进入 selected writer set 的 native/BookKeeper/custom writer无法分配 ledger ID。新 writer 只有看到 Registry 已选择包含自己的 snapshot 后才能启动；移除 writer 要先 fence/drain，再切换 commitment。滚动升级期间 writer set 可以同时包含 old/new source-qualified writer。

其他调整：

- Registry key identity 在 value 中重复校验，`registryEpoch` 每次成功变更严格 `+1`。
- evidence 使用 typed reference，不只存裸 digest。
- assignment row 直接编码 identity、bounds、lifecycle；不要在 M1 提前冻结未来 retirement-proof wire。
- allocator 使用版本化 derived slice view，禁止每次 rollover 读取或复制 64 KiB Registry。
- Receipt 拆成 `REGISTRY_CONFORMANCE` 与 `HARNESS_CONFORMANCE_ONLY`；后者继续固定 `selectionEligible=false`，不能替 Registry 宣称 PASS。

性能上没有过度设计：SHA/codec/CreateTopics 都在控制面；Pulsar 热路径是本地原子检查；Registry 只在 bootstrap、Cell assignment 和 writer rollout 时执行有界 CAS。真正较深的成本是 Pulsar ownership 状态转换兼容和 writer admission 治理，而不是 append/read 吞吐。

本轮只读核对，未修改文件。

## Normative mapping

- Q1/Q2 refine ADRs 0028/0033 and freeze exact deterministic-ID and canonical aggregate representation rules.
- Q3 refines ADRs 0023/0081 and the metadata capability contract without creating child authorities.
- Q4 refines ADRs 0034/0042/0050 and the Kafka M1 scenario rows.
- Q5 refines ADR 0051 with authoritative witness reads, stale-install exclusion, and one atomic hot-path fence word.
- Q6 refines ADRs 0032/0041/0054/0055 and separates Registry correctness evidence from allocator-harness evidence.
