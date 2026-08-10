---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# M1 Readiness Grill round 3: wire, control-plane, and evidence bounds

Date: 2026-08-10

Round 3 descends from ADR 0082 into canonical Cell/incarnation bytes, the remaining NTA1 boundary, Kafka create
configuration and atomic-batch admission, the first Pulsar ownership-witness adapter candidate, BookKeeper ledger-ID
namespace identity, Registry writer membership, and M1 receipt encoding. The answer below was given against clean
Nereus `main` at `5d3250b6bdbcb38e029605153703f5d2e6ee66ff`; no M1 implementation or executable evidence existed before this
documentation update.

The normative result is [ADR 0083](../../decisions/0083-v2-m1-wire-control-and-evidence-bounds.md), with local
refinements in the earlier identity, aggregate, Kafka, Pulsar, Registry, promotion, plan, and scenario owners. This
record preserves the complete adjusted answer. It is not implementation, source promotion, allocator selection, or
PASS evidence.

## Adjusted answer preserved verbatim

结论：没有明显的数据热路径型过度设计，但控制面有四处可以明显减负：

- 不另造 compatibility namespace 随机身份，优先绑定 BookKeeper 原生 `INSTANCEID`。
- Registry 不持久化每次构建的 source SHA。
- Pulsar restart 不复用旧 acquisition。
- Kafka sizing 禁止 O(n²) 重序列化。

建议 Q1/Q3/Q7 按约束确认，Q2 保持部分 OPEN，Q4/Q5 调整后确认，Q6 不按原案确认。

### Q1 — 调整后确认

Pulsar 的 `reservationDomainId` 不建议移出 `cellBytes`。它是已冻结的 slice-owner identity，不是可轮换的 provider 配置。移除后反而要新增跨 Registry 的 Cell/domain 唯一性证明。

```text
Kafka:
NPC1 || u16be(KAFKA)
     || deploymentId[16]
     || kafkaCellId[16]

Pulsar:
NPC1 || u16be(PULSAR)
     || deploymentId[16]
     || reservationDomainId[16]
     || pulsarCellId[16]
```

`ledgerIdCompatibilityNamespaceId`、provider scope、broker alias继续排除。另需冻结：

- protocol code，0 非法；
- Kafka topic UUID 拒绝 zero/reserved UUID；
- Pulsar generation 为 `1..Long.MAX_VALUE`，按 u64be 编码，溢出 fail closed；
- NTA1、NPC1、NTI1 中的 protocol discriminator 必须全等。

这些只在 create/replay 计算，没有热路径成本。

### Q2 — layout 方向确认，numeric caps 继续 OPEN

Flat、顺序、无 TLV/map/self-digest/extension tail 是当前最简单的方案，不算过度设计。

但还不能称为“exact NTA1”：

- 尚缺完整 field width、enum code、`NONE` 组合及 variant payload；
- `cellBytes/incarnationBytes` 仍需显式 u32 framing；
- initial sealed-end presence 在 v1 只能是 `0x00`，不能借 `0x01` 偷渡未来扩展；
- Pulsar pinned source没有原生名称长度上限，因此必须选择并证明 V2 自己的 UTF-8 byte cap；
- total NTA1 cap必须在实现前固定，不能等 parser 写完以后决定。

Kafka 249 是 ASCII bytes，可直接冻结。Deployment 只能降低新写 admission，不能降低 persisted-v1 decoder cap。

### Q3 — 调整后确认

只开放一个 input-only pseudo-config 是很好的复杂度控制：

```text
nereus.storage.profile =
  OBJECT_WAL
  BOOKKEEPER_WAL_ONLY
  BOOKKEEPER_WAL_ASYNC_OBJECT
```

补充约束：

- Kafka M1 只有 Deployment user-topic default，不从 topic 名称推断 Namespace。
- 当前三个 built-in internal topic冻结为 classifier v1；Streams、Connect、MM2 等仍是普通 topic。
- built-in internal topic拒绝显式 profile，由 internal Deployment policy唯一决定。
- 所有能进入 `TopicImage` 的创建入口都必须走相同 resolution/aggregate 路径，不只 public CreateTopics。
- 只拦截精确的 `nereus.storage.profile`；未知 `nereus.*` 交给 stock config validator，避免改变原生错误优先级。
- AlterConfigs 的 SET/DELETE/APPEND/SUBTRACT、same-value SET 全部拒绝。

DescribeConfigs 可以保留原映射，但必须注明继承值是“创建时冻结的默认来源”，不会跟随后续 Deployment 默认变化；两种结果都 `readOnly=true`、无 synonyms。

### Q4 — 调整后确认

Request-order greedy admission可以接受：跳过放不下的 B，继续接受较小的 C，不破坏 Kafka per-topic partial success。最终 A+C 仍只能发布为一个 atomic controller batch。

必须调整：

- 保留 stock 的 request-wide 10,000 partition cap及其 `POLICY_VIOLATION`。
- 新增的 atomic-batch count/byte overflow也使用 per-topic `POLICY_VIOLATION`，不建议新定 `INVALID_REQUEST`。
- 先生成无副作用的 `TopicCreateCandidate`；通过 admission 后才提交 quota、success map和共享 records。
- 被拒 topic不得留下 quota charge、topicId publication或其他 residue。
- ConfigRecord按 key、PartitionRecord按 partition ID稳定排序。
- 抽取与 Raft `RecordSerde/BatchBuilder` 共用的纯增量 sizer；每个 record只计算一次，复用 serialization cache。
- 禁止每加入一个 topic重新序列化整个前缀，也禁止为了 sizing 分配完整最大 batch buffer。
- 最终 Raft guard继续保留，只是不应首次发现正常的 oversized CreateTopics。

额外成本是大型 CreateTopics 的一次 O(total records) sizing，不影响 Produce/Fetch。

### Q5 — 调整后确认

方向正确，但首版能力应明确为“经 current-source 验证的 Oxia-backed MetadataStore ELM adapter”，不能泛化为所有 MetadataStore。当前 TableView是 eventual view，现有 wrapper还会吞 conflict，不能承担 witness authority。

关键调整：

- A/B 使用专用 direct GET + Stat/CAS，且仍复用原生 ELM transition validator，不能创建 sidecar ownership authority。
- 删除“进程 restart 后 authoritative reread即可继承”。
- 同进程、同 backend session 的短暂 ConnectionLost/Reconnected可以继承。
- SessionLost、进程 restart、transfer、forced takeover、missing/tombstone recreate、split child一律使用新 broker incarnation和新 acquisitionId。
- ownership、selector、aggregate三类失效源都推进同一个 local INVALID sequence。
- 安装仍是从精确 `INVALID(seq)` CAS到 `VALID(seq)`；任何先到的 callback都会使旧 installer CAS失败。
- 本地只保留一个 atomic fence word；不新增持久 INVALID/VALID 状态，也不在热路径解析 witness。
- admission捕获 fence，ACK/响应发布前再次全等检查。

普通读写只增加两次本地原子读取。成本集中在 open/takeover；可按 service unit有界合并验证。

### Q6 — 不按原案确认

原案中“另建随机 marker + Registry 保存 source SHA”增加升级状态机，却没有更强的全集证明。

建议改为：

- 优先用 BookKeeper 原生 `INSTANCEID` 经 domain-separated SHA-256 派生32-byte `ledgerIdCompatibilityNamespaceId`。
- BookKeeper format会更换 `INSTANCEID`，新 namespace自然获得新 identity。
- 如果仍保留 Nereus marker，它必须绑定 exact `INSTANCEID`，并在 format/nuke 后永久失效；不能独立存活。
- 0.2只支持 fresh namespace bootstrap，避免 existing-root migration saga。
- inline writer set继续保留，它比外部 snapshot简单且没有跨 key TOCTOU。

Writer row最小化为：

```text
writerKind
writerEntryId
allocator/exclusion contract version
independently revocable principal generation/digest
interlock policy generation/digest
typed conformance-evidence reference
```

source commit/artifact SHA放进 conformance receipt，不放进长期 Registry identity。否则每次无语义变化的 rebuild都要执行 `{old,new} → remove old` Registry迁移，而且仍无法证明实际进程运行的是对应二进制。

其他约束：

- 共享 credential不合格，old/new必须能独立撤销。
- Registry absence已经表示 inactive，不新增持久 `INACTIVE` 生命周期。
- writer count和row bytes必须有独立hard cap，不能只依赖64 KiB总长度。
- 首次激活必须先升级全部 writer、撤销旧无范围限制的principal并做negative allocation proof，最后一次create/CAS激活Registry。
- allocator仍只读derived slice view，不逐 rollover读取64 KiB Registry。

### Q7 — 调整后确认

一个canonical JSON envelope加两个closed payload kind足够，不需要两套框架。

补充：

- 明确采用RFC 8785/JCS或等价固定字节grammar；拒绝duplicate、unknown field、float、BOM和非canonical编码。
- `HARNESS_CONFORMANCE_ONLY.selectionEligible=false`应由schema固定，不能是可自由填写的boolean。
- 测试明细至少包含 `discovered/executed/passed/failed/skipped/aborted`，并按suite/scenario绑定。
- source tuple必须补充Oxia server image digest及client/test artifact identity。
- Registry raw bytes、writer set、ACL/interlock快照和日志作为content-addressed attachments；root receipt只保存path/length/SHA，不复制大量base64。
- content hash只证明文件未变；可信度由trusted workflow和受保护的N3 evidence提交提供，M1无需再引入receipt自签名。
- N3允许提交receipt、attachments及其精确覆盖的scenario status/index；不得修改代码、gate或source locks。

Q7完全不进入生产运行时。

总体看，必要复杂度集中在Kafka创建控制面、Pulsar takeover和Registry bootstrap；正常 append/read仍只有既定本地fence检查。最值得删掉的是Q6的第二身份和source-SHA驱动的运行时升级状态机，而不是删正确性所需的A/B witness或atomic-batch admission。

本轮只读核对，未修改文件。

## Normative mapping

- Q1/Q2 refine ADRs 0028/0033/0082. They freeze NPC1 and the flat NTA1 direction while deliberately retaining the
  complete field/code/variant and numeric-cap table as OPEN.
- Q3/Q4 refine ADRs 0034/0042/0050 with one pseudo-config, classifier-v1, residue-free request-order admission, and
  one-pass incremental sizing.
- Q5 refines ADR 0051 with the first adapter-candidate boundary, restart/session transition rules, and one shared local
  invalidation sequence. Pinned-source verification then narrows the current capability to primitives and keeps adapter
  conformance OPEN.
- Q6 refines ADRs 0032/0041/0054 with INSTANCEID-derived compatibility identity, inline writer membership, fresh-only
  bootstrap, and source identities confined to evidence.
- Q7 refines ADR 0081 with one strict canonical receipt envelope, two closed kinds, attachment references, and an
  evidence-only N3 scope.
