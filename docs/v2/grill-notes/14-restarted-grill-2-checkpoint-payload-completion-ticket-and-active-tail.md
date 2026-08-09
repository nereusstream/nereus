---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 12: checkpoint payload, completion ticket, and active-tail readability

Date: 2026-08-09

Round 11 fixed the permanent class/lane catalog and complete leaf grammar, provider-resolved checkpoint eligibility,
one publisher-epoch-fenced combiner, and the separation between physical lane resolution and binding Durable Frontier.
It also fixed a lazy reconstructible tracker shape rather than a persistent per-Topic TreeMap. The next independent
frontier is the minimal data carried by the physical checkpoint, the normal-path ring index, and the read-publication
cut required before B may ACK. No recommendation below is normative until explicit confirmation.

## Source facts and corrected constraints

- `laneSequence` is an ADR-0046 HKDF/nonce input. ADR 0062 therefore interprets the confirmed pre-allocation “seal” as
  immutable group membership/policy plan seal; sequence must precede final encryption/ciphertext-body seal.
- ADR 0063 makes checkpoint physical inventory. Copying `BindingDurableFrontier` into its page or Seal as authority
  would reunify the two states that Round 11 separated. Omitting every binding summary costs bounded header/directory
  prefix GETs during recovery but adds no new trust path.
- ADR 0064 accepts a normal-path ring/window but no durable `appendOrdinal` exists in the current V2 schema. A ring may
  use an owner-local ticket because serial allocation preserves local order, but recovery must use typed Position Domain
  adjacency from durable descriptors.
- The append contract requires acknowledged coverage to be both durable and readable. A provider-resolved B cannot ACK
  merely because its Object can enter checkpoint: before manifest materialization, an owner-local active-tail view must
  locate B's immutable frame without making checkpoint or a new per-group metadata row the read authority.
- NPD1 numeric caps, NWG1 prefix/row caps and target values, and allocator mode/wire remain evidence-blocked and are not
  part of this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-20` |
| Q2 | `V2-OPEN-OBJ-21` |
| Q3 | `V2-OPEN-READ-01` |

❓ **Q1** - **Physical checkpoint and Seal payload**: Should a checkpoint duplicate append-unit coverage/frontiers so
recovery can avoid directory reads, carry them only as hints, or remain a strictly physical extent inventory?

➡️ Recommend a physical-only 0.2 descriptor:

```text
ProviderResolvedExtentDescriptorV1 {
  walRunRootSha
  laneId
  laneSequence
  directoryPrefixEnd
  bodyLength
  objectSha256
  optionalProviderVersionAndQualifiedProof
}
```

The page adds its ordinal, predecessor SHA, body SHA, publisher epoch, and `LaneExtentResolvedThrough` vector. It stores
no copied `BindingDurableFrontier`, ACK bitmap, waiting-gap state, or per-binding coverage rows. The final Seal binds
Root, terminal physical vector, final page head/SHA, and aggregate extent/body counts needed for validation; it likewise
stores no binding frontier. Recovery uses each descriptor's bounded prefix GET to authenticate the in-body directory
and rebuild binding units. A qualified provider version/proof may avoid weaker provider verification but cannot replace
the leaf-derived expected identity.

This spends bounded recovery GETs to keep one authority, smaller checkpoint rows, and no stale frontier snapshot.
Only benchmark evidence that prefix verification misses the recovery SLO should reopen an optional non-authoritative
binding summary; it must not be added pre-emptively.

❓ **Q2** - **Owner-local completion ticket and ring indexing**: How should the O(1) normal-path ring identify order
without persisting a new append ordinal or treating a local integer as protocol truth?

➡️ Recommend an owner-local checked 64-bit `CompletionTicket`, allocated only after the binding has passed tracker
capacity admission and the Position Domain has allocated exact coverage. The serialized binding writer makes ticket
order match allocation order, but every slot also stores exact coverage and expected predecessor; release still
validates Position Domain adjacency.

The ticket exists only in the owner instance and waiting future. It is not encoded in NWG1, checkpoint, manifest,
idempotency identity, or a metadata key. A bounded ring slot carries ticket generation, coverage, expected predecessor,
completion state, descriptor reference, and optional future; after provider resolution it carries no payload. The owner
must backpressure before overwriting a live slot or numeric wrap. Takeover discards tickets and reconstructs with the
accepted bounded Position-Domain-aware ordered structure.

The tradeoff is two implementations—O(1) ring normally and ordered recovery fallback—but no durable sixth ordering
domain and no permanent TreeMap per Topic.

❓ **Q3** - **Active-tail read publication before ACK**: What makes B readable after independent frontier advancement
when its Object is not yet in a manifest and checkpoint is only physical inventory?

➡️ Recommend one owner-local derived `BindingActiveTailIndex` per active binding, instantiated lazily and storing only
compact authenticated frame/commit-set locator references. For each provider-resolved Object:

1. validate the shared Object/header/directory and B's complete unit;
2. install B's immutable read locator in the active-tail index under its typed coverage;
3. complete B's contiguous tracker and advance `BindingDurableFrontier` plus `ReadableFrontier` together for that
   coverage;
4. only then complete B's protocol ACK.

A can remain pending from the same Object. Index installation failure or local memory pressure backpressures that
binding before further position allocation and cannot produce an ACK. The index stores no payload and is not remote
authority. On owner open/takeover, bounded Root + checkpoint + LIST recovery reconstructs/validates it before append or
active-tail read admission; normal reads then use the local locator without a metadata call. Entries retire only after
the manifest-selected generation covers the same typed range and source protection/read pins permit removal.

This adds a compact per-binding active-tail index and takeover rebuild cost, but it satisfies the existing
durable-and-readable ACK contract without per-group metadata publication or making checkpoint a logical read view.

## Deferred descendants

- Q1 must settle before final checkpoint/Seal field IDs and page-size arithmetic freeze.
- Q2 must settle before completion APIs, ring wrap tests, and exact tracker numeric budgets freeze.
- Q3 must settle before the M3 active-tail reader/index API, ACK completion order, and takeover-open gate freeze.
- Exact numeric tracker/index/page budgets are evidence/admission outputs after the structures are selected.
- `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19`, and
  `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Original confirmation boundary

At proposal time, no Round 12 recommendation above was normative. The adjusted response below records the explicit
confirmation; only its confirmed conclusions move to ADRs/contracts, while evidence-blocked values/modes remain open.

## Adjusted response preserved verbatim

````text
Round 12 不按原文全部确认：Q1、Q2 调整后确认；Q3 确认正确性目标，但收紧实现边界。

Q1 — 调整后确认 physical-only checkpoint/Seal。

1. 不保存 BindingDurableFrontier、ReadableFrontier、ACK bitmap、gap state 或 per-binding coverage，避免第二套逻辑权威。
2. runtime ProviderResolvedExtentDescriptor 可以携带 walRunRootSha 做防御性校验，但 root-bound checkpoint page 的 wire row 不应逐条重复它；page header 已绑定 Root。否则 256 行会白白重复约 8 KiB SHA 数据。
3. optionalProviderVersionAndQualifiedProof 必须是有界、canonical、确定性的 proof 字段；不能是任意 provider opaque blob，也不能替代 Object key/body identity 和 directory AEAD。
4. 恢复只对 manifest 尚未覆盖的 active tail 做有界、并行 prefix GET，并统一计入 GET/bytes/time recovery envelope；不做整 Object GET。
5. Seal 只绑定 physical terminal vector、final page head/SHA 和必要的 aggregate count/bytes。

只有恢复基准无法满足预声明 SLO 时，才重新讨论非权威 binding summary。

Q2 — 调整后确认 owner-local CompletionTicket，但它只是内部运行时机制，不进入产品 wire/API/config。

1. 在协议位置分配前，必须同时预留 tracker slot 和 active-tail locator budget；完成预留后才能分配 coverage 和 ticket，避免位置已经分配后才发现索引容量不足。
2. 一个 KafkaAppendCommitSet 或一个 Pulsar entry 分配一个 ticket，不按 frame、record 或 batched message 细分。
3. ring slot 保存完整 64-bit ticket；完整值相等即可作为 slot ABA fence，不再增加独立 ticketGeneration。
4. exact coverage 决定 Position Domain adjacency；expected predecessor 只能是派生缓存，不是另一种顺序权威。
5. takeover 丢弃 ticket。恢复优先采用 bounded collect + Position Domain sort，再重新生成本地 ticket 并复用同一 ring/window；只有基准证明流式稀疏恢复确实需要时，才保留长期 ordered-map 实现。

正常路径成本只是一轮本地容量检查、64-bit increment 和数组访问，不增加 metadata/Object I/O，预计不是性能风险。

Q3 — 确认“ACK 前必须建立 active-tail 可读视图”，但不冻结为每 Binding、每 append unit 一个重型索引对象。

1. 逻辑上仍按 Binding 隔离；物理实现允许使用 shard-owned segmented index，并采用 Kafka offset range、Pulsar ledger/entry range 的协议专用结构，禁止通用 ProtocolCoverage TreeMap 进入热路径。
2. 每个共享 Object 的 digest/header/directory/AEAD 只验证一次。ACK 路径复用该 VerifiedExtent 结果，禁止新增 HEAD/GET、KMS、metadata 调用或重复整目录解密。
3. locator 尽量按 {binding, extent, contiguous coverage/directory-row span} 聚合，不能默认每个 frame/entry 分配一个长期 Java 对象。
4. locator 容量必须在位置分配前预留。接近限制时只停止该 Binding 的新 admission、移出共享 group并推动 materialization；普通容量压力不能自动 fence Binding 或 rollover 整个 WalRun。
5. 在同一个 owner-local serialized publication cut 中，先安装连续范围所需 locator，再发布 Readable/Durable Frontier，最后 ACK。gap 后方已安装的 locator 在 ReadableFrontier 前不得对读者可见。
6. takeover 必须先完成 Root + checkpoint + LIST 的物理 inventory；active-tail 逻辑视图可以按 Binding 逐步发布，不能要求某个无关 Binding 的 typed gap 阻塞 B 的读视图。新的 append 仍需服从对应 lane 的物理恢复完成条件。
7. manifest 覆盖相同 typed range 且 source protection/read pins 允许后，才回收 locator。

该机制不能配置关闭，因为它属于“ACK 数据必须 durable 且 readable”的正确性合同。可配置的只有：
- checkpoint cadence、recovery concurrency：Protocol Cell × shard；
- active-tail 的 per-binding/tenant soft share；
- shard/Cell/host hard memory ceiling 和 materialization trigger。

Topic 可以选择更保守的软配额，但不能关闭 active-tail readability，也不能放大 hard cap。

最终性能判断：
- Q1：正常 ACK 路径无新增成本，代价集中在故障恢复 GET。
- Q2：热路径成本极低，主要是实现复杂度。
- Q3：是唯一真实性能风险；按上述共享验证、范围聚合和预留机制实现后，ACK 路径应只增加本地索引发布。M3 必须量化 ACK p99 增量、allocation bytes/unit、active-tail bytes/unit、GC 压力以及 takeover GET/bytes/time。
- 核心原则是：冻结正确性行为和资源上限，但不要过早冻结重型 Java 数据结构，更不要把这些正确性机制做成可关闭的 Topic 开关。
````

## Authoritative synchronization

- Q1's physical-only row/Seal payload, single Root binding, closed qualified-proof boundary, and bounded prefix-only
  active-tail recovery are frozen by
  [ADR 0065](../../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md).
- Q2's combined pre-position tracker/locator reservation and purely owner-local 64-bit ticket are frozen by
  [ADR 0066](../../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md).
- Q3's non-disableable active-tail publication order, shared `VerifiedExtent`, range aggregation, implementation
  freedom, per-binding recovery isolation, and pin-safe retirement are frozen by
  [ADR 0067](../../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md).
- Exact wire field IDs/widths/proof caps, numeric tracker/index budgets, reader snapshot mechanics, evidence-selected
  NWG1/NPD1 values, and allocator mode remain open or evidence-blocked.
