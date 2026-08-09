---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 8: hard caps, policy classes, and allocator evidence

Date: 2026-08-09

Round 7 was partially accepted with adjustments. ADRs 0049 through 0054 own only the cross-cutting scope contract and
adjusted Q1, Q2, Q4, Q6, and Q7. Round-7 Q3, Q5, and Q8 remain open. This record contains their next independent
decision frontier; none of the recommendations below is normative until explicit confirmation.

## Source facts and arithmetic

- Pulsar research checkout `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` defaults to 5 MiB
  `maxMessageSize`, 50,000 entries per ledger, 10-minute minimum and 240-minute maximum ledger rollover, and 2,048 MiB
  maximum ledger size. The message limit is configurable as an `int`; a NPD1 deployment cap must therefore reject an
  incompatible profile rather than silently truncate native support.
- Accepted NPO1 bounds are an 8,388,608-byte root, 32-byte root header, four 16-byte section headers, one final 32-byte
  digest, and at most 65,536 sparse rows. A proposed 96-byte fixed row consumes 6,291,456 bytes and leaves 2,097,024
  bytes for all other section payload and fixed overhead. A 24-byte NPB1 entry row consumes 1,572,864 bytes at 65,536
  entries, below a proposed 2-MiB directory cap.
- No V2 NWG1 encoder/parser exists yet. ADRs 0040/0046 fix in-body authenticated directory and crypto semantics but
  deliberately leave context/frame/directory hard caps and initial range assembly open.
- The current strict virtual-ledger proposal requires four successful writes: allocator reserve CAS, immutable node
  put, ManagedLedger head CAS, and allocator clear CAS. Oxia has no qualifying cross-key conditional transaction, so a
  Cell-wide serialized service capacity is bounded approximately by the sum of those write latencies plus recovery.
- With stock Pulsar time rollover alone, 100,000 simultaneously active ledgers imply about 6.9 rollovers/second at four
  hours and 166.7/second at ten minutes before entry/byte-triggered rollovers. These are sizing inputs, not measured V2
  throughput. No allocator benchmark receipt exists.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-BK-11` |
| Q2 | `V2-OPEN-BK-13` |
| Q3 | `V2-OPEN-OBJ-17` |
| Q4 | `V2-OPEN-OBJ-19` |
| Q5 | `V2-OPEN-PUL-OBJ-10` prerequisite for `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **NPD1/NPO1 fixed rows and hard caps**: Which exact fixed row sizes and parser/allocation maxima are format
identity, and which lower deployment cap may reject an incompatible Pulsar native entry limit?

➡️ Recommend retaining the big-endian 32-byte NPD1 header, 64-byte NPB1 block header, and 24-byte entry row, and freezing
one 96-byte NPO1 sparse row. Keep `maxBlocks=65,536` because the explicit 8-MiB arithmetic above leaves 2,097,024 bytes
outside the maximum sparse table; the complete root must still satisfy every existing section and total cap. Freeze
format maxima `maxEntriesPerBlock=65,536`, `maxBlockDirectoryBytes=2 MiB`,
`maxBlockDecodedBytes=64 MiB` of entry payload, and `maxBlockEncodedBytes=68 MiB` for the complete stored block. A
deployment may advertise equal or smaller maxima but a Topic/Tenant cannot raise them. Profile admission proves the
configured native maximum entry plus one row, header, and AEAD envelope fits both deployment maxima. It also proves the
configured ledger byte/entry rollover limits and selected target cannot require more than 65,536 blocks or overflow the
8-MiB root; otherwise `BOOKKEEPER_WAL_ASYNC_OBJECT` is rejected before writing. The retained NONE/ZSTD and
attempt-key/HKDF/AEAD rules remain format contracts rather than topic switches. The tradeoff is an explicit 64-MiB
native-entry ceiling for this profile and 96-byte index overhead per block, in return for bounded allocation and a
root-size proof rather than the vague signed-int limit.

❓ **Q2** - **NPD1 block policy classes and default evidence**: Which finite Topic/Tenant choices replace a free-form
block target/compression flag product, and how is the default chosen without treating 8 MiB as doctrine?

➡️ Recommend candidate attempt-persisted classes `READ_1M`, `BALANCED_4M`, `COST_8M`, and `SCAN_16M`, each using
`ZSTD_FAST_IF_SMALLER`; protocol-marked compressed or opaque-encrypted input and an unprofitable result emit codec NONE.
Add `RAW_4M` as the sole explicit CPU-first NONE class. Read buffer, prefetch, cache, decompression concurrency, and
memory remain Cell/host capacity. Keep the default `NOT_PINNED` until a receipt compares all candidates over random and
sequential cold reads, provider p50/p99, range bytes, GET count/cost, decompression CPU, memory, compression ratio, the
stock 5-MiB native entry, and near-64-MiB dedicated entries. Until a default is pinned, admission requires an explicit
class rather than silently choosing 8 MiB; `NOT_PINNED` is gate notation, not a durable wire value. The tradeoff is five
supported classes and an evidence gate, in return for auditable batching and no machine-local default drift.

❓ **Q3** - **NWG1 hard caps and first cold random read**: What fixed bounds make the authenticated directory safe, and
should a cold reader fetch an always-maximal prefix or first learn its exact length?

➡️ Recommend format maxima `maxBindingContexts=256`, `maxFrames=4,096`,
`maxDirectoryBytes=1,048,576` including directory ciphertext/tag, and
`maxHeaderAndDirectoryPrefixBytes=1,048,832` including the accepted 256-byte header. A topic cannot raise them; lower
Cell/host allocation caps may fail admission but cannot reinterpret an object. The correctness path performs GET
`0..255`, validates header CRC/algorithms/all four bounds before KMS, then performs one exact directory GET and caches
the authenticated result before any frame range. Cache/prefetch may satisfy or coalesce those reads but is not a new
wire mode. The tradeoff is two cold control ranges instead of a promised single GET, in return for avoiding a 1-MiB
overfetch on small groups and never trusting an unbounded prefix.

❓ **Q4** - **NWG1 typed compression/linger/group policy**: Which limited classes preserve cross-topic batching and
avoid expensive recompression without creating independent flags for every dimension?

➡️ Recommend three combined initial-Storage-Epoch classes: `OBJECT_LATENCY` (4-MiB target, 5-ms linger),
`OBJECT_BALANCED` (16 MiB, 20 ms), and `OBJECT_COST` (64 MiB, 50 ms). Each uses deterministic
`ZSTD_FAST_IF_SAVES_12_5_PERCENT` only for protocol-declared eligible clear payloads; already-compressed or
opaque-encrypted frames are pass-through and never trial-compressed. The resolved compression rule is persisted in the
Storage Epoch and the group target/linger class in the WalRun Root; changes start with a new allowed epoch/next WalRun.
Cross-binding grouping requires the same resolved class. AES/CRC/HKDF concurrency, run-key cache, direct memory, and I/O
are Cell/host ceilings resolved by ADR 0049. The tradeoff is only three exposed combinations and fixed initial numbers,
in return for batching compatibility and predictable CPU; benchmark evidence may change a future policy version, not
reinterpret v1 bytes.

❓ **Q5** - **STRICT_SERIALIZED allocator evidence protocol**: What evidence must exist before 0.2 may select the strict
four-write protocol, and what result forces the RANGE_LEASED branch to be designed instead?

➡️ Recommend accepting an evidence protocol, not an allocator mode yet. Benchmark 10,000 and 100,000 active
ManagedLedgers per Protocol Cell; entry-, byte-, age-, and synchronized rollover storms; injected 1/5/10/25-ms metadata
p99; multiple brokers; and crash/response loss at each of the four writes plus takeover of RESERVED. Measure sustained
rollovers/second, all four operation latencies, queue depth/age, per-topic starvation, Cell-wide append stalls, metadata
load, and RESERVED recovery time. Admission computes a conservative required rollover rate from every topic and permits
STRICT only when it is at most 50% of measured serialized p99 service capacity, steady p99 queue delay is at most
250 ms, synchronized-storm p99 is at most 2 seconds, RESERVED recovery p99 is at most 5 seconds, and configured queue
bounds never overflow or starve a topic. Persist those admitted rate/queue/recovery budgets with
`allocatorMode=STRICT_SERIALIZED` in the Cell allocator record. Any target-scale failure keeps
`V2-OPEN-PUL-OBJ-09` open and requires a complete RANGE_LEASED owner-epoch/burn/crash/pending-head contract before
implementation. The tradeoff is an intentionally demanding benchmark and possible rejection of the simpler protocol,
in return for not shipping a hidden Cell-wide head-of-line bottleneck as a feature flag.

## Deferred descendants

- Exact byte offsets and golden vectors follow Q1/Q3. NPD1 and NWG1 defaults cannot be selected before Q2/Q4 evidence.
- `V2-OPEN-PUL-OBJ-09` allocator mode remains downstream of Q5 evidence; RANGE_LEASED details are not assumed.
- WalRun retirement/GC and virtual-ledger RETIRING proof remain later lifecycle branches.
- KoP remains documented and deferred outside the 0.2 runtime.

## Adjusted response preserved verbatim

The user first summarized the review as:

> 结论：Round 8 有局部过度设计，不建议“全部按推荐确认”。性能风险最大的是 Q3 的冷读 GET 放大，以及 Q5 的全 Cell 串行 allocator；Q2/Q4 主要是配置组合和持久化边界过度复杂。Q1 的硬上限方向正确，但具体数值还缺少数据对象总大小和小 entry 开销证明。

The user then supplied this exact disposition:

> Round 8 不全部按推荐确认，请按以下调整继续收敛。
>
> 总体配置原则：
>
> 1. 格式正确性、安全边界和解析 hard cap 是 wire contract，不允许配置关闭。
> 2. Topic/Namespace 只能选择少量 typed policy，不能放大格式上限。
> 3. Cell/host 只负责并发、direct memory、I/O、KMS、CPU 等运行时 ceiling；资源不足时允许 backpressure 或提前 seal，不能静默改变已持久化语义。
> 4. 性能优化可以配置，但必须与其生效边界一致，不能用一个枚举跨越 Storage Epoch、WalRun 和 host 三种生命周期。
>
> Q1 — 保持 OPEN，接受固定 wire 和 hard-cap 原则，但暂不冻结全部数值。
>
> 需要补充：
>
> - 明确定义 decoded block、encoded block、header/directory、ciphertext/tag 的长度域公式。
> - 增加 maxDataObjectBytes、multipart part 数量以及 Provider object-size capability admission。只限制 maxBlocks 会允许理论上约 4 TiB 的单个 data object，边界并不完整。
> - 评估 entry row 是否必须显式保存 entryId。entryId 已可由 block firstEntryId + ordinal 推导；24-byte row 对 100-byte 小 entry 会产生约 24% 的目录开销。冻结前应比较 16-byte 与 24-byte row。
> - 65,536 可以保留为 parser 的绝对格式上限，但正常 Profile 必须使用更低的派生 admission bound，解析器也只能按实际 count 分配。
> - NPO1 剩余 2,097,024 bytes 的计算应明确已经扣除 128 bytes 固定开销。
>
> Q2 — 保持 OPEN，不先把五个 class 固化成正式合同。
>
> 五个 class 的直接热路径损耗不大，但会扩大 benchmark、兼容性和运维矩阵。RAW_4M 与“已压缩、opaque-encrypted 或压缩无收益时写 NONE”存在重叠；COST_8M 与 SCAN_16M 是否都必要也尚无证据。
>
> 建议先把 1/4/8/16 MiB 作为 benchmark candidates，而不是 wire enums。证据完成后压缩为最多三个常用 class；Namespace/Cell 提供经过验证的默认值，Topic 只作为显式 override，不要求每个 Topic 永久手工配置。
>
> 尤其要验证随机读取放大：NPD1 必须读取并认证完整 block，16 MiB block 的随机读、解压 CPU 和内存占用可能明显高于当前约 1 MiB 的 offload read buffer。
>
> Q3 — 保持 OPEN，这是本轮最明确的读取性能风险。
>
> 当前首次冷随机 frame 读取实际需要三个 GET：
>
> 1. header GET；
> 2. directory GET；
> 3. frame GET。
>
> 不能只描述为两个控制 GET。
>
> 建议保留两阶段 header → directory 作为无可信提示时的恢复路径；如果已经验证的 metadata/manifest 提供 exact directory end 和 expected digest，则允许一次精确 prefix GET 同时取得 header + directory，验证后再读取 frame。该提示只能用于加速，Object 本体和 directory AEAD 仍是权威。
>
> 另外，maxFrames=4,096 与 64 MiB OBJECT_COST target 存在明显冲突：平均 frame 小于约 16 KiB 时，会先触发 frame cap。对于大量 1 KiB entry，group 大约 4 MiB 就会 seal，64 MiB cost class 实际无法生效。需要先冻结 directory row 大小，并用小消息分布验证或重新推导 maxFrames。
>
> Q4 — 接受 typed policy 方向，但必须拆成两个持久化维度，当前组合 class 不确认。
>
> 建议拆为：
>
> - FrameEncodingPolicy：属于 Storage Epoch，定义压缩 eligibility、算法和阈值。
> - WalRunPackingClass：属于 WalRun Root，定义 target bytes 和 linger。
>
> 原因是两者的变更边界不同。把它们合并成一个 class，会让单纯修改 batching/linger 也表现为 Storage Epoch 语义变化，并增加跨 binding batching 的碎片化。
>
> 跨 binding batching 只要求 packing class、格式和加密上下文兼容；compression 可以按 frame 记录 codec，不必强制所有 frame 使用相同结果。
>
> 4/16/64 MiB 和 5/20/50 ms 暂时保留为 benchmark candidates。target 必须是 soft target；Cell/host 可以因资源 ceiling 提前 seal 或 backpressure。64 MiB 还需要验证 direct memory、in-flight PUT、响应丢失后的 full GET 校验成本。
>
> Q5 — 调整后确认“证据协议”，不确认 STRICT_SERIALIZED mode。
>
> 这里不是运行时过度设计，测试本身有必要；但当前数据已经表明 STRICT 很可能成为 Cell-wide 性能瓶颈：
>
> - 100,000 个 ManagedLedger、10 分钟 rollover，平均需求约 166.7 rollover/s。
> - 每次 rollover 如果串行经过四个 metadata 写步骤，即使每步只有 1 ms，理论容量也约为 250/s；按 50% admission gate 只允许约 125/s，已经低于需求。
> - metadata p99 为 5/10/25 ms 时差距会更大。
>
> 因此不要等全部测试完成后才开始定义 RANGE_LEASED；应并行补齐其正确性合同，但仍不提前选定 mode。
>
> 同时调整证据定义：
>
> - 用“满足所有 latency/queue/error SLO 时的最大可持续 rollover RPS”替代含义不清的 serialized p99 capacity。
> - 增加 native Pulsar rollover/append-stall 基线，不能只满足 250 ms、2 s 等绝对门槛。
> - 测试必须覆盖实际 rollover-rate 分布、抖动与 synchronized storm，而不只是 active-ledger 数量。
> - allocator record 只持久化 allocator mode、协议版本及恢复身份；rate、queue、latency 和 recovery budgets 应进入可版本化的 Cell policy/evidence，不应冻结为 allocator 的永久身份。
> - host 资源限制仍只作为运行时 ceiling。
>
> 本轮结论：
>
> - Q1：调整后再确认。
> - Q2：继续收集证据并裁剪 class。
> - Q3：必须解决冷读 GET 放大和 frame-cap/64 MiB 冲突。
> - Q4：拆分 encoding policy 与 packing class 后再确认。
> - Q5：确认调整后的证据协议，但 STRICT_SERIALIZED 与 RANGE_LEASED 均继续保持 OPEN。
>
> 请不要把 Q1–Q4 或 allocator mode 写成最终合同。

The user closed with this priority statement:

> 最值得优先处理的是 Q3 和 Q5：前者会直接影响冷读 p99 与 Object 请求成本；后者在 100,000 ledger 场景下已经能通过上界计算看到串行瓶颈。配置化可以降低部分风险，但不能用开关掩盖格式边界或 allocator 正确性问题。

## Authoritative synchronization

- the adjusted cross-lifecycle configuration principles refine
  [ADR 0049](../../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md);
- adjusted Q5 evidence protocol is accepted by
  [ADR 0055](../../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md);
- Q1 / `V2-OPEN-BK-11`, Q2 / `V2-OPEN-BK-13`, Q3 / `V2-OPEN-OBJ-17`, Q4 / `V2-OPEN-OBJ-19`, and allocator-mode
  `V2-OPEN-PUL-OBJ-09` remain open;
- none of the proposed block/object/frame/directory limits, class names, combined policy values, absolute allocator
  thresholds, `STRICT_SERIALIZED`, or `RANGE_LEASED` is normative.
