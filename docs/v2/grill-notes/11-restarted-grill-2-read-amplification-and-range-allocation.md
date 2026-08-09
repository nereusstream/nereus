---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 9: read amplification and range allocation

Date: 2026-08-09

Round 8 accepted only the adjusted cross-lifecycle configuration rules and allocator evidence protocol in ADRs
0049/0055. NPD1/NPO1 numeric limits and policies, NWG1 cold-read/cap/policy choices, and both allocator modes remain
open. This round asks the complete independent frontier, prioritizing Object cold-read p99/request cost and a parallel
RANGE_LEASED correctness branch. No recommendation below is normative until explicit confirmation.

## Source facts and corrected arithmetic

- NPO1's accepted fixed overhead is exactly 128 bytes: 32-byte header, four 16-byte section headers, and 32-byte final
  digest. Therefore `8,388,608 - 128 - (65,536 x 96) = 2,097,024` bytes remain under the former 96-byte-row proposal.
- The rejected 64-MiB decoded / 68-MiB encoded block proposal combined with 65,536 blocks permits up to 4 TiB decoded
  payload or 4.25 TiB encoded data before the NPD1 header. A block-count cap is not a complete Object/provider bound.
- A 24-byte row is 24% of a 100-byte entry's payload size and 19.35% of the combined row-plus-payload bytes. If entry ID
  is derived from NPO1 `firstEntryId + rowOrdinal`, a 16-byte candidate would reduce those figures to 16% and 13.79%.
- Pinned Pulsar source `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` defaults to a 2,048-MiB maximum ledger and
  50,000 entries per ledger in `ServiceConfiguration`, plus a 64-MiB S3 offload block and 1-MiB offload read buffer in
  `OffloadPoliciesImpl`. The buffer is a native comparison point, not proof that native reads authenticate or decode
  exactly one MiB.
- Current `nereus-object-store/.../ObjectStore.java` supports replayable whole-object PUT but exposes no multipart
  upload or provider object-size/part-count capability contract. V2 NPD1 multipart admission therefore has no
  implementation surface yet.
- With the accepted single authenticated NWG1 directory unit, a cold random frame needs three provider ranges when no
  trusted hint exists: header, exact directory, and frame. A verified exact-prefix hint can reduce this to prefix plus
  frame, but cannot replace the header/directory authority or AEAD check.
- At 4,096 frames, one-KiB frames force seal near 4 MiB, so a 64-MiB soft target cannot be reached. Reaching 64 MiB with
  one-KiB frames needs about 65,536 frame rows; a 64-byte row alone would occupy 4 MiB before contexts/fixed directory
  bytes.
- ADR 0055 accepts the allocator evidence protocol but no mode. STRICT upper-bound arithmetic already motivates
  parallel RANGE_LEASED design; no benchmark receipt exists.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-BK-11` |
| Q2 | `V2-OPEN-BK-13` |
| Q3 | `V2-OPEN-OBJ-17` |
| Q4 | `V2-OPEN-OBJ-17` |
| Q5 | `V2-OPEN-OBJ-19` |
| Q6 | `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **NPD1 length domains, row width, and whole-data-Object envelope**: Can the format freeze exact arithmetic
and a bounded provider envelope now, while leaving lower operational targets evidence-driven?

➡️ Recommend these exact length domains: `decodedBlockBytes = sum(entryPayloadLength)`;
`directoryPlaintextBytes = entryCount x entryRowBytes`; codec input is exactly decoded entry payload bytes;
`aeadPlaintextBytes = directoryPlaintextBytes + codecOutputBytes`; GCM ciphertext length equals AEAD plaintext length;
`encodedBlockBytes = 64-byte NPB1 header + ciphertextBytes + 16-byte GCM tag`; and
`dataObjectBytes = 32-byte NPD1 header + sum(encodedBlockBytes)`. Every addition/multiplication is checked unsigned
64-bit arithmetic before allocation or range use, and the parser allocates from actual validated counts only.

Use a 16-byte entry row `{decodedOffset:uint64, payloadLength:uint32, flags:uint32}` and derive entry ID from the NPO1
block `firstEntryId + rowOrdinal`; retain 24 bytes only if a golden-vector requirement proves an independent entry ID is
needed. Keep 65,536 as an absolute parser count, not a normal allocation target. As a 0.2 candidate, cap one data Object
at 4 GiB and one upload at 1,024 parts; a provider is admitted only when its object-size, part-count, and part-size
capabilities cover the resolved lower deployment limit and the configured native ledger can roll before it. The current
2-GiB native ledger default leaves headroom, while a larger native configuration fails profile admission or rolls
earlier. The tradeoff is a conservative 0.2 Object ceiling and possible earlier ledger rollover, in return for removing
the 4-TiB theoretical object and making multipart capability explicit.

❓ **Q2** - **NPD1 candidates, default ownership, and class pruning evidence**: What should be measured before any
block-target/compression enum exists, and who supplies the eventual default?

➡️ Keep 1/4/8/16 MiB as benchmark candidates only. Compare exact-entry random and sequential ranges, provider
p50/p99/request count/bytes, whole-block AEAD/decode CPU, direct/heap peak, concurrency, compression ratio, 100-byte
small-entry overhead, native maximum entries, default 5-MiB messages, and dedicated near-hard-cap entries against the
pinned native Pulsar baseline including its 1-MiB read buffer. Exercise NONE and eligible ZSTD behavior without adding
RAW as a separate candidate. After evidence, admit at most three typed classes; Namespace/Cell supplies a validated
default, Topic is an explicit override, and both are persisted in the offload attempt. Until then there is no wire enum
or mandatory per-topic selection. The tradeoff is delaying a friendly default, in return for a smaller compatibility
and operations matrix chosen from cold-read evidence rather than names.

❓ **Q3** - **NWG1 cold-read fast path and three-GET fallback**: Which facts make a one-prefix accelerator safe without
turning manifest/checkpoint metadata into directory authority?

➡️ Keep the correctness fallback as three ranges: fixed header, exact directory, then exact frame. Allow a two-range
fast path only when an already validated Object Extent descriptor plus accepted `ProviderObjectProof` (or equivalent
prior full-GET proof) binds Cell/provider scope, exact object key and immutable version, body length/SHA-256, Root
identity, and exact directory end. A manifest may cache that tuple but cannot create the proof. The reader issues
`GET [0,directoryEnd)`, then validates the in-body header and directory AEAD exactly as in fallback before issuing the
frame GET. Missing, stale, oversized, wrong-version, or mismatched hints discard the hint and restart the bounded
fallback; they never authorize an offset or skip AEAD. Cache/prefetch may coalesce either path but is not another
persisted mode. The tradeoff is retaining three GETs on recovery/cache miss without a hint, while normal proven-
descriptor cold reads can use two and the hint remains a discardable accelerator.

❓ **Q4** - **NWG1 directory row and frame-cap derivation**: Should 4,096 remain an independent format limit, or should
frame capacity derive from the authenticated-directory budget and measured small-message distributions?

➡️ Do not freeze 4,096. First freeze the canonical binding-context, append-unit, and frame-row field sets and widths;
then define `maxFramesByDirectory = floor((maxDirectoryPlaintextBytes - fixedBytes - actualContextBytes -
actualAppendUnitBytes) / frameRowBytes)`. The absolute frame parser cap is the minimum of 65,536 and that derived value;
the encoder seals at the first of soft byte target, derived directory capacity, append-unit cap, or Cell/host ceiling.
Benchmark at least 1-KiB Pulsar entries and small Kafka batches with 16,384/65,536 frame candidates and the resulting
prefix bytes. A 64-MiB packing candidate is admitted only if it remains reachable for its target distribution without
making directory-prefix p99/cost unacceptable. The tradeoff is a larger potential directory or a smaller realized
group; a byte target is never promised independently of row-count capacity.

❓ **Q5** - **Separate encoding and packing identities**: Can V2 accept the lifecycle split without prematurely
freezing the 4/16/64-MiB and 5/20/50-ms candidates?

➡️ Recommend two unrelated typed identities. `FrameEncodingPolicyV1` lives in Storage Epoch and defines compression
eligibility, codec family/version, and profitability threshold; every frame row records its actual codec, so compatible
frames need not produce the same result. `WalRunPackingClassV1` lives in WalRun Root and defines soft target bytes plus
linger. Cross-binding batching requires compatible NWG1/encryption context and the same packing class, not identical
per-frame codec outcome. Host pressure may early-seal or backpressure but cannot change either persisted identity.
Keep 4/16/64 MiB and 5/20/50 ms as evidence candidates only; freeze names/numbers after direct-memory, in-flight PUT,
response-loss full-GET, cost, and latency evidence. The tradeoff is two small policy axes, but each changes only at its
real lifecycle and packing does not force a new Storage Epoch.

❓ **Q6** - **RANGE_LEASED ownership and crash convergence**: Should ranges belong to a broker-wide pool or to one
ManagedLedger, and can unused IDs ever be reclaimed?

➡️ Recommend ManagedLedger-scoped, non-reclaimable range grants rather than broker-wide pools or wall-clock leases. A
Cell allocator CAS enters `RANGE_RESERVED`, advances `nextUnleasedId` immediately for the whole bounded interval, and
binds exact binding/incarnation, expected head digest/version, owner epoch, grant ID, and request ID. The exact
ManagedLedger head CAS installs `{rangeStart, rangeEndExclusive, nextLedgerId, grantId, ownerEpoch}`; a final allocator
CAS clears the reservation. Thus three serialized writes occur per range, not per ledger rollover. Normal rollover in
an installed range uses only an immutable node put at deterministic `nextLedgerId` plus exact head CAS advancing that
cursor.

Every uncertain step rereads exact allocator/head/node state. Owner-epoch loss or topic deletion burns the entire
unused tail; no timeout or new owner can reclaim it. If an old owner created the deterministic next node but did not
advance the head, the new owner treats it as an unreferenced burned ID and obtains a later range; it never adopts a node
from the stale owner. This avoids an unbounded Cell-wide pending-node scan and makes each pending candidate discoverable
from the exact head. Node creation is single-flight per ManagedLedger, and any failed head CAS fences that owner from
creating another candidate. Range size remains a bounded, versioned Cell policy/evidence choice, not host
configuration; admission must account for worst-case range-tail burn under ownership churn against the immutable
`2^40` slice. The tradeoff is permanent unused-ID burn and three low-frequency range writes, in return for parallel
per-ledger rollover, bounded crash recovery, and no shared broker-local cursor authority.

## Deferred descendants

- Q1/Q2 must close before NPD1 field IDs, multipart SPI, and golden vectors become implementation work.
- Q3/Q4 must close before NWG1 directory offsets, hint fields, prefix bounds, and cold-read acceptance evidence.
- Q5 numeric policies wait for the same workload receipts; the structural split can settle independently.
- Q6 does not select RANGE_LEASED. Exact range-size classes, head/node wire, rollover/seal, cursor/replication/
  transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 9 recommendation above is normative. Confirmed conclusions must be synchronized to ADRs/contracts; adjusted
values and unselected allocator branches remain in the open log.

## Adjusted response preserved verbatim

The user first summarized the review as:

> 结论：Round 9 继续收敛了，但不能全部确认。Q1/Q2/Q4 基本方向正确；真正仍有过度设计并可能损耗性能的是 Q3、Q5、Q6：
>
> - Q3 把“PUT 持久性证明”误当成日常 range-read 的前置条件，会让 2-GET fast path 很难命中。
> - Q5 把 soft packing policy 固化进唯一 WalRun Root，会与“每 shard 一个 current run”冲突。
> - Q6 在 owner 变化时烧掉整个 range，会在 broker failover 时重新制造全 Cell 串行 allocator 风暴。

The user then supplied this exact disposition:

> Round 9 不全部按推荐确认。Q1/Q2/Q4 基本收敛；Q3、Q5、Q6 仍需调整，其中 Q6 已经上升为 broker failover 的高优先级性能风险。
>
> Q1 — 部分确认。
>
> 确认：
>
> - 冻结完整的 decoded/directory/compressed/AEAD/encoded/Object 长度域。
> - 使用 checked arithmetic，并且只按已验证的实际 count 分配。
> - 优先采用 16-byte、派生 entryId 的 row。
> - 增加完整 data Object 上限和 Provider capability admission。
>
> 调整：
>
> - 4 GiB 可以继续作为 0.2 hard-cap candidate，但必须使用 uint64/long 表达，并明确它只是格式和 admission 上限，不是内存分配、单次 GET 或目标对象大小。
> - 上传、SHA-256 和响应丢失后的完整验证必须支持流式或有界分段处理，禁止构造 4 GiB ByteBuffer。
> - 1,024 multipart parts 不应成为 NPD1 wire identity。它属于 Provider adapter/Cell 的上传操作上限，可以配置得更低；Topic 不能放大。
> - Provider admission 必须同时检查 maxObjectSize、min/maxPartSize、maxParts、streaming upload/read，以及 multipart residue cleanup 能力。
>
> 因此，公式和 16-byte row 可以确认；4 GiB/1,024 parts 的最终数值仍等实现与 Provider evidence。
>
> Q2 — 调整默认值权威后可以确认 benchmark 方案。
>
> 1/4/8/16 MiB 只作为 benchmark candidates、不提前创建 wire enum，这个方向正确。
>
> 但“Namespace/Cell 提供默认”存在双重默认权威，应调整为：
>
> - 产品/Deployment 提供经过验证的基础默认；
> - Namespace 继承或覆盖默认；
> - Topic 只做显式 typed override；
> - Protocol Cell 只做 class admission、上限和资源预算，不重新选择语义默认；
> - host 只做 CPU、内存、I/O、并发 ceiling。
>
> 最终 resolved class 必须记录在 offload attempt 中。Cell/host 可以降低有效 target、backpressure 或拒绝 admission，但不能在 failover 后重新解释已经生成的 NPD1。
>
> Q3 — 不按当前推荐确认。三 GET fallback 正确，但 2-GET fast path 的证明条件过重。
>
> 必须区分两种证明：
>
> - ProviderObjectProof/full GET proof：用于 PUT 成功性、Object durability 和响应丢失恢复。
> - header/directory AEAD：用于日常 range-read 的目录、offset、length 和 frame 身份认证。
>
> ProviderObjectProof 不应成为日常 prefix range planning 的强制前置条件。否则普通成功 PUT 如果没有返回 version-bound FULL_OBJECT SHA-256，读取端只能永久走三 GET；如果为了获得 fast-path proof 而额外 full GET，则优化本身失去意义。
>
> 建议改为：
>
> - 无 hint：header → directory → frame，固定三 GET fallback。
> - 有绑定 immutable leaf identity、Root identity、directoryEnd 的 bounded hint：直接 prefix → frame。
> - hint 必须满足 directoryEnd 不超过 wire hard cap，并绑定 exact Object key/extent sequence；如果存在 provider version，也应绑定 version。
> - prefix 返回后仍必须解析 in-body header，并验证 directory AEAD；hint 永远不能授权 frame offset。
> - hint 过短时复用已经取得的 header，只补读缺失 directory range；hint 过长时使用 header 声明的精确子范围，不应无条件丢弃已读 bytes 并从头重启。
> - key/version/Root/AEAD 真正不匹配时才失败或重新走 bounded fallback。
>
> 需要相应收紧原有“读取 frame boundary 前必须验证完整 ObjectExtentDigest”的表述：完整 digest 是 Object durability/recovery proof；正常随机读取由 Root-bound directory/frame AEAD 提供局部认证，否则 range read 的设计目标无法成立。
>
> Q4 — 接受“由 directory budget 推导 frame cap”，但不建议把 65,536 作为当前同等级候选。
>
> 真正需要首先冻结的是 maxDirectoryPrefixBytes，而不是 maxFrames。
>
> 例如假设 frame row 为 64 bytes：
>
> - 4,096 rows 约 256 KiB；
> - 16,384 rows 约 1 MiB；
> - 65,536 rows 约 4 MiB。
>
> 首次读取一个 1 KiB frame 时，1 MiB/4 MiB prefix 分别产生约 1,024x/4,096x 的控制字节放大。即使请求数只有两个，冷读 p99 和流量成本仍可能很差。
>
> 建议 0.2 优先 benchmark 4,096 和 16,384；65,536 只有在 prefix 证据证明可接受时才继续考虑。不要为了支持 65,536 在 0.2 引入分页 directory 或第二套索引权威，那会再次过度设计。
>
> 64 MiB 继续只是 soft target。小 frame 因 directory cap 提前 seal 是正常结果，不能承诺所有消息分布都能达到 64 MiB。
>
> Q5 — 确认 encoding 与 packing 是不同策略，但不确认把 WalRunPackingClass 固化为 WalRun Root identity。
>
> 当前每个 shard 只有一个 CurrentWalRunPointer 和一个 current WalRun。如果 Root 固定一个 packing class，而同一 shard 上存在不同 Topic packing class，就只能：
>
> - 拒绝这些 Topic 共存；
> - 为不同 class 增加多套 current run/pointer；
> - 或频繁 seal/rollover 切换 class。
>
> 三种结果都会增加复杂度或直接损耗性能。
>
> soft target 和 linger 不影响历史 Object 的解码、正确性或恢复权威，因此建议：
>
> - Storage Epoch 的 FrameEncodingPolicyV1 保留，用于 codec family/version、eligibility 和有限 typed policy；0.2 中它在 Topic 创建时确定，在线修改仍不支持。
> - WalRun Root 只保存 format、加密、hard recovery envelope 和最大 Object 上限，不保存单一 soft packing class。
> - Topic/Namespace 选择 packing class；Cell/host 提供 ceiling。
> - batcher 在同一个 WalRun 内按 resolved packing class 使用最多三个有界 scheduling lanes。
> - 一个 Object group 只合并相同 packing class 的 binding，但同一 WalRun 可以连续产生不同 class 的 Object groups。
> - resolved class 和实际 linger/size 可以记录在 NWG1 extent header/descriptor 中用于审计和 benchmark，但不是新的恢复权威。
> - packing 变化从下一个 group 生效，不需要 seal 整个 WalRun。
>
> 这样保留 Topic 级成本/延迟配置，同时避免多个 pointer、多个 run lineage 和配置切换导致的控制面 rollover。
>
> Q6 — 不按当前推荐确认。ManagedLedger-scoped range 正确，但 owner epoch 丢失时烧掉整个未用 tail 过于保守，而且会造成严重 failover 放大。
>
> Range 应归属于 ManagedLedger incarnation，而不是归属于某个 broker owner epoch。Owner epoch 只负责控制谁可以推进 head。
>
> 如果 owner 变化就烧掉整个 range，那么一次 broker failover 会令其承载的所有 ManagedLedger 重新申请 range。按单一 RANGE_RESERVED、每 range 三次串行写计算，10,000 个 ManagedLedger、metadata 单步 10 ms 时，理想下界已经约为：
>
> 10,000 × 3 × 10 ms = 300 秒
>
> 这会重新制造需要 RANGE_LEASED 消除的全 Cell 串行恢复风暴。
>
> 建议改为：
>
> 1. allocator grant 永久绑定 ManagedLedger incarnation、grantId 和 range，不绑定 owner 生命周期。
> 2. takeover 先对 ManagedLedger head 做精确 CAS，只更新 ownerEpoch，同时保留 rangeStart、rangeEnd 和 nextLedgerId。
> 3. 旧 owner 仍然只能存在一个 candidate in-flight。
> 4. 新 owner 对 exact nextLedgerId 做 point lookup/putIfAbsent：
>    - 如果旧 head CAS 实际已经成功，按新 head 状态继续；
>    - 如果发现旧 owner 创建但未发布的 stale node，不采纳它，只 burn 这一个 ledger ID，并通过 head CAS 推进 nextLedgerId；
>    - 如果不存在冲突，新 owner继续使用同一个 range。
> 5. 只有 Topic 删除、ManagedLedger retirement、range 协议不兼容或不可恢复损坏时，才烧掉整个未用 tail。
> 6. orphan candidate 可以永久保留为不可复用证据，但必须计入 metadata bytes/count admission。
> 7. allocator clear 不应位于当前 ManagedLedger 开始使用已安装 range 的关键路径；head install 完成后即可使用，clear 由恢复/后台流程完成，但下一次 grant 前必须收敛。
> 8. “最坏 ownership churn”不能使用无界 lifetime 概念，否则任何非零 range 都无法证明。应改成预先声明的有限 churn/rate/horizon budget，并持续计算 remaining slice、committed IDs、burned IDs、orphan nodes 和 recovery reserve。
>
> Range size 仍属于 versioned Cell policy，Topic/host 都不能放大。此调整只完善 RANGE_LEASED 正确性，不自动选择该 allocator mode。
>
> 本轮结论：
>
> - Q1：确认公式与 16-byte row；Object/part 数值继续候选。
> - Q2：确认 benchmark 方案，默认权威改为 Deployment/Namespace，Cell 只做 ceiling。
> - Q3：保持 OPEN，解除 routine range read 对 ProviderObjectProof 的错误依赖。
> - Q4：确认推导方式，优先 4,096/16,384，暂不接受 65,536。
> - Q5：保持 OPEN；packing 应是 per-group scheduling policy，不是单一 WalRun Root identity。
> - Q6：保持 OPEN；owner takeover 复用 ManagedLedger range，最多 burn exact stale candidate，不烧整个 tail。
> - STRICT_SERIALIZED 与 RANGE_LEASED 仍均未被选择。

The user closed with this priority statement and source pointers:

> 这轮最该挡住的是 Q5/Q6：Q3 影响日常冷读，而 Q5/Q6 会在多 Topic 和 broker failover 时形成架构级放大。[当前 WalRun 合同](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md:35)明确每 shard 只有一个 current pointer，因此 packing class 不能未经 lane/group 设计就成为 Root 的单一身份；[allocator 证据合同](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md:13)也正是为了避免重新引入 Cell-wide stall。

## Authoritative synchronization

- Q1's checked domains, derived-ID row, streaming processing, and capability categories are accepted by
  [ADR 0056](../../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md); exact Object/part values remain
  open;
- Q2's evidence candidates and one-way Deployment/Namespace/Topic default authority are accepted by
  [ADR 0057](../../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md); no class/default value exists yet;
- Q4's prefix-first capacity derivation, 4,096/16,384 evidence priority, and no-pagination constraint are accepted by
  [ADR 0058](../../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md); exact values remain open;
- Q3 / `V2-OPEN-OBJ-17`, Q5 / `V2-OPEN-OBJ-19`, Q6 / `V2-OPEN-PUL-OBJ-09`, and both allocator modes remain open;
- ADRs 0021/0025/0040/0046 now separate whole-Object durability proof from routine range authentication, while ADR
  0039 preserves one current Root/pointer and rejects a singular Topic packing identity in that Root.
