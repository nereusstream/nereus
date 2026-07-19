# BookKeeper Primary WAL Delivery Detailed Design

> 状态：BK-M0 design gate 与 BK-M1 provider-neutral foundation 已于 2026-07-19 complete/final-gated；
> `bookKeeperPrimaryWalM1FinalCheck` 通过 199-task aggregate（包含 Phase 1.5、Phase 4 与 pinned local Pulsar
> regressions）。BK-M2 `BOOKKEEPER_WAL_ONLY` implementation is in progress：strict keyspace、7 类 V1 durable
> records/codecs、factory dispatch 与 focused golden/negative tests 已实现；store、writer、reader、ledger
> lifecycle、retention 尚未完成，因此三个 BookKeeper profile 继续在任何 BookKeeper IO 之前 fail closed。

## 1. Delivery identity

本交付属于 Future 1 的后续扩展，正式简称为 **F1-BK / BookKeeper Primary WAL Delivery**。它不是
Future 5：Future 1–8 是稳定能力轨道编号，其中 F5 已固定为 KoP/Kafka。实施里程碑使用
`BK-M0` 到 `BK-M6`，不会重排 Future 编号。

依赖顺序是：

```text
Phase 1 + Phase 1.5 stable L0 contracts
    -> Future 2 ManagedLedger projection
    -> Future 3 cursor/subscription
    -> Future 4 generation/materialization/retention/GC
    -> F1-BK BookKeeper primary-WAL profiles
```

F1-BK 复用以上已 final-gated 的逻辑事实，不建立第二套 offset、MessageId、commit、cursor、generation、
materialization task、lag 或 logical-retention authority。

## 2. Frozen profile order

```text
BK-M0  contract and local-source audit
BK-M1  provider-neutral seam + nereus-bookkeeper adapter foundation
BK-M2  BOOKKEEPER_WAL_ONLY
BK-M3  BOOKKEEPER_WAL_ASYNC_OBJECT
BK-M4  BOOKKEEPER_WAL_SYNC_OBJECT
BK-M5  Pulsar rollout / ownership transfer / operations
BK-M6  compatibility / scale / chaos / aggregate final gate
```

第一版不实现在线 profile migration。profile 只允许在 topic/stream first-create 时选择，随后作为 durable
stream metadata 保持不可变。修改 namespace/topic policy 不得原地改变已有 stream 的 primary-WAL identity。

## 3. Normative decisions

以下结论是本设计的硬合同；后续实现不能把它们降级成“推荐”：

1. **BookKeeper 只承载 primary bytes。** Oxia stream head 仍是逻辑可见性线性化点；BookKeeper
   `addEntry`/`writeAsync` 成功本身不允许 producer ack。
2. **一个 Nereus offset 对应一个完整 BookKeeper entry。** V1 使用已保留的
   `BookKeeperEntryMapping.ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY`，entry 内容是完整、未改写的 Pulsar Entry
   bytes。一个 append batch 必须占一个连续 entry range，且不得跨 ledger。
3. **物理 ledger id 不是 Pulsar MessageId ledger id。** F2/F3 的 virtual ledger、entry 和 batch index 只由
   stable logical offset/projection 推导；broker restart、seek、历史读取或 generation 切换不得暴露 BK ledger
   rollover。
4. **`BookKeeperEntryRangeReadTarget` 是唯一 durable BK location。** 不新增一份可与它漂移的 durable
   `BookKeeperWalLocation`；进程内 handle/cache key 不是 correctness metadata。
5. **V1 每个 stream/session epoch 至多一个 active writable ledger，且不跨 stream 共享。** rollover 先于不能
   整体容纳的 append batch；uncertain/partial write 会 taint 并 seal/fence 当前 ledger。
6. **ledger allocation 先有 Oxia intent，再创建 reserved-namespace exact ledger id。** V1 在显式、durable
   binding 的 exact provider scope + 正 63-bit ledger-id prefix 内随机选择 candidate，使用 BookKeeper 4.18.0 公共
   `CreateAdvBuilder.withLedgerId(long)` 与 `WriteAdvHandle`。所有 broker/deleter 必须证明该 advanced exact-id
   namespace 只由本 Nereus deployment 创建；否则 profile admission 与 physical delete 都 fail closed。create
   response loss 通过 exact id metadata reload 收敛，不依赖 ledger listing；已发送但 outcome uncertain 的 create
   持有固定 durable allocation slot，并把 `lateCreateHazard` 永久写入 root；即使 matching ledger 后来出现，
   BK-M0–M6 也不自动删除它。它永远不能仅凭 elapsed grace/两次 `NoSuchLedger` 变成可遗忘的 `ABORTED`。
   slot exhaustion 在下一次 provider IO 前拒绝，碰撞绝不删除未知 ledger。
7. **读取使用 non-recovery open。** 普通/历史读以 `withRecovery(false)` 打开并按 committed exact range
   `readUnconfirmedAsync`；只有 ownership recovery/fencing 可以 recovery-open。
8. **V1 禁止 `WriteFlag.DEFERRED_SYNC`。** head CAS 前必须已经满足配置的 BookKeeper ack-quorum durability；
   不能靠进程内 `force()` 的隐含时序解释成功。
9. **retention 只删除完整 ledger。** 每个 append range 使用固定 protection slots，每个 ledger 使用固定 reader
   slots，避免 scan-then-count 竞态；writer state 以 exact physical bytes 与 append-range count 强制 rollover
   上限。只有 sealed
   ledger 内每个 range 都已可回收，并且不存在 reader lease、writer、task、repair、append-recovery 或 cursor/
   logical-retention reference 时才可删除。
10. **async/sync 复用 F4。** generation zero 始终是 BK target；higher generation 才是 Object target。BK
    async/sync 不创建第二套 worker、task、checkpoint、lag 或 GC 协议。
11. **sync 需要独立 completion policy。** `WAL_DURABLE_AND_INDEX_COMMITTED` 只证明 generation-zero BK index，
    不能证明 Object generation。sync producer ack 必须等待 `REQUIRED_OBJECT_GENERATION` 的 exact COMMITTED
    reload/read-admission proof。
12. **head 成功后失败是 committed。** object publication timeout、response loss 或 broker crash 不能回滚 offset
    或重复写 BK bytes；恢复必须复用同一 committed target、stable append result 和 deterministic F4 task。
13. **借用 Pulsar BookKeeper client。** `NereusManagedLedgerStorage` 从 stock
    `BookkeeperManagedLedgerStorageClass` 取得 client 并以 borrowed resource 传给 Nereus runtime；Nereus 不关闭
    client。L0/core 与 managed-ledger API 不依赖 `org.apache.bookkeeper.mledger.*`。

## 4. Document map

| Document | Frozen boundary |
| --- | --- |
| [01-current-contract-and-source-audit.md](01-current-contract-and-source-audit.md) | current Nereus/Pulsar/BK source locks、call paths、blocking gaps |
| [02-domain-api-module-and-target-contract.md](02-domain-api-module-and-target-contract.md) | module DAG、provider-neutral SPI/read result、BK API、target/offset mapping |
| [03-oxia-metadata-ledger-lifecycle-and-codecs.md](03-oxia-metadata-ledger-lifecycle-and-codecs.md) | exact keys/records/codecs/CAS、allocation/segment/protection/delete state |
| [04-append-read-recovery-and-fencing.md](04-append-read-recovery-and-fencing.md) | append/read state machines、rollover、fencing、response-loss recovery |
| [05-retention-materialization-and-completion.md](05-retention-materialization-and-completion.md) | BK_ONLY GC、F4 source adapter、async/sync completion、lag and source release |
| [06-pulsar-runtime-rollout-and-compatibility.md](06-pulsar-runtime-rollout-and-compatibility.md) | broker wiring、first-create admission、capability、ownership transfer、operations |
| [07-implementation-plan-and-gates.md](07-implementation-plan-and-gates.md) | BK-M0–M6 file-level implementation order、mandatory review stops、Gradle gates |
| [08-scenario-evidence-matrix.md](08-scenario-evidence-matrix.md) | deterministic/real-service/two-broker/scale/chaos traceability |

Documents `03`–`08` are part of the same required design. All nine documents and their global links passed
`./gradlew bookKeeperPrimaryWalDocumentationCheck --console=plain` on 2026-07-19。BK-M1 additionally passed
`bookKeeperPrimaryWalM1Check` and the 199-task `bookKeeperPrimaryWalM1FinalCheck` aggregate against the exact source
locks；documentation evidence alone still does not advance BK-M2。

## 5. Correctness authorities

```text
BookKeeper ledger metadata          physical quorum/closed/LAC facts only
BookKeeperLedgerRootRecord          Nereus ownership + lifecycle of one exact ledger
BookKeeper*ProtectionRecord         durable range inventory/reference fence
StreamCommitRecord + stream head    logical append visibility and stable result
generation index                    selected physical representation for one logical range
F4 MaterializationTask/checkpoint   workflow recovery only
F2/F3 projection/cursor metadata    MessageId and consumer progress
```

Projection、task、cache、ledger handle 与 metrics 都只能解释以上 durable facts，不得成为新的 correctness owner。

## 6. Success predicates

| Profile | Producer success predicate | Read before higher generation | BK source release |
| --- | --- | --- | --- |
| `BOOKKEEPER_WAL_ONLY` | BK durable + recoverable intent/target + reachable head commit；strict callers may also require gen0 index | exact BK range | only durable logical trim + whole-ledger proof |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | same stable-head boundary, after pre-IO profile/lag admission | exact BK range; later highest admitted Object generation | trim or healthy exact Object replacement, then whole-ledger proof |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | stable head + gen0 + exact required Object generation COMMITTED and resolvable/read-admitted | consumers may see committed BK bytes while producer is awaiting Object completion | same as async |

“sync” controls producer completion, not logical visibility. Moving visibility after Object publication would create a
second commit protocol and is forbidden.

## 7. Explicit non-goals

- no stock ManagedLedger ledger reuse or stock ManagedLedger metadata mutation；
- no BookKeeper ledger id exposure through Pulsar `MessageIdAdv`；
- no per-entry BookKeeper delete；
- no shared multi-stream writable ledger in V1；
- no `DEFERRED_SYNC` profile in V1；
- no online Object-WAL/BK-WAL migration in BK-M0–M6；
- no producer exactly-once/dedup promise beyond the current durable append recovery contract；
- no BookKeeper-specific materialization scheduler、lag store or cursor truth；
- no physical delete enabled by configuration alone。

## 8. Source policy

The design is based only on this repository and the local Pulsar checkout at
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar`。No internet or non-existent `M1-SNAPSHOT` artifact is an input.
The target Pulsar source lock is `master@eaf7b9a704890a9265c21f30d9f351e02d00c600`。The Nereus pre-design audit
lock and BookKeeper client API surface are recorded in document 01；a changed lock requires re-audit, not silent
compilation against a different checkout.

## 9. Current implementation evidence

BK-M1 implements and final-gates：

- `nereus-bookkeeper` compiled against BookKeeper `4.18.0` and exported from `nereus-bom`；
- generic `ReadSourceRef`、`PhysicalReadStats`、`PhysicalReadResult` and canonical `ReadTargetIdentities`；
- generic `PrimaryPhysicalIdentity`、opaque `ProviderAppendToken` and independent append completion values；
- configuration/GC safe defaults、positive-63-bit namespace generator、read-only provisioned reservation verifier；
- NBKR1、prepared-entry ownership、public-client operations、deadline/error mapping and bounded handle cache；
- provider-neutral `PrimaryWalRegistry` selection、prepared/durable append flow and physical-read accounting；
- generic `PhysicalReferenceProof`、`PrimaryPhysicalReferenceAdapterRegistry` and stable commit/protection/gen0
  publication without Object casts；
- Object-WAL compatibility bridges that preserve authorization、manifest publication、stable results、read bytes and
  accounting while rejecting noncanonical provider results；
- synthetic tagged BK commit/protection/gen0 evidence plus executable module-boundary、focused and predecessor gates。

BK-M1 does not register `BookKeeperPrimaryWalAppender`/reader/lifecycle runtime. Production metadata admits only the
fully validated Object reference proof until BK-M2 installs the exact BookKeeper proof adapter and lifecycle stores；
unknown proof types fail closed。

The first BK-M2 checkpoint implements `BookKeeperKeyspace` with strict root/protection/reader/allocation-slot inverse、
all seven V1 record models and explicit enum wire ids、seven envelope codecs registered in
`MetadataRecordCodecFactory`、frozen envelope SHA-256 vectors and malformed/truncated/trailing/checksum tests。This is
metadata groundwork only；it does not make `BOOKKEEPER_WAL_ONLY` executable。
