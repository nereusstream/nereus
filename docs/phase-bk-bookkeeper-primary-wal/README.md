# BookKeeper Primary WAL Delivery Detailed Design

> 状态：BK-M0 design gate 与 BK-M1 provider-neutral foundation 已于 2026-07-19 complete/final-gated；
> `bookKeeperPrimaryWalM1FinalCheck` 通过 199-task aggregate（包含 Phase 1.5、Phase 4 与 pinned local Pulsar
> regressions）。BK-M2 `BOOKKEEPER_WAL_ONLY` implementation is in progress：metadata/store/scanner and the
> allocator、writer state machine、recovery-open sealing、ordered exact-range appender、fixed physical-reference
> activation、non-recovery reader/lease/checksum、bounded whole-ledger retention and explicit module-local profile
> composition checkpoints are implemented。The real-service checkpoint additionally passes real Oxia + BookKeeper
> exact CreateAdv response loss、delayed matching create recovery through the bounded fixed-slot scanner、permanent
> hazard/retention veto、cold all-256-root/all-16-allocation-slot shard coverage、rollover、fresh-client/runtime cold
> restart、stable-target history、post-restart writer recovery、two independent recovery-process contention、
> whole-ledger trim/deletion、lost DELETE response and a second fresh-process dual-absence convergence；it
> also found and fixed the
> public BookKeeper client's consuming `ByteBuf` ownership boundary。These checkpoints are
> gated by `bookKeeperPrimaryWalM2MetadataCheck` / `bookKeeperPrimaryWalM2RuntimeCheck` /
> `bookKeeperPrimaryWalM2RetentionCheck` / `bookKeeperPrimaryWalM2PulsarCheck`。`BookKeeperWalRuntime` can execute
> BK_ONLY through `DefaultStreamStorage` and the ManagedLedger facade；the pinned local Pulsar broker passes the exact
> borrowed stock-client boundary。The remaining M2 scenario/evidence rows and aggregate/final gate are not yet closed；
> production provider composition、first-create admission and broker ownership rollout belong to BK-M5 and remain
> fail-closed, so the production broker still rejects the profile before primary IO。

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
The target Pulsar source lock is `master@41d1cddb9d29451884002b96de2bc52367cbb8ca`。The Nereus pre-design audit
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

BK-M1 itself did not register a BookKeeper writer/reader. The current BK-M2 checkpoint now supplies the exact
BookKeeper proof adapter and production Oxia proof revalidation, while unknown/unconfigured proof types still fail
closed before head CAS。

The BK-M2 metadata checkpoint implements `BookKeeperKeyspace` with strict
root/protection/reader/allocation-slot inverse、all seven V1 record models and explicit enum wire ids、seven envelope
codecs registered in `MetadataRecordCodecFactory`、frozen envelope SHA-256 vectors and malformed/truncated/trailing/
checksum tests。It also implements focused `BookKeeperWriterMetadataStore`/`BookKeeperLedgerMetadataStore` surfaces、
`OxiaJavaBookKeeperMetadataStore`、the deterministic fake adapter、protocol-edge transition validation、idempotent
create、exact-version CAS/delete recovery after applied-response loss、scope/page-size-bound pagination and complete
256-root/16-allocation-slot shard tests。

The subsequent BK-M2 runtime checkpoint implements reserved-id `CreateAdv` allocation with permanent uncertain-create
hazards；stream writer/allocation/range CAS；exact NBKL1 provider metadata；partial-write recovery-open sealing；ordered
explicit entry-id writes；durable reservation plus mandatory protection slots；generic commit and generation-zero
owner activation through `BookKeeperPhysicalReferenceProof`；fixed reader-slot deletion fencing；non-recovery full-range
read、NBKR1 verification、middle-offset clipping and exact provider-neutral accounting。The retention checkpoint adds
`BookKeeperWalReferenceManager`、`BookKeeperWalOnlyRetirementAuthority`、
`BookKeeperWalOnlyReferenceRetirementCoordinator`、durable `RETIRED` protection tombstones、
`BookKeeperWalRetentionGate` and `BookKeeperLedgerRetentionManager`。It consumes exact monotonic L0 trim or durable
abandoned-reservation authority without choosing the trim offset，then performs bounded complete inventory capture、
exact rollout/namespace/provider
validation、double-capture mark、reader drain、unmark-on-drift、provider delete response-loss recovery and delayed dual
absence before `DELETED`；GC remains disabled/dry-run by default and local config is never deletion authority。
Tests cover normal allocation、
uncertain create、stale-session pre-IO rejection、ownership-transfer sealing、contiguous/reused ranges、partial write
seal/no-tail-reuse、commit/gen0 protection owners、non-recovery reads、checksum failure、retirement-authority failure、
reader veto and mark/drain/lost-delete-response/dual-absence convergence。Every GC root CAS from `SEALED -> MARKED`
through the second-absence `DELETED` transition also has deterministic applied-response-loss recovery through the
production Oxia metadata adapter；the independent-process transport-loss cut remains open。The Docker-backed
`BookKeeperWalOnlyOxiaBkIntegrationTest` now repeats rollover、cold history read、writer recovery、trim、delete response
loss and dual absence against production Oxia adapters plus a real BookKeeper 4.18 cluster, including a fresh process
after the first absence。Profile registration and production Pulsar routing remain deliberately absent for BK-M5。The module-local
`BookKeeperStorageProfileResolver`、`BookKeeperWalRuntime` and generic `DefaultStreamStorage` composition now admit
BK_ONLY only when the exact appender/reader are installed；strict append waits for generation zero and cold read
resolves the same `BookKeeperEntryRangeReadTarget`。Production and fake L0 stores share
`BookKeeperStableAppendProtectionValidator`，so this integration still reloads the exact root/protection proof before
head CAS rather than weakening the gate。Recovery reconstructs
missing mandatory fixed slots from the still-selected active reservation before it may
clear writer state，then terminalizes non-durable RESERVED/WRITING attempts as exact `ABANDONED` authorities。

The latest append checkpoint adds real multi-entry range evidence and real first/middle/last write cuts against
BookKeeper 4.18：each tainted ledger is recovery-opened closed and never tail-reused。The deterministic L0 test also
proves reachable-head generation-zero repair reuses the exact target with zero additional BK writes and that future
profiles/oversize batches fail before provider IO。`DEFERRED_SYNC` is unrepresentable in
`BookKeeperWalConfiguration`; the production adapter is contract-tested to pass an empty `WriteFlag` set。

`BookKeeperAppendReservationIds` and `BookKeeperAppendRecoveryCoordinator` now provide the first restart-recovery
checkpoint for BK-25–BK-29：point lookup by stream/attempt、WRITING seal/abandon、same-session durable replay without a
provider rewrite、new-session fence plus abandoned-range retirement、intent/head response-loss replay and sealed-root
generation-zero repair。These are deterministic D-level cuts；real Oxia/process/two-client evidence remains open where
the matrix says so。
The production-adapter real-service test additionally covers graceful first-process loss/fresh-client recovery for
BK-26 and an expired-session owner replacement for BK-27；the current-session path performs zero writes in the new
client and the fenced path moves the replacement to a new ledger。Abrupt kill remains an explicit C-level gap。

The focused ManagedLedger checkpoint admits BK_ONLY in projection creation/open/Position mapping and maps append to
`WAL_DURABLE` without exposing the physical BK ledger id。`NereusBookKeeperManagedLedgerIntegrationTest` drives exact
entry bytes through `NereusManagedLedger.addEntry/readEntry` over generation zero and proves the returned Position
uses the virtual ledger。The pinned Pulsar fork now obtains the same stock
`BookkeeperManagedLedgerStorageClass.getBookKeeperClient()` instance, passes it as an explicitly borrowed
`NereusRuntimeContext` resource, rejects non-BK/null providers, and passes broker main/test Checkstyle plus its focused
test。`DefaultNereusRuntimeProvider` does not yet compose that client into the production BK runtime；that BK-M5 rollout
boundary does not weaken the now-real BK-M2 storage evidence, but it still prevents first-create and a production broker
BK_ONLY data path。
