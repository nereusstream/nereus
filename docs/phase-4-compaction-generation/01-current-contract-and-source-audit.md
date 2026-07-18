# Current Contract and Local Source Audit

## 1. Purpose

This is the F4-M0 read-only audit. It records the exact current seams that Phase 4 extends and prevents the
implementation from inventing a second append truth、cursor truth or broker policy path. No production source change
is part of M0.

> Post-audit status (2026-07-15)：F4-M4 checkpoint B has closed the generation-zero gap identified in §3.1 with
> prepare/protect/head/materialize/protect sequencing. The hashes and call path below intentionally remain the M0
> baseline；current implementation status is authoritative in this directory's README and documents 03/07.

## 2. Source Locks

### 2.1 Nereus

Repository baseline：`e330969cd5c2c11cd38d0bd7f687185171ae91e2`。

| Source | Git blob | F4 relevance |
| --- | --- | --- |
| `nereus-api/.../target/ReadTarget.java` | `37d2c3c31fe60d5ee044f19743ada68258501508` | sealed generic target union |
| `nereus-core/.../read/ReadResolver.java` | `2b150a2c02dad223b2ffe8c16139c3ce341fcb77` | current generation-0 resolve owner |
| `nereus-core/.../read/ReadTargetDispatcher.java` | `d8a0677c75f7b2b94ed9462916faa41ac8c90d5a` | current dispatch grouping |
| `nereus-core/.../wal/PrimaryWalRegistry.java` | `b77bc805fa078e7549f2252acd0b5a3ba0893596` | currently keyed too coarsely by `ReadTargetType` |
| `nereus-metadata-oxia/.../OxiaMetadataStore.java` | `a315e346128be50062d73c1ecb83ede7cff03466` | current L0 metadata surface |
| `nereus-metadata-oxia/.../PartitionedOxiaClient.java` | `f48f40a00f6f0191ffe5f0c472cbfc60d9493d8f` | get/put/CAS/range/watch；no delete yet |
| `nereus-metadata-oxia/.../records/StreamHeadRecord.java` | `df4382e66f3700ddfed7efbd9b42abaafc40383a` | append/head/trim scalar authority |
| `nereus-metadata-oxia/.../records/StreamCommitRecord.java` | `a3c07e82e5415faac532dfbf0d5b197b745ad1cb` | current genesis-reachable commit node |
| `nereus-object-store/.../ObjectStore.java` | `9209d31549a03ffb1008f77c27a19be3b39c1403` | immutable put/read/head；no list/delete or per-provider-transmission owner guard |
| `nereus-managed-ledger/.../cursor/CursorSnapshotInventory.java` | `e8b46f08ba6904d2312ae245bd1bbc661c2fc739` | read-only live/orphan classification；not delete authority |
| `nereus-managed-ledger/.../cursor/DefaultCursorRetentionCoordinator.java` | `66a81bba145d54150097e2bb1dec3370fe2e1848` | owner-scoped pending protection/trim protocol |
| `nereus-managed-ledger/.../AbstractNereusManagedLedger.java` | `5a3b8214ce81b19280963d799ad1f86e8a68a83c` | current trim housekeeping no-op |
| `nereus-managed-ledger/.../generation/DefaultManagedLedgerGenerationRegistrationBackfillProofCoordinator.java` | `25b21d7b4fe594804b048940ea81a65d4964b06f` | current product-owned exact readiness/proof CAS owner |
| `nereus-managed-ledger/.../NereusManagedLedgerRuntime.java` | `a91a7c36498091fa29a442250a8733751e68fc24` | current activation-store/proof-coordinator lifecycle owner |
| `nereus-pulsar-adapter/.../DefaultNereusRuntimeProvider.java` | `6f96b29762d06bf1ab013693973f6ac40c59f0c7` | current product composition/close owner |

All paths above are under `src/main/java/com/nereusstream/...`; shortened prefixes are only for table readability.

### 2.2 Local Pulsar master

Checkout：`/Users/liusinan/apps/ideaproject/nereusstream/pulsar`，clean
`master@330eeeb3fa9903ed0123c2a0e261d403c32f0a59`。

| Source | Git blob | F4 relevance |
| --- | --- | --- |
| `pulsar-broker/.../nereus/NereusBrokerCapabilityCoordinator.java` | `0b76b1939603c7ba60ed1c4691287819421d3ed4` | two-stable-snapshot barrier plus product-neutral readiness provider |
| `pulsar-broker/.../nereus/NereusGenerationCapabilityReadiness.java` | `782303d1442972a02a290955f920835971a8d8fe` | exact full readiness identity and core conversion |
| `pulsar-broker/.../nereus/DefaultNereusGenerationRegistrationBackfill.java` | `385215ac919a837e11ff999fc4c8bfe02d170b60` | canonical traversal/report plus zero-failure proof handoff |
| `pulsar-broker/.../nereus/NereusManagedLedgerStorage.java` | `be3a1bdd0b036a2c4dea58651bfe519e228bcc8c` | broker-to-product proof completion and publication-activation delegate |
| `pulsar-broker/.../nereus/NereusTopicFeatureValidator.java` | `14916134ed514fc0d2c2e6628e7317463798bb89` | current retention/compaction/admin denylist |
| `pulsar-broker/.../nereus/NereusAdminOperation.java` | `a99c967347ce7b9edc29b57892b220f42bedf8be` | closed loaded/unloaded admin operation set |
| `pulsar-broker/.../service/BacklogQuotaManager.java` | `686e2244b560a16c69303e854719016d610fb305` | cursor skip/mark-delete backlog eviction call path |
| `managed-ledger/.../impl/ManagedLedgerImpl.java` | `26fdb458a21b2edfcdbaed049681e69fd4b99077` | exact time/size OR retention and conservative whole-ledger size-boundary rule |
| `pulsar-broker/.../admin/impl/PersistentTopicsBase.java` | `ca2a1ab569f4cbd6d31d8d464b1b69cd2374045e` | `TRIM_TOPIC` route before managed-ledger call |
| `pulsar-broker/.../admin/impl/NamespacesBase.java` | `a30d50a1ae8ec8c2492df7026e1c5deab6e711f6` | bounded topic-list memory limiter/size-cache pattern reused by backfill |
| `managed-ledger/.../ManagedLedger.java` | `0455f0efa8bb6d0ef248b870b1a68166cdcef2c8` | `trimConsumedLedgersInBackground(CompletableFuture<?>)` contract |
| `pulsar-broker-common/.../resources/TenantResources.java` | `1f954e6bfe89640c214915cc206c0bd52ae95180` | async tenant enumeration for cold-topic registration backfill |
| `pulsar-broker-common/.../resources/NamespaceResources.java` | `1f20be916edd5e2cf92713a169064c32a9b46514` | `listNamespacesAsync(tenant)` backfill traversal |
| `pulsar-broker-common/.../resources/TopicResources.java` | `66d2e81fb0eb9ebfda99deef636857e6200cf0e6` | `listPersistentTopicsAsync(namespace)` including unloaded topics |

The table preserves the M0 blob audit. The following overlay records the exact broker files changed by checkpoint AI
at the current source lock；these hashes, rather than the M0 rows above, are authoritative for the policy/admin cut.

| Checkpoint-AI source | Current Git blob | F4 relevance |
| --- | --- | --- |
| `pulsar-broker/.../nereus/NereusManagedLedgerStorage.java` | `5f35683c1942b5968f1fd6e4aab8e0aec299b03c` | unloaded binding readiness and admin admission |
| `pulsar-broker/.../nereus/NereusResolvedTopicFeatures.java` | `2a3fdd7bdc2697d047077591740ca2956c4f7518` | exact immutable retention/backlog facts |
| `pulsar-broker/.../nereus/NereusTopicFeatureResolver.java` | `7e262fe0a0a61e507ae5b61d11e5f978a84f8b7f` | effective policy precedence and readiness projection |
| `pulsar-broker/.../nereus/NereusTopicFeatureValidator.java` | `33d3942b033de602ded94676226308c30181aacf` | F4 policy/admin admission matrix |
| `pulsar-broker/.../nereus/NereusTopicOpenContext.java` | `5712df5e6057a72bb45fc7727b0ee1f62b20a91c` | exact checked retention snapshot binding |
| `pulsar-broker/.../nereus/NereusTopicPolicySnapshot.java` | `97e9c1a5c27a491b7545ba431ad14e784854040d` | stable complete policy-input comparison |
| `pulsar-broker/.../service/BrokerService.java` | `90d6bdeabe3e887a0abb7e76efab451f341d5dad` | storage-bound capability/readiness resolution |
| `pulsar-broker/.../service/persistent/PersistentTopic.java` | `e1ef70dbd0733782b50104028cb1a462b5f7f703` | marker admission, stable reload and loaded snapshot install |
| `pulsar-broker/.../admin/impl/PersistentTopicsBase.java` | `64d1af0f05db3050fcd26ac635dbc65915ef89da` | loaded and partition-child `TRIM_TOPIC` route |

At the checkpoint-AC source lock, the fork additionally contained
`NereusGenerationProtocolCapability`、`NereusGenerationCapabilityReadiness` and the generation extension to
`NereusBrokerCapabilityCoordinator`：all three protocol versions are advertised/verified under two stable persistent-
broker snapshots, readiness includes broker start timestamps, and broker-registry notifications invalidate the
process-local epoch. It also contains the canonical one-namespace-at-a-time cold-topic registration traversal/report
and its broker lifecycle/config wiring. Checkpoint AA extends that surface with a product-neutral exact-readiness
provider and a zero-failure completion handoff；the broker still does not own activation metadata. The Nereus side
owns the shared-Oxia activation store and response-loss-safe `streamRegistrationBackfill` CAS. Checkpoint AB adds the
product-owned guard、exact six-domain proof、strict projection/L0/registration revalidation and the default-off
first-marker switch. Checkpoint AC adds the product-owned publication coordinator and makes broker zero-failure
backfill completion wait for a proof-gated publication-only ACTIVE transition. The exact current files/tests are
audited by `phase4M5PublicationActivationCheck`；topic marker and concrete mutation call sites remain outside this
checkpoint.

Pulsar M0 uses local source and compiled member checks only；no internet or published M1 snapshot is an input.

## 3. Current Executable Call Paths

### 3.1 Append and generation zero

```text
NereusManagedLedger.asyncAddEntry
  -> StreamStorage.append
  -> ObjectWalAppenderAdapter
  -> immutable Object WAL upload
  -> MetadataStableAppendCommitter
  -> commit-log put/reuse
  -> StreamHeadRecord version CAS                  append linearization
  -> GenerationZeroIndexMaterializer
  -> offset-index/{offsetEnd}/{0}
  -> callback success for strict profile
```

The async profile enum and `WAL_DURABLE` name exist, but `Phase15StorageProfileResolver` rejects them before IO.
F4 must reuse the same stable head commit and may only move generation-zero/index-confirmation and higher-generation
work after the requested acknowledgement boundary.

At the M0 source lock, the implementation still created/reloaded the generic commit intent and performed the head CAS
inside one `commitStableAppend` call. It had no interposed F4 physical-root/protection handshake and was safe only
while product physical deletion was absent. Checkpoint B now implements document 03 §10's two-stage
prepare/protect/head sequence：`REACHABLE_APPEND` precedes head visibility and generation-zero
`VISIBLE_GENERATION` precedes strict success. Physical deletion remains disabled because the separate recovery-root、
retirement and GC gates are not complete.

### 3.2 Ordinary resolve/read

```text
DefaultStreamStorage.read
  -> ReadCoordinator
  -> ReadResolver.resolve
  -> OxiaMetadataStore.scanOffsetIndex
  -> OffsetIndexEntry / ResolvedRange
  -> ReadTargetDispatcher
  -> PrimaryWalRegistry.requireReader(ReadTargetType)
  -> ObjectWalReaderAdapter
```

The registry currently allows only one reader per `ReadTargetType`. Both Object WAL and a compacted object use
`OBJECT_SLICE`, so F4 must replace the dispatch key with an exact target-format key. Registering a second
`OBJECT_SLICE` reader in the current map is forbidden and is not a viable temporary implementation.

### 3.3 Cursor protection and logical trim

```text
writable ledger open
  -> fresh CursorOwnerSession
  -> retention claim/recovery
  -> claim every ACTIVE cursor root
  -> stabilize/rescan
  -> publish ledger/topic

requestTrim(owner, candidate, reason)
  -> require current projection/owner root
  -> activate cursor protocol if absent
  -> reject/recover pending lifecycle
  -> require candidate <= protectedFloor and committedEnd
  -> CAS ACTIVE -> TRIM_PENDING(exact attempt/offset/reason)
  -> StreamStorage.trim
  -> verify L0 trim
  -> CAS TRIM_PENDING -> ACTIVE(completed offset)
```

`AbstractNereusManagedLedger.trimConsumedLedgersInBackground` keeps the compatibility no-op only for non-writable
views. Checkpoints AH–AI override the writable ledger, execute through the shared bounded retention lane and exact
policy provider, then delegate the only logical mutation to the F3 coordinator；the facade never calls
`StreamStorage.trim` directly and completes before physical GC.

### 3.4 Broker policy/admin paths

Current local Pulsar behavior：

- topic open/update stores exact immutable effective retention/backlog values and rejects compaction、Pulsar offload、
  non-precise time eviction and any F4-mutating policy without stable generation readiness；
- a retention/consumer-eviction policy waits for registration-backed marker activation/revalidation and a stable
  post-activation policy reload before installation；
- loaded `TRIM_TOPIC` checks the installed exact feature snapshot, while unloaded bound topics first check current
  cluster readiness；the loaded path validates again before entering the retention service；
- `BacklogQuotaManager` size eviction calls `ManagedCursor.skipEntries` on the slowest cursor；F3 persists that
  destructive movement correctly；
- precise time eviction stays on cursor expiry/mutation；non-precise ledger-segment eviction is rejected because the
  virtual ledger deliberately has no stock rollover semantics；
- compaction trigger/status and `readCompacted=true` remain denied。

F4 therefore admits only the policy paths mapped in document 06. It does not claim that every stock
BookKeeper-ledger retention heuristic is meaningful for one immutable virtual ledger.

## 4. M0 Gap Inventory and Required Owners

The “current fact” column below is the frozen M0 input fact, not a claim about the latest checkpoint. Implemented
closures are tracked in document 07；as of checkpoints AM/AI the generation/reader/task/publication/retention-rollout、
cursor-snapshot execution、current-writer inventory and registration-retirement rows have implementation slices, while
periodic lifecycle scheduling、registration-retirement runtime composition and physical-GC activation/final composition
remain open.

| Gap | Current fact | Phase 4 owner |
| --- | --- | --- |
| higher generation record | only generation-0 legacy/generic records decode | `nereus-metadata-oxia` F4 record/codec/store |
| view isolation | public read is implicitly committed | API enum + metadata namespace；ordinary API remains committed |
| generation allocation | no allocator | per-stream/per-view CAS counter |
| compacted object IO | Object WAL only | `nereus-object-store` compacted package |
| exact reader dispatch | registry keyed by target type only | `nereus-core` target-format registry |
| task/worker | absent | new `nereus-materialization` module |
| restart-safe stream work discovery | no global stream/task enumeration contract | 64-shard Oxia stream registration + process-shared scanner；hint only |
| publication recovery | absent | generation record PREPARED/COMMITTED CAS recovery |
| reader deletion fence | no durable pin | physical-object lease store + core read integration |
| task/catalog reference fence | current reference record is repair/audit only | physical-object protection records |
| object delete/list and guarded PUT retry | absent | `ObjectStore` API/providers |
| metadata delete | absent in `PartitionedOxiaClient` | conditional delete primitive + adapters/tests |
| commit-chain pruning | current validation expects genesis reachability | recovery checkpoint root/object + anchor-aware traversal |
| cursor snapshot deletion | F3 inventory classifies only | managed-ledger F4 reference domain + generic GC |
| retention candidate | F3 accepts an already-safe offset only | managed-ledger policy candidate planner |
| F4 rollout | binding/cursor capabilities only | independent generation capability + activation marker |

## 5. Code Ownership Rules

### 5.1 `nereus-core` remains the committed-read interpreter

`ReadResolver` is extended to understand view-scoped F4 records. It loads head/trim、selects the highest admitted
generation、acquires a pin and dispatches the target. It never asks a task whether output is visible.

### 5.2 `nereus-materialization` owns workflow, not logical facts

The new module may：

- scan committed source ranges；
- create/claim tasks；
- read sources and upload immutable output；
- request generation allocation and publication；
- create recovery checkpoints and object protections；
- schedule reference/GC work。

It may not：

- allocate stream offsets or mutate `committedEndOffset`；
- change append sessions/fencing；
- persist Pulsar `Position/MessageId`；
- infer visibility from task/checkpoint state；
- bypass cursor retention or delete an unregistered reference domain。

### 5.3 Managed-ledger and broker boundaries

`nereus-managed-ledger` owns Pulsar-specific cursor snapshot classification and maps policy candidates to the already
implemented F3 coordinator. The local Pulsar fork owns admission/capability/call-site ordering. Neither layer decodes
or mutates a generation index record directly.

## 6. M0 Narrow Design Decisions

| Question | Decision | Reason |
| --- | --- | --- |
| Does a task state publish bytes? | no | workflow cannot become a second read truth |
| Can a higher `COMMITTED` generation reconstruct Pulsar entries? | no | exact bytes and batch boundaries are observable |
| Can one generation namespace mix semantic views? | no | lossy bytes must never serve ordinary reads |
| Can object GC trust a cached reader count? | no | crash-safe durable bounded lease is required |
| Can source entries be deleted immediately after publish? | no | recovery checkpoint plus read/reference drain is required |
| Can list absence prove unreferenced? | no | list only discovers candidates |
| Can F4 read the F3 owner-scoped facade as a planner? | no | planner uses versioned metadata scan/inventory and final revalidation |
| Can policy trim call L0 directly? | no | current owner must call `requestTrim` |
| Can F4 enable stock Pulsar compaction? | no | F4 publishes the primitive；F8 owns broker semantics |
| Does F4 implement BookKeeper WAL? | no | no writer/reader/location lifecycle exists yet |
| Is cross-key Oxia atomicity assumed? | no | every cross-key transition has prepare/revalidate/recover semantics |

## 7. M0 Gate Verdict

F4-M0 passes as a design-only gate：

- the current code has a usable generic read target and stable append/materialization seam；
- F3 provides the owner/protection/trim/snapshot handoff required for safe physical retention；
- the local Pulsar fork exposes closed admission and capability seams that can be extended without changing
  `ManagedLedger` public APIs；
- the missing pieces are explicit file/module/record/state-machine work rather than an unresolved ownership model；
- F4 can proceed without remapping logical offsets、virtual ledgers、entries、batch indexes or MessageIds。
