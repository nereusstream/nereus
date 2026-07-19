# Implementation Plan and Gates

## 1. Status discipline

This document contains the frozen plan and explicit implementation evidence. Current status：

```text
BK-M0 design/source audit       documentation-gated on 2026-07-19
BK-M1 provider-neutral foundation in progress (first checkpoint implemented on 2026-07-19)
BK-M2 .. BK-M6                  not implemented
all BookKeeper profiles         reserved / rejected before primary IO
BookKeeper ledger deletion      absent / safe default closed
```

`bookKeeperPrimaryWalDocumentationCheck` remains the documentation gate. `bookKeeperPrimaryWalM1Check` and
`bookKeeperPrimaryWalM1FinalCheck` are executable and backed by real module/unit/predecessor dependencies；M2–M6 names
remain frozen target names and must not be registered as empty/success-only Gradle tasks. A milestone becomes complete
only when its ordinary and final tasks execute their documented tests against the exact source locks.

## 2. Delivery dependency graph

```text
BK-M0 design gate
  -> BK-M1 provider-neutral foundation
  -> BK-M2 BOOKKEEPER_WAL_ONLY
  -> BK-M3 BOOKKEEPER_WAL_ASYNC_OBJECT
  -> BK-M4 BOOKKEEPER_WAL_SYNC_OBJECT
  -> BK-M5 Pulsar rollout
  -> BK-M6 compatibility / scale / chaos
  -> bookKeeperPrimaryWalFinalCheck
```

M2–M4 each include focused local-Pulsar tests. M5 closes full rollout/ownership and is not permission to postpone all
broker integration until the end.

## 3. BK-M0 — contract and local-source audit

### 3.1 Owned files

```text
docs/phase-bk-bookkeeper-primary-wal/README.md
docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md
docs/phase-bk-bookkeeper-primary-wal/02-domain-api-module-and-target-contract.md
docs/phase-bk-bookkeeper-primary-wal/03-oxia-metadata-ledger-lifecycle-and-codecs.md
docs/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md
docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md
docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md
docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md
docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md
scripts/check-bookkeeper-primary-wal-documentation.sh
```

### 3.2 Gate

```bash
./gradlew bookKeeperPrimaryWalDocumentationCheck
```

The gate verifies all documents/links、Nereus audit lock
`35c58c575c3da220633c53e48a581f16756ea047`、local Pulsar lock
`eaf7b9a704890a9265c21f30d9f351e02d00c600`、BookKeeper `4.18.0`、profile naming and explicit not-implemented status.
The exact command passed on 2026-07-19. It is BK-M0 design evidence only；the target implementation tasks below do
not exist yet and may not be represented by empty Gradle tasks。

### 3.3 Mandatory review stop A

Before production code：confirm exact-id `CreateAdv` allocation、one-stream/session ledger ownership、provider-neutral
read result、range checksum、retention inventory、sync completion predicate and no-migration boundary. Any change to
those decisions updates docs/golden identifiers first.

## 4. BK-M1 — provider-neutral seam and adapter foundation

### 4.1 Module/build

Add：

```text
settings.gradle.kts                         include(":nereus-bookkeeper")
nereus-bookkeeper/build.gradle.kts
nereus-bookkeeper/src/main/java/com/nereusstream/bookkeeper/...
nereus-bookkeeper/src/test/java/...
nereus-bom                                 exported module version/dependency constraints
```

Boundary script：`scripts/check-bookkeeper-module-boundaries.sh`。It fails on BookKeeper/ManagedLedger imports from
`nereus-api`/`nereus-core` and on `org.apache.bookkeeper.mledger.*` anywhere in `nereus-bookkeeper`.

### 4.2 Provider-neutral API/core

Target changes：

| Area | Target files/types |
| --- | --- |
| generic read values | `ReadSourceRef`、`PhysicalReadStats`、`PhysicalReadResult`; simplify `ReadBatch` |
| compatibility | migrate `WalReadResult`/`WalSliceReadStats` and Object reader tests without logical/metric drift |
| registry | `ReadTargetReader` and `ReadTargetDispatcher` return generic results |
| read validation | `ReadCoordinator` compares canonical target identity; provider-neutral failure classification |
| append durable value | extend `DurablePrimaryAppend` with typed physical identity/evidence |
| append coordinator | select appender from `PrimaryWalRegistry`; remove Object prepared/durable casts |
| protection | `PrimaryPhysicalReferenceAdapterRegistry`; generic prepared stable append |
| completion | `AppendCompletionPolicy` + `AppendAckBoundary` + extended `StorageExecutionPlan` (all BK routes still closed) |

Compatibility constructors may live for one milestone but tests must use both native generic and legacy Object entry
points. No BK reader is allowed to construct a fake `ObjectId`/`EntryIndexRef`.

### 4.3 Metadata genericization

Refactor `PreparedStableAppend`、`ProtectedStableAppend`、`ProtectedGenerationZero` and
`DefaultGenerationZeroPhysicalReferencePublisher` to carry/dispatch `ReadTargetRecord` + exact physical-reference
identity. Keep `StreamCommitTargetRecord` bytes/golden contract and legacy Object dual-read unchanged.

Add failure-injection tests at：physical persist -> commit prepare -> protection -> head CAS -> gen0 publication. Object
profile behavior and current Phase 4 final gates are mandatory predecessors.

### 4.4 BookKeeper foundation classes

Implement without registering an executable profile：

```text
BookKeeperWalConfiguration + binding digest
BookKeeperLedgerGcConfiguration + activation/safe-default validation
BookKeeperLedgerIdNamespace + candidate generator/round-trip validation
BookKeeperLedgerIdNamespaceReservation value/codec/store + read-only runtime verifier
BookKeeperRangeChecksums (NBKR1)
BookKeeperPreparedPrimaryAppend
BookKeeperPrimaryPhysicalIdentity
BookKeeperClientOperations adapter over public 4.18.0 API
BookKeeperLedgerHandleCache
BookKeeper resource/deadline/error mapping utilities
BookKeeperEntryRangeReadTarget codec/golden regression
```

### 4.5 Gates

Target tasks：

```text
bookKeeperPrimaryWalM1Check
  -> API/codec/config/checksum/resource tests
  -> Object append/read/provider-neutral regression
  -> module-boundary/static contract scripts

bookKeeperPrimaryWalM1FinalCheck
  -> M1Check
  -> Phase 1.5 final predecessor
  -> Phase 4 final predecessor
  -> local Pulsar 4.18.0 API compile probe
```

### 4.6 Mandatory review stop B

Do not begin profile IO until Object-WAL passes unchanged through the new seams and a synthetic BK target can traverse
commit/gen0 metadata without Object fields. Review buffer lifetime、deadline propagation and public API compatibility.

### 4.7 Implemented checkpoint evidence (2026-07-19)

Implemented：module/BOM/catalog wiring；generic read source/result/stats and canonical target identity；generic physical
append identity/token and completion-policy values；BookKeeper configuration/GC safe default/namespace/reservation
verifier；NBKR1；prepared buffer ownership；4.18 public client operations；deadline/error mapping；bounded handle cache；
module-boundary script and focused tests。

Still required before BK-M1 is complete：generic commit/protection/gen0 registry without Object casts；provider-neutral
coordinator selection；remaining BK-02/04/06/07/08 evidence；full M1 predecessor final gate。No BookKeeper profile is
executable at this checkpoint。

## 5. BK-M2 — `BOOKKEEPER_WAL_ONLY`

### 5.1 Metadata/keyspace

Implement document 03 exactly：

```text
BookKeeperKeyspace + strict inverse
seven V1 writer/allocation/slot/root/reservation/protection/lease record codecs + wrappers/pages
BookKeeperWriterMetadataStore + production/fake adapters
BookKeeperLedgerMetadataStore + production/fake adapters
shared contract/failure-injection scenarios
all-256-shard root scanner
all-16-shard fixed allocation-slot scanner
```

No generic unbounded list/delete primitive is added. Golden codec changes require protocol/version review.

### 5.2 Physical module

Implement in this order：

1. `BookKeeperLedgerAllocator` reserved-namespace exact-id intent/durable-slot/root reservation/create/reconcile；
2. `BookKeeperWriterStateMachine` allocation/active/reservation/recovering CAS；
3. `BookKeeperPrimaryWalAppender` explicit ordered writes and taint/seal；
4. `BookKeeperLedgerRecovery` recovery-open/fence/seal and nonterminal scan；
5. `BookKeeperPrimaryWalReader` lease/non-recovery exact read/checksum/accounting；
6. `BookKeeperWalReferenceManager` fixed range-protection + fixed reader-slot admission；
7. generation-zero trim/commit/protection retirement adapters；
8. `BookKeeperLedgerRetentionManager` dry-run, mark/drain/delete/dual-absence recovery；
9. `BookKeeperWalRuntime` lifecycle/composition。

### 5.3 Core/profile

Add `BookKeeperStorageProfileResolver` or extend the exact profile matrix so only BK_ONLY maps to
`BOOKKEEPER_ENTRY_RANGE + DISABLED + STABLE_HEAD` after writer/reader/recovery/metadata capability is complete.
Unsupported durability/completion policies fail before allocation.

### 5.4 Focused Pulsar slice

Test-only/local composition borrows the stock BK client and drives `NereusManagedLedger.asyncAddEntry/readEntries`
through exact BK generation zero. Production-wide first-create/capability rollout remains disabled until M5.

### 5.5 Gates

```text
bookKeeperPrimaryWalM2MetadataCheck
bookKeeperPrimaryWalM2AllocatorCheck
bookKeeperPrimaryWalM2AppendReadCheck
bookKeeperPrimaryWalM2RecoveryFencingCheck
bookKeeperPrimaryWalM2RetentionCheck
bookKeeperPrimaryWalM2PulsarCheck
bookKeeperPrimaryWalM2Check                 ordinary aggregate, delete remains dry-run
bookKeeperPrimaryWalM2FinalCheck            real Oxia + real BK + restart/delete-response-loss
```

Final evidence includes empty/nonempty entry/physical-byte/append-range rollover、partial/uncertain writes、head/gen0
cuts、logical trim with mixed live ranges、fixed-slot protection/reader contention、foreign collision、permanent
late-create-hazard deletion veto and exact dual-absence delete recovery.

### 5.6 Mandatory review stop C

Before enabling physical delete：review complete protection inventory、root/stream/reservation scan bounds、foreign
custom-metadata check、exclusive exact-id namespace proof、validate-to-delete ABA、`CREATE_UNCERTAIN` nonterminal
handling、permanent `lateCreateHazard` veto and safe defaults. BK_ONLY must demonstrate bounded reclaim for normal
terminal creates；hazardous ledgers are an explicit bounded fail-closed quarantine, not a reclaim success. M3 cannot be
used to hide a BK_ONLY leak.

## 6. BK-M3 — `BOOKKEEPER_WAL_ASYNC_OBJECT`

### 6.1 F4 source adapters

Modify：

```text
DefaultExactSourceRangeReader
MaterializationTaskProtections
DefaultMaterializationTaskProtectionReconciler
DefaultMaterializationWorker
DefaultGenerationCommitter
DefaultTerminalWorkflowMetadataRetirer
source-retirement / reference-domain / generation fallback handlers
DefaultMaterializationLagSnapshotReader profile admission
```

Every switch is on registered target/provider identity. Output Object protocols/formats/codecs remain unchanged.

### 6.2 Async profile

Add：

```text
BookKeeperSealedLedgerMaterializationTrigger
BK source protection adapter
restart task/protection reconciliation
BK source release -> ledger-retention handoff
profile resolver plan: BOOKKEEPER_ENTRY_RANGE + ASYNCHRONOUS + STABLE_HEAD
```

The sealed-ledger/stream flush may create one-source normal F4 tasks. There is still one task store、worker pool、lag
reader and checkpoint truth.

### 6.3 Gates

```text
bookKeeperPrimaryWalM3ExactSourceCheck
bookKeeperPrimaryWalM3ProtectionCheck
bookKeeperPrimaryWalM3AsyncProfileCheck
bookKeeperPrimaryWalM3LagCheck
bookKeeperPrimaryWalM3SourceRetirementCheck
bookKeeperPrimaryWalM3Check
bookKeeperPrimaryWalM3FinalCheck             real Oxia + BK + Object store, fresh-runtime cuts
```

Final gate proves stable-head ack before object generation、BK reads during lag、task reconstruction、NCP1 exact bytes、
higher-generation selection/fallback、source protection cuts、trim/replacement release and whole-ledger delete.

### 6.4 Mandatory review stop D

Audit for a second scheduler/lag/checkpoint/retention truth and for remaining Object casts. A task/checkpoint must never
authorize BK deletion without exact COMMITTED generation/read proof.

## 7. BK-M4 — `BOOKKEEPER_WAL_SYNC_OBJECT`

### 7.1 API/core/materialization

Implement：

```text
AppendOptions.completionPolicy + compatibility constructors
StorageExecutionPlan exact ack boundary
RequiredObjectGenerationRequest/Proof
RequiredObjectGenerationCoordinator
single-source deterministic task create/reuse/wait/recover
exact committed generation resolver/read-admission proof
append error/recoverAppend KNOWN_COMMITTED integration
```

Profile resolver refuses sync unless gen0/F4 worker/output reader/capability are installed. It refuses
`STABLE_HEAD`/`GENERATION_ZERO_INDEX` as weaker producer policies for a sync-profile stream.

### 7.2 Gates

```text
bookKeeperPrimaryWalM4CompletionPolicyCheck
bookKeeperPrimaryWalM4TaskReuseCheck
bookKeeperPrimaryWalM4KnownCommittedCheck
bookKeeperPrimaryWalM4ReadAdmissionCheck
bookKeeperPrimaryWalM4Check
bookKeeperPrimaryWalM4FinalCheck             process cuts around every task/publication/read/ack stage
```

Final gate proves no ack before exact Object read, logical visibility from BK while producer waits, timeout after head
returns KNOWN_COMMITTED, and recovery never allocates another BK range/offset.

### 7.3 Mandatory review stop E

Review public completion semantics and all error cuts. `WAL_DURABLE_AND_INDEX_COMMITTED` must still mean generation
zero; no code/test may equate it with Object publication for BK.

## 8. BK-M5 — Pulsar integration and rollout

### 8.1 Nereus repository

```text
NereusRuntimeContext borrowed BK client handle
NereusRuntimeConfiguration BookKeeperWalConfiguration
DefaultNereusRuntimeProvider shared registry/runtime composition
NereusManagedLedgerFactory profile-specific exact plan/admission
capability/activation core provider and status values
explicit namespace admin provisioning/revoke command and readiness/activation binding
```

### 8.2 Local Pulsar fork

```text
ServiceConfiguration BK fields/defaults/getters
NereusBrokerStorageConfiguration mapping/validation
NereusBookKeeperPrimaryWalCapability reserved properties
NereusBrokerCapabilityCoordinator profile readiness
NereusManagedLedgerStorage borrowed client wiring and close ownership
first-create/existing-profile admission
loaded/unloaded/partitioned admin/readiness routes
two-broker integration fixtures
```

The Nereus build source-lock checker must fail if any audited fork file/blob/API unexpectedly drifts until the design
and probe are reviewed.

### 8.3 Gates

```text
bookKeeperPrimaryWalM5ConfigurationCheck
bookKeeperPrimaryWalM5CapabilityCheck
bookKeeperPrimaryWalM5FirstCreateCheck
bookKeeperPrimaryWalM5BorrowedClientCheck
bookKeeperPrimaryWalM5AdminRoutingCheck
bookKeeperPrimaryWalM5TwoBrokerCheck
bookKeeperPrimaryWalM5Check
bookKeeperPrimaryWalM5FinalCheck             retry-disabled real two-broker acceptance
```

### 8.4 Mandatory review stop F

Production enablement requires exact broker readiness/config digest、immutable first-create profile、old-broker
exclusion、borrowed client close ownership and safe delete defaults. Online migration remains rejected.

## 9. BK-M6 — compatibility, scale, chaos, aggregate

### 9.1 Scale boundaries

Required exact fixtures：

- all 256 root shards from fresh empty continuations；
- all 16 allocation-slot shards with CLAIMED/CREATE_STARTED/CREATE_UNCERTAIN and lost-release occupants；
- 1,001 roots in one hot shard plus one in every other shard；
- maximum configured range x protection-slot Cartesian inventory across bounded pages；
- maximum fixed reader slots + occupied source-protection slots restart scan；
- 10,000 sealed/deleted roots without recursion/unbounded retained futures；
- 128-source/1,048,576-record F4 task with mixed BK/Object source targets；
- 4,096 generation candidates and 4,097 fail-closed boundary；
- configured concurrent streams/workers/deletes under one shared budget。
- configured maximum uncertain/hazard allocation slots rejects the next create without retiring/reusing any id or
  clearing any deletion veto。

### 9.2 Chaos/process cuts

Inject process loss/response loss at every numbered transition in documents 03–05, including late create after delete、
two allocators choosing the same candidate、two brokers fencing the same ledger、partial writes、head response loss、
task/source protection cuts、Object publication cuts and dual-absence root CAS loss.

### 9.3 Aggregate gates

```text
bookKeeperPrimaryWalM6ScenarioEvidenceCheck
bookKeeperPrimaryWalM6ScaleCheck
bookKeeperPrimaryWalM6ChaosCheck
bookKeeperPrimaryWalM6CompatibilityCheck
bookKeeperPrimaryWalM6Check
bookKeeperPrimaryWalM6FinalCheck

bookKeeperPrimaryWalCheck
bookKeeperPrimaryWalFinalCheck
```

Final aggregate depends on：BK M1–M6 ordinary/final、Phase 1/1.5/2/3/4 final predecessors、documentation/scenario
traceability、module/source-lock isolation、real Oxia/BookKeeper/Object/two-broker fixtures and clean rerun with no
up-to-date test shortcut.

## 10. Test source sets

Suggested bounded source sets：

```text
bkM1IntegrationTest       provider-neutral/API/local BK API probe
bkM2IntegrationTest       Oxia + BookKeeper WAL_ONLY
bkM3IntegrationTest       Oxia + BookKeeper + Object async
bkM4IntegrationTest       sync completion cuts
bkM5PulsarTest            locked local Pulsar focused tests
bkM5TwoBrokerTest         retry-disabled broker fixture
bkM6ScaleTest             high-cardinality deterministic/real metadata
bkM6ChaosTest             process/response-loss matrix
```

Docker-owning/nested-Pulsar tasks use the existing shared serialization service while ordinary unit tasks retain
parallelism. Each final task writes fresh reports and audits that expected test methods actually executed.

## 11. Error and observability test requirements

Every async public operation has tests for：success、validation failure before IO、provider failure、timeout、cancel、
close race、callback/metrics exception and response loss. Tests assert error code、retriability、`AppendOutcome`、attempt
id presence, resource/permit release and absence/presence of later stages.

Metrics tests verify profile labels and redaction; no payload/password/fencing token appears in logs、exceptions、
OpenTelemetry attributes or durable metadata.

## 12. Implementation stop conditions

Stop and return to design review if implementation discovers any need to：

- expose physical BK ledger/entry ids as F2 MessageId identity；
- make task/checkpoint/lag/projection a visibility or deletion authority；
- fake ObjectId/EntryIndexRef for BK；
- share one writable ledger among streams in V1；
- split one append batch across ledgers；
- use recovery-open for ordinary reads；
- ack after BK write but before reachable head；
- treat gen0 BK index as sync Object completion；
- delete a partially reclaimable ledger or a foreign custom-metadata identity；
- run a BK profile without the exact advanced-ledger-id namespace reservation, or treat repeated absence as proof an
  uncertain transmitted create can never appear；
- clear `lateCreateHazard` after matching metadata appears, or release its fixed slot without a separately versioned
  provider-operation fence；
- rely on listing/process cache for correctness；
- mutate profile in place or reinterpret pre-barrier commits。

## 13. Documentation update rule during implementation

Every implementation checkpoint updates this directory and current design index in the same commit：status、exact
files/classes、source locks、gate command/result、crash cuts and remaining exclusions. “Implemented” requires executable
evidence; enum/class presence alone remains “reserved”.
