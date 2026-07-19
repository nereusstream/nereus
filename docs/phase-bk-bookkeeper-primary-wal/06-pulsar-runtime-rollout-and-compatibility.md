# Pulsar Runtime, Rollout, and Compatibility

## 1. Local source boundary

The integration target is the local Pulsar checkout only：

```text
/Users/liusinan/apps/ideaproject/nereusstream/pulsar
master@eaf7b9a704890a9265c21f30d9f351e02d00c600
BookKeeper 4.18.0
```

There is no `M1-SNAPSHOT` dependency. Nereus Gradle gates compile/check the local fork through the existing locked
checkout mechanism.

## 2. Borrowed BookKeeper client ownership

Current `NereusManagedLedgerStorage` creates a `ManagedLedgerClientFactory`, obtains the default stock storage class,
and closes that owner at broker shutdown. Extend initialization：

```java
ManagedLedgerStorageClass raw = localBookkeeper.getDefaultStorageClass();
BookkeeperManagedLedgerStorageClass stock =
    (BookkeeperManagedLedgerStorageClass) raw;
org.apache.bookkeeper.client.api.BookKeeper borrowed =
    stock.getBookKeeperClient();

NereusRuntimeContext context = new NereusRuntimeContext(
    ...,
    new BorrowedBookKeeperClient(borrowed),
    ...);
```

Exact type/wrapper names may follow module conventions. Required ownership：

- `ManagedLedgerClientFactory` is the sole owner/closer；
- Nereus may create/close ledger/read/write handles obtained from the client；
- Nereus never calls `BookKeeper.close()` and never closes broker event loops；
- partial Nereus runtime initialization closes only Nereus-owned resources, then the existing storage error path closes
  the stock factory once；
- a unit test injects a counting client/owner and freezes success/failure/normal-close counts。

`nereus-api`/`nereus-core` do not import Pulsar or ManagedLedger classes. `nereus-bookkeeper` depends on BookKeeper
client API, while `nereus-pulsar-adapter` is the composition boundary.

## 3. Runtime composition

Refactor the Object-named composition seam only as far as needed：

```text
DefaultNereusRuntimeProvider
  -> shared Oxia / ObjectStore / F2-F4 runtime
  -> Object primary-WAL adapter (existing profiles)
  -> BookKeeperWalRuntime (when BK capability/config installed)
       appender + reader + allocator + recovery + retention
  -> PrimaryWalRegistry(Object + BK)
  -> provider-neutral AppendCoordinator / ReadTargetReaderRegistry
  -> one shared F4 planner/task/worker/checkpoint/lag runtime
```

Do not instantiate a second F4 service for BK profiles. Materialization registry scans all registered object-enabled
streams and dispatches each source target through the generic registry.

Runtime startup order：metadata/object provider -> BK borrowed adapter/config validation -> registries -> append/read
core -> managed-ledger factory -> recovery scanners -> optional workers -> optional safe GC. Close runs in reverse and
is bounded.

## 4. Broker configuration

Add checked one-time fields to `ServiceConfiguration` and map them in `NereusBrokerStorageConfiguration`：

| Field | V1 rule/default |
| --- | --- |
| `nereusBookKeeperPrimaryWalEnabled` | `false`; gates registration/profile first-create |
| `nereusBookKeeperClusterAlias` | nonblank durable alias |
| `nereusBookKeeperEnsembleSize` | positive; defaults to explicit stock BK configured value |
| `nereusBookKeeperWriteQuorumSize` | `ensemble >= write >= ack` |
| `nereusBookKeeperAckQuorumSize` | positive |
| `nereusBookKeeperDigestType` | admitted BookKeeper digest enum; immutable binding |
| `nereusBookKeeperPasswordSecretRef` | secret reference only; empty password must be explicit/defaulted consistently |
| `nereusBookKeeperMaxEntriesPerLedger` | positive and bounded |
| `nereusBookKeeperMaxBytesPerLedger` | positive and bounded |
| `nereusBookKeeperMaxLedgerAgeSeconds` | positive |
| `nereusBookKeeperMaxWritesInFlight` | V1 default `1`; bounded |
| `nereusBookKeeperMaxReadsInFlight` | positive |
| `nereusBookKeeperMaxReadBytesInFlight` | at least one admitted target bound |
| `nereusBookKeeperOperationTimeoutSeconds` | positive, within broker close budget |
| `nereusBookKeeperAllocationTimeoutSeconds` | positive |
| `nereusBookKeeperSealTimeoutSeconds` | positive |
| `nereusBookKeeperDeleteTimeoutSeconds` | positive |
| `nereusBookKeeperReaderLeaseSeconds` | greater than renewal interval |
| `nereusBookKeeperReaderLeaseRenewSeconds` | positive |
| `nereusBookKeeperRetentionScanIntervalSeconds` | positive |
| `nereusBookKeeperRetentionScanPageSize` | `[1,1024]` |
| `nereusBookKeeperMaxConcurrentDeletes` | positive bounded |
| `nereusBookKeeperGcDrainGraceSeconds` | at least reader lease + clock skew |
| `nereusBookKeeperLateCreateAuditGraceSeconds` | greater than max provider retry/operation/close uncertainty window |
| `nereusBookKeeperGcEnabled` | `false` safe default |
| `nereusBookKeeperGcDryRun` | `true` safe default |

`nereusDefaultStorageProfile` remains first-create-only. Merely selecting a BK enum while the feature/capability is
disabled must fail broker config validation or topic first-create before any ledger allocation.

Configuration binding SHA-256 includes cluster alias、quorums、digest、non-secret password identity/version and V1
protocol semantics. Every broker advertises the same digest; secret bytes never enter lookup properties/Oxia/logs.

## 5. Cluster capability

Add reserved lookup properties：

```text
nereus.bookkeeper-primary-wal-protocol = "1"
nereus.bookkeeper-primary-wal-config   = <configuration binding SHA-256>
```

`NereusBookKeeperPrimaryWalCapability.requireUnreserved` chains with existing storage-binding/cursor/generation
reserved checks. `NereusBrokerCapabilityCoordinator.decorateLookupProperties` publishes them only after the borrowed
client、module、codecs、writer/reader/recovery are initialized.

Profile barriers use two stable all-persistent-broker snapshots：

| Profile | Required capabilities |
| --- | --- |
| BK_ONLY | storage binding + cursor + BK protocol/config |
| BK_ASYNC_OBJECT | BK_ONLY set + generation protocol |
| BK_SYNC_OBJECT | BK_ONLY set + generation protocol + required-object completion capability |

The sync completion capability may share BK protocol V1 only if every V1 broker implements it; during staged
development it remains a separate reserved property until BK-M4 final-gates it.

Readiness identity includes broker id、start timestamp、all required property key/values and configuration digest.
Registry changes invalidate cached readiness. A broker with missing/different config keeps new BK profile admission
closed; existing topics may read only if the broker can satisfy their exact durable profile, otherwise ownership must
not be assigned/opened there.

## 6. Durable activation and deletion gate

Add a product-owned `BookKeeperProtocolActivationRecord` under a fixed Oxia capability key：

```java
record BookKeeperProtocolActivationRecord(
    int schemaVersion,
    Lifecycle lifecycle,                 // PREPARED, ACTIVE
    int protocolVersion,
    long brokerReadinessEpoch,
    String brokerReadinessSha256,
    String configurationBindingSha256,
    boolean walOnlyPublicationEnabled,
    boolean asyncPublicationEnabled,
    boolean syncPublicationEnabled,
    boolean ledgerDeletionEnabled,
    String rootCoverageProofSha256,
    String streamCoverageProofSha256,
    String bookKeeperScopeProofSha256,
    long activatedAtMillis,
    long metadataVersion) { }
```

Publication bits advance monotonically only after their milestone gate. Ledger deletion additionally requires：

- complete all-256-shard root/protection/lease scan；
- complete registered BK stream/profile/reference-domain scan；
- exact provider-scope create/write/read/fence/delete/response-loss canary；
- current stable broker readiness/config digest；
- one CAS installing all three exact proof digests and deletion bit；
- local `gcEnabled && !gcDryRun` plus exact active proof revalidation on every mutation。

Configuration alone is never deletion authority. Safe defaults start no mutating ledger collector.

## 7. First-create and reopen admission

### 7.1 First create

```text
resolve requested/default profile
storage-class creation guard proves no stock BK/Nereus durable conflict
require exact profile capability/readiness + activation publication bit
create durable Nereus stream metadata with immutable profile
store BK configuration binding for BK profiles
create F2 topic/virtual-ledger projection
register generation/materialization stream if object-enabled
finish storage-class binding ACTIVE
open/recover writer
return topic
```

If any prerequisite fails before stream metadata/profile publication, no BK IO occurs. If durable create steps are
partially applied, existing binding/creation recovery converges the exact profile; it never re-reads a changed broker
default and chooses another profile.

### 7.2 Existing topic

```text
load durable binding + stream metadata + projection
ignore current default profile
require local adapter/config matches stored profile/binding
require current cluster readiness for ownership/writes
recover writer/ledger state before writable publication
install F3 cursor + F4 services as profile requires
```

Loaded and unloaded admin paths use durable binding/profile facts. An unloaded topic is not interpreted from namespace
default policy.

### 7.3 Profile mutation

Any attempt to change `StreamMetadataRecord.profile` or its BK configuration binding after first create is rejected.
Updating `nereusDefaultStorageProfile` affects new streams only. There is no “close and reopen to migrate” behavior.

## 8. Broker ownership lifecycle

### 8.1 Normal load

After topic ownership is acquired, Nereus obtains/renews the Oxia append session, reconciles writer state, and only
publishes `NereusManagedLedger` writable after any prior ACTIVE ledger is resolved. Read-only historical open can start
earlier only through non-recovery BK reads and must not recovery-open/fence.

### 8.2 Unload/close

```text
stop new facade appends
drain bounded in-flight attempts
seal current ledger when time permits; otherwise leave durable SEALING/RECOVERING state
release/expire append session
close Nereus-owned ledger handles/read cache
do not close borrowed BK client
complete unload within broker budget
```

An unload timeout is recoverable durable state, not permission to discard ledger roots/reservations.

### 8.3 Failover/restart

New owner follows document 04 fencing sequence, allocates a new ledger and preserves logical offsets. Two brokers may
contend on recovery, but only the writer-state/session/root CAS winner becomes writable. Workers may run on either
broker against shared F4 tasks; source protections and claims serialize physical work independently of topic owner.

## 9. Stock BookKeeper isolation

Stock Pulsar topics and Nereus BK-profile topics may share the same client/BookKeeper cluster, but not metadata
authority：

- stock ManagedLedger metadata/ledgers are never inserted into Nereus root keyspace；
- Nereus ledgers carry exact `NBKL1` custom metadata and global Oxia root/allocation identity；
- Nereus reader/deleter requires both root and matching custom metadata；
- a ledger-id collision with stock/foreign metadata is never deleted；a new random candidate is chosen；
- Nereus does not invoke stock ManagedLedger trim/delete APIs for its physical ledgers；
- stock control topic write/read/retention remains in every real two-broker gate。

## 10. Profile rollout

Recommended operational rollout mirrors implementation milestones：

```text
1. deploy BK-capable readers/codecs/module with all publication/delete bits off
2. establish stable cluster readiness/config digest
3. enable BK_ONLY publication for canary first-create topics
4. pass restart/unload/failover/rollover/logical-trim gates
5. enable BK_ASYNC publication; keep ledger delete dry-run initially
6. verify lag/object publication/source-retirement; activate scoped ledger deletion
7. enable BK_SYNC publication and completion SLOs
8. expand profile policy only after BK-M6 aggregate gate
```

Removing capability from one broker prevents new BK topic creates and ownership/writes there. Rollback must keep at
least read/recovery support for already-created BK targets; downgrading to a build that cannot decode/read durable BK
targets is prohibited.

## 11. No online migration in BK-M0–M6

These operations are rejected：

```text
OBJECT_WAL_* -> BOOKKEEPER_WAL_*
BOOKKEEPER_WAL_* -> OBJECT_WAL_*
BK_ONLY -> BK_ASYNC_OBJECT
BK_ASYNC_OBJECT -> BK_SYNC_OBJECT
```

A future migration delivery needs a durable state machine：

```text
fence append session
freeze barrier offset/profile generations
drain/reconcile old writer/tasks
publish target-profile capability/checkpoint
new commits after barrier use new primary target
old commits retain original reader/protection/retention rules
complete/backfill under explicit migration policy
```

Migration never rewrites a committed target or makes policy mutation reinterpret old physical identity. The future
design must separately decide whether BK_ONLY -> async backfills pre-barrier history.

## 12. Compatibility matrix

| Dimension | Required behavior |
| --- | --- |
| Object-WAL + BK-WAL topics | same brokers/runtime; correct registry dispatch; no target casts |
| stock BookKeeper topic | unchanged write/read/trim while Nereus BK profiles operate/delete own ledgers |
| old Object target chain | remains decodable/readable after BK module rollout |
| BK generation zero + Object higher generation | highest healthy selection/fallback/source retirement exact |
| ordinary/batched Pulsar entries | raw Entry bytes/properties and virtual MessageIds unchanged |
| cursor seek/history/restart | F3 logical positions independent of physical ledger rollover/deletion |
| topic unload/failover/rejoin | new ledger per session, no offset gap/duplicate head |
| partitioned topics | each partition has its own immutable profile/stream/ledger lifecycle |
| loaded/unloaded admin | durable profile/readiness route; no default-profile reinterpretation |
| config mismatch/old broker | pre-IO rejection and ownership exclusion |

## 13. Operations and observability

Admin/status surfaces may expose：profile、configuration digest、active segment sequence/ledger id (operator-only)、
writer/root lifecycle、materialization lag、sealed/reclaimable ledger counts、blocked veto reason and sync completion
latency. They are read-only projections of durable/provider facts.

Sensitive fields are redacted. Logs use stable hashes for stream/fencing/config identities and never entry payload or
password. Alerts distinguish：allocation/recovery stuck、checksum/auth mismatch、materialization lag、retention veto、
DELETING dual-absence retry and capability/readiness drift.

## 14. BK-M5 acceptance

BK-M5 is not the first integration test; BK-M2–M4 already require focused local-Pulsar entry tests. M5 closes rollout
only after：

- all configuration/reserved-property/readiness/activation tests pass；
- first-create stores immutable exact profile and unsupported routes are pre-IO；
- loaded/unloaded/partitioned routing is deterministic；
- two brokers prove ownership transfer, unload, crash/restart/rejoin and reverse takeover；
- ordinary/compressed-batch exact MessageIds and cursor seek/history remain stable；
- stock BookKeeper control topic coexists throughout；
- borrowed client closes exactly once by its stock owner；
- safe defaults schedule no BookKeeper delete；
- an explicitly activated canary scope proves response-loss-safe deletion without touching foreign/stock ledgers。
