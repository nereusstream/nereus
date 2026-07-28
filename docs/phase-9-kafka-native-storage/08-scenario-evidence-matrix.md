# 08 — Scenario and Evidence Matrix

> 状态：Active scenario contract；146-row JSON manifest synchronized；rows remain `PLANNED` until owning milestone evidence
> 规则：一个 requirement 至少一个稳定 ID；release report 必须给每个 ID 一个实际执行结果
> 当前 F9-M1/M2 implementation 与 direct real-service gates 已通过；F9-M3 codec、bounded Produce 与 whole-request async Fetch、M4 transaction、M5 virtual-log/DeleteRecords/periodic-retention rows have deterministic partial evidence；inherited final gate 因本地 Pulsar checkout 偏离锁定提交而未通过，因此 milestone rows 暂不升级为 final-gated

## 1. Evidence tiers

| Tier | Meaning |
| --- | --- |
| `D` | deterministic unit/contract/golden test，no external service |
| `M` | randomized/property/model test with reproducible seed |
| `R` | real Oxia/Object/BookKeeper/KRaft service，may be one JVM |
| `P` | independent broker/runtime processes，fresh restart/kill capable |
| `K` | Kafka fork/upstream focused compatibility suite |
| `C` | named failure cut/response loss/network/process chaos |
| `A` | clean aggregate report consuming all predecessors |

`R/P/C` rows cannot be satisfied by fake stores or mocks。A row may require multiple tiers。

Current deterministic M5 read-side evidence（2026-07-27）：`KafkaCompactedFetchPlannerTest` and
`KafkaCompactedFetchIntegrationTest` cover KF-FET-011/012 at adapter tier，including an entirely empty compacted prefix；
`KafkaCompactedNoResurrectionIntegrationTest` covers the view-domain portion of KF-FET-013/014 and proves an unavailable
mandatory source never produces a COMMITTED retry。The manifest rows remain `PLANNED` because their required real-service、
restart/Kafka-fork and process/chaos tiers are not yet satisfied。

Current provider-profile evidence（2026-07-28）：the product adapter installs exactly
`OBJECT_WAL_SYNC_OBJECT + OBJECT_WAL_ASYNC_OBJECT` over one shared Oxia runtime and can additionally install
`BOOKKEEPER_WAL_ONLY` after validating an operator-provisioned F1-BK
ledger-ID namespace、ACTIVE publication and exact broker readiness。`f9BookKeeperWalOnlyProviderIntegrationTest` starts real
two-bookie BookKeeper、opens a Kafka leader with `BOOKKEEPER_WAL_ONLY`、strictly appends a magic-v2 batch、publishes the exact
BookKeeper generation-zero target and Fetches it through the shared generation resolver。The client is borrowed and the
provider graph closes before it。Fork `df238bb387` supplies the 100-key typed configuration、exact BookKeeper secret
identity/client ownership、five-profile mapping、six-key fail-closed ledger-GC policy and three-key materialization
retirement policy with focused static/unit evidence。
`f9BookKeeperWalOnlyProcessIntegrationTest` now supplies independent-process evidence for this one profile：a real release
distribution runs against stock ZooKeeper long-hierarchical metadata and two bookies，creates a topic，produces/fetches offset
0，checks earliest=0/latest=1，shuts down normally，then a fresh Kafka JVM recovers offset 0、appends/fetches offset 1 and
checks earliest=0/latest=2 before another normal shutdown。`f9M3ProviderIntegrationTest` also opens the real Object graph
under `OBJECT_WAL_ASYNC_OBJECT`；`f9ObjectWalAsyncObjectProcessIntegrationTest` adds release-distribution evidence with
offset 0 in the first JVM and recovered offset 0 plus offset 1 in a fresh JVM，ending at earliest=0/latest=2 over real
Oxia/LocalStack。The BookKeeper async process gate appends four batches、waits for the real NCP2 S3 object、normally
stops and recovers/appends in a fresh JVM；the sync gate appends one batch only after required NCP2
COMMITTED/readable completion and then performs the same cold recovery。All profile-matrix rows remain `PLANNED` because
manifest status advances only with the owning milestone/final aggregate；the live-takeover matrix plus the common
provider-applied C gate now supply the BookKeeper P/C boundary，while final aggregate tiers are still absent。

Current live authority evidence（product 2026-07-28）：
`f9MultiBrokerTakeoverProviderIntegrationTest` starts two independently owned activated Object-WAL runtime graphs against the
same real four-shard Oxia authority and local-file Object root。Readiness contains both exact broker registrations。
Broker A commits `[0,1)` under `(leaderId=1, leaderEpoch=7, brokerEpoch=31)`；while A's 30-second session is live，broker B
opens `(2,8,41)`，atomically replaces the head session、replays A's exact batch and installs writable end `1`。A's next
offset-1 append is rejected by the old durable token and its local storage enters
`WRITE_FENCED_RECOVERY_REQUIRED` even though the rejected request is known-not-committed；B then commits `[1,2)` and reads
both batches byte-exactly。`DefaultKafkaPartitionStorageTest` locks the corresponding local classification rule，and the
task is a dependency of `phase9M3ProviderCheck`。This supplies R-tier partial evidence for KF-META-007/KF-META-012 and the
post-takeover portion of KF-APP-014。On its own it does not supply release-process/KRaft、already-in-flight old append、
BookKeeper/profile takeover or P/C/A evidence。
Fresh `phase9M3ProviderCheck --rerun-tasks` passes 64/64 actionable tasks，including 146/146 scenario synchronization、
the 29-source Nereus lock、Kafka baseline lock、M1/M2/M3 deterministic predecessors and the Object、BookKeeper and
two-runtime takeover provider gates。

Current release-process takeover evidence（product/fork 2026-07-28）：
fork `fe308359b6` makes a Nereus level-1 singleton reassignment one atomic KRaft
`PartitionChangeRecord` with target replicas/ISR/leader and no adding/removing or transitional RF2；fork `bb7e8937c5`
classifies a local replica removal by consulting the new image and calls `resign` when the exact topic ID/partition still
exists，reserving durable `delete` for an absent/recreated identity；fork `df238bb387` adds one success marker per local
controller epoch after Nereus activation reconciliation completes。
`f9MultiBrokerTakeoverProcessIntegrationTest` starts node 1 combined controller/broker and node 2 broker-only from the real
release distribution against shared KRaft/Oxia/LocalStack authority。It commits offset 0 on `[1]`，Admin-reassigns to `[2]`，
requires `leader=2, replicas=[2], ISR=[2]`、empty ongoing reassignment and both processes alive，then recovers offset 0 and
commits/reads offset 1 through broker 2。Fresh execution passes 73/73 actionable tasks in 1m04s and is included in
`phase9M6KafkaProcessCheck`。This adds P-tier partial evidence for KF-META-007 and the post-handoff part of KF-APP-014，
plus D/K/P evidence for KF-OPS-007。

`f9InFlightTakeoverProcessIntegrationTest` adds the named C cut with three real release JVMs：node 3 remains the sole
controller voter，node 1 owns `[1]` and node 2 is the target。A Toxiproxy downstream timeout holds a retries-disabled offset-1
Produce；a `jcmd Thread.print -l` sample must prove the node-1 storage worker is blocked in
`NereusUnifiedLog.appendStable -> CompletableFuture.get` before `SIGSTOP`。Admin then connects through the two live nodes，
atomically installs `[2]` and proves recovered earliest/latest `0/1`。After `SIGCONT`，the stale future fails with
`append session changed before guarded object upload`，the old JVM survives，the WAL key set and latest remain unchanged，
and node 2 alone commits/fetches offset 1 before latest becomes 2。Fresh execution passes 64/64 actionable tasks in 1m01s
including runtime publication/source lock and is included in `phase9M6KafkaProcessCheck`。This supplies Object-WAL P/C
evidence for KF-META-007/KF-META-012/KF-APP-014。At this point the manifest still required `bookkeeper` service coverage；
the following P/C gates now supply it。The topology still has only one controller and final aggregates remain open，so these
rows retain `PLANNED` under the milestone status policy。

`f9BookKeeperProfileTakeoverProcessIntegrationTest` now supplies the matching post-handoff BookKeeper P-tier matrix over one
real stock ZooKeeper long-hierarchical metadata service and two Bookies。For each of
`BOOKKEEPER_WAL_ONLY`、`BOOKKEEPER_WAL_ASYNC_OBJECT` and `BOOKKEEPER_WAL_SYNC_OBJECT`，the harness creates an
independent KRaft/Nereus authority、starts node 1 combined controller/broker and node 2 broker-only from the release
distribution，and waits for both exact broker readiness identities。Node 1 commits/fetches offset 0 on `[1]`；Admin installs
the atomic singleton `[2]` record and the gate requires `leader=2, replicas=[2], ISR=[2]`、no ongoing reassignment、
earliest/latest `0/1` and the old JVM still alive。Node 2 then reads offset 0、commits/fetches offset 1 and ends at
earliest/latest `0/2`。The WAL-only bucket must stay empty before and after handoff；both Object profiles must expose a real
NCP2 object before handoff and after continuation。The operator-seeded activation uses the same physical-deletion digest as
the async broker configuration，so any profile/config identity drift fails before storage I/O。Fresh execution passes 64/64
actionable tasks in 2m17s；the task is included in both `phase9M6KafkaProcessCheck` and
`phase9M6KafkaBookKeeperProcessCheck`。This closes BookKeeper profile post-handoff P evidence，not the already-dispatched
BookKeeper append C cut by itself。

`f9BookKeeperInFlightTakeoverProcessIntegrationTest` now supplies that C cut at the common appender boundary。A dedicated
test-only Java agent waits for the real BookKeeper provider future to succeed but withholds the future observed by
`BookKeeperPrimaryWalAppender`。The gate requires an installed/captured/applied marker sequence、a `jcmd` stack in
`NereusUnifiedLog.appendStable -> CompletableFuture.get`、the exact Oxia reservation in `WRITING` and a separately readable
physical `(ledgerId, entryId)` before broker 1 may be `SIGSTOP`ped。After atomic `[1] -> [2]`，broker 2's offset-1 append must
drive the old reservation to `ABANDONED` and old root to `SEALED`，then return/fetch offset 1。Releasing and resuming broker 1
must fail its stale completion while the process lives、WAL-only remains object-free and final earliest/latest remains
`0/2`。Fresh execution passes 66/66 actionable tasks in 1m30s and the task belongs to both M6 process aggregates。Because the
cut is before `WRITING -> DURABLE` and profile-specific NCP2 behavior，it combines with the three-profile P matrix as the
BookKeeper service/profile P/C evidence for KF-META-007/KF-META-012/KF-APP-014。The rows remain `PLANNED` under the manifest's
milestone/final-aggregate status policy；the remaining reason is no longer a missing BookKeeper in-flight cut。

Current multi-controller evidence（product/fork 2026-07-29）：
`f9MultiControllerFailoverProcessIntegrationTest` starts three combined broker/controller release JVMs with voter IDs
`[1,2,3]`、one shared Oxia/S3 authority and isolated process directories。Before the fault it requires exact broker/voter
sets，captures the current controller ID and epoch，waits for that controller's exact-epoch Nereus reconciliation marker and
reads ACTIVE/readiness directly from Oxia。The RF1 user partition is assigned to a different combined node，then offset 0 is
produced/fetched。After the active controller is forcibly killed，the surviving two voters must elect a different controller
at a strictly higher epoch and emit a second exact-epoch reconciliation marker；voter IDs stay `[1,2,3]`，the activation
record remains equal，and readiness epoch cannot regress。The surviving bootstrap produces/fetches offset 1，returns
earliest/latest `0/2` and retains a positive S3 object count。Fresh direct execution passes 64/64 actionable tasks in 36s and
is included in `phase9M6KafkaProcessCheck`。This supplies the ACTIVE steady-state P/C subset of KF-OPS-005。

`f9ActivationCutFailoverProcessIntegrationTest` adds four store-publication boundary cuts in isolated
three-dedicated-controller/one-broker clusters。A test-only agent is installed on all controllers but armed only on the
direct-controller-Admin-observed active leader。It blocks before or after the real Oxia store for both
`createActivation(PREPARED)` and `compareAndSetActivation(...ACTIVE)`；before-provider requires `blocked` without provider
application，while after-provider requires `applied` after the real future succeeds。Oxia must expose the expected activation
absent/PREPARED/ACTIVE state plus readiness，and that leader must not yet have emitted reconciliation success when forcibly
killed。A different higher-epoch controller must reuse the exact readiness tuple、resume the same prepared facts to ACTIVE or
observe byte-identical ACTIVE，keep readiness non-regressing for broker `[4]` and emit its exact-epoch reconciliation marker。
The broker then passes native offset-0 Produce/Fetch/ListOffsets `0/1` and positive Object count。Fresh
`--rerun-tasks` execution passes 75/75 actionable tasks in 2m02s and is included in `phase9M6KafkaProcessCheck`。
KF-OPS-005 remains `PLANNED` because initial-proof/readiness、actual provider/transport-error and final aggregate cuts remain
open。

Current deterministic M4 fork evidence（local `ec7f0db991` + `032974067c`）：`NereusProducerStateManagerTest`、
`NereusKafkaRecoveryStateCodecTest` and `NereusUnifiedLogFactoryTest` cover the deterministic portions of
KF-TXN-002/003/005/006/007 and KF-FET-006/007：five-batch/sequence-wrap checkpoint equality、full seven-section hydration
plus committed tail、open/abort LSO、stock transaction verification、HW publication、READ_COMMITTED bounds and
actual-page aborted filtering。The stock `ProducerStateManagerTest` also locks marker-updated timestamp restore。
`ReplicaManagerTest` now covers the deterministic/Kafka-fork part of KF-TXN-010 by deferring the stock transactional append
and TV2 marker through the configured storage executor。`BrokerMetadataPublisherTest` and
`NereusTopicDeltaLifecycleTest` cover the deterministic ordering part of KF-TXN-011/012：both elections wait for ready，
and transaction-state ready waits for exact recovered storage installation。The real process gate now provides partial
R/P evidence for KF-TXN-011/012/013/014：both internal topics recover before elections、a real group resumes its committed
offset after restart，and the same transactional ID commits a new transaction after the transaction coordinator reloads。
The extended gate also leaves one transaction open at a stable data batch，forcibly kills the broker and proves a fresh JVM
resolves it with an ABORT marker before accepting the next transaction；read-committed and the group skip the aborted data。
Rows stay `PLANNED` because their required BookKeeper/profile service matrix、multi-broker takeover、
checkpoint/virtual-segment and mandatory NTC2 failure cuts plus aggregate tiers have not run。The fork commits are published in
`nereusstream/kafka:nereus/future9-native-kafka-storage@df238bb387`。

Current deterministic M5 retention fork evidence（local `4c060aec89` + `feabf6c686` + `378e9f8967`；product
`3eb6b63` + `57dcf35`）：stock DeleteRecords normalization/capture invokes the shared checkpoint-before-trim path；
the fork exposes bounded owned writable partitions and the same `NereusUnifiedLog` local updater；the product schedules
non-overlapping maintenance；and canonical virtual segment/config/time/logical state is rebuilt from checkpoint plus
committed tail。Focused retention、Partition、UnifiedLog、dynamic-config、Checkstyle and SpotBugs evidence passes。Rows
remain `PLANNED` because real provider/process/restart/chaos and stock differential tiers are still missing。The Kafka
fork commits are published at `nereusstream/kafka:nereus/future9-native-kafka-storage@df238bb387`。

Current BookKeeper deletion evidence（product 2026-07-28）：
`KafkaBookKeeperStreamCoverageProofProducerTest` proves complete 64-shard Kafka binding inventory plus complete 64-shard F4
registration inventory、authoritative hint/root reload、stable `NBKKAFKASTREAM1` digest、WAL-only without object
registration、async missing-registration rejection and L0 profile-drift rejection。
`BookKeeperDeletionActivationCoordinatorTest` covers one-CAS installation、idempotent exact winner、readiness rebinding/
mid-proof drift and producer-owned proof digests；`CompositeKafkaRuntimeBackgroundServiceTest` proves materialization →
activation → retention startup、reverse close and partial-start rollback。
`f9BookKeeperLedgerDeletionProviderIntegrationTest` adds R-tier real Oxia + two-bookie evidence：BookKeeper async rollover、
NCP2 COMMITTED、terminal materialization-source release、three mandatory WAL protections RETIRED、root
`SEALED -> MARKED -> DELETING -> DELETED`、provider no-such-ledger and byte-exact NCP2 read after physical deletion。
`TerminalWorkflowMetadataRetirementTest` covers the Kafka-specific versioned logical-format/canonical-payload-format
mapping that previously vetoed terminal retirement。`f9BookKeeperWalAsyncObjectProcessIntegrationTest` adds
release-distribution P-tier evidence：one-entry rollover、metadata `DELETED`、independent-client `NoSuchLedger`、normal
first-JVM shutdown and fresh-JVM offset-0 NCP2 recovery followed by continued append/fetch/ListOffsets。This is
deterministic plus adapter/provider/process partial evidence for KF-RET-009 and KF-OPS-012/018 only。Those rows remain
`PLANNED`。The provider gate now also injects applied-delete response loss on the exact retired Kafka WAL ledger and proves
first/second absence convergence；release-process response-loss restart、multi-broker takeover and final aggregate tiers
have not run。
The same date's fresh current-slice aggregate also proves repeated materialization can replace a physically deleted
BookKeeper prefix with a committed NCP2 higher generation and combine it with a readable BookKeeper tail。The deterministic
planner reproducer and real provider deletion gate pass inside a 109/109 outer-task run；nested Kafka stock/artifact-enabled
builds pass 92/92 and 95/95 actionable tasks。This does not change any row to `PASSED_CURRENT_SOURCE`：the manifest's
required release-process response-loss restart、multi-broker takeover and final aggregate tiers are still absent。

Current checkpoint-failure quarantine evidence（product 2026-07-28）：closed V1 record/envelope and canonical key tests
cover exact partition-incarnation/object identity、immutable first-winner、reference-digest collision、raw-failure
redaction and applied-but-response-lost reconciliation。Recovery and retention tests prove persisted refs skip object I/O，
new eligible permanent failures wait for the durable write before older-root fallback，and metadata read/write failure
fails closed。The real-Oxia integration writes the record，closes/reopens the client/runtime and reloads the same audit；
`phase9M6CheckpointQuarantineCheck --rerun-tasks` composes those tests with 146/146 manifest validation and production
Object-WAL wiring。Rows remain `PLANNED` because this focused evidence does not supply the required real provider trim、
fresh broker process、takeover or aggregate tiers。

Current deterministic compaction fork evidence（product `e18bf36`；fork `58342d9dca`）：typed bounded runtime config、
one-time product composition、leader-only owned-partition registration、internal/user work classification、partition-lock
canonical/HW/LSO capture and stock `CleanedTransactionMetadata` marker pre-scan pass focused adapter/fork tests plus
Checkstyle、SpotBugs and the executable 42-commit/121-file source lock。Rows remain `PLANNED` until real-provider
fresh-process/restart/takeover and full LogCleaner differential evidence exist。

Current M6 launcher/isolation/process evidence（fork `faaffc8a75` + `3bd92c7244` + `9773c8f817` + `d23dc5c787` +
`5ebf31cde8` + `ecde6964c5`）：stock maintenance
paths compile only against `BrokerStorageManagedLog`/`PartitionLeaderAuthority`；artifact-free stock main/test compilation
and `PartitionTest` pass without Nereus classes。The artifact-only `NereusKafka` launcher selects a fresh production factory
and delegates signal/startup/shutdown/await behavior to the shared stock `Kafka.run` path；the executable server-start script
selects that class。The same launcher now passes fresh broker and controller factories；stock controller sources import only
`ControllerStorageRuntime`/context/factory seams，and the preserved three-argument `ControllerServer` constructor keeps
artifact-free Java/stock callers compatible。`NereusControllerStorageRuntimeTest` proves current-controller-only retriable
retry、leadership-loss scheduled-retry cancellation、metadata callback coalescing behind one in-flight attempt and one
non-retriable fault report process-locally per controller epoch；mapper and launcher tests prove no-I/O controller mapping and dual-factory
selection。Fork `d23dc5c787` additionally proves explicit-only `nereus.storage.version` definition、
enabled broker/controller advertisement、dedicated-controller validation、explicit enabled formatting、finalized-feature
activation wait and controller-side RF/minISR/ISR/reassignment/directory rejection。Fork `ecde6964c5` creates and validates
the authoritative cache root's KRaft V1 identity before directory registration。Focused launcher/runtime/feature suites、
complete stock `KafkaConfigTest` compilation、Checkstyle、SpotBugs and the executable 42-commit/121-file source lock pass。
`phase9M6KafkaProcessCheck` additionally builds the real release tarball and passes one combined broker/controller process
against four-shard Oxia and pinned LocalStack S3：explicit feature format、registration/activation、Admin create、acks=all
Produce、byte-exact Fetch、one committed transaction、one real group rebalance/offset commit、earliest=0/latest=3
ListOffsets、S3 object existence and SIGTERM shutdown。It then starts a fresh second JVM with the same KRaft identity，
observes a higher broker epoch，CAS-refreshes ACTIVE readiness，concurrently recovers the user partition and both coordinator
internal topics，reloads group offset 2、commits through the same transactional ID、resumes the group at visible offset 3 and
verifies earliest=0/latest=5 before another normal shutdown。A deterministic regression locks retriable read-budget
backpressure to same-cursor 10–250 ms deadline-bounded retry with one complete state publication。The gate then forcibly
kills a third JVM after open-transaction data offset 5 is stable；a fourth JVM writes ABORT marker 6 before committed
data/marker 7/8，and read-committed/group visibility advances directly to 7 with latest=9。A regression also locks
that M6 task selection republishes the current `0.1.0-f9-dev` artifact rather than reusing stale bytes。The same aggregate
also runs the two-bookie `BOOKKEEPER_WAL_ONLY` cold-restart gate and the real Oxia/LocalStack
`OBJECT_WAL_ASYNC_OBJECT` first-JVM/fresh-JVM offset 0/1 gate；both end at earliest=0/latest=2。It now also runs the
two-release-process Object-WAL singleton reassignment gate：the old broker remains alive，the new broker recovers offset 0，
and the cluster commits offset 1 after exact `[1] -> [2]` KRaft handoff with no transitional reassignment。
The aggregate now also runs the three-voter ACTIVE controller-failover gate：it observes initial/replacement exact controller
epochs and Nereus reconciliation markers、forces the active controller process down、requires immutable ACTIVE and
nondecreasing readiness from Oxia，then continues native IO to final earliest/latest `0/2`。
It additionally runs the dedicated-controller activation publication-cut gate：the exact leader is blocked before or after
PREPARED create and ACTIVE CAS，then killed；a higher-epoch controller preserves or completes the durable state before broker
`[4]` completes native IO。
KF-OPS-006/007 are `PASSED_CURRENT_SOURCE` deterministic evidence；the process gates add real cold-restart/takeover partial evidence to
KF-META-009、KF-APP-005/006、KF-FET-001/006/007/009、KF-TXN-007/011/012/013/014 and
KF-OPS-003/009/013/017，but those rows remain `PLANNED` where live preemption、timestamp/leader-epoch、
remaining provider-profile matrix、checkpoint/virtual-segment、mandatory NTC2、activation-cut/chaos or aggregate requirements are
still absent。
ACTIVE steady-state plus all four PREPARED-create/ACTIVE-CAS store-publication controller takeovers are now present；
initial-proof/readiness and actual provider/transport-error activation epoch process cuts remain open。

## 2. Machine-readable manifest target

F9-M1 implementation start 已创建 `docs/phase-9-kafka-native-storage/f9-scenarios.json`，每个 Markdown row 对应
一个 object；`checkPhase9ScenarioManifest` 当前验证 146/146 ID、required fields、status vocabulary 和 canonical
method uniqueness：

```json
{
  "id": "KF-APP-NNN",
  "milestone": "F9-M3",
  "task": "phase9M3FinalCheck",
  "testClass": "...KafkaNativeAppendRecoveryIntegrationTest",
  "testMethod": "responseLossAfterHeadCasFencesUntilExactRecovery",
  "evidenceTier": ["P", "C"],
  "requiredServices": ["kraft", "oxia", "bookkeeper", "object-store"],
  "sourceLocks": ["nereus", "kafka"],
  "status": "PLANNED"
}
```

当前 `f9M3CodecTest` 为 KF-APP-002、KF-FET-001/KF-FET-002 的 deterministic adapter-level evidence，并覆盖
KF-APP-003 的 pre-storage exact-byte/CRC invariant；`DefaultKafkaPartitionStorageTest` 还提供 KF-APP-005/006/008、
KF-APP-013 和 KF-FET-003 的 adapter state-machine evidence；`KafkaBoundedAppendExecutorTest` 为 KF-APP-011/012
提供 logical queue/byte admission、owned buffer release-once、equal-key FIFO、cross-key concurrency、
single-worker fairness、close/drain 和 cancel-does-not-cancel-task 的 deterministic partial evidence；local fork
`NereusBrokerStorageAppendExecutorTest` 再证明 exact Kafka `MemoryRecords` copy-before-return、worker-owned writable
offset-assignment view、per-partition FIFO 与
request-limit mapping，stock `ReplicaManagerTest` 证明 append/validation-stats/response callback 延后到 executor
terminal。`KafkaAppendFailureClassifierTest` 固化 fail-closed outcome/action mapping。它们不替代这些 row 要求的
Kafka fork/real-service evidence。`KafkaFetchOperationTest` 还为 KF-FET-004/005/016 提供 actual-byte minBytes、
event coalescing、deadline、one-in-flight-read、listener/read cleanup 和 callback-once 的 deterministic partial
evidence；`KafkaFetchWaveOperationTest` additionally covers the stock-compatible whole-request source boundary、
subscribe-before-read、event coalescing、deadline-final-read independent of the event safety budget and caller-cancel
isolation，including the deadline/enough-in-flight race；local fork `NereusBrokerStorageFetchExecutorTest` proves bounded
worker event reread、listener cleanup、whole-lifetime logical-cap rejection and accepted deadline completion，while stock
two-partition `ReplicaManagerTest` proves the optional path defers response、preserves order/per-partition errors and drains
the action queue。These are request-path partial evidence for KF-FET-004/005/016，but do not replace their real KRaft/process
tiers。The exact-head aggregate also proves simultaneous wakeups retain all admitted operations under the logical cap and
passes 80/80 outer、92/92 stock and 95/95 artifact-enabled tasks；`KafkaPartitionLeaderManagerTest` 为 KF-APP-014 提供
process-local higher-term takeover、late-open fencing 和
stale-resign isolation 的 deterministic partial evidence，但不替代 durable authority/real process cut；
`KafkaListOffsetsResolverTest` 为 KF-FET-009 提供 stable-snapshot earliest/latest、compressed exact-record
timestamp/max-timestamp、跨页硬预算、inspector invariant 与 mid-scan leadership fencing 的 deterministic partial
evidence；local fork `NereusRecordTimestampInspectorTest` 进一步提供 stock 4.3 compressed `MemoryRecords` exact-record
iterator、minimum-offset、buffer preservation 与 max-timestamp tie-break 证据；该测试现已包含在远端 F9 branch，
真实 process gate 又验证 earliest/latest 在一次 remote cold recovery 后从 0/1 连续推进为 0/2，但仍不替代
timestamp/max-timestamp time-index restart 与 leader-epoch lookup evidence；local fork
`NereusTopicDeltaLifecycleTest`、`ReplicaManagerTest` 与
`BrokerMetadataPublisherTest` 已为 KF-OPS-017 提供 stock-state-first recovery preparation、coordinator callback-after-open
和 `firstPublishFuture` non-readiness 的 deterministic Kafka-fork partial evidence；provider-backed BrokerServer
activation and one successful KRaft process path now pass，but per-partition failure/offline policy、internal-topic restart
及 chaos evidence 仍未实现，所以该 row 保持 `PLANNED`；fork
`NereusBrokerStorageRuntimeTest` further proves exact one-ReplicaManager binding、construction of that same
ListOffsets/topic-delta chain、disabled/optional append/fetch-executor selection and combined append/fetch/product drain；
`KafkaStorageAdmissionTest` 为 KF-OPS-014/017 提供 readiness recovery、stable pre-I/O rejection、one-winner concurrent
drain 和 irreversible drain/close 的 deterministic partial evidence；`DefaultNereusKafkaRuntimeTest` 与
`KafkaRuntimeResourcesTest` 进一步为 KF-OPS-012/014 提供 protected/deduplicated startup、timeout-view isolation、
late-start fencing、manager drain、idempotent manager-first close、owned/borrowed identity、reverse close 和 failure
aggregation evidence；`NereusKafkaRuntimeFactoryTest` additionally proves immutable assembly validation、one concrete manager、
startup deduplication、owned reverse close、borrowed dependency preservation and duplicate-identity rejection-before-transfer。
`NereusKafkaObjectWalRuntimeConfigurationTest` and the real-Oxia `NereusKafkaObjectWalRuntimeIntegrationTest` further prove the
strict Object-WAL executable-profile fence、no legacy auto-session fallback、provider graph ownership、authority leader open and
stable Produce/Fetch；the real-service path now uses public `createActivated` and proves capability resume plus ACTIVE/readiness
verification before leader IO。`KafkaActivationMetadataCodecTest` freezes activation/capability/readiness V1 bytes and rejects unknown or
non-canonical facts；`KafkaStorageActivationMetadataStoreContractTest` proves exact-key CAS、one-way ACTIVE、immutable heartbeat、
readiness monotonicity、stale-version rejection and applied-but-response-lost recovery；
`KafkaStorageActivationMetadataOxiaIntegrationTest` proves all three authorities survive real Oxia client close/reconnect。
`KafkaBrokerCapabilityPublisherTest` proves broker-epoch fact fencing、monotonic heartbeat and stop-on-first-failure；
`KafkaStorageActivationVerifierTest` freezes the canonical compatibility digest and proves exact live authority admission plus
expired readiness、provider-scope mismatch and broker-epoch drift rejection before partition IO。These are deterministic partial
evidence for KF-OPS-008/009/010/011，not process/controller completion。
`KafkaStorageActivationRuntimeTest` proves publish-before-verify、bounded wait、downstream startup ordering、heartbeat-failure admission
revocation and borrowed-scheduler ownership，adding deterministic partial evidence for KF-OPS-008/010/012/018。
`KafkaStorageFirstActivationCoordinatorTest` adds deterministic partial evidence for KF-OPS-003/004/005：empty-cluster activation、
non-destructive non-empty rejection、activation-absent-with-existing-readiness takeover、PREPARED crash resume、second-proof
cut and post-ACTIVE idempotence。The absent-activation regression requires PREPARED to retain the durable readiness metadata
offset even when the replacement controller observes a newer KRaft snapshot。
`KafkaStorageBindingAwareClusterSnapshotProviderTest` proves all 64 binding-registry shards are examined before an empty result，
any non-empty shard is projected into the activation snapshot，and an already-positive fork fact avoids a weaker rescan；the
activation-backed Object-WAL integration test proves this wrapper remains on the public production path。Kafka controller
seam/context binding and deterministic activation scheduling are now implemented at fork `d23dc5c787`；the feature/control tests
cover controller-current retry、leadership loss、in-flight coalescing、per-epoch process-local fault suppression、explicit feature
format/advertisement and single-copy controller mutation。The current three-voter process gate now covers ACTIVE-state
controller kill/reconciliation，and the dedicated-controller completion-gate process test covers before-provider and
after-provider PREPARED-create/ACTIVE-CAS cuts；initial-proof/readiness、actual transport-error、priority budgets and broader native-storage
process cuts 仍未实现；all three BookKeeper provider constructions and their first
single-node/fresh-JVM native process slices now pass，
除 KF-OPS-006/007 外 rows 保持 `PLANNED`；fork `BrokerStorageRuntimeFactoryTest` 和 stock single-node
KRaft restart 另验证 disabled no-op、enabled-without-factory fail-closed、explicit borrowed context 以及
BrokerServer stock start/drain/close compatibility；`NereusBrokerStorageRuntimeTest` additionally verifies disabled creator
isolation、typed adapter assembly、post-runtime failure rollback、drain mapping and idempotent close，但不替代 provider/
activation/process evidence；
`KafkaStorageProfilePolicyTest` 为 KF-APP-016 提供 exactly-five canonical profile、profile-default durability 和
`PROFILE_DEFAULT` completion 的 deterministic policy evidence，但不替代真实 provider/KRaft profile matrix；
`DefaultKafkaPartitionStorageManagerTest` 为 KF-META-005/019 和 KF-APP-014 提供 binding-first open plan、profile
mismatch、stale resign、drain-before-delete 与 late-open fencing 的 deterministic composition evidence，但不替代真实
Oxia/KRaft process cuts；stable-head API/metadata/core、source-validator 与 default-opener tests 为
KF-META-006/007/009 的 exact authority/head observation、descendant-bound commit reachability 和 recovery composition
提供 deterministic partial evidence；public exact-session renewal 和 partition scheduler tests 进一步覆盖 renewal token
传播、strict monotonic validation、failure fencing、leadership-loss publication 与 queued-append drain，但 fork runtime
wiring 与 real process cuts 尚未闭合。各 row 状态仍不标记为完整通过。

Aggregator validates unique ID、exact class/method、executed-not-skipped status、source commits、service fixture and artifact
hash。Markdown/JSON ID sets must match。

## 3. Source, scope and compatibility

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-SRC-001 | AutoMQ checkout HEAD/version equals locked `1c648d...` / `3.9.0-SNAPSHOT` | `Phase9SourceLockTest` | D | M0/M3 |
| KF-SRC-002 | every AutoMQ reference file blob matches document 01 | `Phase9SourceLockTest` | D | M0/M3 |
| KF-SRC-003 | Nereus locked API/reader/materialization blobs drift is reported，not silently accepted | `Phase9SourceLockTest` | D | M1 |
| KF-SRC-004 | production Kafka fork exact upstream/fork commits and relevant signatures match reviewed lock | fork `NereusSourceLockTest` | D,K | M3 |
| KF-SRC-005 | Nereus inject markers are balanced/narrow and no reflection bypass exists | fork `NereusForkMarkerTest` | D,K | M3 |
| KF-SRC-006 | F5 KoP payload/binding/coordinator records cannot be opened as F9 | `KafkaTrackIsolationTest` | D | M2 |
| KF-SRC-007 | disabled mode loads no Nereus runtime/classes with side effects and passes stock focused suite | fork `NereusDisabledCompatibilityTest` | K | M3/M7 |
| KF-SRC-008 | AutoMQ `elasticstream.enable=true` and F9 enabled are rejected as conflicting modes | fork `NereusKafkaConfigValidatorTest` | D,K | M6 |

## 4. Ranged append/read and physical formats

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-API-001 | legacy OPAQUE one-entry/one-offset constructors/results remain exact | `RangedAppendApiTest` | D | M1 |
| KF-API-002 | Kafka ranged entries accept positive counts and exact sum/count/order | `RangedAppendApiTest` | D,M | M1 |
| KF-API-003 | zero/negative/overflow/mismatched record counts fail before IO | `RangedAppendApiTest` | D,M | M1 |
| KF-API-004 | concatenated payload CRC32C covers exact entry order and detects mutation | `RangedAppendApiTest` | D,M | M1 |
| KF-API-005 | old `append` delegates none precondition；old provider rejects non-empty new semantics explicitly | `StreamStorageCompatibilityTest` | D | M1 |
| KF-API-006 | expected start equal succeeds；lower/higher produces OFFSET_CONFLICT with no physical append | `ConditionalAppendContractTest` | D,R | M1 |
| KF-API-007 | exact result matches requested range/count/bytes across every storage profile | `ConditionalAppendContractTest` | D,R | M1 |
| KF-API-008 | EXACT_START at entry start works；inside ranged entry fails，never skips to next | `RangedReadContractTest` | D,M | M1 |
| KF-API-009 | CONTAINING_ENTRY at first/middle/last returns full exact range/payload | `RangedReadContractTest` | D,M | M1 |
| KF-API-010 | first-entry overflow can exceed records/bytes exactly once；second entry cannot | `RangedReadContractTest` | D,M | M1 |
| KF-API-011 | containing semantics propagate across resolved slices and only global first entry overflows | `RangedReadContractTest` | D,R | M1 |
| KF-API-012 | WAL writer/reader preserves mixed entry counts and Kafka payload format | `WalRangedEntryRoundTripTest` | D,R | M1 |
| KF-API-013 | BookKeeper reader has exact parity with Object WAL for boundaries/limits | `BookKeeperRangedReadIntegrationTest` | R | M1 |
| KF-API-014 | NCP2 dense rows preserve start/count/payload/CRC and reject every schema/count corruption | `Ncp2GoldenAndCorruptionTest` | D,R | M1 |
| KF-API-015 | NTC2 sparse rows/coverage/key tags/control dispositions round-trip and reject corruption | `Ntc2GoldenAndCorruptionTest` | D,R | M1 |
| KF-API-016 | V1 bytes remain readable；V1/V2 exact dispatch never guesses/reinterprets | `CompactedFormatCompatibilityTest` | D,R | M1 |

## 5. Binding, metadata, authority and checkpoint

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-META-001 | Kafka key builders/parsers canonical round-trip and reject wrong cluster/depth/encoding | `KafkaPartitionKeyspaceTest` | D,M | M2 |
| KF-META-002 | topic name reuse with new topicId creates a different root/stream；old data cannot alias | `KafkaBindingLifecycleTest` | D,R | M2 |
| KF-META-003 | two brokers racing absent binding converge to one deterministic stream | `KafkaBindingRaceIntegrationTest` | R,C | M2 |
| KF-META-004 | every create response-loss cut reloads/converges without extra stream | `KafkaBindingRaceIntegrationTest` | R,C | M2 |
| KF-META-005 | binding profile/mapping/authority attributes are immutable and mismatch becomes CORRUPT | `KafkaBindingTransitionTest` | D,R | M2 |
| KF-META-006 | lower KRaft leader epoch cannot acquire/renew session | `KafkaLeaderAuthorityPropertyTest` | D,M,R | M2 |
| KF-META-007 | higher leader epoch immediately preempts live old session before TTL | product `f9MultiBrokerTakeoverProviderIntegrationTest`（real Oxia/two independent Object-WAL runtimes R）+ `f9MultiBrokerTakeoverProcessIntegrationTest`（release/KRaft singleton handoff P）+ `f9InFlightTakeoverProcessIntegrationTest`（Object provider-future freeze/pre-upload fencing C）+ `f9BookKeeperProfileTakeoverProcessIntegrationTest`（three-profile post-handoff P）+ `f9BookKeeperInFlightTakeoverProcessIntegrationTest`（Bookie-acked `WRITING` recovery/fencing C） | R,P,C | M2 |
| KF-META-008 | same leader/owner term renews only exact writer/token；different owner is fenced | `KafkaLeaderAuthorityPropertyTest` | D,M | M2 |
| KF-META-009 | same broker restart with higher broker epoch preempts same leader term | `KafkaLeaderAuthorityIntegrationTest` | R,P | M2 |
| KF-META-010 | higher broker epoch from different owner at same leader epoch is rejected | `KafkaLeaderAuthorityPropertyTest` | D,M | M2 |
| KF-META-011 | legacy caller cannot acquire authority-required Kafka stream after lease expiry | `KafkaLeaderAuthorityIntegrationTest` | D,R | M2 |
| KF-META-012 | old writer primary/protection/head CAS fails after authority preemption | product `f9MultiBrokerTakeoverProviderIntegrationTest` + `DefaultKafkaPartitionStorageTest`（old head-CAS rejection/local recovery fence R/D）+ `f9InFlightTakeoverProcessIntegrationTest`（release process already-dispatched Object writer fails at guarded-upload session revalidation P/C）+ `f9BookKeeperProfileTakeoverProcessIntegrationTest`（BookKeeper post-handoff P）+ `f9BookKeeperInFlightTakeoverProcessIntegrationTest`（old `WRITING` reservation abandoned/ledger sealed；resumed completion fenced C） | R,P,C | M2 |
| KF-META-013 | StreamHead V1 goldens decode to empty authority；V2 round-trip and old reader rejects | `StreamHeadV2CodecTest` | D | M2 |
| KF-META-014 | NKC1 all required sections/golden SHA/CRC/EOF round-trip deterministically | `KafkaCheckpointFormatTest` | D | M2 |
| KF-META-015 | NKC1 length/flag/section/checksum/duplicate/unknown-required corruption fails before unsafe allocation | `KafkaCheckpointCorruptionTest` | D,M | M2 |
| KF-META-016 | checkpoint PUT/HEAD/root-CAS response loss converges across fresh runtime | `KafkaCheckpointPublicationIntegrationTest` | R,P,C | M2 |
| KF-META-017 | newest corrupt/missing checkpoint falls back to referenced older checkpoint only | `KafkaCheckpointRecoveryIntegrationTest` | R,P,C | M2 |
| KF-META-018 | trim>0 with no sufficient checkpoint fails closed；trim=0 permits full replay | `KafkaCheckpointRecoveryIntegrationTest` | D,R | M2 |
| KF-META-019 | topic delete without leader progresses ACTIVE→DELETING→DELETED and survives every cut | `KafkaBindingDeleteIntegrationTest` | R,P,C | M2/M6 |
| KF-META-020 | 64-shard registry scanner pages strictly、recovers leases and treats hints as non-authoritative | `KafkaRegistryScannerIntegrationTest` | D,R,M | M2/M7 |
| KF-META-021 | compaction coverage root never decreases and rejects unverified generation digest | `KafkaCompactionCoverageTransitionTest` | D,M,R | M5 |
| KF-META-022 | binding/root observed offsets never override a newer stream head/trim | `KafkaBindingReconciliationTest` | D,R | M2 |

## 6. Produce, state publication and recovery

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-APP-001 | stock LogValidator assigns offsets/leader epoch before Nereus encoder | fork `NereusUnifiedLogTest` | D,K | M3 |
| KF-APP-002 | one MemoryRecords/multiple RecordBatches maps one AppendBatch/one entry per batch | `KafkaAppendBatchEncoderTest` | D,M | M3 |
| KF-APP-003 | raw batch bytes/CRC/compression/producer fields identical before and after append/read | `KafkaBatchExactBytesIntegrationTest` | D,R,K | M3 |
| KF-APP-004 | gzip/snappy/lz4/zstd/uncompressed supported formats pass exact round trip | `KafkaBatchExactBytesIntegrationTest` | R,K | M3 |
| KF-APP-005 | acks=1 response occurs only after stable head and Kafka derived state update | `KafkaNativeProduceIntegrationTest` | R,P,C | M3 |
| KF-APP-006 | acks=-1 RF1 response follows stable HW；acks=0 still commits/updates exactly once | `KafkaNativeProduceIntegrationTest` | R,P | M3 |
| KF-APP-007 | response loss after head CAS fences until exact recovery；no successor append | `KafkaNativeAppendRecoveryIntegrationTest` | P,C | M3 |
| KF-APP-008 | known-not-committed timeout leaves LEO/producer/HW unchanged and permits safe retry | `KafkaNativeAppendRecoveryIntegrationTest` | R,C | M3 |
| KF-APP-009 | post-stable producer/txn/segment derived update failure becomes known-committed fence/replay | `KafkaPostCommitFailureTest` | D,R,C | M3/M4 |
| KF-APP-010 | duplicate idempotent request returns original offsets without second StreamStorage append | `KafkaNativeIdempotenceIntegrationTest` | R,K | M4 |
| KF-APP-011 | append executor queue/byte rejection occurs before IO and releases owned buffer | fork `NereusProduceBufferTest` | D,M | M3 |
| KF-APP-012 | client disconnect/cancel after enqueue cannot cancel uncertain append；callback/buffer terminal once | fork `NereusProduceBufferTest` | D,R,C | M3 |
| KF-APP-013 | different partitions run concurrently，same partition never reorders under saturation | `KafkaAppendExecutorIntegrationTest` | D,R,M | M3 |
| KF-APP-014 | leader takeover during old in-flight append leaves only current-term publication | product provider gate（post-takeover stale append fenced R）+ `f9MultiBrokerTakeoverProcessIntegrationTest`（Object post-handoff P）+ `f9InFlightTakeoverProcessIntegrationTest`（Object `jcmd` provider-future proof、SIGSTOP takeover、pre-upload fence、no WAL/LEO mutation C）+ `f9BookKeeperProfileTakeoverProcessIntegrationTest`（three-profile post-handoff P）+ `f9BookKeeperInFlightTakeoverProcessIntegrationTest`（common BookKeeper Bookie-acked/`WRITING`/physical-entry proof、new-owner `ABANDONED`/`SEALED`、stale completion fence C） | P,C | M3 |
| KF-APP-015 | broker kill before/after each append cut recovers exact LEO/HW/bytes | `KafkaNativeProcessCutIntegrationTest` | P,C | M3/M7 |
| KF-APP-016 | each claimed Nereus storage profile passes identical Produce correctness contract | `KafkaNativeProfileMatrixIntegrationTest` | R,P | M3/M7 |

## 7. Fetch, views and offset APIs

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-FET-001 | committed Fetch returns exact Kafka batches and respects response/partition byte budgets | `KafkaNativeFetchIntegrationTest` | R,K | M3 |
| KF-FET-002 | fetch offset inside compressed batch returns containing batch without split | `KafkaNativeFetchIntegrationTest` | D,R,K | M3 |
| KF-FET-003 | first batch greater than max bytes progresses only when Kafka minOneMessage allows | `KafkaNativeFetchLimitTest` | D,R,K | M3 |
| KF-FET-004 | minBytes waits on actual bytes，stable append wakes，maxWait returns once | fork `NereusFetchOperationTest` | D,R,M | M3 |
| KF-FET-005 | multi-partition fetch coalesces events，has at most one read in flight and callback once | fork `NereusFetchOperationTest` | D,M,R | M3 |
| KF-FET-006 | READ_UNCOMMITTED upper bound HW and READ_COMMITTED upper bound LSO | fork `NereusUnifiedLogFactoryTest`（D/K partial）；`KafkaNativeIsolationIntegrationTest`（R pending） | R,K | M4 |
| KF-FET-007 | aborted transaction list/filter matches Kafka baseline | fork `NereusUnifiedLogFactoryTest` bounded two-abort page（D/K partial）；`KafkaNativeIsolationIntegrationTest`（R pending） | R,K | M4 |
| KF-FET-008 | request below durable logStart is out-of-range；mid-batch trim hides prefix | product `KafkaDeleteRecordsCoordinatorTest` + fork `PartitionTest`/`NereusUnifiedLogFactoryTest`（D/K partial）；`KafkaNativeDeleteRecordsIntegrationTest`（R pending） | R,K | M5 |
| KF-FET-009 | ListOffsets earliest/latest/max timestamp/timestamp lookup match baseline | product `KafkaDerivedIndexStateCodecV1Test` + fork `NereusCanonicalLogStateTest`/`NereusUnifiedLogFactoryTest`（derived index/real position/bounded payload lookup D/K partial）；`NereusKafkaNativeProcessIntegrationTest`（earliest/latest before/after cold restart R/P partial）；`KafkaNativeListOffsetsIntegrationTest`（timestamp/max-timestamp restart pending） | R,K | M3/M4 |
| KF-FET-010 | leader-epoch end-offset lookup survives checkpoint/restart/takeover | product `KafkaLeaderEpochStateCodecV1Test`（canonical bytes partial）；`KafkaNativeLeaderEpochIntegrationTest`（restart/takeover pending） | R,P,K | M4 |
| KF-FET-011 | compacted sparse holes advance by source coverage without loop/phantom row | `KafkaCompactedFetchIntegrationTest` | D,R | M5 |
| KF-FET-012 | mandatory compacted prefix switches exactly once to committed tail at coverage end | `KafkaCompactedFetchIntegrationTest` | D,R,K | M5 |
| KF-FET-013 | missing/corrupt newest NTC2 uses verified same-view fallback only | `KafkaCompactedNoResurrectionIntegrationTest` | R,P,C | M5 |
| KF-FET-014 | no healthy mandatory NTC2 fails closed and never exposes COMMITTED source | `KafkaCompactedNoResurrectionIntegrationTest` | R,P,C | M5 |
| KF-FET-015 | NCP2 may replace COMMITTED primary for recovery/fetch without changing Kafka bytes/offsets | `KafkaNcp2CommittedViewIntegrationTest` | R,P | M5 |
| KF-FET-016 | broker shutdown cancels/wakes fetch operations，unregisters listeners and releases buffers | fork `NereusFetchOperationTest` | D,P,C | M6 |

## 8. Producer, transactions and coordinators

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-TXN-001 | producer epoch/sequence validation matches selected Kafka baseline | fork `NereusProducerStateCompatibilityTest` | M,K | M4 |
| KF-TXN-002 | sequence wrap and retained duplicate-batch window survive checkpoint/replay | product `KafkaProducerStatePropertyTest` + fork `NereusProducerStateManagerTest`（R pending） | M,R,K | M4 |
| KF-TXN-003 | canonical producer snapshot encode/decode/replay equals full replay state | product `KafkaProducerStatePropertyTest` + fork `NereusProducerStateManagerTest`/`NereusKafkaRecoveryStateCodecTest` | D,M | M4 |
| KF-TXN-004 | producer expiration and checkpoint-before-trim preserve subsequent validation | `KafkaProducerTrimIntegrationTest` | R,K | M4/M5 |
| KF-TXN-005 | open transaction crossing checkpoint restores first unstable offset/LSO | fork `NereusProducerStateManagerTest`/`NereusKafkaRecoveryStateCodecTest`（D partial）；`KafkaTransactionRecoveryIntegrationTest`（R/P pending） | R,P | M4 |
| KF-TXN-006 | commit marker stable append advances LSO in stock order | fork `NereusUnifiedLogFactoryTest`（abort-marker stock order D/K partial）；`KafkaTransactionRecoveryIntegrationTest`（R pending） | R,K | M4 |
| KF-TXN-007 | abort marker builds exact aborted index and read-committed filtering | fork `NereusProducerStateManagerTest`/`NereusUnifiedLogFactoryTest`（D/K partial）+ product `NereusKafkaNativeProcessIntegrationTest`（forced-exit abort/read-committed R/K partial；BookKeeper profile pending）；`KafkaTransactionRecoveryIntegrationTest` | R,K | M4 |
| KF-TXN-008 | crash after transactional data/marker commit before derived update replays correctly | `KafkaTransactionProcessCutIntegrationTest` | P,C | M4 |
| KF-TXN-009 | transaction spanning virtual segments/checkpoint/takeover remains atomic to consumers | `KafkaTransactionProcessCutIntegrationTest` | P,K | M4 |
| KF-TXN-010 | transaction verification guard survives async executor handoff without request-thread local use | fork `ReplicaManagerTest.testStorageAppendExecutorPreservesTransactionGuardAndMarkerVersion`（D/K partial；R pending） | D,R,K | M4 |
| KF-TXN-011 | `__consumer_offsets` opens/replays before group coordinator election | fork `BrokerMetadataPublisherTest.testAsyncTopicLifecycleDefersInternalCoordinatorElectionsUntilLeaderReady`（D partial）+ product `NereusKafkaNativeProcessIntegrationTest`（single-node graceful restart R/P partial；kill/failure cut pending） | P,C | M4 |
| KF-TXN-012 | `__transaction_state` opens/replays before transaction coordinator election | fork `BrokerMetadataPublisherTest.testAsyncTopicLifecycleDefersInternalCoordinatorElectionsUntilLeaderReady` + `NereusTopicDeltaLifecycleTest.testLeaderCallbackWaitsForExactRecoveredStorageInstallation`（D partial）+ product `NereusKafkaNativeProcessIntegrationTest`（single-node graceful restart R/P partial；kill/failure cut pending） | P,C | M4 |
| KF-TXN-013 | group commit/rebalance/restart/takeover works with native internal topic | product `NereusKafkaNativeProcessIntegrationTest`（group commit/rebalance/fresh-JVM resume P/K partial；broker takeover pending）；`KafkaGroupCoordinatorIntegrationTest` | P,K | M4 |
| KF-TXN-014 | ongoing transaction coordinator failover resolves from internal topic | product `NereusKafkaNativeProcessIntegrationTest`（stable open transaction + forced process exit + fresh-JVM abort/next commit P/K partial；BookKeeper/multi-broker profile pending）；`KafkaTransactionCoordinatorIntegrationTest` | P,K | M4 |
| KF-TXN-015 | group offset lag does not protect user-topic retention；client observes normal reset/out-of-range | `KafkaGroupRetentionIndependenceTest` | R,P,K | M5 |
| KF-TXN-016 | mandatory internal-topic NTC2 unavailable blocks coordinator election，no full-source fallback | `KafkaInternalTopicNoResurrectionTest` | P,C,K | M5 |

Implementation note（2026-07-24）：the product-side `KafkaProducerStatePropertyTest` now covers the canonical-format and
sequence-wrap portions of KF-TXN-002/003 with a frozen digest and 200 deterministic randomized round trips。Both rows remain
`PLANNED` in the manifest until the same state is imported into stock `ProducerStateManager` and proven equal to committed
full replay；no request-path or recovery claim is inferred from codec-only evidence。

## 9. Retention and DeleteRecords

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-RET-001 | time retention over virtual closed segments matches stock predicate | product `KafkaRetentionPlannerTest` + `KafkaPartitionMaintenanceRuntimeTest` + fork `NereusCanonicalLogStateTest`（deterministic strict predicate/runtime partial）；fork `KafkaRetentionOracleTest`（stock differential pending） | D,M,K | M5 |
| KF-RET-002 | size retention uses exact Kafka logical bytes and matches stock predicate | product `KafkaRetentionPlannerTest` + fork `NereusCanonicalLogStateTest`/`NereusUnifiedLogFactoryTest`（deterministic logical-byte prefix/facade partial）；fork `KafkaRetentionOracleTest`（stock differential pending） | D,M,K | M5 |
| KF-RET-003 | combined policies choose monotonic next-segment boundary，never active segment | product `KafkaRetentionPlannerTest` + `KafkaPartitionMaintenanceRuntimeTest`（deterministic planner/scheduler partial）；fork `KafkaRetentionOracleTest`（pending） | D,M | M5 |
| KF-RET-004 | insufficient checkpoint blocks trim；new checkpoint then permits exact candidate | product `KafkaTrimBarrierTest`（port-level partial）；`KafkaRetentionBarrierIntegrationTest`（real publication pending） | R,C | M5 |
| KF-RET-005 | trim response loss reloads durable head and completes idempotently | product `KafkaTrimBarrierTest`（port-level partial）；`KafkaRetentionBarrierIntegrationTest`（real provider pending） | R,P,C | M5 |
| KF-RET-006 | DeleteRecords at batch start/middle/end/HW maps durable logStart/low watermark correctly | product `KafkaDeleteRecordsCoordinatorTest` + fork `PartitionTest`/`NereusUnifiedLogFactoryTest`（exact target/HW/local publication D/K partial）；`KafkaNativeDeleteRecordsIntegrationTest`（real-provider matrix pending） | R,K | M5 |
| KF-RET-007 | retention/config/new-append races revalidate and never over-trim | `KafkaRetentionRacePropertyTest` | M,R,C | M5 |
| KF-RET-008 | compact+delete preserves compacted visibility until logical trim passes range | `KafkaCompactionRetentionIntegrationTest` | R,K | M5 |
| KF-RET-009 | logical trim success is independent of delayed protected physical GC | product `KafkaBookKeeperStreamCoverageProofProducerTest` + `BookKeeperDeletionActivationCoordinatorTest`（activation D partial）+ `f9BookKeeperLedgerDeletionProviderIntegrationTest`（real Oxia/two-bookie physical delete + exact-ledger applied-delete response-loss + post-delete NCP2 read R/C partial）+ `TerminalWorkflowMetadataRetirementTest`（Kafka payload-format retirement regression）+ `f9BookKeeperWalAsyncObjectProcessIntegrationTest`（release-process physical delete + fresh-JVM NCP2 recovery P partial）；`KafkaRetentionPhysicalGcIntegrationTest`（release-process response-loss restart/multi-broker takeover pending） | R,P,C | M5 |
| KF-RET-010 | all storage profiles obey same checkpoint barrier and trim semantics | `KafkaRetentionProfileMatrixTest` | R | M5/M7 |

Implementation note（2026-07-27）：product `KafkaDerivedIndexStateCodecV1Test` provides canonical-format partial evidence
for the time-index and exact logical-byte facts consumed by KF-RET-001/002：frozen section 5/6 bytes、cross-section
segment/bounds validation、corrupt input rejection and 200 deterministic randomized round trips。
`KafkaVirtualSegmentStateCodecV1Test` and `KafkaCanonicalCheckpointStateCodecV1Test` add section 4/full-composition facts：
dense segment/config-history bytes、virtual/logical segment equivalence and per-segment index bounds。
`KafkaRetentionPlannerTest` now consumes section-4 state and covers strict time equality、exact size excess、combined prefix、
HW cap、active protection and config mismatch；`KafkaTrimBarrierTest` covers insufficient/unrooted checkpoint blocking、
config revalidation、normal trim、applied response loss and unapplied failure；
`KafkaRetentionCheckpointGateTest` covers newest sufficient selection、closed corruption fallback、stable-end publication
and transient-failure pause；`KafkaRetentionCoordinatorTest` covers no-op、trigger coalescing/cancellation isolation and the
full deterministic planner→barrier path；`KafkaRetentionDurableTrimListenerTest` covers binding-before-local ordering、
binding CAS response loss、changed leader fencing and local-update failure after durable metadata。The existing checkpoint
publication/recovery integration gate now also verifies one exact rooted reference without implicit fallback，which is the
I/O boundary consumed by `KafkaRetentionCheckpointServices`，and performs a canonical seven-section publish→root
reload→exact verify round trip against the local-file object store。`KafkaDeleteRecordsCoordinatorTest` adds product-side
partial KF-RET-006 evidence for exact mid-segment and normalized-HW targets，already-deleted idempotence，policy/range
rejection and a KRaft config race before mutation。Fork `PartitionTest`/`NereusUnifiedLogFactoryTest` now cover stock
normalization/invocation and exact local publication；`NereusCanonicalLogStateTest` covers stable-only rolls、config roll、
derived indexes and trim pruning；`KafkaPartitionMaintenanceRuntimeTest` covers bounded periodic enumeration、
internal-topic priority、overlap coalescing and drain。Real provider/Fetch wake-up/restart evidence is still pending。
Rows remain `PLANNED` until the stock differential
oracle、real retention provider trim and restart/takeover gates pass。

## 10. Kafka compaction

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-CMP-001 | latest keyed value survives；older values removed at same offsets as oracle | product `KafkaCompactionStrategyV1Test` + `KafkaCompactionTwoPassExecutorTest`（engine partial）；`KafkaCompactionOraclePropertyTest`（oracle pending） | M,K | M5 |
| KF-CMP-002 | empty key is keyed；null key is uniquely retained；encodings cannot collide | product `KafkaTopicCompactionCodecV1Test` + `KafkaCompactionStrategyV1Test`（decode/decision partial）；`KafkaCompactionKeyEncodingTest`（engine pending） | D,M | M5 |
| KF-CMP-003 | tombstone retention/drop boundary matches Kafka delete-retention oracle | product `KafkaCompactionStrategyV1Test` + `KafkaCompactionPassOneCollectorTest`（first/later horizon partial）；`KafkaCompactionOraclePropertyTest`（oracle pending） | M,K | M5 |
| KF-CMP-004 | newer key in decision-horizon tail removes eligible older output record | product `DefaultCommittedSourceSetResolverTest` + `KafkaCompactionSourceResolverTest` + `KafkaCompactionBatchSourceTest` + `KafkaCompactionPlannerTest` + `KafkaCompactionPassOneCollectorTest` + `KafkaCompactionTwoPassExecutorTest` + `KafkaCompactionWinnerIndexTest` + `KafkaCompactionPartitionPassTest`（authoritative exact-source stream + spill winner + deterministic local-object full worker partial）；`KafkaCompactionHorizonTest`（append-race matrix pending） | D,M | M5 |
| KF-CMP-005 | append after frozen horizon can leave extra old record but never drops newest incorrectly | `KafkaCompactionHorizonTest` | D,M,C | M5 |
| KF-CMP-006 | spill/no-spill/restart produce deterministic same NTC2 SHA/rows | product `KafkaCompactionWinnerIndexTest` + `KafkaCompactionStreamingExecutorTest` + `KafkaCompactionPartitionPassTest`（restart-recomputed winner、streaming KCRS→NTC2 SHA/strict-read、durable task-rooted pass partial）；`KafkaCompactionSpillPropertyTest`（large randomized worker takeover pending） | M,R,C | M5 |
| KF-CMP-007 | all compression formats、headers、timestamps rewrite to equivalent valid records | product `KafkaTopicCompactionCodecV1Test` + `KafkaCompactionRowMapperTest`（GZIP/header/timestamp/NTC2 partial）；`KafkaCompactionRewriteTest`（full matrix pending） | D,M,K | M5 |
| KF-CMP-008 | idempotent/transactional/control/open/aborted traces match stock cleaner views | product `KafkaTopicCompactionCodecV1Test` + `KafkaCompactionPassOneCollectorTest` + `KafkaCompactionStrategyV1Test`（rewrite/decision partial）；`KafkaCompactionTransactionOracleTest`（stock trace pending） | M,K | M5 |
| KF-CMP-009 | decode ratio/key/task limits abort without publication or lost source refs | product `KafkaCompactionPassOneCollectorTest` + `KafkaCompactionTwoPassExecutorTest` + `KafkaCompactionWinnerIndexTest` + `KafkaCompactionRowSpoolTest` + `KafkaCompactionStreamingExecutorTest` + `KafkaCompactionPartitionPassTest`（bounded key/record/output limits、KCSR/KCRS cleanup and full-pass ownership partial）；`KafkaCompactionResourceLimitTest`（pressure/failure-injection matrix pending） | D,M,R | M5 |
| KF-CMP-010 | generation commit before coverage CAS is not client-visible mandatory compaction | product `KafkaCompactionPublicationCoordinatorTest` + `KafkaActivatedGenerationSetResolverTest` + `KafkaCompactedFetchIntegrationTest` + `KafkaCompactionPartitionPassTest`（write/read-side、durable-output re-entry and full local-object pass deterministic proof；real-provider process restart pending） | R,C | M5 |
| KF-CMP-011 | coverage CAS response loss reloads exact activation and never double-advances epoch | product `KafkaCompactionPublicationCoordinatorTest`（fresh response-loss + repeated durable recovery） + `KafkaCompactionPartitionPassTest`（single-pass publication/retirement composition） + `KafkaBindingTransitionTest` | R,P,C | M5 |
| KF-CMP-012 | compact→delete preserves old mandatory coverage；delete→compact activates only after verified output | `KafkaCompactionPolicyTransitionTest` | R,K,C | M5 |
| KF-CMP-013 | same-range NTC2 replacement protects readers and retires old generation after proof | `KafkaCompactionReplacementIntegrationTest` | R,C | M5 |
| KF-CMP-014 | user and both internal compacted topics pass differential/restart/takeover suite | `KafkaNativeCompactionEndToEndTest` | P,K,C | M5/M7 |

Implementation note（2026-07-27）：product `KafkaCompactionPlannerTest` adds range-only partial KF-CMP-004 facts：closed
segment/LSO bounds、strict min-lag boundary、mandatory-end resume and active/config/coverage protection；it does not yet
resolve/freeze exact sources or decide a key winner。`KafkaTopicCompactionCodecV1Test` is codec-only partial evidence for
KF-CMP-002/007/008。It proves exact one-batch range/count validation、KCK2 empty-key/null-key/control separation、owned
source SHA/base/index facts、GZIP key/value/header rewrite、transactional producer sequence preservation、abort-marker
round trip and range/source-SHA/message-format drift rejection。`KafkaCompactionStrategyV1Test` adds deterministic partial
KF-CMP-001/002/003/004/008 evidence for latest/older keyed decisions、unique null-key retention、aborted/open transaction
handling and full-scan-proven tombstone/control-marker equality boundaries。
`KafkaCompactionPassOneCollectorTest` now adds dense full/output fact SHA、aborted-winner
exclusion、open-tail/crossing and first-pass horizon evidence；`KafkaCompactionTwoPassExecutorTest` replays real Kafka
batches twice，verifies canonical exact source sets/targets、rejects byte-equivalent target re-resolution、emits sparse NTC2
rows and proves the task/result coverage plus NTC2 writer-metadata binding，while
`ExactSourceSetCodecV1Test`/`KafkaCompactionPlanCodecV1Test` freeze byte-stable EXS1/KCP1 target/task/transaction facts and
`KafkaCompactionPlanMetadataStoreContractTest` plus the real-Oxia metadata scenario cover the immutable KCP1 attachment；
`KafkaCompactionPlanCoordinatorTest` proves plan-first publication、pre-task exact KCP1 reread、missing-plan rejection and
task-ID restart recovery；`DefaultCommittedSourceSetResolverTest`/`KafkaCompactionSourceResolverTest` add bounded exact
COMMITTED index tiling、per-generation reread、append-safe/trim-unsafe authority revalidation and deterministic output-task
derivation；`ExactSourceSetBatchPublisherTest`/`KafkaCompactionBatchSourceTest` add exact-identity backpressure streams for
both recovered KCP1 passes，including demand-exhausted completion and substituted-generation rejection；
`KafkaCompactionWinnerIndexTest` adds KCSR sorted-spill/no-spill winner equivalence、two independent restart
recomputations、newer decision-tail suppression、same-length sealed-run corruption detection and cancellation permit/file
cleanup；`KafkaCompactionRowSpoolTest` freezes KCRS single-demand/EOF-accounting/corruption/cleanup behavior；
`KafkaCompactionStreamingExecutorTest` compares reference and streamed NTC2 SHA，runs non-empty/empty local upload plus
strict verification and proves decision-pass cancellation releases KCSR；`GenerationIndexPublicationTest` also proves the
empty survivor can be durably published as a zero-row TOPIC_COMPACTED generation while COMMITTED remains non-empty；
`KafkaCompactionGenerationSetTest` freezes the gap-free committed-index identity digest，and
`KafkaCompactionPublicationCoordinatorTest` composes task claim/heartbeat、guarded local upload、full NTC2 verification、
Generation commit and binding CAS while injecting PUT/CAS response loss plus changed-basis failure；it additionally resumes
from a durable output after activation interruption without a staging file and proves repeated recovery recognizes the exact
already-activated coverage without a second coverage CAS；
`KafkaCompactionTerminalRetirerTest` covers task-first/plan-second
ordering、both response-loss cuts and exact-root fail-closed behavior。`Ncp2Ntc2GoldenAndCorruptionTest` freezes non-empty
tombstone bytes。`KafkaActivatedGenerationSetResolverTest`/`KafkaCompactedFetchIntegrationTest`/
`KafkaCompactedNoResurrectionIntegrationTest` prove unique binding-digest discovery、generation-constrained sparse reads and
mandatory-prefix/committed-tail routing；`KafkaCompactionPlanMetadataStoreContractTest`/real-Oxia metadata scenario now
cover partition-scoped plan pagination，and `KafkaCompactionPlanOrphanScannerTest` covers grace/live-task/final-authority/
delete-response-loss cuts。`KafkaCompactionSchedulerTest` proves startup/fixed-delay、one-active/one-pending coalescing、
caller-cancellation isolation and borrowed-resource close semantics。`KafkaCompactionPartitionPassTest` now composes a durable
PLANNED task/KCP1、exact two-pass streams、actual Parquet NTC2 writer/reader、local-file object upload、Generation publication、
binding coverage CAS and task-first/KCP1-second retirement；the pass itself also owns bounded existing-work recovery、
claim heartbeat、typed failure persistence and activation-set reconstruction。`KafkaCompactionRuntimeTest` adds deterministic
bounded owned-partition enumeration、internal-first ordering、stale-epoch skip、cross-partition concurrency、attempt-all
failure aggregation and drain-waits-accepted-work evidence；`DefaultNereusKafkaRuntimeTest` proves background start gates
readiness and background drain precedes partition-manager shutdown。`DefaultCommittedSourceSetResolverTest` and
`GenerationIndexPublicationTest` now prove canonical projection-free direct-stream authority while preserving the default
projection-required path；`GenerationPublicationFailureInjectionTest` proves caller partition authority loss leaves the
Generation `PREPARED` and invisible immediately before the final CAS；
`KafkaGenerationProtocolActivationGuardTest`/`KafkaPartitionLifecycleCoordinatorTest` prove exact direct registration plus
ACTIVE/readiness admission/revalidation before an ACTIVE Kafka binding is returned。
`KafkaCompactionProductionRuntimeFactoryTest` now proves that the complete Object-WAL compaction graph can be constructed，
starts its bounded owned-partition source and drains cleanly；`NereusKafkaRuntimeFactoryTest` proves the background graph is
late-bound to the exact product partition manager and is started/closed under runtime readiness/drain ownership；
`NereusKafkaCompactionRuntimeConfigurationTest` freezes the cross-limit and private staging-path bounds，while
`KafkaCompactionBatchSourceTest` proves recovered KCP1 `streamId` selects the exact dynamic reader。Product
`DefaultKafkaPartitionMaintenanceTest` and fork mapper/bridge/`NereusUnifiedLogFactoryTest`/`PartitionTest` now prove
ACTIVE binding/source validation、bounded current-leader registration、partition-lock canonical capture and stock
`CleanedTransactionMetadata` marker pre-scan。Provider fresh-process restart and broad stock `LogCleaner` differential
comparison remain absent，so
end-to-end rows remain `PLANNED`。The production-graph、durable-output and full-pass tests are deterministic local
composition/restart evidence only；the real-provider fresh-process restart tier remains absent。

## 11. Configuration, activation, controller and operations

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-OPS-001 | every config default/bound/static rule and secret redaction is executable | `NereusKafkaStorageConfigTest` | D,M | M6 |
| KF-OPS-002 | KRaft-only、remote-log/cleaner/conflicting mode/message-limit violations reject before IO | fork `NereusKafkaConfigValidatorTest` | D,K | M6 |
| KF-OPS-003 | empty cluster first activation PREPARED→ACTIVE succeeds with exact brokers/digests | `KafkaActivationIntegrationTest` | R,P,C | M6 |
| KF-OPS-004 | any topic/internal topic/local authoritative log/binding makes first activation fail non-destructively | `KafkaActivationIntegrationTest` | R,P | M6 |
| KF-OPS-005 | controller failover at every activation cut preserves one-way state | `KafkaActivationControllerFailoverTest`；product `f9MultiControllerFailoverProcessIntegrationTest` adds ACTIVE steady-state P/C；`f9ActivationCutFailoverProcessIntegrationTest` adds before-provider/after-provider PREPARED-create/ACTIVE-CAS P/C | P,C,K | M6 |
| KF-OPS-006 | controller enforces RF=1/minISR=1 for create/create-partitions/manual assignment | fork `ReplicationControlManagerTest.testNereusStorageFeatureGatesTopicCreationAndPartitionGrowth` | D,K | M6 |
| KF-OPS-007 | ISR/reassignment/directory APIs cannot create follower/local-placement semantics | fork `ReplicationControlManagerTest.testNereusStorageFeatureGatesIsrReassignmentAndDirectories` + `testNereusStorageFeatureAtomicallyHandsOffSingletonReplica`；product `f9MultiBrokerTakeoverProcessIntegrationTest` adds P evidence | D,K | M6 |
| KF-OPS-008 | missing/mismatched/expired broker capability excludes leader ownership | `KafkaCapabilityReadinessIntegrationTest` | R,P,C | M6 |
| KF-OPS-009 | compatible rolling restart/new broker epoch reacquires readiness/leadership | `KafkaRollingRestartIntegrationTest` | P,K | M6 |
| KF-OPS-010 | unsupported binary rollback or locally disabled broker remains fenced | `KafkaRollingRestartIntegrationTest` | P,C | M6 |
| KF-OPS-011 | wrong Kafka cluster/provider scope/profile digest blocks readiness before partition IO | `KafkaCapabilityReadinessIntegrationTest` | R,P | M6 |
| KF-OPS-012 | startup failure closes only owned resources in reverse order；borrowed stay open | `NereusKafkaRuntimeLifecycleTest` | D,M | M6 |
| KF-OPS-013 | graceful/timeout/kill shutdown classifies appends，drains/cancels and next leader recovers | `KafkaRuntimeShutdownIntegrationTest` | P,C | M6 |
| KF-OPS-014 | priority/backpressure budgets pause background first and never allocate unbounded | `KafkaStorageAdmissionStressTest` | D,M,R | M6 |
| KF-OPS-015 | metrics have bounded labels，logs/admin redact payload/token/secrets，callbacks cannot reclassify IO | `KafkaObservabilitySecurityTest` | D,M | M6 |
| KF-OPS-016 | admin mutations require exact binding/leader/broker guard；read-only verify is bounded | `NereusKafkaStorageAdminTest` | D,R,C | M6 |
| KF-OPS-017 | metadata publisher waits partition recovery before internal coordinator callback | fork `NereusBrokerMetadataPublisherTest` | D,P,C,K | M6 |
| KF-OPS-018 | registry/retention/checkpoint/compaction schedulers stop and remove deadlines on close | `NereusKafkaRuntimeLifecycleTest` | D,M | M6 |

## 12. Scale, chaos and aggregate

| ID | Scenario / assertion | Planned test owner | Tier | Gate |
| --- | --- | --- | --- | --- |
| KF-SCL-001 | 16,384 bindings across 64 shards plus hot-shard pagination/restart complete without omission | `KafkaBindingScaleIntegrationTest` | R,P | M7 |
| KF-SCL-002 | 10,000 active partition state/open/close/checkpoint scheduling stays bounded | `KafkaPartitionScaleIntegrationTest` | R,P | M7 |
| KF-SCL-003 | 1,000 concurrent Produce/Fetch operations obey queue/byte/thread limits and make progress | `KafkaIoConcurrencyStressTest` | R,P,M | M7 |
| KF-SCL-004 | ranged count near Integer.MAX_VALUE uses checked metadata math without per-record allocation | `KafkaRangedCountLimitTest` | D,M | M7 |
| KF-SCL-005 | 128-source/million-record NCP2/NTC2 task respects memory/spill/source protection | `KafkaMaterializationScaleIntegrationTest` | R,C | M7 |
| KF-SCL-006 | repeated leader churn across three brokers never accepts stale-term write/publication | `KafkaLeaderChurnChaosTest` | P,C | M7 |
| KF-SCL-007 | Oxia/Object/BK/network response-loss matrix converges after fresh process restart | `KafkaProviderChaosIntegrationTest` | P,C | M7 |
| KF-SCL-008 | supported Kafka client/protocol versions pass Produce/Fetch/group/txn/admin compatibility | `KafkaClientCompatibilitySuite` | P,K | M7 |
| KF-SCL-009 | performance report records per-profile latency/throughput/recovery/resource baselines without skipped samples | `KafkaNativePerformanceGate` | P,A | M7 |
| KF-SCL-010 | clean `phase9FinalCheck --rerun-tasks` maps every Markdown/JSON ID to one passing result and exact sources | `Phase9EvidenceAggregatorTest` | A | M7 |

## 13. Coverage audit

The aggregate must additionally prove：

- every public/durable class in documents 02–06 appears in at least one test owner；
- every lifecycle transition has success、condition-lost、response-lost and restart evidence where applicable；
- every ErrorCode/AppendOutcome mapping has an assertion at Kafka response and partition-state levels；
- every physical format/metadata codec has golden + corruption fixture；
- every claimed profile/provider has at least one real-service multi-process path；
- every stock file injection has disabled-mode and enabled-mode focused coverage；
- no scenario is marked passed by a test disabled through assumption/environment absence。

## 14. Planned row count and status transition

This design defines 146 planned IDs：8 source、16 API、22 metadata、16 append、16 fetch、16 transaction、10 retention、
14 compaction、18 operations and 10 scale/aggregate。Before implementation，a link/audit script must compute the count from
the actual tables rather than trusting this sentence；if IDs change，both count and manifest change in the same commit。

Status progression：

```text
PLANNED -> IMPLEMENTED_NOT_RUN -> PASSED_CURRENT_SOURCE
                         \-----> FAILED / BLOCKED_ENVIRONMENT
```

Only `PASSED_CURRENT_SOURCE` from the clean aggregate satisfies release。Historical passing evidence with a different source
lock remains audit history，not current gate。
