# 06 — Runtime, Configuration, Rollout and Observability

> 状态：Implementation in progress；58-key Kafka ConfigDef、immutable typed snapshot、enabled-only pure startup validation、adapter runtime/admission + activation-backed Object-WAL provider/checkpoint-pinned recovery lifecycle、activation/capability/readiness durable records and Oxia CAS store、broker publisher/verifier、controller-side first-activation coordinator、generic BrokerServer seam、typed mapping/deferred Kafka context/provider composition、runtime-owned authoritative log-shell factory、synchronous UnifiedLog correctness bridge and bounded Produce request-path handoff implemented；Kafka controller scheduling、CLI/KafkaRaftServer production selection、bounded Fetch request path、BookKeeper/async providers and observability remain target；F9-M6
> Activation：cluster-wide、KRaft-only、new/empty cluster、one-way protocol activation
> Safe default：`nereus.kafka.storage.enabled=false`

## 1. Runtime composition

### 1.1 `NereusKafkaRuntime`

Target package：`com.nereusstream.kafka.runtime`

```java
public interface NereusKafkaRuntime extends AutoCloseable {
    CompletionStage<Void> start();
    KafkaStorageAdmission admission();
    KafkaPartitionStorageManager partitionStorageManager();
    KafkaStorageHealth health();
    CompletionStage<Void> beginDrain(DrainReason reason);
    CompletionStage<Void> awaitDrained(Duration timeout);
    @Override void close();
}
```

Adapter contract is now executable：`KafkaStorageAdmission` publishes immutable `KafkaStorageHealth` snapshots and permits
`STARTING/NOT_READY -> READY` recovery only before drain。`DRAINING` and `CLOSED` are irreversible；exactly one concurrent
`beginDrain` caller wins，late provider/start callbacks cannot reopen traffic，and `requireReady` rejects before allocation/I/O
with stable Nereus error classification。`DefaultNereusKafkaRuntime` now owns one deduplicated/protected startup operation、
publishes readiness only after its injected startup action completes、starts manager shutdown after synchronous admission
drain、returns a caller-local timeout view from `awaitDrained` and closes manager/resources once。Provider client creation and
activation/capability publication are now implemented for the synchronous Object-WAL profile；the startup action remains the
explicit downstream seam rather than hidden reflection or a global singleton。

`NereusKafkaRuntimeFactory.create` is now executable after provider construction。Its immutable
`NereusKafkaRuntimeConfiguration` freezes Nereus/Kafka cluster IDs、writer identity、session TTL/renewal interval、durable
binding-operation owner/epoch/TTL and the exact non-empty executable-profile set。`NereusKafkaRuntimeDependencies` explicitly supplies `StreamStorage`、the Kafka binding
store、borrowed renewal scheduler、a prepared recovery launcher、clock、startup action and provider resources with exact
ownership。The factory constructs one keyspace/lifecycle/opener/manager/runtime graph and one shared RecordBatch codec；it has
no Kafka server type、reflection、service loader、global registry or duplicate provider lifecycle。Broker/controller identity、
KRaft metadata view、Kafka `Time`/metrics and the mapping from the fork's 58-key snapshot remain inputs to the not-yet-built
provider/activation creator。The manager checks that set before binding lifecycle I/O，so a partial provider deployment cannot
persist an unusable binding。Independent runtimes can run in one JVM。

`NereusKafkaObjectWalRuntimeFactory` is the first concrete provider creator。It accepts an explicit provider instance rather
than a class-name lookup，verifies it against the typed `ObjectStoreConfiguration`，then constructs its `ObjectStore`、one shared
Oxia runtime、L0/physical/Kafka binding stores、object-protection manager、owned callback executor and strict generation-zero
`DefaultStreamStorage`。`NereusKafkaObjectWalRuntimeConfiguration` requires exactly
`OBJECT_WAL_SYNC_OBJECT`、matching cluster/writer/session/commit-scan facts and `autoAcquireAppendSession=false`，preventing a
missing adapter session from falling back to an unfenced legacy acquire。Bootstrap failures close every resource already
created；successful construction transfers the exact ordered list to the process runtime。The only public production creator is
`createActivated`；the unactivated path is package-private for construction failure cuts。It creates the activation store from the
same shared Oxia runtime and installs `KafkaStorageActivationRuntime` ahead of the downstream startup action：publish capability，
poll ACTIVE/readiness under both a wall deadline and a maximum-attempt bound，then continue startup。A heartbeat failure removes
admission immediately。Before each activation verification，`KafkaStorageBindingAwareClusterSnapshotProvider` enriches the
fork-owned KRaft/local-log snapshot by reading the first key from every one of the 64 durable binding-registry shards；a single
hint makes `bindingsPresent=true`，and “no bindings” is returned only after all shards prove empty。An already-positive fork fact
is preserved without scanning。This is intentionally conservative because a stale registry hint must block first activation，
while ACTIVE admission does not require the cluster to remain empty。The same product factory now constructs and owns durable
checkpoint read pins plus the checkpoint reader/verifier/recovery coordinator and bounded COMMITTED replay source；only the
fork-provided `KafkaRecoveryStateFactory`、Kafka scheduler and clock are borrowed。Runtime close cancels owned
heartbeat/poll futures before closing the activation store and provider ledger。

Kafka fork commits `46e6703761..ee608625e4` supply the stock-owned `BrokerStorageRuntimeFactory`/optional
`BrokerStorageAppendExecutor` injection boundaries and the exact create/start/metadata-lifecycle/ready/drain/close ordering。The default factory is no-op only when storage is disabled and rejects
enabled mode before LogManager construction。`NereusBrokerStorageRuntimeFactory` is the concrete adapter bridge：typed creators
return the product runtime and exact scan limits；it does not evaluate them while disabled and closes a created runtime if later
assembly fails。`NereusBrokerStorageRuntime` binds the runtime's single manager to the exact BrokerServer `ReplicaManager` and
constructs the ListOffsets/topic-delta lifecycle only at that point；it also creates one per-broker bounded append executor from
the typed append config and exposes it before ReplicaManager construction。`NereusKafkaRuntimeConfigurationMapper` now implements the
side-effect-free typed-config/broker-identity mapping。The production companion now creates a no-I/O deferred runtime，waits for
the exact registered broker epoch at `start()`，constructs the activation-backed S3/Object-WAL provider graph with borrowed Kafka
scheduler/clock，captures one KRaft image plus conservative local-log facts，and binds a one-time recovery-state factory bridge
to the exact ReplicaManager。Product-owned checkpoint/read-pin/paged replay and fork-owned fresh state construction/exact
Partition publication are executable。Kafka controller activation scheduling、CLI/KafkaRaftServer factory selection and native
log selection remain open。

### 1.2 Resource ownership

| Resource | Owner | Close rule |
| --- | --- | --- |
| `StreamStorage` | Kafka runtime when factory-created | close after all partitions drained |
| Oxia clients/stores | runtime | close after scanners/checkpoints stopped |
| ObjectStore provider | runtime | close after checkpoint/materialization/GC drained |
| BookKeeper client | runtime when selected profiles need it | close after StreamStorage/materialization |
| append/fetch/lifecycle/recovery executors | runtime | stop admission，drain，then interrupt only at final timeout |
| compaction/materialization services | runtime | stop planning，drain workers before provider close |
| Kafka `Time`/metrics/scheduler/metadata suppliers | borrowed | never close |
| request-owned buffers | individual produce/fetch operation | release exactly once at terminal callback |

`KafkaRuntimeResources` implements this ledger now。Every entry has a nonblank name、exact `AutoCloseable` identity and
`OWNED` or `BORROWED` flag。Duplicate identity is rejected even when both declarations use the same flag；mixed ownership is
therefore rejected too。`close()` is idempotent，skips borrowed dependencies，attempts every owned close in reverse list
construction order and aggregates named failures。`DefaultNereusKafkaRuntime.close()` closes the partition manager before
this provider ledger；BrokerServer still owns the outer stop-admission → await-drain → ReplicaManager → LogManager → runtime
ordering。`NereusKafkaRuntimeFactory` now appends the binding store and `StreamStorage` after caller-supplied provider resources，
so reverse close drains stream facade → binding store → underlying clients/providers。It rejects identity duplication before
ownership transfer and never closes borrowed Kafka scheduler/clock/recovery dependencies；ownership is not inferred during
shutdown。

## 2. Configuration namespace

All 58 keys below are registered in Kafka `ConfigDef` by local fork `d312e8e58d`。Primitive types、defaults、independent
hard ranges、profile dependencies and broker-local cross-field rules are executable；dynamic mutation、provider connectivity、
activation/controller enforcement and runtime resource creation remain target behavior。

### 2.1 Core and providers

| Key | Type | Default | Mutability | Validation |
| --- | --- | --- | --- | --- |
| `nereus.kafka.storage.enabled` | boolean | `false` | static | cluster-wide equal on broker/controller roles |
| `nereus.kafka.storage.cluster` | string | no default | static | non-blank canonical Nereus namespace |
| `nereus.kafka.storage.profile` | enum | `BOOKKEEPER_WAL_ASYNC_OBJECT` | activation epoch | executable/activated；immutable per created stream |
| `nereus.kafka.storage.oxia.service.address` | string | no default | static | URI/list parsed by existing Oxia config |
| `nereus.kafka.storage.oxia.namespace` | string | `default` | static | canonical provider namespace |
| `nereus.kafka.storage.object.provider` | enum | no default | static | required for Object-using profile |
| `nereus.kafka.storage.object.bucket` | string | no default | static | required/non-blank；never logged with credentials |
| `nereus.kafka.storage.object.endpoint` | string | provider default | static | URI；TLS policy validated |
| `nereus.kafka.storage.object.region` | string | provider default | static | provider validation |
| `nereus.kafka.storage.object.path.style.access` | boolean | `false` | static | provider-specific |
| `nereus.kafka.storage.bookkeeper.metadata.service.uri` | string | no default | static | required for BK profile |
| `nereus.kafka.storage.cache.dir` | path | no default | static | dedicated writable empty/ephemeral directory |

Credentials are supplied through existing provider credential mechanisms/environment/secret files，not echoed into Kafka
effective-config logs。Config mapper redacts any key matching access/secret/token/password/private-key patterns。

Allowed initial profiles after their existing activation proofs：

```text
OBJECT_WAL_SYNC_OBJECT
OBJECT_WAL_ASYNC_OBJECT
BOOKKEEPER_WAL_ONLY
BOOKKEEPER_WAL_ASYNC_OBJECT
BOOKKEEPER_WAL_SYNC_OBJECT
```

Activation record freezes allowed set/default。Existing partition binding profile never changes；online profile migration is
out of scope。A later default applies only to new bindings after a new readiness/activation epoch。

### 2.2 Append/session

| Key | Type | Default | Validation |
| --- | --- | --- | --- |
| `nereus.kafka.storage.append.timeout.ms` | long | `30000` | `1000..300000` |
| `nereus.kafka.storage.append.executor.threads` | int | `max(8,2*CPU)` capped 64 | `1..256` |
| `nereus.kafka.storage.append.executor.queue.capacity` | int | `4096` | `1..65536` |
| `nereus.kafka.storage.append.inflight.bytes` | bytes | `512 MiB` | `64 MiB..16 GiB` |
| `nereus.kafka.storage.append.request.bytes` | bytes | `128 MiB` | `1 MiB..256 MiB` and >= Kafka request limit |
| `nereus.kafka.storage.session.ttl.ms` | long | `30000` | `>= 3 * renew interval` |
| `nereus.kafka.storage.session.renew.interval.ms` | long | `5000` | `500..ttl/3` |
| `nereus.kafka.storage.session.renew.failure.grace` | int | `2` | `0..10`；never beyond durable expiry |

Queue and byte budget both gate admission。One request reserves owned buffer bytes before enqueue；release only on terminal path。
Changing timeouts/executor sizes is static in initial release。

### 2.3 Fetch

| Key | Type | Default | Validation |
| --- | --- | --- | --- |
| `nereus.kafka.storage.fetch.timeout.ms` | long | `30000` | `1000..300000` |
| `nereus.kafka.storage.fetch.executor.threads` | int | `max(16,4*CPU)` capped 128 | `1..512` |
| `nereus.kafka.storage.fetch.executor.queue.capacity` | int | `4096` | `1..65536` |
| `nereus.kafka.storage.fetch.inflight.bytes` | bytes | `1 GiB` | `64 MiB..32 GiB` |
| `nereus.kafka.storage.fetch.max.entry.bytes` | bytes | `64 MiB` | exactly <= NCP2/WAL hard limit |
| `nereus.kafka.storage.fetch.max.response.bytes` | bytes | `128 MiB` | <= Kafka/socket hard limit |
| `nereus.kafka.storage.fetch.operation.max.rereads` | int | `1024` | safety bound，deadline still primary |

Kafka `fetch.max.bytes`/partition max remain request-level lower bounds。First-entry overflow may return one batch up to 64 MiB，
never exceed format hard limit。

### 2.4 Lifecycle/recovery/checkpoint

| Key | Type | Default | Validation |
| --- | --- | --- | --- |
| `nereus.kafka.storage.lifecycle.executor.threads` | int | `8` | `1..64` |
| `nereus.kafka.storage.lifecycle.executor.queue.capacity` | int | `2048` | `1..65536` |
| `nereus.kafka.storage.recovery.executor.threads` | int | `8` | `1..128` |
| `nereus.kafka.storage.recovery.timeout.ms` | long | `900000` | `10000..3600000` |
| `nereus.kafka.storage.recovery.chunk.records` | int | `100000` | `1..1000000` |
| `nereus.kafka.storage.recovery.chunk.bytes` | bytes | `256 MiB` | `1 MiB..1 GiB` |
| `nereus.kafka.storage.recovery.warn.bytes` | bytes | `8 GiB` | positive soft threshold |
| `nereus.kafka.storage.checkpoint.interval.records` | long | `1000000` | positive |
| `nereus.kafka.storage.checkpoint.interval.bytes` | bytes | `1 GiB` | `64 MiB..1 TiB` |
| `nereus.kafka.storage.checkpoint.interval.ms` | long | `300000` | `10000..86400000` |
| `nereus.kafka.storage.checkpoint.retained.references` | int | `3` | exactly `1..3` initially |
| `nereus.kafka.storage.checkpoint.max.bytes` | bytes | `1 GiB` | <= NKC1 hard limit |
| `nereus.kafka.storage.registry.scan.interval.ms` | long | `30000` | dynamic safe；`1000..3600000` |
| `nereus.kafka.storage.registry.scan.page.size` | int | `256` | `1..1024` |

Checkpoint triggers coalesce；one partition at most one encoder/upload。A retention barrier can request an immediate checkpoint
and wait independently of periodic thresholds。

### 2.5 Retention/compaction

| Key | Type | Default | Validation |
| --- | --- | --- | --- |
| `nereus.kafka.storage.retention.check.interval.ms` | long | `300000` | dynamic safe；`1000..86400000` |
| `nereus.kafka.storage.compaction.enabled` | boolean | `true` | activation-bound；must be true for compatibility claim |
| `nereus.kafka.storage.compaction.worker.threads` | int | `4` | `1..128` |
| `nereus.kafka.storage.compaction.max.concurrent.tasks` | int | `8` | `1..256` |
| `nereus.kafka.storage.compaction.task.max.source.bytes` | bytes | `8 GiB` | `64 MiB..1 TiB` |
| `nereus.kafka.storage.compaction.task.max.records` | long | `100000000` | positive |
| `nereus.kafka.storage.compaction.key.max.bytes` | bytes | `1 MiB` | <= NTC2 hard limit |
| `nereus.kafka.storage.compaction.decode.max.uncompressed.bytes` | bytes | `1 GiB` | per task chunk hard guard |
| `nereus.kafka.storage.compaction.decode.max.ratio` | int | `100` | `1..1000` decompression-bomb guard |
| `nereus.kafka.storage.compaction.spill.dir` | path | `${cache.dir}/spill` | writable private ephemeral |
| `nereus.kafka.storage.compaction.spill.max.bytes` | bytes | `100 GiB` | positive/global budget |

Kafka topic configs `cleanup.policy`、retention、segment、compaction lag/ratio and delete-retention come from KRaft dynamic
config，not these broker defaults。This table controls engine capacity/safety only。

### 2.6 Rollout/shutdown

| Key | Type | Default | Validation |
| --- | --- | --- | --- |
| `nereus.kafka.storage.activation.required` | boolean | `true` | must stay true in production；test-only override not shipped |
| `nereus.kafka.storage.readiness.timeout.ms` | long | `300000` | positive |
| `nereus.kafka.storage.capability.heartbeat.ms` | long | `5000` | `1000..30000` |
| `nereus.kafka.storage.capability.expiry.ms` | long | `30000` | >= 3 heartbeat |
| `nereus.kafka.storage.shutdown.drain.timeout.ms` | long | `120000` | `1000..900000` |
| `nereus.kafka.storage.shutdown.checkpoint.timeout.ms` | long | `60000` | <= drain timeout or explicit warning |

### 2.7 Executable fork-to-product mapping

Local fork `94ecf8c105` freezes the first production mapper as follows；all checked arithmetic failures and unsupported values
terminate before provider I/O：

| Product field | Exact source/mapping |
| --- | --- |
| executable/default profile | exactly `OBJECT_WAL_SYNC_OBJECT`；other profiles fail closed until their creators exist |
| provider | canonical token exactly `s3` → `S3CompatibleObjectStoreProvider.class.getName()`；no class loading |
| S3 region/endpoint | configured values；otherwise `us-east-1` and `https://s3.us-east-1.amazonaws.com` |
| object prefix | `nereus/kafka/` + lowercase SHA-256 of Kafka cluster ID |
| provider timeout | `min(append.timeout, fetch.timeout)` |
| Oxia request/session | provider timeout / append-session TTL；commit-chain scan `10000` |
| writer/operation owner | `kafka-broker-{brokerId}-epoch-{brokerEpoch}` |
| operation epoch | checked `brokerEpoch + 1`；capability keeps exact unmodified KRaft epoch |
| operation TTL | `max(recovery.timeout, session.ttl)` |
| StreamStorage session | exact TTL/renew interval；`autoAcquireAppendSession=false` |
| append/read budgets | typed append/fetch byte and executor limits；object max is `min(append.request, fetch.maxEntry)` |
| ListOffsets | records/bytes from recovery chunk；read operations from fetch reread limit；target `min(1 MiB, object max)` |
| activation wait/publish | readiness timeout、capability heartbeat/expiry from rollout config |

`configCompatibilitySha256` uses a length-prefixed `nereus-kafka-config-compatibility-v1` domain and hashes profile、
append/session hard semantics、fetch hard limits、recovery/checkpoint ceilings、compaction decode/coverage ceilings and mandatory
activation。Local paths、thread/queue capacity and timeout tuning are deliberately excluded。`providerScopeSha256` separately
hashes Nereus/Kafka cluster IDs、Oxia address/namespace、provider、resolved endpoint/region/bucket/prefix and path-style flag。
`codeCapabilitySha256` hashes the exact protocol/API/session/binding/index/NCP2/NTC2/NKC1/compaction/feature tuple plus the
single executable profile。The mapper tests freeze deterministic process-independent digests、epoch-zero handling、derived S3
scope and both unsupported-profile/provider pre-I/O failures。

### 2.8 Executable Kafka context-to-provider lifecycle

Local fork `c27305a7ad..ee608625e4` consumes the mapper、log factory、stable I/O bridge and bounded Produce handoff through the following exact
call path：

```text
NereusBrokerStorageRuntimeFactory.production(recoveryStateFactoryCreator)
  -> create(BrokerStorageRuntimeContext)
  -> mapper.listOffsets(typedConfig)                 # pure, no provider I/O
  -> new NereusKafkaDeferredRuntime(...)             # no provider I/O
  -> new NereusUnifiedLogFactory(context)             # cache-root policy only
  -> BrokerServer passes runtime.unifiedLogFactory to LogManager
  -> LogManager skips local scan/maintenance in Nereus mode
  -> BrokerStorageRuntime.start()
  -> poll brokerEpochSupplier every min(25ms, remaining)
  -> NereusKafkaProductRuntimeCreator.create(exactEpoch, ...)
  -> new S3CompatibleObjectStoreProvider()
  -> NereusKafkaObjectWalRuntimeFactory.createActivated(...)
       -> own checkpoint read pins + reader/verifier/coordinator
       -> own bounded DefaultKafkaPartitionRecoveryLauncher
  -> product start publishes capability and waits ACTIVE/readiness
  -> first asyncTopicDeltaLifecycle(exactReplicaManager)
       -> bind NereusKafkaRecoveryStateFactory through one-time bridge
  -> recovered state + manager storage publish to exact NereusUnifiedLog
  -> runtime appendExecutor -> ReplicaManager
       -> request-wide byte validation + exact owned per-partition capture
       -> keyed bounded workers with RequestLocal.noCaching
  -> Partition required-acks seam -> stock validation -> stable adapter append -> shell LEO/HW
  -> aggregate original validation-stats/delayed-produce/response completion exactly once
  -> NereusUnifiedLog bounded read -> adapter Fetch assembly -> MemoryRecords
```

The broker-epoch wait deadline is the typed rollout readiness timeout。The exact epoch is captured once and passed unchanged into
the capability；the mapper derives only the separate checked operation epoch。`KafkaScheduler.scheduledExecutorService()` exposes
the already-started executor as `BORROWED`；neither the fork creator nor product runtime may shut it down。
`NereusKafkaClock` delegates `millis()/instant()` to the borrowed Kafka `Time`。

`NereusKafkaStorageClusterSnapshotProvider.currentSnapshot()` reads one immutable `metadataCache.currentImage()`，rejects a
negative provenance offset or empty broker set retriably，sorts exact `(brokerId, brokerEpoch)` identities，and derives
`topicsPresent` from that same image。It ignores only directories parsed as the KRaft metadata topic；any other directory or
unparseable directory is conservatively treated as authoritative local log data。It reports `bindingsPresent=false` because the
product-owned `KafkaStorageBindingAwareClusterSnapshotProvider` merges the 64-shard durable binding proof before activation。

`NereusKafkaDeferredRuntime.partitionStorageManager()` is available immediately so the metadata publisher can be assembled before
the first image。Leader open/resign/delete/shutdown futures wait for activation-backed runtime readiness；before dispatch they
re-check the delegate admission, so a capability-heartbeat fence cannot leak queued operations。`beginDrain()`/`close()` cancel the
owned epoch poll, fail pending manager operations closed and prevent late provider creation；a product startup failure drains and
closes every already-created provider resource。

Checkpoint selection、ObjectStore reads、durable reader pins and COMMITTED replay must be created before ReplicaManager and stay
with the product-owned provider graph；Kafka derived state cannot be created until the exact live ReplicaManager exists。The
production factory therefore injects a `Function[ReplicaManager, KafkaRecoveryStateFactory]`；the first
`NereusBrokerStorageRuntime.asyncTopicDeltaLifecycle(exactReplicaManager)` creates it and one-time binds
`NereusKafkaRecoveryStateFactoryBridge`。A second manager is rejected，and a pre-bind recovery call fails retriably。
`NereusKafkaRecoveryStateFactory` then validates exact topic ID/name/partition/current leader epoch，creates a one-shot stock
RecordBatch codec and first publishes the frozen state to the exact `NereusUnifiedLog` shell，then through
`Partition.installNereusRecoveredState`。After final source revalidation，`NereusListOffsetsLifecycle` publishes the exact
manager-returned writable storage to the same shell before installing the ListOffsets lookup。Publication is provisional until
all steps succeed；failed open cleanup calls `cancelLeaderEpochAwareOffsetLookup(epoch)` and identity-safe shell revocation，
which clear lookup admission、storage and provisional state without touching a newer epoch。Idempotent、
transaction/control and NKC1-derived sections remain M4 fail-closed boundaries。`Partition` accepts only the stock
`LeaderEpochAwareRecoveryState` interface；the artifact-only implementation remains excluded from disabled builds。

`dc8c66388a` 的 append/read bridge 使用 typed append/fetch timeout 与 fetch hard-response limit；required acks 不改变
profile completion policy。timeout/interrupt、stable-result mismatch 或 stable commit 后 stock state failure 都会
resign 当前 storage。`ee608625e4` 把 Produce 的 `UnifiedLog` caller 移到 runtime-owned bounded worker：product
executor 在 submit 返回前复制 exact bytes，以 `TopicIdPartition` FIFO、跨分区并行/公平调度，并在 close 后继续完成
已接纳任务。`NereusBrokerStorageRuntime.beginDrain` 先停止该 executor admission；`awaitDrained` 对 append-executor
termination 与 product runtime drained future 做 caller-local `allOf + orTimeout`，不取消底层 admitted append。Fetch
仍未接入 ReplicaManager-level bounded executor，所以整个路径仍不能用于 production rollout readiness。

The selected shell is not a durability shortcut：`NereusUnifiedLogFactory` uses only
`${cacheDir}/{brokerId}/partition-logs`，sets `loadExistingLogs=false` and `scheduleLocalMaintenance=false`，and rejects local
checkpoint offsets、future logs and missing/zero topic IDs。Current `appendAsLeader`/`read` require exact recovered state plus
the same manager-returned writable storage；they fail closed on missing/stale publication and never fall back to local bytes。

## 3. Cross-Kafka validation

When enabled，startup rejects before any partition IO unless：

- process uses KRaft；broker role has stable broker ID and registration epoch；
- `default.replication.factor=1`；offsets/transaction-state topic replication factors=1；
- all effective `min.insync.replicas` defaults for new topics=1；
- `remote.log.storage.system.enable=false` for Nereus partitions/cluster；
- stock `LogCleaner` is not started for Nereus mode；
- if based on the audited AutoMQ fork，`elasticstream.enable=false`；
- `message.max.bytes <= 64 MiB` and socket/request/fetch limits fit Nereus hard bounds；
- cache/spill dirs are dedicated and not Kafka authoritative log dirs；
- selected profile dependencies/configs pass existing typed provider validation；
- activation Kafka cluster ID exactly matches current KRaft cluster ID。

Controller validates RF/assignment independently from broker config using the KRaft `nereus.storage.version` feature。

## 4. Activation records

### 4.1 `KafkaStorageProtocolActivationRecord` V1

Oxia key：`.../kafka/{kafkaClusterId}/activation`。

Field order：

```text
recordVersion:int=1
lifecycleId:int                    PREPARED=1, ACTIVE=2
kafkaClusterId:string
protocolVersion:int=1
apiVersion:int=1
streamHeadSessionVersion:int=2
bindingVersion:int=1
payloadMappingId:int=1
objectWalEntryIndexVersion:int=1
ncpVersion:int=2
ntcVersion:int=2
checkpointVersion:int=1
compactionStrategyVersion:int=1
allowedStorageProfiles:list<string sorted>
defaultStorageProfile:string
requiredCapabilitySha256:32 bytes
requiredBrokerSetSha256:32 bytes
kafkaFeatureLevel:int              nereus.storage.version
preparedAtMetadataOffset:long
activationEpoch:long
preparedAtMillis:long
activatedAtMillis:long             0 while PREPARED
metadataVersion:long               hydrated
```

以上 field order 已由 `KafkaStorageProtocolActivationRecordCodecV1` 封闭并以 golden SHA-256 固定。V1 的所有 version
field 是精确值，不解释为范围；allowed profile 是 1..5 个 canonical、strictly sorted、unique 名称，legacy
`OBJECT_WAL` alias 被拒绝，default 必须属于 allowed set。PREPARED 只允许以同一不可变 tuple CAS 到 ACTIVE；ACTIVE
不能回退或在同一 activation epoch 原地修改任一 digest/profile/version/source metadata offset。

ACTIVE is one-way at a protocol version；cannot return to PREPARED or change digest/profile/version in place。Future protocol
upgrade uses a new preparation/activation epoch and explicit dual-read/write gates。

### 4.2 Broker capability record

Key includes broker ID + KRaft broker epoch，so restart cannot inherit stale readiness。Record：

```text
recordVersion:int=1
kafkaClusterId:string
brokerId:int
brokerEpoch:long
runtimeInstanceId:string
kafkaVersion:string
nereusBuild:string
javaVersion:string
protocolVersion:int
apiVersion:int
streamHeadSessionVersion:int
bindingVersion:int
payloadMappingId:int
objectWalEntryIndexVersion:int
ncpVersion:int
ntcVersion:int
checkpointVersion:int
compactionStrategyVersion:int
kafkaFeatureLevel:int
supportedStorageProfiles:sorted list
configCompatibilitySha256[32]
codeCapabilitySha256[32]
providerScopeSha256[32]
startedAtMillis:long
heartbeatAtMillis:long
expiresAtMillis:long
metadataVersion:long
```

V1 将 supported fields 实现为与 activation 完全同构的 11-field exact tuple：`protocolVersion`、
`apiVersion`、`streamHeadSessionVersion`、`bindingVersion`、`payloadMappingId`、`objectWalEntryIndexVersion`、
`ncpVersion`、`ntcVersion`、`checkpointVersion`、`compactionStrategyVersion`、`kafkaFeatureLevel`；不声称尚未实现的
range negotiation。record 还持久化 `kafkaClusterId` 和 `providerScopeSha256[32]`，使 key/value scope 与 readiness
provider proof 都可独立验证。Heartbeat CAS 只能严格增加 `heartbeatAtMillis` 和 `expiresAtMillis`，其他字段不可变。

Digest uses canonical field encoding，not JSON/property iteration order。Heartbeat only extends expiry if immutable capability
facts match；changed facts require new broker epoch/runtime record。

`KafkaStorageCapabilityDigests.compatibilitySha256` freezes the V1 readiness/activation capability digest as SHA-256 over
length-prefixed ASCII domain `nereus-kafka-capability-v1`，then big-endian exact 11-field protocol tuple、profile count、each
length-prefixed UTF-8 canonical profile、`configCompatibilitySha256[32]` and `codeCapabilitySha256[32]`。It deliberately excludes
broker identity/epoch（owned by `brokerSetSha256`）、provider scope（owned by the separate provider digest）、lease times and
human build labels，so a rolling binary with the same exact code capability can join without weakening protocol admission。
`KafkaBrokerCapabilityPublisher` creates or resumes only the same runtime/epoch facts，clamps heartbeat/expiry monotonically under
wall-clock rollback，owns its scheduled future but borrows the scheduler/store，and permanently stops plus calls the supplied
failure handler after its first failed CAS。

### 4.3 Readiness snapshot

`KafkaStorageReadinessRecord` closed field order：

```text
recordVersion:int=1
kafkaClusterId:string
readinessEpoch:long
kraftMetadataOffset:long
brokers:list<(brokerId:int,brokerEpoch:long)> strictly sorted/unique, 1..16384
brokerSetSha256:32 bytes
capabilitySha256:32 bytes
providerScopeSha256:32 bytes
createdAtMillis:long
expiresAtMillis:long
metadataVersion:long
```

`brokerSetSha256` 必须等于列表按顺序拼接 big-endian `int brokerId || long brokerEpoch` 的 SHA-256；constructor 会重算
并拒绝不匹配。Readiness CAS 必须严格增加 readiness epoch/created time，且不得倒退 KRaft metadata offset。它是
admission proof，不是 leadership truth；membership change invalidates cached readiness，new leader open reloads proof。
首次激活令 `activationEpoch == readinessEpoch`，此时 ACTIVE 的 `requiredBrokerSetSha256` 必须与 readiness 精确相等。
ACTIVE 后的兼容滚动重启发布更高 `readinessEpoch` 和新的 broker epoch set；ACTIVE 中的 broker-set digest 保留为首次
激活审计证据，不再要求等于较新 readiness。较新 readiness 仍必须匹配当前 KRaft broker set，且 capability digest、
provider scope、profile/default 和 protocol tuple 不得改变；readiness epoch 小于 activation epoch 永远拒绝。

三类 record 已注册进 `KafkaMetadataCodecs`，array accessors defensive-copy，metadata version 只由 store hydrate。
`KafkaStorageActivationMetadataStore`/`OxiaJavaKafkaStorageActivationMetadataStore` 将 activation、capability、readiness
路由到同一 deterministic activation partition，提供 create/exact-version CAS；key/value identity、cluster scope、closed
codec、monotonic transition 都在 write/read 边界 fail closed。若 Oxia 已应用 mutation 但响应丢失，store reload exact
key，仅当 durable value 等于 intended value 且 version 已前进时恢复成功；否则保留 condition/availability failure。
deterministic backend 与 real Oxia restart gates 已通过。Coordinator 仍需负责把当前 KRaft broker set、capability
aggregate/provider scope 与 PREPARED/ACTIVE digest 交叉验证。

Broker-side `KafkaStorageActivationVerifier` already implements the read-only half：it consumes a Kafka-type-free
`KafkaStorageClusterSnapshot`，requires the local broker ID+epoch in the exact sorted current set，loads ACTIVE/readiness and every
current capability，then rejects absent/expired authorities、older source offsets、feature/profile/default/provider mismatch、or any
digest divergence before partition IO。At the activation epoch it also requires the historical ACTIVE broker-set digest；at a higher
readiness epoch it admits a changed broker epoch set only when that set exactly matches current KRaft and all compatibility facts stay
unchanged。Absence/expiry/image lag are retriable `METADATA_UNAVAILABLE`；durable contradictions are
non-retriable `METADATA_INVARIANT_VIOLATION`。

Controller-side `KafkaStorageFirstActivationCoordinator` implements the write half behind the same snapshot seam。With no activation
it requires an empty first image，loads every exact broker epoch capability，derives one common capability/provider proof，creates or
CAS-refreshes readiness，and creates PREPARED from that exact readiness。It then reads a second empty image，requires non-regressing
metadata offset and an unchanged broker set，reloads all capabilities and only then CASes ACTIVE。With PREPARED it never rewrites
readiness or immutable activation facts；it only resumes verification and the final CAS。Condition losers reload and accept only a
compatible winner，including a winner already at ACTIVE。With ACTIVE it performs an idempotent policy/image check without requiring
the now-running cluster to remain empty。The Kafka controller still has to supply the snapshot and schedule this coordinator。

## 5. First activation workflow

Initial release supports only a new/empty Kafka cluster：

```text
1. deploy capable code with storage disabled/no client traffic
2. verify KRaft image has zero topics and brokers have no authoritative local topic logs
3. configure Nereus providers/profile identically
4. start brokers; publish exact capabilities
5. controller sets/prepares KRaft feature nereus.storage.version=1
6. build exact current-broker readiness snapshot
7. write PREPARED activation with digests/source metadata offset
8. re-read KRaft emptiness, broker registrations, capabilities and provider scope
9. CAS PREPARED -> ACTIVE
10. enable broker request readiness and internal/user topic creation
```

If any topic/internal topic/binding/local authoritative log exists before step 9，activation aborts。It does not auto-import or
delete。ACTIVE rollback to local storage is unsupported；restore requires cluster recovery from backups or future migration design。

## 6. Rolling broker maintenance after ACTIVE

- a restarting broker advertises same active protocol/digests under a newer broker epoch before leader eligibility；
- controller excludes missing/mismatched capability brokers from Nereus single-replica assignment；
- current leader resigns/drains，new leader gets higher KRaft leader epoch and immediately preempts session；
- broker binary rollback is allowed only if old binary supports exact ACTIVE versions；otherwise it stays fenced/not ready；
- changing provider scope/default profile requires new readiness + activation epoch，not one broker config edit；
- no broker can locally set `enabled=false` and still join ACTIVE cluster。

## 7. Startup order

```text
parse/validate config (no IO)
  -> construct providers/clients/executors
  -> connect Oxia/Object/BK and run non-destructive capability probes
  -> read ACTIVE activation and KRaft feature
  -> publish broker capability
  -> obtain current readiness proof
  -> create StreamStorage/materialization/checkpoint/scanner services
  -> construct LogManager/stock ReplicaManager with runtime-owned factories/executors
  -> apply initial KRaft image and recover assigned partitions
  -> recover internal topics, then elect coordinators
  -> mark broker traffic-ready
```

Provider probe is scoped/non-destructive except writing/reading/deleting an exact activation probe object/key in a reserved prefix
with unique attempt and protected cleanup。Failure cannot partially activate。

## 8. Shutdown order

```text
admission RUNNING -> DRAINING
  -> stop accepting Produce/new Fetch/lifecycle opens
  -> resign/fence leaders through Kafka metadata flow where available
  -> complete or classify all queued/running appends
  -> cancel/wake fetch operations
  -> checkpoint eligible stable partitions within checkpoint timeout
  -> close partition logs/sessions
  -> stop retention/compaction planning and drain workers
  -> stop registry/checkpoint/renew schedulers
  -> close StreamStorage
  -> close BookKeeper/Object/Oxia owned clients
  -> stop owned executors
  -> CLOSED
```

Drain timeout does not mark unknown append successful。On timeout runtime preserves fencing/attempt evidence and closes；next
leader replays。Borrowed Kafka scheduler/time/metrics stay open。

## 9. Admission and backpressure

`KafkaStorageAdmission` implements states：`STARTING`、`READY`、`DRAINING`、`NOT_READY`、`CLOSED`。

Checks before buffer allocation/IO：activation/readiness、partition state、authority、deadline、queue slots、byte budget and
provider circuit state。Priority classes：

1. session renew/recovery/control markers/internal coordinator writes；
2. normal Produce；
3. Fetch；
4. checkpoint/materialization/compaction/retention background。

Priority never bypasses hard memory budget or correctness checks。Background work pauses first under pressure。Internal writes
still use bounded reserved capacity；if exhausted，coordinator fails visibly rather than unbounded allocation。

## 10. Metrics

Kafka metrics group：`nereus-kafka-storage`。Prometheus/JMX adapters may rename separators but semantic names stable。

### 10.1 Bounded-label process metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `runtime_state` | gauge enum | broker |
| `activation_epoch` | gauge | broker |
| `readiness` | gauge 0/1 | broker |
| `partition_state_count` | gauge | state |
| `append_requests_total` | counter | outcome,profile |
| `append_latency_ms` | histogram | outcome,profile |
| `append_queue_depth` / `append_inflight_bytes` | gauge | none |
| `fetch_requests_total` / `fetch_latency_ms` | counter/hist | outcome,view |
| `fetch_queue_depth` / `fetch_inflight_bytes` | gauge | none |
| `session_fence_total` | counter | reason |
| `session_renew_failures_total` | counter | reason |
| `recovery_total` / `recovery_duration_ms` | counter/hist | outcome,source |
| `recovery_replay_bytes` / `recovery_replay_records` | histogram | none |
| `checkpoint_age_ms` / `checkpoint_lag_offsets` | gauge/hist aggregate | none |
| `checkpoint_total` | counter | outcome |
| `retention_trim_total` | counter | reason,outcome |
| `compaction_task_total` / `compaction_latency_ms` | counter/hist | outcome |
| `compaction_mandatory_coverage_lag` | gauge/hist aggregate | none |
| `same_view_fallback_total` | counter | format,reason |
| `corruption_total` | counter | layer,format |
| `metadata_cas_retry_total` | counter | operation |
| `executor_rejection_total` | counter | executor |

Default exporters do not label topicId/partition to avoid unbounded cardinality。Kafka per-topic metrics can expose sampled
topic-name aggregates under existing quotas。Exact partition details belong to admin diagnostics。

### 10.2 Per-partition diagnostic snapshot

`KafkaPartitionDiagnostic` read-only API returns on explicit query：identity/binding version/lifecycle、stream/profile、leader/
broker epoch、session hash/expiry、logStart/LSO/HW/LEO、checkpoint refs/age、virtual segments、mandatory compaction coverage、
in-flight operation IDs and last bounded error。No raw token、credentials、keys/values or message payload。

## 11. Structured logs and audit events

Stable event IDs：

| ID | Event |
| --- | --- |
| `NKF100` | runtime start/config digest |
| `NKF110` | capability/readiness change |
| `NKF120` | activation transition |
| `NKF200` | binding create/recover/delete transition |
| `NKF210` | leader authority acquire/preempt/fence |
| `NKF220` | unknown append fence/recovery |
| `NKF230` | partition open/replay/publication |
| `NKF300` | checkpoint publish/fallback/quarantine |
| `NKF400` | retention trim plan/result |
| `NKF410` | compaction generation/coverage activation |
| `NKF500` | corruption/offline repair decision |
| `NKF900` | drain/close timeout summary |

Fields use stable IDs/hashes and offsets；payload、Kafka key/value、fencing token、secret config never logged。Repeated errors are
rate-limited but first/last/count preserved。

## 12. Alerts

Initial target alerts：

- any `CORRUPT_OFFLINE` or mandatory NTC2 unavailable > 0；
- session renew remaining lease < 2 renew intervals；
- partition recovery > 15 min or replay > 8 GiB；
- checkpoint age > 3 intervals and retention blocked；
- append/fetch executor rejection sustained > 1% over 5 min；
- append p99 > selected SLO for 10 min；
- readiness/activation mismatch on any registered broker；
- registry scanner no successful full pass for 3 intervals；
- compaction mandatory coverage lag beyond `max.compaction.lag.ms`；
- physical GC lag is alerted separately and never treated as logical retention failure。

Thresholds configurable by deployment，but alert semantic names remain stable。

## 13. Admin/diagnostic surface

No new Kafka wire API in first release。A local authenticated admin CLI/service consumes adapter APIs：

```text
kafka-nereus-storage activation describe
kafka-nereus-storage broker readiness
kafka-nereus-storage partition describe --topic-id ... --partition ...
kafka-nereus-storage partition verify --read-only
kafka-nereus-storage partition checkpoint --if-current-leader
kafka-nereus-storage partition recover --if-fenced
kafka-nereus-storage compaction status
kafka-nereus-storage registry scan --read-only
```

Mutation commands require exact binding version、leader/broker epoch and explicit confirmation token；they call the same guarded
coordinators，never write Oxia/object keys directly。Read-only verify has strict byte/time budgets。

## 14. Runbooks

### 14.1 `WRITE_FENCED_RECOVERY_REQUIRED`

1. inspect KRaft leader/broker epoch and authority diagnostic；
2. check exact append attempt/outcome and recovery progress；
3. do not manually clear session or advance stream head；
4. allow guarded recover/reopen or transfer leadership；
5. if recurring，collect event IDs/commit/head facts without payload。

### 14.2 Checkpoint/retention blocked

1. confirm Produce/stable head remains healthy；
2. inspect object provider and checkpoint protection/root CAS；
3. verify older checkpoint fallback；
4. do not force trim without a checkpoint at/after candidate；
5. repair provider/reference then trigger guarded checkpoint。

### 14.3 Mandatory NTC2 unavailable

1. stop coordinator/client reads for affected partition；
2. try verified same-view generation fallback；
3. inspect F4 generation/object pins/protections；
4. rebuild NTC2 deterministically from lossless COMMITTED source under same coverage and CAS replacement；
5. never toggle cleanup policy or force COMMITTED fallback。

### 14.4 Metadata/Oxia unavailable

Read already pinned immutable bytes may complete within its proof；new append/leader open/checkpoint/trim fails closed。Do not use
cached leadership/session beyond durable lease。Restore metadata quorum，then reopen/reconcile。

### 14.5 Executor saturation

Pause background planners，inspect inflight latency/provider health，increase capacity only within byte budgets，and preserve
queue rejection as backpressure。Never switch to network-thread blocking as emergency workaround。

## 15. Security and tenancy

- provider connections require TLS/auth per existing adapters；
- Oxia namespace and object prefix are cluster-scoped；binding verifies exact Kafka cluster ID；
- object keys use encoded IDs，not topic names/user keys；
- payload buffers zero/release according to allocator where supported；
- diagnostic/admin endpoints authenticated/authorized/audited；
- no cross-cluster binding repair；wrong provider-scope digest blocks readiness；
- checkpoint/Parquet decoders treat bytes as hostile：bounds before allocation、strict UTF-8/flags/EOF/checksums。

## 16. Rollout tests

- config matrix for every key/default/bound/secret redaction；
- disabled stock Kafka boot and compatibility suite；
- empty-cluster activation success and every partial cut；non-empty rejection；
- controller failover during PREPARED/ACTIVE；
- rolling restart/new broker epoch/capability mismatch/unsupported rollback；
- profile/provider scope mismatch and mixed executable profiles；
- startup/shutdown at every resource cut，owned/borrowed close assertions；
- append/fetch/background saturation and priority budgets；
- metrics label-cardinality/log redaction/admin guard tests；
- real two/three-broker KRaft + Oxia + selected WAL/Object profiles。
