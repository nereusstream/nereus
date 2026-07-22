# Pulsar Runtime, Rollout, and Compatibility

## 1. Local source boundary

The integration target is the local Pulsar checkout only：

```text
/Users/liusinan/apps/ideaproject/nereusstream/pulsar
master@dfbcc8e11422c965957e3e1fcf809485e437d842
BookKeeper 4.18.0
```

There is no `M1-SNAPSHOT` dependency. Nereus Gradle gates compile/check the local fork through the existing locked
checkout mechanism.

## 2. Borrowed BookKeeper client ownership

Current `NereusManagedLedgerStorage` creates a `ManagedLedgerClientFactory`, obtains the default stock storage class,
and closes that owner at broker shutdown. The focused BK-M2 wiring now performs：

```java
ManagedLedgerStorageClass raw = localBookkeeper.getDefaultStorageClass();
BookkeeperManagedLedgerStorageClass stock =
    (BookkeeperManagedLedgerStorageClass) raw;
org.apache.bookkeeper.client.api.BookKeeper borrowed =
    stock.getBookKeeperClient();

NereusRuntimeContext context = new NereusRuntimeContext(
    ...,
    Optional.of(borrowed),
    capabilityCoordinator, // BookKeeperBrokerReadinessProvider
    capabilityCoordinator::installBookKeeperPrimaryWalCapability);
```

Exact type/wrapper names may follow module conventions. Required ownership：

- `ManagedLedgerClientFactory` is the sole owner/closer；
- Nereus may create/close ledger/read/write handles obtained from the client；
- Nereus never calls `BookKeeper.close()` and never closes broker event loops；
- partial Nereus runtime initialization closes only Nereus-owned resources, then the existing storage error path closes
  the stock factory once；
- the same broker capability coordinator receives the stable publication binding before storage initialization and
  provides live two-snapshot BK deletion readiness after registry attachment；it never transfers client ownership；
- `NereusManagedLedgerStorageBookKeeperClientTest` freezes exact instance identity plus non-BK/null rejection；the
  normal/partial-close ownership counts remain required through the production BK runtime rollout。

`nereus-api`/`nereus-core` do not import Pulsar or ManagedLedger classes. `nereus-bookkeeper` depends on BookKeeper
client API, while `nereus-pulsar-adapter` is the composition boundary.

BK-M5 checkpoints B–D now consume `borrowedBookKeeperClient` through `DefaultBookKeeperClientOperations` and install
the completed M2-M4 runtime without ever closing the client。Before advertising capability，bootstrap performs a
bounded read of the separately provisioned Oxia namespace record、validates its exact deployment/config binding and
probes the password secret reference，then reads the binding-specific activation record。An absent/PREPARED activation
keeps the runtime available for existing-target reads but publishes no BK first-create capability；partial
initialization still closes only Nereus-owned adapters/metadata views。

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

Implemented checkpoint B/C composition uses one `PrimaryWalRegistry.combine(Object, BK)` and passes the BK physical
reference adapter into generation-zero publication plus the BK materialization-source provider into the existing F4
runtime。No second planner、worker pool or lag authority is created。Checkpoint D adds the durable activation control
plane and read-side admission；checkpoint E adds the one binding-filtered all-shard retention loop under explicit
non-dry-run configuration。Checkpoint E.1 adds the producer-owned root/stream/provider-scope proofs and one-CAS
deletion activation described in §6；physical deletion remains fail closed until that exact activation succeeds。

## 4. Broker configuration

Add checked one-time fields to `ServiceConfiguration` and map them in `NereusBrokerStorageConfiguration`：

| Field | V1 rule/default |
| --- | --- |
| `nereusBookKeeperPrimaryWalEnabled` | `false`; gates registration/profile first-create |
| `nereusBookKeeperClusterAlias` | nonblank durable alias |
| `nereusBookKeeperProviderScopeId` | required canonical non-secret stock BK metadata-service/ledger-root identity；adapter derives/verifies SHA-256 |
| `nereusBookKeeperLedgerIdPrefixBits` | required `[8,24]`；leaves at least 39 random bits inside positive 63-bit ids |
| `nereusBookKeeperLedgerIdPrefixValue` | required canonical value with highest prefix bit one；all generated/existing ids must round-trip it |
| `nereusBookKeeperLedgerIdNamespaceReservationId` | required deployment-owned reservation identity；immutable/digest-bound, not a Boolean override |
| `nereusBookKeeperEnsembleSize` | positive; defaults to explicit stock BK configured value |
| `nereusBookKeeperWriteQuorumSize` | `ensemble >= write >= ack` |
| `nereusBookKeeperAckQuorumSize` | positive |
| `nereusBookKeeperDigestType` | admitted BookKeeper digest enum; immutable binding |
| `nereusBookKeeperPasswordSecretRef` | secret reference only; empty password must be explicit/defaulted consistently |
| `nereusBookKeeperMaxEntriesPerLedger` | positive and bounded |
| `nereusBookKeeperMaxBytesPerLedger` | positive and bounded |
| `nereusBookKeeperMaxAppendRangesPerLedger` | positive hard bound for protection/inventory pagination |
| `nereusBookKeeperProtectionSlotsPerRange` | `[4,64]` fixed slots；checked product with max ranges is at most 65,536 |
| `nereusBookKeeperMaxReaderLeasesPerLedger` | positive hard cap on process/ledger lease rows；overflow rejects read before provider IO |
| `nereusBookKeeperMaxUncertainAllocations` | `[1,65536]` fixed durable slot count；exhaustion rejects before another provider create |
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
| `nereusBookKeeperMaxClockSkewSeconds` | nonnegative deployment bound；part of lease/drain safety validation |
| `nereusBookKeeperGcDrainGraceSeconds` | at least reader lease + clock skew |
| `nereusBookKeeperLateCreateAuditGraceSeconds` | positive alert/recheck pacing only；never proves a transmitted create impossible |
| `nereusBookKeeperGcEnabled` | `false` safe default |
| `nereusBookKeeperGcDryRun` | `true` safe default |

The provider/ledger/read/scan fields map to `BookKeeperWalConfiguration`; delete concurrency、clock/drain/audit and
the two local switches map to `BookKeeperLedgerGcConfiguration`。No field is silently dropped or defaulted differently
between brokers.

`nereusDefaultStorageProfile` remains first-create-only. Merely selecting a BK enum while the feature/capability is
disabled must fail broker config validation or topic first-create before any ledger allocation.

Configuration binding SHA-256 includes cluster alias、provider-scope digest、exact ledger-id namespace/reservation、quorums、digest、range/
size bounds、non-secret password identity/version and V1 protocol semantics. Every broker advertises the same digest;
secret bytes never enter lookup properties/Oxia/logs.

### 4.1 Namespace provisioning authority

Runtime code must not turn a config string into its own exclusivity proof. A separate, explicit cluster provisioning
operation creates one immutable/revocable record under the product capability keyspace：

```text
${globalNereusPrefix}/bookkeeper-provider-scopes/v1/${providerScopeSha256}/ledger-id-prefixes/${prefixBits:02d}/${prefixValue:06x}
```

Every Nereus deployment sharing that BookKeeper provider scope must use the same Oxia authority/global prefix；if it
cannot, shared-scope BK profiles are unsupported. Components are canonical and strict-inverse parsed.
`providerScopeSha256` is 64 lowercase hex、prefix bits are two decimal digits and prefix value is six lowercase hex
digits with leading zeroes；decode/re-encode equality is mandatory.

```java
record BookKeeperLedgerIdNamespaceReservationRecord(
    int schemaVersion,
    String reservationId,
    String nereusDeploymentId,
    String clusterAlias,
    String bookKeeperProviderScopeSha256,
    int ledgerIdPrefixBits,
    long ledgerIdPrefixValue,
    Lifecycle lifecycle,                 // ACTIVE, REVOKED
    long reservationEpoch,
    long createdAtMillis,
    long revokedAtMillis,
    String operatorEvidenceSha256,
    long metadataVersion) { }
```

Lifecycle wire ids are `ACTIVE=1` and `REVOKED=2`；ordinals are forbidden. `reservationEpoch` is one at creation and
increments only for the terminal revoke CAS.

`ledgerIdNamespaceSha256` uses the exact binary frame
`SHA-256("NBLN1" || u32be(keyUtf8Length) || keyUtf8 || i64be(Oxia metadata version) ||
decodeLowerHex32(storedValueSha256))`。That exact digest is written into broker readiness、stream config binding、ledger
root/custom metadata and activation；a revoke or record replacement cannot retain the old capability identity.

The admin operation verifies the BookKeeper scope and deployment policy, uses put-if-absent/CAS to prevent two Nereus
deployments reserving the same `(providerScope,prefix)`, and records non-secret evidence digest only. Brokers may only
read/verify this record；startup, first-create and activation never auto-create it. `REVOKED` is terminal and invalidates
readiness immediately. This coordinates trusted Nereus deployments；a BookKeeper environment permitting untrusted
external exact `CreateAdv` in the prefix is outside the supported profile, because the provider cannot enforce a
conditional delete fence.

BK-M5 checkpoint D implements this boundary as two deliberately different surfaces：

- `BookKeeperLedgerIdNamespaceReservationStore` remains read-only and is the only interface received by broker WAL
  bootstrap；
- `BookKeeperLedgerIdNamespaceReservationAdminStore` adds exact put-if-absent/version-CAS only for
  `BookKeeperLedgerIdNamespaceProvisioningCoordinator`；
- `BookKeeperPrimaryWalAdministration.provisionNamespace/revokeNamespace` is the explicit embedding surface for a
  broker/admin route；normal runtime never calls it；
- provision is idempotent only for the same ACTIVE epoch-one identity and operator evidence digest；any foreign or
  terminal record fails closed；revoke requires the observed Oxia version、increments the reservation epoch exactly
  once and cannot be reversed；
- both create and revoke recover an applied-but-response-lost write only when a reload contains the byte-identical
  desired `NBLR1` value at a newer valid version。

## 5. Cluster capability

Add reserved lookup properties：

```text
nereus.bookkeeper-primary-wal-protocol = "1"
nereus.bookkeeper-primary-wal-config   = <configuration binding SHA-256>
nereus.bookkeeper-ledger-namespace     = <reserved-prefix + reservation binding SHA-256>
nereus.bookkeeper-primary-wal-activation = <stable NBKAP1 publication binding SHA-256>
nereus.bookkeeper-required-object-generation = "1"
```

`NereusBookKeeperPrimaryWalCapability.requireUnreserved` chains with existing storage-binding/cursor/generation
reserved checks. `NereusBrokerCapabilityCoordinator.decorateLookupProperties` publishes them only after the borrowed
client、module、codecs、writer/reader/recovery and exact deployment namespace reservation are initialized.

Profile barriers use two stable all-persistent-broker snapshots：

| Profile | Required capabilities |
| --- | --- |
| BK_ONLY | storage binding + cursor + BK protocol/config + exact ledger-id namespace |
| BK_ASYNC_OBJECT | BK_ONLY set + generation protocol |
| BK_SYNC_OBJECT | BK_ONLY set + generation protocol + independent required-object completion property |

BK-M4 has final-gated the module-local completion path；the production rollout nevertheless keeps sync completion as a
separate reserved property so a broker cannot infer Object publication support from generation-zero durability。The
implemented first-create barrier verifies the complete profile-specific property set in one stable two-snapshot fact，
not by composing independent readiness checks from different broker epochs。

Readiness identity includes broker id、start timestamp、all required property key/values and configuration digest.
Registry changes invalidate cached readiness. A broker with missing/different config keeps new BK profile admission
closed. First-create uses the all-broker stable-snapshot fact；an existing topic's writable open instead requires the
persisted profile's exact immutable local BK capability after storage initialization and broker registration。This
allows a capable owner to continue during a rolling old-broker window without weakening local admission。Read-only
historical open does not require writable capability；an incapable broker must not publish the topic writable。

Deletion activation also requires `maxReaderLeasesPerLedger` to cover the stable persistent-broker set plus one
conservative rolling-restart overlap slot in V1. A larger broker set invalidates deletion readiness；the limit cannot
be silently exceeded and later used as a truncated GC scan. This capacity check is performed again from live broker
readiness on every physical-deletion gate, not trusted from operator-supplied activation bytes.

## 6. Durable activation and deletion gate

Add a product-owned `BookKeeperProtocolActivationRecord` under a binding-specific Oxia capability key：

```text
/nereus/bookkeeper-primary-wal/v1/clusters/{sha256(clusterAlias)}/bindings/
  {configurationBindingSha256}/{ledgerIdNamespaceSha256}/activation
```

The activation key includes the physical configuration and the exact `NBLN1` namespace identity. A terminal namespace
revoke therefore cannot be overwritten in place or accidentally authorize a replacement prefix；a new binding receives
a new activation key while old streams keep their original target/reader contract. The partition identity is
`bookkeeper-primary-wal-v1-{sha256(clusterAlias)}`。

```java
record BookKeeperProtocolActivationRecord(
    int schemaVersion,
    Lifecycle lifecycle,                 // PREPARED, ACTIVE
    int protocolVersion,
    String clusterAlias,
    String providerScopeSha256,
    long brokerReadinessEpoch,
    String brokerReadinessSha256,
    String configurationBindingSha256,
    String ledgerIdNamespaceSha256,
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

Activation lifecycle wire ids are `PREPARED=1` and `ACTIVE=2`；record identity fields and enabled bits are validated
independently of enum ordinal.

Publication bits advance monotonically only after their milestone gate. Ledger deletion additionally requires：

- complete all-256-shard root/protection/lease scan；
- complete registered BK stream/profile/reference-domain scan；
- exact provider-scope create/write/read/fence/delete/response-loss canary；
- exact exclusive advanced-ledger-id namespace reservation digest and no stock/foreign exact-create permission in
  that prefix；
- zero `lateCreateHazard` and no retained allocation slot for every candidate selected for physical deletion；
- current stable broker readiness/config digest；
- one CAS installing all three exact proof digests and deletion bit；
- local `gcEnabled && !gcDryRun` plus exact active proof revalidation on every mutation。

Configuration alone is never deletion authority. Safe defaults start no mutating ledger collector. Namespace proof
is an operational trust boundary backed by cluster provisioning/readiness；the canary demonstrates the configured
scope but cannot manufacture exclusivity. If the reservation cannot be asserted, no BookKeeper profile may be
first-created and an already loaded topic must fail ownership/write/provider-delete closed；logical unload and delete
remain available so an operator can shed ownership or retire the topic without requiring a healthy BK rollout。

Likewise, an unknown create outcome is not repaired by an admin Boolean. Its fixed slot/hazard may exhaust new ledger
allocation by design；BK-M0–M6 require fail-closed operational escalation or a new provider-scope/prefix deployment,
and define no online hazard-clear operation.

The implemented wire value is deterministic `NBKA1` and keeps Oxia `metadataVersion` outside the stored bytes. Two
different identities are intentionally derived from it：

- exact `activationRecordSha256 = SHA-256("NBKA1" || u32be(keyUtf8Length) || keyUtf8 ||
  i64be(metadataVersion) || storedValueSha256Bytes)` binds every deletion candidate/proof；any activation CAS changes
  it and invalidates in-flight destructive work；
- stable `publicationActivationSha256 = SHA-256` over the `NBKAP1` domain、canonical key、protocol/schema、cluster、
  provider/config/namespace and the ACTIVE all-three-publication facts. It excludes broker readiness、deletion bit/
  proofs、timestamps、Oxia version and stored-value digest，and is the only activation identity advertised in broker
  lookup properties.

This split prevents deletion activation or readiness rollover from changing the property that is itself an input to
broker readiness；`NBKA1` remains the destructive-operation identity and `NBKAP1` remains the first-create publication
identity. They are not interchangeable.

`PREPARED=1` and `ACTIVE=2` are explicit wire ids；PREPARED has zero publication bits/proofs，ACTIVE must at least
publish WAL_ONLY，async requires WAL_ONLY，sync requires async，and deletion requires sync plus three individually
nonzero proof digests. Readiness epoch cannot decrease or change digest at the same epoch；publication/deletion bits
cannot clear；deletion proofs become immutable once enabled。

`BookKeeperProtocolActivationCoordinator` and `BookKeeperPrimaryWalAdministration.prepareActivation/activate` now
provide the explicit CAS control plane. Broker bootstrap performs an exact read only：it installs writer/reader support
even when activation is absent or PREPARED so historical topics remain readable，but publishes no BK lookup capability
until the same durable record is ACTIVE with WAL_ONLY、async and sync publication enabled. The broker property set
includes the stable `NBKAP1` digest，so any old/missing/drifted publication binding fails the same stable two-snapshot
first-create barrier. Enabling publication after broker bootstrap requires the normal broker rollout/restart so every
broker advertises the newly verified stable binding；the runtime never hot-patches lookup identity from an admin CAS。

Physical deletion has a separate live barrier. `BookKeeperBrokerReadinessProvider` obtains two stable snapshots of the
strongest `BOOKKEEPER_WAL_SYNC_OBJECT` property set under the
`nereus-bookkeeper-primary-wal-broker-readiness-v1` digest domain. `DefaultBookKeeperProtocolActivationVerifier`
reloads the exact NBKA1 record，derives its deletion proof，then requires the live epoch/SHA to equal the record and
checks broker-count-plus-one against reader-lease capacity. Registry events、broker restart/set drift or property drift
clear the cache and fail closed. Thus an admin caller cannot authorize deletion by storing an arbitrary readiness
epoch/digest even if every other activation field is syntactically valid。

Checkpoint E installs a production `BookKeeperLedgerRetentionService` only for explicit
`gcEnabled && !gcDryRun`。It scans every root shard、retires exact owner-authorized protections and drives the existing
mark/drain/delete/dual-absence manager one step per pass。This does not make local configuration deletion authority：
the current activation record、namespace and live strongest-profile broker readiness are reloaded/revalidated by the
gate and again before each provider-facing mutation。

Checkpoint E.1 implements the three proof producers and the only deletion-activation path：

- `BookKeeperRootCoverageProofProducer` starts with an empty continuation in every one of the 256 root shards，requires
  strict ordered/unique pages，reconstructs the exact root provider identity/custom metadata and traverses every
  protection and reader-lease page for roots matching the configuration/namespace binding。`NBKROOT1` hashes the
  canonical key、metadata version and durable stored-value digest for each matching fact plus the one broker-readiness
  identity；a partial or malformed traversal returns no proof；
- `BookKeeperStreamCoverageProofProducer` starts with an empty continuation in all 64 F4 registration shards。For every
  BK profile registration it requires the exact canonical registry key/version，ACTIVE/SEALED L0 stream/profile，F2
  virtual-ledger binding/current topic and `NPR1` projection reference/digest to agree。`NBKSTREAM1` binds those durable
  records and their current L0 semantic frontier to the same readiness；
- `BookKeeperScopeCapabilityProbe` first creates a permanent `QUARANTINED` Oxia audit root inside the reserved prefix，
  then uses exact `CreateAdv` + `NBKL1` metadata to write entry 0、read it without recovery、reopen with recovery to
  prove fencing/closed metadata、delete and observe absence twice。Applied create/delete response loss converges only
  after exact provider metadata/absence；foreign metadata is never fenced or deleted。An indeterminate create leaves
  the audit root quarantined and fails closed instead of guessing；
- `BookKeeperDeletionActivationCoordinator` accepts only `{runId, expectedActivationMetadataVersion, timeout}`。It
  requires non-dry-run GC、all three publication bits、live strongest-profile readiness and lease capacity，then runs
  scope canary -> readiness recheck -> root -> readiness recheck -> stream -> readiness recheck -> namespace recheck。
  Running the mutating canary first ensures its permanent audit root is included in the subsequent root coverage。All
  proof records must bind the same epoch/SHA；one activation CAS installs the three producer-owned digests and
  `ledgerDeletionEnabled=true`。A lost CAS response reloads and accepts only the exact installed proof tuple；an already
  active record is returned idempotently without rerunning provider IO；
- the ordinary `BookKeeperPrimaryWalAdministration.activate` rejects every request that sets the deletion bit，so a
  caller cannot inject digests through the older publication API。Production composition publishes the proof-capable
  administration object to the Pulsar storage plugin through a one-time sink。

Checkpoint E.2 installs the authenticated broker REST and durable-profile routing boundary：

| Method/path below `/admin/v2/brokers` | Request authority |
| --- | --- |
| `PUT /bookkeeper-primary-wal/namespace` | `{operatorEvidenceSha256, timeoutSeconds}` |
| `POST /bookkeeper-primary-wal/namespace/revoke` | `{revocationEvidenceSha256, expectedMetadataVersion, timeoutSeconds}` |
| `POST /bookkeeper-primary-wal/activation/prepare` | `{brokerReadinessEpoch, brokerReadinessSha256, timeoutSeconds}` |
| `POST /bookkeeper-primary-wal/activation/publications` | readiness identity、async/sync bits、expected version、timeout；no deletion/proof fields |
| `POST /bookkeeper-primary-wal/activation/deletion` | `{runId, expectedActivationMetadataVersion, timeoutSeconds}` only |
| `GET /bookkeeper-primary-wal/activation?timeoutSeconds=...` | authoritative read；missing record is 404 |

Every route completes `validateSuperUserAccessAsync()` before storage lookup or mutation，rejects a non-Nereus broker
before IO and caps request timeouts at 86,400 seconds。REST models return non-secret identity/version/digest facts but
omit internal canonical keys。Publication activation constructs
`BookKeeperProtocolActivationUpdate.publications(...)`，which fixes deletion false and all three proofs to zero；the
deletion route can only construct `BookKeeperDeletionActivationRequest`，so the producer-owned proof boundary survives
JSON deserialization and route mapping。

Loaded and unloaded topics now call the same `NereusManagedLedgerStorage.validateBoundAdminOperation` path。It reads
the immutable storage-class binding，loads the exact F2/L0 snapshot，requires
`projection.storageClassBindingGeneration == binding.bindingGeneration` and obtains the canonical profile from L0
`StreamMetadata`；the namespace/current broker default is never consulted。A partitioned parent enumerates and checks
every concrete partition independently。Supported operations that can read primary bytes or create storage-visible
state (`TERMINATE_TOPIC`、backlog analyze/clear、skip/expire、cursor reset、trim) require the exact durable profile's
current capability；trim first requires F4 generation readiness。Unload、logical delete and durable-subscription delete
remain available during capability drift；unsupported compaction/offload/truncate/shadow/migration operations fail
before any readiness lookup。

The retained QUARANTINED canary roots are intentional bounded-per-activation audit evidence and permanent allocation
vetoes；they are never reclaimed or reused by the ordinary allocator。Operators must therefore use a unique stable
run id per reviewed activation attempt and treat repeated failed canaries as a scope-health incident，not as garbage
eligible for automatic cleanup。

## 7. First-create and reopen admission

### 7.1 First create

```text
resolve requested/default profile
storage-class creation guard proves no stock BK/Nereus durable conflict
require exact profile capability/readiness + activation publication bit
create durable Nereus stream metadata with immutable profile
store BK configuration + ledger-id namespace binding for BK profiles
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
require exact local capability for ownership/writes
recover writer/ledger state before writable publication
install F3 cursor + F4 services as profile requires
```

Loaded and unloaded admin paths use durable binding/profile facts. An unloaded topic is not interpreted from namespace
default policy。Checkpoint E.2 implements this admin boundary。Checkpoint E.3 calls
`NereusCreationPermit.validateStorageProfileBeforeWritableOpen` only after reloading the existing projection/L0 profile
and before writable cursor/runtime hydration。Read-only open skips that check；trusted logical-delete open has a
separate explicit bypass so capability drift cannot make deletion impossible。

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

### 8.4 Implemented checkpoint E.3

`NereusBrokerCapabilityCoordinator.requireLocalStorageProfileReady` is intentionally different from
`requireStorageProfileReady`：

- first-create still requires two identical all-persistent-broker snapshots for the exact profile；
- existing writable open requires storage initialized、the local broker registry started/registered and the immutable
  local `BookKeeperPrimaryWalCapabilityBinding` installed；
- an old/incapable remote broker therefore blocks new BK first-create but does not stop an already capable owner；
- a locally incapable broker fails writable open before facade hydration；historical read-only open and trusted logical
  delete remain available；
- generation-aware L0 reads admit generation zero for BK_ONLY and reject any positive generation as an invariant
  violation；Reader seek reconnect returns the cached open non-durable cursor, matching stock ManagedLedger behavior。

The real `NereusBookKeeperMultiBrokerIntegrationTest`
`preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` provisions the real Oxia namespace/activation，runs
two Pulsar brokers against the borrowed real BookKeeper client and LocalStack-backed runtime，then proves unload、owner
stop、survivor takeover、old-owner restart/rejoin、survivor stop and reverse takeover。It compares ledger/entry/partition/
batch fields of every `MessageIdAdv` for ordinary and LZ4-compressed batches，checks both exclusive and inclusive seek，
directly reads the provider-neutral generation-zero path and keeps a stock BookKeeper topic readable/writable across
both takeovers。This is BK-82/BK-84 evidence。

### 8.5 Implemented checkpoint E.4

`NereusBookKeeperOwnershipFilter` runs in the extensible load-manager ownership pipeline for the Nereus storage
class。For every namespace with active Nereus bindings it reloads the exact durable L0 profile through
`NereusManagedLedgerStorage.requiredBookKeeperOwnershipProfile` and applies these rules before owner selection：

- no BK profile leaves candidates unchanged；
- any BK profile requires one complete exact reserved-property signature for that immutable strongest profile；
- candidates without the signature are removed；two different complete signatures or a scan/reload failure clear all
  candidates rather than guessing；
- a noncapable broker may receive the client's initial lookup and redirect it，but cannot win writable ownership；
- existing topic profile truth always comes from durable binding/L0 metadata，never the broker's current default。

The deletion coordinator treats the readiness epoch as an opaque identity derived from the live exact broker set，not
as a numeric monotonic counter。If the live identity changes，it regenerates and CAS-rebinds all root、stream and scope
proofs under the same stable publication identity，then revalidates live readiness before returning；an unchanged
identity is idempotent。

`NereusBookKeeperCapabilityRolloverTest` proves old-broker exclusion、redirect to the capable owner、pre-binding
first-create rejection and two deletion-proof rebindings as the broker set leaves and rejoins。
`NereusMixedPrimaryProfilesMultiBrokerTest` changes only the broker default between first creates，persists
BK_SYNC -> BK_ASYNC -> Object-WAL topics，then proves old topics retain their exact profiles across cold load，both
single-broker takeovers read every history entry，and all MessageId ledger/entry/partition/batch fields remain exact。
Together with E.3 this closes BK-83、BK-85 and BK-86；online profile migration remains absent by design。

## 9. Stock BookKeeper isolation

Stock Pulsar topics and Nereus BK-profile topics may share the same client/BookKeeper cluster, but not metadata
authority：

- Nereus uses only its reserved positive-63-bit advanced-id prefix；stock clients use normal allocation and must not
  call exact `CreateAdv` inside that prefix；
- stock ManagedLedger metadata/ledgers are never inserted into Nereus root keyspace；
- Nereus ledgers carry exact `NBKL1` custom metadata and global Oxia root/allocation identity；
- Nereus reader/deleter requires both root and matching custom metadata；
- a ledger-id collision with stock/foreign metadata is never deleted；a new random candidate inside the reserved
  prefix is chosen and the collision invalidates deletion activation for audit；
- Nereus does not invoke stock ManagedLedger trim/delete APIs for its physical ledgers；
- stock control topic write/read/retention remains in every real two-broker gate。

## 10. Profile rollout

Recommended operational rollout mirrors implementation milestones：

```text
1. reserve/provision the exact ledger-id namespace, then deploy BK-capable readers/codecs/module with all
   publication/delete bits off
2. establish stable cluster readiness/config/namespace digests
3. enable BK_ONLY publication for canary first-create topics
4. pass restart/unload/failover/rollover/logical-trim gates
5. enable BK_ASYNC publication; keep ledger delete dry-run initially
6. verify lag/object publication/source-retirement; activate scoped ledger deletion
7. enable BK_SYNC publication and completion SLOs
8. keep broad production expansion guarded until the BK-M6 aggregate gate
```

Removing capability from one broker prevents new BK topic creates cluster-wide and writable ownership on that broker；
an already capable owner uses local admission and may continue serving existing topics。Rollback must keep at least
read/recovery support for already-created BK targets；downgrading to a build that cannot decode/read durable BK targets
is prohibited。

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

Checkpoint E.4 completes the remaining surface：mixed BK async/sync/Object profiles、old/noncapable broker ownership
exclusion、opaque readiness-identity proof rebinding and the named ordinary/final aggregates are executable and green。
BK-M5 is complete/final-gated；BK-M6 owns the remaining aggregate scenario/scale/chaos/compatibility evidence。
