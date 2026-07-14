# Facade, Broker, and Future Compatibility

## 1. Compatibility Goal

F3 must make a Nereus-backed durable subscription observable like a Pulsar ManagedLedger subscription while
preserving this fixed identity chain：

```text
topic incarnation -> streamId -> virtualLedgerId -> entry offset -> optional batch index
```

Cursor generation, Oxia version, snapshot ID and F4 object generation are internal coordinates。They never enter
`Position` or client `MessageId`。

Consequences：

- broker restart/unload/failover returns the same ledger ID + entry ID for an existing Entry；
- seek/reset/history read does not remap the MessageId；
- subscription delete/recreate changes cursor generation but not retained topic MessageIds；
- topic delete/recreate changes the F2 topic incarnation and therefore gets a new stream/virtual-ledger identity；
- F4 compaction may replace bytes/object references but must retain the same logical Entry coordinate。

## 2. Nereus Facade Changes

### 2.1 Runtime composition

`NereusManagedLedgerRuntime` gains owned dependencies：

```java
CursorStorage cursorStorage();
CursorRetentionCoordinator cursorRetentionCoordinator();
CursorStorageConfig cursorStorageConfig();
CursorProtocolActivationGuard cursorProtocolActivationGuard();
```

The existing `NereusLedgerOpenResult(topicProjection, projection, streamMetadata)` remains the projection/L0 result
used by get-only/read-only inspection。F3 adds a writable-only wrapper：

```java
public record NereusWritableLedgerOpenResult(
        NereusLedgerOpenResult ledger,
        CursorOwnerSession cursorOwnerSession,
        List<CursorHandle> durableCursors,
        CursorRetentionView cursorRetention) {
}
```

Its constructor requires all fields non-null，copies the list，and validates the owner ledger、every handle and the
retention view against `ledger.topicProjection().projectionIdentity()` and managed-ledger name/hash。
The owner session is fresh for this writable ledger instance；the list is immutable and complete for the stabilized
claimed open scan。`NereusManagedLedger` is constructed only from this wrapper and creates its exact-name cursor map
before it is visible to broker。`NereusReadOnlyManagedLedger` continues accepting the base result and neither scans nor
claims cursor state。A writable open always creates or claims one ACTIVE retention owner root，even when the cursor
activation marker and cursor list are absent；this root fences old writable sessions but does not activate the cursor
protocol or become visible to an F2 reader。

The coordinator keeps its existing base method and adds the writable path：

```java
final class NereusManagedLedgerOwnershipGuard {
    static NereusManagedLedgerOwnershipGuard checked(
            Supplier<CompletableFuture<Boolean>> checker,
            Duration timeout);
    static NereusManagedLedgerOwnershipGuard trustedDirect(Duration timeout);
    CompletableFuture<Void> requireOwned(String operation);
    boolean isTrustedDirect();
}

CompletableFuture<NereusLedgerOpenResult> open(
        String managedLedgerName,
        ManagedLedgerOpenConfigView config);

CompletableFuture<NereusWritableLedgerOpenResult> openWritable(
        String managedLedgerName,
        ManagedLedgerOpenConfigView config,
        NereusManagedLedgerOwnershipGuard ownershipGuard);
```

`openWritable` first calls `requireOwned`，then completes the existing projection/L0 protocol，creates/claims the
retention owner root and performs the cursor claim/hydration protocol before returning。It calls `requireOwned` again
after stabilization immediately before returning。The
factory's broker-facing checked `asyncOpen` constructs the guard from the exact supplied
`mlOwnershipChecker`；get-only、storage-state inspection and admin
reads that construct `NereusReadOnlyManagedLedger` use the base `open` and cannot mutate owner fields。

The no-checker async overload and synchronous `open` retain a named `trustedDirect` compatibility guard for embedded
tests/direct trusted callers；`BrokerService` never uses that path。A checked guard rejects supplier throw、null future、
false、exception or timeout as fenced/open failure。`NereusManagedLedger` retains the guard and brackets first durable
cursor create/open with it；a failed final check leaves any already-durable root transition for the next owner to
hydrate but emits no success callback。The guard is broker authority/admission evidence，not a metadata CAS token and
cannot replace `CursorOwnerSession`。

### 2.2 Durable and non-durable cursor construction

F3 keeps one facade class with two explicit modes：

```text
DURABLE
  CursorHandle + CursorStorage
  participates in getCursors(), retention, slowest-consumer and restart hydration

NON_DURABLE
  in-memory CursorAckState + same pure CursorStateMachine
  no metadata/snapshot/retention；joins live getCursors()/getActiveCursors() only after local creation
```

The current F2 “durable-boundary cursor that rejects mutation” is removed only when F3 durable construction is wired。
No intermediate release may silently accept a durable open but keep state only in the ledger-local map。

One ledger-local exact-name registry contains both modes but never aliases them。A non-durable create whose requested
name is already registered to a durable cursor（or vice versa）fails instead of returning the wrong mode。The
analyze-backlog duplicate chooses a random name with bounded collision retry；its later `asyncDeleteCursor(name)`
atomically removes/closes that exact non-durable instance and performs no metadata write。Only durable entries enter
open-time hydration、retention scans and slowest-consumer ordering；both modes remain visible in the live cursor
registry after local non-durable creation，matching locked `ManagedLedgerImpl` behavior。

### 2.3 ManagedLedger cursor methods

| Method | F3 implementation |
| --- | --- |
| sync `openCursor` overloads | block only through existing ManagedLedger sync wrapper/deadline around async durable open |
| async `openCursor` overloads | exact-name registry; durable storage open/create; callback after handle hydration |
| `getCursors` | safe snapshot iterable of all currently registered local facades；at open callback this is the complete hydrated ACTIVE durable set，and later non-durable creates join it |
| `getActiveCursors` | active-flag subset across both registered modes；not a retention authority |
| `getSlowestConsumer` | root-lifecycle ACTIVE durable cursor with lowest projected mark-delete，regardless of its broker-local active flag；deterministic exact-name tie break，null when none |
| active-entry/backlog estimates | retain the F2 ledger formulas and consume hydrated F3 mark-delete/whole-range state plus document-02 cursor formulas；never infer from local read position |
| sync/async `deleteCursor` | registry mode dispatch：non-durable remove/close locally；durable idempotent tombstone then remove/close；missing name performs idempotent durable delete lookup |
| `newNonDurableCursor` | F2 local construction enhanced with in-memory individual/batch ack |
| `trimConsumedLedgersInBackground(promise)` | complete normally without calling L0 trim or the retention coordinator；F4 alone wires policy trim through `requestTrim` |
| `asyncTruncate` | continue failed/unsupported；it cannot bypass cursor protection |
| ledger close | close local facades/watchers; never delete durable roots |
| topic delete | F2 stream/projection delete remains owner; cursor prefix becomes F4 reclamation input |

Same-name opens coalesce one in-flight future per ledger instance。Cross-ledger/broker duplicate opens are resolved by
metadata CAS。A failed open is removed from coalescing map so a later retry can run。

## 3. ManagedCursor Compatibility Matrix

| Observable area | F3 guarantee | Deliberate difference / limit |
| --- | --- | --- |
| cursor name/durability | exact encoded name; durable survives broker restart | no BookKeeper cursor ledger ID exposed |
| broker ownership | fresh owner session claims retention + every ACTIVE root before open callback | mixed/stale sessions fail fenced；owner ID is not MessageId identity |
| cumulative ack | durable before callback; position properties atomic | no callback-before-persist throttle |
| individual ack | durable ranges/partial batches, including Shared | no truncation/redelivery caused by configured persistence cap |
| read position | moves during live dispatch/seek/rewind | not persisted; restart redelivers from first unacked |
| mark-delete position | same Position-facing previous-entry semantics | stored internally as first-unacked offset |
| replay | whole-deleted positions skipped; partial Entry returned with remaining words | only retained F1 bytes before F4 compacted view |
| reset | disconnect/fence handled by broker, root CAS is destructive boundary | force cannot resurrect trimmed bytes before F4 |
| clear/skip/expire | durable cursor progress | no immediate physical deletion |
| backlog entries | exact whole-entry count | message/byte counts may be documented estimates |
| cursor properties | root CAS, internal prefix preserved | strict UTF-8/byte caps |
| `isCursorDataFullyPersistable` | true for every admitted state | oversize mutation fails instead of truncating |
| active/lastActive/stats | broker-local | lastActive is not a durable correctness field |
| offload/compaction hooks | fail closed | F4 owns materialized view |

## 4. Supported Subscription Modes

| Mode | Durable | Non-durable | Ack shape | F3 result |
| --- | --- | --- | --- | --- |
| Exclusive | yes | yes | cumulative and individual, whole/batch | supported |
| Failover | yes | yes | cumulative and individual, whole/batch | supported |
| Shared | yes | yes | individual, whole/batch | supported |
| Key_Shared | no | no | individual + hash ownership | rejected；Future 8 |

Consumer exclusivity、Failover active selection and Shared dispatch fairness remain broker runtime policy。F3 stores no
subscription type, consumer ID, active broker or assignment in cursor root。

Still rejected：

- transaction pending ack / transactional acknowledgement；
- replicated subscription and replication cursors；
- read-compacted subscription；
- delayed delivery；
- deduplication cursor；
- Key_Shared ranges/ordering。

## 5. Pulsar Fork Changes

The fork changes below are implemented at local Pulsar
`master@12edc9381c147ceec8bedd530acb5be7db339707` and are locked by `phase3M4Check`。The gate runs the eight focused
broker suites named in document 06 together with broker/broker-common spotless checks against an exact clean source
checkout。

### 5.1 `NereusTopicFeatureValidator`

`validateSubscribe` now replaces the F2 blanket durable rejection with：

```text
allow:
  durable/non-durable Exclusive
  durable/non-durable Failover
  durable/non-durable Shared

reject:
  Key_Shared or nonempty KeySharedMeta hash ranges
  readCompacted
  replicated subscription
```

An empty non-null `KeySharedMeta` carrier supplied by `ServerCnx` is accepted for non-Key_Shared types。

`validateCreateSubscription` becomes capability-aware and admits ordinary durable create。It must run after resolving
the Nereus topic context but before cursor IO。

`validateExistingDurableCursors` no longer rejects a hydrated F3 cursor；instead it requires cursor protocol runtime
ready。A broker lacking that runtime fails topic open before `createPersistentSubscriptions`。

Topic policy changes：

| Feature | F3 admission |
| --- | --- |
| message TTL | allow；expiry advances durable cursor only |
| subscription expiration | allow；expiration uses idempotent cursor delete |
| durable subscription admin | allow selected operations below |
| retention policy / backlog eviction | keep rejected until F4 |
| compaction / Pulsar offload / truncate | keep rejected |
| geo replication / replicated subscription | keep rejected |
| dedup / system-internal / entry filters / shadow/migration / auto-skip | keep rejected |

TTL/subscription expiration does not authorize trim or object deletion；it only changes/deletes cursor state。

### 5.2 `NereusAcknowledgeValidator`

The validator remains the first Nereus-specific action in `Consumer.messageAcked`。New decision table：

| Check | Durable cursor | Non-durable cursor |
| --- | --- | --- |
| transaction bits/pending-ack path | reject | reject |
| protocol validation error | reject before mutation | reject before mutation |
| Exclusive/Failover cumulative | exactly one MessageId; allow whole/valid batch | same, but `requirePersistedAck` rejected |
| Exclusive/Failover individual | allow bounded list; whole/valid batch | allow in-memory bounded list |
| Shared individual | allow bounded list; whole/valid batch | allow in-memory bounded list |
| Shared cumulative | reject | reject |
| Key_Shared | reject | reject |
| `requirePersistedAck` | allow and wait for durable callback | reject because no durable state |
| partial batch while deletion-at-batch-index disabled | reject explicitly | reject explicitly |

Validator checks command shape only。Projection ledger ID、offset bounds、batch size and Entry metadata are validated by
facade/storage where authoritative state is available。

### 5.3 `Consumer.messageAcked` completion ordering

The implemented F3 broker path uses this ordering：

```text
validate Nereus ack command
  -> build immutable Position list
  -> call PersistentSubscription.acknowledgeMessageAsync
  -> await cursor callback for every Nereus durable acknowledgement
  -> only on success:
       update persisted-ack response
       clear pending/redelivery state
       apply unacked/counter effects that imply acknowledgement
  -> on failure:
       leave those effects unapplied and return protocol error
```

The explicit condition for awaiting individual cursor persistence is：

```java
requirePersistedAck || (nereusTopic && cursor.isDurable())
```

The code names the second term `nereusDurableAcknowledgement`；it is false for a Nereus non-durable cursor。Cumulative
ack already completes through `asyncMarkDelete`'s callback and uses the same durable/non-durable error mapping。

For durable Nereus acknowledgements，`redeliveryTracker.removeBatch`、pending-state cleanup、timestamp/counter updates
and end-of-topic effects now run only after cursor persistence succeeds。A failed cursor CAS leaves them unchanged；
non-durable Nereus and BookKeeper paths retain their applicable stock timing。

### 5.4 PersistentSubscription call path

No alternate cursor protocol is added。The existing mapping remains：

```text
cumulative -> asyncMarkDelete
individual -> asyncDelete
clear      -> asyncClearBacklog
skip/TTL   -> asyncSkipEntries / mark-delete path
reset      -> asyncFindNewestMatching + asyncResetCursor
delete     -> ManagedLedger.deleteCursor
```

`AckCallback` future completion is the client-visible persistence boundary。Reset keeps the existing subscription
fence/disconnect behavior；F3 maps root CAS conflict to the existing busy error path。

### 5.5 `NereusAdminOperation`

Allow for Nereus F3 topics：

```text
TERMINATE_TOPIC
DELETE_TOPIC
UNLOAD_TOPIC
DELETE_DURABLE_SUBSCRIPTION
ANALYZE_BACKLOG
CLEAR_BACKLOG
SKIP_MESSAGES
EXPIRE_MESSAGES
RESET_CURSOR
```

Continue rejecting：

```text
TRIGGER_COMPACTION
READ_COMPACTION_STATUS
TRIGGER_OFFLOAD
READ_OFFLOAD_STATUS
TRIM_TOPIC
TRUNCATE_TOPIC
SET_SHADOW_TOPICS
MIGRATE_TOPIC
```

Every loaded-topic and namespace/bulk admin path must use the same closed enum；a REST route cannot bypass it by
opening a cursor directly。`ANALYZE_BACKLOG` validates before calling `duplicateNonDurableCursor`；the duplicate starts
from durable mark-delete and copies the current whole/partial ack view without persisting a new cursor。
`PersistentTopicsBase.trimNonPartitionedTopic` validates `TRIM_TOPIC` immediately after resolving the loaded topic and
before invoking `trimConsumedLedgersInBackground`；partitioned trim reaches the same check on each child。F3 rejects
that admin operation explicitly while BrokerService periodic housekeeping continues to receive a normal no-op。

## 6. Broker Topic-open Ordering

Required order：

```text
binding/projection validation
  -> Nereus runtime + cursor protocol ready
  -> generate writable CursorOwnerSession
  -> claim retention root + every ACTIVE cursor root；recover/stabilize
  -> ManagedLedger hydrate all claimed durable cursors
  -> authoritative policy snapshot validation
  -> PersistentTopic.initialize
  -> createPersistentSubscriptions from ledger.getCursors
  -> admit subscribe/ack/admin
```

The F2 call to `validateExistingDurableCursors` moves from “reject if any” to “assert cursor runtime capability and
hydration completed”。It must not cause a second metadata scan that can disagree with `getCursors()`。

The claim is part of writable ManagedLedger open，not `PersistentTopic.initialize` and not a watch callback。Read-only
F2 inspection/ledger APIs do not claim cursor ownership。A stale session maps to ManagedLedger fenced，and the broker
must not retry that handle under a newly observed owner ID；only a fresh writable open may claim。

## 7. Capability and Rolling Upgrade

### 7.1 Capability property

F3 reserves broker lookup property：

```text
nereus.cursor-protocol=1
```

It is distinct from existing：

```text
nereus.storage-binding-protocol=1
```

The broker rejects operator-configured spoofing of either reserved property。Cursor capability is published only
after：

- shared Oxia runtime and cursor metadata store are initialized；
- snapshot ObjectStore/provider is initialized and the V1 codec/config startup self-check passes；
- `CursorStorage` and retention recovery are ready；
- the concrete Nereus ManagedLedger registry is attached；
- F2 storage-binding capability is already ready。

Startup does not upload a disposable probe object because the current generic ObjectStore has no delete API。Real
provider PUT/HEAD/read behavior is proven by the deployment/integration gate；a runtime IO failure later fails the
cursor mutation/open closed and cannot publish a root reference。

### 7.2 First durable cursor gate

Before the first unactivated topic projection can enter ABSENT/DELETED -> ACTIVE cursor creation，the capability
coordinator requires **two consecutive stable snapshots** of all persistent brokers showing both protocol properties
at supported versions。The second snapshot must have the same broker set as the first；the
`acquireFirstActivationPermit` future completes only after that stability recheck。The coordinator is wired as
`CursorProtocolActivationGuard`；there is no permissive production fallback。Then `DefaultCursorStorage`
CAS-activates `TopicProjectionRecord` with the internal `nereus.cursor-protocol=1` minimum-reader marker，updates its
local projection view，revalidates the already-claimed owner-only retention root，and executes
ACTIVE -> PROTECTION_PENDING -> cursor-attempt proof -> ACTIVE before callback。

Opening/acking an already existing durable cursor does not wait for a cluster-wide snapshot；the local broker either
has protocol 1 and operates safely or fails topic open。This avoids making ongoing ack availability depend on a
temporarily unreachable unrelated broker。A topic whose monotonic activation marker already exists also skips this
cluster-wide gate for cursor recreate/crash-resume；the marker itself already makes older readers fail closed。

### 7.3 Rollout sequence

This rollout starts only from one release containing the marker-aware decoder、M3 writable claim/hydration and M4
broker admission together。M1/M2 are repository/test milestones，not independently deployable broker artifacts。

```text
1. Deploy F3-capable code with no durable Nereus cursor yet；writable opens may create owner-only retention roots but
   do not change the topic projection marker.
2. Wait until every persistent broker advertises both capabilities stably.
3. First durable create durably activates the topic projection，then attempts cursor state；a later failure leaves the
   monotonic marker activated for safe retry.
4. From the activation CAS onward, rolling downgrade to F2-only brokers is unsupported even before a cursor callback.
```

The locked F2 binary does not scan F3 cursor keys。Ignoring an owner-only preactivation root is safe because no cursor
or pending retention state can coexist with a missing marker and F2 trim is a no-op。It instead fails Nereus topic open while decoding the activated
projection because its exact `ProjectionCreateRequest.canonicalProperties` rejects the reserved `nereus.*` marker。
The F3 runtime permits only that exact internal value，filters it from user properties and preserves it through every
property/lifecycle/delete/recreate CAS。Thus an old broker cannot expose an empty cursor view or recreate an existing
subscription at Latest。Mixed-version availability may be reduced, but correctness is preserved。

Schema major/version upgrade requires readers to understand existing V1 before advertising a higher capability。
Removing/changing the activation marker or making the locked F2 decoder accept it requires a new downgrade-fence gate。

## 8. Runtime Configuration and Lifecycle

`NereusRuntimeConfiguration` adds the two cursor records while retaining the locked F2 constructor as a source bridge：

```java
public record NereusRuntimeConfiguration(
        OxiaClientConfiguration oxia,
        ObjectStoreConfiguration objectStore,
        StreamStorageConfig streamStorage,
        NereusManagedLedgerFactoryConfig managedLedger,
        ProjectionMetadataStoreConfig projectionMetadata,
        CursorMetadataStoreConfig cursorMetadata,
        CursorStorageConfig cursorStorage) {

    public NereusRuntimeConfiguration(
            OxiaClientConfiguration oxia,
            ObjectStoreConfiguration objectStore,
            StreamStorageConfig streamStorage,
            NereusManagedLedgerFactoryConfig managedLedger,
            ProjectionMetadataStoreConfig projectionMetadata) {
        this(oxia, objectStore, streamStorage, managedLedger, projectionMetadata,
                CursorMetadataStoreConfig.defaults(), CursorStorageConfig.defaults());
    }
}
```

The five-argument configuration constructor is source compatibility for the locked F2 fork and uses only frozen F3
defaults。The M4 `NereusBrokerStorageConfiguration` now builds the canonical seven-argument value from typed broker
settings；operator configuration never selects the compatibility overload。

The borrowed broker context adds the activation guard explicitly：

```java
public record NereusRuntimeContext(
        EventLoopGroup eventLoopGroup,
        OpenTelemetry openTelemetry,
        NereusCreationGuard creationGuard,
        CursorProtocolActivationGuard cursorProtocolActivationGuard,
        ObjectStoreSecretResolver secretResolver,
        ClassLoader pluginClassLoader) {

    // Source-compatibility bridge for the locked F2 fork only.
    // It delegates to the canonical constructor with a named fail-closed
    // NEREUS_CURSOR_CAPABILITY_NOT_READY guard.
    public NereusRuntimeContext(
            EventLoopGroup eventLoopGroup,
            OpenTelemetry openTelemetry,
            NereusCreationGuard creationGuard,
            ObjectStoreSecretResolver secretResolver,
            ClassLoader pluginClassLoader) {
        this(eventLoopGroup, openTelemetry, creationGuard,
                CursorProtocolActivationGuard.unavailable(),
                secretResolver, pluginClassLoader);
    }
}
```

The five-argument compatibility constructor is retained only for locked F2 source compatibility；it can never
activate a cursor protocol or advertise cursor capability。The M4 `NereusManagedLedgerStorage` now constructs the
cursor-capable coordinator before runtime creation and passes its fail-closed delegate through the canonical field；
before `BrokerRegistryImpl` attaches，the delegate fails rather than allowing activation。
`DefaultNereusRuntimeProvider` wires that exact borrowed guard into `DefaultCursorStorage` and
`DefaultCursorRetentionCoordinator`，and never creates an internal allow-all guard。The runtime does not close the
borrowed guard/coordinator。

Validation requires：

- cursor metadata timeout equals the shared Oxia request/managed-ledger metadata timeout；
- snapshot total timeout is positive and `<= managedLedger.closeTimeout`；each snapshot object subcall is capped by
  the smaller of the remaining snapshot deadline and `objectStore.requestTimeout`；
- inline ack budget < metadata hard max minus the safety margin；
- property budgets + minimum root overhead < hard max；
- maximum cursor name + property maps / maximum target batch each fit the protection-intent cap；
- queue/count/page/snapshot limits are positive and mutually consistent。
- owner-claim concurrency is positive/bounded and the open deadline covers a full ACTIVE-root claim + rescan at the
  configured cursor-count limit。

Startup ownership：

```text
SharedOxiaClientRuntime
  -> F1/F2/Cursor metadata stores
ObjectStore
StreamStorage
CursorSnapshotStore
CursorStorage + CursorRetentionCoordinator recovery
NereusManagedLedgerRuntime
broker storage registry
cursor capability publication
```

Failure closes constructed resources in exact reverse order and never publishes capability。Shutdown first removes
capability/admission, then drains cursor lanes within the global shutdown budget, closes ledgers/storage, and finally
shared clients。

## 9. MessageId and Projection Stability

### 9.1 Forward/reverse conversion

F3 exclusively calls F2 `PositionProjection`：

```text
Position -> require virtualLedgerId -> entryId as offset
offset -> same virtualLedgerId + offset
batch MessageId -> same Position plus validated batchIndex/ack-set extension
```

It does not add an entry index per cursor or per MessageId。Ledger metadata and batch metadata remain：

| Fact | Location |
| --- | --- |
| virtual ledger / stream incarnation | F2 Oxia projection record |
| entry offset -> committed object slice | F1/F2 stream/offset metadata |
| Pulsar batch size/index payload | persisted Pulsar Entry bytes |
| cursor whole/partial ack | F3 root + referenced cursor snapshot |

The scale of cursor metadata is proportional to cursor ack holes, not topic entry count。

### 9.2 Restart/seek/history

- restart reloads the same projection before cursor roots；
- seek/reset validates the same virtual ledger and direct offset；
- retained history is read from the same F1 logical offset；
- batch size is decoded from immutable Entry bytes；
- a projection/incarnation mismatch fails rather than translating coordinates。

## 10. F4 Compatibility Contract

F4 may add：

- immutable materialized/compacted objects；
- generation-aware read routing for the same logical offsets；
- policy-driven retention and exact physical accounting；
- snapshot/object orphan scanning and deletion；
- reader/reference grace needed for concurrent object GC；
- compacted-view support for force reset/read-compacted。

F4 must preserve：

1. cursor root/generation and `markDeleteOffset` semantics；
2. one Entry = one offset and stable Position/MessageId；
3. partial batch remaining-bit representation；
4. snapshot visibility from cursor root reference；
5. every new/recreated cursor and backward reset stays in recoverable PROTECTION_PENDING through cursor CAS，lowering
   protection when needed；
6. logical trim completes through the retention `TRIM_PENDING` protocol；
7. floor raise/trim/GC cannot bypass either pending lifecycle；
8. physical GC consults current cursor snapshot references and other reference domains；
9. every writable open claims retention + ACTIVE cursor roots under one fresh F3 owner session before topic-owned
   trim work or dispatch；stale sessions cannot trim or mutate already-claimed roots，and an already-pending
   CREATE/RECREATE target-key race cannot stale-finalize/callback and is claimed during takeover recovery。Read-only
   F4 planner/GC workers use the versioned `CursorMetadataStore` read/scan surface rather than owner-scoped facade
   methods；they do not claim，but must include/revalidate observed root versions/session IDs before acting；
10. projection never becomes append/visibility correctness owner。

For a cursorless unactivated topic，F4's first `requestTrim` carries the current writable owner session，requires the
already-claimed owner-only retention root，and must use the F3 activation guard/marker before TRIM_PENDING。It cannot special-case “no cursor today” into a direct L0 trim，because that would
race the first durable cursor create。This activation is a minimum-reader fence and is expected once F4 policy/GC
semantics are used for the topic。

F4 may extend a snapshot reference with new optional flags only under a new readable schema/capability。It cannot
reinterpret V1 object type/code or delete V1 objects solely from age。

## 11. Later-future Boundaries

| Future | May consume from F3 | Must not overload F3 record |
| --- | --- | --- |
| F5 KoP | stream coordinate and retention reference API | Kafka group metadata/offsets do not become Pulsar cursor names |
| F6 Lakehouse | committed/trim/reference facts | table snapshots do not change cursor ack truth |
| F7 routing | topic/stream ownership | routing epochs do not become cursor generations |
| F8 advanced Pulsar | base cursor ack state | Key_Shared ownership, pending txn ack, delayed index, replicated-sub state use separate records |

Each new reference domain must participate in F4 retention/GC explicitly；it cannot hide a protection offset inside
cursor properties。

## 12. Operational Surface

F3 metrics use only bounded labels such as outcome/kind/state/version；neither exact nor hashed stream/cursor names are
metric labels。Hashes are allowed only in rate-limited logs/traces and point admin diagnostics。Cursor exact name is
allowed in debug logs only under existing Pulsar logging policy。

Required metric families：

```text
nereus_cursor_open_total{outcome}
nereus_cursor_hydration_seconds
nereus_cursor_mutation_total{kind,outcome}
nereus_cursor_cas_conflict_total{kind}
nereus_cursor_queue_depth (aggregate histogram)
nereus_cursor_snapshot_write_total{outcome}
nereus_cursor_snapshot_read_total{outcome}
nereus_cursor_snapshot_bytes
nereus_cursor_orphan_candidate_total{reason}
nereus_cursor_retention_lag_offsets (aggregate histogram)
nereus_cursor_retention_pending_count{state}
nereus_cursor_trim_recovery_total{outcome}
nereus_cursor_protocol_capability{version}
```

Stats/health expose：

- cursor generation、root mutation sequence/version；
- durable mark-delete and local read offset as distinct fields；
- inline/snapshot range and partial counts；
- current snapshot key hash/size, never credentials or raw bytes；
- retention lifecycle/mutation-sequence/Oxia-version/floor/completed/pending offsets；
- last corruption/error operation with redacted context。

No repair endpoint is part of F3。Corrupt stable root/reference requires explicit offline/operator procedure so a
broker cannot guess away acknowledged data。
