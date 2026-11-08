---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m1
---

# Protocol integrations and product gates

## Storage Fabric boundary

One Nereus Storage Fabric may contain multiple Kafka and Pulsar Protocol Cells. Cells may use the same external Object
Storage or BookKeeper infrastructure and may share worker processes, compatible transport capacity, and observability.
Each cell nevertheless owns a distinct Cell Provider Scope/session, namespace, credential/KMS and operator scope,
admission/retry/circuit-breaker state, queue and cache accounting, task root, GC capability, drain, and close lifecycle.
Object groups do not cross cells in 0.2.

Sharing never makes control planes, positions, Native Write Authorities, provider sessions, or physical-delete authority
interchangeable. A cell-local close, throttle, credential failure, stale task, or GC request cannot mutate another cell.
An outage of intentionally shared physical infrastructure may still affect every attached cell. Dedicated provider
infrastructure is an optional deployment topology for stronger SLO, compliance, or physical-failure isolation.

| Protocol/profile path | Protocol position truth | Protocol Coverage | Physical Extent |
| --- | --- | --- | --- |
| Kafka / `OBJECT_WAL` | Kafka Offset | Kafka Offset Range | Object Extent |
| Kafka / BookKeeper profiles | Kafka Offset | Kafka Offset Range | BookKeeper Extent |
| Pulsar / BookKeeper profiles | Pulsar Position/MessageId | ledger-keyed Pulsar Coverage | BookKeeper Extent |
| Pulsar / `OBJECT_WAL` | Pulsar Position/MessageId | ledger-keyed Pulsar Coverage | Object Extent |

## Kafka native path

The Kafka fork retains stock Kafka protocol and state-machine semantics around a Nereus-backed log:

- KRaft topic/partition identity, controller epochs, leader changes, and reassignment of the sole RF=1/minISR=1 replica;
- `UnifiedLog` append/fetch/list-offset/delete-records behavior;
- producer ID/epoch/sequence and duplicate handling;
- transaction visibility, markers, high watermark, and last stable offset;
- consumer groups and internal topics;
- topic compaction, retention, timestamp lookup, and leader-epoch queries;
- stock clients and admin APIs.

Nereus replaces the local segment durability path, not Kafka's protocol truth. Every Kafka Topic Protocol Binding keeps
Kafka Offset as its Position Domain across Storage Epochs. The Object profile absorbs useful AutoMQ patterns such as
group WAL, immutable objects, asynchronous materialization, bounded handoff hints, parallel timestamp lookup, and
inflight limits. The exact research commits are recorded in [source locks](source-locks.json); research is not executable
acceptance evidence.

For both Kafka BookKeeper-primary profiles, ADR 0086 combines Kafka logical offsets with a Pulsar-style per-partition
ledger lifecycle. Low-frequency run/generation roots route a Kafka range to one ACTIVE or SEALED run; packed immutable
range-index control entries route one complete RecordBatch to one DATA entry. Owner-local locators cover the ACKed
tail after the latest index checkpoint. Produce performs no per-append control-metadata mutation, and Fetch uses
run/block floor lookup plus a targeted entry read. Offset/entry admission remains ordered while bounded BookKeeper
futures overlap; the committed/ACK frontier never advances around a gap. Consumer-group offsets remain Kafka cursors,
not BookKeeper coordinates.

ADR 0087, as revised by NSIP-1, defines the protocol layer over this mapping. Kafka partitions expose distinct Allocated, profile-Durable,
Readable/LEO, HW, and LSO boundaries; Object materialization and checkpoint coverage are not visibility frontiers.
Offset admission validates PID/epoch/sequence against committed plus speculative state before allocation. Ordered
publication installs locators, producer state, transaction/aborted state, and leader-epoch state atomically before LEO
or success ACK. Under RF=1/minISR=1, both `acks=1` and `acks=all` await complete profile durability, legal shared
commit qualification, and coherent publication; `acks=0` follows the same admitted write path without a success reply.

One Controller-selected Owner allocates offsets and runs the Kafka state machine. BK HW advances only with a complete,
continuous, legal shared commit whose locator and recoverable producer/transaction/leader-epoch state are coherently
published. LSO derives from that HW and the first unstable transaction. BookKeeper quorum supplies physical durability,
not another logical replica or a substitute ISR. On failure, native Controller reassignment selects an eligible new
Broker, which closes old admission, fences and recovers the legal checkpoint plus tail, then activates only its current
generation. The focused native process checks exercise both BookKeeper-primary profiles and the Object WAL profile.
The Object route persists and verifies its exact grant before entering the native assignment gate. Under that gate,
the current Owner publishes the coherent append and signals affected Fetch waiters. A revoked Owner leaves the grant
for successor recovery. The successor closes and recovers the prior Owner's history before opening its own.

Replica/read-uncommitted/read-committed Fetch use LEO/HW/LSO, delayed Fetch waits on local frontier changes, and
compaction lookup uses floor plus coverage check plus successor. Read-committed returns native batches through LSO
plus aborted-transaction response metadata rather than filtering bytes as a storage-only shortcut. Each Fetch pins one
coherent local view and performs no normal-path remote metadata lookup. Native Object WAL finds the first
covering extent in that local view and reuses a disposable sequential position only when its fence, view
version and requested offset still match.

The 0.2 internal Deployment policy selects `BOOKKEEPER_WAL_ONLY` for `__consumer_offsets` and
`__transaction_state`. `__share_group_state` remains fail-closed until its own explicit internal policy is frozen; no
internal topic inherits a tenant default.

V2 Kafka metadata is enabled only by fresh-bootstrap finalized `nereus.storage.version=2`; the V2 build supports only
`[2,2]`, rejects level-1 V1 state, and forbids runtime upgrade or downgrade. A successful native CreateTopics item
atomically publishes logical aggregate schema v1 with the topic records. The sole exact, case-sensitive/no-trim input-
only pseudo-config `nereus.storage.profile` is resolved and removed before native `ConfigRecord` emission; Aggregate is
sole persisted authority, DescribeConfigs may synthesize a read-only projection, and AlterConfigs mutation is rejected.
Classifier v1 contains only the three pinned Kafka built-ins; Streams/Connect/MM2/`__remote_log_metadata` use the user-
topic path. Every successful TopicImage topic has one aggregate. Validate-only performs the same resolution/validation/
exact cumulative-batch admission and emits no records; native failed items leave no residue while per-topic partial
success remains. Admission counts the actual native configuration-derived records, including applicable
`ClearElrRecord`, and uses request-order greedy linear exact sizing. One generated non-flexible typed record at API key
32000 is owned by `TopicImage`; completed snapshot
order places it between `TopicRecord` and that topic's partitions, and topic removal cascades without a second delete
authority. Ordinary MetadataLoader publication validates only touched topics; snapshot/bootstrap validates every live
topic.

### Kafka product target

“Stronger than AutoMQ” is evaluated per profile:

- `OBJECT_WAL`: with the same machine class, object-store conditions, replication/durability, record set, and Object
  request budget, no agreed correctness or compatibility regression is allowed; throughput/cost and p99 thresholds are
  pinned before M8, with at least one material product advantage demonstrated.
- BookKeeper profiles: lower latency is an explicit higher-cost option and is not presented as an equal-cost Object
  comparison.
- mixed topic profiles: isolation, fairness, recovery, and operations are first-class differentiators.

The acceptance source is an exact clean AutoMQ commit and receipt, never the moving word “latest”.

## Pulsar native path

`BOOKKEEPER_WAL_ONLY` must preserve the native ManagedLedger call path and feature behavior before Nereus-specific
advantages are counted. `BOOKKEEPER_WAL_ASYNC_OBJECT` uses ManagedLedger ledger/offload metadata as sole lifecycle
authority and offloads sealed non-current ledgers as one deterministic data/root Object pair through a Nereus
`LedgerOffloader`; persisted attempt metadata pins key derivation and a bounded root is verified through the real read
path before success. The root is bounded NPO1; native dual-source reads use at most one whole-range fallback, and exact
Object revalidation plus BK read-pin drain precedes BookKeeper-source deletion. NPO1 indexes independently verifiable,
gap-free NPD1 multi-entry blocks, and one ManagedLedger-owned composite handle owns Object/BK children and fallback.
`RETAIN_BK` keeps delete state NONE; `DELETE_AFTER_VERIFIED` irreversibly advances through BK_DELETE_INTENT/DONE, and
only DONE proves physical absence.
`OBJECT_WAL` uses an explicit ObjectManagedLedger path plus a reserved-slice Pulsar-cell MetadataStore/Oxia virtual-
ledger authority. While admitted, one bounded Registry for the immutable ledger-ID compatibility namespace proves all
slice ranges non-overlapping/never-reused and binds a bounded inline writer commitment plus exclusion interlock. The
namespace derives from exact BookKeeper `INSTANCEID`, and 0.2 admits only a root authoritatively absent immediately
before init; changed identity/format is not cleanup proof. Slice owner
identity is the immutable Pulsar Protocol Cell, retirement is
permanent, `k=40` plus 64 KiB/256-lifetime/192-byte-row bootstrap caps bound the registry, and 0.2 forbids every
resize/second-slice
path and fails closed at exhaustion. The profile accepts its cost-first
latency tradeoff. Every path keeps
`PulsarPosition(ledgerId, entryId)`, MessageId, and the ledger chain as protocol truth; Object/BookKeeper coordinates
remain Physical Extents. The [closed M3 Final](detailed_design/m3/README.md#hard-frozen-closure) selects `RANGE_64`;
M6 consumes its exact packaged-source admission and does not reopen allocator selection. An installed
ManagedLedger-incarnation grant survives owner-only head fencing, takeover may finish the same RESERVED grant,
and at most one stale candidate burns. M3's source-bound scale evidence does not replace M6 Broker integration or M8 parity.

Pulsar V2 ownership admission requires a collision-resistant acquisition identity and authoritative witness A/B around
the exact selector/aggregate read. The Oxia-backed MetadataStore ELM integration must consume the qualified P1 acquisition transitions and
session/gap invalidation hook; direct GET/Stat/CAS primitives alone do not establish ownership qualification. MetadataStore ELM with syncer disabled and all ownership writers upgraded is the initial capability
gate. A gap-safe invalidation sequence and CAS install one atomic ACTIVE fence word;
position admission captures it and every success completion/ACK rechecks equality. Endpoint, boolean ownership,
resettable version, eventual TableView, or best-effort watch is insufficient. Unsupported ownership backends fail V2
topic/Cell admission without disabling stock non-V2 Pulsar paths. Same-session reconnect preserves at most identity and
must repeat A/read/B; SessionLost/process restart rotates broker incarnation, and any real service-unit reacquisition
uses a new acquisition ID.

Object frames retain exact assigned Kafka RecordBatch bytes or exact Pulsar ManagedLedger entry bytes after only the
outer Object envelope is decoded. Kafka makes all frames from one partition storage append an all-or-none commit set;
Pulsar makes one entry one frame/commit set. CRC32C/v1 protects each protocol-native frame blob while the native protocol
checksum remains independently validated. NWG1 stores its authoritative binding contexts and append-unit directory in
the Object body; one Kafka commit set never spans ObjectExtents and every frame block is independently decoded. NWG1
uses one KMS-wrapped run key, domain-separated per-Object keys, and disjoint authenticated directory/frame nonce
domains. Every known leaf carries the bounded exclusive directory-prefix end, so routine reads need no provider proof
or extra HEAD before prefix+frame GET. Permanent `OBJECT_LATENCY/BALANCED/COST` class IDs `0/1/2` bind
keys/HKDF/nonces to lane-local sequences under one Root/pointer. Checkpoint pages inventory provider-resolved extents
through one publisher-epoch-fenced predecessor/vector chain, not one chain per lane and not member ACK state; uncovered
tails still require LIST and the sealed final vector inventory is mandatory.

`LaneExtentResolvedThrough` is physical recovery order, while each binding's Position Domain owns
`BindingDurableFrontier`. Before position allocation, tracker slot and active-tail locator budget reserve together. One
full 64-bit owner-local ticket represents one complete Kafka commit set or Pulsar entry and never enters wire/API/config.
Normal ring/window completion and bounded collect/sort recovery perform no metadata read. One shared
`VerifiedExtent` feeds protocol-specific, range-aggregated active-tail locators; locators publish before Readable/Durable
frontiers and ACK. Shared Object/header/directory failure blocks all members; a later frame/commit-set-local failure
remains isolated to that complete binding unit.

Append does not publish a read snapshot per ACK. Low-frequency source-selection generations are pinned allocation-free
for one Binding-scoped Kafka fetch/range or Pulsar Object-WAL `readEntries` batch through a bounded cross-Binding slot
pool and generation-tagged hazard capture. A multi-Binding Kafka Fetch reserves all needed slots or releases every
partial reservation before protocol-legal split/failure. One ABA-safe lease word remains pinned through terminal source
drain; cancellation and late callbacks cannot force-clear it. One pinned snapshot may span disjoint manifest and
active-tail ranges while each Kafka commit set/Pulsar entry remains source-pure. Pulsar sealed-ledger async-offload
keeps its separate whole-range source-pure fallback. Object-WAL locator/protection retirement requires fenced no-
fallback E+1 granted by the same Binding selector CAS that closes E and carries its anchor. Each source releases against
its own `[first_i,sharedLast]` proof interval through an exact CAS; N members retain bounded O(N) recovery work. Proofs
bind irreversible terminal cuts and immutable capability evidence. Quarantine blocks batch retirement/capacity but not
eligible siblings. One small selector-owned inline anchor set preserves an emergency STOPPED envelope; terminal safety
uses the immutable candidate/closed verifier and pruning remains asynchronous. Completed batch metadata only moves by
irreversible same-key `FULL_V1 -> RETIRED_V1`; compact tombstones remain permanent in 0.2 and never authorize source
GC. Ordinary reads perform zero remote metadata I/O.

The parity matrix covers at least:

- append/read and exact MessageId/Position behavior;
- batched entries, properties, schema, and checksum fidelity;
- durable subscriptions, individual/cumulative ACK, seek, reset, and backlog;
- retention, ledger rollover, unload, ownership transfer, broker restart, and BookKeeper recovery;
- topic compaction and offload fallback;
- transactions, delayed delivery, replication, deduplication, and system topics to the extent enabled by the selected
  runtime contract;
- admin and client compatibility against the pinned native Pulsar baseline.

The native acceptance baseline and thresholds are still unpinned; M8 must pin them before execution. No V1 Pulsar
receipt is inherited as V2 parity evidence.

## M6 process activation matrix

This is the accepted integration target after M5. In the focused native Kafka path, a BK-only deployment admits
`BOOKKEEPER_WAL_ONLY`; an async Object deployment also admits `BOOKKEEPER_WAL_ASYNC_OBJECT` with a qualified
Object provider. The bounded Object WAL path admits `OBJECT_WAL` and BK-only bindings with a qualified Object
provider and Vault Transit key. An explicit unadmitted profile fails CreateTopics, including validate-only, without
creating a Binding. Each Broker checks the persisted Binding and Storage Epoch before opening its storage route.
Focused coverage does not promote a row or grant deployment authority.
Kafka rows require fresh feature-2 bootstrap and RF=1/minISR=1. The existing no-online-migration boundary applies to
both protocols.

The focused native Pulsar BookKeeper route uses an explicit Protocol Cell and pre-admitted Bindings. The Broker
checks the live Oxia namespace, the provisioned BookKeeper namespace and the actual BK client artifact, then opens
the topic through the M5 ManagedLedger factory. Pulsar system topics still use their native storage. A native
ownership or aggregate notification revokes admission until a fresh authoritative read. An in-flight operation
cannot acknowledge with a later authority word. Planned unload and shutdown wait for the original factory and Cell
drain before releasing ownership. The focused process check is `scripts/run-v2-m6-pulsar-bk-process-check.py`.

| Protocol/profile | Startup and routing authority | Recovery and lifecycle gate | Mandatory M6 exit |
| --- | --- | --- | --- |
| Kafka `BOOKKEEPER_WAL_ONLY` | native Controller assignment and exact feature-2 aggregate; admitted BK capability, Cell and internal-topic policy | close old run admission, fence all legal ledgers, recover common checkpoint plus tail, install stock protocol state and activate only current generation | `V2-KAF-META-004/005`, `V2-KAF-PROC-001` and applicable `V2-KAF-DATA` rows |
| Kafka `BOOKKEEPER_WAL_ASYNC_OBJECT` | Binding selects BK primary plus admitted Object materializer and read/retention owners | same BK ACK contract with Object unavailable within admitted BK/capacity limits; recover published Object/BK generation and exact fallback protection before retiring sources | `V2-KAF-PROC-002`, `V2-KAF-DATA-012/013/022` plus applicable Kafka semantics |
| Kafka `OBJECT_WAL` | Binding selects qualified Object provider/KMS, run authority and per-partition authorization Head | exact PUT proof plus durable grant precede publication; close grants and recover selected NWKCP1 plus authorized tail; UNKNOWN cannot reopen admission | `V2-KAF-PROC-002`, `V2-KAF-DATA-021` and applicable Kafka semantics, including 012/013/022 where the route uses them |
| Pulsar `BOOKKEEPER_WAL_ONLY` | native Broker ownership and ManagedLedger factory with exact namespace, Cell and source scope | preserve native Position/MessageId, cursor and ledger recovery; qualified M5 enrollment and terminal drain | `V2-PUL-PROC-001/004` |
| Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT` | same native authority plus SourceSafe offloader, actual retention policy and enrolled writer/reference owners | sealed-source lifecycle reuses M5; newly produced NPD1/NPO1 must separately acquire birth/quota/member admission before publication | `V2-PUL-PROC-001/002/004` |
| Pulsar `OBJECT_WAL` | Broker-selected ObjectManagedLedger bridge, qualified ownership backend, namespace Registry and exact `RANGE_64` artifacts/receipt | typed chain and entry bytes survive rollover/crash/takeover without ID reuse; preserve source pins and exact original UNKNOWN candidates | `V2-PUL-PROC-003/004` |

All rows reject a missing or mismatched Binding/profile/Cell/namespace/capability, incompatible persisted state,
unqualified ownership backend, or superseded recovery generation before write admission. Internal Kafka
`__consumer_offsets` and `__transaction_state` stay `BOOKKEEPER_WAL_ONLY`; `__share_group_state` stays rejected.
No Cell shares another Cell's session, credentials, GC capability or close authority. Mixed profiles must prove this
through actual requests and recovery, not configuration parsing alone. An Object outage may eventually exhaust
existing retention/capacity budgets and stop new admission; asynchronous offload never adds Object durability to an
already admitted BK append's ACK conditions.

The focused Kafka Object provider check reopens the configured S3 bucket incarnation through its original Oxia
namespace and the exact registered Cell profile. It uses the named credentials in the Cell's configured file and
verifies an immutable conditional write and full read on real MinIO. This qualifies only the provider adapter;
Kafka Object profiles still require native materialization, read and recovery wiring before Controller admission.

The Kafka BK adapter for `__consumer_offsets` keeps each native coordinator RecordBatch intact and
tracks its producer epoch and transaction range. The group coordinator starts each internal write at
sequence zero; the adapter does not treat that sequence as a client retry identity. Append and cold
replay select the same interpretation from the exact internal topic binding. A committed offset becomes
visible after COMMIT, and ABORT preserves the previous committed offset.

M6 Kafka evidence uses real Broker/Controller processes and clients for Produce, Fetch, PID retry, transaction
COMMIT/ABORT, transactional offset commit, group operations, Admin and snapshot/catch-up/restart. An ACKed record must
survive cold recovery; a timed-out idempotent request must retain its original offset; stale ready/coordinator callbacks
cannot activate. M6 Pulsar evidence uses an official client through the Broker for write/read, ACK, seek/reset,
backlog, rollover, unload and restart. Cancellation/close completion is not proof of physical I/O drain.

For new native offload output, the existing first sealed/offloaded-source qualification in M5 is only a dependency.
An already enrolled factory must reserve the actual output birth/quota and register complete member/reference facts
for NPD1/NPO1, publish through the native lifecycle, and retain the original candidate and charges across UNKNOWN.
Success and failure must both converge through actual terminal I/O and settlement. `RETAIN_BK` remains the default;
explicit `DELETE_AFTER_VERIFIED`, exact M4 release, native INTENT/DONE and physical absence are still required for
deletion. Cold backlog must remain readable after source replacement/deletion. M5's pre-enrollment offload fixture
cannot certify this new production path, and an NMS1 plan cannot be substituted for native NPD1/NPO1 identity.

The [implementation plan](08-implementation-plan-and-gates.md#post-m5-sequence-and-acceptance) assigns batch exits.
The [scenario matrix](09-scenario-evidence-matrix.md) and JSON own scenario status; all new process rows are
`PLANNED` with no receipt. M8's complete parity matrix does not replace these M6 process checks.

## Kafka/Pulsar secondary access and authority migration

The same business data may be exposed through the other protocol only as an Access Projection backed by a durable
Projection Map. The target is not a second Native Write Authority. Changing protocol authority uses a Migration Link
between source and target Topic Protocol Bindings; it is not a Storage Epoch transition.

ADR 0016 retains that boundary and dual-authority rejection but excludes projection mapping, secondary-protocol serving,
semantic state transfer, and authority-migration runtime from 0.2. Their detailed questions are deferred and do not
block the release. No design or implementation may use a universal logical offset as a shortcut or allow simultaneous
Kafka and Pulsar native writers.

## KoP

KoP is intentionally outside the 0.2 runtime and release gates. Its existing design is retained at
[KoP/Kafka compatibility](../v1/design/nereus-future5-kop-compatibility.md) with status “Designed / deferred”. V2 must
not
delete that design or claim its payload/coordinator mapping is implemented. Before activation it requires a fresh audit
against V2 bindings, protocol-native Kafka work, and the then-current KoP source.

Relevant tradeoffs: `T-PROTOCOL-01`, `T-MULTIPROTOCOL-01`, `T-FABRIC-01`, `T-POLICY-01`, `T-PROJECTION-01`,
`T-BENCH-01`, and `T-KOP-01`. Required scenarios: `V2-MULTIPROTOCOL-001`, `V2-FABRIC-001..003`,
`V2-PROJECTION-001`, `V2-POSITION-002..018`, `V2-OBJ-002/004..024`, `V2-READ-003..015`, `V2-BK-005..013`,
`V2-KAF-META-001..005`, `V2-POLICY-001..002`, `V2-KAF-001`, `V2-PUL-001`, and `V2-KOP-001`.
