# Phase 2 ManagedLedger Facade Detailed Design

本文档目录是 Future 2 的 implemented/final-gated code-level contract。F2-M0 在 2026-07-11 完成第一轮 API spike；
F2-M0R 补齐 append recovery、topic incarnation、role-aware Position、interface matrix 和 broker runtime
bootstrap。2026-07-12 的 F2-M0R2 使用锁定 commit 的真实 Pulsar checkout 重新核验接口和 broker 私有
调用路径，并关闭 metadata type collision、write-fence handoff、storage-state inspection、ack admission、
S3 provider 和 rollout capability 等实现前缺口。P15-M1-M6 已实现并通过 final
gate，包括 M0R2 发现的 exact cumulative logical-size handoff。F2-M1 projection/Position/entry foundation 也已
实现并通过 locked-composite gate。F2-M2 projection metadata 已实现：键空间、严格创建请求、四类
durable record、第三 codec registry、golden bytes、fake/real 单键 CAS/修复合同和 shared Oxia runtime
均通过普通与 Docker gate。F2-M3 已完成：配置/runtime/snapshot/callback、open/recreate/get-only
inspection coordinator、factory exact-name registry、writable/get-only ledger、append/direct-read/Position/
properties/lifecycle/write-fence、admin/cache/stats 已落地；真实 Object WAL、100-way same-name open、
out-of-order callback、uncertain recovery 和错误映射 gate 均通过。F2-M4 也已完成：read-only、
non-durable 和 durable-boundary cursor、coalesced tail poll/read-or-wait、local wake/cancel/close 已落地。
F2-M5 product-side runtime bootstrap is now implemented in `nereus-pulsar-adapter`：typed runtime/projection config、
cryptographic process identity、TCCL-compatible provider construction、shared Oxia/Object-WAL assembly and partial
failure cleanup have unit gates。The deployable S3 provider now satisfies the section-3.2 async/deadline、conditional
write、checksum、key/metadata、strict-range、redaction and startup-probe contract against pinned LocalStack。The Pulsar
fork now has typed broker fields、BookKeeper-default/Nereus lookup、runtime assembly、the deterministic `NSB1`
codec/keyspace/records，BookKeeper adoption and class-switch rejection，generation-safe open activation and
delete/recreate state-machine APIs，distribution dependency/license accounting and value-correct
`PersistencePolicies.equals`。`BrokerService` open now prepares the binding before factory selection and withholds the
ledger until activation succeeds。Loaded and unloaded delete paths capture the bound-class permit before policy
cleanup，delete through that class and publish the `DELETED` tombstone only after storage is terminal。Feature-admission
now resolves the managed-ledger config and immutable effective feature view from one local/global/namespace snapshot；
Nereus system/internal topics，replication，dedup，TTL/expiration，retention，compaction，backlog eviction，Pulsar
offload，entry filters，shadow/migration，auto-skip and managed-ledger interceptors fail before topic events，binding
claim or factory IO。The fork now installs that validated context before `PersistentTopic.initialize()` and gates
remote producers、transactional/marker/delayed publishes、unsupported subscription types、durable subscription
creation/recovery and transaction buffer operations before stock mutation；ordinary publish and non-durable
Exclusive/Failover subscribe remain admitted。The limited non-durable cumulative whole-entry ack is now validated as
the first `Consumer.messageAcked` action；rejections leave counters/pending state/cursor untouched，and admitted local
ack counters wait for cursor-future completion。The closed admin enum now gates loaded-topic and namespace
durable-subscription/backlog/cursor mutations，compaction/offload status and trigger paths，truncate、loaded shadow
updates and cluster migration；terminate/delete/unload remain admitted。Authoritative live-policy refresh and
same-snapshot replacement are now implemented：Nereus initialization and every live topic/namespace-policy hint
refetch one authoritative namespace/global/local tuple，validate it before `ledger.setConfig` or policy side effects，
and use a monotonic per-topic coordinator so stale success or failure follows the latest result。A rejected current
snapshot marks admission failed and closes the loaded topic while leaving its last accepted config untouched。
The broker capability path now publishes the reserved `nereus.storage-binding-protocol=1` property only after the
hybrid runtime is initialized and the concrete extensible registry is attached，rejects configured spoofing，and
requires two stable all-capable persistent-broker snapshots before the first Nereus binding claim。Binding-aware
loaded/unloaded topic persistence mutation and namespace persistence serialization are now implemented：both storage
classes' create-if-missing opens share the namespace lock，namespace class changes require version-CAS plus bounded
pre/post empty scans，and live binding class switches fail before policy mutation。Generation-safe broker write-fence
handoff is now implemented。The facade keeps the fence visible through `addFailed` and starts its terminal observer
only after that callback returns；the fork captures it before producer disconnect，coalesces by generation，retains an
early terminal until pending writes drain，fresh-checks a newer generation and fails closed on permanent recovery or
executor rejection。The shared binding coordinator now also gates conflicting first-create races and peer-broker
`CLAIMED/ACTIVE/DELETING/DELETED` restart/resume through a real CAS MetadataStore。F2-M5 is complete：a real
two-broker `PulsarService` gate uses real Oxia、pinned LocalStack Community S3 `4.14.0` and a real one-bookie
BookKeeper control path，then verifies not-yet-created topic policy、single/batched exact bytes and `MessageIdAdv`
coordinates、unload/reload、owner shutdown/takeover、original-owner process restart/reverse takeover、binding
generation/class、concrete ledger types and positive S3 object presence。F2-M6 then composed scenarios 3–8：
committed-response loss recovers one callback/Position，real Oxia restart repairs
both removed derived indexes，and close/trim/reopen/terminate/delete/recreate preserves retained Position and object
contracts。Its final slice closes scenarios 10–19：real BookKeeper enumeration proves virtual-ledger isolation，
watch-disabled independent runtimes prove remote polling wake，the stock `PersistentTopic` branch is explicitly
gated，a cross-layer Object-WAL upload failure proves callback/admission recovery，and the aggregate binding、feature、
ack、capability/policy and LocalStack suites are selected by the release gates。Scenario 19 additionally verifies
saved ordinary and middle-batch MessageIds in exclusive/inclusive Reader modes across unload、owner failover and
runtime restart。All 19 scenarios pass；F2-M6 and
Future 2 are complete。

2026-07-14 Phase 3 M0/M0R 已消费并复核本合同：F2 的 one-Entry/one-offset、stable virtual ledger、
Position roles、durable-boundary cursor、single-key metadata primitive、ObjectStore immutability 和 L0 trim API
足以承载 F3 single-root cursor/snapshot/retention protocol；无需修改 F2 MessageId 或 append correctness model。
F3 additionally uses a fresh per-writable-open owner-session claim over retention and all ACTIVE cursor roots before
topic publication；this is an F3 cursor fence and does not alter the locked F2 projection、MessageId or append model。
F3 code-level target见 `../phase-3-cursor-subscription/README.md`；其 M1 metadata/snapshot foundation 已
final-gated，M2 CursorStorage/retention state machines 与真实 Oxia/S3 recovery gate 也已 final-gated；
M3-M6 facade/broker/end-to-end runtime 仍待实现。

Future 2 的目标是在不改变 L0 storage truth 的前提下，为 Pulsar broker 提供
`ManagedLedgerStorageClass(name=nereus) -> ManagedLedgerFactory -> ManagedLedger` 兼容路径。
首个可执行版本只接受 `OBJECT_WAL_SYNC_OBJECT`；BookKeeper storage class 与 Nereus storage
class 可以在 broker 内共存，但这不表示 Nereus 的 BookKeeper primary profile 已实现。

## 1. Locked Inputs

| Input | F2-M0 lock |
| --- | --- |
| F2-M0R2 Nereus design baseline | `nereusstream/nereus@fb98174c99a7379deb684d6f8d5f1fa74517c5f5`（P15-M5） |
| Pulsar fork | `nereusstream/pulsar` |
| Pulsar API/source-review baseline | `100d3ef0ff7c7da36d497453b141ddff6f34a9d3` |
| Current product implementation | current Nereus commit（this document is committed with the F2-M6 completion code/gates） |
| Current local Pulsar implementation commit | `7efae25af39a15407c1397d9e1f4ac4658d09daa`（based on locked baseline；remote publication awaits repository permission） |
| Pulsar source-project version at that commit | `5.0.0-M1-SNAPSHOT`（source composite selector，not a published Maven snapshot） |
| Java/build baseline | Pulsar/Nereus build with JDK 21 or 25；published production classes target Java 17 bytecode |
| Executable Nereus profile | `OBJECT_WAL_SYNC_OBJECT` only |
| Completed F2 prerequisite | P15-M6 carries protocol-neutral `AppendResult.cumulativeSize` from existing `CommittedAppend` truth；final-gated 2026-07-12 |

The original F2-M0 probe predated Phase 1.5. F2-M0R2 therefore replaces that Nereus input with the final-gated P15
commit above. The API/source review used
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar` at the exact clean target commit；the compile probe and every
recorded interface/call-site blob were revalidated there on 2026-07-12。The same implementation checkout now carries
the reviewed local fork commits through `7efae25af3`。Exact baseline evidence is in
`01-pulsar-api-spike-and-repository-boundary.md`。

An upgrade to another Pulsar commit invalidates the lock. The API probe and broker integration call-site
audit must pass again before implementation continues.

## 2. F2-M0 / F2-M0R / F2-M0R2 Decisions

1. The broker passes `TopicName.getPersistenceNamingEncoding()` and the facade treats it as an exact opaque
   identity. A topic projection has a monotonic incarnation. Incarnation 1 uses
   `"pulsar-ml-v1\0" + managedLedgerName + "\0" + "1"`; delete/recreate increments the incarnation and therefore
   creates a new deterministic stream ID and a newly allocated, durably persisted virtual ledger ID. The facade never
   reconstructs or normalizes a topic URI.
2. F2 v1 creates one stable virtual ledger for one stream/incarnation and does not roll it. A positive, high-range
   `virtualLedgerId` is allocated through one Oxia allocator key. The value is a compatibility coordinate,
   never a BookKeeper ledger ID.
3. The durable payload mapping is `PULSAR_ENTRY_V1`, stored as immutable L0 stream attribute
   `nereus.payloadMapping`. One Pulsar Entry maps to one `AppendEntry(recordCount=1)` and therefore one stream offset.
   `Position.entryId == stream offset`. Pulsar `batchIndex` remains a sub-index inside the entry and does
   not consume another stream offset.
4. Topic projection is authoritative for the current name -> incarnation/stream/virtual-ledger identity.
   L0 stream state remains authoritative for append/seal/delete; the projection lifecycle field is only a monotonic
   mirror. Virtual-ledger and position-index records are
   idempotently repairable derived records. No workflow assumes a multi-key atomic Oxia transaction.
5. Position metadata is formula-based, not per-entry: mapping version 1 is
   `offset = entryId`. F2 does not add one Oxia write per append.
6. Broker hybrid mode means stock `bookkeeper` and Nereus `nereus` storage classes coexist. The
   Nereus facade rejects every non-object-WAL Nereus profile before opening a topic.
   A fork-owned single-key storage-class binding serializes first create/delete/new-lifetime class selection;
   switching a live topic between BookKeeper and Nereus is rejected rather than creating an empty second view.
7. Durable mark-delete, individual acknowledgements and ack holes remain Future 3. F2 can support
   read-only and non-durable cursor flows; durable cursor mutation must fail explicitly.
8. `terminate` and logical `delete` need explicit L0 lifecycle operations. F2-M3 cannot synthesize these
   only in facade metadata.
9. `KNOWN_COMMITTED` / `MAY_HAVE_COMMITTED` append failures need an L0 `AppendAttemptId` and
   `recoverAppend` method. Outcome certainty alone cannot reconstruct the committed `Position` or safely resume a
   suspended lane.
10. All asynchronous callbacks are terminal exactly once. Callback invocation is outside locks and on a
   designated callback executor. A method returning `CompletableFuture` never throws synchronously.
11. Virtual ledger IDs are never sent to a BookKeeper client, ledger handle, offloader or BookKeeper
    metadata path. Unsupported offload/BookKeeper-shaped operations fail explicitly.
12. `ManagedLedger.getFirstPosition()` returns the position immediately before the first retained entry. Direct
    entries, next-read positions, mark-delete positions and inclusive max positions use separate conversion methods.
13. F2 consumes the Phase 1.5 generic append/read result only through logical range/payload fields；it never derives
    Position from an object or BookKeeper target。
14. F2 projection identity is named `ManagedLedgerProjectionIdentity`；the shorter `ProjectionIdentity` name is
    already owned by Phase 1.5 L0 projection-ref decoding and cannot be redefined in the same package。
15. The fork never infers Nereus durable state from `asyncExists` alone。A get-only
    `inspectStorageState` surface distinguishes `MISSING/ACTIVE/SEALED/DELETING/DELETED` and treats a published
    projection with a missing L0 head as corruption。
16. A callback-time unresolved append exposes one product-owned write-fence view。The fork keeps
    `PersistentTopic` fenced until that exact attempt reaches committed/proven-not-committed；the stock
    pending-write auto-unfence path cannot bypass it。
17. F2 broker subscriptions are limited to non-durable, whole-entry cumulative progress。Individual ack、durable
    ack and batch-index ack are rejected in `Consumer.messageAcked` before cursor or pending-ack mutation；Reader and
    direct read-only flows remain supported。
18. Production Object WAL requires the code-level S3-compatible provider contract in document 07；a local file store
    and an unspecified object client are not release evidence。
19. Runtime construction precedes Pulsar broker-ID creation。It generates a cryptographic process-run ID and uses
    `pulsar-f2/{processRunId}`；broker capability is published later as the exact lookup-data property
    `nereus.storage-binding-protocol=1` only after hybrid storage is ready。
20. Missing binding plus existing BookKeeper may be adopted as generation-1 `ACTIVE`；any Nereus projection with a
    missing binding is corruption because the projection embeds its binding generation and the binding tombstone is
    never removed。Binding bytes use the closed `NSB1` codec。
21. Empty-namespace storage-class policy update and both BookKeeper/Nereus first creation share a namespace
    coordination permit plus post-claim policy revalidation；a pre-update topic scan alone is not sufficient。
22. F2 has exactly one limited broker ack shape：nontransactional、non-durable、one-position cumulative、whole-entry
    and no persisted-ack request。Every other shape fails before timestamp/counter/transaction/pending-ack/cursor
    mutation。
23. S3 `ifAbsent` is one PUT with `If-None-Match:*`；exact CRC32C is computed independently of ETag，range/zero-length
    responses and timeout/cancellation have closed behavior，and production rejects local/file providers。
24. A successful append must advance both local tail and exact lifetime logical size without a fallible second read。
    Internal `CommittedAppend` already has that value；P15-M6 carries it in generic `AppendResult` before F2-M1。
25. `ManagedLedgerFactory.getManagedLedgerPropertiesAsync` returns an empty map for a genuinely missing ledger so
    broker pre-create probing matches stock behavior；`DELETING/DELETED` and corrupt published state still fail
    closed rather than masquerading as missing。
26. An explicit `PositionFactory.EARLIEST` always starts at the retained trim offset. A concrete non-durable
    `startCursorPosition` is already consumed and reads its next retained Entry. Null、`LATEST` and concrete future
    positions consult `InitialPosition`; the two-argument overload defaults Latest. Direct seek/reset positions remain
    read targets and do not reuse the cursor-start increment。
27. The target `ServerCnx` supplies an empty non-null `KeySharedMeta` even for ordinary subscriptions。F2 accepts
    that empty carrier for admitted Exclusive/Failover readers but rejects any actual hash range and every
    unsupported subscription type。
28. In hybrid mode only，topic persistence GET/SET/DELETE may validate namespace/bundle ownership and operate before
    the topic exists，without creating it。BookKeeper-only mode retains the stock NotFound behavior。Hybrid startup
    also requires persistent topics and `ExtensibleLoadManagerImpl` before any Nereus client is constructed。

## 3. Phase 1.5 Production Prerequisite

F2-M0R was completed against the exact Phase 1 implementation baseline and remains the facade authority。
`../phase-1.5-core-storage-foundation/` P15-M1-M6 have implemented and proved the complete prerequisite：

- generic `ReadTarget`/`AppendResult`/`ResolvedRange` with Object WAL strict parity；
- legacy/new L0 metadata compatibility；
- `AppendAttemptId` and exact `recoverAppend`；
- stream `seal` and logical `delete`；
- protocol-neutral F2 prerequisite fixtures and unchanged Phase 1 gates。

M0R2 found one remaining in-memory handoff gap：generic `AppendResult` omitted the committed record's exact cumulative
logical size。P15-M6 added only that field/fixture；no durable metadata/WAL byte or commit/recovery boundary changed。
Both Phase 1.5 gates passed on 2026-07-12，and F2-M1 has consumed that complete prerequisite。

F2 must consume those APIs rather than reintroducing temporary L0 types in `nereus-managed-ledger` or implementing
head/replay/lifecycle truth in projection metadata。

## 4. Repository Boundary

| Repository/module | Ownership |
| --- | --- |
| `nereus-api` | P15-M6 implements generic result cumulative-size handoff；F2 consumes result/recovery/lifecycle API and adds no Pulsar or duplicate L0 type |
| `nereus-metadata-oxia` | Exact managed-ledger name/hash helper；F2 keyspace, records, codecs, fake/real projection metadata contract |
| `nereus-managed-ledger` | Projection model, entry codec, factory, ledger, entry/read-only/non-durable cursor facade |
| `nereus-pulsar-adapter` | Product-owned configuration/bootstrap helpers that do not depend on Pulsar private internals |
| `nereusstream/pulsar` fork | hybrid provider, cross-class binding/migration guard, broker config/distribution wiring, policy and integration tests |

`ManagedLedgerStorage` and `ManagedLedgerStorageClass` are Pulsar `@Private @Unstable` interfaces.
The class that composes stock `ManagedLedgerClientFactory` with the Nereus factory therefore belongs in
the Pulsar fork. Stable product logic stays in Nereus.

## 5. Document Map

| Document | Purpose |
| --- | --- |
| `01-pulsar-api-spike-and-repository-boundary.md` | Locked API evidence, broker call sites and code ownership |
| `02-projection-and-entry-contract.md` | Virtual ledger, Position, batch and Entry byte contracts |
| `03-oxia-metadata-and-recovery.md` | Keyspace, records, single-key CAS creation and repair protocol |
| `04-facade-state-machines-and-compatibility.md` | Factory/ledger/cursor lifecycle, callbacks, errors and unsupported surface |
| `05-implementation-plan-and-gates.md` | F2-M1 through F2-M6 files, tests, gates and completion criteria |
| `06-code-level-interface-contract.md` | Exact L0 prerequisites, class contracts and complete locked method behavior |
| `07-runtime-bootstrap-and-reference-review.md` | Hybrid provider/config/admission, resource ownership and `pulsar-storage` review |
| `spikes/PulsarManagedLedgerApiProbe.java` | Compile-only exact-signature probe |
| `spikes/pulsar-api-probe.init.gradle` | Temporary Pulsar source-set injection; does not edit the Pulsar checkout |

## 6. F2-M0 Evidence

From the exact locked Pulsar checkout:

```bash
NEREUS_F2_PROBE_DIR=/path/to/nereus/docs/phase-2-managed-ledger-facade/spikes \
  ./gradlew \
  -I /path/to/nereus/docs/phase-2-managed-ledger-facade/spikes/pulsar-api-probe.init.gradle \
  :pulsar-broker:compileF2ApiProbeJava
```

Observed initially on 2026-07-11 and repeated against the exact target checkout on 2026-07-12 (84 scheduled tasks):

```text
> Task :managed-ledger:compileJava
> Task :pulsar-broker:compileJava
> Task :pulsar-broker:compileF2ApiProbeJava
BUILD SUCCESSFUL
```

The spike proves compilation of the selected storage/factory/append/read/terminate/cursor boundary. The F2-M0R
source audit additionally locks `ReadOnlyCursor`, `ReadOnlyManagedLedger`, `Entry`, `Position`, callbacks,
exceptions, config/scan/admin DTO, cache and MXBean blobs in `06`, plus the broker-private admission/binding call-site
blobs in `01`. It
does not prove implementation behavior, callback ordering, resource ownership or broker integration.
Those are later milestone gates.

F2-M1 additionally uses the locked checkout as a Gradle composite and provides：

```bash
./gradlew phase2M1Check
```

The gate verifies exact clean Pulsar HEAD、the strengthened L0 source/dependency boundary、Phase 1/1.5 regressions、
deterministic projection/name/Position mappings and Pulsar entry codec/buffer ownership tests。The API compile probe
was repeated after the M1 implementation and remained green。

## 7. Milestone State

| Milestone | State | Exit |
| --- | --- | --- |
| F2-M0 design/API spike | Complete | Locked target and successful compile probe |
| F2-M0R code-level review | Complete | Initial documents 02-07 and L0 prerequisite review |
| F2-M0R2 code-level closure | Complete | Exact target checkout revalidated；compile/type/state/runtime gaps closed in docs |
| Phase 1.5 P15-M1-M5 | Complete | Generic L0/recovery/lifecycle implemented；ordinary and Docker final gates pass |
| Phase 1.5 P15-M6 | Complete | `AppendResult.cumulativeSize` from existing committed truth；ordinary and Docker final gates pass |
| F2-M1 projection model | Complete | Pure model/codec、locked Pulsar composite and restart-stable mapping tests |
| F2-M2 projection metadata | Complete | Model/keyspace/codec、fake/real CAS/repair、shared runtime and Docker restart/race gates |
| F2-M3 ManagedLedger facade | Complete | Writable/get-only factory/ledger、recovery/lifecycle/admin/cache/stats and locked interface audit gates pass |
| F2-M4 cursor boundary | Complete | Read-only/non-durable/durable-boundary cursors and shared tail polling implemented/tested |
| F2-M5 broker integration | Complete | Hybrid bootstrap/binding/admission/capability/policy/write-fence/peer lifecycle plus real dual-broker Oxia/LocalStack/BookKeeper restart/failover E2E pass |
| F2-M6 final acceptance | Complete | all scenarios 1–19 are executable slices；ordinary and Docker-backed aggregate gates pass with exact source and storage-isolation locks |

Future 2 is complete。F3/F4 may now consume this facade contract；neither capability is implied by F2 completion。
