# Nereus Commit Protocol

> 状态：Current cross-track protocol
> Append truth 已与 Phase 1 stream-head CAS 实现合同对齐；Phase 1.5 generic target/recovery/lifecycle 已实现并
> final-gated；F3 cursor protocol 已完成 M0/M0R design gate 与 M1-M6 implementation/final gates；generation、
> async/GC 已完成 F4-M0 code-level target design，但尚未实现；txn、catalog 仍为 target design。

## 1. Purpose

本文统一以下提交边界：

- primary WAL durability；
- stable append/producer result；
- exact in-process append recovery and stream lifecycle；
- generation-0 read-index materialization；
- async object materialization and compaction generation publish；
- cursor、transaction、SBT/SDT and GC commits。

精确 Phase 1 records/keys/algorithms 见
`../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md` 和
`../phase-1-core-stream-storage/04-core-state-machines.md`。
Phase 1.5 target records/state machines见 `../phase-1.5-core-storage-foundation/README.md`。
F3 cursor exact contract见 `../phase-3-cursor-subscription/README.md`。
F4 generation/async/recovery/GC exact target contract见
`../phase-4-compaction-generation/README.md`。

## 2. Commit domains

Nereus 不把所有状态塞进一个“commit”概念：

| Domain | Authority | Linearization/publish point |
| --- | --- | --- |
| Logical append | Oxia stream head | successful head `putIfVersion` |
| Commit identity | reachable immutable commit log | head links the record |
| Generation-0 read/replay index | Oxia derived records | records materialized and validated |
| Stream seal | Oxia stream head | `ACTIVE -> SEALED` head CAS |
| Logical stream delete | Oxia stream head | first `ACTIVE/SEALED -> DELETING` head CAS |
| Higher-generation target | Oxia generation index | final index key same-key `PREPARED -> COMMITTED` CAS |
| Cursor progress | Oxia `CursorStateRecord` | one-root cursor version-CAS |
| Cursor trim protection | Oxia `CursorRetentionRecord` + L0 trim truth | retention pending/completion CAS protocol |
| Transaction result | transaction state + required stream markers | explicit state-machine terminal CAS |
| SBT visibility | table catalog | catalog snapshot commit |
| SDT visibility | target catalog | idempotent delivery commit |
| Object deletion | GC state + physical delete | all references checked, delete recorded |

这些 domain 可以按顺序关联，但不能假装存在跨 Oxia shard、object store 和 external catalog 的
全局事务。

## 3. Global invariants

1. Any successful append has durable primary WAL bytes and a stable stream-head commit。
2. `streamId + offset` is the only logical ordering coordinate。
3. `committedEndOffset` and `commitVersion` are monotonic per stream。
4. Reachable committed ranges are dense；orphan intents/bytes never create visible gaps。
5. A physical multi-stream object does not imply cross-stream visibility。
6. Generation replacement changes location/format, not offsets or protocol projections。
7. A snapshot/object becomes authoritative only through its Oxia/catalog reference。
8. Watch and object list never replace a linearizable metadata read。
9. Failure after a domain's irreversible point cannot be reported as known rollback。
10. Every derived state has a source、identity、repair budget and corruption policy。

## 4. Current Phase 1 key model

Readable form：

```text
/nereus/clusters/{cluster}/streams/{streamId}/head
/nereus/clusters/{cluster}/streams/{streamId}/commit-log/{commitId}
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd19}/{generation19}
/nereus/clusters/{cluster}/streams/{streamId}/committed-slices/{objectId}/{sliceHash}
/nereus/clusters/{cluster}/objects/{objectId}/manifest
/nereus/clusters/{cluster}/objects/{objectId}/references
/nereus/clusters/{cluster}/objects/{objectId}/gc
```

`OxiaKeyspace` applies canonical component encoding。Same-stream operations carry
`PartitionKey(streamId)`；object records carry `PartitionKey(objectId)`。Cross-partition atomicity is not
assumed。

The Phase 1.5 target keeps these head/commit/index keys and adds
`/streams/{streamId}/committed-appends/{commitId}`。The existing commit-log/offset-index key may contain a frozen
legacy object record or a new generic-target record；envelope record type selects the decoder and one head may link a
dense mixed-version chain。P15-M2/M3 implemented this dual-read/new-write behavior and P15-M5 final-gated it。

## 5. Append session protocol

Append session lives inside `StreamHeadRecord` so the same authoritative CAS can validate writer state。

### 5.1 Acquire/renew

```text
read head
  -> validate ACTIVE
  -> allow empty / same writer / permitted expired steal
  -> build next session snapshot
  -> putIfVersion(head)
```

Rules：

- epoch increases on ownership transfer；
- same non-expired writer renews leaseVersion without changing epoch/token；
- expired session stealing requires explicit option；
- routing preference does not grant write authority；
- append validates the snapshot in the latest head, not a broker cache alone。

### 5.2 Compatible head changes

Renew and trim also change the head key version。An append CAS conflict caused by same-writer renew or a
compatible trim update can refresh and retry if all append preconditions still hold。It must not be
misclassified as fencing；true stale epoch/token takes precedence over offset conflict。

## 6. Object WAL preparation

### 6.1 Build and upload

```text
append batches
  -> deterministic WAL layout and slice ids
  -> size/format validation
  -> immutable object upload
  -> storage checksum confirmation
  -> object manifest put/validate
```

Manifest requirements before stream commit：

- object id/key/type/format/length；
- canonical WAL checksum and exact-storage checksum；
- writer id/run id/epoch/version；
- slice descriptor identity and checksums。

Manifest existence does not make a slice visible。

### 6.2 Multi-stream rule

Each slice commits independently。If A commits、B is fenced and C remains pending：

- A's logical range is visible；
- B/C are not reachable from their stream heads；
- the shared object remains protected by A；
- retry/GC treats each slice identity separately。

## 7. Stable append protocol

### 7.1 Deterministic commit identity

Phase 1 builds a canonical object identity over every durable replay field。Phase 1.5 uses a domain-separated v2
identity containing the same logical fields plus a canonical tagged read-target identity：

```text
stream / physical read-target identity
writer epoch and fencing-token hash
expected offset and counts
logical bytes and event-time bounds
payload format / schema refs / projection identity
target-specific checksums and read framing/index identity
```

`commitId` is a deterministic hash of that identity。The same physical slice replay must compute the same
id；different identity must not alias。

`previousCommitId`、next `commitVersion` and cumulative size are CAS-snapshot-derived record fields。They
must match when an existing intent is reused，but they are deliberately not hashed into the physical-attempt
`commitId`；otherwise compatible renew/trim retries could not compute one stable id before head CAS。

### 7.2 Intent write

```text
putIfAbsent(commit-log/{commitId}, legacy StreamCommitRecord or generic StreamCommitTargetRecord)
```

If an existing record is found，all canonical request fields and the current head-derived
`previousCommitId/offsetEnd/cumulativeSize/commitVersion` fields must match before reuse。A commit-log record
alone is an invisible intent and can be orphaned when head CAS loses。

### 7.3 Stream-head CAS

The coordinator reads the latest head and validates：

- stream exists/ACTIVE；
- current session matches epoch/token；the adapter performs an expiry preflight，while the single-key CAS
  atomically fences through the current epoch/token snapshot rather than an unavailable server-time predicate；
- `expectedStartOffset == committedEndOffset`；
- provider-specific durable target identity is valid before CAS；Object WAL requires the exact manifest/slice，a
  future BookKeeper adapter validates its ledger/entry range without fake object records；
- offset/cumulative size arithmetic is safe；
- replay is not already reachable under another incompatible identity。

It constructs next head：

```text
committedEndOffset += recordCount
cumulativeSize += logicalBytes
commitVersion += 1
lastCommitId = commitId
```

Then：

```text
putIfVersion(headKey, nextHead, observedMetadataVersion)
```

This successful conditional put is the logical append linearization point。There is no conditional
multi-key offset-index batch in the active design。

### 7.4 Derived index materialization

After head success，materialize idempotently from the committed record：

1. generation-0 legacy `OffsetIndexRecord` or generic `OffsetIndexTargetRecord`；
2. matching legacy `CommittedSliceRecord` or generic `CommittedAppendRecord` replay marker；
3. object reference/audit state。

Every record validates the same `commitVersion` and durable identity。Object reference failure after head
commit is repairable audit/GC state，not logical rollback。

### 7.5 Result boundaries

| Requested level | Success requires | Read-after-success |
| --- | --- | --- |
| `WAL_DURABLE` | WAL durable + F4 root/commit-owned `REACHABLE_APPEND` + reachable head commit | primary target protected/recoverable from commit log；resolver may repair index |
| `WAL_DURABLE_AND_INDEX_COMMITTED` | above + offset index/marker + index-owned `VISIBLE_GENERATION` confirmed | normal protected generation-0 lookup immediately available |

Current Phase 1 implements only strict success。The name `WAL_DURABLE` never authorizes success before the
head CAS or with a temporary local offset。

Phase 1.5 P15-M2/M3 separate `commitStableAppend` from `materializeGenerationZero` internally while still executing only the
strict second row。The split is a prerequisite for later completion policies, not implementation of
`WAL_DURABLE` success。

Phase 4's target protocol further splits commit-intent preparation from head CAS. After upload it stores/reloads the
deterministic intent, registers the physical root, acquires `REACHABLE_APPEND` owned by that exact intent, and only
then may CAS the head. Generation-zero materialization returns the exact index key/version/value digest and acquires
`VISIBLE_GENERATION` before strict success. The head CAS remains logical linearization；the added records are physical
retention fences, not alternate commit truth. Full interfaces and crash cuts are in
`docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md` §10.

## 8. Retry and failure classification

| Last boundary | `AppendOutcome` | Required action |
| --- | --- | --- |
| before WAL durable | `KNOWN_NOT_COMMITTED` | normal retry |
| after WAL durable, before intent | `KNOWN_NOT_COMMITTED`；orphan bytes possible | reuse deterministic bytes or GC |
| after intent, before head CAS is sent | `KNOWN_NOT_COMMITTED` | retry same id or abandon intent |
| after head CAS is sent, response unavailable | `MAY_HAVE_COMMITTED` | re-read head/chain；never assume rollback |
| head reachable, indexes missing | `KNOWN_COMMITTED` | repair indexes then satisfy requested boundary |
| result constructed, response lost | `KNOWN_COMMITTED` | replay returns same range/version |

Timeout and caller cancellation follow the same table。Cancellation cannot undo an already-sent CAS。

Replay lookup order：

```text
version-matched committed-slice/committed-append marker
  -> latest head offset relation
  -> reachable commit chain only for an older expectedStartOffset
  -> orphan intent check
```

Marker absence is not proof of non-commit。
When `head.committedEndOffset == expectedStartOffset`，dense positive-record offsets prove a normal new append
cannot already be deeper in history，so it must not consume replay scan budget。Only an older expected start
requires the bounded walk。Within that walk，the first different reachable record whose range starts at or
before `expectedStartOffset` proves not-found；the adapter does not continue toward genesis after crossing
the only possible commit position。
Bounded chain-search exhaustion is also not proof of non-commit；it returns retriable metadata failure with
`MAY_HAVE_COMMITTED`。Reachable-chain reads validate dense `(offsetEnd, cumulativeSize, commitVersion)`
progression。Derived-index repair pages count every scanned commit and carry the original observed-head
anchor plus the exact expected tuple for the next commit。

### 8.1 In-process append recovery handle

Future 2 exposes a callback API that must choose one terminal `addComplete` or `addFailed`. Phase 1.5 therefore owns
the protocol-neutral prerequisite。`AppendOutcome` alone says
how certain the result is but does not identify which physical attempt to replay. Therefore every public append failure
after the head request is sent also carries an opaque `AppendAttemptId`。

The core retains the exact generic `CommitAppendRequest`、durable provider result and read target behind that ID。
`recoverAppend` replays only that identity and either returns the original `AppendResult`（including the commit's
protocol-neutral cumulative logical size implemented by P15-M6）or preserves/increases
certainty. It cannot prepare a new target or persist new primary bytes. A stream lane with a non-known attempt remains
suspended until recovery returns committed, proves
`KNOWN_NOT_COMMITTED` by complete commit-identity inspection, or reaches a permanent invariant failure; observing only
that the head advanced is insufficient because another writer may own the new range。Historical inspection is paged
from an immutable observed-head anchor and retains its continuation across recovery calls；retrying the newest bounded
page is not progress。The original mutation runner must quiesce before that anchor can prove non-commit。A committed or
proven-uncommitted terminal releases retained attempt capacity；a permanent invariant leaves the facade write-fenced。

This is an in-process callback-recovery contract, not producer-sequence deduplication across broker crashes。

### 8.2 Seal and logical delete

Phase 1.5 P15-M4 implements head-only lifecycle transitions：

```text
ACTIVE -> SEALED
ACTIVE/SEALED -> DELETING -> DELETED
```

Seal linearizes at the first edge and keeps committed reads/trim available。Logical delete linearizes on entry to
`DELETING`; append/session/read/resolve/trim are then rejected。`DELETED` is terminal completion。Neither transition
deletes offset indexes or object bytes；physical deletion still requires the GC protocol in section 15。A local
lifecycle barrier waits for older exact append recovery, while remote races are ordered only by the same head CAS。

## 9. AutoMQ-like async materialization

> Status: Designed / F4-M0 code-level contract frozen；not implemented

Async profile shares the entire stable append protocol。Only secondary publication is decoupled：

```text
WAL_DURABLE
  -> matching 64-shard stream registration + generation activation proved before primary IO
  -> COMMIT_INTENT
  -> HEAD_COMMITTED
  -> producer success at requested boundary
  -> registry scanner can reconstruct a missing materialization task from authoritative head/index
  -> upload object/read-optimized output
  -> publish higher generation
  -> release primary-WAL retention when safe
```

F4 只为 `OBJECT_WAL_ASYNC_OBJECT` 定义这条首个可执行 async 路径。Object WAL 作为
primary WAL 仍必须在 success 前完成自身 object upload、stable head commit 和 `WAL_DURABLE`
所需证据；延后的是 read-optimized higher generation。BookKeeper-WAL-then-async-object
需要独立的 BookKeeper adapter，不属于 F4。

Required durable states：

- a projection-resolvable stream registration for restart-safe work discovery（hint only）；
- source commit/range and primary location；
- deterministic task id and attempt state；
- output object/checksum；
- published generation；
- checkpoint and retention protection；
- lag/backpressure state。

If a future design wants success before Oxia head commit，it needs a different offset-reservation and
recovery protocol and a new API contract。It is explicitly outside the current profile semantics。

## 10. BookKeeper primary WAL

> Status: Designed/Reserved

BookKeeper provides primary bytes durability only。The same head/commit-log protocol assigns logical
offsets。The commit record/read target must eventually represent a real BK ledger/entry range：

| Profile | After stable append |
| --- | --- |
| `BOOKKEEPER_WAL_ONLY` | generation 0 continues to reference BK range；no Nereus object task |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | object-backed target is published before strict success |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BK range serves reads until background generation publish |

Phase 1.5 freezes a generic BookKeeper entry-range target value/codec and adapter registry, but the final-gated runtime registers
only Object WAL IO。A real BookKeeper adapter/client/retention implementation remains a separate profile gate。

## 11. Generation publish protocol

> Status: Designed / F4-M0 code-level contract frozen；not implemented

Generation 0 is append-derived。Generation > 0 is published by materialization/compaction：

```text
plan source ranges and identities
  -> write immutable output object
  -> verify format/checksums/coverage
  -> CAS task to PUBLISHING(stable publicationId, no generation)
  -> allocate unique (streamId, view) generation
  -> CAS that generation into the exact task
  -> create task-owned output-object visible-generation protection
  -> create the final generation-index key as PREPARED
  -> revalidate task/head-or-recovery-root/source-index/object-root facts
  -> CAS the same final index key PREPARED -> COMMITTED
  -> CAS-transfer protection ownership to the exact committed index
  -> record task/checkpoint progress
  -> protect source until GC-safe
```

Publish conditions include：

- source ranges still logically committed；
- expected source generations/checksums match；
- output exactly covers the declared offset range；
- projection/transaction information is preserved；
- the target carries a closed read-view identity; ordinary offset-index publication requires `COMMITTED` and the
  payload mapping's lossless observable-byte/entry-boundary proof；
- generation is allocated by one view-scoped per-stream CAS counter，is never reused and may contain gaps；
- output identity contains exact content SHA-256 plus the durable worker claim/output-attempt id, is
  generation-neutral and never reuses a deleted physical key。

The final generation-index key's successful `PREPARED -> COMMITTED` CAS is the sole physical-target switch point，
not a new logical append。`PREPARED`、task/checkpoint state and object upload are never visible truth。Reader picks
the highest valid visible generation within the requested view。`TOPIC_COMPACTED` uses a separate index namespace and
cannot supersede or serve as fallback for `COMMITTED`。Lower-generation fallback is permitted only while explicitly
retained。

## 12. Cursor commit protocol

> Status: F3-M0/M0R design-gated；F3-M1-M6 implemented/final-gated

One `CursorStateRecord` key owns exact projection/name、current writable-ledger `ownerSessionId`、cursor
generation/lifecycle、`ackStateEpoch`、first-unacked `markDeleteOffset`、normalized whole ranges、partial-batch
remaining bits、properties and snapshot reference。These fields are not split across keys。

Every F3 writable ledger open first creates or claims an ACTIVE/no-pending `CursorRetentionRecord` owner root，even
when no cursor exists and the topic projection is unactivated。That preactivation record is a session fence only；it
does not make cursor semantics visible。Before the first PROTECTION_PENDING transition or `CursorStateRecord`，a
separate monotonic CAS adds internal `nereus.cursor-protocol=1` to the F2 topic projection。This is a
minimum-reader/downgrade fence，not cursor ack truth；the locked F2 decoder rejects the reserved property before it can
expose an empty cursor collection。

Every writable ledger open generates a fresh owner session，claims `CursorRetentionRecord` first，then CAS-claims
every ACTIVE cursor root to that same session without changing cursor semantics。It recovers pending retention work and
stabilizes the full ACTIVE-root scan before publishing the ledger/open callback。The exact broker-supplied ownership
checker must pass before claim and again immediately before publication；a failed final check publishes no ledger and
the next owner safely reclaims the unpublished session。Pulsar topic ownership decides who may
start this protocol but does not replace it：an old broker CAS against an existing root that linearizes before the
corresponding root claim is included when the new owner reloads；one that arrives after that claim loses the
version/session fence。A CREATE/RECREATE whose pending retention intent was already durable may still race on its
ABSENT/DELETED target key because no cross-key atomic condition is assumed，but the stale owner cannot finalize the
claimed pending root or callback success；takeover recovery claims/rebuilds the winner before publication。Ordinary
operations require the handle/root/retention owner to match and never steal a session through retry。

```text
read root + Oxia version; require current owner session
  -> strictly hydrate current immutable snapshot + bounded root delta
  -> normalize ack/reset/property transition
  -> optionally upload + HEAD a replacement full snapshot
  -> CAS one root to new inline state/reference
  -> only then complete the ManagedCursor callback
```

The CAS is cursor-progress linearization。Snapshot upload before a failed CAS creates an orphan；snapshot existence
alone never advances acknowledgements。Ack retry may prove `ALREADY_APPLIED` or recompute monotonic union/AND against
the latest same-generation root only while `ackStateEpoch` is unchanged。Reset and clear-backlog increment that epoch；
a non-subsumed old ack cannot be recreated across destructive replacement。Reset/property replacement does not
blindly rebase。

Normal dispatch `readPosition` is broker-local and never committed。Restart/failover derives the next read from first
unacked；redelivery is permitted, skipping delivered-but-unacked data is not。Delete keeps a generation tombstone；
same-name recreate increments generation and fences stale handles。The owner-session claim independently fences
crash-delayed mutations and stale dispatch state from the previous writable broker instance。

Cursor and stream append are separate commit domains。All retention/cursor writes below carry the current claimed
owner session。New/recreated cursor and backward reset order two CAS domains
without a global transaction：each first CASes `CursorRetentionRecord` ACTIVE -> PROTECTION_PENDING with a complete
intent and `protectedFloorOffset=min(currentFloor,target)`，then writes the same attempt ID into the cursor root，proves
that root and finalizes retention back to ACTIVE before success。Floor raise and trim are blocked for the entire
pending interval；a one-shot version bump is explicitly insufficient because a raise could start after the bump but
before cursor CAS。Crash recovery completes/proves the durable intent and never guesses it away。Logical trim uses a
separate `TRIM_PENDING` candidate with offset、attempt ID and exact bounded composed reason，invokes idempotent L0
trim，verifies L0 truth，then records completion。Recovery reuses those exact persisted trim inputs；F4 consumes
completed trim and current references for physical GC。

## 13. Transaction commit protocol

> Status: Designed

Transaction state is explicit：

```text
OPEN -> COMMITTING -> COMMITTED
OPEN -> ABORTING  -> ABORTED
```

Cross-stream transaction cannot assume Oxia global transaction。The coordinator records affected ranges，
publishes per-stream markers/visibility state idempotently，then moves the coordinator state to a terminal
state。Readers derive `read_committed`/Pulsar dispatch visibility from terminal transaction state plus required
markers。

Pending-ack transaction applies ack ranges through cursor CAS before terminal commit；abort never advances
cursor truth。

## 14. SBT and SDT commit protocols

> Status: Designed

### 14.1 SBT

```text
logical range committed
  -> compacted generation published
  -> catalog snapshot committed with stream/generation lineage
  -> Oxia SBT state/reference recorded or repaired
```

Table visibility linearizes at catalog snapshot commit。Catalog failure does not roll back stream or
generation publish。

### 14.2 SDT

```text
deliveryId = hash(stream, range, target, schema version)
select committed range
  -> create/reuse files
  -> idempotent target catalog commit
  -> record delivery state/checkpoint
```

SDT terminal visibility belongs to target catalog。Timeout recovery queries the delivery id before retry。

## 15. GC protocol

> Status: Designed / F4-M0 code-level contract frozen beyond the implemented Phase 1 reference boundary

An object is deletable only when all relevant conditions are true：

- no visible generation references it；
- no reachable commit needs it as primary recovery source；
- completed L0 trim/retention protocols and all cursor/group/reference domains allow deletion；
- no active reader/repair/materialization/compaction task protects it；
- no catalog snapshot/delivery references it；
- orphan TTL/audit/ownership/checksum state allows deletion。

Superseded offset-index keys are themselves retained metadata until the same root scan permits tombstone/removal;
otherwise physical compaction would leave Oxia cardinality at `O(all historical appends)`. Resolve-to-read creates or
renews a durable per-object/per-process lease, then revalidates the exact view/generation/index identity and
`PhysicalObjectRootRecord.lifecycleEpoch` before IO. GC marks the physical root, denies new leases/protections, waits
for prior leases to release/expire and then revalidates every registered reference domain. A process-local cache flag
is insufficient.

If the reachable commit chain still names the primary target needed for exact append recovery, that target remains a
GC root even after a higher physical generation is visible. F4 publishes immutable `NRC1` recovery-checkpoint bytes
and a versioned `RecoveryCheckpointRootRecord`; only the root CAS makes anchor-aware replay/index repair independent
of the replaced prefix. Source/commit/index deletion remains disabled until that checkpoint and all reference roots
are revalidated.

Recommended sequence：

```text
ACTIVE physical root
  -> CAS MARKED with expected reference-domain registry/version set
  -> deny new leases/protections and drain bounded existing leases
  -> revalidate generation/recovery/task/cursor/catalog roots
  -> CAS DELETING
  -> delete physical object idempotently
  -> CAS DELETED and retire eligible metadata
```

The 256-shard Oxia root scan is authoritative for lifecycle recovery, including `DELETING` after bytes disappear。
Object list can discover missing-root/audit candidates but cannot prove absence of references。

`DELETED` is the terminal data-lifecycle result, but its audit key need not live forever. After a separately configured
long grace, two exact HEAD-absence windows and two unchanged complete owner/domain scans, F4 conditionally removes the
Phase 1 object-reference record, manifest and finally the exact-version root. Every actual provider PUT/retry first
revalidates its durable owner and requires an absent or same ACTIVE root；a stale attempt is rejected and a new attempt
uses a fresh key. Therefore audit retirement bounds metadata without changing the no-reuse rule.

## 16. Linearization summary

| Operation | Point |
| --- | --- |
| Append logical commit | stream-head CAS success |
| Strict append success | logical commit + protected generation-0 index/marker confirmation |
| Async append success | protected logical commit + requested primary durability boundary |
| Generation replacement | final generation-index key `PREPARED -> COMMITTED` CAS |
| Cursor update | cursor-state CAS |
| Transaction terminal result | coordinator terminal CAS after required per-stream work |
| SBT table visibility | catalog snapshot commit |
| SDT delivery visibility | target catalog idempotent commit |
| Routing change | routing-ring/version CAS；does not change data truth |
| GC | reference-validated delete protocol |

## 17. Repair priority

When states disagree，repair follows：

```text
stream head + reachable commit log
  > generation-aware offset index
  > object manifest/reference audit
  > task/checkpoint state
  > lakehouse catalog lineage
  > object list
```

For cursor/transaction domains，their own authoritative CAS state joins the first tier for that domain。

## 18. Implementation gates

M7 production Oxia adapter and M8 core/Object-WAL composition pass the Phase 1 ordinary and Docker-backed final gates。

Phase 1.5 P15-M1-M6 implemented and verified：

- generic target API/codec and legacy/new metadata compatibility；
- Object WAL adapter parity through split stable commit/materialization；
- exact retained-attempt recovery with anchored multi-page progress；
- authoritative seal/logical-delete state machines；
- exact cumulative logical size in normal and recovered public append results；
- unchanged Phase 1 ordinary/Docker gates and one-way rollout boundary。

F4-M0 has frozen the async task/checkpoint/idempotence、higher-generation publication/overlap/read-view、
64-shard restart-safe stream discovery、reader lease、recovery checkpoint、retention/GC and Object-WAL async error contracts in
`../phase-4-compaction-generation/`。Before enabling execution：

- implement F4-M1–M6 in the specified order and pass their mandatory review stops；
- enable only `OBJECT_WAL_ASYNC_OBJECT` in Phase 4；any BookKeeper profile still requires its real primary adapter；
- add the designed retention protections across primary WAL、readers、cursor/group and future catalogs；
- implement durable reader leases、recovery checkpoints and superseded-index retirement before physical GC；
- create/verify the matching stream registration before marker/async admission and test fresh-process task recovery；
- provide fault injection at every irreversible boundary；
- keep code/docs/golden bytes updated together。

## 19. References

- `nereus-overall-architecture.md`
- `nereus-storage-object-format.md`
- `nereus-future1-core-stream-storage.md`
- `../automq-like-stream-storage/README.md`
- `../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md`
- `../phase-1.5-core-storage-foundation/README.md`
- `../phase-1-core-stream-storage/09-legacy-oxia-multi-key-commit-design.md`（Historical）
