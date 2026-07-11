# Nereus Commit Protocol

> 状态：Current cross-track protocol
> Append 部分已与 Phase 1 stream-head CAS 实现合同对齐；generation、cursor、txn、catalog 部分为
> target design。

## 1. Purpose

本文统一以下提交边界：

- primary WAL durability；
- stable append/producer result；
- generation-0 read-index materialization；
- async object materialization and compaction generation publish；
- cursor、transaction、SBT/SDT and GC commits。

精确 Phase 1 records/keys/algorithms 见
`../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md` 和
`../phase-1-core-stream-storage/04-core-state-machines.md`。

## 2. Commit domains

Nereus 不把所有状态塞进一个“commit”概念：

| Domain | Authority | Linearization/publish point |
| --- | --- | --- |
| Logical append | Oxia stream head | successful head `putIfVersion` |
| Commit identity | reachable immutable commit log | head links the record |
| Generation-0 read/replay index | Oxia derived records | records materialized and validated |
| Higher-generation target | Oxia generation index | conditional generation entry publish |
| Cursor progress | Oxia cursor state | cursor CAS |
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

The coordinator builds a canonical identity over every durable replay field，including：

```text
stream / object / slice identity
writer epoch and fencing-token hash
expected offset and counts
logical bytes and event-time bounds
payload format / schema refs / projection identity
object and slice checksums
entry-index identity
```

`commitId` is a deterministic hash of that identity。The same physical slice replay must compute the same
id；different identity must not alias。

`previousCommitId`、next `commitVersion` and cumulative size are CAS-snapshot-derived record fields。They
must match when an existing intent is reused，but they are deliberately not hashed into the physical-attempt
`commitId`；otherwise compatible renew/trim retries could not compute one stable id before head CAS。

### 7.2 Intent write

```text
putIfAbsent(commit-log/{commitId}, StreamCommitRecord)
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
- object manifest/slice identity is valid；
- stored manifest `objectId` and aggregate/per-slice visibility states are consistent；
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

1. generation-0 `OffsetIndexRecord`；
2. `CommittedSliceRecord` replay marker；
3. object reference/audit state。

Every record validates the same `commitVersion` and durable identity。Object reference failure after head
commit is repairable audit/GC state，not logical rollback。

### 7.5 Result boundaries

| Requested level | Success requires | Read-after-success |
| --- | --- | --- |
| `WAL_DURABLE` | WAL durable + reachable head commit | primary target recoverable from commit log；resolver may repair index |
| `WAL_DURABLE_AND_INDEX_COMMITTED` | above + offset index and marker confirmed | normal generation-0 lookup immediately available |

Current Phase 1 implements only strict success。The name `WAL_DURABLE` never authorizes success before the
head CAS or with a temporary local offset。

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
committed-slice marker
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

## 9. AutoMQ-like async materialization

> Status: Designed/Reserved

Async profile shares the entire stable append protocol。Only secondary publication is decoupled：

```text
WAL_DURABLE
  -> COMMIT_INTENT
  -> HEAD_COMMITTED
  -> producer success at requested boundary
  -> durable materialization task/checkpoint
  -> upload object/read-optimized output
  -> publish higher generation
  -> release primary-WAL retention when safe
```

Required durable states：

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

Current object-shaped API cannot safely encode this range；generic target design is a pre-implementation
gate。

## 11. Generation publish protocol

> Status: Designed

Generation 0 is append-derived。Generation > 0 is published by materialization/compaction：

```text
plan source ranges and identities
  -> write immutable output object
  -> verify format/checksums/coverage
  -> conditionally publish higher-generation index entry
  -> record task/checkpoint
  -> protect source until GC-safe
```

Publish conditions include：

- source ranges still logically committed；
- expected source generations/checksums match；
- output exactly covers the declared offset range；
- projection/transaction information is preserved；
- target generation is monotonic and idempotent。

The generation entry publish is the physical-target switch point，not a new logical append。Reader picks
the highest valid visible generation。Lower-generation fallback is permitted only while explicitly retained。

## 12. Cursor commit protocol

> Status: Designed

Cursor small state uses a single CAS：

```text
read cursor state/version
  -> normalize mark-delete + ack ranges
  -> optionally upload immutable snapshot
  -> CAS cursor state to inline ranges/snapshot ref
```

The CAS is cursor progress linearization。Snapshot upload before a failed CAS creates an orphan；snapshot
existence alone never advances acknowledgements。

Cursor and stream append are separate commit domains。Trim/GC uses committed cursor low-watermarks but does
not require a global append+cursor transaction。

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

> Status: Designed beyond Phase 1 reference boundary

An object is deletable only when all relevant conditions are true：

- no visible generation references it；
- no reachable commit needs it as primary recovery source；
- trim/retention and all cursor/group low-watermarks allow deletion；
- no active reader/repair/materialization/compaction task protects it；
- no catalog snapshot/delivery references it；
- orphan TTL/audit/ownership/checksum state allows deletion。

Recommended sequence：

```text
mark GC candidate with expected reference/version set
  -> revalidate conditions
  -> delete physical object idempotently
  -> record tombstone/result
```

Object list can discover audit candidates but cannot prove absence of references。

## 16. Linearization summary

| Operation | Point |
| --- | --- |
| Append logical commit | stream-head CAS success |
| Strict append success | logical commit + generation-0 index/marker confirmation |
| Async append success | logical commit + requested primary durability boundary |
| Generation replacement | higher-generation index publish |
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

Before final Phase 1 exit，the M7 production Oxia adapter must pass the same manifest validation、single-key
CAS、`AppendOutcome`、bounded replay/repair continuation and watch/partition contract suite as the fake，plus
its independent Docker/Testcontainers integration gate。

Before extending beyond Phase 1 strict Object WAL：

- implement and test a generic primary read target；
- define `WAL_DURABLE` read-after-success SLA and repair error mapping；
- freeze async task/checkpoint/idempotence schema；
- freeze higher-generation publish CAS and overlap rules；
- add retention protection across primary WAL、readers、cursor/group and catalogs；
- provide fault injection at every irreversible boundary；
- keep code/docs/golden bytes updated together。

## 19. References

- `nereus-overall-architecture.md`
- `nereus-storage-object-format.md`
- `nereus-future1-core-stream-storage.md`
- `../automq-like-stream-storage/README.md`
- `../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md`
- `../phase-1-core-stream-storage/09-legacy-oxia-multi-key-commit-design.md`（Historical）
