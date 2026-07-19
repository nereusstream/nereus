# Retention, Materialization, and Producer Completion

## 1. One logical-retention truth, two physical collectors

F1-BK does not add a second cursor or retention policy engine：

```text
Pulsar effective retention/backlog policy
  -> existing F4 stable RetentionCandidatePlanner
  -> existing F3 CursorRetentionCoordinator.requestTrim
  -> durable stream trim offset                         only logical deletion truth

stream trim / healthy higher generation / task state
  -> owner-specific metadata retirement
  -> Object physical GC or BookKeeper whole-ledger GC   physical consequences
```

BookKeeper retention may consume F3/F4 evidence, but it cannot move a cursor、choose a trim offset or declare a higher
generation committed.

## 2. Range eligibility

For a committed BK generation-zero range `R=[start,end)`，the source bytes become logically replaceable by either：

### 2.1 Durable logical trim

```text
stream.trimOffset >= R.endOffset
AND exact F3 trim/cursor owner facts are stable
AND generation/commit/recovery owner-specific retirement permits removal
```

`trimOffset` in the middle of `R` does not reclaim the range. F2 presents one Nereus entry per offset, but the durable
target checksum/protection covers the complete append range.

### 2.2 Healthy higher-generation replacement

```text
one or more exact COMMITTED generations in COMMITTED view
tile all of R without gap
each generation > 0 and strictly newer than source generation 0
each target is a supported Object target
policy/source/projection identities are valid
object root/protection are ACTIVE and exact
generation can acquire a read pin and pass target/output verification
final source/index/root revalidation is unchanged
```

A PREPARED/QUARANTINED/DRAINING/RETIRED generation, workflow task, checkpoint offset or object upload by itself is not
a replacement. Current-policy coverage is required for lag/sync success; physical source retirement may consume any
healthy admitted same-view replacement accepted by the final-gated F4 source-retirement contract.

## 3. Protection retirement

The complete ledger range inventory is the set of exact protection records. Each protection type has an owner-specific
retirement rule：

| Protection | May retire only after |
| --- | --- |
| `REACHABLE_APPEND` | commit is safely represented by generation zero/recovery checkpoint, or exact logical trim/source-retirement plan removes the commit owner |
| `VISIBLE_GENERATION` | generation-zero index is conditionally retired after trim or healthy higher replacement |
| `MATERIALIZATION_SOURCE` | task is terminal/reconstructable, required output generation is exact COMMITTED, and source-retirement journal authorizes owner then protection removal |
| `APPEND_RECOVERY` | recovery checkpoint/anchor has advanced past the source under existing exact commit-chain rules, or trim authorizes retirement |
| `REPAIR` | bounded lease expired, owner is absent/terminal, and exact root/reference state is revalidated |

The implemented BK-M2 order is always：

```text
freeze authoritative owner + protection versions/digests
persist/reload retirement evidence where F4 requires it
revalidate the exact monotonic owner-retirement authority
CAS ACTIVE -> RETIRED (or verified abandoned RESERVED -> RETIRED)
retain the fixed tombstone with exact historical owner facts
revalidate the retirement authority again
```

Deleting a protection row before whole-ledger GC is forbidden。`RETIRED` is non-protecting but remains part of the
complete bounded ledger inventory；this avoids an unbounded stream-reservation scan when later compaction/GC is added。
The verifier is owner-specific：logical trim、healthy higher generation and exact abandoned append are distinct
authorities and elapsed time alone cannot produce a tombstone。

BK-M2 implements the BK_ONLY production bridge as `BookKeeperWalOnlyRetirementAuthority` plus
`BookKeeperWalOnlyReferenceRetirementCoordinator`。The authority reloads the exact L0 stream-head snapshot，requires a
BookKeeper profile and `trimOffset >= range.endOffset`，and freezes canonical `NBKTRIM1` authority before the
pre/post-CAS revalidation。For RESERVED failed ranges it instead reloads the exact ABANDONED reservation key/version/
stored SHA。The coordinator only consumes these facts；it never advances trim or deletes logical metadata。

BK-M3 extends that bridge without changing the tombstone protocol。`CommittedObjectGenerationAuthority` scans a
bounded COMMITTED-view candidate set and accepts only a canonical higher index whose Object target、ACTIVE physical
root、exact index-owned `VISIBLE_GENERATION` protection and covering materialization checkpoint all agree。
`BookKeeperAsyncObjectRetirementAuthority` binds that index key/version/stored SHA into
`HEALTHY_HIGHER_GENERATION` evidence and reconstructs the complete F4 proof before and after each protection CAS。
`REACHABLE_APPEND`/`APPEND_RECOVERY` rows whose own `commitVersion=0` obtain it only from the exact same-range slot-1
generation-zero anchor；the anchor may be ACTIVE or the just-produced RETIRED tombstone。Dynamic
`MATERIALIZATION_SOURCE` rows are deliberately excluded from tombstoning and are removed only by terminal task
retirement, preventing permanent dynamic-slot leakage。

The live-read checkpoint closes the remaining deterministic half of this authority。Metadata proof construction now
calls `NormalPathCommittedObjectGenerationReadVerifier`，which repeatedly fresh-resolves the source interval through
`GenerationReadResolver`，requires the exact index key/version/stored SHA、generation、commit version and target，acquires
the normal durable Object pin，and reads every source record in bounded pages through the registered production target
reader。Every page must preserve dense logical offsets、the exact `ReadSourceRef` and provider-neutral byte accounting；
the compacted reader still owns physical format/slice checksum verification。Pin release is joined on success and
failure。Metadata revalidation repeats this live proof，so a checkpoint or active root alone cannot authorize BK
release。

The shared resolver now admits every object-materializing profile。Before publication，a BK generation-zero candidate
is returned without an Object lease only because `BookKeeperPrimaryWalReader` owns the durable ledger reader lease and
final lease revalidation；positive generations remain Object-only and resolver-pin-owned。This preserves normal BK
fallback during lag without weakening higher-generation GC safety。`Phase4ObjectWalRuntime` constructs and exports the
live `CommittedGenerationRetirementAuthority` for BK composition。The first real-service fixture now runs this chain
over real Oxia + BookKeeper + LocalStack：stable-head append with no Object generation，BK read across a fresh runtime，
ordinary scanner/worker NCP1 publication，higher-generation normal read and exact retirement proof all pass。It also
locks two shared prerequisites uncovered by the composition：default process IDs are canonical base32 publication
identities，and Oxia checkpoint scans use the checkpoint key's exact two descendant segments。The physical-retirement
extension advances the shared metadata-audit grace and proves that terminal task cleanup releases the exact dynamic
`MATERIALIZATION_SOURCE` slot；`BookKeeperAsyncObjectRetirementAuthority` then uses the same healthy Object proof to
retire `REACHABLE_APPEND`、`VISIBLE_GENERATION` and `APPEND_RECOVERY`。The existing whole-ledger gate and retention
manager mark、drain、physically delete、record two separated absence observations and reach `DELETED`，after which the
normal resolver still reads the complete range from Object。The fresh-runtime response-loss checkpoint now covers an
applied task-create response loss before source protection、an applied BK-source create response loss、an applied
compacted-Object PUT response loss followed by durable `RETRY_WAIT` and source release，and the entire recovered
claim/output/source-transfer/publish/generation-commit/task-published/source-release CAS chain。Every process reloads
the same deterministic task/source/output identity，and BK remains the readable generation-zero fallback until the
exact higher generation is COMMITTED。The real-load checkpoint installs `BookKeeperAsyncAppendAdmissionGuard` only as
a profile filter over the shared `MaterializationLagGate`：an exact two-record lag rejects the next append with
`BACKPRESSURE_REJECTED/KNOWN_NOT_COMMITTED` before writer state or BK bytes change，and committed Object coverage makes
the following append admissible。Its negative chain deletes the exact COMMITTED Object bytes，after which the retirement
read proof fails with `OBJECT_NOT_FOUND`、all fixed BK references stay ACTIVE、the ledger remains present and the normal
generation reader quarantines/falls back to BK。No BookKeeper-specific lag or fallback truth was added。The focused M3
surface is closed；the final milestone still waits for the M2 predecessor and later abrupt-process/chaos aggregate before
broker registration。

## 4. `BookKeeperWalRetentionGate`

The gate evaluates one sealed root and produces a process-local candidate, never a durable second truth：

```java
record BookKeeperLedgerRetirementCandidate(
    VersionedBookKeeperLedgerRoot root,
    VersionedBookKeeperWriterState writerState,
    List<VersionedBookKeeperProtection> retiredProtections,
    Checksum referenceSetSha256,
    BookKeeperProtocolActivationProof activationProof,
    long capturedAtMillis) { }
```

Candidate admission requires all of the following：

1. root is exact `SEALED` with stable Oxia version/lifecycle epoch and matching closed BookKeeper metadata；
2. complete protection and reader pages were scanned from empty continuations；
3. all existing protection rows are `RETIRED`；mandatory slots `0..2` exist for every contiguous range slot and their
   range identities/checksums agree；a final abandoned/partial range may extend beyond the sealed LAC but never leave
   an uncovered physical entry；
4. no unexpired reader lease remains；expired leases are handled only after drain/revalidation；
5. writer state does not select the ledger and no allocation slot may still create/write/recover it；
6. every commit/generation/task/recovery/repair/abandoned-reservation domain has already supplied exact retirement
   authority recorded in its permanent tombstone；
7. root's stream projection/profile remains a supported BookKeeper profile；
8. exact cluster BK deletion activation/config/ledger-id-namespace binding/scope digest is current；
9. candidate/reference count is within configured bounds; overflow is a veto；
10. a second complete capture has the same exact SEALED root and the same canonical `NBKREF1` digest before
    `SEALED -> MARKED`。

Any retained `CREATE_UNCERTAIN` allocation slot or root `lateCreateHazard=true` is a non-expiring veto. The hazard
survives later matching discovery/seal；repeated physical absence cannot retire it, authorize candidate reuse or
permit whole-ledger delete. Because BookKeeper delete is not metadata-version conditional, loss of the exclusive
advanced-ledger-id namespace proof also invalidates every in-flight MARK/DELETING mutation before the provider call.

`NBKREF1` includes every tombstone key/version/stored-value SHA and the exact activation proof。For the root and
stream-wide writer it hashes only immutable sealed-ledger identity plus the semantic “writer does not select this
ledger” binding；it deliberately excludes mutable root GC version/bytes and unrelated later-ledger writer counters，
so `SEALED -> MARKED -> DELETING` and later appends on a different ledger cannot manufacture false drift。Every pass
still reloads the exact observed root version/digest and rechecks the writer predicate；the digest never hashes only
counts。

## 5. Whole-ledger behavior by profile

### 5.1 `BOOKKEEPER_WAL_ONLY`

There is no higher Object representation. Generation-zero/commit protections are retired only when durable logical
trim and recovery-anchor rules release them. A ledger containing one trimmed and one live range stays intact；the
trimmed range's protections may retire, but physical delete waits until all remaining ranges/references are gone.

BK-M2 must provider-neutralize/use the existing F4 generation-zero/commit retirement machinery even though no
materialization runtime is enabled. Deferring that work to BK async would make BK_ONLY leak ledgers indefinitely.

### 5.2 `BOOKKEEPER_WAL_ASYNC_OBJECT`

A range may release through trim or a healthy exact higher generation. The gate never treats “task SUCCEEDED” as
sufficient; it reloads the COMMITTED index/Object root and source-retirement outcome. BK source bytes remain readable
while publication is absent/failed/uncertain.

### 5.3 `BOOKKEEPER_WAL_SYNC_OBJECT`

The same physical release rules apply. Producer sync completion does not itself delete the BK source；normal source
retirement/ledger collector runs independently and respects reader/cursor/recovery references.

## 6. Reusing F4 materialization

### 6.1 Existing identity is sufficient

`SourceGeneration` already freezes：

```text
view + logical range + generation + commitVersion
index key/version/stored SHA
encoded ReadTarget + target identity SHA
policy/projection/schema/logical accounting
```

`MaterializationTask.taskId` already hashes canonical sources、coverage and policy digest. A BK generation-zero source
therefore needs no new task record, queue or identifier. Open ledger handles and segment sequence are not task identity.

### 6.2 Required provider-neutral changes

BK-M3 changes these seams：

- `DefaultExactSourceRangeReader` dispatches stored `ReadTarget` through `ReadTargetReaderRegistry` instead of requiring
  `ObjectSliceReadTarget`；
- `MaterializationTaskProtections.sources` becomes a provider-neutral source-protection handle list/union, while
  output remains an Object protection；
- `DefaultMaterializationTaskProtectionReconciler` creates `MATERIALIZATION_SOURCE` via the target's physical-reference
  adapter；
- worker/committer/terminal-retirer/source-retirement code revalidates generic source target identity and invokes the
  corresponding reference adapter；
- lag authority admits all `StorageProfile.objectMaterializationEnabled()` profiles while still requiring higher
  generations to be valid compacted Object targets；
- generation fallback/error handling keeps BK generation zero readable until its exact retirement completes。

No F4 durable codec changes are needed merely because the source target is BK; current task V2 already stores tagged
`ReadTargetRecord` descriptors.

Current code implements this provider-neutral seam through `MaterializationSourceProtection`、
`MaterializationSourceProtectionAdapter`、`MaterializationSourceProtectionRegistry` and the matched
`MaterializationSourceProvider` runtime pair。Object sources keep their existing adapter；BK sources use durable bounded
`MATERIALIZATION_SOURCE` slots and the exact BK reader。The worker、generation committer and task recovery reconciler
receive the same registry instance，so recovery cannot silently fall back to an Object-only protection path。

### 6.3 Task/protection ordering

```text
load exact generation-zero source
create/reload deterministic task
create/reconcile BK MATERIALIZATION_SOURCE protection for every BK source
create/reconcile Object source protections for Object sources
reload exact task + every source index/protection
only then claim/run task
read sources under durable reader pins
write/verify/protect Object output
publish COMMITTED generation
checkpoint
source retirement removes owners/protections when eligible
```

Generation-zero `VISIBLE_GENERATION` protection covers the brief task-created/source-protection-not-yet-created
window. Each task/reference claims its own fixed dynamic protection slot before source IO；a full per-range slot set
rejects task/repair admission and remains a GC veto. A task with an unprotected source is never runnable.

### 6.4 Exact BK source read

The worker requests the complete `SourceGeneration.range` and receives exact raw Pulsar Entry bytes through the BK
reader. It validates source index/target/checksum before and after read, then feeds the same NCP1 lossless row publisher
used by Object generation zero. NRC1 recovery/checkpoint objects and Object publication remain unchanged.

Compression/Parquet output cannot change Pulsar Entry bytes：the resulting higher-generation reader must reproduce
the byte sequence and F2 MessageIds exactly.

## 7. Async profile state machine

### 7.1 Producer path

```text
pre-IO profile/capability/lag admission
BK durable range
generic commit + REACHABLE_APPEND protection
head CAS
stable AppendResult
schedule/reconstruct gen0 publication + materialization discovery
ack producer at STABLE_HEAD
```

Scheduling failure after head does not change producer success. Durable stream registration、commit tail and gen0
repair allow another process to discover the range.

### 7.2 Background path

```text
repair/reload generation-zero BK index
planner selects canonical contiguous sources
task create/reload + source protections
exact BK range read(s)
NCP1 Object output write/verify/root/protection
generation allocate + PREPARED -> COMMITTED publication
checkpoint reconcile
source retirement
BookKeeper whole-ledger collector
```

Planner tasks may span multiple ledgers and may combine BK/Object sources. Source protection is per exact source
target; output remains one normal F4 Object generation.

### 7.3 Terminal/rollover flush

The code-level audit corrected an earlier assumption here。`DefaultMaterializationPlanner.eligible` applies
`minMergeSourceRanges` only when every source is already a current-policy higher Object generation；a generation-zero
source, including BK, is ordinary materialization input and is already eligible as a one-source deterministic task。
Creating a BookKeeper-specific range scanner/grouper would therefore introduce a second planner and duplicate-task
race。

`BookKeeperSealedLedgerMaterializationTrigger` instead performs only this bounded hint protocol：

```text
require exact SEALED root
reload the same root key/version/digest
invoke MaterializationStreamTrigger
  -> existing DefaultMaterializationService.scanNow
  -> existing 64-shard registration scan/planner/task store/worker
reload the same SEALED root again
```

It never scans generation indexes、groups sources or creates/publishes tasks itself。
`MaterializationPlannerTest.plansFinalSingleBookKeeperGenerationZeroThroughTheOrdinaryPlanner` freezes the one-source
behavior with `minMergeSourceRanges=2`；the trigger test freezes exact pre/post root revalidation。Sync completion may
request the same shared scanner, but does not get a second task-creation path。

### 7.4 Lag/backpressure

Reuse `MaterializationLagSnapshot` exactly：verified covered offset、committed end、lag records、lag bytes、oldest age、
head version and observation time. Task count、BK ledger count、checkpoint position or local worker queue length are not
lag truth.

`MaterializationLagGate` keeps throttle/reject behavior：

```text
measure exact stable authority
if reject threshold crossed -> retriable reject before BK IO
if throttle threshold crossed -> one bounded delay -> remeasure
otherwise admit
```

Workers/reads/repair are never throttled by producer admission. BK metrics may expose ledger pressure but cannot become
a second admission truth.

## 8. Sync completion contract

### 8.1 Public and internal expression

Keep `DurabilityLevel` meanings unchanged and add：

```java
public enum AppendCompletionPolicy {
    PROFILE_DEFAULT,
    STABLE_HEAD,
    GENERATION_ZERO_INDEX,
    REQUIRED_OBJECT_GENERATION
}

enum AppendAckBoundary {
    STABLE_HEAD,
    GENERATION_ZERO_VISIBLE,
    REQUIRED_OBJECT_GENERATION
}
```

`AppendOptions` gains `completionPolicy` with source-compatible constructors defaulting to `PROFILE_DEFAULT`。
`StorageProfileResolver` maps public policy/profile/durability to one exact `StorageExecutionPlan` boundary. A sync BK
profile rejects a weaker explicit policy; async/BK_ONLY defaults stay stable-head. `GENERATION_ZERO_INDEX` requires
the existing index-confirmed durability. `REQUIRED_OBJECT_GENERATION` implies stable head + gen0 but does not rename
either durability level.

### 8.2 Required source identity

After head/gen0, construct：

```java
record RequiredObjectGenerationRequest(
    StreamId streamId,
    OffsetRange range,
    long sourceCommitVersion,
    Checksum sourceIndexSha256,
    Checksum sourceTargetIdentitySha256,
    Checksum materializationPolicySha256,
    Duration remainingTimeout) { }
```

Identity is source/policy based. It does **not** reserve a generation number; the existing generation allocator owns
that sequence.

### 8.3 Completion sequence

```text
reload exact SourceGeneration(gen0 BK)
search for already-COMMITTED current-policy Object coverage
if absent:
  MaterializationTask.create(exact one source, existing lossless policy)
  create/reconcile task and BK source protection
  dispatch through shared bounded F4 worker
wait/recover task under append's monotonic remaining deadline
reload exact COMMITTED generation produced by task or equivalent exact coverage
require Object target + ACTIVE root + visible protection
resolve/pin that exact generation
read and verify the append's exact logical range through normal reader path
final source/index/task/generation/root/profile revalidation
return RequiredObjectGenerationProof
ack producer
```

The read step proves the published generation is usable, not merely syntactically COMMITTED. It compares returned
raw Entry bytes/checksum/logical range to source facts; no MessageId is regenerated from Object identity.

### 8.4 Completion proof

```java
record RequiredObjectGenerationProof(
    RequiredObjectGenerationRequest request,
    String taskId,
    long generation,
    String generationIndexKey,
    long generationIndexMetadataVersion,
    Checksum generationIndexSha256,
    Checksum objectTargetIdentitySha256,
    long objectRootMetadataVersion,
    Checksum objectRootSha256,
    long verifiedAtMillis) { }
```

The proof is a checked process value returned to the coordinator, not a second durable commit. Recovery reconstructs
it from source/task/generation/root facts.

### 8.5 Failure after head

If Object upload/publication/read verification times out or fails after head：

- append remains logically visible from BK；
- producer receives an error with `AppendOutcome.KNOWN_COMMITTED` and the original `AppendAttemptId`；
- reservation/target/commit/result remain unchanged；
- task remains durable/retriable or terminal with classified failure；
- `recoverAppend` reloads the same commit, reconstructs/reuses the same task, and returns the original stable result only
  after the required Object proof；
- a new BK range, offset or MessageId is never allocated by completion recovery。

Current `AppendAttemptId` is process-scoped and is not stored in `StreamCommitTargetRecord`. This design does not claim
cross-process producer dedup/exactly-once for a completely new attempt; that remains an advanced Pulsar/F8 concern.

## 9. Read selection and source release

Before higher publication, ordinary reads select generation zero and read BK. After COMMITTED publication, resolver
selects the highest admitted healthy generation and reads Object. A corrupt/unavailable higher generation can fall
back to still-protected BK generation zero.

Once source retirement has removed gen0/commit protections and the whole ledger is deleted, the Object generation is
the remaining durable representation. F4 Object root/protection/GC rules must therefore remain active; deleting BK is
never justified merely because an upload once succeeded.

## 10. Retention crash matrix

| Cut | Required recovery |
| --- | --- |
| task exists, BK source protection missing | gen0 protection retains source; reconciler creates exact task protection before claim |
| BK read complete, Object upload absent | task retry reads same target; BK retained |
| Object bytes uploaded, root/publication absent | F4 guarded output recovery; BK retained |
| generation COMMITTED, task/checkpoint response lost | exact reload converges; task/source protection remains until retirement |
| source/index owner retirement response lost | retirement verifier reloads exact authority; protection stays ACTIVE or converges to the same RETIRED tombstone |
| all protections RETIRED, reader lease exists | ledger remains SEALED/MARKED until lease drain and final revalidation |
| BookKeeper DELETE response lost | reload exact metadata; never blind-delete foreign identity; dual absence proof |
| matching ledger reappears during absence grace | validate allocation metadata, repeat delete under same intent, restart grace |
| allocation create once entered unknown outcome | persist permanent slot + `lateCreateHazard`；matching ledger is recovery-opened/sealed because its writable handle cannot be reconstructed, and automatic delete remains forbidden |
| namespace readiness/reservation changes before delete | invalidate activation and stop before provider delete；never trust prior metadata check |
| physical ledger appears for ABORTED/DELETED tombstone | conditional escalation to QUARANTINED；no automatic delete/reuse |
| reference appears after MARKED | unmark to SEALED; no physical delete |

## 11. Metrics

Expose profile-labelled metrics without using them as authority：

- BK append entries/bytes/latency and exception class；
- active/allocating/sealing/sealed/marked/deleting/quarantined roots；
- rollover reason and uncertain-write count；
- BK read bytes/ranges/checksum failures/cache hits；
- source protection/reader lease counts；
- materialization lag records/bytes/age (shared metric names)；
- required-object completion latency/task reuse/failure；
- reclaimable ranges、blocked ledgers by veto type、delete/absence-recovery outcomes。

No ledger password、entry bytes、fencing token or raw secret reference is logged/tagged.
