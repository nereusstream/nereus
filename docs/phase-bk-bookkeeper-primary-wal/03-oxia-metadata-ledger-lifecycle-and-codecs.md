# Oxia Metadata, Ledger Lifecycle, and Codecs

## 1. Metadata design rules

BookKeeper and Oxia cannot participate in one atomic transaction. F1-BK therefore uses ordered durable intents、exact
identity reload and monotonic CAS；it never describes a multi-key or cross-system operation as atomic.

The authority split is：

```text
stream writer state / allocation / reservation keys    stream partition
ledger root / protections / reader leases              physical-ledger shard partition
generic commit / stream head / generation index        existing stream partition
BookKeeper ledger metadata                              physical provider
```

Any transition touching more than one row has a documented crash cut and is restart-reconstructable from the rows
already written. Process-local queues, watches and handles are hints only.

## 2. Keyspace

Add `BookKeeperKeyspace` in `nereus-metadata-oxia`. Every component uses the existing canonical
`KeyComponentCodec`; handwritten escaping and ambiguous `/` concatenation are forbidden.

### 2.1 Stream-scoped keys

For `S = encodeComponent(streamId)`：

```text
${clusterPrefix}/streams/${S}/bookkeeper/v1/writer-state
${clusterPrefix}/streams/${S}/bookkeeper/v1/allocations/${allocationId}
${clusterPrefix}/streams/${S}/bookkeeper/v1/append-reservations/${reservationId}
```

They use `OxiaKeyspace.streamPartitionKey(streamId)`。Allocation/reservation scans are bounded under exactly one
stream and are used for recovery/terminal retirement, not ordinary append discovery.

### 2.2 Physical-ledger keys

Define `BOOKKEEPER_LEDGER_SHARDS = 256` and：

```text
ledgerIdentity = SHA-256(utf8(clusterAlias) || 0x00 || u64(ledgerId))
shard          = unsigned(first byte of ledgerIdentity)
ledgerHash     = lowercase hex ledgerIdentity

${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/roots/${ledgerHash}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/protections/${type:02d}/${referenceId}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/readers/${processRunId}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/retirement/${gcAttemptId}/manifest
```

All four key families use one `PartitionKey("physical-bookkeeper-v1-%03d")` per shard. This permits root/protection/
lease conditional operations in one partition where Oxia supports them, but the protocol still specifies individual
CAS/reloads and never assumes a transaction that the client surface does not expose.

Root scan boundaries are `000..255`, each from an empty opaque continuation. Page tokens bind cluster、shard、prefix
and page size; callers cannot convert tokens to key order or reuse them in another shard.

### 2.3 Strict inverse and identity rules

`BookKeeperKeyspace` must provide strict parsers for root、protection and reader keys used by recovery/retirement.
Parsing succeeds only when re-encoding yields byte-for-byte the supplied key. `ledgerHash` is always recomputed from
record `(clusterAlias, ledgerId)` and must match the key/shard. A hash collision between different exact identities is
a fail-closed metadata invariant；it is never resolved by choosing one record.

## 3. Durable record set

Every record is framed by the existing metadata envelope with explicit record type/schema version、bounded payload、
stored-value SHA-256 and Oxia metadata version. The pseudo-Java below freezes field meaning and ordering; code may use
records plus validated wrappers following current repository conventions.

### 3.1 `BookKeeperWriterStateRecord`

One mutable row serializes provider-side ownership for a stream：

```java
record BookKeeperWriterStateRecord(
    int schemaVersion,
    String streamId,
    String clusterAlias,
    String configurationBindingSha256,
    BookKeeperWriterLifecycle lifecycle,      // IDLE, ALLOCATING, ACTIVE, RECOVERING, CLOSED
    long writerStateEpoch,
    String writerId,
    String writerRunIdHash,
    long appendSessionEpoch,
    String fencingTokenHash,
    long appendSessionLeaseVersion,
    long nextSegmentSequence,
    String allocationId,                      // only ALLOCATING
    long allocationLedgerId,
    long activeSegmentSequence,               // only ACTIVE/RECOVERING
    long activeLedgerId,
    long activeLedgerRootEpoch,
    long nextEntryId,
    String activeReservationId,               // at most one, empty otherwise
    long openedAtMillis,
    long updatedAtMillis,
    String stateReason,
    long metadataVersion) { }
```

Invariants：

- `writerStateEpoch` increments exactly once on every lifecycle/owner replacement CAS；
- `nextSegmentSequence` is monotonic and a segment sequence is never reused；
- only one allocation or active ledger identity is populated；
- ACTIVE permits exactly one `activeReservationId` because the stream append lane is serialized；
- `nextEntryId` is advanced by checked addition **before** any write for that reservation；it never moves backward；
- a new append-session epoch cannot adopt the old ledger as ACTIVE: it first enters RECOVERING, fences/seals, then
  allocates a new ledger；
- metadata version is hydrated from Oxia and encoded as zero in desired-value comparisons, following current wrappers。

`IDLE` is a real durable state, not absence. Absence is allowed only before the first BK profile append and is created
with the stored profile/config binding after first-create admission.

### 3.2 `LedgerAllocationIntentRecord`

```java
record LedgerAllocationIntentRecord(
    int schemaVersion,
    String allocationId,                     // >=128-bit random base32
    String streamId,
    long segmentSequence,
    String clusterAlias,
    long candidateLedgerId,                  // positive 63-bit exact id
    String configurationBindingSha256,
    String writerId,
    String writerRunIdHash,
    long appendSessionEpoch,
    String fencingTokenHash,
    long writerStateEpoch,
    LedgerAllocationLifecycle lifecycle,     // PREPARED, ROOT_RESERVED, PHYSICAL_CREATED,
                                            // ACTIVATED, FOREIGN_COLLISION, ABORTED
    String bookKeeperMetadataSha256,
    long createdAtMillis,
    long updatedAtMillis,
    String stateReason,
    long metadataVersion) { }
```

The intent is workflow/recovery evidence, not physical ownership authority. The root owns the candidate globally and
the writer state selects the active segment. Every lifecycle CAS reloads and verifies both identities.

### 3.3 `BookKeeperLedgerRootRecord`

```java
record BookKeeperLedgerRootRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    String clusterAlias,
    long ledgerId,
    String streamId,
    long segmentSequence,
    String allocationId,
    String configurationBindingSha256,
    String writerId,
    String writerRunIdHash,
    long appendSessionEpoch,
    String fencingTokenHash,
    int ensembleSize,
    int writeQuorumSize,
    int ackQuorumSize,
    String digestType,
    String customMetadataSha256,
    BookKeeperLedgerLifecycle lifecycle,
    long lifecycleEpoch,
    long createdAtMillis,
    long activatedAtMillis,
    long sealStartedAtMillis,
    long sealedAtMillis,
    long sealedLastEntryId,                   // -1 allowed only for sealed empty ledger
    long sealedLength,
    String sealReason,
    String gcAttemptId,
    String referenceSetSha256,
    long markedAtMillis,
    long deleteNotBeforeMillis,
    long deleteStartedAtMillis,
    long firstAbsentAtMillis,
    long deletedAtMillis,
    String stateReason,
    long metadataVersion) { }
```

Lifecycle wire ids are frozen and never derived from enum ordinals：

| Wire id | Lifecycle | Physical meaning |
| --- | --- | --- |
| 1 | `ALLOCATING` | global exact-id reservation exists; physical create may be absent/unknown |
| 2 | `ACTIVE` | exact matching ledger exists and belongs to one current segment owner |
| 3 | `SEALING` | no new reservation/write allowed; recovery close/fence is in progress |
| 4 | `SEALED` | BookKeeper metadata proves closed and freezes last entry/length |
| 5 | `MARKED` | whole-ledger reference set frozen; drain grace in progress |
| 6 | `DELETING` | exact delete intent durable; provider deletion/dual-absence proof in progress |
| 7 | `DELETED` | two separated exact `NoSuchLedger` observations complete |
| 8 | `ABORTED` | allocation never became an owned active physical ledger |
| 9 | `QUARANTINED` | identity/provider/metadata conflict; automatic mutation prohibited |

Only `SEALED -> MARKED -> DELETING -> DELETED` can physically delete a previously active ledger. `ALLOCATING` cleanup
uses its own late-create reconciliation and reaches ABORTED only after two separated absence observations; if matching
physical bytes later appear, recovery resumes exact cleanup rather than ignoring them.

`lifecycleEpoch` starts at one and increments on every root CAS. Immutable identity/quorum/custom-metadata fields never
change. `DELETED` roots are retained as compact durable tombstones in BK-M0–M6; the allocator treats every existing
root, including ABORTED/DELETED, as a consumed candidate id. A later root-compaction feature would have to create a
permanent exact ledger-id tombstone before removing the full root and is outside this delivery.

### 3.4 `BookKeeperAppendReservationRecord`

One immutable/logically monotonic row makes the provider crash cut reconstructable before a generic commit exists：

```java
record BookKeeperAppendReservationRecord(
    int schemaVersion,
    String reservationId,
    String appendAttemptId,
    String streamId,
    String writerId,
    String writerRunIdHash,
    long appendSessionEpoch,
    String fencingTokenHash,
    long writerStateEpoch,
    long ledgerId,
    long ledgerRootEpoch,
    long firstEntryId,
    int entryCount,
    String rangeChecksumSha256,
    long expectedStartOffset,
    String payloadFormat,
    int recordCount,
    long logicalBytes,
    List<SchemaRef> schemaRefs,
    String projectionIdentity,
    long minEventTimeMillis,
    long maxEventTimeMillis,
    AppendReservationLifecycle lifecycle,    // RESERVED, WRITING, DURABLE, COMMIT_PREPARED,
                                            // HEAD_COMMITTED, ABANDONED
    String commitId,
    String commitKey,
    long commitMetadataVersion,
    String commitRecordSha256,
    long createdAtMillis,
    long updatedAtMillis,
    String stateReason,
    long metadataVersion) { }
```

`BookKeeperEntryRangeReadTarget` is deterministically reconstructed from ledger/range/checksum/alias. The reservation
stores the bounded logical fields needed by `CommitAppendRequest`; it never stores entry payload bytes or secrets.

Lifecycle is recovery workflow, not visibility. Only a reachable stream head proves `HEAD_COMMITTED`; a row claiming
that lifecycle without the exact generic commit/marker/head is an invariant violation.

### 3.5 `BookKeeperLedgerProtectionRecord`

Protection rows are also the complete ledger-to-range inventory：

```java
record BookKeeperLedgerProtectionRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    String clusterAlias,
    long ledgerId,
    long rootLifecycleEpoch,
    int protectionTypeId,
    String referenceId,
    long firstEntryId,
    int entryCount,
    String rangeChecksumSha256,
    String streamId,
    long offsetStart,
    long offsetEnd,
    long commitVersion,
    String ownerKey,
    long ownerMetadataVersion,
    String ownerIdentitySha256,
    long createdAtMillis,
    long expiresAtMillis,
    long metadataVersion) { }
```

Frozen V1 types：

| Wire id | Type | Durable owner |
| --- | --- | --- |
| 1 | `REACHABLE_APPEND` | generic commit intent, acquired before head CAS |
| 2 | `VISIBLE_GENERATION` | exact generation-zero index |
| 3 | `MATERIALIZATION_SOURCE` | nonterminal F4 task/checkpoint source |
| 4 | `APPEND_RECOVERY` | append recovery root/checkpoint |
| 5 | `REPAIR` | bounded repair intent; only type with expiry |

Permanent types have `expiresAtMillis == 0`。Every owner removal is metadata-first and conditional：prove a replacement
or trim, persist retirement journal/evidence where required, remove owner, then protection, then permit physical
deletion. A protection whose owner cannot be read/revalidated is a veto, not stale garbage.

### 3.6 `BookKeeperLedgerReaderLeaseRecord`

```java
record BookKeeperLedgerReaderLeaseRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    long ledgerId,
    long rootLifecycleEpoch,
    String processRunId,
    long leaseEpoch,
    long acquiredAtMillis,
    long expiresAtMillis,
    long metadataVersion) { }
```

One key per process/ledger is locally reference-counted. Renewal increments `leaseEpoch` by CAS. The read revalidates
the current lease/root after physical IO before exposing bytes. Release conditionally deletes the row only when the
process-local reference count reaches zero. Expired leases still veto until the deletion coordinator has waited its
drain grace and revalidated exact expiry/root state.

## 4. BookKeeper custom metadata

Every created ledger carries a bounded canonical map：

```text
nereus.format              = "NBKL1"
nereus.cluster-sha256      = SHA-256(cluster name)
nereus.cluster-alias       = exact UTF-8 alias
nereus.stream-sha256       = SHA-256(streamId UTF-8)
nereus.segment-sequence    = canonical unsigned decimal
nereus.allocation-id       = exact allocation id
nereus.config-sha256       = configuration binding digest
```

The canonical map digest sorts keys by unsigned UTF-8 bytes and frames each key/value length. Create reconciliation、
open、seal and delete all compare the complete expected map/digest plus BookKeeper quorum/digest facts. A ledger with
the candidate id but mismatching metadata is foreign and moves the Nereus allocation root to QUARANTINED/intent to
FOREIGN_COLLISION; Nereus never deletes it.

## 5. Allocation protocol

### 5.1 Normal path

```text
1. require current AppendSession + stored BK profile/config binding
2. choose random positive 63-bit candidate; require no root at exact identity
3. putIfAbsent LedgerAllocationIntent(PREPARED)
4. CAS writer IDLE -> ALLOCATING(exact intent/candidate/session)
5. putIfAbsent global root ALLOCATING                 global id reservation
6. CAS intent PREPARED -> ROOT_RESERVED
7. createAdv.withLedgerId(candidate) with exact custom metadata/quorums/digest
8. validate returned handle/ledger metadata
9. CAS root ALLOCATING -> ACTIVE(epoch + 1)
10. CAS intent ROOT_RESERVED -> PHYSICAL_CREATED/ACTIVATED
11. CAS writer ALLOCATING -> ACTIVE(nextEntryId=0, no reservation)
```

The final three rows may temporarily disagree after a crash. Recovery accepts only exact same allocation/root/session
and advances missing monotonic stages. It never creates a second ledger because one stage's response was lost.

### 5.2 Collision and response loss

- Root `putIfAbsent` conflict with another exact identity：reload. Same allocation converges; any other root consumes
  the candidate, so writer state/intent are terminalized and a fresh candidate is selected。
- BookKeeper `LedgerExists` or create timeout：call `getLedgerMetadata(candidate)`。Exact custom metadata/quorums
  means the original create succeeded；mismatch is `FOREIGN_COLLISION` and no delete；`NoSuchLedger` while outcome is
  uncertain leaves ALLOCATING for bounded scanner reconciliation/retry。
- A definitively failed create is not immediately forgotten. The allocator performs two `NoSuchLedger` observations
  separated by `lateCreateAuditGrace` before `ALLOCATING -> ABORTED`。A matching late ledger restarts cleanup；a foreign
  one quarantines。
- Listing is diagnostic only. Exact id/root/metadata lookup is the correctness path。

## 6. Entry-range reservation protocol

```text
1. validate batch fits active ledger thresholds and compute NBKR1 checksum
2. desired range = [writer.nextEntryId, nextEntryId + entryCount)
3. putIfAbsent immutable reservation(RESERVED, exact logical/provider facts)
4. CAS writer ACTIVE(no reservation, next=N)
              -> ACTIVE(activeReservation=id, next=N+entryCount)
5. CAS reservation RESERVED -> WRITING
6. perform explicit-id writes
7. CAS reservation WRITING -> DURABLE
8. generic commit/protection/head protocol
9. CAS reservation -> COMMIT_PREPARED/HEAD_COMMITTED as facts become provable
10. CAS writer clear activeReservation; never decrement nextEntryId
```

If step 4 fails, no BK write is permitted and an unreferenced reservation can be retired after exact state/head/commit
absence plus grace. If any write is partial/unknown, the current ledger moves to SEALING and no later range reuses its
tail.

## 7. Seal and ownership-recovery protocol

```text
writer ACTIVE/RECOVERING with exact ledger
  -> CAS root ACTIVE -> SEALING(epoch + 1, reason)
  -> CAS writer -> RECOVERING (if not already)
  -> recovery-open exact ledger withRecovery(true)       only here
  -> close/reload LedgerMetadata; require closed
  -> CAS root SEALING -> SEALED(epoch + 1, lastEntryId, length)
  -> terminalize/clear exact active reservation
  -> CAS writer RECOVERING -> IDLE
```

Close/recovery response loss converges by exact `getLedgerMetadata` closed/LAC/length/custom-metadata validation.
An empty sealed ledger records `lastEntryId=-1,length=0`。A mismatch quarantines; it is never guessed from a cached
handle.

## 8. Whole-ledger deletion lifecycle

The exact eligibility proof is detailed in document 05. Metadata transition mechanics are：

```text
SEALED(version V, epoch E)
  -> collect complete protections/leases/writer/reservation/reference evidence
  -> final exact root/evidence reload
  -> CAS SEALED -> MARKED(E+1, attempt, referenceSetSha, deleteNotBefore)
  -> wait drain grace; reject new leases/protections
  -> recapture and compare complete evidence
  -> CAS MARKED -> DELETING(E+1, deleteStarted)
  -> getLedgerMetadata and validate exact custom metadata
  -> deleteLedger(exact id)
  -> observe NoSuchLedger at T1
  -> wait lateCreateAuditGrace; observe NoSuchLedger at T2 > T1
  -> final root/attempt/evidence revalidation
  -> CAS DELETING -> DELETED(E+1, T1, T2)
```

Delete timeout/response loss never causes a blind second delete. Recovery first reloads metadata：

- `NoSuchLedger` continues the dual-absence proof；
- matching original custom metadata permits idempotent retry under the same DELETING intent；
- mismatching metadata moves root to QUARANTINED and **must not delete** the new/foreign ledger。

Any reference drift while MARKED causes conditional `MARKED -> SEALED(epoch + 1)` unmark. Drift after DELETING is
fail-closed/manual unless the journal proves it is only the coordinator's planned metadata-first removal; bytes are
not deleted without the exact frozen proof.

## 9. Codecs and store surfaces

Add V1 codecs registered in `MetadataRecordCodecFactory`/focused BK registry：

- `BookKeeperWriterStateRecordCodecV1`；
- `LedgerAllocationIntentRecordCodecV1`；
- `BookKeeperLedgerRootRecordCodecV1`；
- `BookKeeperAppendReservationRecordCodecV1`；
- `BookKeeperLedgerProtectionRecordCodecV1`；
- `BookKeeperLedgerReaderLeaseRecordCodecV1`。

Each codec requires：golden bytes、round trip、truncation/trailing-byte rejection、unknown enum/schema rejection、
UTF-8/bounds/overflow tests、stored-envelope checksum tests and a maximum encoded-size fixture. The task/commit target
continues to use the existing `ReadTargetRecord` and BookKeeper target codec; no duplicate target encoding is added.

Focused interfaces：

```java
interface BookKeeperWriterMetadataStore { get/create/cas writer; create/get/cas/scan allocation/reservation; }
interface BookKeeperLedgerMetadataStore { get/create/cas/scan root; create/get/cas/delete/scan protection/lease; }
```

Production Oxia and fake stores must pass one shared contract scenario, including put/CAS/delete response loss and
fresh-process pagination. A generic unbounded metadata delete/list API is not introduced.

## 10. Scale bounds

- 256 root shards; page size validated in `[1, 1024]`；
- one active writer row and at most one active reservation per stream；
- terminal reservation/allocation rows retire only after commit/recovery/root proofs and audit grace；
- protections scale with live ranges/tasks, not messages: one record per append range/reference owner；
- reader lease keys scale with active process/ledger pairs, not read calls；
- DELETED/ABORTED roots remain compact tombstones to prevent ledger-id ABA；
- rollover defaults and hard maxima are configuration-gated so one ledger cannot accumulate an unbounded protection
  scan；BK-M6 includes the exact maximum-ranges-per-ledger boundary。

Metrics or sampled inventories may summarize these records, but no summary is deletion authority.
