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
ledgerIdentity = SHA-256(decodeLowerHex32(providerScopeSha256) || u64be(ledgerId))
shard          = unsigned(first byte of ledgerIdentity)
ledgerHash     = lowercase hex ledgerIdentity

${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/roots/${ledgerHash}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/protections/${rangeSlot:05d}/${protectionSlot:02d}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/readers/${readerSlot:05d}
${clusterPrefix}/physical-bookkeeper/v1/${shard:03d}/ledgers/${ledgerHash}/retirement/${gcAttemptId}/manifest
${clusterPrefix}/physical-bookkeeper/v1/allocation-slots/${slotShard:02d}/${slot:05d}
```

The first four physical-ledger families use one `PartitionKey("physical-bookkeeper-v1-%03d")` per root shard. This
permits root/protection/lease conditional operations in one partition where Oxia supports them, but the protocol still
specifies individual CAS/reloads and never assumes a transaction that the client surface does not expose.

Allocation slots use 16 fixed slot shards and `PartitionKey("physical-bookkeeper-allocation-v1-%02d")`。The configured
slot count is immutable in the binding；slot numbers are `[0,maxUncertainAllocations)`、`slotShard = slot & 0x0f`, and
strict parsing/re-encoding rejects any key outside that range or with the wrong shard.

Protection slots are fixed by `(rangeSlot, protectionSlot)`：`rangeSlot` is
`[0,maxAppendRangesPerLedger)` and `protectionSlot` is `[0,protectionSlotsPerRange)`。Reader slots are fixed in
`[0,maxReaderLeasesPerLedger)`。Put-if-absent/CAS on these exact keys, rather than scan-then-increment, is the
cluster-wide admission authority for each bound.

Root scan boundaries are `000..255`, each from an empty opaque continuation. Page tokens bind cluster、shard、prefix
and page size; callers cannot convert tokens to key order or reuse them in another shard.

### 2.3 Strict inverse and identity rules

`BookKeeperKeyspace` must provide strict parsers for root、protection and reader keys used by recovery/retirement.
Parsing succeeds only when re-encoding yields byte-for-byte the supplied key. Fixed slot components are canonical
zero-padded decimal and must be inside the stored configuration bounds. `ledgerHash` is always recomputed from
record `(providerScopeSha256, ledgerId)` and must match the key/shard. `clusterAlias` must resolve to the same stored
scope/config binding. A hash collision between different exact identities is
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
    long activePhysicalBytes,
    int activeAppendRangeCount,
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
- `nextEntryId`、`activePhysicalBytes` and `activeAppendRangeCount` are advanced by checked addition **before** any
  write for that reservation；none moves backward while the ledger is ACTIVE；
- a new append-session epoch cannot adopt the old ledger as ACTIVE: it first enters RECOVERING, fences/seals, then
  allocates a new ledger；
- metadata version is hydrated from Oxia and encoded as zero in desired-value comparisons, following current wrappers。

`IDLE` is a real durable state, not absence. Absence is allowed only before the first BK profile append and is created
with the stored profile/config binding after first-create admission.

Writer lifecycle wire ids：`IDLE=1`、`ALLOCATING=2`、`ACTIVE=3`、`RECOVERING=4`、`CLOSED=5`。Codecs use explicit
ids, never enum ordinals。

### 3.2 `LedgerAllocationIntentRecord`

```java
record LedgerAllocationIntentRecord(
    int schemaVersion,
    String allocationId,                     // >=128-bit random base32
    String streamId,
    long segmentSequence,
    String clusterAlias,
    long candidateLedgerId,                  // positive 63-bit exact id inside reserved prefix
    int allocationSlot,
    String configurationBindingSha256,
    String writerId,
    String writerRunIdHash,
    long appendSessionEpoch,
    String fencingTokenHash,
    long writerStateEpoch,
    LedgerAllocationLifecycle lifecycle,     // PREPARED, ROOT_RESERVED, CREATE_UNCERTAIN,
                                            // PHYSICAL_CREATED, ACTIVATED, FOREIGN_COLLISION, ABORTED
    boolean lateCreateHazard,                // monotonic false -> true; never clears in BK-M0-M6
    String bookKeeperMetadataSha256,
    long createdAtMillis,
    long updatedAtMillis,
    String stateReason,
    long metadataVersion) { }
```

The intent is workflow/recovery evidence, not physical ownership authority. The root owns the candidate globally and
the writer state selects the active segment. Every lifecycle CAS reloads and verifies both identities.

Allocation lifecycle wire ids：`PREPARED=1`、`ROOT_RESERVED=2`、`CREATE_UNCERTAIN=3`、`PHYSICAL_CREATED=4`、
`ACTIVATED=5`、`FOREIGN_COLLISION=6`、`ABORTED=7`。Adding a state requires a new id/schema review；ids never shift。
`lateCreateHazard` is independent of workflow progress：`CREATE_UNCERTAIN -> PHYSICAL_CREATED -> ACTIVATED` is legal
only while preserving `true`。No codec/store transition accepts `true -> false`。

### 3.2.1 `BookKeeperAllocationSlotRecord`

A fixed durable slot is acquired before any `CreateAdv` transmission, making the cluster-wide uncertainty cap
race-free across brokers：

```java
record BookKeeperAllocationSlotRecord(
    int schemaVersion,
    int slot,
    String allocationId,
    String streamId,
    long candidateLedgerId,
    String ledgerIdentitySha256,
    String configurationBindingSha256,
    AllocationSlotLifecycle lifecycle,      // CLAIMED, CREATE_STARTED, CREATE_UNCERTAIN
    long createdAtMillis,
    long updatedAtMillis,
    long metadataVersion) { }
```

Wire ids are `CLAIMED=1`、`CREATE_STARTED=2` and `CREATE_UNCERTAIN=3`。`CLAIMED` proves provider transmission is
forbidden；the coordinator must durably CAS to `CREATE_STARTED` immediately before invoking `CreateAdv`, and from that
point it conservatively assumes the request may appear later even if the process dies before the call. A broker probes
the fixed slot set from a hash-derived start and claims with put-if-absent；no free slot rejects before provider IO.

A slot is conditionally removed after exact activation only when the original provider future durably reached a
terminal create result without ever entering unknown-outcome recovery, or after a proven pre-transmission abort. If a
deadline/process cut leaves `CREATE_STARTED`, recovery first CASes slot + intent/root `lateCreateHazard=true` and slot
`CREATE_UNCERTAIN`。A later matching ledger may be activated or sealed, but that slot and hazard remain permanently in
BK-M0–M6；provider absence、matching metadata、elapsed grace and eventual trim do not prove a delayed original create
cannot execute after delete. Slot-delete response loss reloads the exact slot/allocation/root before accepting
absence, so capacity may leak safe but can never undercount uncertainty.

A crash after slot claim but before intent creation leaves `CLAIMED` with no intent/root/writer reference. Because
provider transmission is forbidden in that state, the slot scanner may release it only after complete exact absence
and grace. A different occupant after lost conditional-delete response proves the old slot delete applied；recovery
must never delete the new occupant.

### 3.3 `BookKeeperLedgerRootRecord`

```java
record BookKeeperLedgerRootRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    String clusterAlias,
    String providerScopeSha256,
    long ledgerId,
    String streamId,
    long segmentSequence,
    String allocationId,
    int allocationSlot,
    String configurationBindingSha256,
    String ledgerIdNamespaceSha256,
    boolean lateCreateHazard,                // monotonic false -> true; physical GC veto
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
| 8 | `ABORTED` | provider create was never transmitted; allocation cannot later create physical bytes |
| 9 | `QUARANTINED` | identity/provider/metadata conflict; automatic mutation prohibited |

Only `SEALED -> MARKED -> DELETING -> DELETED` can physically delete a previously active ledger. An allocation may
reach `ABORTED` only when its durable slot never advanced beyond `CLAIMED`, which is the protocol proof that provider
transmission was forbidden. Once the slot reaches `CREATE_STARTED`, transmission is
possible, timeout/absence moves the intent to `CREATE_UNCERTAIN` while the root remains `ALLOCATING` and permanently
consumes the id. Before recovery can adopt matching bytes it persists `lateCreateHazard=true` in intent/root and leaves
the exact allocation slot occupied. A bounded scanner keeps exact-checking it；matching late bytes are activated for
the same still-current session or fenced/sealed-and-retained, while mismatching metadata quarantines. Time alone never
proves non-creation or physical-delete safety.

`lifecycleEpoch` starts at one and increments on every root CAS. Immutable identity/quorum/custom-metadata fields never
change. `DELETED` roots are retained as compact durable tombstones in BK-M0–M6; the allocator treats every existing
root, including ABORTED/DELETED, as a consumed candidate id. A later root-compaction feature would have to create a
permanent exact ledger-id tombstone before removing the full root and is outside this delivery.

`lateCreateHazard` is initialized false and may only move false -> true. It remains true across ACTIVE/SEALING/SEALED
and permanently vetoes MARKED/DELETING；a root with the hazard therefore cannot reach DELETED in this protocol version.

`ABORTED` and `DELETED` are normal-path terminal states. The only outgoing edge is a conditional
`ABORTED/DELETED -> QUARANTINED` safety escalation after exact physical reappearance；it never authorizes delete or id
reuse and must retain the original identity/audit timestamps.

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
    int ledgerRangeSlot,
    long firstEntryId,
    int entryCount,
    String rangeChecksumSha256,
    long expectedStartOffset,
    String payloadFormat,
    int recordCount,
    long logicalBytes,
    long physicalBytes,
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
`physicalBytes` is the checked sum of exact BookKeeper entry payload lengths and drives ledger rollover independently
of protocol logical-size accounting. `ledgerRangeSlot` equals the pre-reservation writer
`activeAppendRangeCount` and is never reused inside that ledger, including after abandonment.

Lifecycle is recovery workflow, not visibility. Only a reachable stream head proves `HEAD_COMMITTED`; a row claiming
that lifecycle without the exact generic commit/marker/head is an invariant violation.

Reservation lifecycle wire ids：`RESERVED=1`、`WRITING=2`、`DURABLE=3`、`COMMIT_PREPARED=4`、
`HEAD_COMMITTED=5`、`ABANDONED=6`。They are explicit codec values, not enum ordinals。

### 3.5 `BookKeeperLedgerProtectionRecord`

Protection rows are also the complete ledger-to-range inventory：

```java
record BookKeeperLedgerProtectionRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    String clusterAlias,
    long ledgerId,
    long rootLifecycleEpoch,
    int ledgerRangeSlot,
    int protectionSlot,
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
    ProtectionLifecycle lifecycle,           // RESERVED, ACTIVE
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

Protection lifecycle wire ids are `RESERVED=1` and `ACTIVE=2`；a RESERVED row is already a physical-GC veto but cannot
stand in for its eventual owner.

Every append range owns exactly `protectionSlotsPerRange` fixed keys. Slots `0`、`1` and `2` are created as RESERVED
before physical write for `REACHABLE_APPEND`、`VISIBLE_GENERATION` and `APPEND_RECOVERY` respectively, then CASed to
ACTIVE only after the exact owner exists. A crash may leave a RESERVED veto that recovery either activates from the
append reservation/commit facts or conditionally removes after proven pre-write abandonment.

Slots `[3, protectionSlotsPerRange)` admit materialization、repair and create-before-retire contenders by
hash-started bounded probing plus exact put-if-absent. Each row represents one exact owner/reference, so multiple F4
tasks may protect the same range without collapsing ownership. Replacement creates its new exact row first, proves the
new owner, retires the old owner/row conditionally, then keeps or removes the new row according to that owner's normal
lifecycle. Concurrent contenders win distinct fixed slots or fail before task/repair IO when all slots are occupied；
there is no scan-then-count race and GC scans the complete Cartesian slot bound from empty continuations.

### 3.6 `BookKeeperLedgerReaderLeaseRecord`

```java
record BookKeeperLedgerReaderLeaseRecord(
    int schemaVersion,
    String ledgerIdentitySha256,
    long ledgerId,
    long rootLifecycleEpoch,
    int readerSlot,
    String processRunId,
    long leaseEpoch,
    long acquiredAtMillis,
    long expiresAtMillis,
    long metadataVersion) { }
```

One fixed slot per process/ledger is locally reference-counted. Renewal increments `leaseEpoch` by CAS. The read revalidates
the current lease/root after physical IO before exposing bytes. Release conditionally deletes the row only when the
process-local reference count reaches zero. Expired leases still veto until the deletion coordinator has waited its
drain grace and revalidated exact expiry/root state.

Lease admission first finds an existing exact `processRunId` occupant, otherwise probes the fixed reader-slot set from
a hash-derived start and claims one key with put-if-absent. No free slot rejects before provider IO. A process renews or
conditionally removes only the slot whose full occupant identity it owns；a lost delete response reloads that key and
never deletes a replacement occupant. GC enumerates all fixed slots from an empty continuation and validates slot/key/
record equality. Invalid, duplicate-process or out-of-range rows are a read failure and GC veto, not truncated
evidence.

## 4. BookKeeper custom metadata

Every created ledger carries a bounded canonical map：

```text
nereus.format              = "NBKL1"
nereus.cluster-sha256      = SHA-256(cluster name)
nereus.cluster-alias       = exact UTF-8 alias
nereus.provider-scope      = exact provider scope SHA-256
nereus.ledger-namespace    = exact namespace reservation digest
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

For `P=ledgerIdPrefixBits` and `R=63-P`：

```text
candidate = (ledgerIdPrefixValue << R) | secureRandomUnsigned(R)
candidate > 0
(candidate >>> R) == ledgerIdPrefixValue
```

`P` is `[8,24]` and the highest prefix bit is one, so candidates stay in the explicitly reserved high positive-id
scope with at least 39 random bits. Random collision is handled as a normal root/provider collision and never treated
as impossible.

```text
1. require current AppendSession + stored BK profile/config/ledger-id-namespace binding
2. generate allocationId and a random candidate inside the configured positive-63-bit prefix; round-trip namespace
   predicate and require no root at exact identity
3. claim one durable allocation slot with allocationId/candidate; no slot fails before writer/root/provider mutation
4. putIfAbsent LedgerAllocationIntent(PREPARED, exact slot)
5. CAS writer IDLE -> ALLOCATING(exact intent/candidate/session)
6. putIfAbsent global root ALLOCATING                 global id reservation
7. CAS intent PREPARED -> ROOT_RESERVED
8. CAS allocation slot CLAIMED -> CREATE_STARTED      irreversible uncertainty boundary
9. invoke createAdv.withLedgerId(candidate) with exact custom metadata/quorums/digest
10. validate returned handle/ledger metadata
11. CAS root ALLOCATING -> ACTIVE(epoch + 1, lateCreateHazard=false)
12. CAS intent ROOT_RESERVED -> PHYSICAL_CREATED/ACTIVATED(lateCreateHazard=false)
13. CAS writer ALLOCATING -> ACTIVE(nextEntryId=0, bytes=0, ranges=0, no reservation)
14. conditionally release exact allocation slot after reloading the terminal provider future proof
```

The final four rows may temporarily disagree after a crash. Intent/root both store the exact slot, so recovery performs
an O(1) slot reload rather than a cluster scan；it accepts only exact same allocation/root/session and advances missing
monotonic stages. It never creates a second ledger because one stage's response was lost.

### 5.2 Collision and response loss

- Root `putIfAbsent` conflict with another exact identity：reload. Same allocation converges; any other root consumes
  the candidate, so writer state/intent are terminalized and a fresh candidate is selected。
- BookKeeper `LedgerExists` or create timeout：call `getLedgerMetadata(candidate)`。Exact custom metadata/quorums
  means the original create succeeded；mismatch is `FOREIGN_COLLISION` and no delete；`NoSuchLedger` after possible
  transmission records slot/intent `CREATE_UNCERTAIN` plus monotonic intent/root `lateCreateHazard=true`, and leaves
  the root `ALLOCATING` for bounded scanner reconciliation. A matching metadata reload proves ownership/bytes, but it
  does not clear the hazard or release the slot because it cannot prove that every older create request is terminal。
- Repeated `NoSuchLedger` observations pace alerts/audit but never convert a transmitted uncertain create to
  `ABORTED`. The id/root remain consumed；matching late bytes resume exact activation or stale-session seal-and-retain,
  and a foreign ledger quarantines. `ABORTED` is reserved for pre-transmission/definitively-not-sent paths。
- Listing is diagnostic only. Exact id/root/metadata lookup is the correctness path。

Only the process that observes the original create future's terminal success before any unknown-outcome cut may keep
`lateCreateHazard=false` and release the slot after activation. Recovery from a durable `CREATE_STARTED` state without
that proof is conservative even when matching metadata already exists. BK-M0–M6 define no admin override that clears
the hazard；a future provider-specific operation fence would require a separate protocol/version gate.

An uncertain create does not immediately block one stream. After persisting intent/root hazard and slot
`CREATE_UNCERTAIN`, a CAS may
detach that exact allocation from writer `ALLOCATING -> IDLE` while consuming its segment sequence；the append is
`KNOWN_NOT_COMMITTED` because no entry write was possible. The stream may allocate a fresh candidate subject to the
remaining fixed-slot budget. If matching physical metadata appears later, the scanner must not reactivate a
detached/stale allocation：it drives `ALLOCATING -> SEALING -> SEALED` as an empty-or-provider-reported owned ledger,
but the permanent hazard vetoes automatic physical deletion. A still-selected/current-session allocation may instead
finish normal activation while retaining the same hazard/slot. These branches are chosen only after exact
writer/root/intent/slot CAS reload. Reaching the configured slot maximum therefore fails new ledger allocation closed；
it never silently clears a hazard to regain availability.

## 6. Entry-range reservation protocol

```text
1. compute checked exact physical bytes + NBKR1 checksum and validate entry/byte/append-range thresholds
2. desired range = [writer.nextEntryId, nextEntryId + entryCount)
3. putIfAbsent immutable reservation(RESERVED, rangeSlot=R, exact logical/provider facts)
4. CAS writer ACTIVE(no reservation, next=N, bytes=B, ranges=R)
              -> ACTIVE(activeReservation=id, next=N+entryCount,
                        bytes=B+physicalBytes, ranges=R+1)
5. putIfAbsent mandatory protection slots 0..2 as RESERVED for the exact reservation/range
6. reload writer/reservation/root/all three slots; CAS reservation RESERVED -> WRITING
7. perform explicit-id writes
8. CAS reservation WRITING -> DURABLE
9. generic commit/protection/head protocol
10. CAS reservation -> COMMIT_PREPARED/HEAD_COMMITTED as facts become provable
11. CAS writer clear activeReservation; never decrement nextEntryId
```

If step 4 fails, no BK write is permitted and an unreferenced reservation can be retired after exact state/head/commit
absence plus grace. If any write is partial/unknown, the current ledger moves to SEALING and no later range reuses its
tail. Mandatory protection-slot response loss is resolved by exact deterministic keys. No head CAS is allowed until
REACHABLE_APPEND and APPEND_RECOVERY are ACTIVE；VISIBLE_GENERATION remains a RESERVED veto until generation-zero
publication or reachable-commit repair activates it.

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
  -> require current exclusive ledger-id namespace activation/binding
  -> require lateCreateHazard == false and no retained allocation slot
  -> collect complete protections/leases/writer/reservation/reference evidence
  -> final exact root/evidence reload
  -> CAS SEALED -> MARKED(E+1, attempt, referenceSetSha, deleteNotBefore)
  -> wait drain grace; reject new leases/protections
  -> recapture and compare complete evidence
  -> CAS MARKED -> DELETING(E+1, deleteStarted)
  -> getLedgerMetadata and validate exact namespace + custom metadata
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

BookKeeper has no metadata-version-conditional delete, so the validate-to-delete interval is safe only under the
activated exclusive advanced-ledger-id namespace contract. If its readiness/reservation digest changes or cannot be
revalidated, the coordinator stops before provider delete and invalidates deletion activation. A stock/foreign client
is never permitted to create exact ids in that prefix；normal stock ledger allocation remains outside it. Any physical
reappearance observed for an `ABORTED`/`DELETED` tombstone is an invariant incident and conditionally escalates the
root to `QUARANTINED` for manual handling, never automatic deletion.

Any reference drift while MARKED causes conditional `MARKED -> SEALED(epoch + 1)` unmark. Drift after DELETING is
fail-closed/manual unless the journal proves it is only the coordinator's planned metadata-first removal; bytes are
not deleted without the exact frozen proof.

## 9. Codecs and store surfaces

Add V1 codecs registered in `MetadataRecordCodecFactory`/focused BK registry：

- `BookKeeperWriterStateRecordCodecV1`；
- `LedgerAllocationIntentRecordCodecV1`；
- `BookKeeperAllocationSlotRecordCodecV1`；
- `BookKeeperLedgerRootRecordCodecV1`；
- `BookKeeperAppendReservationRecordCodecV1`；
- `BookKeeperLedgerProtectionRecordCodecV1`；
- `BookKeeperLedgerReaderLeaseRecordCodecV1`。

Each codec requires：golden bytes、round trip、truncation/trailing-byte rejection、unknown enum/schema rejection、
UTF-8/bounds/overflow tests、stored-envelope checksum tests and a maximum encoded-size fixture. The task/commit target
continues to use the existing `ReadTargetRecord` and BookKeeper target codec; no duplicate target encoding is added.

Focused interfaces：

```java
interface BookKeeperWriterMetadataStore { get/create/cas writer; create/get/cas/scan allocation/slot/reservation; }
interface BookKeeperLedgerMetadataStore { get/create/cas/scan root; create/get/cas/delete/scan protection/lease; }
```

Production Oxia and fake stores must pass one shared contract scenario, including put/CAS/delete response loss and
fresh-process pagination. A generic unbounded metadata delete/list API is not introduced.

Implementation checkpoint (2026-07-19)：`BookKeeperWriterMetadataStore` and
`BookKeeperLedgerMetadataStore` expose only the focused operations above；`OxiaJavaBookKeeperMetadataStore` borrows
the existing shared Oxia runtime and validates every decoded key against record identity/configured slot bounds；the
test-fixture adapter runs the identical public contract over deterministic durable state。Create reloads after either
condition failure or ambiguous response loss and succeeds only for the same stored envelope digest；CAS reloads and
accepts only the exact replacement digest at a version greater than the expected version；conditional delete reloads
and accepts only observed absence。All other races fail closed。Scan continuations bind cluster、kind、scope digest、
fixed-depth prefix and page size；the adapter reads at most `limit + 1` rows for a validated `[1,1024]` page and never
exposes an unbounded list primitive。`BookKeeperMetadataStoreContractTest` covers the production/fake shared scenario、
fresh adapter construction、all 256 root shards、all 16 allocation-slot shards and applied-response-loss recovery。

## 10. Scale bounds

- 256 root shards; page size validated in `[1, 1024]`；
- 16 allocation-slot shards scan the configured fixed slot interval from empty opaque continuations；
- one active writer row and at most one active reservation per stream；
- terminal reservation/allocation rows retire only after commit/recovery/root proofs；`CREATE_UNCERTAIN` and
  `lateCreateHazard` remain globally scan-visible/id-consuming；the cluster-wide configured maximum rejects new BK
  allocation before another provider create and never deletes or clears a hazardous root；
- fixed durable allocation slots make that maximum race-free across brokers；slot exhaustion is fail-closed
  availability backpressure, not authority to clear uncertain state；
- protection keys are the fixed Cartesian product of append-range and per-range slots, and each occupied row names one
  exact reference owner；
- reader lease keys are a fixed per-ledger slot set and each process uses at most one, independent of read-call count；
- DELETED/ABORTED roots remain compact tombstones to prevent ledger-id ABA；
- `maxAppendRangesPerLedger` is an explicit configuration/binding field, enforced from durable writer counters before
  reservation, so one ledger cannot accumulate an unbounded protection scan；BK-M6 exercises the exact boundary。
- checked `maxAppendRangesPerLedger * protectionSlotsPerRange` and `maxReaderLeasesPerLedger` bound complete
  ledger-local scans without scan-then-count races. Append creates its three mandatory RESERVED slots before physical
  write；F4 task/repair and reader admission claim exact fixed slots before IO and fail closed at the cap. Any invalid,
  duplicate or out-of-range metadata is an invariant/GC veto, never truncated evidence。

Metrics or sampled inventories may summarize these records, but no summary is deletion authority.
