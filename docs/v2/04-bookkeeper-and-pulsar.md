---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: CurrentSourceReceipt
authority: NormativeWithOpenGates
sourceTuple: v2-m1
---

# BookKeeper and Pulsar

The profile and ACK boundaries, protocol-native position model, Kafka BookKeeper run/range-index direction, and Pulsar
ManagedLedger offload authority are accepted. The M2-K0 contract fixes how exact Kafka `NBKE2` wire/hard caps,
modules, provider sessions, and evidence surfaces must land; their production implementation has not started.
Operational index/pipeline/default bounds and dedicated-ledger capacity remain M2-K9 evidence under
`V2-OPEN-BK-02`. Pulsar 0.2 offload execution and its one-data-plus-root Object pair are accepted.

## Shared BookKeeper contract

Both BookKeeper profiles:

- acknowledge only after the configured quorum accepts the complete typed Protocol Coverage;
- fence old writers through Owner Epoch plus BookKeeper fencing/recovery semantics;
- keep a sealed ledger readable until its replacement generation and all reader/retention protections are safe;
- treat create/add/seal/delete response loss as uncertain provider outcomes;
- never add Object latency to the ACK boundary.

BookKeeper ledger/entry coordinates always identify a `BookKeeperExtent`. They become protocol positions only on the
native Pulsar BookKeeper path; Kafka retains Kafka Offsets regardless of its physical ledger placement.

## Kafka BookKeeper layout

ADR 0086 fixes one Kafka Position Domain across all three canonical profiles. Kafka Offset remains protocol truth;
consumer-group committed offset is only a cursor into that domain and has no independent BookKeeper mapping. Object or
BookKeeper generations may replace Physical Extents without changing the covered Kafka range. `OBJECT_WAL` uses its
authenticated Object directory; the two BookKeeper-primary profiles use the run/range-index layout below.

The default is one logical BookKeeper ledger chain per Kafka partition. One ACTIVE/SEALED/RETIRED run is the owner,
rollover, recovery, materialization, retention, and source-retirement unit. BookKeeper journal/entry-log files are still
physically shared; this is not a dedicated-disk choice. A global mixed-partition ledger is excluded from 0.2. A pooled
cold-partition layout needs a future ADR and evidence rather than becoming an automatic scale fallback.

Mapping has two levels: the Kafka authority/manifest system stores only low-frequency partition-to-run and sealed
generation roots, while immutable `RANGE_INDEX_BLOCK` control entries inside the ledger map RecordBatch Kafka ranges
to exact DATA entries. One partition storage append is one all-or-none `KafkaAppendCommitSet`; one complete
RecordBatch is one lookup unit; one sealed run is one lifecycle unit. Offset coverage comes from each assigned
RecordBatch header, never record count. The first implementation stores one RecordBatch per DATA entry.

Before offset assignment, the owner reserves completion-tracker and active-tail-locator capacity. It assigns Kafka
ranges and contiguous ledger-entry ranges in admission order, pipelines bounded BookKeeper writes, and publishes
visibility/ACK only through the greatest contiguous successful group. A later durable group waits behind an earlier
gap; definitive failure fences the run instead of committing around the hole. Index checkpoints are asynchronous and
do not enter the ACK cut. Owner-local locators make the ACKed active tail readable before the next checkpoint.

Random Fetch floor-searches run, index block, and packed locator, then reads only the target entry or minimum adjacent
range. It validates BookKeeper digest, `NBKE2` CRC32C, and Kafka RecordBatch header/CRC. SHA-256 block/run roots serve
seal, scrub, recovery, and materialization rather than forcing a full append-range hash on every Fetch.

Takeover starts at the last valid index checkpoint, scans only a bounded entry/byte/time tail, reconstructs the greatest
gap-free committed offset, seals the old run, publishes its footer/root, and opens a new run. Candidate checkpoint,
index, locator, recovery-tail, pipeline, and rollover operational defaults remain evidence inputs; persisted parser
caps and checked allocation/admission formulas must already be fixed by K0. M2 must also cover 10k/100k
partitions, open-handle memory, metadata operations, recovery time, bookie pressure, and rollover rate. Failure of that
gate blocks the profile or triggers a new layout decision; it does not silently weaken the accepted authority.

Normal append never writes one remote metadata reservation/mapping per Produce. The V1 reservation/protection and
extent-wide `rangeChecksum` path is not retained or dual-written. Exact implementation-input and data-path cuts are in
[the M2-K0 closure](detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) and
[the M2 Kafka BookKeeper detailed design](detailed_design/m2/kafka-bookkeeper-offset-range-index.md).

### Kafka protocol frontiers on shared BookKeeper

ADR 0087 separates `Allocated`, profile-`Durable`, `Readable/LEO`, Kafka HW, and Kafka LSO. The required order is
`LogStart <= LSO <= HW <= LEO <= Durable <= Allocated`. A BookKeeper quorum result proves physical durability; it does
not by itself become Kafka HW. `acks=1` waits for the coherent locator/producer/transaction/leader-epoch publication
that advances LEO. That publication checks exact Binding/Storage/Owner/Kafka-leader/state fences under the same cut as
leadership transition; a stale durability callback cannot publish then fail a later check. `acks=all` retains native
ISR/minISR admission and waits for HW.

The default shared-storage replica path writes payload bytes once. Compact commit descriptors flow through native
replica Fetch/fetcher, and followers validate/journal them before Observed progress. Payload/source read and producer /
transaction/leader-state replay advance Applied later. Kafka derives HW over eligible Observed progress; leader
admission requires Applied through the native election-adoptable frontier. No follower performs a second WAL payload
append merely to preserve Kafka replication semantics.

Producer-state, transaction-index, leader-epoch, and range-index checkpoints form one compatible recovery vector. A
takeover scans only their bounded suffix to derive a physical candidate and producer/transaction/first-unstable /
leader-epoch state. Native election caps adopted LEO; native recovery supplies HW; LSO is then derived. Physical bytes
beyond the elected boundary remain inert. Component checkpoint cadence is asynchronous, but aggregate uncovered
entries/bytes/age/time and sealed-run completeness are mandatory. Each BK run is bound to one Kafka leader epoch.

Random Fetch performs floor plus coverage check plus successor so a compacted-away batch does not make the previous
batch cover a hole. Replica/read-uncommitted/read-committed upper bounds are LEO/HW/LSO. Sequential reads may reuse a
version-checked disposable cursor but capture a fresh pinned read view per Fetch. Detailed implementation is in
[the M2 Kafka Produce/Fetch design](detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md).

## Pulsar native BookKeeper path

For Pulsar BookKeeper profiles, ManagedLedger remains the native append/read/cursor lifecycle. Nereus must not insert a
generic remote-metadata commit between BookKeeper completion and the ManagedLedger result. `(ledgerId, entryId)` stays
the protocol-visible position and MessageId truth.

A `PulsarCoverage` is a ledger-keyed collection of half-open entry ranges. When it crosses ledgers, the authoritative
ManagedLedger Ledger Chain proves ordering and adjacency. V2 does not persist a durable ledger base or compute
`logicalOffset = ledgerBase + entryId`; it also does not order ledgers by numeric ledger ID alone.

For Pulsar `OBJECT_WAL`, the same Pulsar Position Domain and ledger-chain rules describe `PulsarCoverage`, while
durable bytes use `ObjectExtent`. A Pulsar-cell `PulsarVirtualLedgerStore` in MetadataStore/Oxia allocates reserved-domain
virtual ledger IDs and publishes explicit append-only Ledger Chain order. Entry IDs are allocated serially in the active
virtual ledger. The deployment excludes `[2^62, 2^63 - 2]` from native allocation and assigns each cell one
non-overlapping, never-reused slice. The immutable `ledgerIdCompatibilityNamespaceId` names the actual shared numeric
space. Its 32-byte identity is
`SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])`; the admitted native `INSTANCEID` is an exact lowercase
canonical, non-zero, 36-byte ASCII UUID. M1
admits only a ledger root proven absent immediately before init, either never created or after a qualified expected-ID,
non-force nuke. Format, missing/recreated INSTANCEID, force/direct nuke, or a changed ID does not prove freshness.
Before admission it may have no Registry; while V2 allocation is admitted exactly one bounded Registry is selected.
Its canonical complete assignment table advances through single-key CAS, while allocators consume a namespace-bound
versioned derived slice view and never reread/copy the 64-KiB Registry per rollover. The cell allocator issues
increasing IDs with permitted gaps. Numeric monotonicity keeps stock MessageId comparison compatible, but explicit
predecessor/head metadata remains chain authority. Object identity, bytes, and group/run sequence never become
MessageId truth.

Each slice owner is the durable deployment/reservation-domain/Pulsar Protocol Cell tuple, independent of broker,
session, alias, and provider configuration. Lifecycle is irreversible `ACTIVE -> RETIRING -> RETIRED`; only ACTIVE
allocates, and RETIRED assignments/bounds remain permanent never-reuse tombstones. Exhaustion is derived, not a
lifecycle state. Each Cell has one immutable equal-size aligned `2^k` slice. Numeric capacity is
`floor((2^62 - 1) / 2^k)`, the top `2^k - 1` IDs remain unused, and separate registry-byte/lifetime-assignment limits
include retired Cells. Bootstrap fixes `k=40`, 65,536 canonical registry bytes, 256 lifetime assignments, and a
192-byte maximum assignment row. 0.2 forbids resize, relocation, in-place extension, and a second slice. Exhaustion
fails closed before another ID is allocated; more capacity requires a new Protocol Cell and does not migrate existing
topics or ledgers. After 256 lifetime Cells, only a new `ledgerIdCompatibilityNamespaceId` backed by a bootstrap-proven
disjoint numeric namespace, or an independent deployment/cluster, may allocate again; a second logical reservation
domain in the same namespace cannot reuse the interval. Allocator mode,
exact RANGE wire/size, rollover, and later Ledger Chain mechanics remain open.

ADR 0055 freezes the evidence protocol but selects neither allocator mode. It measures maximum sustainable rollover
RPS while all predeclared queue/latency/error/recovery SLOs hold, covers real rollover distributions/jitter/storms and
all crash cuts, including broker-wide takeover of 10,000/100,000 ManagedLedgers, and compares native Pulsar
rollover/append-stall behavior. STRICT and RANGE correctness work proceed in parallel. Allocator identity may persist
only mode, protocol version, and recovery/fencing identity; performance budgets remain versioned Cell policy/evidence
and host capacity remains a runtime ceiling. ADR 0061 constrains any RANGE candidate without selecting it: a grant
belongs to the ManagedLedger incarnation, takeover changes only head owner epoch, a new owner may finish an unchanged
RESERVED grant, and at most one stale candidate ID burns instead of the range tail. Installed-range use begins after
head install; allocator clear is a high-priority background reconciliation that still blocks the next Cell grant.
Unknown responses reread exact equality, while only definitive conflicts fence. Permanent orphan candidates are
bounded metadata evidence rather than a new 0.2 GC protocol.

M1 implements the complete mode-independent Registry and real-Oxia conformance. Registry authority contains one bounded
inline canonical writer set; there is no external snapshot/reference in 0.2. Rows bind stable writer, exclusion-contract,
independently revocable principal, interlock-policy, and typed evidence identities; source/artifact SHA remains in the
receipt. Exclusive admin/ACL interlock, fresh-root proof/init, writer upgrade, legacy-principal revoke plus negative-
allocation proof precede the final Registry activation. A new writer is committed before start; removal follows fence,
drain, and independent revoke; rolling upgrade may commit old and new entries together. Shared credentials are invalid.
After activation, root/INSTANCEID format/nuke/mutation fences the Registry and all derived views.
Writer kinds close to native BookKeeper ID allocation and Nereus virtual-ledger ID allocation. Each independently
revocable cohort uses one exact 120-byte row containing kind/contract, positive principal/interlock generations and
non-zero SHA-256 values, plus typed evidence kind/version/SHA; there is no random writer-entry ID or generic external
kind. Immutable `RegistryAdmissionEvidenceV1` proves the complete activation cut but is not allocation authority and is
not read per rollover. M1.1c-R0 evidence freezes `maxWriterCount=14`: each of the two kinds has four
binary-by-credential overlap rows, one fresh-principal rollback row, one fenced residue, and at most one
allocation-capable bootstrap/admin row. The exact formula is
`184 + writerCount * 120 + sum(assignmentRowCanonicalBytes)`, so the unchanged 256-by-192 assignment boundary produces
a 51,016-byte largest legal canonical Registry/Oxia value and leaves 14,520 bytes inside the inherited 65,536-byte
envelope. Row 15 and byte 51,017 fail with stable count/byte errors. There is no separate writer-set-byte cap.
Patching one Pulsar generator alone is not a completeness proof. The Registry emits `REGISTRY_CONFORMANCE`; the former
V1 allocator is removed or isolated rather than renamed. STRICT/RANGE candidate SPI and cut injection exist only in
test/evidence code and emit `HARNESS_CONFORMANCE_ONLY` with schema-fixed `selectionEligible=false`; they persist no mode
and install no production allocator. M3 owns 10k/100k multi-broker capacity evidence and any eventual selection.

Online Pulsar BookKeeper/Object evolution is not implied by this model. New-incarnation migration versus a future hybrid
ledger-chain design remains `V2-OPEN-PUL-MIGRATION-01`.

## Async Object offload authority

For Kafka `BOOKKEEPER_WAL_ASYNC_OBJECT`, the Nereus manifest joins sealed Kafka Offset Range coverage to the preferred
Object Extent while retaining the BookKeeper Extent as protected fallback.
Materialization may combine several SEALED Kafka BookKeeper runs, but the Object directory must reproduce their exact
gap-free RecordBatch coverage. Publishing the new generation changes source selection only; Kafka offsets and group
committed offsets remain unchanged. BookKeeper deletion still waits for generation publication, exact Object
verification, logical retention, read-pin/source-protection drain, and response-loss-safe delete proof.

For Pulsar, native ManagedLedger ledger/offload metadata is the sole authority for attempt identity, completion,
offloaded read selection and fallback, and BookKeeper deletion eligibility. Nereus implements a custom
`LedgerOffloader` that produces the accepted immutable Object format and completes only after its bytes and authoritative
root are durable and readable. A Nereus manifest is a rebuildable derived read/materialization index; it cannot complete
a native offload, overrule fallback, or independently authorize ledger deletion. Disagreement fails closed in favor of
ManagedLedger. ADR 0017 is authoritative.

0.2 calls the ledger-based `offload(ReadHandle, UUID, ...)` only for sealed, non-current ledgers. It does not stream the
current append-admitting ledger to Object storage. Ledger rollover bounds immutable coverage; size/entry/age policy and
lag admission bound cold-copy delay without moving Object into the ACK path. ADR 0020 is authoritative.

One attempt publishes exactly one bounded immutable data Object followed by one deterministic immutable sparse-index/
root Object. Both keys are attempt-scoped and derivable from `(ledgerId, UUID)` inside the Cell Provider Scope. Multipart
transfer may construct the single data Object. Offload completes only after both objects, contiguous `0..LAC` coverage,
digests, and one ledger-equivalent `ReadHandle` are verified. Reads present one sealed ledger; idempotent cleanup derives
both keys even when the root is absent. ADR 0024 is authoritative.

Key-derivation v1 uses
`pulsar-offload/v1/ledger-<ledgerId>/attempt-<uuid>/{data|root}` beneath the persisted Cell Provider Scope; native driver
metadata retains exact location/prefix/version so current configuration cannot reinterpret an old attempt. The bounded
canonical root binds attempt identity, sanitized closed-ledger metadata, data length/SHA-256 and outer format, contiguous
sparse rows, and a separate root self-digest. Publication verifies data, root, then the actual offloaded read path before
success. Cleanup proves root absent before data absent and covers attempt-scoped multipart residue. ADR 0029 is
authoritative.

The root uses the independent big-endian `NPO1` major format: a fixed 32-byte header, exactly four ordered typed
sections (`ATTEMPT`, `SEALED_LEDGER`, `DATA_EXTENT`, `SPARSE_INDEX`), and a trailing SHA-256 over all preceding canonical
bytes. Strict UTF-8, canonical map ordering, duplicate/trailing/overflow rejection, and hard limits bound the complete
root to 8 MiB, sparse rows to 65,536, metadata/strings/ensemble dimensions to their ADR 0035 caps, and entry count to
`1..2^31-1`. HEAD enforces root size before a bounded full GET/self-digest; empty ledgers are not offload attempts.
The ATTEMPT section also stores a bounded wrapped-key envelope with the exact provider, wrapping-key identity/version,
and wrap-algorithm identity. Native restart validates NPO1 before unwrapping that persisted envelope; plaintext AES-256
attempt-key bytes never enter NPO1 or native driver metadata, and current host configuration cannot replace the
persisted wrapping identity.

The data Object uses ordered, gap-free `NPD1` multi-entry blocks. Each NPO1 sparse row binds one block ordinal,
contiguous entry range, offset, encoded/decoded lengths, codec/encryption family, and SHA-256 of the exact encoded
block. A bounded canonical block directory uses 16-byte `{decodedOffset:uint64,payloadLength:uint32,flags:uint32}` rows;
entry IDs derive from authenticated `firstEntryId + ordinal`. The checked domain is 32-byte NPD1 header plus the sum of
64-byte NPB1 header, directory+compressed-payload GCM ciphertext, and 16-byte tag. Entries never cross blocks; an
oversize entry gets a dedicated bounded block. Compression, AEAD, and integrity reset per block, with no padding or
cross-block state. Every operation uses checked arithmetic/actual-count allocation, while upload, SHA-256, and full
verification stream or use bounded segments rather than a data-Object-sized `ByteBuffer`.

The selected hard envelope is 4 GiB per data Object, 1,024 multipart parts, 64 MiB per entry and decoded block, and
65,536 entries per block. Multipart part count is an adapter/Cell operational ceiling, not wire identity. Admission checks
provider max Object size, min/max part size, max parts, streaming upload/read, and deterministic multipart-residue
cleanup. Missing capability rejects the profile.

NPD1 block-policy evidence compared every 1/4/8/16-MiB candidate and selected `latency-1mib`, `balanced-4mib`, and
`scan-8mib`; 4 MiB is the Deployment base default and 16 MiB is rejected. Product/Deployment owns the validated base
default, Namespace inherits/overrides it, Topic may explicitly
override, Cell admits/caps, and host ceilings resources. The resolved class is fixed in the offload attempt. Resource
pressure may early-close/backpressure/reject but never reinterprets existing NPD1.

Native read source eligibility is fixed by ManagedLedger metadata. Before completion it is BookKeeper-only; after
`complete && !bookkeeperDeleted` both sources may participate in one bounded whole-range fallback; after
`bookkeeperDeleted=true` reads are Object-only even if physical BookKeeper residue exists. That boolean is only the
compatibility fence for `BK_DELETE_INTENT` or `BK_DELETE_DONE`; only DONE proves physical absence. Object-first integrity or
availability failure may retry the complete range once from BookKeeper. BookKeeper-first may use Object only after
native missing-ledger resolution. Partial entries are released, sources are never mixed within a range, fallback never
loops, and Object corruption remains quarantined/deletion-vetoed even when fallback succeeds.

ManagedLedger caches one `DualSourceReadHandle` per ledger, with lazy Object and BookKeeper children. Every admitted
range owns one source pin bound to the exact native metadata version/offload attempt. Before the deletion CAS,
ManagedLedger fences new BK pins, waits boundedly for admitted BK pins to drain, performs final Object revalidation,
then CASes `BK_DELETE_INTENT` plus `bookkeeperDeleted=true`; only afterward does it invalidate/close the BK child and
issue physical deletion. Success or authoritative `NoSuchLedger` advances the same attempt to `BK_DELETE_DONE`.
Fallback releases partial entries and the primary pin before rechecking eligibility and pinning the secondary.
Composite close stops admission, drains both sources, and closes each child exactly once.

A BookKeeper source becomes physically deletable only after all of these are durable and revalidated:

- the exact sealed Kafka Offset Range or ledger-keyed Pulsar Coverage and checksum were materialized;
- the preferred Object generation was published and is readable;
- native Pulsar offload/ledger metadata recognizes the replacement where applicable;
- typed logical retention passed the whole source coverage;
- no cursor, reader pin, recovery root, task, or source protection references it;
- grace and response-loss reconciliation completed.

Offloader completion creates deletion eligibility; it does not itself bypass the remaining native retention, cursor,
read-pin, deletion-lag, or Nereus source-protection checks.

Each offload attempt persists Topic/Namespace retention class `RETAIN_BK` or `DELETE_AFTER_VERIFIED`. RETAIN_BK keeps
delete state NONE. DELETE_AFTER_VERIFIED makes pin drain, final Object revalidation, INTENT, delete proof, and DONE
mandatory; after INTENT it cannot return to retention. Retirement, audit, and physical capacity use the three-state
fact, not the compatibility boolean. Delete concurrency/bandwidth/retries are Cell/host budgets only.

Immediately before native BK_DELETE_INTENT plus `bookkeeperDeleted=true`, ManagedLedger revalidates the exact persisted
attempt, complete NPO1/self-digest, data immutable version/length/SHA, closed-ledger facts, and production-reader
first/last/sparse boundaries without holding the metadata mutex across Object I/O. The final native CAS rechecks the
same attempt UUID and eligible state. Timeout, missing, or mismatch retains BookKeeper; permanent corruption
quarantines the attempt.

## Lag policy

Async offload exposes pending ledgers/bytes/age and the oldest unmaterialized typed Protocol Frontier. Policy may alert,
throttle, or stop new admission before BookKeeper capacity is exhausted. It never changes an already admitted append
into a synchronous Object write.

Relevant tradeoffs: `T-BK-01`, `T-LEDGER-01`, `T-KAFKA-01`, `T-PROTOCOL-01`, `T-POSITION-01`, and
`T-POLICY-01`. Required scenarios: `V2-BK-001..017`, `V2-KAF-DATA-001..022`, `V2-POSITION-001..018`, and
`V2-POLICY-001..002`. See
[ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md),
[ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md),
[ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md),
[ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md),
[ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md),
[ADR 0029](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md),
[ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md),
[ADR 0035](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md),
[ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md),
[ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md),
[ADR 0044](../decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md),
[ADR 0045](../decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md),
[ADR 0048](../decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md),
[ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md),
[ADR 0052](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md),
[ADR 0054](../decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md), and
[ADR 0055](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md), plus
[ADR 0056](../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md) and
[ADR 0057](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md), with RANGE takeover constrained by
[ADR 0061](../decisions/0061-v2-pulsar-range-grant-owner-takeover.md) and M1 Registry/witness/evidence bounds refined by
[ADRs 0082](../decisions/0082-v2-m1-domain-and-control-authority-contracts.md) and
[0083](../decisions/0083-v2-m1-wire-control-and-evidence-bounds.md), with the Kafka BookKeeper path fixed by
[ADR 0086](../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md) and its protocol semantics by
[ADR 0087](../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md).
