# ADR 0086: V2 Kafka BookKeeper run, range index, and ordered pipeline

## Status

Accepted for the 0.2 Kafka Position Domain and BookKeeper-primary profiles. This ADR freezes the semantic authority,
index granularity, ACK/publication order, and recovery shape. The accepted
[M2-K0 input closure](../v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) fixes how exact
`NBKE2`/run/index wire and hard parser/admission caps must land before a writer. Their production constants/codecs are
M2 implementation; checkpoint cadence, memory, pipeline, rollover, and 10k/100k admission defaults remain M2-K9
evidence outputs. ADR 0087 refines Kafka protocol frontiers, producer/transaction state, ISR/HW, and Fetch semantics
on this physical layout. The current NSIP-1 BK slice implements Owner admission closure, shared commit and
closed-history recovery; focused unit and actual BK/Oxia cases have passed. Native Controller assignment relocation
has focused controller tests. The native Broker runtime port and the real Controller/Broker acceptance remain open;
these results do not close M5/M6 or establish performance.

## Context

Kafka Offset is protocol truth while BookKeeper ledger/entry coordinates are physical placement. Treating every
`KafkaOffsetRange -> BookKeeperExtent` mapping as an independent remote metadata mutation would put control-plane RTT,
CAS serialization, watch traffic, and metadata GC in the normal Produce path. Conversely, reading a complete append
extent and recomputing one extent-wide checksum before locating one requested batch defeats random Fetch.

V2 needs three distinct granularities:

- one partition storage append / `KafkaAppendCommitSet` for all-or-none visibility and ACK;
- one complete Kafka RecordBatch for offset lookup and targeted read;
- one sealed BookKeeper ledger run for rollover, recovery, materialization, retention, and source retirement.

The three canonical 0.2 profiles share one Kafka Position Domain. They differ in primary-WAL ACK boundary,
preferred source, and BookKeeper-retirement eligibility; they do not define different Kafka offsets. V1 aliases,
sync-Object variants, per-append Oxia reservations, and V1 `rangeChecksum` records are not V2 compatibility paths.

## Decision

### One protocol position model

Every Kafka Topic Partition has one half-open Offset Domain. Consumer-group committed offset remains a Kafka cursor
into that domain and has no separate BookKeeper mapping. A Storage Epoch, materialization generation, Object
replacement, or compaction may replace Physical Extents without changing the covered Kafka Offset Range.

`OBJECT_WAL`, `BOOKKEEPER_WAL_ONLY`, and `BOOKKEEPER_WAL_ASYNC_OBJECT` therefore share the same logical coverage:

```text
Kafka Offset Range
  -> coarse generation/run routing
  -> source-local authenticated directory or range index
  -> exact Kafka RecordBatch bytes
```

`OBJECT_WAL` uses the NWG1 authenticated directory. BookKeeper-primary profiles use the run/index contract below.
`BOOKKEEPER_WAL_ASYNC_OBJECT` may publish an Object generation over the same Kafka range and retain BookKeeper as a
protected fallback. This source-generation switch is not the online cross-profile Storage-Epoch transition excluded
by ADR 0015.

### Default ledger lifecycle

The 0.2 default is one logical BookKeeper ledger chain per Kafka Topic Partition. A run is bound to the exact Binding,
Topic Incarnation, partition, Storage Epoch, Owner Epoch, Kafka leader epoch, Cell Provider Scope, ledger identity, and
a contiguous Kafka Offset Range. Its lifecycle is `ACTIVE -> SEALED -> RETIRED`. A Kafka leader-epoch change always
stops/reconciles the old run and opens a new one; Owner Epoch is not a substitute:

- ACTIVE admits ordered append groups and has an open logical end;
- normally SEALED has a final logical end, complete index directory, footer, and immutable physical bounds;
- a crash-terminated run instead carries an immutable closed-Owner recovery cut: closure digest, actual fenced LAC,
  independently verified final Kafka end and optional inert-suffix start. It does not invent a footer or complete
  persisted index directory; the focused cold-history reader rebuilds its exact locators;
- RETIRED has passed manifest/source-protection/read-pin/retention conditions and no longer serves reads.

BookKeeper journal and entry-log files remain physically shared by many ledgers; logical per-partition chains do not
mean dedicated bookie disks. A global mixed-partition ledger is not the 0.2 default because it destroys partition-local
entry continuity and couples retention/GC. Cold-partition pooled lanes remain a future optimization requiring a new
accepted contract and evidence. They cannot be selected silently when the dedicated-chain scale gate fails.

### Two-level BookKeeper range map

The coarse authority routes a Kafka offset to one run. It stores only low-frequency owner/run/rollover/seal/generation
roots in the Kafka authority/manifest system; normal append and Fetch perform no remote control-metadata operation.

Inside the selected ledger, immutable control entries checkpoint packed RecordBatch locators:

```text
RUN_HEADER
DATA...
RANGE_INDEX_BLOCK
DATA...
RUN_FOOTER
```

DATA members of one append group are physically contiguous. Every DATA/control write passes through the same ledger-
entry sequencer; a control entry may be inserted only at an append-group boundary and never between that group's DATA
members. An index block covers only a greatest contiguous committed/ACK-eligible prefix. Its presence can accelerate
read/recovery but cannot authorize an ACK or make post-gap DATA visible.

One locator covers exactly one complete raw RecordBatch and binds at least:

- its half-open Kafka Offset Range, encoded relative to the block anchor where profitable;
- exact ledger entry ID and, only if later packing is admitted, payload offset/length;
- append-group identity or compact group delta sufficient to recover commit-set membership;
- the required physical/checksum generation.

Kafka coverage comes from the assigned RecordBatch header (`baseOffset` and `lastOffsetDelta`), never from Kafka
record count. A common DATA entry contains one full RecordBatch. Multiple tiny RecordBatches per entry are deferred
until evidence shows BookKeeper entry TPS is the limiting resource.

The final `RUN_FOOTER` authenticates the run identity, logical/physical terminal bounds, ordered index-block directory,
and a SHA-256/v1 root over the sealed index/checksum structure. Exact wire and fanout remain M2 work.

### Append-group durability without per-append metadata

One `KafkaAppendCommitSet` owns one durable append-group descriptor in the BookKeeper data path. A single-batch group
may carry it in that DATA entry; a multi-batch group may carry the terminal descriptor in its last DATA entry or in an
exactly associated control frame selected by the `NBKE2` design. ACK requires:

1. combined completion-tracker and active-tail-locator capacity was reserved before Kafka offsets were assigned;
2. the Kafka native leader/Position Domain assigned one exact contiguous Offset Range;
3. the ledger entry sequencer reserved one contiguous ordered DATA-entry range and submitted every member in entry-ID
   order, even though their BookKeeper futures may overlap;
4. every DATA entry and the complete append-group descriptor reached BookKeeper quorum and verified exact identity;
5. the ordered commit queue reached this group without an earlier gap;
6. the owner-local locator view was installed hidden, then locator, producer/transaction/leader-epoch state and
   Readable/Durable Frontiers passed ADR 0087's exact fenced coherent publication CAS/lock cut;
7. only after legal publication may an optional response-time fence check decide success ACK versus outcome-unknown.

The descriptor binds the complete Kafka range, first/last data-entry identity, member count, owner/storage/Kafka leader
epochs, commit-set/storage-attempt identity, and exact stored-assigned payload digest. Response-unknown storage
recovery accepts only exact identity and bytes. This storage digest never changes native Kafka duplicate handling:
PID/producer-epoch/base-sequence/last-sequence alone drive native duplicate lookup, while non-idempotent retries may
append twice.

No normal append creates a remote metadata reservation or individual mapping record. Index blocks are asynchronous
checkpoints and are not in the ACK cut. The active tail remains readable through the pre-reserved owner-local locator
view required by ADR 0067.

### Offset admission and ordered I/O pipeline

The partition owner separates three local stages:

- an Offset Admission Sequencer under Kafka native partition leadership;
- a bounded BookKeeper write pipeline;
- an ordered commit/publication queue.

Admission order fixes Kafka ranges and ledger-entry ranges. Multiple append groups may have BookKeeper I/O in flight,
but visibility and ACK advance only through the greatest contiguous successful Kafka prefix. If group B becomes durable
before group A, B waits. A definitive A failure or unresolved gap fences the run, prevents B from becoming visible,
recovers the greatest contiguous committed offset, seals or quarantines the old run, and retries eligible unacknowledged
work only under a fresh run/fence. The system never commits around a hole. The ordered publication cut includes native
producer/transaction and Kafka leader-epoch state rather than advancing a storage-only `committedEndOffset`.

Pipeline depth, per-partition/global in-flight bytes, fairness, and early backpressure are Cell/host operational bounds.
They cannot change persisted ordering or recovery semantics.

### Targeted Fetch and integrity domains

For a requested offset, the reader:

1. floor-searches the run manifest/generation root;
2. floor-searches the sealed footer directory or ACTIVE-tail directory;
3. reads or cache-hits one bounded `RANGE_INDEX_BLOCK`;
4. binary-searches the packed locator array, accepts the floor only if it covers the requested offset, and otherwise
   follows the first successor across the block/run boundary;
5. reads the selected DATA entry, or the minimum contiguous entries needed for the Fetch result;
6. validates BookKeeper digest, `NBKE2` entry CRC32C, and Kafka RecordBatch CRC/header coverage.

Normal random Fetch does not read the complete append extent and does not recompute an append/run-wide SHA-256.
Index-block/run roots are used for checkpoint validation, scrub, recovery, materialization, and sealed-run verification.
An in-memory floor cache is disposable acceleration only; the sealed footer/index blocks and bounded ACTIVE-tail scan
remain recovery authority.

### Bounded recovery

Takeover first closes the old Owner's same-key run admission and discovers its complete immutable membership,
including preceding closed Owners. It opens and fences every admitted ledger before replaying any run. Ordered replay
validates exact headers, continuous run boundaries, complete DATA append-group descriptors and Kafka bytes. It adopts
the greatest gap-free legal prefix, including complete groups whose response was lost. No election-adoptable offset,
Follower Applied boundary or maximum physical offset cuts the shared committed prefix.

Range-index coverage alone is insufficient to restore Kafka visibility. A checkpoint must align the range, producer,
transaction and leader-epoch vector. Otherwise recovery rebuilds protocol state from the preceding verified run/prefix.
HW equals the legal shared committed end; LSO also accounts for ongoing transactions. An incomplete/conflicting
terminal group leaves an inert physical suffix and contributes no visible Kafka range.

Takeover preserves each old run's creator Owner and Kafka leader epochs. An ACTIVE crash run is sealed with a
persisted recovery cut only after independent actual native metadata/prefix validation; the fenced old ledger receives
no fabricated footer. A new Owner admits a distinct successor run after the recovered terminal. The current
`KafkaBookKeeperPartitionV1` composes that path with the existing ordered pipeline and targeted reader, retaining exact
old-run indexes and one coherent current commit snapshot. A later takeover rechecks each persisted recovery cut
against its original closed Owner digest and freshly fenced native LAC, and discovers checkpoint/footer metadata
within the accepted prefix before any inert suffix. The cut does not hide a checkpoint or existing footer from
selection. DATA discovery probes contribute entry/byte debt only for the selected unchecked tail; all history
remains fenced and elapsed-time bounded.

Unchecked recovery debt is bounded simultaneously by entry count, encoded bytes, and recovery time. The current
BK-only native slice persists an aligned producer/transaction/leader/source checkpoint at same-Owner rollover.
Checkpoint, index, exact close/seal, read reopen, successor attachment and publication must all complete before
covered entry/byte debt is released; failures stop new offset admission. Cold recovery fences every legal unretired
ledger, then uses normal footer directories or protected checkpoint locators and replays only the uncovered tail.
Discovery/fencing still consumes the time bound and Owner/run metadata remains bounded at 1,024. Native create
qualification retains one current-run scope and fences its predecessor before replacement; there is no fixed
256-Owner preallocation or native epoch limit. Complete M4/M5 retirement remains outside this slice. Candidate values such as 1,024
RecordBatches or 1 MiB per checkpoint, 16--64 KiB blocks, 4,096--16,384 active locators, 8--32 in-flight groups, and a
4--32 MiB recovery tail are benchmark inputs, not frozen format/default values.

### Materialization and source switching

For `BOOKKEEPER_WAL_ASYNC_OBJECT`, materialization may combine one or more SEALED runs into an Object extent while
preserving exact Kafka coverage. A new generation may select Object as preferred and the old BookKeeper runs as
protected fallback. BookKeeper deletion still requires complete Object coverage/integrity, durable generation
publication, reader-pin/source-protection drain, logical retention, and exact deletion proof. Kafka offsets and consumer
group offsets never change. Producer, transaction/aborted, and leader-epoch side indexes must describe the same
compatible logical cut; materialization never advances Kafka LEO, HW, or LSO.

## Consequences and tradeoffs

- Gain: no remote metadata RTT/CAS/watch/GC per Produce; one indexed entry read serves random Fetch; BookKeeper I/O can
  pipeline without weakening ordered Kafka visibility; takeover scans a bounded tail.
- Cost: `NBKE2`, packed index blocks, active-tail locators, ordered completion, footer/root validation, and explicit
  control-entry parsing add implementation and recovery complexity.
- Cost: one active ledger per hot partition consumes handles and BookKeeper metadata. M2 must prove 10k/100k viability;
  failure blocks the profile or requires a new pooled-lane ADR, not an implicit layout substitution.
- Tradeoff: asynchronous index checkpoints move work off ACK but require bounded owner memory and tail scan.
- Tradeoff: entry-local verification makes random reads cheap; full-run SHA verification moves to scrub,
  materialization, seal, and disaster recovery rather than every Fetch.

## Evidence and implementation boundary

M1/K1 owns Kafka KRaft Topic/Aggregate authority and does not implement this data layout. M2 owns the code-level
`NBKE2`, run/index/footer, targeted reader, ordered pipeline, recovery harness, and scale evidence. Exact implementation
is split between the
[M2-K0 input closure](../v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) and the
[BookKeeper run/index design](../v2/detailed_design/m2/kafka-bookkeeper-offset-range-index.md). K0's planned
`v2M2KafkaInputsCheck` is non-promotable and must include production wire/cap/provider/module surfaces plus non-zero
tests; K9 selects operational defaults from real fault/scale evidence without rewriting persisted v1 bytes.

Required evidence covers at least:

- zero normal-append and normal-Fetch remote control-metadata I/O;
- exact RecordBatch-header coverage, multi-batch commit-set atomicity, and control-entry exclusion;
- B-before-A completion, definitive failure, unknown response, owner takeover, gap, duplicate, and digest conflict;
- targeted entry-read amplification and all checksum-layer corruptions;
- checkpoint loss/corruption and bounded tail recovery by entries/bytes/time;
- async Object generation switch with stable Kafka/group offsets and safe BookKeeper fallback/deletion;
- 10k/100k partition handle/memory/metadata/rollover/recovery evidence.

This ADR refines ADRs 0011, 0031, 0066, and 0067 and is further refined by ADR 0087. It resolves the Kafka ledger-layout
semantic choice; `V2-OPEN-BK-02` now tracks executable M2 production inputs and evidence rather than an open
architecture choice.
