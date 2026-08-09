---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeQuestionLog
sourceTuple: v2-m0
---

# V2 open questions

This file records proposals that have not been accepted as runtime contracts and retains resolved gate IDs for
traceability. An answer moves into a normative document or ADR only after explicit confirmation; editing this file
alone cannot close a gate.

## Restarted Grill 2: current frontier

Round 11 accepted the permanent class/lane catalog and complete leaf grammar, provider-resolved checkpoint eligibility
with one publisher-epoch-fenced combiner, and the separation between physical lane resolution and owner-local binding
frontiers in ADRs 0062..0064. It introduced no per-binding remote metadata, persistent runtime gap map, or hot-path
Owner Epoch read. Evidence-blocked numeric/class/mode decisions wait; the next independent frontier is:

| Gate | Decision needed now | Current recommendation, not a decision |
| --- | --- | --- |
| `V2-OPEN-OBJ-20` | choose the minimal physical checkpoint/Seal descriptor and whether any copied binding summary belongs there | consider physical-only extent identity with optional qualified provider proof; rebuild bindings from authenticated directories and store no frontier/ACK state |
| `V2-OPEN-OBJ-21` | identify the O(1) normal completion ring without adding a durable append ordinal | consider an owner-local checked completion ticket plus exact coverage/predecessor validation, discarded on takeover |
| `V2-OPEN-READ-01` | make independently ACKed Object-WAL coverage readable before checkpoint/manifest publication | consider a lazy owner-local active-tail locator index installed before contiguous Durable/Readable frontier release |

The complete questions and recommendations are in
[round 12](grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md). None of its
recommendations is accepted yet. `V2-OPEN-BK-11/13`, remaining `V2-OPEN-OBJ-17/19`, and
`V2-OPEN-PUL-OBJ-09` wire/size/mode work are blocked on accepted evidence protocols rather than questions in this round.

## Configuration scope

Correctness/recovery/compatibility and parser hard caps are non-configurable; Topic/Tenant-or-Namespace typed intent,
Protocol Cell/shard budgets, and host/process ceilings resolve by minimum. Durable choices persist at their exact
epoch/run/group/attempt boundary, and one enum cannot span Storage Epoch, Object group, offload attempt, and host
lifecycles. Product/Deployment owns the base semantic default; Protocol Cell and host cannot replace it. Resolved by
[ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md).

## Initial binding and epoch publication

### `V2-OPEN-META-01`: resolved atomic visible create

Resolved by [ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md). Topic Protocol Binding and its
initial Storage Epoch form one visible `TopicBindingAggregate`. Incomplete or uncertain create is recovered or rejected;
it never admits open, append, or read and never causes a default epoch to be invented.

### `V2-OPEN-META-02`: resolved aggregate physical representation

Resolved by [ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md). One immutable
`TopicBindingAggregateRecord` physically contains the complete binding and initial epoch. Kafka adds it to atomic
`CreateTopics`; MetadataStore/Oxia creates one key and resolves response loss by exact reread equality. Logical stores
are typed projections, and 0.2 does not use a cross-key `CREATING` saga.

### `V2-OPEN-META-03`: resolved aggregate incarnation, key, and deterministic IDs

Resolved by [ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md). Kafka native topic UUID or
Pulsar canonical persistence/name facts plus binding generation form a protocol-discriminated incarnation. Aggregate
keys are incarnation-scoped, values repeat the complete identity, and binding/initial-epoch IDs are separate
domain-separated deterministic SHA-256 derivations with no retry-dependent input.

### `V2-OPEN-META-04`: resolved aggregate logical schema v1

Resolved by [ADR 0033](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md). One closed whole-record
logical schema v1 owns the complete immutable binding plus ordinal-zero epoch, excludes mutable/retry-dependent fields,
and maps both Kafka and Oxia physical records through one validator and shared semantic vectors.

### `V2-OPEN-KAF-META-01`: resolved Kafka V2 feature activation

Resolved by [ADR 0034](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md). V2 uses
`nereus.storage.version=2` only at fresh KRaft format/bootstrap, advertises `[2,2]`, permanently rejects level-1 V1
state, and forbids every runtime upgrade/downgrade.

### `V2-OPEN-KAF-META-02`: resolved Kafka aggregate record and image ownership

Resolved by [ADR 0042](../decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md). One generated
typed wire-v0 record belongs to `TopicImage`, completed snapshots order it between topic and partitions, topic removal
cascades, and every published complete image requires exactly one valid aggregate per live Nereus topic.

### `V2-OPEN-PUL-META-01`: resolved Pulsar aggregate retirement and recreation ABA

Resolved by [ADR 0043](../decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md). A permanent
name-scoped selector retains monotonic generation and durable deletion; only exact reference-free proof may replace a
full aggregate with a compact permanent same-key tombstone. Neither key nor generation is reused.

### `V2-OPEN-KAF-META-03`: resolved Kafka aggregate generated wire and validation hook

Resolved by [ADR 0050](../decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md). Kafka reserves
`32000..32767`, uses API key 32000 strict non-flexible wire v0, validates only touched topics at ordinary actual image
publication, and scans all live topics only for snapshot/bootstrap. The correctness check cannot be disabled.

### `V2-OPEN-PUL-META-02`: resolved Pulsar selector and aggregate CAS state machine

Resolved by [ADR 0051](../decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md). Exact
`RESERVED -> ACTIVE -> DELETING -> DELETED` CAS transitions recover separate keys; open/ownership/version change
validates ACTIVE plus aggregate identity and installs a local versioned fence, so normal append/read has no Oxia call.

## Object WAL durability verification

### `V2-OPEN-OBJ-01`: per-binding frontier and cross-binding head-of-line isolation

Resolved by [ADR 0064](../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md). Physical
`LaneExtentResolvedThrough` is separate from each Position Domain's `BindingDurableFrontier`; an owner-local lazy
ring/window with bounded sparse-recovery fallback advances only one binding, stores no payload/persisted gap state, and
performs cached O(1) owner fencing. Shared Object/header/directory failures block all members, while later
frame/commit-set-local failures isolate to that complete binding unit.

### `V2-OPEN-OBJ-02`: resolved PUT-response-loss proof

Resolved by [ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md). When an immutable Object WAL PUT may have
succeeded but the response was lost:

1. use HEAD only when it returns exact length plus a trustworthy content checksum bound to the immutable object
   identity/version;
2. otherwise perform a bounded GET and recompute the expected checksum;
3. never treat ETag alone as content identity;
4. do not admit a provider to `OBJECT_WAL` when deterministic immutable create, the required read-after-write
   behavior, or bounded verification cannot be established.

This closes the design choice only. M3 still needs real-provider response-loss and checksum-drift evidence.

### `V2-OPEN-OBJ-04`: resolved checksum byte domains

Resolved by [ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md). `ObjectExtentDigest` protects the exact
canonical provider request body, while `FramePayloadChecksum` protects the binding-defined protocol payload bytes after
Object decode. The fields and proof domains are distinct and cannot substitute for each other.

### `V2-OPEN-OBJ-05`: resolved initial algorithms and provider-bound proof

Resolved by [ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md).
`ObjectExtentDigest` is SHA-256/v1; `FramePayloadChecksum` is CRC32C/v1. Expected extent identity remains outside the
body, and typed `ProviderObjectProof` must match version, length, SHA-256, and `FULL_OBJECT` scope or recovery performs a
bounded full GET. ETag, user metadata, and composite checksums do not qualify.

### `V2-OPEN-OBJ-06`: resolved canonical protocol frame bytes

Resolved by [ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md). Frame CRC32C covers exact assigned
Kafka `MemoryRecords`/batch bytes or exact Pulsar ManagedLedger entry bytes after only the outer Object envelope is
decoded. Application records/messages are not reserialized, and native protocol checksums remain independent.

### `V2-OPEN-OBJ-07`: resolved Object WAL group identity and crash discovery

Resolved by [ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md). One immutable root
is persisted before opening a run; ADRs 0059/0062 refine every group key to the final one-digit lane, fixed-width
sequence/prefix-end/body-length, and full SHA-256 grammar. Bounded strong same-prefix LIST discovers a
provider-resolved open tail without per-group metadata publication. Async checkpoint pages are accelerators only, and
non-qualifying providers are rejected for `OBJECT_WAL`.

### `V2-OPEN-OBJ-08`: resolved protocol frame and append commit-set granularity

Resolved by [ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md). One assigned Kafka RecordBatch is a
frame and every frame from one partition storage append is one all-or-none commit set. One Pulsar ManagedLedger entry is
one frame/commit set. Object groups, requests, transactions, and individual batched messages do not redefine append
atomicity.

### `V2-OPEN-OBJ-09`: resolved multi-binding WalRun epoch placement

Resolved by [ADR 0037](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md). The root remains
physical/run authority, while bounded object-local binding contexts carry each frame's exact incarnation, Storage
Epoch, and Owner Epoch, preserving cross-binding PUT amortization.

### `V2-OPEN-OBJ-10`: resolved provider-absent in-flight group after process loss

Resolved by [ADR 0038](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md). 0.2 has no broker-local
ciphertext journal claim. A present object is verified; a proven-absent never-ACKed gap fences the old run and may retry
only through protocol idempotency in a fresh run; unknown presence remains fail-closed.

### `V2-OPEN-OBJ-11`: resolved bounded WalRun lifecycle

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). Every run has hard
extent/byte/age/predecessor limits and stops admission, drains/reconciles, seals, and publishes a successor before a
limit can be crossed.

### `V2-OPEN-OBJ-12`: resolved recovery envelope as an admission invariant

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). One cumulative
worst-case envelope constrains normal ACK/admission across provider, decode, memory, retry, and time work. Fallback never
resets it; predicted exhaustion backpressures and actual exhaustion fails closed.

### `V2-OPEN-OBJ-13`: resolved current WalRun Root discovery authority

Resolved by [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md). One low-frequency
per-shard CAS pointer binds exact root key/SHA/run epoch and anchors a bounded predecessor lineage; normal admitted
group append remains free of metadata I/O.

### `V2-OPEN-OBJ-14`: resolved in-object append-unit directory and co-location

Resolved by [ADR 0040](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md). NWG1 carries one bounded
authoritative in-body binding-context/append-unit directory, co-locates every Kafka commit set in one ObjectExtent, and
independently compresses/authenticates/checks each frame block.

### `V2-OPEN-OBJ-15`: resolved NWG1 key hierarchy, AEAD, and authenticated directory

Resolved by [ADR 0046](../decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md). NWG1 mandates
AES-256-GCM/HKDF-SHA-256 v1, wraps one random run key under the immutable Cell KMS version, derives unique per-Object
keys, and uses disjoint authenticated directory/frame nonce domains with rotation only at rollover.

### `V2-OPEN-OBJ-16`: resolved WalRun Root home and immutable seal publication

Resolved by [ADR 0047](../decisions/0047-v2-walrun-root-seal-and-successor-publication.md). Immutable Root and Seal
records live in Cell control metadata; a successor binds both and one exact pointer CAS advances only after publication.
A sealed run is never reopened.

### `V2-OPEN-OBJ-17`: exact NWG1 cryptographic framing

ADR 0058 makes `maxHeaderAndDirectoryPrefixBytes` primary, derives frame capacity after row widths, benchmarks
4,096/16,384 first, and rejects pagination/secondary authority. ADR 0059 places an exclusive bounded
`directoryPrefixEnd19` in every leaf, keeps it outside content digest identity, uses structured descriptors instead of
full-key repetition, and fixes short/long incremental range reuse. Routine reads need neither ProviderObjectProof nor
HEAD. ADR 0062 fixes the one-digit `0..2` lane token and complete key grammar. The remaining gate must freeze only exact
prefix/header/directory/row numeric caps; exact key/Root/version/AEAD mismatch still fails or uses bounded fallback.

### `V2-OPEN-OBJ-18`: resolved WalRun checkpoint pages and open-tail handoff

Resolved by [ADR 0053](../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md). Pages publish
asynchronously at Protocol Cell x shard scope; finite extent/byte/age limits remain mandatory even if proactive cadence
is disabled, open uncovered tails always use bounded strong LIST, and the sealed final gap-free inventory is mandatory.

### `V2-OPEN-OBJ-19`: NWG1 typed operational policy classes

Encoding and packing remain separate. ADR 0060 fixes one Root/pointer with at most three lazily instantiated lanes,
lane-local extent-resolution barriers, binding-move drain, lane-aware key/HKDF/nonce/header identity, aggregate hard
budgets, and one run-wide checkpoint/Seal vector chain. Three lane-local chains and eager target-sized allocation are
rejected.
ADR 0062 permanently maps `0/1/2` to `OBJECT_LATENCY/BALANCED/COST` and ADR 0063 fixes the one-combiner protocol.
The remaining gate is evidence-selected target/linger/quantized values and numeric budgets; former 4/16/64-MiB and
5/20/50-ms values remain candidates only and cannot change class meanings.

### `V2-OPEN-OBJ-20`: physical checkpoint and Seal descriptor payload

This remains open. ADR 0063 fixes provider-resolved eligibility and publisher fencing, while ADR 0064 forbids a copied
binding frontier from becoming physical checkpoint authority. The remaining gate chooses whether pages/Seal stay
physical-only and pay bounded directory prefix GETs during recovery, or carry any optional non-authoritative binding
summary. Round 12 asks the current recommendation.

### `V2-OPEN-OBJ-21`: owner-local completion ticket and normal-path ring

This remains open. ADR 0064 accepts an O(1) normal ring and Position-Domain-aware recovery fallback but no durable
append ordinal exists. The remaining gate must freeze a purely owner-local indexing ticket, exact coverage/predecessor
validation, live-slot/wrap behavior, and takeover discard without creating another wire/metadata ordering domain.

### `V2-OPEN-READ-01`: Object-WAL active-tail readability before checkpoint/manifest

This remains open. A binding can ACK only after its coverage is both durable and readable, but checkpoint is now
physical inventory and manifest materialization is asynchronous. The remaining gate must freeze the derived owner-local
active-tail locator installation/rebuild/retirement cut that makes B readable without per-group remote metadata. Round
12 asks the current recommendation.

## Storage Epoch transitions

### `V2-OPEN-MIGRATION-01`: resolved 0.2 transition scope

Which profile transitions are implemented in 0.2, and which remain domain-model capability only?

Earlier transition ordering proposal, retained as input rather than a decision:

1. Pulsar `BOOKKEEPER_WAL_ONLY` ↔ `BOOKKEEPER_WAL_ASYNC_OBJECT` is easiest because BookKeeper remains primary.
2. Kafka `OBJECT_WAL` ↔ a BookKeeper profile can cut at a Kafka Offset frontier.
3. Pulsar BookKeeper ↔ Object WAL is substantially harder because native ManagedLedger ledger-chain semantics change.

Resolved by [ADR 0015](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md): 0.2 persists the Storage Epoch chain model
and enforces typed-cut and single-admitting-epoch invariants, but exposes no online transition API/state machine. The
runtime creates one initial epoch per Topic Incarnation; later releases may activate transitions only after accepting
their own contracts.

### `V2-OPEN-MIGRATION-02`: transition state machine

Deferred beyond the 0.2 runtime. The historical proposed states are retained as future design input:

```text
ACTIVE_OLD
PREPARING_NEW_EPOCH
DRAINING_OLD_WRITER
OLD_EPOCH_SEALED
NEW_EPOCH_ACTIVE
MATERIALIZING_HISTORY
RETIRING_OLD_PHYSICAL
COMPLETED
```

A future transition feature still needs exact authority, retry, cancellation, response-loss, crash-cut, rollback, and
operator-visible semantics. This question does not block 0.2.

### `V2-OPEN-MIGRATION-03`: historical data movement

Deferred beyond the 0.2 runtime. Must a future profile transition backfill old Protocol Coverage into the new physical
profile, or may the reader retain a permanent multi-epoch history? If backfill is optional, which cost/latency policies
trigger it and when may the old Physical Extent be retired?

## Pulsar BookKeeper/Object evolution

### `V2-OPEN-BK-01`: resolved Pulsar async Object authority

Resolved by [ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md): native ManagedLedger
ledger/offload metadata is the sole authority for attempts/completion, read/fallback, and BookKeeper deletion
eligibility. Nereus implements its Object format through a `LedgerOffloader`; a Nereus manifest is derived and cannot
independently authorize native ledger deletion.

The local Pulsar checkout already records an attempt UUID before calling the offloader, marks completion afterward,
opens offloaded reads from ledger metadata, and consults the offload context before BookKeeper deletion. Reusing that
state machine best preserves the “not weaker than native Pulsar” requirement.

### `V2-OPEN-BK-03`: resolved sealed-ledger execution

Resolved by [ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md). 0.2 offloads sealed, non-current
ManagedLedger ledgers through the ledger-based offloader and excludes active-ledger streaming. Rollover and lag
admission bound cold-copy delay without adding Object latency to BookKeeper ACK.

### `V2-OPEN-BK-04`: resolved sealed-ledger Object layout

Resolved by [ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md). One native attempt uses exactly one
bounded immutable data Object plus one deterministic sparse-index/root Object. Data publishes before root; offload
success proves both objects, `0..LAC` coverage, integrity, and a ledger-equivalent `ReadHandle`. Both cleanup keys remain
derivable when the root is absent.

### `V2-OPEN-BK-05`: resolved sealed-ledger keys, root v1, and lifecycle order

Resolved by [ADR 0029](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md). Persisted attempt scope and key
version derive both conditional-create keys. A bounded root binds attempt/sealed metadata, data SHA/format, contiguous
index, and self-digest. Publication verifies data, root, and the real read path; cleanup proves root then data absent and
covers attempt-scoped multipart residue.

### `V2-OPEN-BK-06`: resolved sealed-ledger root v1 wire and parser limits

Resolved by [ADR 0035](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md). NPO1 is an independent bounded
big-endian four-section canonical root with strict ordering/UTF-8/duplicate/overflow rules, hard parser limits, and a
root SHA validated before index trust.

### `V2-OPEN-BK-07`: resolved Object revalidation before BookKeeper source deletion

Resolved by [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md). ManagedLedger
revalidates the exact root/data/read path without holding its metadata mutex, then CAS-rechecks the same eligible attempt
before `bookkeeperDeleted=true`; failure retains BookKeeper and permanent mismatch quarantines Object.

### `V2-OPEN-BK-08`: resolved native Object/BookKeeper read fallback

Resolved by [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md). Native metadata
permits at most one whole-range, single-source fallback while both sources remain eligible; Object corruption remains a
deletion veto, and `bookkeeperDeleted=true` is permanently Object-only.

### `V2-OPEN-BK-09`: resolved sealed-ledger NPD1 data-block contract

Resolved by [ADR 0044](../decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md). NPO1 indexes ordered, gap-free,
independently verifiable NPD1 multi-entry blocks with bounded directories, no split entries or cross-block state, and
dedicated bounded oversize blocks.

### `V2-OPEN-BK-10`: resolved ManagedLedger dual-source handle and read pins

Resolved by [ADR 0045](../decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md). ManagedLedger owns one cached
composite handle with lazy children and exact source pins; deletion fences/drains BK pins before final Object
revalidation and native CAS, and close drains both sources.

### `V2-OPEN-BK-11`: NPD1 block wire, limits, codec, and crypto

ADR 0056 resolves the checked decoded/directory/compressed/AEAD/encoded/Object formulas, 32-byte NPD1 and 64-byte NPB1
headers, 16-byte derived-entry-ID row, actual-count allocation, streaming processing, and required provider-capability
categories. The remaining gate must select exact block/Object/adapter numeric maxima, lower derived admission, and
provider evidence. Four GiB and 1,024 parts remain candidates only; multipart count is never NPD1 wire identity.

### `V2-OPEN-BK-12`: resolved persisted BookKeeper physical-delete intent and fact

Resolved by [ADR 0052](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md). Persisted
`RETAIN_BK` or `DELETE_AFTER_VERIFIED` policy controls entry into irreversible
`BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`; the compatibility boolean is only a read fence and retirement,
audit, and physical capacity require the three-state fact.

### `V2-OPEN-BK-13`: NPD1 typed block policy and default evidence

ADR 0057 accepts the 1/4/8/16-MiB native-relative evidence candidates without wire enums and a later maximum of three
classes. Product/Deployment owns the base default, Namespace inherits/overrides it, Topic is an explicit typed
override, Protocol Cell performs admission/budgets only, and host is a ceiling. The resolved class is persisted in the
offload attempt. The remaining gate is evidence execution plus exact class/default selection.

## Pulsar Object WAL

### `V2-OPEN-PUL-OBJ-01`: resolved virtual ledger identity and chain authority

Resolved by [ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md). A Pulsar-cell
`PulsarVirtualLedgerStore` owns virtual ledger allocation and an explicit append-only Ledger Chain. Object identity,
byte offsets, and Object-run sequence never become Pulsar positions or chain authority.

### `V2-OPEN-PUL-OBJ-02`: resolved numeric compatibility and namespace enforcement

Resolved by [ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md). The deployment reserves
`[2^62, 2^63 - 2]`, excludes native allocation, and assigns non-overlapping never-reused cell slices. Cell allocators are
increasing with gaps and no reuse. Numeric order preserves stock comparison only; explicit predecessor/head metadata
remains Ledger Chain authority.

### `V2-OPEN-PUL-OBJ-03`: resolved deployment reservation registry authority

Resolved by [ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md). One bounded deployment-wide
registry is slice-allocation authority; its canonical assignment table advances through single-key CAS. Per-cell lookup
and watches are derived. Exact capacity, slice lifecycle, allocator, and Ledger Chain protocols remain descendants.

### `V2-OPEN-PUL-OBJ-04`: resolved durable slice owner identity

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). The durable owner is the
deployment/reservation-domain/Pulsar Protocol Cell tuple; broker/session/alias/provider change cannot consume or mutate
the assignment.

### `V2-OPEN-PUL-OBJ-05`: resolved slice lifecycle and retirement

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). Lifecycle is irreversible
`ACTIVE -> RETIRING -> RETIRED`; only ACTIVE allocates, RETIRED remains a permanent tombstone, and exhaustion is derived
rather than a lifecycle state.

### `V2-OPEN-PUL-OBJ-06`: resolved slice geometry and registry lifetime capacity

Resolved by [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md). Every Cell gets one immutable
equal-size aligned `2^k` slice, while numeric and encoded/lifetime registry caps both include retired Cells. The then-
downstream expansion and exact bootstrap geometry are now resolved by ADRs 0048 and 0054.

### `V2-OPEN-PUL-OBJ-07`: resolved virtual-ledger slice expansion policy

Resolved by [ADR 0048](../decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md). 0.2 forbids resize,
relocation, extension, and another slice; exhaustion fails before allocation, and new capacity requires a new Cell plus
a future explicit migration contract for existing topics or ledgers.

### `V2-OPEN-PUL-OBJ-08`: resolved virtual-ledger exponent and registry lifetime caps

Resolved by [ADR 0054](../decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md). Bootstrap fixes `k=40`,
64 KiB, 256 lifetime rows, and 192 bytes/row. A new logical reservation-domain label cannot reuse the interval; a new
domain must prove a disjoint ledger-ID namespace or use an independent deployment/cluster.

### `V2-OPEN-PUL-OBJ-09`: virtual-ledger allocator reservation and head publication

This remains open. STRICT_SERIALIZED has four successful writes and still requires ADR 0055 evidence. ADR 0061 now
constrains any RANGE candidate: the grant belongs to ManagedLedger incarnation, takeover changes only owner epoch, the
new owner may finish the same RESERVED grant when exact allocation state is unchanged, an unknown response converges by
exact reread, and at most one stale candidate burns. Installed-range use does not wait for allocator clear, but the
next Cell grant does and therefore requires a high-priority reconciler. Permanent orphan evidence is bounded/admitted.
The remaining gate must freeze exact reservation/head/node wire and range size, prove Cell grant concurrency and every
failure cut, execute the evidence protocol, and then select at most one persisted mode.

### `V2-OPEN-PUL-OBJ-10`: allocator target-scale evidence protocol

Resolved by [ADR 0055](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md). Evidence measures the
maximum sustainable rollover RPS while all predeclared latency/queue/error/recovery SLOs hold, includes actual
rollover-rate distribution/jitter/storms and native Pulsar rollover/append-stall baseline, and keeps performance budgets
out of allocator durable identity. Execution remains `PLANNED`; this resolution is not a performance PASS.

### `V2-OPEN-PUL-MIGRATION-01`: new incarnation or HybridManagedLedger

The initial proposal is to migrate between Pulsar BookKeeper and Object WAL through a new Topic Incarnation, backfill,
catch-up, and alias/routing cutover. A later alternative is a `HybridManagedLedger` whose Ledger Chain contains both
Object virtual ledgers and BookKeeper ledgers.

The choice is not accepted. It must account for cursor and MessageId stability, partial batch ACK, retention, offload,
recovery, compaction, replication, transactions, and rollback.

## Cross-protocol access and migration

### `V2-OPEN-PROJECTION-SCOPE-01`: resolved 0.2 runtime scope

Resolved by [ADR 0016](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md): 0.2 retains the domain identities,
invariants, and rejection of a second Native Write Authority, but does not implement Projection Map storage,
secondary-protocol serving, semantic state translation, or authority-transfer runtime.

### `V2-OPEN-PROJECTION-01`: Projection Map granularity

Deferred beyond the 0.2 runtime. Should future Projection Map entries be segment-level coverage mappings, ledger-level
mappings, batch mappings, or a hybrid? The proposal avoids one control-metadata mutation per message, but random seek,
partial batch ACK, and corruption repair must remain bounded.

### `V2-OPEN-PROJECTION-02`: Migration Link state machine

Deferred beyond the 0.2 runtime. The historical proposed Kafka/Pulsar authority-transfer saga is:

```text
SOURCE_ACTIVE
TARGET_PREPARED
BACKFILLING
TAIL_CATCHING_UP
TARGET_CAUGHT_UP
SOURCE_FENCED
TARGET_ACTIVATED
SOURCE_RETIRED
```

Failure and rollback semantics at every cut remain undecided. In particular, no state may permit both source and target
to allocate native positions.

### `V2-OPEN-PROJECTION-03`: semantic transfer contract

A future runtime must decide how to translate:

- Kafka consumer groups and Pulsar subscription cursors;
- Pulsar batch indexes and Kafka record offsets;
- partial batch ACK;
- transactions and visibility;
- compaction tombstones;
- delayed delivery;
- Key_Shared routing;
- schemas, Pulsar properties, and Kafka headers;
- producer deduplication state.

For example, one Pulsar entry with batch indexes `0..2` might map to one Kafka Offset Range of length three. This is an
input example, not an accepted canonical payload mapping.

## Resolved questions

### Restarted Grill 2 round 11 adjusted decisions: resolved by ADRs 0062 through 0064

Resolved on 2026-08-09 after explicit adjusted confirmation:

- Q1 permanent `0/1/2 = OBJECT_LATENCY/BALANCED/COST`, complete leaf grammar, policy-version compatibility, and
  post-plan/pre-HKDF sequence allocation ->
  [ADR 0062](../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md);
- Q2 one publisher-epoch-fenced combiner/candidate, exact takeover/CAS, bounded residue, and provider-resolved
  checkpoint eligibility -> [ADR 0063](../decisions/0063-v2-provider-resolved-checkpoint-publisher.md);
- Q3 physical `LaneExtentResolvedThrough` versus logical `BindingDurableFrontier`, owner-local reconstructible
  ring/window, bounded sparse fallback, early buffer release, and layered failure isolation ->
  [ADR 0064](../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md).

The confirmed “canonical body seal before sequence” wording is clarified by the already accepted cryptographic
dependency: immutable group membership/policy plan seals first, sequence then feeds HKDF/nonce, and final ciphertext
body seals afterward. Exact numeric values remain evidence-blocked. The complete response is preserved in
[the round 11 record](grill-notes/13-restarted-grill-2-lane-binding-checkpoint-publisher-and-frontiers.md).

### Restarted Grill 2 round 10 adjusted decisions: partially resolved by ADRs 0059 through 0061

Resolved on 2026-08-09 after explicit partial/adjusted confirmation:

- Q1 exclusive leaf prefix hint, structured descriptors, incremental reuse, and leakage tradeoff ->
  [ADR 0059](../decisions/0059-v2-object-wal-leaf-prefix-hint.md);
- Q2 at most three lazy lanes, lane-local sequences/ACK barriers, aggregate budgets, and one vector chain ->
  [ADR 0060](../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md);
- Q3 incarnation-owned RANGE grant, owner-only takeover, RESERVED continuation, one-candidate burn, background clear,
  and permanent-orphan accounting ->
  [ADR 0061](../decisions/0061-v2-pulsar-range-grant-owner-takeover.md).

Three lane-local checkpoint chains are rejected. Exact numeric/class values, canonical lane/key wire, final RANGE wire /
size/evidence, and both allocator modes remain open. The complete response is preserved in
[the round 10 record](grill-notes/12-restarted-grill-2-hints-lanes-and-range-takeover.md).

### Restarted Grill 2 round 9 adjusted decisions: partially resolved by ADRs 0056 through 0058

Resolved on 2026-08-09 after explicit partial/adjusted confirmation:

- Q1 checked length domains, derived-ID row, streaming processing, and provider-capability categories ->
  [ADR 0056](../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md);
- Q2 candidate evidence plus Deployment/Namespace/Topic default authority ->
  [ADR 0057](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md);
- Q4 directory-prefix-first frame-cap derivation and evidence priority ->
  [ADR 0058](../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md).

Q1 numeric values / `V2-OPEN-BK-11`, Q2 evidence-selected class values / `V2-OPEN-BK-13`, Q3 /
`V2-OPEN-OBJ-17`, Q5 / `V2-OPEN-OBJ-19`, Q6 / `V2-OPEN-PUL-OBJ-09`, and both allocator modes remain open. The complete
response is preserved in
[the round 9 record](grill-notes/11-restarted-grill-2-read-amplification-and-range-allocation.md).

### Restarted Grill 2 round 8 adjusted decision: resolved by ADR 0055

Resolved on 2026-08-09 after explicit partial/adjusted confirmation:

- cross-lifecycle policy scope further refined
  [ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md);
- Q5 / `V2-OPEN-PUL-OBJ-10` evidence-protocol decision →
  [ADR 0055](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md).

Q1 / `V2-OPEN-BK-11`, Q2 / `V2-OPEN-BK-13`, Q3 / `V2-OPEN-OBJ-17`, Q4 / `V2-OPEN-OBJ-19`, and allocator-mode
`V2-OPEN-PUL-OBJ-09` remain open. No Round-8 numeric cap, class set, combined policy, absolute allocator threshold, or
allocator mode was promoted. The complete response is preserved in
[the round 8 record](grill-notes/10-restarted-grill-2-hard-caps-policy-classes-and-allocator-evidence.md).

### Restarted Grill 2 round 7 adjusted decisions: resolved by ADRs 0049 through 0054

Resolved on 2026-08-09 after explicit partial/adjusted confirmation:

- cross-cutting configuration scope →
  [ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md);
- Q1 / `V2-OPEN-KAF-META-03` →
  [ADR 0050](../decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md);
- Q2 / `V2-OPEN-PUL-META-02` →
  [ADR 0051](../decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md);
- Q4 / `V2-OPEN-BK-12` →
  [ADR 0052](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md);
- Q6 / `V2-OPEN-OBJ-18` →
  [ADR 0053](../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md);
- Q7 / `V2-OPEN-PUL-OBJ-08` →
  [ADR 0054](../decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md).

Q3 / `V2-OPEN-BK-11`, Q5 / `V2-OPEN-OBJ-17`, and Q8 / `V2-OPEN-PUL-OBJ-09` remain open with the user's constraints;
they were not promoted by repetition. The exact response is preserved in
[the round 7 record](grill-notes/09-restarted-grill-2-wire-state-machines-and-checkpoints.md).

### Restarted Grill 2 round 5 decisions: resolved by ADRs 0033 through 0041

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-04` → [ADR 0033](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md);
- `V2-OPEN-KAF-META-01` → [ADR 0034](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md);
- `V2-OPEN-BK-06` → [ADR 0035](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md);
- `V2-OPEN-BK-07..08` →
  [ADR 0036](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md);
- `V2-OPEN-OBJ-09` → [ADR 0037](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md);
- `V2-OPEN-OBJ-10` → [ADR 0038](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md);
- `V2-OPEN-OBJ-11..13` →
  [ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md);
- `V2-OPEN-OBJ-14` → [ADR 0040](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md);
- `V2-OPEN-PUL-OBJ-04..06` →
  [ADR 0041](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md).

Their original recommendations and source rationale remain in
[the round 5 record](grill-notes/07-restarted-grill-2-wire-recovery-and-slice-contracts.md).

### Restarted Grill 2 round 4 decisions: resolved by ADRs 0028 through 0032

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-03` → [ADR 0028](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md);
- `V2-OPEN-BK-05` → [ADR 0029](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md);
- `V2-OPEN-OBJ-07` → [ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md);
- `V2-OPEN-OBJ-08` → [ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md);
- `V2-OPEN-PUL-OBJ-03` → [ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md).

Their original recommendations and source rationale remain in
[the round 4 record](grill-notes/06-restarted-grill-2-schema-discovery-and-registry.md).

### Restarted Grill 2 round 3 decisions: resolved by ADRs 0023 through 0027

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-02` → [ADR 0023](../decisions/0023-v2-topic-binding-aggregate-record.md);
- `V2-OPEN-BK-04` → [ADR 0024](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md);
- `V2-OPEN-OBJ-05` → [ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md);
- `V2-OPEN-OBJ-06` → [ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md);
- `V2-OPEN-PUL-OBJ-02` → [ADR 0027](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md).

Their original recommendations and source rationale remain in
[the round 3 record](grill-notes/05-restarted-grill-2-physical-proof-and-native-ordering.md).

### Restarted Grill 2 round 2 decisions: resolved by ADRs 0019 through 0022

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-META-01` → [ADR 0019](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md);
- `V2-OPEN-BK-03` → [ADR 0020](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md);
- `V2-OPEN-OBJ-04` → [ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md);
- `V2-OPEN-PUL-OBJ-01` → [ADR 0022](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md).

Their original recommendations and source rationale remain in
[the round 2 record](grill-notes/04-restarted-grill-2-initial-authority-and-object-identity.md).

### Restarted Grill 2 decisions: resolved by ADRs 0015 through 0018

Resolved on 2026-08-09 after explicit confirmation:

- `V2-OPEN-MIGRATION-01` → [ADR 0015](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md);
- `V2-OPEN-PROJECTION-SCOPE-01` → [ADR 0016](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md);
- `V2-OPEN-BK-01` → [ADR 0017](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md);
- `V2-OPEN-OBJ-02` → [ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md).

Their original recommendations and source rationale remain in
[the restarted Grill 2 record](grill-notes/03-restarted-grill-2-scope-and-offload-frontier.md).

### `V2-OPEN-FABRIC-01`: resolved by ADR 0014

Resolved on 2026-08-09. Multiple Protocol Cells may share physical provider infrastructure, compatible transport
capacity, worker processes, and observability. Each cell owns a distinct Cell Provider Scope/session, namespace,
credential/KMS and operator scope, admission/retry/circuit-breaker state, queue/cache accounting, task root, GC
capability, drain, and close lifecycle. Object groups do not cross cells in 0.2. Dedicated provider infrastructure is an
optional stronger deployment topology; an outage of shared physical infrastructure may affect all attached cells.

The normative contract is [ADR 0014](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md). This ID is no
longer an active design gate.
