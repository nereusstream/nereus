---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: NormativeIndex
sourceTuple: v2-m0
---

# Nereus V2 design index

Nereus V2 replaces the V1 append correctness model. It is not an incremental optimization, compatibility layer, or
online migration of V1. The V2 product is a Storage Fabric containing independent Kafka and Pulsar Protocol Cells. It
combines shared storage lifecycle contracts with two deliberately different data paths: cost-first Object WAL and
performance-first BookKeeper WAL.

## Current status

- `main` develops `0.2.0-SNAPSHOT` from source tuple `v2-m0`.
- M0 freezes the V2 documentation contract only; V2 Java implementation and runtime evidence have not started.
- M1 implementation-readiness Round 1 freezes the pure-active-graph, module, milestone, source-lock, gate, and
  cross-repository promotion boundaries in ADR 0081; it does not claim that an M1 module or gate exists yet.
- M1 implementation-readiness Round 2 freezes the outer NTB1/NSE1/NTA1 domain contracts, exact M1 metadata
  capabilities/results, Kafka topic-create authority, Pulsar ownership-fence capability, and compatibility-namespace
  Registry/writer-set evidence boundary in ADR 0082; exact descendant byte tables/adapters remain OPEN.
- M1 implementation-readiness Round 3 freezes NPC1/NTI1 layouts and flat NTA1 structure; one Kafka profile input and
  linear residue-free batch admission; the Oxia-backed ELM witness-candidate boundary (current source proves primitives,
  not the adapter); INSTANCEID-derived, fresh-only Registry identity with inline writers; and the canonical virtual-
  ledger evidence envelope in ADR 0083. Exact numeric tables, caps, provider hooks, hash preimage, and receipt inner
  schema remain OPEN.
- M1 implementation-readiness Round 4 freezes `KAFKA=1/PULSAR=2`, NPN1 Pulsar authority leaves, Kafka last-wins/error/
  policy and remote-log admission, a local store-wide watch-continuity contract, canonical UUID/NLI1 namespace bytes,
  and one receipt result hierarchy plus safe attachment grammar in ADR 0084. Complete NTA1/name caps, concrete Oxia
  hook/source tuple, writer caps, and receipt numeric caps remain OPEN.
- Existing Java modules and Phase/Future evidence on `main` are V1 residue until replaced by a V2 milestone.
- The ordinary CI Pulsar API checkout remains a legacy V1-residue build baseline until the V2 Pulsar slice replaces
  that code; it is recorded separately and is not the V2 fork-development or parity baseline.
- The V1 implementation authority is branch `v0.1` at
  `a14d925da5763f36208f8ddca7bef31f3eb90b0b`; it is historical evidence, not a V2 contract.
- No V1 API, durable schema, object format, or online compatibility obligation is carried into V2.

## Authority order

When V2 sources disagree, use this order:

1. accepted V2 ADRs;
2. normative contracts in this directory;
3. the current milestone implementation plus its exact-source executable receipt;
4. V2 scenario matrix and implementation plan;
5. V1 Phase/Future documents and external research as historical input only.

Code does not become V2 authority merely because a similarly named V1 class still exists. A milestone may claim
`Verified` only when its implementation, normative docs, scenario status, source tuple, and receipt are synchronized.

## Frozen protocol and profile model

| Profile | ACK waits for | Object lifecycle | Product objective |
| --- | --- | --- | --- |
| `OBJECT_WAL` | durable Object WAL group coverage | background object-to-object materialization | cost first |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper quorum | no Object copy | performance first |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper quorum | sealed Protocol Coverage asynchronously offloaded | performance first with later cold-cost reduction |

A Topic Protocol Binding is immutable for one Topic Incarnation and fixes Protocol Cell, protocol kind, Position Domain,
payload mapping, and Native Write Authority. Kafka Offset and Pulsar Position are separate truths; shared storage uses
typed Protocol Coverage and never creates a universal logical offset.

A profile is immutable within one Storage Epoch. The durable model permits an append-only epoch chain with
protocol-native cutover frontiers, but ADR 0015 limits 0.2 to exactly one initial epoch per Topic Incarnation and no
online profile-transition API/state machine. Operational batching, cache, throttling, and compaction policy remain
separately tunable only at their contract-defined activation boundaries.

Correctness, recovery, security/parser hard caps, and durable compatibility are never configuration switches.
Topic/Tenant-or-Namespace policy uses closed typed classes for latency/cost intent and cannot enlarge a format cap;
Protocol Cell/shard policy owns shared scheduling and recovery budgets; host/process configuration only caps resources
and may cause backpressure or early seal. Effective budgets use the minimum across scopes. Any resolved value that
affects bytes or recovery is persisted at its Storage Epoch, hard-recovery WalRun Root, Object-group, or offload-attempt
boundary. Product/Deployment owns the base semantic default; Namespace/Topic may inherit or explicitly override, Cell
admits/caps, and host only ceilings. One configurable identity never spans those lifecycles or lets failover silently
reinterpret state. Topic-specific soft packing is not a singular WalRun Root identity: at most three lazy lanes share
one Root/pointer, vector checkpoint chain, and aggregate hard budgets. Permanent IDs map `0/1/2` to
`OBJECT_LATENCY/BALANCED/COST`; target/linger values change by `packingPolicyVersion`. Lane sequence allocates after
immutable group-plan admission and before HKDF/encryption/final-body seal.

ADR 0016 retains Access Projection/Migration Link identities and rejects a second Native Write Authority, while
excluding cross-protocol serving and authority-transfer runtime from 0.2. For Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`, ManagedLedger ledger/offload metadata is the sole lifecycle authority and Nereus supplies
a sealed-ledger `LedgerOffloader`; one attempt writes one data/root Object pair and any Nereus manifest is derived.
Persisted attempt scope pins deterministic keys and a bounded root/read/delete lifecycle. Binding plus initial epoch use
one closed logical schema v1 keyed by a typed native incarnation with deterministic IDs; Kafka activates it only at
fresh-bootstrap feature level 2. Pulsar async offload uses bounded NPO1 plus native whole-range fallback and final
deletion revalidation. Kafka owns its generated aggregate in TopicImage; Pulsar uses a permanent generation selector
and same-key retired tombstone. Pulsar async data uses independently verifiable NPD1 blocks with a checked 16-byte
derived-entry row/streaming envelope and Deployment->Namespace->Topic policy resolution, plus a ManagedLedger-owned
dual-source handle whose BK pins drain before persisted BK_DELETE_INTENT/DONE; Topic/Namespace policy selects RETAIN_BK
or DELETE_AFTER_VERIFIED. Pulsar Object WAL positions use fixed aligned `2^40`, permanent-lifecycle Cell slices from one
64-KiB/256-assignment registry and fail closed rather than expand a slice. Any RANGE candidate preserves an installed
ManagedLedger-incarnation grant across owner takeover, burns at most one stale candidate, and leaves allocator mode
unselected. NWG1 uses
object-local binding epochs, in-body append-unit authority, co-located Kafka commit sets, a KMS-wrapped run key with
per-Object AEAD, content-addressed strong-LIST discovery, explicit provider-absent crash cuts, and immutable
Root/Seal/successor pointer lineage. Every known leaf carries its bounded exclusive directory-prefix end. Up to three
lane-local sequences build lazily, while one publisher-epoch-fenced combiner checkpoints provider-resolved extents
through a run-wide predecessor/vector chain with aggregate bounds; member protocol ACK is not checkpoint eligibility.
Physical `LaneExtentResolvedThrough` is separate from binding `BindingDurableFrontier`; owner-local lazy trackers
release independent commit sets without remote metadata or persisted gap maps. Page Root identity is encoded once,
physical rows/Seal contain no Binding/ACK state, and active-tail recovery uses bounded prefix rather than whole-Object
GET. Tracker plus locator capacity reserves before position allocation; one full 64-bit local ticket represents one
Kafka commit set or Pulsar entry. One shared verified extent feeds compact protocol-range locators that publish before
Readable/Durable frontiers and ACK; a generic Protocol Coverage TreeMap is forbidden on the hot path, without freezing
one heavy per-Binding/unit Java index. Open tails still require LIST and sealed runs require one final physical vector
inventory. `NONE` is the default Root-fixed checkpoint provider-proof mode; an evidenced version-bound row stores only
tag/length/bounded canonical binary token and remains an accelerator. 0.2 has no partial-run recovery-skip vector;
whole-WalRun retirement is the only coarse exclusion before M3/M7 evidence reopens that branch. Routine frame ranges
authenticate the Root-bound directory/frame rather than requiring a new whole-Object provider proof or HEAD; prefix
bytes, not frame count alone, bound cold-read amplification.

Append does not allocate a read snapshot per ACK. A logical `BindingReadViewSnapshot` combines release-published
frontiers with low-frequency source-selection generations pinned allocation-free for one Binding-scoped protocol read
batch. Each unfinished batch owns one bounded cross-Binding pool slot; StoreLoad-ordered hazard publication and a
generation-tagged coherent cell prevent pin-after-retire and torn frontier reads. Disjoint manifest/active-tail ranges
may share one snapshot, while an atomic append unit or declared whole-range fallback stays source-pure. The slot owns
one ABA-safe `SlotLeaseWord`; cancellation stops new source use and only complete terminal drain clears it.

Object-WAL protection retirement uses one Binding selector CAS to atomically select `PREFERRED_ONLY`, close E, grant
same-owner no-fallback E+1, and carry E's closure anchor; takeover competes on the same predecessor. Only
`ADMITTING/STOPPED` are durable read-admission states, and an unresolved response forbids further E admission. One
small inline canonical unresolved-anchor set preserves a dedicated emergency STOPPED envelope. Each source row
inherits its own `first_i` and releases against `[first_i,sharedLast]`; batch minimum is summary only.

Each fallback-relevant proof is deterministic create-only, source-independent, reusable, and follows one asynchronous
irreversible terminal cut plus immutable historical capability evidence. Inline batch activation costs one selector
CAS; reference mode requires an atomically validated immutable create plus that CAS; N sources retain up to N release
CAS operations and bounded O(N) reconciliation. Quarantine blocks batch retirement/capacity but not eligible sibling
release. Terminal safety uses the immutable candidate plus one closed verifier; valid anchors prune asynchronously in
batches. Completed batch metadata moves only through irreversible same-key `FULL_V1 -> RETIRED_V1`, whose compact
tombstone remains permanent in 0.2 and authorizes no source GC. No-fallback epochs create no proof liability. Normal
reads still perform zero remote metadata I/O.

Provider sharing is physical, not authoritative. Multiple cells may use the same external Object Storage or BookKeeper
infrastructure, while each cell owns its Cell Provider Scope/session, namespace, credential/KMS and operator scope,
admission, task/cache roots, and GC authority. Object groups do not cross Protocol Cells in 0.2. Compatible transport
pooling is optional and remains below the independently owned sessions.

## Reading order

1. [V2 Context Map](../../CONTEXT-MAP.md) and the linked Kafka, Pulsar, and Shared Storage glossaries
2. [Correctness and append](01-correctness-and-append.md)
3. [Protocol binding, Storage Epochs, and profiles](02-storage-profiles-and-topic-binding.md)
4. [Object WAL](03-object-wal.md)
5. [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md)
6. [Manifest, read, retention, and GC](05-manifest-read-retention-gc.md)
7. [Metadata backends and handoff](06-metadata-backends-and-handoff.md)
8. [Protocol integrations and product gates](07-protocol-integrations.md)
9. [Implementation plan and gates](08-implementation-plan-and-gates.md)
10. [Scenario evidence matrix](09-scenario-evidence-matrix.md)
11. [Architecture tradeoffs](tradeoffs.md)
12. [Open questions](open-questions.md) and [grill session records](grill-notes/)
13. [Structured source locks](source-locks.json) and [structured scenarios](v2-scenarios.json)

Accepted decisions:

- [ADR 0007: WAL-linearized append](../decisions/0007-v2-wal-linearized-append.md)
- [ADR 0008: storage profiles and ACK boundaries](../decisions/0008-v2-storage-profiles-and-ack-boundaries.md)
- [ADR 0009: protocol-native data paths](../decisions/0009-v2-protocol-native-data-paths.md)
- [ADR 0010: topic profile binding](../decisions/0010-v2-topic-profile-binding.md) — superseded by ADR 0012
- [ADR 0011: position domains and multi-protocol Storage Fabric](../decisions/0011-v2-position-domains-and-multi-protocol-fabric.md)
- [ADR 0012: Storage Epochs and profile evolution](../decisions/0012-v2-storage-epochs-and-profile-evolution.md)
- [ADR 0013: cross-protocol projection and migration boundary](../decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md)
- [ADR 0014: provider sharing and Protocol Cell isolation](../decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md)
- [ADR 0015: 0.2 Storage Epoch runtime scope](../decisions/0015-v2-0.2-storage-epoch-runtime-scope.md)
- [ADR 0016: 0.2 cross-protocol runtime scope](../decisions/0016-v2-0.2-cross-protocol-runtime-scope.md)
- [ADR 0017: Pulsar ManagedLedger offload authority](../decisions/0017-v2-pulsar-managed-ledger-offload-authority.md)
- [ADR 0018: Object WAL uncertain PUT proof](../decisions/0018-v2-object-wal-uncertain-put-proof.md)
- [ADR 0019: initial binding/epoch atomic visibility](../decisions/0019-v2-initial-binding-epoch-atomic-visibility.md)
- [ADR 0020: Pulsar sealed-ledger async offload](../decisions/0020-v2-pulsar-sealed-ledger-async-offload.md)
- [ADR 0021: Object WAL checksum domains](../decisions/0021-v2-object-wal-checksum-domains.md)
- [ADR 0022: Pulsar Object WAL virtual-ledger authority](../decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md)
- [ADR 0023: Topic Binding Aggregate physical record](../decisions/0023-v2-topic-binding-aggregate-record.md)
- [ADR 0024: Pulsar sealed-ledger Object layout](../decisions/0024-v2-pulsar-sealed-ledger-object-layout.md)
- [ADR 0025: initial checksum algorithms and provider proof](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md)
- [ADR 0026: protocol-native frame payload bytes](../decisions/0026-v2-protocol-native-frame-payload-bytes.md)
- [ADR 0027: Pulsar virtual-ledger numeric compatibility](../decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md)
- [ADR 0028: Topic incarnation keys and deterministic IDs](../decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md)
- [ADR 0029: Pulsar sealed-ledger root and lifecycle](../decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md)
- [ADR 0030: Object WAL run root and content-addressed discovery](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md)
- [ADR 0031: protocol frame and append commit set](../decisions/0031-v2-protocol-frame-and-append-commit-set.md)
- [ADR 0032: Pulsar virtual-ledger reservation registry](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md)
- [ADR 0033: Topic Binding Aggregate logical schema v1](../decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md)
- [ADR 0034: Kafka feature level 2 bootstrap activation](../decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md)
- [ADR 0035: Pulsar NPO1 sealed-ledger root format](../decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md)
- [ADR 0036: Pulsar native dual-source read and deletion safety](../decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md)
- [ADR 0037: Object WAL binding-context epoch authority](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md)
- [ADR 0038: Object WAL provider-absent crash contract](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md)
- [ADR 0039: bounded WalRun lifecycle, recovery, and root pointer](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md)
- [ADR 0040: NWG1 append-unit directory and co-location](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md)
- [ADR 0041: Pulsar virtual-ledger slice contract](../decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md)
- [ADR 0042: Kafka topic aggregate KRaft record and image ownership](../decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md)
- [ADR 0043: Pulsar topic generation selector and retired tombstone](../decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md)
- [ADR 0044: Pulsar NPD1 sealed-ledger data blocks](../decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md)
- [ADR 0045: Pulsar dual-source read handle and source pins](../decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md)
- [ADR 0046: NWG1 run key, AEAD, and authenticated directory](../decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md)
- [ADR 0047: WalRun Root, seal, and successor publication](../decisions/0047-v2-walrun-root-seal-and-successor-publication.md)
- [ADR 0048: Pulsar virtual-ledger fixed-slice exhaustion](../decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md)
- [ADR 0049: configuration scopes and persisted semantics](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md)
- [ADR 0050: Kafka aggregate wire and publication validation](../decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md)
- [ADR 0051: Pulsar selector state machine and cached fence](../decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md)
- [ADR 0052: Pulsar BookKeeper delete state and retention policy](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md)
- [ADR 0053: WalRun checkpoint bounds and open-tail recovery](../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md)
- [ADR 0054: Pulsar virtual-ledger bootstrap geometry](../decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md)
- [ADR 0055: Pulsar virtual-ledger allocator evidence protocol](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md)
- [ADR 0056: NPD1 checked envelope and derived entry row](../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md)
- [ADR 0057: NPD1 policy default authority and evidence](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md)
- [ADR 0058: NWG1 directory-prefix capacity and evidence](../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md)
- [ADR 0059: Object WAL leaf directory-prefix hint](../decisions/0059-v2-object-wal-leaf-prefix-hint.md)
- [ADR 0060: WalRun lazy lanes and vector checkpoint](../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md)
- [ADR 0061: Pulsar range-grant owner takeover](../decisions/0061-v2-pulsar-range-grant-owner-takeover.md)
- [ADR 0062: Object WAL packing catalog and leaf sequence](../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md)
- [ADR 0063: provider-resolved checkpoint publisher](../decisions/0063-v2-provider-resolved-checkpoint-publisher.md)
- [ADR 0064: Object WAL physical and binding frontiers](../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md)
- [ADR 0065: physical checkpoint row and Seal payload](../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md)
- [ADR 0066: pre-position reservation and completion ticket](../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md)
- [ADR 0067: active-tail readable publication and index boundary](../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md)
- [ADR 0068: checkpoint provider-proof mode and row encoding](../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md)
- [ADR 0069: Binding read-view generation and pin boundary](../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md)
- [ADR 0070: generation-tagged read publication and hazard slots](../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md)
- [ADR 0071: durable owner-read quiescence and protection release](../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md)
- [ADR 0072: slot lease word and terminal source drain](../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md)
- [ADR 0073: Read Admission Epoch and source-independent quiescence window](../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md)
- [ADR 0074: quiescence capability evidence and historical binding](../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md)
- [ADR 0075: Binding read selector and fallback-interval linearization](../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md)
- [ADR 0076: Read Admission Epoch terminal cut and on-demand proof](../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md)
- [ADR 0077: fused selector closure and no-fallback epoch cut](../decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md)
- [ADR 0078: per-source retirement interval and batch retirement](../decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md)
- [ADR 0079: bounded inline closure anchors and terminal publication](../decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md)
- [ADR 0080: irreversible Source Retirement Batch tombstone](../decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md)
- [ADR 0081: M1 pure active graph and promotion boundary](../decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md)
- [ADR 0082: M1 domain and control-authority contracts](../decisions/0082-v2-m1-domain-and-control-authority-contracts.md)
- [ADR 0083: M1 wire, control-plane, and evidence bounds](../decisions/0083-v2-m1-wire-control-and-evidence-bounds.md)
- [ADR 0084: M1 leaf, witness, Registry, and receipt contracts](../decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md)

## Open design gates

`V2-OPEN-FABRIC-01` was resolved by ADR 0014. ADRs 0015 through 0018 resolve `V2-OPEN-MIGRATION-01`,
`V2-OPEN-PROJECTION-SCOPE-01`, `V2-OPEN-BK-01`, and `V2-OPEN-OBJ-02`; ADRs 0019 through 0022 resolve
`V2-OPEN-META-01`, `V2-OPEN-BK-03`, `V2-OPEN-OBJ-04`, and `V2-OPEN-PUL-OBJ-01`; ADRs 0023 through 0027 resolve
`V2-OPEN-META-02`, `V2-OPEN-BK-04`, `V2-OPEN-OBJ-05`, `V2-OPEN-OBJ-06`, and `V2-OPEN-PUL-OBJ-02`; ADRs 0028 through
0032 resolve `V2-OPEN-META-03`, `V2-OPEN-BK-05`, `V2-OPEN-OBJ-07`, `V2-OPEN-OBJ-08`, and
`V2-OPEN-PUL-OBJ-03`; ADRs 0033 through 0041 resolve `V2-OPEN-META-04`, `V2-OPEN-KAF-META-01`,
`V2-OPEN-BK-06..08`, `V2-OPEN-OBJ-09..14`, and `V2-OPEN-PUL-OBJ-04..06`; ADRs 0042 through 0048 resolve
`V2-OPEN-KAF-META-02`, `V2-OPEN-PUL-META-01`, `V2-OPEN-BK-09..10`, `V2-OPEN-OBJ-15..16`, and
`V2-OPEN-PUL-OBJ-07`; ADRs 0049 through 0054 resolve `V2-OPEN-KAF-META-03`, `V2-OPEN-PUL-META-02`,
`V2-OPEN-BK-12`, `V2-OPEN-OBJ-18`, and `V2-OPEN-PUL-OBJ-08`; ADR 0055 resolves the
`V2-OPEN-PUL-OBJ-10` evidence-protocol decision without selecting a mode or producing a PASS. ADRs 0056 through 0058
partially resolve NPD1 wire/policy evidence and NWG1 prefix capacity. ADRs 0059 through 0061 fix the leaf hint,
lazy-lane/vector-checkpoint structure, and RANGE takeover constraints. ADRs 0062 through 0064 additionally fix the
class/lane grammar, provider-resolved checkpoint publisher, and physical-versus-binding frontier split without
selecting remaining numeric values, final RANGE wire/size, or an allocator mode. ADRs 0065 through 0067 resolve
physical-only checkpoint/Seal rows, pre-position reservation/local ticket semantics, and non-disableable active-tail
publication. ADRs 0068 through 0080 resolve compact provider-proof semantics, logical read-view scope,
generation-tagged hazard capture, minimal slot reuse/terminal drain, source-independent owner-proof coverage,
historically bound capability evidence, fused selector closure, terminal/on-demand proof behavior, per-source release
intervals, explicit O(N) work, small bounded inline anchors with emergency STOPPED capacity, closed-verifier terminal
publication, and permanent same-key compact batch tombstones. The pure-document M0 runtime-design frontier is
exhausted. ADR 0081 freezes the M1 execution/promotion boundary; ADR 0082 freezes the outer M1 domain/control authority;
ADR 0083 fixes the next structural wire/control/evidence cuts. ADR 0084 closes protocol/authority leaves, Kafka
precedence and remote-log interlock, minimal store-wide continuity semantics, canonical UUID/NLI1 namespace identity,
and receipt accounting/path safety. It deliberately retains the complete NTA1/name caps, concrete ownership hook and
source evidence, exact Registry writer schema/caps, remaining receipt payload fields, and numeric attachment caps as
OPEN implementation-readiness descendants. Partial recovery omission, numeric caps,
physical proof-fold/capability encodings, and any tombstone-deletion authority remain evidence gates. The rows below
are the remaining active 0.2 evidence gates.

| Gate | Required decision/evidence | Must close before |
| --- | --- | --- |
| `V2-OPEN-BK-11` | select exact NPD1 block/Object/adapter numeric maxima, lower admission, and provider evidence after ADR 0056's checked wire/streaming contract | M2 Object format freeze |
| `V2-OPEN-BK-13` | execute ADR 0057's 1/4/8/16-MiB native-relative evidence, then select at most three classes and the Deployment base default | M2 offload policy freeze |
| `V2-OPEN-OBJ-17` | freeze exact NWG1 header/directory/row numeric caps after ADRs 0059/0062 fixed the complete leaf grammar | M3 Object WAL format freeze |
| `V2-OPEN-OBJ-19` | execute evidence and select target/linger/quantized values and numeric budgets without changing ADR-0062 class semantics | M3 Object WAL policy freeze |
| `V2-OPEN-PUL-OBJ-09` | choose an allocator only after evidence plus exact reservation/head/node wire, range size, Cell reservation concurrency, and ADR 0061 conformance | M1/M3 virtual-ledger allocator freeze |
| `V2-OPEN-OBJ-22` | execute bounded recovery and skip-hit evidence; only an SLO miss may reopen a whole-WalRun-first, Root/Seal-bound recovery omission certificate | M3/M7 recovery optimization decision |
| `V2-OPEN-OBJ-24` | admit a Provider version token only after canonical-binary cap, immutable-version, FULL_OBJECT SHA-256, rows/page, and range-benefit evidence; otherwise retain Root mode NONE | M3 checkpoint provider-proof admission |
| `V2-OPEN-READ-08` | execute M4 evidence and freeze the bounded proof-window/head/fold physical representation and numeric caps without a per-batch accumulator | M4 durable proof wire freeze |
| `V2-OPEN-READ-09` | execute M4/M5 evidence and freeze canonical capability/receipt encodings, verifier availability/revocation behavior, and admitted backend generations | M4/M5 takeover and GC capability freeze |
| `V2-OPEN-READ-15` | execute M4/M5 tombstone lifetime/capacity and concrete-backend ordered-history/lineage/stale-create evidence before reconsidering a metadata-only tombstone-deletion authority; 0.2 otherwise retains `RETIRED_V1` permanently | M4/M5 optional metadata-retirement authority |
| `V2-OPEN-BK-02` | validate one-active-ledger-per-Kafka-partition at 10k and 100k partitions | M2 Kafka BK layout freeze |
| `V2-OPEN-BENCH-01` | pin clean AutoMQ and native Pulsar acceptance baselines plus thresholds | M8 performance execution |

`V2-OPEN-MIGRATION-02..03`, `V2-OPEN-PUL-MIGRATION-01`, and `V2-OPEN-PROJECTION-01..03` remain recorded as deferred
future-design questions. ADRs 0015 and 0016 make them non-blocking for 0.2.

## Maintenance rule

Every V2 milestone change must update the normative contract, affected ADR/context language, tradeoff/open-question
status, scenario manifest, and gate in the same coherent change as the implementation. Confirmed decisions move out of
the non-normative question log; unconfirmed proposals never become contracts by repetition. Evidence is append-only and
source-qualified; an older PASS never becomes current-source evidence automatically.
