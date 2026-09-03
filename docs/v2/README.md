---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: CurrentSourceReceipt
authority: NormativeIndex
sourceTuple: v2-m1
---

# Nereus V2 design index

Nereus V2 replaces the V1 append correctness model. It is not an incremental optimization, compatibility layer, or
online migration of V1. The V2 product is a Storage Fabric containing independent Kafka and Pulsar Protocol Cells. It
combines shared storage lifecycle contracts with two deliberately different data paths: cost-first Object WAL and
performance-first BookKeeper WAL.

## Current status

- `main` develops `0.2.0-SNAPSHOT` from the N2 source tuple `v2-m1`; historical focused inputs retain their original
  `v2-m0` identity instead of being relabelled.
- M1 implementation and the pure-V2 active-graph prune are complete. The authoritative completion state is derived
  only from the trusted Fast/Exact gate results, N3 receipts, Final index, and scenario manifest; focused receipts alone
  never imply M1 PASS or activate an M2 data path.
- M1 implementation-readiness Round 1 freezes the pure-active-graph, module, milestone, source-lock, gate, and
  cross-repository promotion boundaries in ADR 0081; that readiness decision alone did not claim an implementation.
- M1 implementation-readiness Round 2 freezes the outer NTB1/NSE1/NTA1 domain contracts, exact M1 metadata
  capabilities/results, Kafka topic-create authority, Pulsar ownership-fence capability, and compatibility-namespace
  Registry/writer-set evidence boundary in ADR 0082. At that readiness cut, exact descendant byte tables/adapters were
  still open; ADRs 0083 through 0085 and the completed M1 slices now close the M1 descendants.
- M1 implementation-readiness Round 3 freezes NPC1/NTI1 layouts and flat NTA1 structure; one Kafka profile input and
  linear residue-free batch admission; the Oxia-backed ELM witness-candidate boundary (current source proves primitives,
  not the adapter); INSTANCEID-derived, fresh-only Registry identity with inline writers; and the canonical virtual-
  ledger evidence envelope in ADR 0083. At that readiness cut, exact numeric tables, caps, provider hooks, hash
  preimage, and receipt inner schema remained open; later accepted contracts and current-source gates now close them.
- M1 implementation-readiness Round 4 freezes `KAFKA=1/PULSAR=2`, NPN1 Pulsar authority leaves, Kafka last-wins/error/
  policy and remote-log admission, a local store-wide watch-continuity contract, canonical UUID/NLI1 namespace bytes,
  and one receipt result hierarchy plus safe attachment grammar in ADR 0084. The later M1.1b acceptance closes
  NTA1/name caps, M1.1c-R0 later closes the writer-count/canonical-capacity input, and M1-2 closes the persisted-v1
  receipt numeric caps from non-promotable evidence.
  O1 owns the confirmed Oxia client API, exact client/server bases, final fork, and focused artifact/runtime evidence
  without promoting `V2-META-006`.
- M1 implementation-readiness Round 5 freezes minimal independent NTA1 semantics without claiming its complete codec,
  the Oxia client-only dummy ready barrier and source/artifact/image separation, two writer kinds plus a 120-byte row
  and immutable admission evidence, and one content-identified receipt/Final hierarchy in ADR 0085. M1.1a module,
  identity, deterministic-ID, SPI, dependency, and continuity scaffolding may start. The 2026-08-12 M1.1b acceptance
  subsequently closes the NTA1 v1 policy/name/total table. M1.1c-R0 subsequently closes writer count and canonical
  Registry capacity; M1-2 closes only receipt numeric inputs. The separately reviewed G1 validator and executable N2/N3
  promotion evidence are now complete for the current source tuple.
- The accepted [M1 execution index](detailed_design/m1/README.md) and
  [M1.1a-A code-level design](detailed_design/m1/m1.1a-domain-spi-foundation.md) split the first implementation into
  module, identity/domain, SPI/gate, continuity-fork, and metadata-oxia targets. The first three are implemented by
  `nereus-domain`, `nereus-metadata-spi`, and `v2M1FoundationCheck`; the Oxia fork is focused-evidence complete; and
  [metadata-oxia O2](detailed_design/m1/m1.1a-oxia-capability-scaffold.md) is locally verified by its locked dependency,
  69-test scaffold gate, 299-test whole-module compatibility run, and non-promotable local receipt.
- [M1.1b-Q1](detailed_design/m1/m1.1b-nta1-codec.md) has 14 historical evidence-only tests and an immutable
  [structured readiness receipt](evidence/v2-m0/m1.1b-q1/README.md). It measures 4-KiB and 16-KiB Pulsar-name
  candidates, checked 8,397/32,973-byte candidate parser caps, all six protocol/profile rows, and strict allocation
  boundaries. The current design is now `Accepted`: v1 selects `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`, 4,096 bytes per
  classic-persistent Pulsar name, and exact checked caps `54/8,214/8,397`. Q1 remains
  `READINESS_EVIDENCE_ONLY`. The production slice is now exact-locally verified at `01a70f17`: 55 domain tests, 73
  focused O2 tests, and 303 whole metadata-oxia tests bind the codec, exact goldens, inventory boundary, and aggregate
  adapter in a [non-promotable receipt](evidence/v2-m0/m1.1b/README.md); no scenario changes.
- The accepted [M1.1c-R0 Registry-capacity design](detailed_design/m1/m1.1c-registry-capacity-spike.md) freezes an
  evidence-only writer-cohort/lifecycle model, a 184-byte fixed capacity-accounting header, exact checked sizing, and
  a non-promotion gate without implementing R1. It measures full binary-by-credential coexistence, rollback, fenced
  residue, bootstrap/admin allocation capability, omitted writers, and add/fence/drain/remove ordering. Eighteen focused
  tests and `v2M1RegistryCapacityCheck` bind Nereus `03d27256` to accepted `maxWriterCount=14`, a 51,016-byte largest
  legal canonical Registry value, and 14,520-byte reserved margin in deterministic
  [non-promotable evidence](evidence/v2-m0/m1.1c-r0/README.md). The inherited 120/192/256/65,536 contracts are unchanged,
  at the R0 readiness cut every V2-POSITION scenario was `PLANNED` and the production Registry codec was unavailable.
  R1 and canonical N3 evidence now promote exactly `V2-POSITION-003..011` as described below.
- The accepted [M1-2 receipt/parser-cap design](detailed_design/m1/m1-2-receipt-parser-caps.md), 36 focused tests, and
  deterministic [readiness evidence](evidence/v2-m0/m1-2-receipt-caps/README.md) bind Nereus `75593faf`, eleven named
  sample families, source-lock input SHA, formulas, observed maxima, stable error categories, and JSON SHA. ADR 0084
  is the sole normative cap table. G1 now consumes those constants in a production parser and non-rerunning Final
  resolver; the historical M1-2 evidence itself added no N1/K1/P1/R1, N2/N3, scenario promotion, or M1 Final authority.
- The accepted [N1 artifact design](detailed_design/m1/n1-immutable-domain-artifact.md) publishes only the domain and
  metadata-SPI modules under an exact source-qualified coordinate and absent source-SHA repository path. It requires
  two byte-identical clean builds and later locks every JAR/source-JAR/POM/Gradle-metadata length and SHA; this is an
  immutable K1/P1/R1 input, not a source-tuple promotion or M1 PASS. Source `330aaec3`, manifest `9058ff01`, and the
  [N1 receipt](evidence/v2-m1/n1/README.md) are now verified by `v2M1N1ArtifactCheck`.
- The accepted [K1 Kafka KRaft metadata-authority design](detailed_design/m1/k1-kafka-kraft-metadata-authority.md)
  translates feature 2, API 32000, direct domain mapping, CreateTopics, TopicImage publication, and Admin projection
  into the source-locked fork. K1 is focused-exact complete at pushed Kafka commit `8afbc42566`: 39 exact tests in 16
  suites pass with zero failure/error/skip and the [receipt](evidence/v2-m1/k1/README.md) remains metadata-only,
  `promotionEligible=false`, and not M1 PASS.
- The accepted [P1 Pulsar selector and ownership-fence design](detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md)
  preserves its focused receipt at Pulsar `778862323d` while locking final M1 execution to the clean pure-V2 fork
  `072aa1c440`: immutable N1/P1/O1 inputs, 100 Nereus metadata tests, two real-Oxia tests, and 36 Pulsar tests are
  rerun by `v2M1P1FocusedCheck` with zero failure/error/skip. The final fork contains no V1 adapter/runtime, and the
  [receipt](evidence/v2-m1/p1/README.md) remains `P1_FOCUSED_ONLY`, `promotionEligible=false`, and does not activate
  BrokerService/PersistentTopic Produce/read paths, choose an allocator, prune V1, promote a scenario, or claim M1 PASS.
- The accepted [R1 Registry design](detailed_design/m1/r1-virtual-ledger-registry.md) preserves its focused receipt at
  Nereus `8a213a85bf` and binds current exact-source execution to `42598fe633`: exact NLI1/NVR1/NVA1/RAE1, held writer
  interlock, immutable evidence, closed response-loss
  authority, derived views, 35 domain tests, eight metadata tests, and two source-locked real-Oxia tests are verified by
  `v2M1R1FocusedCheck`. Its [receipt](evidence/v2-m1/r1/README.md) is a non-promotable
  `R1_FOCUSED_ONLY` wrapper around the `REGISTRY_CONFORMANCE` subject; it selects no allocator and promotes no scenario.
- The accepted [G1 validator design](detailed_design/m1/g1-receipt-validation-and-gates.md) is focused-current complete
  at Nereus `ba11fe4a29`: 49 production receipt/Final tests and 14 evidence-only allocator tests are clean, the three
  promotion gate surfaces are registered, and the [receipt](evidence/v2-m1/g1/README.md) remains
  `PASS_G1_FOCUSED_ONLY`. The pure-V2 graph is now the only active build/runtime graph; N2/N3 promotion is governed by
  the trusted gate results and Final index rather than this historical focused receipt. Final now also requires the
  receipt-bound tested Nereus commit to precede a linear evidence-only N3 descendant and rejects dirty, source-lock-
  mismatched, merge, empty, or non-evidence changes through `v2M1EvidenceFreshnessCheck`. Trusted execution rebuilds
  both gate files, two normalized reports, two receipts, and the Final index from actual JUnit XML and byte-compares
  that complete seven-file set with committed N3 evidence.
- ADR 0086 fixes the Kafka BookKeeper semantic layout: one Kafka Offset Domain across profiles, one logical ledger
  chain per partition, low-frequency run/generation roots, packed in-ledger RecordBatch indexes, owner-local active-tail
  locators, targeted Fetch, bounded overlapping writes with ordered publication, and bounded checkpoint-tail recovery.
  The accepted [M2-K0 implementation-input closure](detailed_design/m2/kafka-m2-k0-implementation-input-closure.md)
  now fixes how exact `NBKE2` bytes/caps, numeric classes, modules, provider sessions, and evidence gates must land.
  Their production implementation and 10k/100k evidence remain `NotStarted`; no runtime is implied.
- ADR 0087 fixes Kafka protocol semantics over every profile: distinct Allocated/Durable/LEO/HW/LSO frontiers,
  native duplicate identity plus speculative producer state, fenced locator/protocol publication, compact descriptor
  replication with hard-bounded Observed/Applied eligibility, election-bounded tail adoption, profile-neutral BK/Object
  protocol checkpoints with an independent terminal Head, coherent/delayed Fetch, native read-committed metadata, and
  semantic compaction. `NBKE2` checkpoint carriage is M2, exact Object `NWKCP1` bytes/Head/key caps are M3, and native
  Kafka transport/process activation is M6. The M2 carrier is verified by M2 Final, and M3 Final closes the exact
  `NWKCP1` scope; native transport/process activation remains M6 work.
- ADR 0088, [ADR 0089's exact Header amendment](../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and the
  accepted [M3-I0 NWG1 input closure](detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) freeze the exact
  256-byte Header offsets, 116/104/96/48-byte NWG1 rows, strict code/cap/crypto tables, six-vector/114-component A corpus,
  84-record/240-path B contract, 50-trace C contract, and tiered D evidence boundary. The Header has no node session or
  duplicate class field; `laneId` is the permanent class ID, Object digest is SHA-256/v1 code `1/1`, and all twelve
  first-satisfied close reasons are distinct. That M3-I0 acceptance was documentation-only; later accepted ADRs and
  exact-source implementation descendants now supply the production codec/manifests/runners/harnesses,
  Root/Pointer, `NWKCP1`, and Provider/KMS/native-reference children. The later common tested source
  `e5e53e62865c21845621037bea5f18c092bd4259` supplies canonical V5 `RANGE_SELECTED(RANGE_64)`, all eleven fresh Final
  children, 26 scenario promotions, and immutable M3 Final
  `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json` with SHA-256
  `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a`. M3 is closed and hard-frozen; native
  broker/controller activation remains M6.
- The accepted [M4 design index](detailed_design/m4/README.md) and M4-A/B/C/D preserve the complete fixed-reviewer
  responses from Grills 32 through 35 and hard-freeze the logical read view, typed source plan, hazard lifetime,
  read-quiescence proof, exact protection-generation release, M4/M5 ownership, and evidence hierarchy. Runtime,
  current-source Kafka/Pulsar integration, four exclusive evidence tasks, and fail-closed child/Final validation are
  implemented without amending those inputs. Exact tested source
  `6af0877fcb2a1df09f32b26db74f1edf6b29d784` now binds four exclusive children and the current immutable
  [M4 Final](evidence/v2-m4/final/final-source-6af0877fcb2a1df09f32b26db74f1edf6b29d784/m4-final.json)
  SHA-256 `af9de78c8280f6fbe60c08b2c6b26c819381466fb18bd5a8ca76bb472823f704`. Exactly
  `V2-READ-001/003/004/005/007` are promoted; shared M4/M5 rows remain `PLANNED`. `v2M4Check` is Final
  authority while `v2M4DesignCheck` remains the historical design-only gate.
- ADR 0091 is a later M3-P1 implementation descendant: exact `NVAC1`/`NVAH1`/`NVAN1`, bounded Oxia keys, production
  SPI/transitions, 48 ordinary allocator tests, and formal-runner contracts. It did not by itself select a mode or
  promote a scenario; the later e5 Final closes those evidence obligations with `RANGE_64`. It still does not activate
  a native broker process.
- The initial foundation supplied Java-17/JDK-only domain values, NPC1/NTI1/NPN1 plus NTB1/NSE1 goldens, direct
  aggregate validation, exactly four metadata capabilities, closed create/CAS outcomes, production dependency/API
  guards, and reproducible JAR/source-JAR/POM hashes. It deliberately made no NTA1, Registry, P1/R1, real-backend, or
  M1 Final claim; later source-locked slices and canonical N3 evidence now supply those M1 descendants.
- The active Java/build/publication/ordinary-CI graph on `main` is pure V2. V1 implementation is retained only in
  protected history and `docs/v1`; historical Phase/Future records are not executable authority.
- Source locks retain the former ordinary-build Pulsar API checkout only as `HISTORICAL_ONLY`. Final P1 exact-source
  execution uses `nereus/v2-m1-p1-selector-fence-pure-v2` at `072aa1c440`.
- The V1 implementation authority is `v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`; it is historical evidence, not
  a V2 contract.
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

Append does not allocate a read snapshot per ACK. A logical `BindingReadViewSnapshot` combines locally published
frontiers (through the exact fenced state cut for Kafka) with low-frequency source-selection generations pinned
allocation-free for one Binding-scoped protocol read
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

1. [V2 Context Map](context-map.md) and the linked Kafka, Pulsar, and Shared Storage glossaries
2. [Overall architecture](architecture.md)
3. [Correctness and append](01-correctness-and-append.md)
4. [Protocol binding, Storage Epochs, and profiles](02-storage-profiles-and-topic-binding.md)
5. [Object WAL](03-object-wal.md)
6. [BookKeeper and Pulsar](04-bookkeeper-and-pulsar.md)
7. [Manifest, read, retention, and GC](05-manifest-read-retention-gc.md)
8. [Metadata backends and handoff](06-metadata-backends-and-handoff.md)
9. [Protocol integrations and product gates](07-protocol-integrations.md)
10. [Implementation plan and gates](08-implementation-plan-and-gates.md)
11. [M1 execution index](detailed_design/m1/README.md), [M2 detailed-design index](detailed_design/m2/README.md),
    [M3 closed execution index](detailed_design/m3/README.md), and
    [M4 detailed-design index](detailed_design/m4/README.md)
12. [Scenario evidence matrix](09-scenario-evidence-matrix.md)
13. [Architecture tradeoffs](tradeoffs.md)
14. [Open questions](open-questions.md) and [grill session records](grill-notes/)
15. [Structured source locks](source-locks.json) and [structured scenarios](v2-scenarios.json)

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
- [ADR 0085: M1 foundation start and deferred codec bounds](../decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md)
- [ADR 0086: Kafka BookKeeper run, range index, and ordered pipeline](../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md)
- [ADR 0087: Kafka Produce/Fetch frontiers, shared-storage ISR, and protocol recovery](../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md)
- [ADR 0088: M3 NWG1 implementation-input closure](../decisions/0088-v2-m3-nwg1-implementation-input-closure.md)
- [ADR 0089: M3 NWG1 v1 Header layout amendment](../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md)
- [ADR 0090: M3 NWG1 mutation external-call profiles](../decisions/0090-v2-m3-nwg1-mutation-external-call-profiles.md)
- [ADR 0091: M3 Pulsar virtual-ledger allocator wire and selection](../decisions/0091-v2-m3-pulsar-virtual-ledger-allocator-wire-and-selection.md)
- [ADR 0092: M3 Object-WAL protocol checkpoint and data path](../decisions/0092-v2-m3-object-wal-protocol-checkpoint-and-data-path.md)
- [ADR 0093: M3 WalRun control wire and lifecycle](../decisions/0093-v2-m3-walrun-control-wire-and-lifecycle.md)
- [ADR 0094: M3 allocator evidence workload and selection amendment](../decisions/0094-v2-m3-allocator-evidence-workload-and-selection-amendment.md)
- [ADR 0095: M3 recovery envelope and C1 lifecycle amendment](../decisions/0095-v2-m3-recovery-envelope-and-c1-lifecycle-amendment.md)
- [ADR 0096: M3 owner-open conservative rollover amendment](../decisions/0096-v2-m3-owner-open-conservative-rollover-amendment.md)
- [ADR 0097: M3 reproducible allocator Oxia image amendment](../decisions/0097-v2-m3-reproducible-allocator-oxia-image-amendment.md)
- [ADR 0098: M3 current-source M2 regression prerequisite projection](../decisions/0098-v2-m3-current-source-m2-regression-prerequisite-projection.md)
- [ADR 0100: M3 allocator mass-takeover recovery endpoint amendment](../decisions/0100-v2-m3-allocator-mass-takeover-recovery-endpoint-amendment.md)
- [ADR 0101: M3 allocator Cell-proof concurrency scheduling amendment](../decisions/0101-v2-m3-allocator-cell-proof-concurrency-scheduling-amendment.md)
- [ADR 0102: M3 allocator JUnit diagnostic-output containment amendment](../decisions/0102-v2-m3-allocator-junit-diagnostic-output-containment-amendment.md)
- [ADR 0103: M3 allocator population-construction batch scheduling amendment](../decisions/0103-v2-m3-allocator-population-construction-batch-scheduling-amendment.md)
- [ADR 0104: M3 allocator validator-proof adaptive campaign amendment](../decisions/0104-v2-m3-allocator-validator-proof-adaptive-campaign-amendment.md)
- [ADR 0105: M3 preselection evidence source-lock amendment](../decisions/0105-v2-m3-preselection-evidence-source-lock-amendment.md)
- [ADR 0106: M3 allocator V2 child and Final evidence amendment](../decisions/0106-v2-m3-allocator-v2-child-final-evidence-amendment.md)
- [ADR 0107: M3 allocator bounded-adaptive formal-entry wiring amendment](../decisions/0107-v2-m3-allocator-bounded-adaptive-formal-entry-wiring-amendment.md)
- [ADR 0108: M3 allocator protocol feasibility, asynchronous admission, and baseline amendment](../decisions/0108-v2-m3-allocator-protocol-feasibility-async-admission-baseline-amendment.md)
- [ADR 0109: M3 Native baseline executor composition and formal-diagnostic equivalence amendment](../decisions/0109-v2-m3-native-baseline-executor-composition-formal-diagnostic-equivalence-amendment.md)
- [ADR 0110: M3 candidate warm-up load-rejection classification amendment](../decisions/0110-v2-m3-allocator-candidate-warmup-load-rejection-classification-amendment.md)
- [ADR 0111: M3 derived-floor physical budget projection amendment](../decisions/0111-v2-m3-allocator-derived-floor-physical-budget-projection-amendment.md)
- [ADR 0112: M3 V3 derived-rate workload entry amendment](../decisions/0112-v2-m3-allocator-v3-derived-rate-workload-entry-amendment.md)
- [ADR 0113: M3 V3 completed-workflow Cell reconciliation amendment](../decisions/0113-v2-m3-allocator-v3-completed-workflow-cell-reconciliation-amendment.md)
- [ADR 0114: M3 V3 warm-up unexpected-failure attribution amendment](../decisions/0114-v2-m3-allocator-v3-warmup-unexpected-failure-attribution-amendment.md)
- [ADR 0115: M3 V3 diagnostic inventory sealing amendment](../decisions/0115-v2-m3-allocator-v3-diagnostic-inventory-sealing-amendment.md)
- [ADR 0116: M3 V3 diagnostic testcase identity amendment](../decisions/0116-v2-m3-allocator-v3-diagnostic-testcase-identity-amendment.md)
- [ADR 0117: M3 V3 RANGE completion reservation-handoff amendment](../decisions/0117-v2-m3-allocator-v3-range-completion-reservation-handoff-amendment.md)
- [ADR 0118: M3 V3 promotion-attachment and cutoff-attribution amendment](../decisions/0118-v2-m3-allocator-v3-promotion-attachment-and-cutoff-attribution-amendment.md)
- [ADR 0119: M3 V3 Native representative warm-up observation amendment](../decisions/0119-v2-m3-allocator-v3-native-representative-warmup-observation-amendment.md)
- [ADR 0120: M3 V3 per-actor offer-producer amendment](../decisions/0120-v2-m3-allocator-v3-per-actor-offer-producer-amendment.md)
- [ADR 0121: M3 V3 Native warm-up pre-admission observation amendment](../decisions/0121-v2-m3-allocator-v3-native-warmup-preadmission-observation-amendment.md)
- [ADR 0122: M3 V3 final-offer dispatch-precision amendment](../decisions/0122-v2-m3-allocator-v3-final-offer-dispatch-precision-amendment.md)
- [ADR 0123: M3 V3 candidate cutoff terminal-telemetry amendment](../decisions/0123-v2-m3-allocator-v3-candidate-cutoff-terminal-telemetry-amendment.md)
- [ADR 0124: M3 V3 diagnostic suite worker-isolation amendment](../decisions/0124-v2-m3-allocator-v3-diagnostic-suite-worker-isolation-amendment.md)
- [ADR 0125: M3 V4 terminal-admission drain amendment](../decisions/0125-v2-m3-allocator-v4-terminal-admission-drain-amendment.md)
- [ADR 0126: M3 V4 RANGE latency-attribution amendment](../decisions/0126-v2-m3-allocator-v4-range-latency-attribution-amendment.md)
- [ADR 0127: M3 V4 RANGE authority-proof concurrency amendment](../decisions/0127-v2-m3-allocator-v4-range-authority-proof-concurrency-amendment.md)
- [ADR 0128: M3 V4 25ms operation-attribution amendment](../decisions/0128-v2-m3-allocator-v4-25ms-operation-attribution-amendment.md)
- [ADR 0129: M3 V4 installed-RANGE proof-reuse amendment](../decisions/0129-v2-m3-allocator-v4-installed-range-proof-reuse-amendment.md)
- [ADR 0130: M3 V4 applied-mutation acknowledgement amendment](../decisions/0130-v2-m3-allocator-v4-applied-mutation-acknowledgement-amendment.md)
- [ADR 0131: M3 V4 applied-mutation instrumentation forwarding amendment](../decisions/0131-v2-m3-allocator-v4-applied-mutation-instrumentation-forwarding-amendment.md)
- [ADR 0132: M3 V4 evidence-store specialized mutation forwarding amendment](../decisions/0132-v2-m3-allocator-v4-evidence-store-specialized-mutation-forwarding-amendment.md)
- [ADR 0133: M3 V4 fixed-storm retry attribution amendment](../decisions/0133-v2-m3-allocator-v4-fixed-storm-retry-attribution-amendment.md)
- [ADR 0134: M3 V4 independent installed-RANGE reservation amendment](../decisions/0134-v2-m3-allocator-v4-independent-installed-range-reservation-amendment.md)
- [ADR 0135: M3 V4 RANGE-renewal acknowledged-proof reuse amendment](../decisions/0135-v2-m3-allocator-v4-range-renewal-acknowledged-proof-reuse-amendment.md)
- [ADR 0136: M3 V4 controlled-delay scheduler capacity amendment](../decisions/0136-v2-m3-allocator-v4-controlled-delay-scheduler-capacity-amendment.md)
- [ADR 0137: M3 V5 storm admission and diagnostic raw-integrity amendment](../decisions/0137-v2-m3-allocator-v5-storm-admission-and-diagnostic-raw-integrity-amendment.md)
- [ADR 0138: M3 V5 diagnostic candidate-outcome boundary amendment](../decisions/0138-v2-m3-allocator-v5-diagnostic-candidate-outcome-boundary-amendment.md)
- [ADR 0139: M3 V5 100k fault-attachment bound amendment](../decisions/0139-v2-m3-allocator-v5-100k-fault-attachment-bound-amendment.md)
- [ADR 0140: M3 V5 selection, child, and Final source-binding amendment](../decisions/0140-v2-m3-allocator-v5-selection-child-final-source-binding-amendment.md)
- [ADR 0141: M3 Final common-tested-source recertification amendment](../decisions/0141-v2-m3-final-common-tested-source-recertification-amendment.md)
- [ADR 0142: M3 Final documentation scenario closure amendment](../decisions/0142-v2-m3-final-documentation-scenario-closure-amendment.md)
- [ADR 0143: M3 V5 frozen-target delivery amendment](../decisions/0143-v2-m3-allocator-v5-frozen-target-delivery-amendment.md)
- [ADR 0144: M3 V5 frozen-target diagnostic inventory amendment](../decisions/0144-v2-m3-allocator-v5-frozen-target-diagnostic-inventory-amendment.md)
- [ADR 0145: M3 V5 population-construction budget alignment amendment](../decisions/0145-v2-m3-allocator-v5-population-construction-budget-alignment-amendment.md)

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
and receipt accounting/path safety. ADR 0085 closes the M1.1a start boundary, minimal independent aggregate semantics,
client-only continuity direction, exact 120-byte writer row/evidence shape, and content-identified receipt/Final
hierarchy. The later M1.1b acceptance closes the complete NTA1 FrameEncodingPolicy/legality/name/total table, and
M1.1c-R0 closes the Registry writer-count/canonical-capacity input at 14 rows and 51,016 bytes. M1-2 closes the
persisted-v1 receipt numeric caps. R1 production authority now has focused real-Oxia evidence. G1 production
receipt/Final validation is implemented under a focused non-promotion gate. Executable Fast/Exact/Final aggregation,
pure-V2 pruning, and N2/N3 promotion are complete for the current source tuple. ADR 0086 resolves the
Kafka BookKeeper semantic layout. The accepted M2-K0 contract now closes its implementation-input structure while
leaving production wire/constants/provider/gates and dedicated-ledger scale as M2 implementation/evidence.
ADR 0087 closes the protocol-frontier/ISR/idempotency/transaction/Fetch semantic layer, including fenced publication,
native election adoption, compact descriptor transport, hard-bounded Observed/Applied eligibility, and distinct
BK/Object protocol checkpoint carriers with a terminal selection Head, without selecting Java structures or numeric
queue/checkpoint/waiter bounds. M2 owns the BK carrier; M3 owns exact `NWKCP1`; M6 owns native Kafka integration.
ADR 0088 closes `V2-OPEN-OBJ-17` at the documentation/input layer by freezing the NWG1 v1 structures, caps, crypto,
golden/mutation/trace contracts, and evidence taxonomy. Later M3 descendants implement its machine projections,
immutable bytes, production codec, executable ordinary gates, and exact-source receipts. Later accepted allocator
amendments and `ae8e3f7f...` common-source recertification closed `RANGE_SELECTED(RANGE_64)`, all M3 scenario
promotions, and canonical Final `1089d4f6...` for that immutable source. ADR 0142 corrects the documentation gate's
closed M3 group and therefore requires a new exact-source recertification without changing selection semantics. ADR
0143 preserves the failed `af2f6039...` diagnostic and removes host-scheduler nondeterminism from V5 frozen-target
delivery without changing the workload, zero-drop rule, admission caps, SLOs, or selector. ADR 0144 preserves the
subsequent lossless `a981e612...` run as diagnostic-only because its old 24-test sealer omitted the two new boundary
contracts; new-source NADV5 authority now requires the exact 26-test inventory while the two prior selected sources
retain an explicit legacy verification path.
ADR 0145 preserves the subsequent exact `3b96a298...` campaign as immutable infrastructure-invalid evidence. Its
RANGE-64 100k prerequisite completed every Head and 84,118/89,424 initial grants before an older 600-second harness
cutoff, while the accepted path already charged 900 seconds. New exact-source V5 execution uses one shared
900-second charge/runtime constant without changing plan/profile bytes, workload, SLOs, candidates, qualification,
or selection. The failed 106-file/17,569,678-byte attempt is externally bound by archive identity
`27b28916...91de`. At that historical cut a fresh 100k guard, canonical diagnostic, and formal remained required; the
later e5 common-source chain completed those obligations and published the hard-frozen M3 Final identified above.
ADR 0089 amends that input before any production NWG1 bytes exist with the gap-free exact 256-byte Header table. It
retains `wireVersion=1`, removes node session and any duplicate packing-class field from the Header, fixes Object digest
`SHA-256/v1=1/1` plus close-reason codes `1..12`, and requires the future projection to mechanically transcribe the ADR.
Target/linger selection remains owned by `V2-OPEN-OBJ-19` evidence.
ADR 0091 closes the production virtual-ledger allocator wire/key/transition input with exact fixed-width
`NVAC1`/`NVAH1`/`NVAN1`, STRICT's four-write path, RANGE same-RESERVED takeover/one-ID burn, bounded versioned Oxia
keys, and receipt-only exact-source activation. Its 27 local tests and deterministic `8 x 9` schedule are not
real/native 10k/100k evidence. ADR 0140 later resolves `V2-OPEN-PUL-OBJ-09` with canonical RANGE-64 selection, and
ADR 0141 records completed selected-source recertification while retaining one common-tested-source Final chain as an
evidence gate.
ADR 0101 retains the exact-current-Cell proof while giving installed RANGE paths a shared proof phase and Cell-mutating
grant chains an exclusive measured phase; its focused/diagnostic gates cannot substitute for the complete raw matrix.
ADR 0102 retains the fixed 16-MiB JUnit cap and filters only the exact expected native-harness cleanup WARN while
preserving every other WARN and all ERROR output. The passed-test/failed-seal `1ef4f108...` matrix is diagnostic only,
selected no mode, and cannot be resealed after the runner source changed.
ADR 0103 retains the 600-second RANGE construction cap and exact Cell proof after the `bd254d24...` matrix timed out
while expanding RANGE-1024 from 10k to 100k. Its immutable diagnostic selected nothing. RANGE population construction
now runs one exclusive batch: immutable unique-Head creates drain in parallel before the unchanged index-ordered
reserve/install/clear chains begin. Exact progress is reported on timeout, but no construction counter is selection
authority. ADR 0104 now supersedes exhaustive formal execution and the one-JVM proof lock as performance authority:
the 288 logical cells remain fixed, while V2 execution is adaptive only through validator-reproved dispositions over
four independent actor coordinators. The offline `--plan-only` projection freezes the 13/17/288 execution bounds and
separate hard phase budgets. Old V1 matrices, including interrupted `full-matrix-16254510-r1`, remain immutable
diagnostics and cannot be resumed, sealed, evaluated, selected, receipted, or promoted as V2 evidence.
The pure domain V2 campaign schema/planner/validator/selector now implements the unchanged 288-cell inventory,
descending adaptive execution, native-relative rate eligibility, validator-reproved dispositions, exact
offered/admitted conservation, and the four closed evaluation outcomes. Thirteen focused tests cover the 13-cell
minimum, 17-cell minimum promotable, and 288-cell maximum paths plus caller-tamper failures. It has no Oxia access and
is implementation conformance only. The production-neutral bounded workflow is now implemented in the metadata SPI.
Each coordinator fixes request, descriptor, exact Head, and slice-view identity; retries only through exact store CAS/reread;
and fails closed on owner, slice/context, descriptor, or retry-budget drift without a shared Java lock. Eight focused
tests cover STRICT/RANGE happy paths, every response-loss stage, two independent coordinator conflict, and one-ID
consumption. The bounded four-actor Runner/harness now enforces four independent coordinators, at most one in-flight
request per lane, physical admission cutoff, exact terminal conservation, and no Java correctness lock. At exact
clean source `520838a...`, a fresh 246-test offline pre-campaign rerun and the four-test real-Oxia prerequisite pass
with zero failure/error/skip; the latter seals diagnostic-only NADV2 `4bd6d4fe...c0cc`. Historical V1 Cell-proof and
construction runs remain immutable diagnostics. None can select a mode or promote a scenario, and no full formal
campaign is currently authorized.
ADR 0107 now wires the bounded-adaptive formal entry without granting that authorization. Pure `--plan-only` freezes
288 interval, 360 single-cut fault, 32 RANGE-row scale, and 680 maximum total physical actions, a 48,000-second
process cap, stable zero-decision plan hash, and the exact source tuple without external-service access or evidence
writes. One explicit default-off Gradle task and script mode execute exactly one planned action per adapter call and
fail closed on authorization, source/runtime-lock, frozen-plan, dedicated-worktree, and new-empty-output drift. Phase A
runs no campaign and leaves allocator mode `UNSELECTED`, every M3 scenario `PLANNED`, and all evaluation, selection,
source-lock, child, scenario, and Final work open for separately authorized later phases.
The separately authorized V2 campaign subsequently completed at exact source `6c92d937...`; validator-reproved final
NACP2 `2a500526...e33c` sealed the valid non-promotable NAEV2 `10fa2033...a7e5d` as `NONE_QUALIFIED`. The immutable
119-file/461,226-byte payload is archived under
`/Users/liusinan/Documents/Codex/2026-08-27/nereus-v2-m3-allocator/bounded-adaptive-formal-6c92d937-r1-none-qualified`
with `SHA256SUMS` digest `39eb6e70...aca18e`. It produced no NARS2 and leaves allocator mode `UNSELECTED`.
[ADR 0108](../decisions/0108-v2-m3-allocator-protocol-feasibility-async-admission-baseline-amendment.md) records that
the old four-by-one runner is structurally capped at 160 requests/second for the frozen 25-millisecond row. It preserves
all thresholds and V2 bytes while requiring a distinct V3 protocol, bounded four-by-64 asynchronous admission,
native-baseline-unavailable evaluation, exact derived-rate slots, offline feasibility proof, and diagnostic-only
investigation before any later formal authorization.
The accepted V3 implementation freezes zero-decision plan digest `019fcac7...35e9`: 328 logical/interval slots,
360 fault actions, 32 scale actions, 720 maximum total actions, and a 34,260-second budget sum inside the unchanged
48,000-second cap. Exact source `baae2625...326` completed the first three-layer diagnostic-only run with 13/0/0/0.
Native and direct real-Oxia rows did not identify the runner scheduler or Oxia RTT as the primary limiter; bounded
STRICT and smaller-RANGE workflow rows instead exposed CAS/reconcile contention, while installed RANGE-64 sustained
the strongest short row. These outputs have `authority=false` and `selectionEligible=false`; they are not NAEV3,
NARS3, formal campaign evidence, or permission to optimize by weakening a contract.

The first V3 formal campaign later completed at exact source `4bf51a38...e360` with 36 actions and 37 checkpoints.
Its canonical NAEV3 `37cb5e2c...1e09d` is the legal non-promotable state `NATIVE_BASELINE_UNAVAILABLE`; no NARS3
exists and allocator mode remains `UNSELECTED`. [ADR 0109](../decisions/0109-v2-m3-native-baseline-executor-composition-formal-diagnostic-equivalence-amendment.md)
preserves that result and corrects the Native executor composition: formal and diagnostic use one source-bound true
async ManagedLedger runtime with no admission-after hidden queue. Stage B.2 is diagnostic-only and grants no formal
rerun, source-lock, child, current-source M2, scenario, or Final update.
The amended source-bound schedule/profile identities are `b0e923a0...e798` and `4b11530b...d751`, yielding plan
digest `5f94079e...b283` without changing the V3 logical action inventory or any qualification rule. Its NADV3 gate
seals and parse-canonically revalidates all six diagnostic suites and, after ADR 0117, 18 exact testcase identities.
The [Stage B.2 current-source recertification record](detailed_design/m3/stage-b2-native-executor-current-source-recertification.md)
binds the same accepted profile to a new exact clean pushed source through diagnostic-only canary and NADV3 replay.
It expressly creates no formal authorization, selection, production source-lock, child, M2, scenario, or Final input.

The next authorized attempt at exact source `e60327ae...` established all eight Native baselines at 1000 requests per
second, then failed infrastructure classification after its first STRICT row because bounded typed warm-up rejection
was treated as a launcher/runtime failure. [ADR 0110](../decisions/0110-v2-m3-allocator-candidate-warmup-load-rejection-classification-amendment.md)
preserves that 20-file attempt and its external archive, partitions warm-up failures into explicit load rejection and
unexpected failure, and lets only the former reach the unchanged measured validator and adaptive rate descent. Every
warm-up counter is emitted in the action attachment; measured zero-drop/failure/timeout qualification is unchanged.

The following exact-source attempt at `c0e28f8e...` reached that deterministic descent: eight Native rows established
1000 requests per second and the failed fixed STRICT row resolved its next action to the distinct derived-800 slot.
[ADR 0111](../decisions/0111-v2-m3-allocator-derived-floor-physical-budget-projection-amendment.md) preserves the
21-file failed attempt and corrects the physical budget projection so only FIXED ordinal zero can receive first-action
setup charges. The derived logical identity, resolved rate, frozen plan digest, action budgets, and qualification rules
remain unchanged.

The next exact-source attempt at `1771b000...` passed the same Native baseline inventory and consumed the failed fixed
STRICT row, then exposed one final version-entry mismatch before dispatching derived 800: the V3 candidate runtime
still called the ADR-0094 V1/V2 fixed-rate-only workload entry. [ADR 0112](../decisions/0112-v2-m3-allocator-v3-derived-rate-workload-entry-amendment.md)
preserves the 21-file failed attempt and its external archive. V1/V2 remain closed to their six fixed rates; explicit
V3 workload and arrival entries admit only the fixed rates plus exact reconstructible 800/600/400/267 values. The
schedule/profile/plan digests, jitter bytes, thresholds, budgets, and selection rules remain unchanged.

The subsequent exact-source attempt at `ba7e313f...` executed derived 800 and then completed the RANGE-16 scale and
fixed-rate interval before the first following fault action paired a post-interval exact Head with the formal
population's stale pre-interval Cell snapshot. [ADR 0113](../decisions/0113-v2-m3-allocator-v3-completed-workflow-cell-reconciliation-amendment.md)
preserves the 26-file failed attempt and external archive. The V3 completion callback now monotonically reconciles
the workflow's returned exact Cell before handing its exact Head to later fault/scale helpers. Oxia CAS/reread remains
the production correctness authority; no cross-actor correctness lock, workload, SLO, budget, or selection change is
introduced.

The next exact-source attempt at `ee335a8c...` completed all Native baselines and a validator-consumable failed
STRICT fixed-1000 row, then stopped on derived 800 because one of 937 admitted warm-up failures was unexpected while
the other 936 were exact typed load rejections. [ADR 0114](../decisions/0114-v2-m3-allocator-v3-warmup-unexpected-failure-attribution-amendment.md)
preserves the 22-file failed attempt and external archive. The async runner now retains the first unexpected failure
separately from the first failure overall, and the current-source diagnostic inventory replays the exact consecutive
fixed-1000 and derived-800 formal schedules on one real-Oxia population. This changes no attachment bytes,
classification, workload, retry bound, threshold, SLO, budget, disposition, or selection rule.

The first clean-source ADR-0114 diagnostic at `372ca975...` completed all six suites at 17/0/0/0, including the
formal-equivalent STRICT sequence, but the closed NADV3 allowlist still named the pre-ADR-0114 five-suite/16-test
inventory. [ADR 0115](../decisions/0115-v2-m3-allocator-v3-diagnostic-inventory-sealing-amendment.md) preserves the
unsealed diagnostic and corrects only the exact suite/test allowlist. It changes no diagnostic wire bytes, workload,
formal planner, qualification, or selection semantics.

The next clean-source diagnostic at `bc867579...` again completed 17/0/0/0, and the sealer accepted all six suite
files before rejecting the newly listed testcase name: ADR 0115 had named an intended description rather than the
actual JUnit method identity. [ADR 0116](../decisions/0116-v2-m3-allocator-v3-diagnostic-testcase-identity-amendment.md)
preserves that second unsealed diagnostic and binds the exact emitted method name. It adds an explicit wrong-name
negative contract without changing any evidence byte or allocator rule.

The following exact-source formal attempt at `b9659232...` completed all 30,000 measured RANGE-16 fixed-1000
requests but stopped fail-closed because 26 warm-up callbacks returned an exact snapshot containing another
in-flight request's RANGE reservation. [ADR 0117](../decisions/0117-v2-m3-allocator-v3-range-completion-reservation-handoff-amendment.md)
preserves the 25-file failed attempt and external archive. The harness now ignores a validated transient reserved
completion snapshot for its reservation-free population proof and still requires a later cleared completion to
advance cursor/grant state. The current-source NADV3 inventory becomes six suites/18 tests by adding the exact
RANGE-16 10k/1ms fixed-1000 formal schedule; production CAS/reread authority and every frozen campaign rule remain
unchanged.

The subsequent exact-source campaign at `d22a693a...` completed with 44 physical actions and 23 checkpoints. Final
NACP3 `f7e44c95...1fbb` sealed canonical NAEV3 `0b0f3953...5726` as valid non-promotable `NONE_QUALIFIED`; no NARS3
exists and allocator mode remains `UNSELECTED`. [ADR 0118](../decisions/0118-v2-m3-allocator-v3-promotion-attachment-and-cutoff-attribution-amendment.md)
preserves that terminal and corrects two independent verification identities: current-source NADV3 binds the full
zero-decision plan digest, and the promotion CLI reconstructs each logical execution-record digest from all exact
physical interval, scale, and ordered fault files. A separate exact-schedule RANGE cutoff diagnostic attributes the
small pre-admission drop boundary without changing cutoff, workload, SLO, or selection semantics. This is not formal
rerun authorization.
The exact `436bca8b...` cutoff diagnostic completed 1/0/0/0: fixed-1000 was 30,000/30,000 with zero drop, while
derived-800 retained 21 `PRE_ADMISSION_CUTOFF` outcomes among 24,000 offers. The first dropped offer fired with only
62 microseconds lag, so the source makes no runner or workflow change from this diagnostic.
[ADR 0119](../decisions/0119-v2-m3-allocator-v3-native-representative-warmup-observation-amendment.md) preserves a
later failed full-diagnostic attempt where a 500-rps observational row completed all 15,000 measured offers but had
one warm-up pre-admission drop. The eight 200-rps baseline gates remain unchanged; representative warm-up drops stay
visible telemetry rather than becoming an unaccepted ninth qualification threshold.
[ADR 0120](../decisions/0120-v2-m3-allocator-v3-per-actor-offer-producer-amendment.md) preserves a later hard-gate
failure where the final 10k/25ms/200 request was offered on time but missed dispatch before cutoff. Four persistent
per-actor offer producers replace the remaining global serialized offer coordinator while retaining the same frozen
arrival bytes, barriered phase transition, bounded admission, physical cutoff, and drop classification.
[ADR 0121](../decisions/0121-v2-m3-allocator-v3-native-warmup-preadmission-observation-amendment.md) closes the
remaining schema/gate mismatch: all Native rows retain `warmupDropped` as separate raw telemetry, while the eight
baseline rows continue to require measured drop/failure/timeout=0 and complete drain. The exact `7dcab4be...` run
proved ADR 0120's 25ms/200 measured correction before stopping on that non-measured assertion.
[ADR 0122](../decisions/0122-v2-m3-allocator-v3-final-offer-dispatch-precision-amendment.md) preserves a later 5ms/200
failure where the final two offers woke about 5.4ms late. The existing per-actor producers now use a bounded final
50ms precision window and an immediate async-dispatch fast path only when the unchanged queue and permits allow it;
late/full/busy requests still receive the same pre-admission drop.
[ADR 0123](../decisions/0123-v2-m3-allocator-v3-candidate-cutoff-terminal-telemetry-amendment.md) preserves the later
`c1ba429b...` full diagnostic at 18/1/0/0 after its Native rows passed but RANGE-16 fixed-1000 retained 15
pre-admission requests. The independent cutoff run showed first-drop scheduler lag zero, so diagnostic attachments now
retain binding identity, queue wait, and rollover p99 before the unchanged zero-drop assertion. This is observability,
not a cutoff, qualification, workload, or formal-authorization change.
[ADR 0124](../decisions/0124-v2-m3-allocator-v3-diagnostic-suite-worker-isolation-amendment.md) preserves the following
`704056b7...` full diagnostic at 18/1/0/0: its eight Native baselines passed, but five final offers in the first 500-rps
representative row fired late after unrelated RANGE/STRICT/workflow classes had run in the same worker. The full task
remains serial and one canonical inventory but now forks once per test class; the Native class still runs all ten rows
in one shared runtime and formal execution is unchanged.
[ADR 0125](../decisions/0125-v2-m3-allocator-v4-terminal-admission-drain-amendment.md) preserves the later exact
`0cc962e9...` V3 `NONE_QUALIFIED` result and corrects a protocol-level terminal-censoring defect only in V4. The exact
derived-800 schedule repeats binding 9730 after 23.875ms and offers the successor only 25ms before cutoff; scheduler
lag was zero and rollover p99 was 132.270ms. V4 keeps every request, zero-drop rule, SLO, queue/outstanding cap, and
per-binding single-flight, but closes offers at 40s and gives already-offered work the existing two-second starvation
bound to reach admission before the unchanged drop partition is finalized. Distinct NACP4/NAEV4/NARS4/NADV4 and a
new source-bound plan prevent any reinterpretation of V3 evidence.
The ADR-0125 V4 diagnostic source gate was independently versioned as 21 tests across eight exact JUnit suites, comprising the
unchanged V3 18-test inventory plus two V4 runner drain contracts and one real-Oxia RANGE-16 fixed/derived drain test.
Only the V4 seal/validate tasks may turn that inventory into NADV4; it remains diagnostic-only and cannot select a
mode.
The accepted V4 source now has a separate pure plan, process-supervised launcher, default-off Gradle formal task, and
offline NACP4/NAEV4/promotion/NARS4 commands. Both launcher and task bind clean pushed source, V4 plan/profile,
locks/worktrees/JAR/image/executor, canonical NADV4 with its current exact JUnit inventory, and a create-new output
directory. The shared Native canary emits V4 row schemas only when the V4 drain runtime is selected; V3 keeps its
prior row bytes. This wiring is not a formal result or allocator selection.

[ADR 0126](../decisions/0126-v2-m3-allocator-v4-range-latency-attribution-amendment.md) freezes the exact
`c44a56c2...-r1` V4 result as `NONE_QUALIFIED`. Its NACP4/NAEV4/promotion chain and 99 physical attachments validate,
but every RANGE is eliminated by 10k/10ms or earlier; RANGE-1024 derived-800 drops 1,335 on-time requests before
admission while draining all admitted work. Its 135-file, 20,556,200-byte payload is preserved under manifest
`ab829692...39db3`. Current-source NADV4 adds one formal-equivalent RANGE-1024 10ms attribution suite and is exactly
22 tests/nine suites. This adds telemetry only and does not modify any V4 threshold, plan, evidence, or selection
semantic.

[ADR 0127](../decisions/0127-v2-m3-allocator-v4-range-authority-proof-concurrency-amendment.md) binds the resulting
`c4f442ea` diagnostic failure and attribution. The task produced no NADV4 because a separate runner-only test used a
non-deterministic 20ms admission window, but its RANGE-1024 row validly measured 1,312 derived-800 drops, 256 real
operations outstanding, ten metadata operations per request, and 171.980ms workflow p99. The production correction
keeps every exact read and mutation while dispatching three independent authority-proof pairs concurrently. It does
not alter the V4 plan, workload, qualification, or selection contract.

[ADR 0128](../decisions/0128-v2-m3-allocator-v4-25ms-operation-attribution-amendment.md) preserves the resulting exact
`83193069...-r1` V4 `NONE_QUALIFIED` campaign. RANGE-1024 now clears 10ms but its 25ms derived-800 action drops 6,402
offers and reports operation/workflow p99 near 267ms. The current-source diagnostic adds an exact 25ms fixed/derived
sequence, making NADV4 23 tests in the same nine suites, before any delay-scheduler or workflow correction is allowed.
No V4 protocol, threshold, plan, selection rule, or historical evidence changes.

[ADR 0129](../decisions/0129-v2-m3-allocator-v4-installed-range-proof-reuse-amendment.md) binds the exact
`d434f910...` 25ms attribution: real RTT and one-thread scheduler lag are secondary, while the seven-stage installed
RANGE proof chain drives 289–565ms workflow p99. Only the workflow-owned store-observed steady-state path reuses its
exact Cell/Head and create-reread node, reducing ten operations/seven stages to six/five. Mutation same-key rereads,
public allocator proof reads, conflict/fault/renewal paths, V4 plan, thresholds, and historical evidence are preserved.

[ADR 0130](../decisions/0130-v2-m3-allocator-v4-applied-mutation-acknowledgement-amendment.md) preserves the resulting
exact `ad9dce4f...` diagnostic and its seven-file archive. Six operations/five stages still drop 2,882 derived-800
offers at 25ms despite zero retry/failure/timeout. The pinned Oxia success result already binds the committed key and
version, so only the store-observed installed-RANGE path may use it as the exact mutation snapshot. Missing/failed or
conflicting responses retain same-key reread; public/STRICT/renewal/fault paths and every V4 evidence threshold remain
unchanged.

[ADR 0131](../decisions/0131-v2-m3-allocator-v4-applied-mutation-instrumentation-forwarding-amendment.md) preserves
the exact `3bc11088...` diagnostic and identifies why it still measured six operations: the shared instrumented client
fell back to interface-default legacy mutations and discarded the acknowledgement. Formal and diagnostic wrappers now
forward the exact result through latency/loss/crash instrumentation. Production semantics and every V4 threshold stay
unchanged.

[ADR 0132](../decisions/0132-v2-m3-allocator-v4-evidence-store-specialized-mutation-forwarding-amendment.md)
preserves the exact `e53b3af8...` diagnostic: acknowledgements reached the wrappers and eliminated reconcile retries,
but the outer evidence-store decorator inherited SPI defaults and re-entered the ordinary mutation path. The decorator
now forwards both installed-RANGE specialized methods through the same exact-key and fault telemetry helper; production
semantics, V4 plan, and every threshold stay unchanged.

[ADR 0133](../decisions/0133-v2-m3-allocator-v4-fixed-storm-retry-attribution-amendment.md) preserves the exact
`792c77de...` diagnostic. Derived-800 now passes with four operations and zero drop, but fixed-1000 still drops 1,999
offers and reports 25,780 retries. A diagnostic-only closed retry-reason inventory now binds those retries before the
next proof-preserving correction; formal bytes and all V4 thresholds remain unchanged.

[ADR 0134](../decisions/0134-v2-m3-allocator-v4-independent-installed-range-reservation-amendment.md) preserves the
exact `026dfddf...` diagnostic and attributes 25,814 of 25,890 fixed-row retries to a foreign Cell reservation. The
bounded workflow now permits an exact Head to consume its own installed RANGE grant before interpreting another
Head's grant-renewal reservation; Heads without a usable grant retain the same bounded reservation path. V4 protocol,
plan, workload, thresholds, and evidence bytes remain unchanged.

[ADR 0135](../decisions/0135-v2-m3-allocator-v4-range-renewal-acknowledged-proof-reuse-amendment.md) preserves the
exact `9fcbc7f2...` diagnostic: fixed drop falls to 156, but acknowledged grant install/clear still reread exact
authorities and lengthen the global reservation. The bounded success path now reuses those exact mutation results;
all uncertain/conflicting outcomes return to the original proof reads. Public API, STRICT, V4 plan, thresholds, and
evidence semantics remain unchanged.

[ADR 0136](../decisions/0136-v2-m3-allocator-v4-controlled-delay-scheduler-capacity-amendment.md) preserves the exact
`e50c455e...` diagnostic: fixed drop falls to three and derived-800 remains zero-drop, while controlled-delay
scheduler firing lag reaches 11.323ms p99 despite 7.773ms real RTT and 102us callback lag. The shared formal/diagnostic
latency injector now owns four bounded timer workers per actor and reports that source-governed value. This changes no
production allocator authority, V4 plan/profile, workload, threshold, or evidence semantics.

[ADR 0137](../decisions/0137-v2-m3-allocator-v5-storm-admission-and-diagnostic-raw-integrity-amendment.md) preserves
the standalone lossless 25ms result and the later full-suite V4 zero-drop failure, fixes raw diagnostic enforcement,
and versions the already-frozen 2R storm admission contract as bounded `4/128/512/1`. The implementation record is
[Stage B V5 storm admission and diagnostic raw integrity](detailed_design/m3/stage-b-v5-storm-admission-and-diagnostic-raw-integrity.md).
V5 now has strict source-bound plan/profile/codec/launcher entrypoints and a canonical 19-file diagnostic raw
manifest. A fresh 26-test/ten-suite current-source diagnostic and its NADV5 must pass before the launcher can create a
formal output; no V5 formal evaluation or selection exists at this cut.

[ADR 0138](../decisions/0138-v2-m3-allocator-v5-diagnostic-candidate-outcome-boundary-amendment.md) preserves the
exact `a1664de9...` FAILED diagnostic and separates candidate qualification from diagnostic infrastructure validity.
STRICT and RANGE-16 compatibility rows now use the exact V5 admission/drain factory; STRICT raw bytes must conserve
every terminal and drain fully, but its loss remains a formal candidate outcome rather than a pre-campaign failure.
RANGE-16, RANGE-1024, Native, and terminal-drain lossless gates remain unchanged. No threshold, SLO, plan, selection
rule, source lock, or production mode changes.

[ADR 0139](../decisions/0139-v2-m3-allocator-v5-100k-fault-attachment-bound-amendment.md) preserves the first complete
V5 formal at exact `8a60d931...`. Its canonical NAEV5 reports RANGE-64, but the subsequent promotion gate rejected a
valid 27,203,520-byte 100k mass-takeover attachment at the inherited V3 16 MiB file cap before writing any promotion
decision or NARS5. The immutable promotion-invalid archive binds all 158 formal files and the exact JUnit. V3/V4 keep
16 MiB; V5 alone uses a tested 32 MiB closed physical-attachment cap. A fresh exact-source diagnostic and formal run
remained required at that source boundary.

[ADR 0140](../decisions/0140-v2-m3-allocator-v5-selection-child-final-source-binding-amendment.md) records the fresh
exact `d5b3569b...` 24-test/ten-suite NADV5 and completed formal campaign. Independent replay verifies 22 intervals,
ten fault rows, 306 dispositions, 123 physical files, canonical `RANGE_SELECTED(RANGE_64)` NAEV5, a promotable
decision, and canonical NARS5. The selected archive binds 160 files and remains immutable. The production source
lock therefore moves to `RANGE`, while the V5 governed wrapper and `ALLOCATOR_SELECTION` child profile require a
fresh diagnostic/formal run at the new exact selected source before any downstream child, scenario, or Final can be
fresh. See [Stage B V5 selection, child, and Final source binding](detailed_design/m3/stage-b-v5-selection-child-final-source-binding.md).

[ADR 0141](../decisions/0141-v2-m3-final-common-tested-source-recertification-amendment.md) records that exact
`54d0ca7c...` then completed the selected-source V5 diagnostic/formal, reproduced canonical RANGE-64 selection, and
published its source-bound allocator child; a subsequent intermediate W1 run passed 25 children and 688 tests. Those
immutable receipts remain valid at their own sources. The remaining gate is a fresh diagnostic/formal, W1, all ten
children, scenarios, and Final at one common tested source after the last non-evidence documentation change.

M2-P6 closes `V2-OPEN-BK-11/13`: the selected NPD1 hard envelope is 4 GiB/1,024 parts/64-MiB entry and decoded
block/65,536 entries per block; the typed catalog is 1/4/8 MiB with 4 MiB as the Deployment base default. LocalStack,
fixed MinIO, and pinned-native receipts preserve their provider/benchmark claim boundaries.
Partial recovery omission, remaining numeric caps,
and any tombstone-deletion authority remain evidence gates. The rows below
are the remaining active 0.2 evidence gates.

| Gate | Required decision/evidence | Must close before |
| --- | --- | --- |
| `V2-OPEN-OBJ-19` | execute evidence and select target/linger/quantized values and numeric budgets without changing ADR-0062 class semantics | M3 Object WAL policy freeze |
| `V2-OPEN-PUL-OBJ-09` | execute current-source real multi-broker/native 10k/100k evidence against ADR 0091, prove all reservation/head/node/concurrency/takeover/cut SLOs with zero skip, select an exact RANGE size if eligible, and select at most one allocator mode | M3 virtual-ledger allocator activation |
| `V2-OPEN-OBJ-22` | execute bounded recovery and skip-hit evidence; only an SLO miss may reopen a whole-WalRun-first, Root/Seal-bound recovery omission certificate | M3/M7 recovery optimization decision |
| `V2-OPEN-OBJ-24` | admit a Provider version token only after canonical-binary cap, immutable-version, FULL_OBJECT SHA-256, rows/page, and range-benefit evidence; otherwise retain Root mode NONE | M3 checkpoint provider-proof admission |
| `V2-OPEN-READ-15` | execute M4/M5 tombstone lifetime/capacity and concrete-backend ordered-history/lineage/stale-create evidence before reconsidering a metadata-only tombstone-deletion authority; 0.2 otherwise retains `RETIRED_V1` permanently | M4/M5 optional metadata-retirement authority |
| `V2-OPEN-BK-02` | implement M2-K0's exact NBKE2/index/footer/checkpoint bytes, hard parser/admission caps, module/provider surfaces, and non-promotable input gate; then use M2-K9 evidence to select apply-lag/pipeline/recovery/waiter/cursor/rollover defaults and validate the accepted one-ledger-chain-per-partition layout at 10k/100k partitions; exact Object `NWKCP1` bytes/Head/key caps remain M3, and pooled lanes or storage-native ISR require a new ADR | M2 Kafka BookKeeper implementation and evidence admission |
| `V2-OPEN-KAF-DATA-01` | select an explicit evidence-backed initial profile for `__share_group_state`; it cannot inherit a tenant default, while `__consumer_offsets` and `__transaction_state` are already fixed to `BOOKKEEPER_WAL_ONLY` | M6/release internal-topic admission |
| `V2-OPEN-BENCH-01` | pin clean AutoMQ and native Pulsar acceptance baselines plus thresholds | M8 performance execution |

`V2-OPEN-MIGRATION-02..03`, `V2-OPEN-PUL-MIGRATION-01`, and `V2-OPEN-PROJECTION-01..03` remain recorded as deferred
future-design questions. ADRs 0015 and 0016 make them non-blocking for 0.2.

## Maintenance rule

Every V2 milestone change must update the normative contract, affected ADR/context language, tradeoff/open-question
status, scenario manifest, and gate in the same coherent change as the implementation. Confirmed decisions move out of
the non-normative question log; unconfirmed proposals never become contracts by repetition. Evidence is append-only and
source-qualified; an older PASS never becomes current-source evidence automatically.
