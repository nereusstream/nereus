---
productLine: V2
designStatus: Proposed
implementationStatus: InProgress
evidenceStatus: CurrentSourceReceipt
authority: NonNormativeQuestionLog
sourceTuple: v2-m1
---

# V2 open questions

This file records proposals that have not been accepted as runtime contracts and retains resolved gate IDs for
traceability. An answer moves into a normative document or ADR only after explicit confirmation; editing this file
alone cannot close a gate.

## M1 implementation-readiness grill: current frontier

Round 1 accepted ADR 0081's pure V2 active graph, module/gate, milestone, and cross-repository promotion boundaries.
Round 2 accepted ADR 0082's 16/32-byte identity boundary and exact NTB1/NSE1 outer preimages, strict NTA1 direction,
exact four-capability M1 SPI and closed mutation outcomes, Kafka input-only CreateTopics authority, Pulsar authoritative
ownership witness plus stale-install-safe atomic fence, and compatibility-namespace Registry with complete writer-set
interlock and distinct Registry/harness receipts. The complete adjusted answers are preserved in
[round 1](grill-notes/22-m1-readiness-round-1-pure-graph-and-promotion.md) and
[round 2](grill-notes/23-m1-readiness-round-2-domain-control-authorities.md).

Round 3 accepted ADR 0083's NPC1/NTI1 layouts, flat/no-tail NTA1 direction, one Kafka profile pseudo-config plus
classifier-v1 and residue-free linear admission, the Oxia-backed MetadataStore ELM adapter-candidate boundary with restart-new
acquisition, INSTANCEID-derived fresh-only Registry identity with inline writer membership, and one canonical virtual-
ledger receipt envelope. Its complete adjusted answer is preserved in
[round 3](grill-notes/24-m1-readiness-round-3-wire-control-and-evidence.md).

Round 4 accepted ADR 0084's exact protocol codes and NPN1 Pulsar authority leaves, Kafka duplicate/validation/policy
precedence plus remote-log fail-closed admission, one local store-wide watch-continuity epoch, canonical UUID/NLI1
compatibility identity, and one receipt result hierarchy with safe attachment grammar. The dedicated design-review
answer is preserved in
[round 4](grill-notes/25-m1-readiness-round-4-leaf-witness-registry-and-receipt.md).

Round 5 accepted ADR 0085's M1.1a start boundary: NTA1 persists only independent semantics; Oxia continuity reuses the
existing v0.9 dummy notification barrier through a client-only hook; Registry writers use two closed kinds, a fixed
120-byte row, and immutable proof-only admission evidence; receipts use canonical content identity, five attachment
kinds, and one non-authoritative Final manifest. It explicitly did not accept complete NTA1, `maxWriterCount=8`, or any
proposed receipt numeric cap. The dedicated design-review answer is preserved in
[round 5](grill-notes/26-m1-readiness-round-5-foundation-start-and-deferred-codecs.md).

The Nereus-local M1.1a-A module/identity/deterministic-ID/SPI/dependency foundation is implemented and locally gated.
The Oxia O1 client-continuity target has a pushed final fork and focused evidence. Metadata-oxia O2 is also locally
verified at Nereus `050f908a`: it consumes the immutable O1 client/API bundle, implements four single-key adapters and
store-wide continuity invalidation, and binds 69 focused plus 299 whole-module tests in a non-promotable receipt. Those
early slices did not claim complete NTA1, Registry production authority, receipt validation, M1 Final, or promotion
evidence. NTA1, K1, P1, and focused R1 are now implemented under narrower non-promotable receipts; the remaining
descendants and recently closed evidence input are:

- **Oxia O1 focused implementation complete:** the confirmed 2026-08-12 source audit uses Java client
  `24b730d1d66a1da701f4c99957361f6b3c5d748c` plus server
  `37a17bef17202d5fd6e23282da5fd26d94865484` after an exact dummy/payload compatibility probe. The
  [accepted O1 design](detailed_design/m1/m1.1a-oxia-client-continuity.md) selects a public immutable snapshot,
  per-generation ready stage, listener, and registration handle. Its source-seam review also requires internal
  assignment-stream loss/restoration publication and one cancelable notification attempt so no opaque retry can
  precede loss. Client final fork `091a42c2780d92da56e9ec1f02ce1c3d988adc16`, artifact hashes, exact server image,
  and focused compatibility receipt are bound separately in schema-v2 source locks. This closes only O1 execution;
  it does not promote a scenario. O2 subsequently consumed this exact fork without modifying it.
- **Oxia O2 local scaffold complete:** the [accepted design](detailed_design/m1/m1.1a-oxia-capability-scaffold.md)
  binds Nereus implementation `050f908a`, the complete O1 client/client-api bundle manifest, exact key/version and
  conditional-reread adapters, continuity race cuts, 69 focused tests, and 299 whole-module V1-residue compatibility
  tests. Its result is `PASS_LOCAL_SCAFFOLD_ONLY`, not real Oxia/Pulsar conformance, and `promotionEligible=false`.
- **M1.1b exact-local implementation complete; promotion evidence pending:** the
  [accepted design](detailed_design/m1/m1.1b-nta1-codec.md) freezes `NONE={0,0,empty}` and
  `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`, the six legal rows, classic `persistent://` only, 4,096 strict UTF-8 bytes per
  Pulsar canonical name, and exact checked `maxCellBytes/maxIncarnationBytes/maxNta1Bytes=54/8,214/8,397`. It rejects
  the 12.5-percent, 16-KiB-name, rounded-total, and scalable-domain alternatives for v1. No current customer inventory
  blocks 0.2; future existing-cluster import requires a qualified pure-input name inventory, while a fresh deployment
  does not. The [Q1 receipt](evidence/v2-m0/m1.1b-q1/README.md) remains historical
  `READINESS_EVIDENCE_ONLY`. Production codec/goldens, pure-input admission, and O2 aggregate integration are complete
  at `01a70f17` with 55 domain, 73 focused O2, and 303 whole metadata-oxia tests in a
  [non-promotable receipt](evidence/v2-m0/m1.1b/README.md). Its historical boundary remains unchanged; later K1/P1/R1
  receipts do not promote it. A real existing-cluster inventory is deferred migration evidence, not a fresh-only 0.2
  or M1-Final blocker.
- **Registry capacity, R1 authority, and exact Registry promotion complete:** the unsupported `maxWriterCount=8`
  candidate is rejected.
  The accepted [M1.1c-R0 spike design](detailed_design/m1/m1.1c-registry-capacity-spike.md), 18 focused tests, and
  `v2M1RegistryCapacityCheck` bind seven source-qualified/principal-generation cohorts per closed writer kind,
  `maxWriterCount=14`, the exact `184 + writerCount * 120 + sum(assignmentRowCanonicalBytes)` formula, and a
  51,016-byte largest legal canonical Registry value. The inherited 120/192/256/65,536 limits remain unchanged, with
  14,520 bytes reserved margin. Production NLI1/NVR1/NVA1/RAE1, Store/interlock, response loss, derived views, and two
  source-locked real-Oxia tests are now covered by `v2M1R1FocusedCheck` and the
  [non-promotable R1 receipt](evidence/v2-m1/r1/README.md). Canonical N3 `REGISTRY_CONFORMANCE` now promotes exactly
  `V2-POSITION-003..009`; allocator mode selection remains a later evidence decision.
- **Receipt-cap input, G1 validator, and N3 Final resolved:** ADR 0084 now owns the sole persisted-v1 cap table. The M1-2
  JDK-only test model, 36 clean focused tests, eleven representative sample families, and
  [`RECEIPT_CAPACITY_READINESS_ONLY`](evidence/v2-m0/m1-2-receipt-caps/README.md) evidence bind the formulas,
  observed maxima, stable rejection taxonomy, source commit, and JSON SHA. Deployment may only lower new-receipt
  admission; host/provider limits cannot change persisted-v1 parsing. This closes the stale numeric-cap OPEN, not the
  separately reviewed G1 production parser/resolver, now covered by its [focused receipt](evidence/v2-m1/g1/README.md),
  or the trusted N3 promotion now represented by canonical receipts and the Final index. Final freshness is executable:
  the receipt-bound Nereus commit must be a strict ancestor of a clean checkout, every intervening commit must be
  single-parent and N3-evidence-only, and the receipt source-lock digest must equal the current file bytes.
- **Promotion evidence is not a prose OPEN:** O1 binds the final Oxia client fork, JAR/source-JAR/POM, exact server
  image, and focused-test identities. Executable continuity/Registry conformance and trusted N3 receipts are represented
  only by the canonical evidence objects and scenario manifest, not by another design decision.
- **Current promotion execution state:** as of 2026-08-13 the repository has a `v2-m1-promotion` Environment with a
  required reviewer and a `main`-only deployment policy. The dedicated macOS ARM64 runner is online with the
  `nereus-v2-m1` label and the source-qualified Oxia image. The protected workflow has completed a successful exact-head
  run that regenerated and byte-compared all seven gate/report/receipt/Final files before `v2M1FinalCheck`. Current M1
  completion remains valid only while that successful run, the canonical N3 evidence, and `main` name the same head;
  Environment/runner configuration alone is not promotion evidence or an allocator/runtime architecture OPEN.

O1, O2, R0, M1-2, N1, and focused K1/P1/R1 are governed by their accepted implementation designs and non-promotable
receipts. K1 is exact-source complete at Kafka `8afbc42566`; P1 preserves its focused receipt at Pulsar `778862323d`
and locks final N2 execution to pure-V2 Pulsar `072aa1c440`; R1 preserves focused provenance at Nereus `8a213a85bf`
and locks current exact-source execution to `42598fe633`;
G1 parser/Final mechanics are focused-current complete at
`ba11fe4a29`. None promotes a scenario or M1. The pure-V2 active graph and independent V1/KoP mechanical deletion are
complete. N2/N3 completion is derived from their required gate, receipt, Final-index, and scenario evidence rather than
tracked as an architectural OPEN here.

## Restarted Grill 2: evidence frontier

Round 18 accepted the small bounded inline selector-owned anchor set, dedicated emergency STOPPED envelope, immutable-
candidate closed-verifier terminal safety, and asynchronous batched prune in ADR 0079. It also accepted ADR 0080's
irreversible same-key `FULL_V1 -> RETIRED_V1` compaction and permanent 0.2 compact tombstone. It explicitly did not
accept tombstone deletion or a retired-through/frontier authority.

The pure-document M0 decision frontier for this branch is exhausted. The remaining descendants require evidence:

| Gate | Evidence required before another design decision |
| --- | --- | --- |
| `V2-OPEN-READ-08` | M4 proof-window/head/fold representation, terminal-row retirement, response-loss, capacity, and throughput receipt |
| `V2-OPEN-READ-09` | M4/M5 capability/receipt encoding, verifier lifetime/revocation, and admitted backend-generation receipt |
| `V2-OPEN-READ-15` | M4/M5 tombstone lifetime/capacity plus a concrete backend's gap-free activation history, monotonic conditional authority, lineage, stale-create, and recovery receipt before any deletion authority is reconsidered |

The adjusted response is preserved in
[round 18](grill-notes/20-restarted-grill-2-anchor-terminal-and-batch-metadata-retirement.md); the no-question evidence
handoff is recorded in [round 19](grill-notes/21-restarted-grill-2-round-19-evidence-frontier.md). The grill must not
invent another prose-only authority while these facts are absent. `V2-OPEN-OBJ-22/24`, remaining
`V2-OPEN-OBJ-17/19`, and `V2-OPEN-PUL-OBJ-09` are likewise evidence-blocked.

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
ring/window advances only one binding, stores no payload/persisted gap state, and performs cached O(1) owner fencing.
ADR 0066 refines takeover recovery to bounded collect/sort plus fresh tickets rather than a long-lived ordered map.
Shared Object/header/directory failures block all members, while later frame/commit-set-local failures isolate to that
complete binding unit.

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

M3 owner-open does not reopen an admitting Root: [ADR 0096](../decisions/0096-v2-m3-owner-open-conservative-rollover-amendment.md)
requires a terminal, conservative rollover cut. A future same-Root resume remains open until durable fenced pending
dispatch wire is separately accepted and evidenced.

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

### `V2-OPEN-OBJ-17`: resolved exact NWG1 cryptographic framing

Resolved by [ADR 0088](../decisions/0088-v2-m3-nwg1-implementation-input-closure.md), its accepted
[ADR 0089 Header amendment](../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and the
[M3-I0 input closure](detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md). NWG1 v1 now has an exact
256-byte Header, 32-byte Directory preamble, 116-byte Binding row, 104/96-byte Kafka/Pulsar AppendUnit rows, 48-byte
Frame row, 37-byte HKDF info, 12-byte nonce, 272/328-byte AAD, closed algorithms, 4-MiB prefix and 4-GiB body/decoded
aggregate format ceilings, canonical order/equations, and strict verifier precedence. Production projection/goldens,
codec, mutation/trace runners, Provider evidence and receipts remain M3 implementation work and provide no current
scenario evidence.

ADR 0089 supplies the previously missing gap-free Header offset table before any production NWG1 byte exists, so the
first production version remains `wireVersion=1`. The Header excludes node session, owner witness, body SHA and a
duplicate class field; `laneId` is the permanent class ID. Object digest is SHA-256/v1 code `1/1`, and the twelve
accepted first-satisfied actual-close reasons use codes `1..12`. Normal target/linger values remain evidence-owned by
`V2-OPEN-OBJ-19`; the future projection must mechanically transcribe the ADR rather than create another authority.

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

Resolved by [ADR 0065](../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md). Page/Seal is physical-only,
Root SHA appears once per page rather than once per row, optional provider proof is a closed bounded canonical field
set with deterministic encoding, and bounded parallel prefix-only recovery charges one cumulative envelope. No
binding/read frontier, ACK, gap, or per-binding coverage is copied.

### `V2-OPEN-OBJ-21`: owner-local completion ticket and normal-path ring

Resolved by [ADR 0066](../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md). Tracker slot and
active-tail locator budget reserve together before position allocation; one complete Kafka commit set or Pulsar entry
receives one full 64-bit owner-local ticket, full equality fences slot ABA, exact coverage remains authority, and
takeover rebuilds via bounded collect/sort plus fresh tickets. No ticket enters wire/API/config/metadata.

### `V2-OPEN-READ-01`: Object-WAL active-tail readability before checkpoint/manifest

Resolved by [ADR 0067](../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md). One shared
`VerifiedExtent` feeds range-aggregated protocol-specific locators in a possible shard-owned segmented index. Locator
capacity reserves before position allocation; contiguous locators install hidden before Readable/Durable frontiers and
ACK; takeover publishes Bindings independently; exact manifest coverage plus source protection/read pins precede
retirement. Correctness and hard caps cannot be disabled.

### `V2-OPEN-OBJ-22`: manifest-covered recovery-skip authority

This remains open and evidence-blocked. 0.2 does not admit `FullyManifestCoveredThrough`, a bitmap, per-Binding row, or
partial extent certificate. Apart from an authoritative whole-WalRun retirement frontier, recovery performs bounded
parallel prefix GET under one cumulative envelope. Only an M3/M7 recovery-SLO miss plus useful measured skip hit rate
may reopen the branch; whole-WalRun retirement reuse is preferred before an explicit Root/Seal-bound,
non-regressing `ActiveTailRetiredThrough` or `RecoverySkipCertificate`. Such a certificate would additionally depend on
the accepted read-view handoff and permanent active-tail responsibility transfer and could never authorize ACK,
checkpoint, Seal, or GC. M3/M4 may measure hypothetical skip hit rate with read-view recovery; only the combined M3/M7
end-to-end SLO and hit-rate evidence can reopen the design.

### `V2-OPEN-OBJ-23`: closed qualified-provider-proof row wire

Resolved by [ADR 0068](../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md). `NONE` is the default;
an evidenced Provider may enable the version-bound FULL_OBJECT SHA-256 family at the next Root. Root fixes adapter /
canonicalizer and token cap; the row stores only tag, length, and bounded canonical binary token. Invalid candidate
proof becomes `NONE` before row seal, while malformed persisted wire fails closed. `NONE` adds no routine full GET.

### `V2-OPEN-OBJ-24`: provider token admission evidence

This remains evidence-blocked. Before any Root admits the optional version-bound mode, M3 must prove canonical binary
token encoding/cap, immutable-version binding, FULL_OBJECT SHA-256 scope, version-pinned range benefit, and acceptable
rows/page. Without a current-source receipt the Root remains `NONE`.

### `V2-OPEN-READ-02`: active-tail/manifest reader snapshot

Resolved by [ADR 0069](../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md).
`BindingReadViewSnapshot` is logical rather than one heap object per ACK/read; append publishes frontiers locally
(through ADR 0087's exact fenced state-root cut for Kafka) while
low-frequency source generations use allocation-free read-batch pins. One snapshot may span disjoint manifest/tail
ranges, but atomic append units and declared whole-range fallback stay source-pure. Locator and protection retirement
use two bounded drain stages; pressure never deletes early.

### `V2-OPEN-READ-03`: allocation-free coherent capture

Resolved by [ADR 0070](../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md). Each unfinished
Binding-scoped batch owns one bounded cross-Binding slot; standard hazard order publishes `{Binding,G}`, establishes
StoreLoad, revalidates G, and only then captures a stable generation-tagged frontier/view cell before dereference. The
slot lasts through all source use, and multi-Binding admission is all-or-release.

### `V2-OPEN-READ-04`: durable fallback-removal cut

Resolved by [ADR 0071](../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md). Fenced
`PREFERRED_ONLY`, current slot drain, durable proof for every old read-admitting owner, and exact protection-generation
release are separate prerequisites. Only an authoritative planned drain or a complete read-admission expiry capability
qualifies; otherwise protection and its Cell capacity charge remain.

### `V2-OPEN-READ-05`: async slot reuse and cancellation

Resolved by [ADR 0072](../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md). One 64-bit atomic
`SlotLeaseWord` is `FREE` or `PINNED(generation)`; callbacks equality-check it and only complete terminal source drain
CAS-clears it. Cancellation only stops new use, nonresponsive work stays in bounded quarantine, wrap retires the slot,
and no shared per-read ticket increment or force clear is admitted.

### `V2-OPEN-READ-06`: bounded multi-owner quiescence accumulator

Resolved at the logical proof-authority level by
[ADR 0073](../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md). One monotonic
Binding Read Admission Epoch order, immutable retirement interval, and at most one reusable source-independent proof
per epoch replace the rejected mutable per-batch accumulator. Continuous interval coverage is mandatory; exact
physical proof-window/head/fold representation remains evidence gate `V2-OPEN-READ-08`.

### `V2-OPEN-READ-07`: backend quiescence capability record

Resolved by [ADR 0074](../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md).
`DURABLE_DRAIN_ONLY_V1` and `AUTHORITY_EXPIRY_V1` are closed discriminators whose authorization comes only from one
immutable admission-generation evidence digest. Every historical grant/epoch/proof/fold/batch/release binds it;
missing/revoked evidence retains protection. Exact encoding/admitted backends remain evidence gate
`V2-OPEN-READ-09`.

### `V2-OPEN-READ-08`: proof-window/head/fold physical evidence

This is evidence-blocked. M4 must compare bounded physical representations and numeric limits for the ADR-0073 logical
window without introducing a mutable record per retirement batch. It must preserve contiguous per-epoch coverage,
capability binding, bounded bytes/count/age, response-loss recovery, and proof reuse.

### `V2-OPEN-READ-09`: capability/receipt encoding and backend admission evidence

This is evidence-blocked. M4/M5 must freeze canonical binary encodings and hard caps, verifier availability/revocation
cuts, conformance receipt identities, and actual Kafka/Pulsar backend admission generations. Until then missing
evidence remains `RETAIN` and no backend is promoted by prose alone.

### `V2-OPEN-READ-10`: resolved fallback-capable epoch interval

Resolved by [ADR 0075](../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md). Takeover/read
grant and fallback removal compete on one Binding/incarnation selector CAS or proven equivalent transaction comparing
the exact selected-view/owner/read-epoch/admission-state tuple. Source identities inherit their own first epoch, a batch
minimum is summary only under ADR 0078, and no-fallback epochs create no proof liability.

### `V2-OPEN-READ-11`: resolved source-independent epoch proof publication

Resolved by [ADR 0076](../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md). An irreversible
terminal cut precedes each relevant deterministic create-only proof; publisher fencing governs authorization while the
closed-verified canonical candidate remains safety authority on a non-transactional backend. Exact reread resolves
unknown response, invalid occupants quarantine, and no-fallback epochs are not prewritten.

### `V2-OPEN-READ-12`: selector terminal-state publication

Resolved by [ADR 0077](../decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md). One selector CAS
atomically freezes shared last E, closes E, grants same-owner no-fallback E+1, and persists E's closure anchor while
takeover competes on the same predecessor. Only `ADMITTING/STOPPED` exist; response unknown fences E and STOPPED
recovers only to a fresh epoch.

### `V2-OPEN-READ-13`: retirement-batch construction and completion

Resolved by [ADR 0078](../decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md). Every source row
owns `first_i`, release checks `[first_i,sharedLast]`, selector-only inline/reference activation and N/O(N) costs are
explicit, progress remains derived, and full batch metadata must eventually compact after every source/reference
retires without becoming source-GC authority.

### `V2-OPEN-READ-14`: bounded anchor carry-forward and terminal publisher

Resolved by [ADR 0079](../decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md). The selector
logically owns one small bounded inline canonical unresolved-anchor set and preserves a dedicated emergency STOPPED
envelope. Terminal correctness comes from the exact immutable candidate and one closed verifier rather than a racy
non-transactional owner check; eligible anchors prune asynchronously in batches. K and bytes remain evidence-selected.

### `V2-OPEN-READ-15`: compact batch metadata retirement

Partially resolved by [ADR 0080](../decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md). 0.2 permits
only exact-version same-key `FULL_V1 -> RETIRED_V1`; matching BatchId/fullBatchSha converges delayed create/unknown
replacement, reverse transition is forbidden, and the tombstone grants no source-protection or physical-GC authority.
The compact tombstone remains permanent under Binding/Cell lifetime budgets; exhaustion stops new fallback/handoff
admission and never deletes by age.

Tombstone deletion remains evidence-blocked. Only unacceptable M4/M5 lifetime capacity plus a concrete backend's gap-
free ordered activation history, monotonic conditional authority, exact incarnation/selector-lineage binding, never-
reactivation, stale-create, and recovery evidence may reopen a metadata-deletion authority. No retired-through/frontier
symbol is an accepted 0.2 contract.

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

### `V2-OPEN-BK-02`: resolved semantics and inputs; M2 implementation and evidence remain

Resolved by [ADR 0086](../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md): Kafka uses one
Position Domain across profiles; BookKeeper-primary paths use one logical ledger chain per partition, low-frequency
run/generation roots, packed in-ledger RecordBatch range-index checkpoints, owner-local active-tail locators, targeted
entry reads, and ordered publication over bounded overlapping I/O. Normal append writes no per-append remote metadata.

[ADR 0087](../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) also resolves the protocol semantic
shape: Allocated/Durable/LEO/HW/LSO remain distinct; shared BookKeeper durability does not replace logical ISR;
producer/transaction/leader-epoch state passes a fence-protected coherent publication with locators; compact native
replica-Fetch descriptors split Observed/Applied progress under mandatory journal/source and offset/byte/age lag
bounds; native election caps shared-tail adoption; WAL replay does not invent HW/LSO; Fetch uses coherent isolation
snapshots, native aborted metadata, delayed local wakeup, and floor-plus-successor lookup. BK `NBKE2` and Object
`NWKCP1` implement one protocol-checkpoint contract; one fenced terminal `KafkaProtocolCheckpointHeadV1` selects Object
checkpoints while physical Object checkpoint pages/Seal remain physical-only. A storage-native ISR shortcut is not an
open implementation option.

The accepted
[M2-K0 implementation-input closure](detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) fixes the
remaining implementation structure: exact `NBKE2` tables and hard parser/admission caps land with production codecs,
the minimum module graph, a Cell-scoped BookKeeper session/capability contract, immutable vectors, source locks, and a
non-promotable `v2M2KafkaInputsCheck`. M2-K9 evidence then selects apply-lag/pipeline/recovery/waiter/cursor/rollover
defaults and proves dedicated-ledger viability at 10k/100k partitions. Exact Object `NWKCP1` bytes and Head/vector/key
caps are M3 outputs, not M2 inputs.

This document acceptance created no gate or scenario PASS by itself. The later global M2 receipt now binds the exact
Kafka and Pulsar Final child roots and their 21 complete M2 rows; shared M2/M3/M4/M5/M6 rows remain `PLANNED`.
Failure blocks the profile or requires a new pooled-lane ADR; it cannot silently select a global mixed-partition ledger
or restore V1 reservation dual writes.

### `V2-OPEN-KAF-DATA-01`: `__share_group_state` initial profile remains OPEN

The versioned 0.2 internal-topic Deployment policy now fixes `__consumer_offsets` and `__transaction_state` to
`BOOKKEEPER_WAL_ONLY`. Classifier v1 also recognizes `__share_group_state`, but this review did not select its initial
profile. It must receive an explicit evidence-backed mapping before M6/release activation; it cannot inherit a tenant
default or silently select Object WAL.

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

### `V2-OPEN-BK-11`: resolved NPD1 block wire, limits, codec, and provider evidence

Resolved by [ADR 0056](../decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md) and the M2-P6 receipt.
The 0.2 hard envelope is 4 GiB per data Object, 1,024 multipart parts, 64 MiB per entry/decoded block, and 65,536
entries per block. LocalStack covers exact S3 protocol behavior; fixed MinIO provider execution and published S3
capability sources close concrete provider admission without claiming Amazon S3 performance or endorsement.

### `V2-OPEN-BK-12`: resolved persisted BookKeeper physical-delete intent and fact

Resolved by [ADR 0052](../decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md). Persisted
`RETAIN_BK` or `DELETE_AFTER_VERIFIED` policy controls entry into irreversible
`BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`; the compatibility boolean is only a read fence and retirement,
audit, and physical capacity require the three-state fact.

### `V2-OPEN-BK-13`: resolved NPD1 typed block policy and default evidence

Resolved by [ADR 0057](../decisions/0057-v2-npd1-policy-default-authority-and-evidence.md) and the M2-P6 receipt. The
selected classes are `latency-1mib`, `balanced-4mib`, and `scan-8mib`; 4 MiB is the Deployment base default and 16 MiB
is rejected. Namespace/Topic precedence, Cell/host non-reinterpretation, and failover-stable persisted class identity
are executable contracts.

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

### `V2-OPEN-PUL-OBJ-03`: resolved compatibility-namespace reservation Registry authority

Resolved by [ADR 0032](../decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md). Before V2 admission an
immutable ledger-ID compatibility namespace may have no Registry; while admitted it has exactly one bounded
slice-allocation Registry. Its complete writer commitment/interlock and canonical assignment table advance through
single-key CAS; per-cell allocator state is a versioned derived view. Exact physical key/writer-set encodings, allocator,
and Ledger Chain protocols remain descendants.

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
64 KiB, 256 lifetime rows, and 192 bytes/row. A new logical reservation-domain label cannot reuse the interval; further
allocation requires a new immutable compatibility namespace backed by a disjoint ledger-ID space, or an independent
deployment/cluster.

### `V2-OPEN-PUL-OBJ-09`: virtual-ledger allocator reservation and head publication

This remains open only at the real evidence/selection boundary. ADR 0091 now fixes exact 384-byte `NVAC1`, 192-byte
`NVAH1`, and 256-byte `NVAN1` records, 512-byte-capped versioned Oxia keys, STRICT's four successful writes, RANGE's
allowed `[2,2^40]` size domain, ACTIVE versioned-slice admission, exact-reread reconciliation, same-RESERVED takeover,
and one stale-candidate ID burn. STRICT has no separate install transition and cannot clear an unconsumed reservation;
after takeover its only pre-publish recovery exact-store-proves the prior-owner RESERVED node, consumes that one ID in
the Head without changing the visible pointer, and rereads the same node before clear. The burn cannot allocate or
publish the node, and absent/fabricated/current-owner/wrong-grant/wrong-ID/wrong-predecessor proofs fail closed.
RANGE cannot normally abandon or regrant an installed unused tail; accepted terminal retirement/incompatibility/
corruption authority emits only a non-allocator accounting fact. Every non-Cell-CAS mutation proves the exact current
stored Cell, and all Head/node transitions prove exact namespace/slice/key/version provenance plus consumed-prefix
geometry. Production activation is constructible only from a selection-eligible fixed `selection.nars` reparsed
against the exact same-directory `test/native/fault/scale-10000/scale-100000.naea` files and source artifacts; it
rehashes the actual packaged domain/SPI/Oxia code sources and has no caller digest, aggregate, Boolean, or default mode.
Formal candidates use the same production coordinator while remaining
`runtimeActivated=false`, so the evidence seam cannot activate either candidate.

The current 48 ordinary allocator tests and deterministic `8 workloads x 9 cuts` schedule are implementation
conformance, not real/native evidence; the exact-source verification run must still prove their zero
failure/error/skip counts. ADR 0094 now freezes, before formal execution, the exact executor, workload, independent
telemetry, numeric absolute/native-relative SLOs, RANGE candidates, and closed at-most-one selection rule that ADRs
0055/0091 required but did not numerically supply. ADR 0100 clarifies that broker-crash recovery ends at exact
fresh-owner append admission after the production Head takeover; it forbids an unrelated second allocator rollover
per affected ledger and retains late completion as a disqualifying timeout. ADR 0101 keeps installed RANGE Head/node
operations concurrent through shared exact-Cell proof phases while Cell-mutating grant chains use an exclusive measured
phase; the failed `d819500f...` matrix is diagnostic only and selected nothing. ADR 0104 supersedes the one-JVM lock
as formal performance authority and replaces exhaustive execution with a validator-proof adaptive V2 campaign over
the unchanged 288 logical cells. The interrupted `full-matrix-16254510-r1` and all earlier V1 products remain
immutable diagnostic-only and cannot be resumed or promoted. The remaining gate must execute that source-qualified
multi-broker/native 10,000/100,000 protocol with zero skip, select the smallest qualifying RANGE size if RANGE alone
qualifies, and select at most one persisted mode. Until then both modes remain unselected and no
`V2-POSITION-013/014/017/018` scenario may cite a local or diagnostic result as PASS.
ADR 0105 records this state as source-lock mode `UNSELECTED`; it permits unrelated non-allocator child evidence but
explicitly rejects allocator sealing and Final until a completed uniquely qualified V2 evaluation changes the lock
to `STRICT` or `RANGE` and all Final-owned evidence is refreshed.

### `V2-OPEN-PUL-OBJ-10`: allocator target-scale evidence protocol

Resolved at the protocol layer by
[ADR 0055](../decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md), with the previously omitted
executable workload, telemetry, numeric SLOs, executor envelope, and selection algorithm frozen before the formal run
by [ADR 0094](../decisions/0094-v2-m3-allocator-evidence-workload-and-selection-amendment.md), with exact mass-takeover
recovery termination refined by
[ADR 0100](../decisions/0100-v2-m3-allocator-mass-takeover-recovery-endpoint-amendment.md), and exact Cell-proof
concurrency scheduling refined by
[ADR 0101](../decisions/0101-v2-m3-allocator-cell-proof-concurrency-scheduling-amendment.md), and exact JUnit diagnostic
containment refined without changing the 16-MiB cap by
[ADR 0102](../decisions/0102-v2-m3-allocator-junit-diagnostic-output-containment-amendment.md), with the unchanged
600-second construction cap and immutable-Cell two-phase RANGE batch refined by
[ADR 0103](../decisions/0103-v2-m3-allocator-population-construction-batch-scheduling-amendment.md). The exact-source
execution protocol is further amended by
[ADR 0104](../decisions/0104-v2-m3-allocator-validator-proof-adaptive-campaign-amendment.md): all 288 logical cells
remain, but executed and disposition cells are distinct, every disposition is validator-reproved, four actor lanes use
independent coordinators, and budget exhaustion interrupts rather than completing the campaign. The exact-source
pure schema/planner/validator/selector is now implemented and covered by 13 focused zero-skip tests, including all
four evaluation outcomes and the 13/17/288 execution bounds. It accesses no Oxia and is not formal evidence. The
bounded CAS/reconcile workflow, four-actor Runner, checkpoint/sealer/promotion gates, and required short real-Oxia
diagnostics were the implementation prerequisites before any formal V2 campaign. The bounded CAS/reconcile workflow is implemented in the
metadata SPI and covered by eight focused zero-skip tests: it fixes request/candidate context, uses only exact store
CAS/reread with a global retry bound, contains no shared Java lock, and rejects owner/slice/descriptor drift. The
four-actor Runner and production-workflow harness are now implemented and covered by seven focused zero-skip tests.
They keep four concurrent actor lanes at one in-flight each, physically bound the queue to twice the offered rate,
drop only pre-admission work at cutoff, apply the frozen cleanup deadline to admitted work, expose terminal/gauge
facts, and revalidate both conservation equations without a shared Java correctness lock. This is focused local
conformance, not real-Oxia evidence. Canonical NACP2 checkpoint/resume, NAEV2 evaluation, diagnostic-only NADV2, and
the exact-source attachment/JUnit promotion decision are now implemented and have explicit Gradle/script entrypoints.
Only a complete checkpoint seals; NONE/BOTH remain valid non-promotable decisions; interruption/infrastructure failure
produces no evaluation. Formal workflows additionally enforce the exact 64-retry/four-second/25-ms envelope inside
the five-second Runner cleanup grace and prevent late store completions from dispatching a post-timeout operation.
The old default full/V1 script path fails closed. Real-Oxia diagnostics remain
diagnostic-only rather than promotion authority: exact source `5d86b572...` passes the required four scenarios as
`4/0/0/0`, with NADV2 `9694673...d6e3` and JUnit `a8b0f884...55f3e`. The promotion gate does not trust a
caller-created canonical evaluation or diagnostic attestation: it rederives exact NAEV2 bytes and rehashes both the
diagnostic and formal JUnit inputs. Formal V2 campaign/evaluation/selection remain open and prohibited.
ADR 0106 closes the child/Final governance version boundary without changing that prohibition. The V2 child wrapper
binds NACP2/NAEV2/NADV2, both JUnit inputs, source/executor/workload identity, and every external attachment; its Python
checker independently replays planner/disposition/selector semantics rather than trusting a promotion JSON. Exact
`cccf16c4...` governance covers validator-derived 20-cell STRICT and 17-cell RANGE_16 selections plus a fully rehashed
disposition-forgery rejection as part of 100/0/0/0 local contract tests. No formal campaign has run.
Exact `9355e64a...` adds the bounded adaptive executor without changing that boundary. The executor checkpoints before
execution and after every validator-required action or terminal transition, admits no action whose independent phase
budget is insufficient, rejects source/prefix drift and reordered observations, and never seals NAEV2. Its eight
offline tests bring the freshly rerun pre-campaign inventory to 254/0/0/0, while the separate plan/configuration
contracts remain 5/5. No formal-run task or script path was enabled, no Oxia service was accessed, and no campaign
evidence, selection, receipt, or scenario PASS exists.
ADR 0107 now adds the missing formal-entry wiring while preserving that execution boundary. A pure frozen projection
proves the 288 interval, 360 single-cut fault, 32 RANGE-row scale, and 680 total physical-action maxima, a 48,000-second
process cap, and zero-decision plan SHA-256
`4fbeb2d43bd5865cb6139277a5021ed1b0762223f4983fc8fa50f8edc975ff08`. Exactly one default-off Gradle task and
matching explicit script mode bind one planned action to one adapter call and fail closed on authorization/source,
lock/runtime, plan, worktree, and empty-output drift. This Phase-A wiring does not authorize or execute the task and
does not create campaign evidence or selection. `V2-OPEN-PUL-OBJ-09` therefore remains open, allocator mode remains
`UNSELECTED`, and its M3 scenarios remain `PLANNED` until a later exact-clean-SHA Phase-B authorization.
That V2 Phase-B authorization was exercised exactly once at clean source `6c92d937...`. The completed campaign sealed
validator-reproved NACP2 `2a500526...e33c` as valid non-promotable NAEV2 `10fa2033...a7e5d`, status
`NONE_QUALIFIED`, with no NARS2. ADR 0108 records that the V2 runner's four-by-one admission is structurally capped
below the 25-millisecond/200-request row; it does not reopen or relabel the evidence. The active question is now whether
the separately versioned V3 four-by-64 asynchronous model and exact native-derived floors meet the unchanged SLOs.
Stage B.1 may answer only protocol feasibility and diagnostic performance. Formal execution, mode selection, source
lock, child, current-source M2, scenario, and Final remain separately authorized work.
The V3 implementation now has stable plan digest `019fcac7...35e9`, 328/360/32/720 action maxima, and a 34,260-second
budget sum. The first exact-source `baae2625...326` diagnostic-only run passed 13/0/0/0 and separated healthy native,
runner-scheduler, and direct Oxia behavior from bounded allocator CAS/reconcile contention. This narrows the question;
it does not answer whether any candidate meets every formal SLO, and it does not make the installed RANGE-64 short-row
observation selectable.

The first exact-source V3 formal later returned legal `NATIVE_BASELINE_UNAVAILABLE` rather than a comparable
candidate result. ADR 0109 closes the discovered composition mismatch before another run: the four-worker hidden
Native executor queue is removed, formal and diagnostic share one non-blocking ManagedLedger chain, a Native-only
exact-schedule canary must clear all eight minimum baseline rows, and NADV3 must cover the full diagnostic suite. The
remaining open question is still formal candidate qualification on the later explicitly authorized exact SHA; Stage
B.2 diagnostic conformance cannot select a mode or promote a scenario.
The Stage B.2 profile is closed at schedule digest `b0e923a0...e798`, execution digest `4b11530b...d751`, and plan
digest `5f94079e...b283`; only a later exact-SHA formal authorization may answer that remaining qualification question.
That authorization first ran at `e60327ae...`: all Native baselines were valid at 1000 requests per second, while the
first STRICT row produced a complete measured failure inventory plus expected typed warm-up contention. ADR 0110
freezes the failed attempt and corrects only the infrastructure classifier so the validator can consume the failed
measured row and descend. It does not turn warm-up rejection into PASS, change any rate or threshold, or answer the
still-open candidate-selection question.
The subsequent `c0e28f8e...` attempt reached the derived-800 transition but exposed a physical budget-projection bug
before dispatch: first-population detection treated the DERIVED slot as FIXED. ADR 0111 freezes that failed attempt and
requires explicit FIXED ordinal-zero setup detection while retaining the exact planner-resolved derived rate. This
correction does not answer candidate qualification or change the frozen plan, budget, SLO, or selection question.
The next `1771b000...` attempt passed that projection boundary but stopped before the derived-800 dispatch because the
V3 candidate runtime still called the ADR-0094 fixed-rate-only workload entry. ADR 0112 preserves that attempt and
adds separate closed V3 schedule entries for fixed plus exact reconstructible derived rates; V1/V2 stay fixed-only.
This is still a physical conformance correction, not a candidate PASS, threshold change, or selection decision.
The following `ba7e313f...` attempt proved that entry and completed a RANGE-16 interval, then stopped when its first
fault action observed a post-interval Head beside the harness's stale pre-interval Cell snapshot. ADR 0113 preserves
that failed attempt and requires a monotonic exact-Cell completion handoff before later fault/scale helpers. It does
not replace Oxia correctness with a Java lock, reinterpret the failed row, or change any candidate threshold,
workload, budget, disposition, or selection rule.
The next `ee335a8c...` attempt passed all Native baselines and the first failed STRICT action, then stopped because the
derived-800 warm-up contained one unexpected failure among 937 admitted failures. ADR 0114 preserves the 22-file
attempt and adds separate first-unexpected attribution plus an exact consecutive fixed-1000/derived-800 real-Oxia
diagnostic. It neither reclassifies an exception nor changes interval bytes, candidate qualification, workload,
budget, SLO, disposition, or selection semantics. The allocator selection question therefore remains open pending a
new exact-source formal terminal.
The first current-source replay at `372ca975...` completed the expanded diagnostic inventory at 17/0/0/0 but created
no NADV3 because the sealer still expected five suite files. ADR 0115 preserves that diagnostic and closes the
allowlist at six suites/17 tests without changing the still-open formal qualification question.

ADR 0105 additionally prevents the typed-evidence source lock from preselecting a mode: the V2 lock schema accepts
`UNSELECTED` only for non-allocator children and derives native/allocator provenance from the dedicated M3 forks and
ADR-0097 image.
The exact-source
`1ef4f108...` matrix passed its one testcase but failed the sealed-evidence task on an oversized 113,519,059-byte
JUnit XML caused by 970,241 copies of one expected native-harness cleanup WARN. It produced no evaluation, selection,
or verifier output and remains diagnostic. The following `bd254d24...` run kept JUnit at 1,988 bytes but failed
RANGE-1024 10k-to-100k construction after 600 seconds, before that candidate's 100k measurement; its partial archives
also selected nothing. Neither execution can be resumed or reused by the ADR-0104 V2 campaign. Evidence measures the
maximum sustainable rollover RPS while all bounds hold, includes actual rollover distribution/jitter/storms and native
Pulsar rollover/append-stall baseline, and keeps performance budgets out of allocator durable identity. Execution
remains `PLANNED`; the `9f88fbfb...` 10k RANGE Cell-proof diagnostic passes, and the source now exposes a separate
diagnostic-only RANGE-1024 100k construction guard. That guard passes at exact `e739799f...` in 459.537 seconds with
zero failure/error/skip and unchanged caps, with attachment-set `SHA256SUMS` SHA-256
`1161419f12ad18b6402a31c36f42f2f7571a97ecc540f217d562a075d8e85229`. Neither result is a performance PASS or a
selection input. ADR 0104's plan, compatibility, bounded-runner, four-actor, and short real-Oxia diagnostic gates are
now clean, but the current execution authority still explicitly forbids any full formal allocator campaign until a
later instruction authorizes the adaptive engine and exact-source run.

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

### M3 NWG1 implementation-readiness rounds 1 through 9: resolved by ADRs 0088/0089/0090

Resolved on 2026-08-23 after explicit acceptance and repository-landing authorization. The focused review froze exact
NWG1 layout/caps/crypto, writer/reader/dispatch cuts, four corpus classes, six positive vectors and 114 component rows,
84 negative records across 240 verifier paths, 50 deterministic Object-WAL kernel traces, and the D1/D2/D3 capacity
evidence boundary. It also retains explicit exclusions for positive Storage Epoch ordinal, mixed-policy production
evidence, production-compressor exact output, and synthetic complete Root/Pointer wire.

ADR 0089 then closes the one implementation-input ambiguity found before production: the exact 256-byte Header offset
table. With no production bytes to migrate, it keeps v1; removes node session and duplicate packing class from the
Header; fixes `laneId` as class ID, SHA-256/v1 code `1/1`, and twelve close-reason codes; and makes the projection a
mechanical transcription. The earlier `1..11` close-reason count was erroneous and does not remove or merge the twelfth
accepted reason.

[ADR 0090](../decisions/0090-v2-m3-nwg1-mutation-external-call-profiles.md) expands the frozen `X0/XU=30/54` split
into exact post-preloaded-cut call caps: `X0` permits no external call and `XU` permits at most one KMS unwrap while
forbidding metadata and Provider calls. It changes no wire byte or corpus total and prevents the manifest from hiding
calls behind undefined abbreviations.

This closes `V2-OPEN-OBJ-17` only at the design/input layer. `V2-OPEN-OBJ-19`, `V2-OPEN-PUL-OBJ-09`,
`V2-OPEN-OBJ-22`, and `V2-OPEN-OBJ-24` keep their evidence/selection boundaries. The complete adjusted decision trail
is preserved in [the M3 readiness record](grill-notes/31-m3-nwg1-implementation-readiness.md).

### Restarted Grill 2 round 18 adjusted decisions: resolved/partially resolved by ADRs 0079/0080

Resolved on 2026-08-10 after explicit adjusted confirmation:

- Q1 logical selector-owned bounded anchor set, small inline canonical 0.2 baseline, validated-byte copy, complete
  emergency STOPPED envelope admission, immutable-candidate closed-verifier safety for non-transactional backends,
  monotonic reconciler epoch, different-valid terminal convergence, asynchronous batched/piggyback prune, and terminal
  retirement only through the evidenced proof/fold authority ->
  [ADR 0079](../decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md);
- Q2 exact-version same-key irreversible `FULL_V1 -> RETIRED_V1`, delayed-create/unknown-response convergence by
  matching BatchId/fullBatchSha, strict non-GC meaning, permanent compact tombstone, lifetime budget and no age deletion
  -> [ADR 0080](../decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md).

Tombstone deletion and any retired-through/frontier authority remain `V2-OPEN-READ-15`, evidence-blocked. Proof/fold
and capability encodings remain `V2-OPEN-READ-08/09`. The complete response is preserved in
[the round 18 record](grill-notes/20-restarted-grill-2-anchor-terminal-and-batch-metadata-retirement.md), and
[round 19](grill-notes/21-restarted-grill-2-round-19-evidence-frontier.md) records that no decision-only M0 frontier
remains.

### Restarted Grill 2 round 17 adjusted decisions: resolved by ADRs 0077/0078

Resolved on 2026-08-10 after explicit adjusted confirmation:

- Q1 closed `ADMITTING/STOPPED` selector states, closure/quiescence separation, one fused
  `PWF(O,E) -> PO(O,E+1,batch[last=E],anchor[E])` CAS, same-predecessor takeover competition, successor-carried or
  transaction-atomic anchor, fresh-epoch STOPPED recovery, hard-cap fencing, response-unknown E admission stop, and no
  proof for never-fallback epochs ->
  [ADR 0077](../decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md);
- Q2 exact immutable membership, per-source `first_i`, `[first_i,sharedLast]` release, deterministic digest identity,
  selector-only inline/reference activation, explicit N release CAS plus bounded O(N) scan, sibling-independent
  release, quarantine budget impact, no mutable completion state, and mandatory derived full-batch retirement ->
  [ADR 0078](../decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md).

Selector/terminal/batch membership and per-source release remain non-disableable correctness contracts. Round 18 later
resolved bounded anchor/terminal publication in ADR 0079 and selected permanent same-key compact tombstones in ADR
0080; only tombstone deletion remains under `V2-OPEN-READ-15`. Proof-window/fold and capability encodings remain
evidence gates `V2-OPEN-READ-08/09`. The complete Round 17 response is preserved in
[the round 17 record](grill-notes/19-restarted-grill-2-selector-terminal-and-retirement-batch.md).

### Restarted Grill 2 round 16 adjusted decisions: resolved by ADRs 0075/0076

Resolved on 2026-08-10 after explicit adjusted confirmation:

- Q1 one Binding/incarnation selector CAS or proven equivalent transaction for takeover/read grant versus no-fallback
  publication, exact comparison tuple, whole-epoch conservative interval, inherited source first epoch, mixed-first
  earliest interval, and fallback-only proof liability ->
  [ADR 0075](../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md);
- Q2 one irreversible Read Admission Epoch terminal cut, deterministic create-only and first-valid proof, fenced
  publisher plus closed verifier, exact-reread response-loss convergence, invalid-occupant quarantine, and on-demand
  generation only for an intersecting fallback interval ->
  [ADR 0076](../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md).

Selector linearization, terminal closure, and on-demand proof eligibility are non-disableable correctness contracts.
Round 17 later replaced batch-min release with per-source intervals and resolved `V2-OPEN-READ-12/13` in ADRs 0077/0078;
Round 18 then resolved `V2-OPEN-READ-14` and retained only tombstone deletion under `V2-OPEN-READ-15`. Proof-window/fold
and capability encodings remain evidence gates `V2-OPEN-READ-08/09`. The complete Round 16 response is preserved in
[the round 16 record](grill-notes/18-restarted-grill-2-read-admission-interval-and-proof-publication.md).

### Restarted Grill 2 round 15 adjusted decisions: resolved by ADRs 0072/0073/0074

Resolved on 2026-08-10 after explicit adjusted confirmation:

- Q1 one atomic `FREE/PINNED(generation)` lease word, equality-only callbacks, cancellation as no-new-source-use,
  complete terminal drain, bounded quarantine, fail-closed wrap, and no shared per-read ticket hotspot ->
  [ADR 0072](../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md);
- Q2 one monotonic Binding Read Admission Epoch, immutable retirement interval, source-independent reusable proof per
  epoch, continuous coverage, hard bounds, and explicit rejection of a mutable per-batch accumulator ->
  [ADR 0073](../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md);
- Q3 two closed discriminators authorized only by immutable capability-evidence digest, historical generation binding,
  exact per-owner evidence, fail-safe revocation/missing verifier, and zero normal-read metadata I/O ->
  [ADR 0074](../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md).

Exact bit/layout/counter choices and proof-window/capability encodings remain M4/M5 evidence-selected. The original
per-batch `OwnerReadQuiescenceAggregateV1` recommendation was rejected. The complete response is preserved in
[the round 15 record](grill-notes/17-restarted-grill-2-read-slot-lifecycle-and-quiescence-accumulator.md).

### Restarted Grill 2 round 14 adjusted decisions: resolved by ADRs 0070/0071

Resolved on 2026-08-10 after explicit adjusted confirmation:

- Q1 standard hazard ordering with explicit StoreLoad, pointer revalidation before dereference, stable generation-
  tagged frontier/view capture, one exclusive slot per unfinished Binding read batch, bounded sharded cross-Binding
  pool, complete async source lifetime, and all-or-release multi-Binding reservation ->
  [ADR 0070](../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md);
- Q2 fenced `PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY`, current slot drain, durable all-old-owner quiescence,
  capability-qualified unplanned expiry, exact protection release, bounded retirement batches, and retained-source Cell
  admission -> [ADR 0071](../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md).

No Java layout, numeric pool/time limit, per-read allocation, remote per-read metadata, distributed refcount, generic
lease flag, or unsafe timeout clear was accepted. Round 15 later refined slot reuse, logical proof coverage, and
capability evidence while leaving physical encodings evidence-selected.
The complete response is preserved in
[the round 14 record](grill-notes/16-restarted-grill-2-allocation-free-read-capture-and-durable-handoff.md).

### Restarted Grill 2 round 13 adjusted decisions: Q2/Q3 resolved by ADRs 0068/0069; Q1 remains open

Resolved on 2026-08-09 after explicit adjusted confirmation:

- Q1 rejected `FullyManifestCoveredThrough` for the 0.2 baseline and remains `V2-OPEN-OBJ-22`; bounded prefix recovery
  plus whole-WalRun retirement continues until M3/M7 evidence justifies reopening a partial certificate;
- Q2 `NONE`-default, conditionally admitted version-bound proof, Root mode/canonicalizer/cap, compact binary row, and
  no-routine-full-GET semantics ->
  [ADR 0068](../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md);
- Q3 logical rather than per-object snapshot, append-versus-handoff split, protocol-read-batch pin, mixed-source
  boundary, two-stage locator/protection reclamation, hard backlog bounds, and independent takeover ->
  [ADR 0069](../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md).

That round left exact Provider/token values, coherent capture, durable fallback removal, and numeric pin/view budgets
open; Round 14 later resolved the two read-path contracts through ADRs 0070/0071. The complete Round 13 response is
preserved in
[the round 13 record](grill-notes/15-restarted-grill-2-recovery-skip-proof-provider-proof-wire-and-read-snapshot.md).

### Restarted Grill 2 round 12 adjusted decisions: resolved by ADRs 0065 through 0067

Resolved on 2026-08-09 after explicit adjusted confirmation:

- Q1 physical-only checkpoint/Seal, once-per-page Root, bounded canonical proof boundary, and active-tail prefix-only
  recovery -> [ADR 0065](../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md);
- Q2 pre-position tracker/locator reservation, one full 64-bit owner-local ticket per protocol unit, and bounded
  collect/sort recovery -> [ADR 0066](../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md);
- Q3 non-disableable locator-before-frontier-before-ACK view, shared extent validation, range aggregation, segmented /
  protocol-specific implementation freedom, independent takeover publication, and pin-safe retirement ->
  [ADR 0067](../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md).

No heavy per-Binding/per-unit Java index or generic hot-path TreeMap was frozen. Exact numeric budgets and reader
snapshot mechanics remain open/evidence-blocked. The complete response is preserved in
[the round 12 record](grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md).

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

Three lane-local checkpoint chains are rejected. At that round, exact numeric/class values, canonical lane/key wire,
final RANGE wire/size/evidence, and both allocator modes remained open. ADR 0091 later closes allocator wire/key and the
allowed RANGE domain only; exact RANGE size, real evidence, and both mode candidates remain open. The complete round-10
response is preserved in [the round 10 record](grill-notes/12-restarted-grill-2-hints-lanes-and-range-takeover.md).

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
