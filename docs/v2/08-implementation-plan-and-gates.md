---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: DocumentationOnly
authority: NormativePlan
sourceTuple: v2-m0
---

# Implementation plan and gates

## Milestones

| Milestone | Scope | Current status | Required aggregate |
| --- | --- | --- | --- |
| M0 | V1 archive references, Context Map/glossaries, V2 ADRs/contracts, open-question/session logs, source/scenario manifests, tradeoff register, documentation gate | DocumentationGated | `v2M0Check` |
| M1 | pure V2 active graph; Java-17/JDK-only domain and exact four-capability metadata SPI; NTB1/NSE1 identities and strict NTA1 aggregate; complete Kafka API-key-32000 KRaft record/image/CreateTopics pseudo-config/resolution/sizing/projection/publication authority; Pulsar selector CAS plus authoritative ABA-safe ownership witness, gap-safe stale-install exclusion, and local atomic ACTIVE fence; compatibility-namespace Registry, complete writer-set/interlock, versioned derived slice view, and `REGISTRY_CONFORMANCE`; allocator `HARNESS_CONFORMANCE_ONLY` with no mode selection; remove every active V1 runtime/gate | **InProgress: M1.1a-A foundation only** | `v2M1FinalCheck` |
| M2 | Owner Epoch lane, typed frontier contract, BookKeeper foundation, Pulsar deterministic NPD1-data/NPO1-root pair with checked 16-byte-row/streaming envelope and provider admission plus native-relative block-policy evidence, ManagedLedger-owned dual-source handle/read pins/final-delete revalidation and persisted BK_DELETE state/retention policy, Kafka ledger-layout scale spike | Planned | `v2M2Check` |
| M3 | one-cell NWG1 Object WAL groups; binding-context epoch authority and commit-set co-location; run-key/per-Object AEAD; final class/lane leaf grammar and post-plan sequence allocation; up to three lazy lanes under one Root/pointer; provider-resolved physical frontier plus owner-local per-binding typed frontier; physical-only de-duplicated checkpoint rows/Seal; one publisher-epoch-fenced vector chain; pre-position tracker/locator reservation and local tickets; shared-verified range-aggregated active-tail publication before ACK; Root-fixed NONE/optional bounded provider-proof mode; provider-absent cuts; conservative bounded prefix/LIST recovery with no partial skip vector; provider/session evidence; fixed-slice Pulsar virtual-ledger path with RANGE evidence | Planned | `v2M3Check` |
| M4 | manifest, protocol-position/timestamp indexes, Storage Epoch resolver, logical Binding read snapshot, bounded sharded generation-tagged hazard slots, ABA-safe lease word and terminal source drain, `ADMITTING/STOPPED` Binding selector with fused fallback-removal/E+1 cut, small inline closure-anchor set plus emergency STOPPED envelope, closed-verifier terminal publication and async batched prune, per-source first/shared-last intervals, deterministic on-demand proofs/window, exact inline/reference activation, explicit bounded O(N) reconciliation, and two-stage retirement | Planned | `v2M4Check` |
| M5 | materialization, compaction, retention, immutable capability-evidence verification, exact per-source protection release, irreversible same-key `FULL_V1 -> RETIRED_V1` batch compaction with permanent compact tombstone, retained-source/batch/tombstone admission and alerting, per-cell cache/task isolation, physical GC | Planned | `v2M5Check` |
| M6 | Kafka/Pulsar broker/controller process integration; Produce/Fetch/Admin, restart/catch-up/snapshot, and protocol compatibility evidence over the M1 metadata authorities | Planned | `v2M6Check` |
| M7 | fencing, planned handoff, bounded recovery, cell-local drain/close isolation, mixed-profile operations | Planned | `v2M7Check` |
| M8 | scale, shared-infrastructure/noisy-neighbor chaos, exact-source AutoMQ comparison, Pulsar native parity, release evidence | Planned | `v2M8Check` and `v2FinalCheck` |

M0 gates and the partial `v2M1FoundationCheck` are registered. The latter is explicitly not `v2M1Check` or a PASS
claim; every later M1/M2-M8 task name in this plan remains a future contract until implemented.

ADR 0015 limits 0.2 to one initial Storage Epoch per Topic Incarnation and no online profile-transition runtime. ADR 0016
excludes Kafka/Pulsar Access Projection and Migration Link runtime. Their future state machines, Pulsar
BookKeeper/Object profile transition, and KoP therefore remain outside the 0.2 implementation plan; their deferred
questions do not block this release.

## Milestone delivery contract

Every implementation milestone must land one coherent set:

1. production and test implementation;
2. the affected normative V2 contract, ADR/context language, and open-question update;
3. scenario Markdown and JSON statuses;
4. tradeoff status or mitigation changes;
5. source-lock changes, when an external or fork source changed;
6. ordinary deterministic gate;
7. exact-source receipt for any `Verified` or performance/parity claim.

M1 finishes with no compilable, publishable, or runnable V1 graph. The transition first lands domain/SPI and dependency
checks, then cuts old settings/BOM/publication/CI edges, then removes the V1 runtime and Phase/F9 tasks/scripts in
separate mechanical commits. Architecture, gate rewiring, and the large deletion are not one review. Nothing is marked
deprecated or retained as a compatibility shim; protected `v0.1`/`v0.1.0` history preserves the old product line. KoP
runtime leaves the active graph while its design documents remain.

M1 has started with the explicitly partial M1.1a-A foundation: Java-17/JDK-only modules, bootstrap identities,
ProtocolKind/NPC1/NTI1/NTB1/NSE1 and authority-leaf codecs, minimal independent aggregate domain types, four closed
metadata capabilities, dependency/API boundaries, deterministic tests, and reproducible JAR/source-JAR/POM hashing.
The separate Oxia client-continuity target is complete at final fork `091a42c`, and metadata-oxia O2 is locally
verified at Nereus `050f908a` with its immutable client/API bundle, four single-key adapters, continuity scaffold,
69 focused tests, 299 whole-module tests, and `promotionEligible=false` receipt. These slices neither implement nor
activate complete NTA1, P1, or R1. M1.1b-Q1 readiness evidence is complete at `94881e67`: 14 test-scope tests measure
the 4-KiB and 16-KiB per-name candidates, 8,397/32,973-byte checked parser caps, all six legality rows, and strict
allocation boundaries. Its result is `READINESS_EVIDENCE_ONLY`, `promotionEligible=false`; M1.1b still owns its strict
production codec/goldens only after the Proposed FrameEncodingPolicy, legality matrix, name caps, and formula are
grilled and explicitly accepted.

## M1 implementation and promotion contract

The M1 module dependency is `nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia`. Domain is Java-17/JDK-only
and contains canonical values, deterministic IDs, and validators. SPI contains exactly aggregate Publisher/Reader,
Pulsar generation Selector Store, and Pulsar virtual-ledger Registry Store, with ADR 0082's closed create/CAS outcomes.
One `VersionedAggregateSnapshot` supplies Binding/initial-Epoch projections; child authorities and generic metadata
facades are forbidden. Kafka `:metadata` consumes the exact immutable domain JAR/POM without transitively importing
SPI/Oxia and implements none of these SPIs; its generated physical record remains Kafka-owned. Published domain
artifacts are source-qualified by JAR/POM SHA and may not be overwriteable SNAPSHOT/changing/composite inputs.

Kafka feature 2, API-32000, atomic CreateTopics aggregate, input-only pseudo-config resolution, read-only config
projection, TopicImage/Delta/replay/snapshot/remove, and publication validation form one activatable M1 source tuple. A
generated record may land dormant but cannot be advertised, formatted, or emitted alone. M6 owns complete
broker/controller process and Produce/Fetch/Admin/restart evidence only. Ordinary delta validation is touched-topic-
only without canonical SHA recomputation; full scans are bootstrap/snapshot/full-catch-up only; CreateTopics and
validateOnly apply the stock request-wide partition guard and then request-order greedy pre-admission of the exact final
cumulative record count and serialized Raft-batch bytes while preserving native per-topic partial-success behavior.
Duplicate pseudo-configs are last-wins, the production native validator runs after pseudo removal, and
`CreateTopicPolicy` sees only native configs. The candidate list includes actual native configuration-derived records, including applicable `ClearElrRecord`; the
linear sizer shares effective Raft limits, and a current-batch miss is re-estimated with fresh-batch offset deltas before
the final append may reject it. V2 admission requires `remote.log.storage.system.enable=false`; M1 tests the interlock
and M6 proves RLMM remains inactive.

Pulsar M1 builds the first witness candidate only for Oxia 0.9.0-backed MetadataStore ELM: pinned source proves direct
GET/Stat/versioned-CAS primitives, not a complete adapter. M1 adds acquisition fields/transitions, a qualified provider
lifecycle/gap hook, and one closed writer kernel, with syncer disabled and all ownership writers upgraded. It arms gap-
safe invalidation, captures authoritative ABA-safe native ownership witness A/B around exact selector/aggregate
validation, and installs only by CAS from the same invalidation sequence. Legacy/TableView/mixed/third-party backends
missing acquisition identity, authoritative read, ordered loss hook, or reconnect-gap invalidation fail V2 admission
closed. Ordinary access captures/rechecks one atomic fence word with zero remote metadata I/O. Full
aggregate-to-retired-tombstone replacement remains M5; complete process integration remains M6.
The provider hook exports only a local store-wide `WatchContinuityEpoch` plus ready barrier; provider internal
connection/session/shard identities are not persisted. A gap invalidates all store fences and triggers bounded,
coalesced A/read/B recovery. The accepted O1 client fork uses the confirmed server base's existing no-start-offset dummy
notification batch as the ready barrier and adds no server wire/RPC; a gap discards the old offset and obtains a new
barrier. Source-lock schema v2 owns the confirmed implementation/conformance bases while final fork/artifact/server-
image identities and conformance remain distinct pending promotion evidence.

M1 implements the mode-independent virtual-ledger Registry bound to the immutable 32-byte ledger-ID compatibility
namespace `SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])`. Only an exact lowercase canonical non-zero
36-byte UUID from a root authoritatively absent immediately before init is
admitted; format/changed identity is not cleanup proof. V2 admission requires exactly one selected Registry, one bounded
inline canonical writer commitment, and an ACL/credential/deployment interlock with independently revocable writers;
there is no external membership reference. Allocators use a namespace-bound versioned derived slice view. Its real-
Oxia receipt is `REGISTRY_CONFORMANCE`. STRICT/RANGE candidate SPI and cut injection remain evidence-only and emit a distinct
`HARNESS_CONFORMANCE_ONLY` receipt with `selectionEligible=false`; M1 runs deterministic/small smoke only and neither
persists nor activates a mode. M3 owns 10k/100k multi-broker capacity evidence and selection.

The Registry closes writer kinds to native BookKeeper and Nereus virtual-ledger ID allocation. Each cohort uses a
120-byte canonical row; `RegistryAdmissionEvidenceV1` is bounded immutable activation proof, not allocation authority.
`maxWriterCount=8` remains only a candidate pending the full bounded cohort/rollout/rollback/residue inventory and
Registry size formula. The writer-count choice blocks the Registry codec/capacity gate, not M1.1a domain/SPI work.

`docs/v2/source-locks.json` is the sole expected-SHA authority for external Kafka/Pulsar/Oxia sources. Checkout paths
may be overridden; expected SHAs may not. The manifest cannot self-lock the current Nereus commit; a promotion receipt
binds it. M1 gates are:

- `v2M1FoundationCheck`: current partial domain/SPI unit, golden, API/dependency, documentation, and artifact-hash gate;
  it cannot prove backend/runtime conformance, the pure final graph, or M1 PASS;
- `v2M1Check`: no Docker/fork/composite; local domain/schema/SPI/codec/harness, active-graph, and V1-absence checks;
- `v2M1ExactSourceCheck`: clean exact forks before/after, isolated immutable artifacts, real Oxia, and focused fork tests;
- `v2M1FinalCheck`: aggregate previous outcomes and receipt schema without rerunning their suites.

Zero tests, skipped mandatory tests, failure, dirtied/changed source, or digest mismatch fails. PR CI runs the fast gate;
trusted promotion runs Exact/Final. Promotion uses N1 foundation, P1/K1 fork commits, N2 source-tuple/final execution,
then evidence-only N3. Virtual-ledger conformance payloads use one strict RFC-8785/JCS envelope with the closed kinds
`REGISTRY_CONFORMANCE | HARNESS_CONFORMANCE_ONLY`; this is not the universe of all M1 evidence kinds. Each receipt binds
N2/P1/K1, source-lock digest, domain JAR/POM SHAs, Oxia server image plus client/test identities, closed receipt kind,
one `scenarios[] -> suites[]` result hierarchy and normalized `discovered/executed/passed/failed/skipped/aborted`
counts. Derived summaries cannot become another authority. Internal retries/dynamic tests are forbidden and mandatory
PASS has non-zero execution with no failure/skip/abort. Attachments are allowlisted regular files referenced by kind,
safe sorted POSIX-relative path, length, and SHA-256 rather than embedded. The canonical root contains exactly
`schema/kind/sourceTuple/scenarios/attachments`; canonical bytes supply content identity, with no `runIdentity`, leaf
IDs, or separately authoritative aggregate result. Attachment kinds are `TEST_REPORT`, `REGISTRY_BYTES`,
`REGISTRY_ADMISSION_EVIDENCE`, `WRITER_INTERLOCK_SNAPSHOT`, and `SANITIZED_LOG_EXCERPT`. A Registry scenario requires
`REGISTRY_CONFORMANCE`; allocator cut scenarios require `HARNESS_CONFORMANCE_ONLY` and `selectionEligible=false`.
Cross-M1 scenario rows are split before promotion so future evidence cannot be borrowed. N3 may change only receipts,
attachments, and their exactly covered scenario status/index; it may not modify code, gates, workflows, ADRs, or source
locks. The Final index is a typed path/length/SHA promotion manifest, not another result authority. Evidence-derived
root/count/path/file/total/log caps remain OPEN until representative early-M1 outputs establish them; they block the
receipt validator/N3 promotion, not M1.1a.

## Status model

Normative documents carry:

- `designStatus: Accepted | Proposed`;
- `implementationStatus: NotStarted | InProgress | Verified`;
- `evidenceStatus: NotRun | DocumentationOnly | CurrentSourceReceipt`;
- `sourceTuple` pointing to the structured lock.

`Verified` requires `CurrentSourceReceipt` and a non-empty receipt path. Historical or focused evidence must use a
lower status.

Scenario statuses are `PLANNED`, `IMPLEMENTED_NOT_RUN`, `PASSED_CURRENT_SOURCE`, `FAILED`, or
`BLOCKED_ENVIRONMENT`. Moving a row to `PASSED_CURRENT_SOURCE` requires the exact product/fork/baseline tuple and
receipt to be present in the same change.

## M0 gate

`v2DocumentationCheck` validates required files and front matter, Context Map/glossaries, accepted/superseded ADR
state, the three-profile vocabulary, typed-position, Storage Epoch, and cell-scoped Provider contracts, JSON structure,
source/archive identity, tradeoff IDs, scenario synchronization, local Markdown links, and receipt/status consistency.
`v2M0Check` is the M0 aggregate and is wired into CI.

The legacy Phase 3, Phase 4, and BookKeeper documentation checks remain available for V1 implementation evidence, but
their literal requirements no longer govern the V2 overview/index.

## Final evidence

M8 evidence is append-only under `docs/v2/evidence/<source-tuple-id>/`. The receipt records:

- exact clean product, Kafka, Pulsar, AutoMQ, BookKeeper/provider, and client identities;
- environment, object request accounting, durability/replication, dataset, warmup, duration, and thresholds;
- scenario results with no missing, duplicate, or silently skipped mandatory rows;
- artifacts and checksums required to reconstruct the run.

The acceptance baseline fields in [source locks](source-locks.json) intentionally remain `NOT_PINNED` at M0.
