---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: NormativePlan
sourceTuple: v2-m0
---

# Implementation plan and gates

## Milestones

| Milestone | Scope | Status at M0 | Required aggregate |
| --- | --- | --- | --- |
| M0 | V1 archive references, Context Map/glossaries, V2 ADRs/contracts, open-question/session logs, source/scenario manifests, tradeoff register, documentation gate | DocumentationGated | `v2M0Check` |
| M1 | pure V2 active graph; Java-17/JDK-only domain and exact four-capability metadata SPI; NTB1/NSE1 identities and strict NTA1 aggregate; complete Kafka API-key-32000 KRaft record/image/CreateTopics pseudo-config/resolution/sizing/projection/publication authority; Pulsar selector CAS plus authoritative ABA-safe ownership witness, gap-safe stale-install exclusion, and local atomic ACTIVE fence; compatibility-namespace Registry, complete writer-set/interlock, versioned derived slice view, and `REGISTRY_CONFORMANCE`; allocator `HARNESS_CONFORMANCE_ONLY` with no mode selection; remove every active V1 runtime/gate | Planned | `v2M1FinalCheck` |
| M2 | Owner Epoch lane, typed frontier contract, BookKeeper foundation, Pulsar deterministic NPD1-data/NPO1-root pair with checked 16-byte-row/streaming envelope and provider admission plus native-relative block-policy evidence, ManagedLedger-owned dual-source handle/read pins/final-delete revalidation and persisted BK_DELETE state/retention policy, Kafka ledger-layout scale spike | Planned | `v2M2Check` |
| M3 | one-cell NWG1 Object WAL groups; binding-context epoch authority and commit-set co-location; run-key/per-Object AEAD; final class/lane leaf grammar and post-plan sequence allocation; up to three lazy lanes under one Root/pointer; provider-resolved physical frontier plus owner-local per-binding typed frontier; physical-only de-duplicated checkpoint rows/Seal; one publisher-epoch-fenced vector chain; pre-position tracker/locator reservation and local tickets; shared-verified range-aggregated active-tail publication before ACK; Root-fixed NONE/optional bounded provider-proof mode; provider-absent cuts; conservative bounded prefix/LIST recovery with no partial skip vector; provider/session evidence; fixed-slice Pulsar virtual-ledger path with RANGE evidence | Planned | `v2M3Check` |
| M4 | manifest, protocol-position/timestamp indexes, Storage Epoch resolver, logical Binding read snapshot, bounded sharded generation-tagged hazard slots, ABA-safe lease word and terminal source drain, `ADMITTING/STOPPED` Binding selector with fused fallback-removal/E+1 cut, small inline closure-anchor set plus emergency STOPPED envelope, closed-verifier terminal publication and async batched prune, per-source first/shared-last intervals, deterministic on-demand proofs/window, exact inline/reference activation, explicit bounded O(N) reconciliation, and two-stage retirement | Planned | `v2M4Check` |
| M5 | materialization, compaction, retention, immutable capability-evidence verification, exact per-source protection release, irreversible same-key `FULL_V1 -> RETIRED_V1` batch compaction with permanent compact tombstone, retained-source/batch/tombstone admission and alerting, per-cell cache/task isolation, physical GC | Planned | `v2M5Check` |
| M6 | Kafka/Pulsar broker/controller process integration; Produce/Fetch/Admin, restart/catch-up/snapshot, and protocol compatibility evidence over the M1 metadata authorities | Planned | `v2M6Check` |
| M7 | fencing, planned handoff, bounded recovery, cell-local drain/close isolation, mixed-profile operations | Planned | `v2M7Check` |
| M8 | scale, shared-infrastructure/noisy-neighbor chaos, exact-source AutoMQ comparison, Pulsar native parity, release evidence | Planned | `v2M8Check` and `v2FinalCheck` |

Only M0 tasks are registered at M0. A future task name in this plan is not an implementation or PASS claim.

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
validateOnly pre-admit the exact final cumulative record count and serialized Raft-batch bytes while preserving native
per-topic partial-success behavior.

Pulsar M1 arms gap-safe invalidation, captures authoritative ABA-safe native ownership witness A/B around exact
selector/aggregate validation, and installs only by CAS from the same invalidation sequence. Legacy/ELM/third-party
backends missing acquisition identity, authoritative read, ordered loss hook, or reconnect-gap invalidation fail V2
admission closed. Ordinary access captures/rechecks one atomic fence word with zero remote metadata I/O. Full
aggregate-to-retired-tombstone replacement remains M5; complete process integration remains M6.

M1 implements the mode-independent virtual-ledger Registry bound to the immutable ledger-ID compatibility namespace.
V2 admission requires exactly one selected Registry, a complete inline or exact referenced writer commitment, and an
ACL/credential/deployment interlock; allocators use a versioned derived slice view. Its real-Oxia receipt is
`REGISTRY_CONFORMANCE`. STRICT/RANGE candidate SPI and cut injection remain evidence-only and emit a distinct
`HARNESS_CONFORMANCE_ONLY` receipt with `selectionEligible=false`; M1 runs deterministic/small smoke only and neither
persists nor activates a mode. M3 owns 10k/100k multi-broker capacity evidence and selection.

`docs/v2/source-locks.json` is the sole expected-SHA authority for external Kafka/Pulsar/Oxia sources. Checkout paths
may be overridden; expected SHAs may not. The manifest cannot self-lock the current Nereus commit; a promotion receipt
binds it. M1 gates are:

- `v2M1Check`: no Docker/fork/composite; local domain/schema/SPI/codec/harness, active-graph, and V1-absence checks;
- `v2M1ExactSourceCheck`: clean exact forks before/after, isolated immutable artifacts, real Oxia, and focused fork tests;
- `v2M1FinalCheck`: aggregate previous outcomes and receipt schema without rerunning their suites.

Zero tests, skipped mandatory tests, failure, dirtied/changed source, or digest mismatch fails. PR CI runs the fast gate;
trusted promotion runs Exact/Final. Promotion uses N1 foundation, P1/K1 fork commits, N2 source-tuple/final execution,
then receipt-only N3. Each receipt binds N2/P1/K1, source-lock digest, domain JAR/POM SHAs, Oxia identity, closed receipt
kind, scenario IDs, test/failure/skip counts, and aggregate result. A Registry scenario requires
`REGISTRY_CONFORMANCE`; allocator cut scenarios require `HARNESS_CONFORMANCE_ONLY` and `selectionEligible=false`.
Cross-M1 scenario rows are split before promotion so future evidence cannot be borrowed.

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
