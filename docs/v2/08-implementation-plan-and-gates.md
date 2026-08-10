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
| M1 | typed protocol-native incarnations and deterministic IDs; closed aggregate logical schema v1; Kafka API-key-32000 TopicImage record with publication-boundary incremental/full validation; Pulsar selector CAS state machine, cached ACTIVE fence, and retired tombstone; `k=40`/64-KiB/256-assignment registry bootstrap contract plus ADR-0055 native-relative allocator evidence harness and parallel RANGE contract; typed policy-scope resolver; capability-split metadata SPI; remove superseded V1 API | Planned | `v2M1Check` |
| M2 | Owner Epoch lane, typed frontier contract, BookKeeper foundation, Pulsar deterministic NPD1-data/NPO1-root pair with checked 16-byte-row/streaming envelope and provider admission plus native-relative block-policy evidence, ManagedLedger-owned dual-source handle/read pins/final-delete revalidation and persisted BK_DELETE state/retention policy, Kafka ledger-layout scale spike | Planned | `v2M2Check` |
| M3 | one-cell NWG1 Object WAL groups; binding-context epoch authority and commit-set co-location; run-key/per-Object AEAD; final class/lane leaf grammar and post-plan sequence allocation; up to three lazy lanes under one Root/pointer; provider-resolved physical frontier plus owner-local per-binding typed frontier; physical-only de-duplicated checkpoint rows/Seal; one publisher-epoch-fenced vector chain; pre-position tracker/locator reservation and local tickets; shared-verified range-aggregated active-tail publication before ACK; Root-fixed NONE/optional bounded provider-proof mode; provider-absent cuts; conservative bounded prefix/LIST recovery with no partial skip vector; provider/session evidence; fixed-slice Pulsar virtual-ledger path with RANGE evidence | Planned | `v2M3Check` |
| M4 | manifest, protocol-position/timestamp indexes, Storage Epoch resolver, logical Binding read snapshot, bounded sharded generation-tagged hazard slots, ABA-safe lease word and terminal source drain, one Binding selector linearizing takeover/read grant with `PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY`, inherited conservative fallback intervals, irreversible epoch terminal cuts, deterministic on-demand reusable source-independent proofs/window, and bounded two-stage retirement | Planned | `v2M4Check` |
| M5 | materialization, compaction, retention, immutable capability-evidence verification, exact source-protection release, retained-source admission/alerting, per-cell cache/task isolation, physical GC | Planned | `v2M5Check` |
| M6 | Kafka KRaft level-2 record/image/snapshot integration and protocol compatibility | Planned | `v2M6Check` |
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

Superseded V1 Java, tests, Phase/Future prose, and literal checks are deleted with the V2 slice that replaces them. They
are not marked deprecated and are not kept as compatibility shims; branch `v0.1` preserves the old product line.

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
