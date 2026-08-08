---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeEvidencePlan
sourceTuple: v2-m0
---

# V2 scenario evidence matrix

The machine-readable owner is [v2-scenarios.json](v2-scenarios.json). At M0 every runtime scenario is `PLANNED`;
the documentation gate proves synchronization only and does not promote runtime evidence.

| ID | Milestone | Contract | Required evidence | Status |
| --- | --- | --- | --- | --- |
| V2-APP-001 | M1 | ACK covers the complete binding-scoped typed Protocol Coverage in the selected primary WAL | deterministic Position Domain/WAL/profile matrix plus recovery | PLANNED |
| V2-APP-002 | M1 | normal admitted append has zero remote metadata reads and mutations | instrumented hot-path test and metrics receipt | PLANNED |
| V2-APP-003 | M2 | uncertain/fenced completions cannot create duplicate successful protocol positions | response-loss and takeover cut matrix | PLANNED |
| V2-PROFILE-001 | M1 | Topic Protocol Binding is immutable; profile is immutable within a Storage Epoch; operational policy is mutable | binding/epoch create-open and policy-version tests | PLANNED |
| V2-POSITION-001 | M1 | Kafka Offset and Pulsar Position remain binding/incarnation-scoped typed domains without a universal logical offset | cross-domain serialization, compare-rejection, and ledger-chain coverage tests | PLANNED |
| V2-MULTIPROTOCOL-001 | M1 | one Storage Fabric hosts independent Kafka/Pulsar Protocol Cells without shared position or write authority | multi-cell identity, ownership, and cross-authority rejection tests | PLANNED |
| V2-FABRIC-001 | M1 | cells sharing physical provider infrastructure retain distinct Provider scopes/sessions, namespace/security scope, and lifecycle | scope identity/configuration, namespace/credential isolation, independent close and foreign-access rejection | PLANNED |
| V2-FABRIC-002 | M3 | Object groups never cross Protocol Cells and cell-local throttle/credential/close faults cannot mutate another session | shared-provider dual-cell shard, admission, credential, response-loss, drain and close cuts | PLANNED |
| V2-FABRIC-003 | M5 | shared worker/cache/GC capacity cannot cross cell queue, task, cache, publication, or delete authority | noisy-neighbor, cache-key collision, stale task, foreign publication/delete and shared-executor restart cuts | PLANNED |
| V2-MIGRATION-001 | M1 | Storage Epoch history uses exact typed cuts and cannot expose two append-admitting epochs | epoch-chain schema/invariant tests; runtime transition matrix remains open | PLANNED |
| V2-PROJECTION-001 | M1 | Access Projection and Migration Link cannot grant a second Native Write Authority | model validation and dual-authority rejection tests; runtime mapping remains open | PLANNED |
| V2-OBJ-001 | M3 | Object ACK waits for verified group durability | real provider completion and restart | PLANNED |
| V2-OBJ-002 | M3 | unrelated bindings avoid shard-wide typed-frontier HOL | stalled/corrupt binding concurrency test | PLANNED |
| V2-OBJ-003 | M3 | response-loss recovery verifies bytes and remains bounded | real provider lost-response, checksum drift, budget tests | PLANNED |
| V2-BK-001 | M2 | BookKeeper ACK never waits for Object | real quorum plus unavailable Object matrix | PLANNED |
| V2-BK-002 | M2 | Pulsar async offload preserves native ledger authority | native ManagedLedger/offload/cursor/retention gate | PLANNED |
| V2-BK-003 | M2 | Kafka ledger layout is viable at 10k/100k partitions | memory, handle, metadata, recovery, rollover spike | PLANNED |
| V2-READ-001 | M4 | one binding-scoped typed logical view survives publication, damage, and fallback | publication cuts, corruption quarantine, protected fallback | PLANNED |
| V2-READ-002 | M5 | trim/GC cannot delete a live or ambiguously owned source | multi-reader/worker and response-loss matrix | PLANNED |
| V2-META-001 | M2 | both metadata backends satisfy shared invariants | backend conformance and controller/store failover | PLANNED |
| V2-HO-001 | M7 | typed handoff hint is optional and safely rejected | healthy, missing, stale, duplicated, corrupt hint cuts | PLANNED |
| V2-KAF-001 | M8 | Kafka Object profile meets pinned AutoMQ targets | same-environment exact-source benchmark/compatibility receipt | PLANNED |
| V2-PUL-001 | M8 | Pulsar BookKeeper path is not weaker than pinned native Pulsar | feature matrix plus latency/throughput/resource receipt | PLANNED |
| V2-KOP-001 | M0 | KoP design is retained but excluded from 0.2 gates | documentation presence/status check | PLANNED |

## Promotion rules

- `IMPLEMENTED_NOT_RUN` requires a production owner and executable gate.
- `PASSED_CURRENT_SOURCE` requires the exact source tuple and a non-empty append-only receipt.
- A focused result does not promote a broader row.
- A changed product/fork/provider source invalidates inherited current-source status until the gate is rerun.
- Mandatory failures or missing rows fail the aggregate; they are never converted to skipped PASS results.
