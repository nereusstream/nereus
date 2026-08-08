---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# V2 architecture tradeoff register

This register makes every intentional compromise addressable from ADRs, design contracts, implementation reviews, and
evidence. `Accepted` freezes the direction; `Provisional` requires its named spike or gate before implementation freeze.

| ID | Status | Decision | Gain | Cost / risk | Mitigation and evidence gate |
| --- | --- | --- | --- | --- | --- |
| T-APPEND-01 | Accepted | Primary-WAL durability, not a per-append metadata CAS, linearizes append | removes control-plane latency and availability from normal append | owner recovery and response-loss resolution become more demanding | `V2-APP-001..003`; zero normal-append remote metadata reads and mutations |
| T-PROTOCOL-01 | Accepted | Kafka and Pulsar keep protocol-native hot paths | protects Kafka semantics and Pulsar ManagedLedger parity | deliberate duplication at protocol boundaries | Kafka compatibility matrix and Pulsar native parity gate |
| T-POSITION-01 | Accepted | Kafka Offset and Pulsar Position remain separate binding/incarnation-scoped Position Domains | preserves native semantics and avoids a false cross-protocol position truth | shared algorithms require typed coverage/frontier operations | `V2-POSITION-001`; reject cross-domain compare and `ledgerBase + entryId` mapping |
| T-MULTIPROTOCOL-01 | Accepted | independent Kafka/Pulsar Protocol Cells may share one Storage Fabric | one lifecycle platform without merging protocol authority | multi-protocol identity and operations remain explicit | `V2-MULTIPROTOCOL-001`; provider sharing follows `T-FABRIC-01` |
| T-FABRIC-01 | Accepted | share physical provider infrastructure only through Cell-scoped Provider scopes/sessions; no cross-cell Object groups in 0.2 | provider utilization and common capacity without shared correctness or deletion authority | duplicate sessions consume resources and forgo cross-cell batching; shared infrastructure remains a physical failure domain | `V2-FABRIC-001..003`; namespace/credential/lifecycle, one-cell group, cache/task/GC isolation and noisy-neighbor evidence |
| T-PROFILE-01 | Accepted | Topic Protocol Binding is immutable while profile/format are immutable within each Storage Epoch | stable protocol identity plus explicit profile evolution | readers and recovery must resolve multi-epoch history | fail-fast binding/epoch tests and `V2-PROFILE-001` |
| T-MIGRATION-01 | Accepted | same-protocol profile evolution uses exact typed Storage Epoch cuts, never a universal offset or required dual write | permits topic-level profile changes without changing protocol identity | transition, rollback, backfill, and old-extent retirement are complex | `V2-MIGRATION-001`; no runtime transition before `V2-OPEN-MIGRATION-01..03` close |
| T-PROJECTION-01 | Accepted | secondary protocol access uses Access Projection; protocol authority change uses Migration Link; both retain one Native Write Authority | allows future cross-protocol access without dual-native writers | durable mapping and semantic state transfer are substantial | `V2-PROJECTION-001`; no runtime delivery before `V2-OPEN-PROJECTION-01..03` close |
| T-OBJECT-01 | Accepted | Object WAL uses group commit and bounded runs | viable Object request cost and throughput | linger latency, cross-binding coupling, and recovery scanning | per-binding typed durable frontier, shard budgets, `V2-OBJ-001..003` |
| T-BK-01 | Accepted | BookKeeper ACK never waits for Object storage | preserves the performance-first hot path | async offload needs two-provider publication and GC proof | native offload authority decision plus `V2-BK-001..003` |
| T-LEDGER-01 | Provisional | Kafka starts with one active ledger per partition | simple ownership and retention reasoning | ledger handles and metadata may not scale to high partition counts | 10k/100k partition spike before M2 layout freeze |
| T-META-01 | Accepted | Kafka uses KRaft; Pulsar uses MetadataStore/Oxia | native authority and failure semantics | two backends and conformance suites | capability-split SPI; no high-churn KRaft worker leases |
| T-MANIFEST-01 | Accepted | one binding-scoped typed logical read view may temporarily have overlapping physical generations | safe publication, fallback, and repair | extra storage during grace and more resolver states | generation authority, read pins, source protection, and GC proof |
| T-HANDOFF-01 | Accepted | planned typed handoff data is a hint only | lower healthy failover latency | hint can be missing, stale, or partial | validate binding/incarnation/Storage Epoch/Owner Epoch/root and fall back to WAL recovery |
| T-COMPAT-01 | Accepted | V2 is a clean break from V1 | avoids dual-read/write complexity before customers exist | no in-place upgrade | distinct format identities and explicit V1-state rejection |
| T-BENCH-01 | Accepted | performance claims use profile-specific, exact-source comparisons | makes “stronger” falsifiable | more expensive release evidence | pinned source tuple, same resources/request budget, receipt-backed thresholds |
| T-KOP-01 | Accepted | KoP remains designed but is outside the 0.2 runtime and release gate | keeps future compatibility thinking without distracting the core rewrite | KoP design may drift while deferred | retain the document; re-audit before activation |

No V2 implementation may close a `Provisional` row by changing prose alone. The spike result, selected decision, and
receipt must land in the same milestone change.
