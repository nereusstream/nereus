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
| T-PROFILE-01 | Accepted | semantic topic profile is immutable | deterministic recovery and lifecycle | no online profile conversion | fail-fast binding tests; any migration requires a later ADR |
| T-OBJECT-01 | Accepted | Object WAL uses group commit and bounded runs | viable Object request cost and throughput | linger latency, cross-stream coupling, and recovery scanning | per-stream durable prefix, shard budgets, `V2-OBJ-001..003` |
| T-BK-01 | Accepted | BookKeeper ACK never waits for Object storage | preserves the performance-first hot path | async offload needs two-provider publication and GC proof | native offload authority decision plus `V2-BK-001..003` |
| T-LEDGER-01 | Provisional | Kafka starts with one active ledger per partition | simple ownership and retention reasoning | ledger handles and metadata may not scale to high partition counts | 10k/100k partition spike before M2 layout freeze |
| T-META-01 | Accepted | Kafka uses KRaft; Pulsar uses MetadataStore/Oxia | native authority and failure semantics | two backends and conformance suites | capability-split SPI; no high-churn KRaft worker leases |
| T-MANIFEST-01 | Accepted | one logical read view may temporarily have overlapping physical generations | safe publication, fallback, and repair | extra storage during grace and more resolver states | generation authority, read pins, source protection, and GC proof |
| T-HANDOFF-01 | Accepted | planned handoff data is a hint only | lower healthy failover latency | hint can be missing, stale, or partial | validate identity/epoch/root and fall back to WAL recovery |
| T-COMPAT-01 | Accepted | V2 is a clean break from V1 | avoids dual-read/write complexity before customers exist | no in-place upgrade | distinct format identities and explicit V1-state rejection |
| T-BENCH-01 | Accepted | performance claims use profile-specific, exact-source comparisons | makes “stronger” falsifiable | more expensive release evidence | pinned source tuple, same resources/request budget, receipt-backed thresholds |
| T-KOP-01 | Accepted | KoP remains designed but is outside the 0.2 runtime and release gate | keeps future compatibility thinking without distracting the core rewrite | KoP design may drift while deferred | retain the document; re-audit before activation |

No V2 implementation may close a `Provisional` row by changing prose alone. The spike result, selected decision, and
receipt must land in the same milestone change.
