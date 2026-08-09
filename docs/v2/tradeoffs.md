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
| T-PROTOCOL-01 | Accepted | Kafka and Pulsar keep protocol-native frames; Kafka storage-append commit sets and Pulsar entries preserve native atomicity | protects Kafka batch/idempotency semantics, opaque Pulsar payloads, and ManagedLedger parity | protocol-specific frame/index envelopes and two checksum layers | ADRs 0026/0031, Kafka compatibility, Pulsar parity, and `V2-OBJ-004/006` |
| T-POSITION-01 | Accepted | Kafka Offset and Pulsar Position remain separate binding/incarnation-scoped domains; Pulsar Object WAL uses explicit chain authority and reserved-slice IDs from one bounded registry | preserves native semantics and stock MessageId comparison without Object identity becoming position truth | serialized registry administration, capacity limit, native-generator fork, and protocol-specific chain recovery | ADRs 0022/0027/0032, `V2-POSITION-001..004`; reservation and explicit-chain evidence |
| T-MULTIPROTOCOL-01 | Accepted | independent Kafka/Pulsar Protocol Cells may share one Storage Fabric | one lifecycle platform without merging protocol authority | multi-protocol identity and operations remain explicit | `V2-MULTIPROTOCOL-001`; provider sharing follows `T-FABRIC-01` |
| T-FABRIC-01 | Accepted | share physical provider infrastructure only through Cell-scoped Provider scopes/sessions; no cross-cell Object groups in 0.2 | provider utilization and common capacity without shared correctness or deletion authority | duplicate sessions consume resources and forgo cross-cell batching; shared infrastructure remains a physical failure domain | `V2-FABRIC-001..003`; namespace/credential/lifecycle, one-cell group, cache/task/GC isolation and noisy-neighbor evidence |
| T-PROFILE-01 | Accepted | Topic Protocol Binding is immutable while profile/format are immutable within each Storage Epoch | stable protocol identity plus explicit profile evolution | readers and recovery must resolve multi-epoch history | fail-fast binding/epoch tests and `V2-PROFILE-001` |
| T-MIGRATION-01 | Accepted | keep the append-only typed Storage Epoch model, but create exactly one initial epoch and ship no online transition in 0.2 | future-compatible durable shape without transition risk in the core rewrite | no online cost/performance switch for an existing Topic Incarnation | ADR 0015 and `V2-MIGRATION-001`; future transition requires new accepted gates |
| T-PROJECTION-01 | Accepted | retain Access Projection/Migration Link boundaries and dual-authority rejection, but ship no cross-protocol runtime in 0.2 | focuses acceptance on native Kafka and Pulsar paths | no secondary-protocol serving or authority migration in 0.2 | ADR 0016 and `V2-PROJECTION-001`; detailed map/semantic questions remain deferred |
| T-OBJECT-01 | Accepted | Object WAL uses bounded group commit, a pre-open run root, sequence/length/SHA leaf identity, strong bounded LIST discovery, CRC32C frames, and provider proof/full-GET fallback | one data PUT and no per-group metadata commit while preserving crash discovery and integrity | linger, hashing/CRC, rare full GET, LIST recovery, narrower provider admission, and cross-binding coupling | ADRs 0018/0021/0025/0026/0030/0031; per-binding frontier and `V2-OBJ-001..006` |
| T-BK-01 | Accepted | BookKeeper ACK never waits for Object; native Pulsar metadata owns sealed-ledger offload and each attempt uses deterministic data/root keys plus a bounded verified root | preserves the performance-first hot path and whole-ledger native lifecycle with config-stable cleanup | cold copy may lag; one data Object limits parallelism; root verification and two-provider GC proof | ADRs 0017/0020/0024/0029 plus `V2-BK-001..005`; pair/read/delete cuts and lag admission |
| T-LEDGER-01 | Provisional | Kafka starts with one active ledger per partition | simple ownership and retention reasoning | ledger handles and metadata may not scale to high partition counts | 10k/100k partition spike before M2 layout freeze |
| T-META-01 | Accepted | Kafka uses KRaft and Pulsar uses MetadataStore/Oxia; one immutable composite record is keyed by typed native incarnation and owns deterministic binding/initial-epoch IDs | native ABA fences, retry-stable identity, and no half-created/cross-key topic state | two backend integrations, Pulsar generation evidence, and whole-record schema/equality evolution | ADRs 0019/0023/0028, `V2-META-001..003`; exact retry and zero normal-append metadata I/O |
| T-MANIFEST-01 | Accepted | one binding-scoped typed logical read view may temporarily have overlapping physical generations | safe publication, fallback, and repair | extra storage during grace and more resolver states | generation authority, read pins, source protection, and GC proof |
| T-HANDOFF-01 | Accepted | planned typed handoff data is a hint only | lower healthy failover latency | hint can be missing, stale, or partial | validate binding/incarnation/Storage Epoch/Owner Epoch/root and fall back to WAL recovery |
| T-COMPAT-01 | Accepted | V2 is a clean break from V1 | avoids dual-read/write complexity before customers exist | no in-place upgrade | distinct format identities and explicit V1-state rejection |
| T-BENCH-01 | Accepted | performance claims use profile-specific, exact-source comparisons | makes “stronger” falsifiable | more expensive release evidence | pinned source tuple, same resources/request budget, receipt-backed thresholds |
| T-KOP-01 | Accepted | KoP remains designed but is outside the 0.2 runtime and release gate | keeps future compatibility thinking without distracting the core rewrite | KoP design may drift while deferred | retain the document; re-audit before activation |

No V2 implementation may close a `Provisional` row by changing prose alone. The spike result, selected decision, and
receipt must land in the same milestone change.
