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
| T-APPEND-01 | Accepted | Primary-WAL durability, not per-append metadata, linearizes append; Object ACK also preserves a future bounded recovery envelope | removes control-plane latency from normal append and prevents ACKing unrecoverable state | provider slowdown can force earlier backpressure; absent crash attempts need fresh client retry | ADRs 0038/0039, `V2-APP-001..003`, `V2-OBJ-008..011`; zero normal-append metadata I/O |
| T-PROTOCOL-01 | Accepted | Kafka/Pulsar keep protocol-native frames; NWG1 co-locates each Kafka commit set and independently decodes frames | protects Kafka batch/idempotency semantics, opaque Pulsar payloads, random reads, and ManagedLedger parity | per-frame descriptors/AEAD tags, weaker cross-frame compression, and two checksum layers | ADRs 0026/0031/0040, Kafka/Pulsar parity, `V2-OBJ-004/006/012` |
| T-POSITION-01 | Accepted | Pulsar Object WAL uses explicit chain authority plus fixed aligned, never-reused Cell slices from one bounded lifetime registry | preserves native semantics and stock MessageId comparison without Object identity becoming position truth | serialized registry administration, permanent tombstones, fixed-slice waste, native-generator fork, and finite capacity | ADRs 0022/0027/0032/0041, `V2-POSITION-001..007`; reservation, geometry, and explicit-chain evidence |
| T-MULTIPROTOCOL-01 | Accepted | independent Kafka/Pulsar Protocol Cells may share one Storage Fabric | one lifecycle platform without merging protocol authority | multi-protocol identity and operations remain explicit | `V2-MULTIPROTOCOL-001`; provider sharing follows `T-FABRIC-01` |
| T-FABRIC-01 | Accepted | share physical provider infrastructure only through Cell-scoped Provider scopes/sessions; no cross-cell Object groups in 0.2 | provider utilization and common capacity without shared correctness or deletion authority | duplicate sessions consume resources and forgo cross-cell batching; shared infrastructure remains a physical failure domain | `V2-FABRIC-001..003`; namespace/credential/lifecycle, one-cell group, cache/task/GC isolation and noisy-neighbor evidence |
| T-PROFILE-01 | Accepted | Topic Protocol Binding is immutable while profile/format are immutable within each Storage Epoch | stable protocol identity plus explicit profile evolution | readers and recovery must resolve multi-epoch history | fail-fast binding/epoch tests and `V2-PROFILE-001` |
| T-MIGRATION-01 | Accepted | keep the append-only typed Storage Epoch model, but create exactly one initial epoch and ship no online transition in 0.2 | future-compatible durable shape without transition risk in the core rewrite | no online cost/performance switch for an existing Topic Incarnation | ADR 0015 and `V2-MIGRATION-001`; future transition requires new accepted gates |
| T-PROJECTION-01 | Accepted | retain Access Projection/Migration Link boundaries and dual-authority rejection, but ship no cross-protocol runtime in 0.2 | focuses acceptance on native Kafka and Pulsar paths | no secondary-protocol serving or authority migration in 0.2 | ADR 0016 and `V2-PROJECTION-001`; detailed map/semantic questions remain deferred |
| T-OBJECT-01 | Accepted | NWG1 Object WAL combines cross-binding group PUTs with binding-context epochs, content-addressed strong-LIST recovery, no local journal, and bounded run/pointer/envelope authority | one data PUT and zero per-group metadata commits while preserving native atomic units and bounded crash discovery | linger, per-frame overhead, LIST/provider restrictions, rollover control, and availability backpressure | ADRs 0030/0037..0040; per-binding frontier and `V2-OBJ-001..012` |
| T-BK-01 | Accepted | BookKeeper ACK never waits for Object; native Pulsar metadata owns a deterministic data/NPO1 pair, one-shot whole-range fallback, and final revalidation before BK deletion | preserves performance-first ACK and whole-ledger native lifecycle with bounded config-stable recovery | cold-copy lag, one-data-object parallelism, bounded fallback/revalidation I/O, read-pin complexity, and cross-provider TOCTOU | ADRs 0017/0020/0024/0029/0035/0036 plus `V2-BK-001..008` |
| T-LEDGER-01 | Provisional | Kafka starts with one active ledger per partition | simple ownership and retention reasoning | ledger handles and metadata may not scale to high partition counts | 10k/100k partition spike before M2 layout freeze |
| T-META-01 | Accepted | Kafka KRaft and Pulsar MetadataStore/Oxia map one closed aggregate logical schema keyed by typed incarnation; Kafka level 2 is fresh-bootstrap only | native ABA fences, retry-stable equality, and no half-created/cross-key topic state | two physical encodings, whole-record evolution, Pulsar generation evidence, and no online Kafka format upgrade | ADRs 0019/0023/0028/0033/0034, `V2-META-001..004`, `V2-KAF-META-001` |
| T-MANIFEST-01 | Accepted | one binding-scoped typed logical read view may temporarily have overlapping physical generations | safe publication, fallback, and repair | extra storage during grace and more resolver states | generation authority, read pins, source protection, and GC proof |
| T-HANDOFF-01 | Accepted | planned typed handoff data is a hint; the current WalRun root comes from one hashed CAS pointer and bounded lineage | lower healthy failover latency without making hint cleanup correctness | pointer/lineage control work; hint may be missing, stale, or partial | ADR 0039; validate binding/epochs/root and fall back to bounded WAL recovery |
| T-COMPAT-01 | Accepted | V2 is a clean break; Kafka level 2 is fresh-format only and V1/runtime feature transitions are rejected | avoids dual-read/write and state reinterpretation before customers exist | no in-place upgrade or downgrade | ADR 0034; distinct format identities, bootstrap gates, and explicit V1-state rejection |
| T-BENCH-01 | Accepted | performance claims use profile-specific, exact-source comparisons | makes “stronger” falsifiable | more expensive release evidence | pinned source tuple, same resources/request budget, receipt-backed thresholds |
| T-KOP-01 | Accepted | KoP remains designed but is outside the 0.2 runtime and release gate | keeps future compatibility thinking without distracting the core rewrite | KoP design may drift while deferred | retain the document; re-audit before activation |

No V2 implementation may close a `Provisional` row by changing prose alone. The spike result, selected decision, and
receipt must land in the same milestone change.
