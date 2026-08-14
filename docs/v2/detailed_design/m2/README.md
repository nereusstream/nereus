---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: CurrentSourceReceipt
authority: NormativeExecutionIndex
sourceTuple: v2-m1
---

# M2 detailed-design index

M2 designs are written before their production slice starts. Acceptance of a semantic direction does not close exact
wire, numeric admission, provider capability, fault-cut, or scale evidence.

| Slice | Design | Status |
| --- | --- | --- |
| M2-K0 | [Kafka implementation-input closure](kafka-m2-k0-implementation-input-closure.md) | verified current-source input aggregate; non-promotable and no writer/runtime claim |
| M2-K0-M | [Module graph and immutable N1 input](kafka-m2-k0-module-graph.md) | current immutable-input receipt; provider/wire/runtime/scenarios excluded |
| M2-K0-P | [Cell-scoped BookKeeper provider contract](kafka-m2-k0-provider-contract.md) | verified production contract; covered by current Kafka Inputs receipt |
| M2-K0-W | [Closed NBKE2 v1 wire contract](kafka-m2-k0-nbke2-wire.md) | verified codec/projection/goldens; covered by current Kafka Inputs receipt |
| M2-K0-N | [Checked numeric admission and recovery envelope](kafka-m2-k0-numeric-admission.md) | verified admission/envelope; covered by current Kafka Inputs receipt |
| M2-K0-E | [Exact source, receipt, and Kafka Inputs gate](kafka-m2-k0-evidence-and-input-gate.md) | current canonical input receipt: 5 gates, 16 suites, 54 tests, zero skip |
| M2-K1 | [Coherent frontier and fenced publication cell](kafka-m2-k1-frontier-publication.md) | pure production state/CAS cut and 26-test local gate; Kafka Fast receipt pending |
| M2-K2 | [Kafka-native assigned RecordBatch adapter](kafka-m2-k2-assigned-record-batch-adapter.md) | exact 4.3 native plus local header/CRC/coverage matrix; appender/runtime excluded |
| M2-K3 | [Leader-epoch run lifecycle and entry sequencer](kafka-m2-k3-run-lifecycle.md) | fake-provider header/drain/checkpoint/seal/successor/retire gate; real BK pending |
| M2-K4 | [Capacity-first ordered DATA pipeline](kafka-m2-k4-ordered-pipeline.md) | partition/global entries+bytes admission and B-before-A completion gate; connected to K5 publication |
| M2-K5 | [Coherent producer/transaction/locator publication](kafka-m2-k5-coherent-protocol-publication.md) | pre-offset protocol validation plus one fenced K1 root replacement; ACK/HW/runtime excluded |
| M2-K6 | [Packed targeted and sequential reader](kafka-m2-k6-targeted-reader.md) | entry-local NBKE2/Kafka validation and captured isolation bounds; runtime/recovery/real BK excluded |
| M2-K7 | [Checkpoint kernel and election-bounded recovery](kafka-m2-k7-checkpoint-recovery.md) | aligned KPC1/NBKE2 state, cumulative suffix envelope, and native adoption cut; HW/runtime/real BK excluded |
| M2-K8 | [Replica descriptor, journal, and eligibility kernel](kafka-m2-k8-replica-observation.md) | fixed KRD1/KRO1, exact sync seam, Observed/Applied bounds, source replacement, and election harness; runtime excluded |
| M2-K9 | [Real BookKeeper fault and scale evidence](kafka-m2-k9-real-bookkeeper-evidence.md) | current-source exact-image receipt: 110k actual partitions, 110,256 ledgers, 239 local plus 9 real tests, selected defaults; non-promotable until K10 |
| M2-K10 | [Kafka Final evidence](kafka-m2-k10-final-evidence.md) | current-source canonical Kafka Final receipt; 10 exact-M2 scenarios, 40 named suite references, and 7 bound attachments |
| M2-KBK | [Kafka BookKeeper offset, run, and range index](kafka-bookkeeper-offset-range-index.md) | implementation started at K0 module/provider/wire/numeric inputs; runtime/evidence pending |
| M2-KAF-DATA | [Kafka Produce/Fetch frontiers and protocol recovery](kafka-produce-fetch-frontiers-and-recovery.md) | implementation started at K1 pure coherent state; storage/runtime/evidence pending |
| M2-P0 | [Pulsar offload input closure](pulsar-m2-p0-input-closure.md) | sealed-ledger attempt, deterministic keys, provider capability, candidate-limit, retention, and delete-state inputs; non-promotable |
| M2-P1 | [NPD1/NPB1 data codec](pulsar-m2-p1-npd1-codec.md) | streaming file encode, block-local NONE/ZSTD plus AES-GCM, derived 16-byte rows, and targeted corruption matrix; defaults unselected |
| M2-P2 | [NPO1 sealed-ledger root](pulsar-m2-p2-npo1-root.md) | canonical four-section root, complete sealed-ledger facts, dual-domain sparse coverage, hard parser caps, and self-digest matrix |
| M2-P3 | [Sealed-ledger publication engine](pulsar-m2-p3-publication-engine.md) | data-before-root immutable publication, response-loss HEAD resolution, exact-EOF streaming body, actual-reader verifier, and root-first cleanup |
| M2-P4-OBJ | [Object read handle](pulsar-m2-p4-object-reader.md) | bounded root HEAD/GET, exact immutable data proof, block-local targeted reads, typed failures, and streaming complete-ledger revalidation |

M1 Final is complete at the trusted predecessor source tuple. M2 remains `InProgress`. K0 through K9 have focused
implementation gates, and K10 now has a production closed receipt/resolver plus a current-source Kafka Final receipt.
The receipt binds the refreshed K9 evidence and promotes only the exact ten-scenario allowlist. No Kafka broker runtime,
native ISR/HW/election behavior, mixed downstream scenario, Pulsar M2 result, or global `v2M2Check` PASS is claimed.

Kafka M2 uses its own planned sub-aggregate through `v2M2KafkaFinalCheck`. Global `v2M2Check` is a separate aggregate
that also requires the Pulsar-owned M2 work. A Kafka sub-aggregate cannot be reported as global M2 Final.

## Planned Kafka delivery sequence

M2 closes `BOOKKEEPER_WAL_ONLY` end to end before enabling the async-Object source-switch seam. M3, not M2, owns the
production `OBJECT_WAL` carrier. The implementation order is:

| Slice | Deliverable | Exit boundary |
| --- | --- | --- |
| `M2-K0` | exact wire/cap tables, checked numeric model, minimum module graph, Cell-scoped provider contract, and source-qualified evidence/gate schema | production inputs aggregate under non-promotable `v2M2KafkaInputsCheck`; no writer/runtime/scenario PASS |
| `M2-K1` | pure partition frontier state, commit slots, read snapshot, deterministic scheduler, and fenced coherent publication | no BookKeeper dependency; publication/fence interleavings and waiter non-wakeup proven |
| `M2-K2` | consume the K0-W codec through Kafka-native assigned-RecordBatch validation and run-facing adapters | native header/CRC/coverage cross-check matrix before appender work; persisted NBKE2 v1 bytes remain unchanged |
| `M2-K3` | one-partition leader-epoch-bound BookKeeper run lifecycle and sequencer | fake-BK open/drain/checkpoint/seal/successor/retire cuts |
| `M2-K4` | capacity-before-offset admission, speculative deltas, bounded async BK I/O, and ordered completion | partition/global entries+bytes limits and no speculative resource hole |
| `M2-K5` | committed/speculative producer state, transaction/aborted state, first-unstable offset, and leader-epoch index | one fenced publication advances locator, protocol state, result, Durable, and LEO |
| `M2-K6` | packed active-tail/RangeIndex lookup and targeted random plus disposable sequential reads | floor+coverage+successor and no ordinary whole-run checksum/read amplification |
| `M2-K7` | profile-neutral checkpoint kernel, BookKeeper control-entry implementation, and election-bounded recovery | bounded suffix scan and complete crash/response-loss/old-owner cut matrix |
| `M2-K8` | compact descriptor codec, observation journal, Observed/Applied apply kernel, ISR eligibility, and election validator | hard journal/source/apply-lag bounds, corruption recovery, and no Kafka-wire activation |
| `M2-K9` | real BookKeeper fault, recovery, resource, and 10k/100k evidence | evidence selects cadence, index, pipeline, tail, rollover, and handle-cache values |
| `M2-K10` | `v2M2KafkaFinalCheck` and Kafka-owned scenario receipts | only complete claims owned exactly by M2 may promote; mixed M2/M3/M4/M5/M6 rows remain `PLANNED` |

Exact queue depths, checkpoint cadence, index size, recovery-tail limits, rollover thresholds, handle-cache policy, and
latency/scale thresholds are evidence outputs. Accepted design text alone does not activate any M2 slice on `main`.
