---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeExecutionIndex
sourceTuple: v2-m1
---

# M2 detailed-design index

M2 designs are written before their production slice starts. Acceptance of a semantic direction does not close exact
wire, numeric admission, provider capability, fault-cut, or scale evidence.

| Slice | Design | Status |
| --- | --- | --- |
| M2-K0 | [Kafka implementation-input closure](kafka-m2-k0-implementation-input-closure.md) | design accepted; production constants/codecs/provider/gates and evidence not started |
| M2-K0-M | [Module graph and immutable N1 input](kafka-m2-k0-module-graph.md) | current immutable-input receipt; provider/wire/runtime/scenarios excluded |
| M2-KBK | [Kafka BookKeeper offset, run, and range index](kafka-bookkeeper-offset-range-index.md) | semantic direction accepted; exact NBKE2 bytes/numeric bounds/evidence not started |
| M2-KAF-DATA | [Kafka Produce/Fetch frontiers and protocol recovery](kafka-produce-fetch-frontiers-and-recovery.md) | protocol semantics accepted; exact Java/wire/integration/evidence not started |

M1 Final is complete at the trusted predecessor source tuple. M2 is `InProgress` only because the K0-M production
module graph and its non-zero local gate landed together. The remaining K0 provider/wire/numeric/evidence cuts are not
implemented, no scenario is promoted, and `v2M2KafkaInputsCheck` plus global `v2M2Check` remain absent.

Kafka M2 uses its own planned sub-aggregate through `v2M2KafkaFinalCheck`. Global `v2M2Check` is a separate aggregate
that also requires the Pulsar-owned M2 work. A Kafka sub-aggregate cannot be reported as global M2 Final.

## Planned Kafka delivery sequence

M2 closes `BOOKKEEPER_WAL_ONLY` end to end before enabling the async-Object source-switch seam. M3, not M2, owns the
production `OBJECT_WAL` carrier. The implementation order is:

| Slice | Deliverable | Exit boundary |
| --- | --- | --- |
| `M2-K0` | exact wire/cap tables, checked numeric model, minimum module graph, Cell-scoped provider contract, and source-qualified evidence/gate schema | production inputs aggregate under non-promotable `v2M2KafkaInputsCheck`; no writer/runtime/scenario PASS |
| `M2-K1` | pure partition frontier state, commit slots, read snapshot, deterministic scheduler, and fenced coherent publication | no BookKeeper dependency; publication/fence interleavings and waiter non-wakeup proven |
| `M2-K2` | exact `NBKE2` header/DATA/control/footer codecs | golden/corruption/checked-length matrix before appender work |
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
