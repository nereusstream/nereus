---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeExecutionIndex
sourceTuple: v2-m1
---

# M2 detailed-design index

M2 designs are written before their production slice starts. Acceptance of a semantic direction does not close exact
wire, numeric admission, provider capability, fault-cut, or scale evidence.

| Slice | Design | Status |
| --- | --- | --- |
| M2-KBK | [Kafka BookKeeper offset, run, and range index](kafka-bookkeeper-offset-range-index.md) | semantic direction accepted; exact NBKE2 bytes/numeric bounds/evidence not started |
| M2-KAF-DATA | [Kafka Produce/Fetch frontiers and protocol recovery](kafka-produce-fetch-frontiers-and-recovery.md) | protocol semantics accepted; exact Java/wire/integration/evidence not started |

M1 remains the active implementation milestone. This index does not authorize starting M2 before M1 promotion or
claim `v2M2Check` exists.

## Planned Kafka delivery sequence

M2 closes `BOOKKEEPER_WAL_ONLY` end to end before enabling the async-Object source-switch seam. M3, not M2, owns the
production `OBJECT_WAL` carrier. The implementation order is:

| Slice | Deliverable | Exit boundary |
| --- | --- | --- |
| `M2-K1` | pure partition frontier state, commit slots, read snapshot, deterministic scheduler, and fenced coherent publication | no BookKeeper dependency; publication/fence interleavings and waiter non-wakeup proven |
| `M2-K2` | exact `NBKE2` header/DATA/control/footer codecs | golden/corruption/checked-length matrix before appender work |
| `M2-K3` | one-partition leader-epoch-bound BookKeeper run lifecycle and sequencer | fake-BK open/drain/checkpoint/seal/successor/retire cuts |
| `M2-K4` | capacity-before-offset admission, speculative deltas, bounded async BK I/O, and ordered completion | partition/global entries+bytes limits and no speculative resource hole |
| `M2-K5` | committed/speculative producer state, transaction/aborted state, first-unstable offset, and leader-epoch index | one fenced publication advances locator, protocol state, result, Durable, and LEO |
| `M2-K6` | packed active-tail/RangeIndex lookup and targeted random plus disposable sequential reads | floor+coverage+successor and no ordinary whole-run checksum/read amplification |
| `M2-K7` | profile-neutral checkpoint kernel, BookKeeper control-entry implementation, and election-bounded recovery | bounded suffix scan and complete crash/response-loss/old-owner cut matrix |
| `M2-K8` | compact descriptor codec, observation journal, Observed/Applied apply kernel, ISR eligibility, and election validator | hard journal/source/apply-lag bounds, corruption recovery, and no Kafka-wire activation |
| `M2-K9` | real BookKeeper fault, recovery, resource, and 10k/100k evidence | evidence selects cadence, index, pipeline, tail, rollover, and handle-cache values |
| `M2-K10` | `v2M2Check` and scenario receipts | `V2-BK-001..017`, `V2-KAF-DATA-001..022`, zero normal metadata I/O, targeted reads, bounded recovery |

Exact queue depths, checkpoint cadence, index size, recovery-tail limits, rollover thresholds, handle-cache policy, and
latency/scale thresholds are evidence outputs. No slice silently activates M2 on main before M1 Final.
