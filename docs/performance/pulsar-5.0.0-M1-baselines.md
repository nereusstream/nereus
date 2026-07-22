# Pulsar 5.0.0-M1 performance baselines

This document freezes the source boundaries and comparison order for the first Nereus performance run. It does not
claim that evidence collected from a later Pulsar `master` applies to this backport.

## 1. Source baselines

| Role | Repository / branch | Source anchor | Version |
| --- | --- | --- | --- |
| Apache Pulsar control | `nereusstream/pulsar:5.0.0-M1` | `8dae0236c0a0d405ed7f8303081080520fe91551` (`v5.0.0-M1`) | `5.0.0-M1` |
| Nereus Pulsar | `nereusstream/pulsar:5.0.0-M1-nereus` | `5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e` | `5.0.0-M1` |
| Nereus | `nereus:v0.1.0` | based on `main@7d89eea`; M1 compatibility/test tip `18cb06e` | `0.1.0-SNAPSHOT` |

The Nereus Pulsar branch contains 55 commits after the release tag：

- 54 Nereus commits, replayed in their original order from the Nereus integration range whose parent is
  `100d3ef0ff` and whose former `master` tip was `2f9c1eb93b`；
- one explicit M1 compatibility commit, `5ffc2caa0e`；
- none of the 64 community commits between `v5.0.0-M1` and `100d3ef0ff`。

`nereus/gradle.properties` pins `pulsarExpectedHead` to `5ffc2caa0e08dac95bc8c2ea76ed3d32382dfe3e`.

## 2. M1-specific backport decisions

The backport preserves the M1 behavior instead of importing later community APIs：

- distribution wiring adds the Nereus adapter but not the post-M1 minimized-fastutil module；
- `PersistentTopic` retains M1 replication/migration behavior while adding Nereus admission and admin routing；
- `PersistentSubscription` uses M1's precise backlog count because `ManagedCursor.hasBacklog(...)` is post-M1；
- policy refresh uses the M1 ordered executor because `PersistentTopic.getPoliciesNotifyThread()` is post-M1；
- the facade does not write `ManagedLedgerInternalStats.properties`, which is absent in M1；
- the public-surface test is locked to the M1 `ManagedCursor` API。

## 3. Independent final-gate evidence

The final gate was rerun against the backported Pulsar checkout on 2026-07-22：

```bash
./gradlew bookKeeperPrimaryWalFinalCheck --rerun-tasks \
  -PpulsarCheckout=/Users/liusinan/apps/ideaproject/nereusstream/pulsar
```

Result at Nereus code/test tip `18cb06e` and Pulsar tip `5ffc2caa0e`：

```text
BUILD SUCCESSFUL in 34m 59s
227 actionable tasks: 227 executed
```

Two failures found before that successful run were evidence that a green gate on the former Pulsar `master` could not
be inherited：the M1 `ManagedCursor` surface differs from post-M1, and one manifest-visibility test used a timing-only
deadline before reaching its intended commit cut. Both are now explicit and deterministic.

## 4. Comparison matrix

Use the same Pulsar metadata backend for all rows. Pulsar 5.0.0-M1 already provides the `oxia://host:port/[namespace]`
metadata-store provider, so using Oxia for both branches avoids accidentally measuring ZooKeeper-versus-Oxia while
claiming to measure Nereus. Use separate Oxia namespaces for Pulsar control-plane metadata and Nereus durable metadata.

| Stage | Pulsar branch | Topic storage path | Purpose |
| --- | --- | --- | --- |
| A | `5.0.0-M1` | stock BookKeeper | community control |
| B | `5.0.0-M1-nereus` | stock `bookkeeper` storage class | cost of carrying the broker integration while Nereus is not selected |
| C | `5.0.0-M1-nereus` | `nereus` + `BOOKKEEPER_WAL_ONLY` | facade + Nereus Oxia metadata path, without Object materialization |
| D | `5.0.0-M1-nereus` | `nereus` + `BOOKKEEPER_WAL_ASYNC_OBJECT` | background Object materialization cost and lag |
| E | `5.0.0-M1-nereus` | `nereus` + `BOOKKEEPER_WAL_SYNC_OBJECT` | producer-path Object completion cost |

Interpret deltas in order：`A -> B` isolates the dormant integration tax；`B -> C` introduces the facade and Nereus
metadata protocol；`C -> D -> E` compares the three BookKeeper-WAL profiles without changing the fork or broker
metadata backend. Do not use existing topics when switching storage classes or profiles；the selection is
first-create-only and online migration is outside this baseline.

## 5. Run controls and outputs

Keep hardware, CPU affinity, JDK/JVM flags, broker/bookie/Oxia/Object-store topology, network shaping, quorum settings,
topic/partition count, message size, batching, compression, producer/consumer concurrency, backlog, warm-up, run length,
and dataset constant. Randomize or alternate the row order and run enough repetitions to report dispersion, not only a
single best result.

Collect at least：

- publish and consume throughput；
- end-to-end and publish latency at p50/p95/p99/p99.9；
- broker/bookie/Oxia/Object-store CPU, RSS, GC, network, disk and request latency；
- BookKeeper add/read latency and ledger rollovers；
- Nereus append, metadata-CAS, materialization lag, recovery and retirement/GC metrics；
- restart recovery time and correctness counters before treating a throughput result as valid。

Record the exact branch heads, configuration files, image digests, workload seed, start/end timestamps and raw result
paths with every run.
