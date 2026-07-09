# AutoMQ-like Stream Storage Profile

> This directory is the design home for the AutoMQ-like Nereus implementation path.
> It complements the Ursa-like Phase 1 core design instead of replacing it.

## 1. Motivation

The current Phase 1 design is Ursa-like: append success waits for primary WAL durability and an
Oxia-visible stream commit/read-index boundary. That is the right correctness baseline, but Nereus also
needs an AutoMQ-like profile where producer latency is governed by primary WAL durability and long-lived
object materialization happens asynchronously.

This profile is useful when a topic wants:

- lower producer ack latency than sync object publication;
- BookKeeper as a low-latency primary WAL with background object copy;
- Object WAL as a low-cost primary WAL with background read-optimized object generation;
- topic-level control over whether object storage is disabled, synchronous, or asynchronous;
- compatibility with Kafka Remote Log Storage / Pulsar offload without running two remote-log systems.

AutoMQ-like in Nereus does not mean a second log model. It is a profile over the same internal truth:

```text
streamId + offset
+ primary WAL durable proof
+ stable offset / protocol projection
+ async object materialization
```

## 2. Profile Matrix

| Profile | Primary WAL | Object materialization | Ack boundary | Read before materialization |
| --- | --- | --- | --- | --- |
| `BOOKKEEPER_WAL_ONLY` | BookKeeper | disabled | BK durable + stable projection | BK range |
| `BOOKKEEPER_WAL_SYNC_OBJECT` | BookKeeper | sync / Ursa-like | BK durable + Oxia visible index | BK or object range selected by index |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` | BookKeeper | async / AutoMQ-like | BK durable fast ack + stable projection | BK range until object generation is published |
| `OBJECT_WAL_SYNC_OBJECT` | Object WAL | sync / Ursa-like | Object WAL durable + Oxia visible index | Object WAL range |
| `OBJECT_WAL_ASYNC_OBJECT` | Object WAL | async / AutoMQ-like | Object WAL durable fast ack + stable projection | Object WAL range until read-optimized object is published |

The legacy `StorageProfile.OBJECT_WAL` name maps to `OBJECT_WAL_SYNC_OBJECT`.
`StorageProfile.defaultDurabilityLevel()` is the code-level branch point: sync object profiles map to
`WAL_DURABLE_AND_INDEX_COMMITTED`, while async and WAL-only profiles map to `WAL_DURABLE`.

## 3. Required Common Refactor

Ursa-like and AutoMQ-like modes should share the protocol-neutral L0 contracts and avoid parallel
implementations of offset, fencing, object key, and read semantics.

Suggested package split once `nereus-core` grows beyond the marker class:

```text
io.nereus.core.common
  StreamProfileResolver
  PrimaryWalAppender
  StableOffsetCommitter
  AppendSessionManager
  OffsetProjectionBuilder
  ReadPathSelector
  MaterializationLagTracker

io.nereus.core.ursalike
  SyncAppendCoordinator
  SyncObjectVisibilityPublisher

io.nereus.core.automqlike
  FastAckAppendCoordinator
  AsyncMaterializationPlanner
  MaterializationWorker
  MaterializationCheckpointStore
  WalRetentionGate
```

Common interfaces:

```java
interface PrimaryWalAppender {
    CompletionStage<WalAppendResult> append(WalAppendRequest request);
}

interface StableOffsetCommitter {
    CompletionStage<StableAppendResult> commitStableOffset(StableAppendRequest request);
}

interface ObjectMaterializer {
    CompletionStage<MaterializationResult> materialize(MaterializationTask task);
}
```

Rules:

- `PrimaryWalAppender` owns bytes durability only.
- `StableOffsetCommitter` owns stable offset and protocol projection, not object upload.
- `ObjectMaterializer` owns background object publication and generation replacement.
- `ReadPathSelector` decides whether a read should use primary WAL or a published object generation.
- All profiles use the same `StreamId`, `OffsetRange`, `AppendSession`, `ProjectionRef`, checksum, and
  key-encoding helpers from `nereus-api`.

## 4. AutoMQ-like Append State Machine

Recommended state machine:

```text
ACCEPT_APPEND
  -> ENSURE_APPEND_SESSION
  -> APPEND_PRIMARY_WAL
  -> COMMIT_STABLE_OFFSET
  -> ACK_PRODUCER
  -> ENQUEUE_MATERIALIZATION
  -> MATERIALIZE_OBJECT_RANGE
  -> PUBLISH_OBJECT_GENERATION
  -> RELEASE_WAL_RETENTION
```

State rules:

- `APPEND_PRIMARY_WAL` writes BookKeeper or Object WAL and verifies checksum.
- `COMMIT_STABLE_OFFSET` must produce stable `streamId + offset` and Pulsar/Kafka projection metadata.
- `ACK_PRODUCER` must not wait for `MATERIALIZE_OBJECT_RANGE`.
- `PUBLISH_OBJECT_GENERATION` never changes offsets; it only changes where future reads resolve.
- `RELEASE_WAL_RETENTION` is allowed only after cursor/reader/repair references no longer need the
  primary WAL range.

For the current Oxia Java client constraint, `COMMIT_STABLE_OFFSET` should still use the Phase 1
stream-head single-key CAS plus commit-log protocol. The AutoMQ-like difference is that the append path
does not wait for read-optimized object generation publication.

## 5. Metadata Model

AutoMQ-like needs two durable states per committed append range:

```text
stable append truth:
  stream head / commit-log
  committedEndOffset
  projection ref
  primary WAL location

materialization truth:
  materialization task/checkpoint
  published object generation
  source WAL range references
  lag and retry state
```

Suggested Oxia key additions:

```text
/streams/{streamId}/materialization/checkpoint
/streams/{streamId}/materialization/tasks/{taskId}
/streams/{streamId}/wal-ranges/{offsetEnd}/{commitVersion}
/streams/{streamId}/object-generations/{offsetEnd}/{generation}
```

The existing offset-index generation overlay can be reused if a primary WAL range is also represented as
a generation-0 read target. The implementation should avoid a second read-index namespace unless the
first M4/M5 code proves the overlay cannot express primary WAL fallback cleanly.

## 6. Read Path

Read resolution order:

1. Resolve the requested offset from Oxia or validated cache.
2. Prefer the highest published object generation that covers the range.
3. If no object generation is published, read the primary WAL range.
4. Never infer visibility from object-store list.
5. Surface materialization lag separately from read correctness.

For `BOOKKEEPER_WAL_ASYNC_OBJECT`, read-before-materialization requires a BK range reader. For
`OBJECT_WAL_ASYNC_OBJECT`, read-before-materialization uses the object WAL reader and may later switch to
read-optimized compacted objects after generation publish.

## 7. Remote Storage Compatibility

Nereus must own the final policy mapping:

- If `BOOKKEEPER_WAL_ONLY`, Pulsar offload or Kafka Remote Log Storage may be allowed as legacy behavior,
  but it is not Nereus object truth.
- If any `*_SYNC_OBJECT` or `*_ASYNC_OBJECT` profile is enabled, Nereus object materialization owns remote
  objects for that topic.
- Kafka Remote Log Storage metadata should project to Nereus materialized generations, not create another
  remote log.
- Pulsar offload configuration should either be translated into Nereus object policy or rejected with a
  clear configuration error.

## 8. Backpressure And Operations

Async object materialization must be observable and enforceable:

| Signal | Meaning |
| --- | --- |
| `materializationLagRecords` | committed records not yet published as object-backed generation |
| `materializationLagBytes` | WAL bytes still protected by materialization lag |
| `oldestUnmaterializedAgeMs` | retention risk and recovery window |
| `materializationRetryTotal` | object upload or publish retries |
| `primaryWalRetentionBlockedTotal` | WAL ranges retained because materialization or readers still need them |

Backpressure triggers:

- materialization lag exceeds topic policy;
- primary WAL retention window is close to capacity;
- materializer retry rate indicates object-store outage;
- read-before-materialization load exceeds configured primary WAL read budget.

## 9. Failure Model

| Failure | Recovery |
| --- | --- |
| Broker crashes before WAL durable | no acknowledged data |
| Broker crashes after WAL durable before stable offset commit | retry from WAL attempt if identity is recoverable, otherwise no ack |
| Broker crashes after stable offset commit before ack | data is visible through primary WAL; client retry relies on upper-layer dedup |
| Materializer fails before object upload | task retry |
| Object upload succeeds before generation publish | orphan or retry reuse by deterministic task id |
| Generation publish succeeds before checkpoint update | repair from offset index/generation |
| WAL GC races materialization | blocked by `WalRetentionGate` until published generation and reader/cursor references are safe |

## 10. Implementation Gate

Before implementing AutoMQ-like code, review and freeze:

- profile config mapping at broker, namespace, and topic level;
- how `DurabilityLevel.WAL_DURABLE` returns stable offsets without weakening fencing;
- primary WAL range read support for BK and Object WAL;
- materialization task idempotency and checkpoint records;
- retention interaction between WAL ranges, cursors, readers, and object generations;
- remote storage/offload compatibility behavior;
- metrics and backpressure thresholds.
