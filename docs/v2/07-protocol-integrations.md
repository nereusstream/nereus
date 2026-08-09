---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Protocol integrations and product gates

## Storage Fabric boundary

One Nereus Storage Fabric may contain multiple Kafka and Pulsar Protocol Cells. Cells may use the same external Object
Storage or BookKeeper infrastructure and may share worker processes, compatible transport capacity, and observability.
Each cell nevertheless owns a distinct Cell Provider Scope/session, namespace, credential/KMS and operator scope,
admission/retry/circuit-breaker state, queue and cache accounting, task root, GC capability, drain, and close lifecycle.
Object groups do not cross cells in 0.2.

Sharing never makes control planes, positions, Native Write Authorities, provider sessions, or physical-delete authority
interchangeable. A cell-local close, throttle, credential failure, stale task, or GC request cannot mutate another cell.
An outage of intentionally shared physical infrastructure may still affect every attached cell. Dedicated provider
infrastructure is an optional deployment topology for stronger SLO, compliance, or physical-failure isolation.

| Protocol/profile path | Protocol position truth | Protocol Coverage | Physical Extent |
| --- | --- | --- | --- |
| Kafka / `OBJECT_WAL` | Kafka Offset | Kafka Offset Range | Object Extent |
| Kafka / BookKeeper profiles | Kafka Offset | Kafka Offset Range | BookKeeper Extent |
| Pulsar / BookKeeper profiles | Pulsar Position/MessageId | ledger-keyed Pulsar Coverage | BookKeeper Extent |
| Pulsar / `OBJECT_WAL` | Pulsar Position/MessageId | ledger-keyed Pulsar Coverage | Object Extent |

## Kafka native path

The Kafka fork retains stock Kafka protocol and state-machine semantics around a Nereus-backed log:

- KRaft topic/partition identity, controller epochs, leader changes, ISR/minISR, and reassignment;
- `UnifiedLog` append/fetch/list-offset/delete-records behavior;
- producer ID/epoch/sequence and duplicate handling;
- transaction visibility, markers, high watermark, and last stable offset;
- consumer groups and internal topics;
- topic compaction, retention, timestamp lookup, and leader-epoch queries;
- stock clients and admin APIs.

Nereus replaces the local segment durability path, not Kafka's protocol truth. Every Kafka Topic Protocol Binding keeps
Kafka Offset as its Position Domain across Storage Epochs. The Object profile absorbs useful AutoMQ patterns such as
group WAL, immutable objects, asynchronous materialization, bounded handoff hints, parallel timestamp lookup, and
inflight limits. The exact research commits are recorded in [source locks](source-locks.json); research is not executable
acceptance evidence.

V2 Kafka metadata is enabled only by fresh-bootstrap finalized `nereus.storage.version=2`; the V2 build supports only
`[2,2]`, rejects level-1 V1 state, and forbids runtime upgrade or downgrade. A successful native CreateTopics item
atomically publishes logical aggregate schema v1 with the topic records. Validate-only and native errors publish no
aggregate. One generated non-flexible typed record at API key 32000 is owned by `TopicImage`; completed snapshot order
places it between `TopicRecord` and that topic's partitions, and topic removal cascades without a second delete
authority. Ordinary MetadataLoader publication validates only touched topics; snapshot/bootstrap validates every live
topic.

### Kafka product target

“Stronger than AutoMQ” is evaluated per profile:

- `OBJECT_WAL`: with the same machine class, object-store conditions, replication/durability, record set, and Object
  request budget, no agreed correctness or compatibility regression is allowed; throughput/cost and p99 thresholds are
  pinned before M8, with at least one material product advantage demonstrated.
- BookKeeper profiles: lower latency is an explicit higher-cost option and is not presented as an equal-cost Object
  comparison.
- mixed topic profiles: isolation, fairness, recovery, and operations are first-class differentiators.

The acceptance source is an exact clean AutoMQ commit and receipt, never the moving word “latest”.

## Pulsar native path

`BOOKKEEPER_WAL_ONLY` must preserve the native ManagedLedger call path and feature behavior before Nereus-specific
advantages are counted. `BOOKKEEPER_WAL_ASYNC_OBJECT` uses ManagedLedger ledger/offload metadata as sole lifecycle
authority and offloads sealed non-current ledgers as one deterministic data/root Object pair through a Nereus
`LedgerOffloader`; persisted attempt metadata pins key derivation and a bounded root is verified through the real read
path before success. The root is bounded NPO1; native dual-source reads use at most one whole-range fallback, and exact
Object revalidation plus BK read-pin drain precedes BookKeeper-source deletion. NPO1 indexes independently verifiable,
gap-free NPD1 multi-entry blocks, and one ManagedLedger-owned composite handle owns Object/BK children and fallback.
`RETAIN_BK` keeps delete state NONE; `DELETE_AFTER_VERIFIED` irreversibly advances through BK_DELETE_INTENT/DONE, and
only DONE proves physical absence.
`OBJECT_WAL` uses an explicit ObjectManagedLedger path plus a
reserved-slice Pulsar-cell MetadataStore/Oxia virtual-ledger authority. One bounded deployment CAS registry proves all
slices non-overlapping and never reused. Slice owner identity is the immutable Pulsar Protocol Cell, retirement is
permanent, `k=40` plus 64 KiB/256-lifetime/192-byte-row bootstrap caps bound the registry, and 0.2 forbids every
resize/second-slice
path and fails closed at exhaustion. The profile accepts its cost-first
latency tradeoff. Every path keeps
`PulsarPosition(ledgerId, entryId)`, MessageId, and the ledger chain as protocol truth; Object/BookKeeper coordinates
remain Physical Extents. STRICT_SERIALIZED and RANGE_LEASED remain unselected; ADR 0055 requires actual rollover
distribution/storm/crash evidence and native Pulsar rollover/append-stall comparison while RANGE fencing/recovery is
designed in parallel. Broker-wide takeover is an explicit scale cut, and the rejected owner-scoped range design cannot
reacquire every ManagedLedger range through one serialized allocator.

Object frames retain exact assigned Kafka RecordBatch bytes or exact Pulsar ManagedLedger entry bytes after only the
outer Object envelope is decoded. Kafka makes all frames from one partition storage append an all-or-none commit set;
Pulsar makes one entry one frame/commit set. CRC32C/v1 protects each protocol-native frame blob while the native protocol
checksum remains independently validated. NWG1 stores its authoritative binding contexts and append-unit directory in
the Object body; one Kafka commit set never spans ObjectExtents and every frame block is independently decoded. NWG1
uses one KMS-wrapped run key, domain-separated per-Object keys, and disjoint authenticated directory/frame nonce
domains. Immutable Root and Seal records live in Cell control metadata; one exact CAS advances the current-run pointer.
Checkpoint pages are Cell x shard scoped async accelerators; uncovered open tails always require bounded strong LIST,
and the sealed final gap-free inventory cannot be disabled.

The parity matrix covers at least:

- append/read and exact MessageId/Position behavior;
- batched entries, properties, schema, and checksum fidelity;
- durable subscriptions, individual/cumulative ACK, seek, reset, and backlog;
- retention, ledger rollover, unload, ownership transfer, broker restart, and BookKeeper recovery;
- topic compaction and offload fallback;
- transactions, delayed delivery, replication, deduplication, and system topics to the extent enabled by the selected
  runtime contract;
- admin and client compatibility against the pinned native Pulsar baseline.

The native baseline and thresholds are not pinned at M0. No V1 Pulsar receipt is inherited as V2 parity evidence.

## Kafka/Pulsar secondary access and authority migration

The same business data may be exposed through the other protocol only as an Access Projection backed by a durable
Projection Map. The target is not a second Native Write Authority. Changing protocol authority uses a Migration Link
between source and target Topic Protocol Bindings; it is not a Storage Epoch transition.

ADR 0016 retains that boundary and dual-authority rejection but excludes projection mapping, secondary-protocol serving,
semantic state transfer, and authority-migration runtime from 0.2. Their detailed questions are deferred and do not
block the release. No design or implementation may use a universal logical offset as a shortcut or allow simultaneous
Kafka and Pulsar native writers.

## KoP

KoP is intentionally outside the 0.2 runtime and release gates. Its existing design is retained at
[KoP/Kafka compatibility](../design/nereus-future5-kop-compatibility.md) with status “Designed / deferred”. V2 must not
delete that design or claim its payload/coordinator mapping is implemented. Before activation it requires a fresh audit
against V2 bindings, protocol-native Kafka work, and the then-current KoP source.

Relevant tradeoffs: `T-PROTOCOL-01`, `T-MULTIPROTOCOL-01`, `T-FABRIC-01`, `T-POLICY-01`, `T-PROJECTION-01`,
`T-BENCH-01`, and `T-KOP-01`. Required scenarios: `V2-MULTIPROTOCOL-001`, `V2-FABRIC-001..003`,
`V2-PROJECTION-001`, `V2-POSITION-002..010`, `V2-OBJ-004..016`, `V2-BK-005..013`,
`V2-KAF-META-001..003`, `V2-POLICY-001`, `V2-KAF-001`, `V2-PUL-001`, and `V2-KOP-001`.
