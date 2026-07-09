# Nereus Terminology

> Nereus uses stream terminology for internal correctness. Pulsar ledger terms and Kafka log terms
> are protocol projections, not storage truth.

## 1. Core Terms

| Term | Meaning | Layer |
| --- | --- | --- |
| Nereus | Pulsar-native shared-storage streaming engine built on Oxia and shared object storage | product |
| Stream | Internal durable ordered record sequence for a topic partition | L0 |
| `streamId + offset` | The only internal truth for ordering, visibility, cursor progress, trim, compaction, and lakehouse | L0 |
| Stream offset | Dense, monotonically increasing record offset assigned at Oxia commit time | L0 |
| Append session | Oxia-fenced write session for a stream | L0/L4 |
| Fencing token | Monotonic token checked on visible commit | L0/L4 |
| BookKeeper WAL | BookKeeper-backed primary WAL used by latency-oriented profiles | L0 |
| Object WAL | Write-ahead log stored in object storage | L0 |
| Storage profile | Topic/stream-level choice of primary WAL and object materialization mode | L0/L1/L2 |
| Ursa-like profile | Append profile that waits for WAL durability plus synchronous Oxia-visible commit before ack | L0 |
| AutoMQ-like profile | Append profile that acks after primary WAL durability and lets background workers materialize read-optimized object ranges | L0/L3 |
| Object materialization | Copying or transforming WAL ranges into object-backed read/retention/lakehouse ranges | L0/L3 |
| Multi-stream WAL object | One physical object containing slices for multiple streams | L0 |
| Stream slice | The part of a WAL object belonging to one stream | L0 |
| Offset index | Oxia index mapping stream offset ranges to physical object ranges | L0 |
| Object manifest | Oxia metadata describing object identity, checksum, format, references, and visibility | L0 |
| Read resolver | Component that maps `streamId + offset` to WAL/compacted object range and entry index | L0 |
| Compacted object | Read-optimized per-stream object that preserves offsets | L3 |
| Generation replacement | Compaction mechanism that changes offset index targets without changing offsets | L3 |
| SBT | Stream-Backed Table: built-in table view backed by committed stream objects | L3 |
| SDT | Stream-Delivered-to-Table: delivery into external table/catalog targets | L3 |
| Preferred broker | Routing/locality hint for cache and batching, not durable ownership | L4 |
| Brown-out | State where a broker is degraded and removed from preferred routing before hard failure | L4 |

## 2. Pulsar Projection Terms

| Term | Nereus Meaning |
| --- | --- |
| ManagedLedger facade | Compatibility layer that lets Pulsar broker runtime use Nereus storage |
| Ledger | Virtual ledger projection over committed stream ranges |
| Entry | Pulsar entry projection over one or more stream offsets |
| `Position(ledgerId, entryId)` | External Pulsar coordinate mapped to an entry offset range |
| `MessageId(ledgerId, entryId, batchIndex)` | Stable Pulsar protocol coordinate projected from `streamId + offset` |
| ManagedCursor | Cursor facade backed by Oxia cursor state and optional object snapshots |
| Mark-delete | Cursor committed offset, expressed internally as stream offset |
| Individual ack holes | Stream offset ranges or snapshots, not BookKeeper cursor ledger truth |

## 3. Kafka Projection Terms

| Term | Nereus Meaning |
| --- | --- |
| Kafka offset | Equal to stream record offset |
| Log end offset | Stream `committedEndOffset` |
| Fetch offset | `streamId + offset` resolved through offset index |
| Group offset | Oxia group offset state |
| Leader epoch | Projection of stream epoch / routing generation, not broker durable leadership |
| Transaction marker | Stream-visible marker plus Oxia transaction state |

## 4. Terms To Avoid

Avoid these phrases in Nereus design docs:

- "object storage decides visibility";
- "broker owns partition";
- "ledger is the storage truth";
- "Kafka log and Pulsar log are separate durable logs";
- "lakehouse commit is on the producer ack path";
- "object list is used for correctness";
- "local broker RocksDB stores durable cursor truth";
- "Nereus is only tiered storage";
- "Nereus is only a BookKeeper replacement";
- "Nereus is AutoMQ for Pulsar";
- "AutoMQ-like means object storage decides visibility";
- "WAL durable alone is enough to invent protocol offsets".

Preferred language:

- "Oxia is the offset and visibility authority";
- "broker is stateless for correctness";
- "ManagedLedger is a compatibility facade";
- "KoP is a protocol projection";
- "SBT/SDT are lakehouse projections over committed stream offsets";
- "object store stores bytes, not truth".
- "AutoMQ-like is an async object-materialization profile over the same stream truth".

## 5. Naming

Product name:

```text
Nereus
```

Storage class:

```properties
managedLedgerStorageClassName=nereus
```

Configuration prefix:

```properties
nereus.*
```

Internal module prefix:

```text
pulsar-nereus-*
```

## 6. Future 1 Reading Notes

When reviewing `pip/Nereus/nereus-future1-core-stream-storage.md`, keep these boundaries:

- `StreamStorage` is L0 and must stay protocol-neutral.
- `AppendResult` may include projection references, but projection references are not ordering truth.
- `OffsetIndexEntry` is the read and visibility truth for committed ranges.
- Object WAL durability is necessary but not sufficient for visibility.
- Read resolver must start from Oxia offset index or a validated cache.
- Cursor, KoP group, transaction pending ack, and lakehouse catalog are future projections over L0.
- `StorageProfile` selects the primary WAL and object materialization mode; it must not create a second
  offset truth.

## 7. Future 2 Reading Notes

When reviewing `pip/Nereus/nereus-future2-managed-ledger-facade.md`, keep these boundaries:

- Broker-level `managedLedgerStorageClassName` is the `ManagedLedgerStorage` implementation class.
- Policy-level `PersistencePolicies.managedLedgerStorageClassName` is the selected storage class name, such as `nereus`.
- `ManagedLedgerFactory` and `ManagedLedger` are compatibility facades.
- `ledgerId` in Nereus is a virtual projection id, not a BookKeeper ledger id.
- `Position` is stable only through virtual ledger projection.
- Full cursor semantics belong to Future 3, even if Future 2 exposes a basic cursor boundary.

## 8. Future 3 Reading Notes

When reviewing `pip/Nereus/nereus-future3-cursor-subscription.md`, keep these boundaries:

- `markDeleteOffset` is the first not cumulatively acknowledged stream offset.
- `readPositionOffset` is a dispatch/recovery hint, not ack truth.
- Individual acknowledgments are half-open stream offset ranges above mark-delete.
- Cursor snapshot objects are immutable; Oxia cursor state decides which snapshot is visible.
- Cursor low-watermark protects stream data from trim and GC.
- Key_Shared ordering, delayed delivery, pending ack transaction, and replicated subscription are Future 8 topics.

## 9. Future 4 Reading Notes

When reviewing compaction and generation replacement, keep these boundaries:

- Compaction never changes stream offsets.
- Generation replacement changes the object/index target for an offset range, not the logical range.
- Readers choose the highest visible generation that covers the requested offset.
- Old generations remain readable until cursor, reader, catalog, and task references are safe for GC.
- Pulsar topic compaction and Kafka topic compaction share the same generation replacement primitive.

## 10. Future 5 Reading Notes

When reviewing KoP/Kafka compatibility, keep these boundaries:

- Kafka offset is exactly the stream record offset.
- KoP must not introduce a second durable log.
- Kafka group offsets are not Pulsar subscription cursors, even though both use stream offsets.
- Kafka leader and leader epoch are protocol projections over routing/stream epoch state.
- Kafka transaction visibility must be derived from Nereus transaction state and stream markers.

## 11. Future 6 Reading Notes

When reviewing SBT/SDT lakehouse design, keep these boundaries:

- Lakehouse catalog commits are not on the producer ack path.
- Oxia offset index remains stream truth even when catalog snapshots lag.
- SBT is a Nereus-managed table view over committed stream offsets.
- SDT is external delivery and must be idempotent by stream range and delivery id.
- Catalog repair can reconstruct table metadata from Oxia offset index and committed objects.

## 12. Future 7 Reading Notes

When reviewing routing and elasticity, keep these boundaries:

- Preferred broker is a locality/cache/batching hint, not durable ownership.
- Broker session and routing ring live in Oxia metadata.
- Append session fencing, not routing preference, rejects stale commits.
- Brown-out changes routing state; it does not move durable data.
- Pulsar lookup and Kafka MetadataResponse must project the same routing truth.

## 13. Future 8 Reading Notes

When reviewing advanced Pulsar semantics, keep these boundaries:

- Key_Shared, delayed delivery, pending ack transaction, replicated subscription, schema/system topics,
  and geo-replication are Pulsar-native projections over L0/L1 state.
- None of these features may create a second durable log.
- Broker runtime state can optimize dispatch, but durable recovery state must live in Oxia or referenced
  snapshot objects.
- Geo-replication must make source/target stream offset translation explicit.
