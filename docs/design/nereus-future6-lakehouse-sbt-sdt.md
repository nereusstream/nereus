# Nereus Future 6：Lakehouse SBT / SDT

> 状态：Designed；当前仓库没有 catalog/table implementation
> 前置：Future 4 compacted generation、schema/projection preservation、GC references

本文定义 Nereus L3 lakehouse 设计。Future 6 把 lakehouse 作为 Nereus 的一等能力：
> SBT 是 Nereus-managed table view，SDT 是 external table delivery。Lakehouse catalog
> 不参与 producer ack。Stream head + reachable commit log 决定 logical append truth；generation-aware
> offset index决定 table file 所引用的 physical source。

## 1. Motivation

Ursa-like 架构的关键价值之一是 stream-table duality：同一份 committed stream data 可以服务
streaming read，也可以服务 analytical table query。

Future 6 的目标是：

- 让 Nereus 内建 Stream-Backed Table；
- 支持向外部 catalog/table 做 Stream-Delivered-to-Table；
- 明确 Iceberg-first、Delta/Hudi adapter boundary；
- 让 catalog lag、partial commit、repair 都不影响 stream visibility；
- 让 lakehouse snapshot 能追溯到 Oxia offset index generation。

## 2. Scope

Future 6 覆盖：

- Stream-Backed Table (SBT)；
- Stream-Delivered-to-Table (SDT)；
- Iceberg catalog commit；
- Delta/Hudi adapter boundary；
- catalog snapshot metadata；
- catalog repair；
- delivery idempotence；
- table lag metrics；
- table object reference protection。

## 3. Non-scope

Future 6 不解决：

- producer ack 依赖 lakehouse catalog；
- 外部 query engine 写回 stream object；
- 每个 catalog 的深度优化；
- 完整 SQL engine；
- 用户自定义 ETL runtime；
- benchmark、real table workload、catalog compatibility certification。

当前实现边界：`STREAM_COMPACTED_OBJECT` 只是 enum/format reservation，higher generation、Parquet
writer 和 catalog adapter 都不存在。本文件中的 API/key/schema 均是 target pseudo-code。

## 4. Layer Boundary

Future 6 位于 L3：

```text
Committed stream offsets
  -> compacted object / table file
  -> SBT managed catalog snapshot
  -> SDT external delivery metadata
  -> catalog repair and audit
```

Future 6 可以做：

- 读取 Oxia offset index 和 compacted object manifest；
- 生成 table metadata；
- 提交 Iceberg catalog snapshot；
- 记录 SBT snapshot state；
- 向外部 catalog 做 SDT delivery；
- 维护 delivery idempotence；
- 根据 Oxia truth repair catalog lag。

Future 6 不能做：

- 让 catalog snapshot 决定 stream visibility；
- 让 catalog commit 进入 producer ack path；
- 让外部 table 覆盖 stream object；
- 从 object list 推断 stream truth；
- 删除仍被 catalog snapshot 引用的 object。

## 5. Concepts

| Concept | Meaning |
| --- | --- |
| SBT | Stream-Backed Table，Nereus 管理的内建表视图 |
| SDT | Stream-Delivered-to-Table，向用户外部 catalog/table 投递 stream range |
| Source range | `[offsetStart, offsetEnd)` stream offset range |
| Index generation | Oxia offset index generation used by a table snapshot |
| Catalog snapshot | Iceberg/Delta/Hudi snapshot visible to query engine |
| Delivery id | deterministic idempotence key for SDT |
| Catalog lag | stream committed end offset minus latest table-visible offset |

## 6. Internal API

```java
interface StreamBackedTableService {
    CompletableFuture<SbtSnapshot> publishSnapshot(
            StreamId streamId,
            OffsetRange range,
            SbtPublishOptions options);
}

interface StreamDeliveredTableService {
    CompletableFuture<SdtDeliveryResult> deliver(
            StreamId streamId,
            OffsetRange range,
            SdtTarget target,
            SdtDeliveryOptions options);
}

interface CatalogRepairService {
    CompletableFuture<RepairResult> repair(StreamId streamId, CatalogTarget target);
}
```

All APIs use stream offsets and index generation as input. None of them allocate stream offsets.

## 7. Oxia Metadata Schema

```text
/nereus/clusters/{cluster}/streams/{streamId}/sbt/config
/nereus/clusters/{cluster}/streams/{streamId}/sbt/snapshots/{snapshotId}
/nereus/clusters/{cluster}/streams/{streamId}/sbt/checkpoints/{committerId}

/nereus/clusters/{cluster}/streams/{streamId}/sdt/targets/{targetId}/config
/nereus/clusters/{cluster}/streams/{streamId}/sdt/targets/{targetId}/deliveries/{deliveryId}
/nereus/clusters/{cluster}/streams/{streamId}/sdt/targets/{targetId}/checkpoints/{committerId}

/nereus/clusters/{cluster}/objects/{objectId}/references/catalog/{snapshotId}
```

### 7.1 SBT snapshot

```json
{
  "streamId": "s-123",
  "tableName": "tenant.namespace.topic.partition0",
  "catalog": "iceberg",
  "snapshotId": "sbt-s-123-42",
  "catalogSnapshotId": 202607030001,
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "indexGeneration": 18,
  "dataFiles": [
    {
      "objectId": "co-s-123-1048576-1114112-g18",
      "path": "s3://bucket/compacted/s-123/1048576-1114112-g18.parquet",
      "format": "PARQUET",
      "recordCount": 65536,
      "fileSizeBytes": 67108864
    }
  ],
  "state": "COMMITTED",
  "commitVersion": 92
}
```

### 7.2 SDT delivery

```json
{
  "streamId": "s-123",
  "targetId": "customer-iceberg-analytics-events",
  "deliveryId": "sha256:...",
  "targetCatalog": "customer-iceberg",
  "targetTable": "analytics.events",
  "offsetStart": 1048576,
  "offsetEnd": 1114112,
  "sourceIndexGeneration": 18,
  "state": "COMMITTED",
  "retryCount": 0,
  "lastError": null
}
```

Delivery id：

```text
deliveryId = hash(streamId, offsetStart, offsetEnd, targetCatalog, targetTable, schemaVersion)
```

## 8. Object / Table Format

Future 6 consumes compacted objects from Future 4. The default table file format is Parquet.

Required table columns:

| Column | Meaning |
| --- | --- |
| `stream_id` | Nereus stream id |
| `offset` | stream record offset |
| `publish_time` | Pulsar/Kafka publish time |
| `event_time` | event time if present |
| `key` | message key |
| `payload` | payload or normalized row field |
| `schema_id` | schema reference |
| `producer_name` | producer metadata |
| `sequence_id` | producer sequence |
| `txn_id` | transaction id if present |
| `headers` | Pulsar properties / Kafka headers projection |

SBT table metadata must store:

- `streamId`；
- `offsetStart/end`；
- `indexGeneration`；
- source compacted object ids；
- schema snapshot；
- catalog snapshot id。

## 9. Commit Protocol

### 9.1 SBT commit

Recommended order：

```text
1. Read Oxia offset index generation.
2. Ensure compacted objects for target range exist and checksum-valid.
3. Commit Iceberg catalog snapshot.
4. Write Oxia SBT snapshot state.
5. Add catalog object references for GC protection.
```

Catalog commit success does not make stream data visible; stream data was already visible by Oxia offset
index. Catalog commit only makes table query see the range.

### 9.2 SDT commit

```text
1. Select committed stream range.
2. Generate deterministic delivery id.
3. Write or reuse target table files.
4. Commit external catalog idempotently.
5. Write Oxia SDT delivery state.
```

SDT failure never rolls back stream visibility.

## 10. Repair

Repair must handle partial commit:

| State | Repair action |
| --- | --- |
| Oxia offset index has compacted object, catalog missing | Commit or recommit catalog snapshot |
| Catalog snapshot exists, Oxia SBT state missing | Reconstruct SBT state from catalog snapshot metadata |
| Oxia SBT state exists, catalog snapshot missing | Mark `NEEDS_REPAIR` and recommit catalog |
| SDT external commit timeout | Query target catalog by delivery id |
| SDT Oxia state missing after external success | Insert delivery state with same delivery id |
| Catalog references object no longer active in highest generation | Keep reference until catalog snapshot expires or is superseded |

Repair source order：

```text
stream head + reachable commit log
  > generation-aware offset index
  > object manifest
  > catalog snapshot/delivery lineage
  > object list
```

Object list is never a correctness source.

## 11. Lag and Metrics

SBT lag:

```text
sbtLagRecords = stream.committedEndOffset - latestSbtSnapshot.offsetEnd
```

SDT lag:

```text
sdtLagRecords = stream.committedEndOffset - latestCommittedDelivery.offsetEnd
```

Suggested metrics:

| Metric | Meaning |
| --- | --- |
| `pulsar_nereus_sbt_snapshot_commit_latency_seconds` | SBT catalog commit latency |
| `pulsar_nereus_sbt_lag_records` | SBT lag by records |
| `pulsar_nereus_sbt_lag_seconds` | SBT materialization lag |
| `pulsar_nereus_sdt_delivery_latency_seconds` | SDT delivery latency |
| `pulsar_nereus_sdt_lag_records` | SDT lag by target |
| `pulsar_nereus_catalog_repair_total` | repair count |
| `pulsar_nereus_catalog_commit_conflicts_total` | catalog commit conflicts |

## 12. Failure Model

| Failure | Expected behavior |
| --- | --- |
| Catalog commit fails before visibility | Stream read unaffected; table lag increases |
| Catalog commit succeeds but Oxia SBT state write fails | Repair reconstructs Oxia state |
| Oxia SBT state succeeds but catalog commit missing | State becomes `NEEDS_REPAIR`; stream read unaffected |
| SDT target catalog timeout | Delivery id used to determine success before retry |
| Duplicate SDT retry | Same delivery id must be idempotent |
| Schema evolves during range selection | Snapshot records schema version; next range may use new schema |
| Object referenced by catalog is superseded | Catalog reference protects object until snapshot expiry |
| External query engine reads stale snapshot | Allowed; stream-head committed end may be newer and catalog lag remains observable |

## 13. Compatibility Impact

### Pulsar

- SBT/SDT do not change Pulsar producer ack, consumer dispatch, cursor, or retention semantics.
- Pulsar schema metadata can be used to build table schema snapshots.
- Topic policies can enable or disable SBT/SDT per namespace/topic.

### KoP / Kafka

- Kafka offset remains stream offset.
- Kafka compacted topics can share Future 4 compacted objects, but table visibility remains Future 6.
- Kafka headers and keys map into table columns through projection.

### Operations

- Operators must be able to see stream lag and table lag separately.
- Catalog repair must be explicit and auditable.
- Object GC must include catalog references.

## 14. Future Gate

Future 6 may enter implementation planning only after the following are reviewed:

- SBT/SDT metadata schema；
- Iceberg-first catalog commit order；
- Delta/Hudi adapter boundary；
- deterministic SDT delivery id；
- catalog repair state machine；
- table file required columns；
- object reference protection；
- metrics for stream lag vs table lag。

This is a design gate. It does not require benchmark, catalog certification, CI, or real evidence.
