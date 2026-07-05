# Nereus Design Index

> Nereus 当前处于整体设计阶段。本文定义文档入口、术语边界和推进顺序，避免把
> prototype、validation、benchmark、chaos 材料误认为当前主线。

## 1. Product Definition

Nereus 是一个对标 Ursa 架构范式的 Pulsar-native shared-storage streaming engine：

```text
Pulsar native protocol
+ KoP Kafka compatibility
+ Oxia metadata and coordination plane
+ shared object data plane
+ stateless / leaderless broker correctness
+ stream offset index
+ multi-stream WAL object
+ compaction and lakehouse-native stream-table duality
```

Nereus 的内部真相只有一个：

```text
streamId + offset
```

Pulsar `MessageId`、ManagedLedger `Position`、Kafka offset、cursor progress、
transaction visibility、retention、compaction 和 lakehouse snapshot 都必须投影到这个
内部坐标，或者从这个内部坐标派生。

## 2. Current Stage

当前阶段只做整体设计冻结：

- north-star architecture；
- Ursa parity matrix；
- Pulsar-native enhancement matrix；
- L0-L4 layer boundary；
- metadata ownership；
- object format；
- commit protocol；
- read path；
- compaction and GC model；
- routing and elasticity model；
- future split。

当前阶段不做：

- cloud benchmark；
- chaos/Jepsen/Maelstrom；
- real evidence collection；
- CI required gate；
- Kafka compatibility certification；
- production profile claim。

这些工作在 future 设计冻结后进入验证阶段。

## 3. Authoritative Design Docs

| Doc | Role | Stage |
| --- | --- | --- |
| `pip/Nereus/nereus-overall-architecture.md` | Nereus north-star architecture and Ursa parity | current |
| `pip/Nereus/nereus-futures.md` | Future split and module boundaries | current |
| `pip/Nereus/nereus-terminology.md` | Shared vocabulary and banned ambiguity | current |
| `pip/Nereus/nereus-future1-core-stream-storage.md` | Future 1 L0 Core StreamStorage + Object WAL design | current future design |
| `pip/Nereus/nereus-future2-managed-ledger-facade.md` | Future 2 Pulsar ManagedLedger facade design | current future design |
| `pip/Nereus/nereus-future3-cursor-subscription.md` | Future 3 cursor/subscription durable progress design | current future design |
| `pip/Nereus/nereus-future4-compaction-generation.md` | Future 4 compaction and generation replacement design | current future design |
| `pip/Nereus/nereus-storage-object-format.md` | Nereus object/index/table format for Future 1/4/6/8 | current design draft |
| `pip/Nereus/nereus-commit-protocol.md` | Nereus append/compaction/cursor/txn commit protocol for Future 1/3/4/6/8 | current design draft |
| `pip/Nereus/nereus-future5-kop-compatibility.md` | Future 5 KoP/Kafka projection on Nereus storage | current design draft |
| `pip/Nereus/nereus-future6-lakehouse-sbt-sdt.md` | Future 6 lakehouse SBT/SDT design | current future design |
| `pip/Nereus/nereus-future7-routing-brownout-elasticity.md` | Future 7 routing, brown-out, and elasticity design | current design draft |
| `pip/Nereus/nereus-future8-advanced-pulsar-semantics.md` | Future 8 advanced Pulsar semantics design | current future design |

Current design documents use the `nereus-` prefix. Future-specific detailed docs use
`nereus-futureN-*` names so the implementation sequence is visible from the file name.

## 4. Later-stage Materials

The following materials are useful later, but they are not the current design source of truth:

- prototype slice gates；
- real evidence contracts and collectors；
- validation suite；
- simulation reports；
- TLA+ reports；
- benchmark/chaos runbooks；
- CI workflows for evidence collection。

They should be reintroduced after the relevant future design has been reviewed and frozen.

## 5. Reading Order

1. Read `pip/Nereus/nereus-overall-architecture.md` for the full product architecture.
2. Read `pip/Nereus/nereus-terminology.md` before writing or reviewing future docs.
3. Read `pip/Nereus/nereus-futures.md` to understand module boundaries.
4. Read `pip/Nereus/nereus-future1-core-stream-storage.md` for Future 1 L0 details.
5. Read `pip/Nereus/nereus-future2-managed-ledger-facade.md` for Future 2 Pulsar facade details.
6. Read `pip/Nereus/nereus-future3-cursor-subscription.md` for Future 3 cursor/subscription details.
7. Read `pip/Nereus/nereus-future4-compaction-generation.md` for Future 4 compaction details.
8. Read `pip/Nereus/nereus-future5-kop-compatibility.md` for Future 5 Kafka compatibility details.
9. Read `pip/Nereus/nereus-future6-lakehouse-sbt-sdt.md` for Future 6 lakehouse details.
10. Read `pip/Nereus/nereus-future7-routing-brownout-elasticity.md` for Future 7 elasticity details.
11. Read `pip/Nereus/nereus-future8-advanced-pulsar-semantics.md` for Future 8 Pulsar-native details.
12. Defer validation and benchmark docs until a future explicitly enters verification.

## 6. Future Doc Rule

Each future design document must use this shape:

```text
1. Motivation
2. Scope
3. Non-scope
4. Layer boundary
5. API
6. Oxia metadata schema
7. Object or snapshot format
8. State transition
9. Failure model
10. Compatibility impact
11. Future gate
```

`Future gate` means the design-level exit criteria for moving into implementation or verification.
It does not mean benchmark results are required in the overall design stage.
