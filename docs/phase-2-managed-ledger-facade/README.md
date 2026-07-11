# Phase 2 ManagedLedger Facade Detailed Design

本文档目录是 Future 2 的 active code-level design。F2-M0 已在 2026-07-11 完成设计收敛和
Pulsar API spike；生产 facade 仍未实现，当前实现里程碑是 F2-M1。

Future 2 的目标是在不改变 L0 storage truth 的前提下，为 Pulsar broker 提供
`ManagedLedgerStorageClass(name=nereus) -> ManagedLedgerFactory -> ManagedLedger` 兼容路径。
首个可执行版本只接受 `OBJECT_WAL_SYNC_OBJECT`；BookKeeper storage class 与 Nereus storage
class 可以在 broker 内共存，但这不表示 Nereus 的 BookKeeper primary profile 已实现。

## 1. Locked Inputs

| Input | F2-M0 lock |
| --- | --- |
| Nereus repository | `nereusstream/nereus@ad8c272787fe77f908397515864ef1f72945e8ee` |
| Pulsar fork | `nereusstream/pulsar` |
| Pulsar fork commit | `100d3ef0ff7c7da36d497453b141ddff6f34a9d3` |
| Pulsar version at that commit | `5.0.0-M1-SNAPSHOT` |
| Java/build baseline | Pulsar JDK 21 or 25; Nereus Java 21 |
| Executable Nereus profile | `OBJECT_WAL_SYNC_OBJECT` only |

The local Pulsar checkout used for the compile spike was
`apache/pulsar@320fbce6d540b618d35f1dd374e0aaf5fbd3c35c`. The five locked interface
files have the same Git blob IDs in that checkout and the target fork commit, so the successful local
compile probe is evidence for the locked target API rather than a nearby-source approximation. Exact
blob IDs and the reproducible command are in
`01-pulsar-api-spike-and-repository-boundary.md`.

An upgrade to another Pulsar commit invalidates the lock. The API probe and broker integration call-site
audit must pass again before implementation continues.

## 2. F2-M0 Decisions

1. A Pulsar managed-ledger name is mapped to the L0 `StreamName`
   `"pulsar-ml-v1\0" + managedLedgerName`. The broker passes
   `TopicName.getPersistenceNamingEncoding()`; the facade does not reconstruct or normalize a topic URI.
2. F2 v1 creates one stable virtual ledger for one stream and does not roll it. A positive, high-range
   `virtualLedgerId` is allocated through one Oxia allocator key. The value is a compatibility coordinate,
   never a BookKeeper ledger ID.
3. One Pulsar Entry maps to one `AppendEntry(recordCount=1)` and therefore one stream offset.
   `Position.entryId == stream offset`. Pulsar `batchIndex` remains a sub-index inside the entry and does
   not consume another stream offset.
4. Topic projection is the authoritative F2 record. Virtual-ledger and position-index records are
   idempotently repairable derived records. No workflow assumes a multi-key atomic Oxia transaction.
5. Position metadata is formula-based, not per-entry: mapping version 1 is
   `offset = entryId`. F2 does not add one Oxia write per append.
6. Broker hybrid mode means stock `bookkeeper` and Nereus `nereus` storage classes coexist. The
   Nereus facade rejects every non-object-WAL Nereus profile before opening a topic.
7. Durable mark-delete, individual acknowledgements and ack holes remain Future 3. F2 can support
   read-only and non-durable cursor flows; durable cursor mutation must fail explicitly.
8. `terminate` and logical `delete` need explicit L0 lifecycle operations. F2-M3 cannot synthesize these
   only in facade metadata.
9. All asynchronous callbacks are terminal exactly once. Callback invocation is outside locks and on a
   designated callback executor. A method returning `CompletableFuture` never throws synchronously.
10. Virtual ledger IDs are never sent to a BookKeeper client, ledger handle, offloader or BookKeeper
    metadata path. Unsupported offload/BookKeeper-shaped operations fail explicitly.

## 3. Repository Boundary

| Repository/module | Ownership |
| --- | --- |
| `nereus-api` | Minimal protocol-neutral lifecycle additions required by facade; no Pulsar types |
| `nereus-metadata-oxia` | F2 keyspace, records, codecs, fake/real projection metadata contract |
| `nereus-managed-ledger` | Projection model, entry codec, factory, ledger, entry/read-only/non-durable cursor facade |
| `nereus-pulsar-adapter` | Product-owned configuration/bootstrap helpers that do not depend on Pulsar private internals |
| `nereusstream/pulsar` fork | `ManagedLedgerStorage` hybrid provider, broker config/distribution wiring, policy and broker integration tests |

`ManagedLedgerStorage` and `ManagedLedgerStorageClass` are Pulsar `@Private @Unstable` interfaces.
The class that composes stock `ManagedLedgerClientFactory` with the Nereus factory therefore belongs in
the Pulsar fork. Stable product logic stays in Nereus.

## 4. Document Map

| Document | Purpose |
| --- | --- |
| `01-pulsar-api-spike-and-repository-boundary.md` | Locked API evidence, broker call sites and code ownership |
| `02-projection-and-entry-contract.md` | Virtual ledger, Position, batch and Entry byte contracts |
| `03-oxia-metadata-and-recovery.md` | Keyspace, records, single-key CAS creation and repair protocol |
| `04-facade-state-machines-and-compatibility.md` | Factory/ledger/cursor lifecycle, callbacks, errors and unsupported surface |
| `05-implementation-plan-and-gates.md` | F2-M1 through F2-M6 files, tests, gates and completion criteria |
| `spikes/PulsarManagedLedgerApiProbe.java` | Compile-only exact-signature probe |
| `spikes/pulsar-api-probe.init.gradle` | Temporary Pulsar source-set injection; does not edit the Pulsar checkout |

## 5. F2-M0 Evidence

From the locked-compatible Pulsar checkout:

```bash
NEREUS_F2_PROBE_DIR=/path/to/nereus/docs/phase-2-managed-ledger-facade/spikes \
  ./gradlew \
  -I /path/to/nereus/docs/phase-2-managed-ledger-facade/spikes/pulsar-api-probe.init.gradle \
  :pulsar-broker:compileF2ApiProbeJava
```

Observed on 2026-07-11:

```text
> Task :managed-ledger:compileJava
> Task :pulsar-broker:compileJava
> Task :pulsar-broker:compileF2ApiProbeJava
BUILD SUCCESSFUL
```

The spike proves compilation of the selected storage/factory/append/read/terminate/cursor boundary. It
does not prove implementation behavior, callback ordering, resource ownership or broker integration.
Those are later milestone gates.

## 6. Milestone State

| Milestone | State | Exit |
| --- | --- | --- |
| F2-M0 design/API spike | Complete | This directory, locked target and successful compile probe |
| F2-M1 projection model | Next | Pure model/codec implementation and restart-stable mapping tests |
| F2-M2 projection metadata | Not started | Fake/real Oxia contract and crash-repair tests |
| F2-M3 ManagedLedger facade | Not started | Factory/ledger append/read/lifecycle and exactly-once callback tests |
| F2-M4 cursor boundary | Not started | Read-only/non-durable cursor; explicit durable mutation rejection |
| F2-M5 broker integration | Not started | Hybrid storage provider and broker load/unload/restart tests |
| F2-M6 final acceptance | Not started | Real Pulsar + Oxia + Object WAL end-to-end gate |

Future 2 is not complete until F2-M1 through F2-M6 are implemented and their gates pass.
