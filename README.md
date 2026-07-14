# Nereus

Nereus is a Pulsar-native shared-storage streaming engine built around an Oxia
metadata/coordination plane, selectable primary-WAL/object-materialization profiles,
a shared object data plane, broker-locality without durable broker ownership, and a
single logical `streamId + offset` coordinate.

The project website and namespace authority is `nereusstream.com`; Java packages and Maven coordinates use
`com.nereusstream`. This is the standalone product repository for `github.com/nereusstream/nereus`.
Pulsar and KoP changes are developed in organization-owned fork repositories,
not as patch overlays inside this repo.

## Repository Layout

```text
nereus/
  gradle/                               Gradle wrapper and version catalog
  nereus-api/                           public internal APIs and value types
  nereus-core/                          L0 StreamStorage core
  nereus-metadata-oxia/                 Oxia metadata and coordination adapter
  nereus-object-store/                  object WAL / object IO boundary
  nereus-managed-ledger/                ManagedLedger facade implementation boundary
  nereus-pulsar-adapter/                Pulsar broker integration boundary
  nereus-kop-adapter/                   KoP/Kafka projection boundary
  docs/design/                          north-star and capability-track designs
  docs/phase-1-core-stream-storage/     implemented Phase 1 contracts and milestones
  docs/phase-1.5-core-storage-foundation/ implemented L0 evolution and gates
  docs/phase-2-managed-ledger-facade/   F2 code-level contracts, API spike and milestones
  docs/phase-3-cursor-subscription/      F3 code-level design and M0/M0R gates
  docs/automq-like-stream-storage/      reserved async materialization profile design
```

## Related Organization Repositories

Expected upstream forks:

```text
github.com/nereusstream/pulsar  -> fork of apache/pulsar
github.com/nereusstream/kop     -> fork of streamnative/kop
```

The main Nereus repository holds product-owned modules and authoritative design documents.
Forks hold changes that must land inside upstream Pulsar or KoP trees.

## Current Phase

Future 1 / Phase 1 Core StreamStorage M0-M8 is complete:

- protocol-neutral API/value/error contracts；
- Oxia key/record/codec, fake metadata, and production Oxia Java adapter；
- Object WAL v1 writer/reader, entry index, checksums, and resource guards；
- append, resolve/read, trim/recovery, restart, and post-head repair state machines；
- ordinary and Docker-backed final acceptance gates。

Only `OBJECT_WAL_SYNC_OBJECT` is a Phase 1 execution target. BookKeeper and async
materialization profiles are reserved design/API boundaries, not implemented support.

Future 2 F2-M0/M0R/M0R2 design and Phase 1.5 prerequisites are complete. P15-M0-M6 and F2-M1-M6 are implemented/final-gated。
`nereus-managed-ledger` now provides the
writable facade、strict get-only read-only ledger、exact append recovery/write-fence handoff、lifecycle/admin/stats
surfaces and audited unsupported channels。F2-M4 cursor boundary is implemented；F2-M5 has product runtime/S3
gates plus the fork hybrid bootstrap，durable `NSB1` binding open/delete coordinator and claim-before-open topic
feature admission。Producer attach、publish metadata、non-durable subscribe、durable-subscription and transaction
operation gates are also wired before their stock mutations；the limited non-durable cumulative ack gate runs before
ack timestamp/counter/pending-ack/cursor mutation。Loaded-topic admin mutation gates now reject durable-subscription、
backlog/cursor、compaction/offload、truncate、shadow and migration operations。Authoritative live-policy refresh、
namespace/capability convergence、generation-safe write-fence recovery and shared-store peer lifecycle are gated。
A real two-broker Oxia/LocalStack/BookKeeper test proves unload、ownership failover、process restart/reverse takeover、
exact single/batch Position and bytes、hybrid coexistence and real S3 objects。`phase2Check` and Docker-backed
`phase2FinalCheck` exist and pass。F2-M6 now also composes committed-response loss into one recovered callback/
Position，real-Oxia restart repair of both derived projection indexes，and facade close/trim/reopen/terminate/
logical-delete/recreate with retained Object-WAL bytes。The final slice proves real BookKeeper virtual-ledger
isolation、watch-disabled cross-runtime polling wake、Object-WAL failure recovery、stock topic admission/write-fence
behavior and the complete binding/ack/capability/S3 matrix。All F2-M6 scenarios 1–19 pass；Future 2 is complete。

Future 3 / Phase 3 已于 2026-07-14 完成 design-only F3-M0 和 F3-M0R：本地 Pulsar
`master@7efae25af39a15407c1397d9e1f4ac4658d09daa` 的 `ManagedCursor` API、broker ack/recovery/admin
调用路径以及 F2 handoff 已锁定；single-root cursor CAS、generation/tombstone、remaining-bit batch ack、
destructive `ackStateEpoch`、immutable snapshot、local-only dispatch read position、per-writable-open
owner-session claim/fencing、broker ownership guard handoff 和 recoverable retention barrier 已冻结到
Java/file/wire/state-machine/test 级。
Phase 3 当前是 implementation-ready，F3-M1-M6 代码尚未开始；不能描述为已支持
durable Nereus subscriptions。

Phase 1.5 does not expand executable storage profiles. BookKeeper WAL, `WAL_DURABLE` success, async
materialization and Future 4 workers remain designed/reserved.

Start with `docs/design/nereus-design-index.md` for document authority and current status. Use
`docs/phase-1-core-stream-storage/README.md` for the implemented L0 contract and
`docs/phase-1.5-core-storage-foundation/README.md` for the active L0 evolution contract. Use
`docs/phase-2-managed-ledger-facade/README.md` for the implemented F2 contract，and
`docs/phase-3-cursor-subscription/README.md` for the current F3 code-level target contract and M0/M0R decisions.

## Build

Pulsar APIs are consumed from an exact source composite。The fork's `5.0.0-M1-SNAPSHOT` string is its local Gradle
project version，not a published Maven artifact；source-bound tasks fail early when no checkout is configured：

```bash
export NEREUS_PULSAR_CHECKOUT=/absolute/path/to/nereusstream/pulsar
```

The ordinary repository build may use the locked API baseline `100d3ef0ff7c7da36d497453b141ddff6f34a9d3`。
The Phase 2 broker gates require the clean implemented fork commit currently recorded by
`checkPulsarSourceLock`。

```bash
./gradlew checkPhase0
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
./gradlew build
./gradlew phase2Check
./gradlew phase2FinalCheck --rerun-tasks
```
