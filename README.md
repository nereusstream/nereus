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

Future 2 F2-M0/M0R/M0R2 design and Phase 1.5 prerequisites are complete. P15-M0-M6、F2-M1 projection foundation、
F2-M2 projection metadata and F2-M3 factory/ledger facade are implemented。`nereus-managed-ledger` now provides the
writable facade、strict get-only read-only ledger、exact append recovery/write-fence handoff、lifecycle/admin/stats
surfaces and audited unsupported channels。F2-M4 cursor boundary is implemented；F2-M5 now has product runtime/S3
gates plus the fork hybrid bootstrap and durable `NSB1` binding open/delete coordinator，while broker feature
admission、namespace/capability convergence and the remaining lifecycle race gates and
end-to-end restart gates remain in progress。

Phase 1.5 does not expand executable storage profiles. BookKeeper WAL, `WAL_DURABLE` success, async
materialization and Future 4 workers remain designed/reserved.

Start with `docs/design/nereus-design-index.md` for document authority and current status. Use
`docs/phase-1-core-stream-storage/README.md` for the implemented L0 contract and
`docs/phase-1.5-core-storage-foundation/README.md` for the active L0 evolution contract. Use
`docs/phase-2-managed-ledger-facade/README.md` for the active F2 code-level design.

## Build

```bash
./gradlew checkPhase0
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
./gradlew build
```
