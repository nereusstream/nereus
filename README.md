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
  docs/phase-1-core-stream-storage/     current code-level contracts and milestones
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

Start with `docs/design/nereus-design-index.md` for document authority and current status, then
`docs/phase-1-core-stream-storage/README.md` for the active code-level design.

## Build

```bash
./gradlew checkPhase0
./gradlew phase1Check
./gradlew phase1FinalCheck --rerun-tasks
./gradlew build
```
