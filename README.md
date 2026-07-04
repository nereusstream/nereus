# Nereus

Nereus is a Pulsar-native shared-storage streaming engine built around an Oxia
metadata/coordination plane, shared object data plane, stateless broker serving,
and a single internal `streamId + offset` truth.

This is the standalone product repository for `github.com/nereusstream/nereus`.
Pulsar and KoP changes are developed in organization-owned fork repositories,
not as patch overlays inside this repo.

## Repository Layout

```text
nereus/
  gradle/                      Gradle wrapper and version catalog
  nereus-api/                  public internal APIs and value types
  nereus-core/                 L0 StreamStorage core
  nereus-metadata-oxia/        Oxia metadata and coordination adapter
  nereus-object-store/         object WAL / object IO boundary
  nereus-managed-ledger/       ManagedLedger facade implementation boundary
  nereus-pulsar-adapter/       Pulsar broker integration boundary
  nereus-kop-adapter/          KoP/Kafka projection boundary
```

## Related Organization Repositories

Expected upstream forks:

```text
github.com/nereusstream/pulsar  -> fork of apache/pulsar
github.com/nereusstream/kop     -> fork of streamnative/kop
```

The main Nereus repository should hold product-owned modules. Forks hold changes that must
land inside upstream Pulsar or KoP trees. Design documents are intentionally kept out of the
pushed code repository for now.

## Current Phase

Phase 0 creates the standalone Gradle repository scaffold, module boundaries, and organization
fork workflow. It does not implement the storage engine yet.

Recommended implementation sequence:

1. Future 1: Core StreamStorage + Object WAL
2. Future 2: ManagedLedger Facade
3. Future 3: Cursor / Subscription State
4. Future 4: Compaction + Generation Replacement
5. Future 7: Routing / Brown-out / Elasticity
6. Future 5: KoP Compatibility
7. Future 6: Lakehouse SBT / SDT
8. Future 8: Advanced Pulsar Semantics

## Build

```bash
./gradlew checkPhase0
./gradlew build
```
