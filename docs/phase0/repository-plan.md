# Phase 0 Repository Plan

> Status: Historical / completed. Current implementation status is maintained in
> `../design/nereus-design-index.md`.

Phase 0 established the standalone `nereusstream/nereus` repository.

## Goals

- Keep Nereus product code in a standalone repository, not a Pulsar fork.
- Use Gradle Kotlin DSL, aligned with Apache Pulsar's current build style.
- Keep Pulsar and KoP source-tree changes in organization-owned fork repositories.
- Keep authoritative design and code-level documents in the repository under `docs/`.
- Build a Java 21 Gradle monorepo that can grow into the Future 1-8 plan.

## Non-goals

- Implementing StreamStorage.
- Running validation or benchmark suites.
- Keeping `integrations/patches` inside the main product repository.
- Vendoring the full Pulsar or KoP source tree.
- Publishing artifacts.

## Module Boundaries

| Module | Purpose |
| --- | --- |
| `nereus-api` | Protocol-neutral internal API and value types |
| `nereus-core` | L0 StreamStorage implementation |
| `nereus-metadata-oxia` | Oxia metadata and coordination adapter |
| `nereus-object-store` | Object WAL and object IO boundary |
| `nereus-managed-ledger` | ManagedLedger facade implementation |
| `nereus-pulsar-adapter` | Pulsar broker integration boundary |
| `nereus-kop-adapter` | KoP/Kafka projection boundary |

## Fork Workflow

Use separate repositories under the `nereusstream` organization:

- `nereusstream/nereus`: product code, docs, distribution, adapters.
- `nereusstream/pulsar`: fork of `apache/pulsar` for source-tree integration changes.
- `nereusstream/kop`: fork of `streamnative/kop` for KoP source-tree integration changes.

The Nereus repo can depend on fork artifacts, local composite builds, or published snapshots later, but it
should not own the full upstream source trees.

## First Implementation Gate (completed)

Before Future 1 starts:

- `nereusstream/nereus` exists on GitHub as a standalone repository.
- `nereusstream/pulsar` and `nereusstream/kop` forks exist or are explicitly deferred.
- local `main` branch has this scaffold committed.
- `./gradlew checkPhase0` passes.
- target Pulsar and KoP upstream commits are recorded in `docs/phase0/upstream-forks.md`.

The scaffold gate now passes. Future 1 / Phase 1 is the active implementation track.
