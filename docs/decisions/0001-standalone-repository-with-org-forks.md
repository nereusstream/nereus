# ADR 0001: Standalone Repository with Organization Forks

## Status

Accepted. The repository scaffold and product-owned modules are implemented；Pulsar/KoP source-tree integration and
the Designed F9 native Kafka integration continue through organization forks. The Kafka-specific source/base decision
is refined by ADR 0005.

## Context

Nereus must integrate deeply with Apache Pulsar and KoP and, for F9, Apache Kafka, but the product should have its own GitHub
identity, contribution graph, release history, and commercial packaging boundary.

Long-lived development directly inside an Apache Pulsar fork would make Nereus look like a Pulsar branch
rather than an independent product. Keeping patch overlays in the product repository would also duplicate
what GitHub forks already provide better: branch history, PRs, reviews, and commit attribution.

## Decision

Use `github.com/nereusstream/nereus` as the standalone product repository.

Use organization forks for source-tree changes:

- `github.com/nereusstream/pulsar`
- `github.com/nereusstream/kop`
- `github.com/nereusstream/kafka`（planned by F9；not created or pinned by this ADR）

Nereus-owned code lives in Gradle modules such as `nereus-core`, `nereus-metadata-oxia`, and
`nereus-managed-ledger`.

## Consequences

- Product commits land in a standalone repository rather than a fork.
- Pulsar, KoP, and future Kafka source-tree changes have normal GitHub branch/PR history under the Nereus organization.
- The main product repository remains smaller and focused.
- Adapter module compile dependencies may need published snapshots, local composite builds, or explicit
  dependency substitution once fork-only APIs appear.
