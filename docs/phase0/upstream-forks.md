# Upstream Forks

> Status: Active integration register. Exact pins must be refreshed before the corresponding adapter track starts.

Nereus keeps source-tree modifications for Pulsar、KoP and the Designed native Kafka integration in
organization-owned fork repositories.

## Forks

| Repository | Upstream | Purpose |
| --- | --- | --- |
| `github.com/nereusstream/pulsar` | `github.com/apache/pulsar` | Pulsar storage hooks, ManagedLedger provider integration, packaging hooks |
| `github.com/nereusstream/kop` | `github.com/streamnative/kop` | KoP offset projection, group/txn state integration, Nereus adapter hooks |
| `github.com/nereusstream/kafka`（planned） | exact pinned `github.com/apache/kafka` commit | F9 native log/storage hooks、broker lifecycle and packaging；separate from KoP |

## Branch Naming

Use branch names that map to Nereus futures:

```text
nereus/future1-storage-hooks
nereus/future2-managed-ledger-facade
nereus/future5-kop-compatibility
nereus/future9-native-kafka-storage
```

## Version Pins

Record the exact upstream commit before implementation in the corresponding organization fork begins.

| Component | Upstream ref | Nereus fork branch | Status |
| --- | --- | --- | --- |
| Pulsar | `320fbce6d540b618d35f1dd374e0aaf5fbd3c35c` | `main` or `nereus/main` | initial local planning ref |
| KoP | `TODO_PIN_EXACT_COMMIT` | `main` or `nereus/main` | pending |
| Apache Kafka | `TODO_PIN_EXACT_APACHE_KAFKA_COMMIT` | `nereus/future9-native-kafka-storage` | must be pinned before F9-M3 Kafka-fork code starts |

AutoMQ is a reference implementation, not an upstream fork entry. F9-M0 audited local
`automq/main@1c648d84819d5c3fef2af585f02149c397584870` (`3.9.0-SNAPSHOT`) and recorded exact file/blob evidence in
`../phase-9-kafka-native-storage/01-current-contract-and-automq-source-audit.md`. Choosing an AutoMQ-derived production
base later would require a new ADR and an updated compatibility/license/source audit；it is not implied by this row.

## Local Development Pattern

Recommended local checkout layout:

```text
GITHUB/
  nereus/        # github.com/nereusstream/nereus
  pulsar/        # github.com/nereusstream/pulsar or apache/pulsar checkout
  kop/           # github.com/nereusstream/kop checkout
  kafka/         # future github.com/nereusstream/kafka checkout
  automq/        # optional read-only F9 reference checkout; never a build-time dependency
```

The product repository should stay buildable without requiring the fork checkouts until adapter modules need
compile-time integration against fork-only APIs.
