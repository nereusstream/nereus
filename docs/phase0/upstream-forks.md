# Upstream Forks

Nereus keeps source-tree modifications for Pulsar and KoP in organization-owned fork repositories.

## Forks

| Repository | Upstream | Purpose |
| --- | --- | --- |
| `github.com/nereusstream/pulsar` | `github.com/apache/pulsar` | Pulsar storage hooks, ManagedLedger provider integration, packaging hooks |
| `github.com/nereusstream/kop` | `github.com/streamnative/kop` | KoP offset projection, group/txn state integration, Nereus adapter hooks |

## Branch Naming

Use branch names that map to Nereus futures:

```text
nereus/future1-storage-hooks
nereus/future2-managed-ledger-facade
nereus/future5-kop-compatibility
```

## Version Pins

Record the exact upstream commit before implementation begins.

| Component | Upstream ref | Nereus fork branch | Status |
| --- | --- | --- | --- |
| Pulsar | `320fbce6d540b618d35f1dd374e0aaf5fbd3c35c` | `main` or `nereus/main` | initial local planning ref |
| KoP | `TODO_PIN_EXACT_COMMIT` | `main` or `nereus/main` | pending |

## Local Development Pattern

Recommended local checkout layout:

```text
GITHUB/
  nereus/        # github.com/nereusstream/nereus
  pulsar/        # github.com/nereusstream/pulsar or apache/pulsar checkout
  kop/           # github.com/nereusstream/kop checkout
```

The product repository should stay buildable without requiring the fork checkouts until adapter modules need
compile-time integration against fork-only APIs.
