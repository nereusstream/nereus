# Nereus design documents

`main` is the V2 development line. Start with:

1. [Design index](nereus-design-index.md)
2. [V2 overall architecture](nereus-overall-architecture.md)
3. [V2 Context Map](../../CONTEXT-MAP.md)
4. [Normative V2 contracts](../v2/README.md)
5. [Accepted decisions](../decisions/)
6. [Open questions](../v2/open-questions.md)

## Authority boundary

Accepted ADRs, the Context Map/glossaries, and normative files under `docs/v2/` define V2. ADR 0010 is superseded by
ADR 0012. M0 is documentation-only; current Java and runtime evidence have not yet been promoted to V2.

The following design and Phase/Future files remain as V1 implementation evidence while matching V1 code still exists
on `main`:

- [V1 commit protocol](nereus-commit-protocol.md)
- [V1 object format](nereus-storage-object-format.md)
- [V1 terminology](nereus-terminology.md)
- [V1 Futures roadmap](nereus-futures.md)
- [V1 Phase 1 through Phase 9 documents](../phase-1-core-stream-storage/README.md)
- [V1 BookKeeper primary-WAL evidence](../phase-bk-bookkeeper-primary-wal/README.md)

The exact V1 product line is preserved by branch
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`. V1 history is not copied into a second directory and is not V2
authority.

## KoP

[KoP/Kafka compatibility](nereus-future5-kop-compatibility.md) is deliberately retained. It is Designed / deferred
from the 0.2 runtime and release gates and requires a fresh V2 audit before activation.

## Maintenance

Every V2 implementation milestone updates its normative contract, affected ADR/context language,
tradeoff/open-decision state, scenario JSON/Markdown, source tuple, and gate receipt together.
`./gradlew v2M0Check` owns the M0 documentation baseline.
