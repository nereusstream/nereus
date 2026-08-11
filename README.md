# Nereus

> Product line: V2 on `main` at `0.2.0-SNAPSHOT`.
> The current V2 milestone is documentation/readiness work; existing Java and runtime evidence remain V1 residue until
> a V2 milestone explicitly replaces and promotes them.

Nereus V2 is a multi-protocol Storage Fabric with independent Kafka and Pulsar Protocol Cells. It combines
protocol-native position and control authority with shared storage-lifecycle contracts and three Storage Epoch
profiles:

- `OBJECT_WAL`: cost-first; ACK waits for verified durable Object WAL coverage;
- `BOOKKEEPER_WAL_ONLY`: performance-first; ACK waits for BookKeeper quorum and performs no Object offload;
- `BOOKKEEPER_WAL_ASYNC_OBJECT`: performance-first; ACK waits for BookKeeper quorum and sealed protocol coverage is
  offloaded asynchronously.

Kafka Offset and Pulsar Position remain separate truths. Shared storage uses typed Protocol Coverage and Physical
Extents; it does not introduce a universal logical offset or a second native write authority.

## Documentation

Current V2 authority:

1. [V2 design index](docs/v2/README.md)
2. [Overall architecture](docs/v2/architecture.md)
3. [Context map and domain glossaries](docs/v2/context-map.md)
4. [Implementation plan and gates](docs/v2/08-implementation-plan-and-gates.md)
5. [Accepted decisions](docs/decisions/)

Historical V1 design, Phase/Future contracts, release evidence, and delivery history are isolated under the
[V1 archive](docs/v1/README.md). The exact V1 product line is preserved at
`v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b`; V1 documents are never V2 architecture or current-source V2 evidence.

Release-independent freeze and tag procedures remain under [docs/releases](docs/releases/README.md).

## Repository layout

```text
nereus/
  docs/v2/          current V2 architecture, contracts, scenarios, open questions, and Grill records
  docs/v1/          frozen V1 design, Phase/Future contracts, performance notes, and release evidence
  docs/decisions/   repository-wide, V1/V2 transition, and accepted V2 decisions
  docs/releases/    product-line-neutral release freeze and tag process
  nereus-*/         current source graph; V1 residue until replaced by explicit V2 milestones
```

## Verification

The V2 documentation baseline is executable:

```bash
./gradlew v2M0Check
```

The ordinary repository build remains available while M1 constructs and promotes the pure V2 graph:

```bash
./gradlew check
```

Historical V1 gates remain runnable only as explicitly labeled evidence while their corresponding source slices still
exist on `main`. Passing a V1 gate does not promote V2 implementation or evidence status.
