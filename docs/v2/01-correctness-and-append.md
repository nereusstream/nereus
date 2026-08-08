---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Correctness and append

## Authorities

V2 separates five authorities:

| Authority | Owns | Does not own |
| --- | --- | --- |
| Native Write Authority | the only protocol allowed to allocate positions for one Topic Incarnation | physical placement or another protocol's positions |
| ownership authority | current binding/incarnation, Owner Epoch, admission | committed bytes or protocol kind |
| primary WAL | durable Physical Extents and binding-scoped Protocol Coverage/frontier | Topic Protocol Binding or protocol coordinator state |
| manifest authority | one typed logical read view over immutable physical generations | active-tail append linearization |
| protocol authority | Kafka/Pulsar coordinator, cursor, transaction, and visibility semantics | physical object retirement by itself |

Control metadata selects an owner and stores low-frequency roots. It is not consulted on every normal append.

## Append state machine

For one Topic Protocol Binding, Topic Incarnation, Storage Epoch, and Owner Epoch:

1. the protocol-native owner resolves the immutable Topic Protocol Binding and active Storage Epoch;
2. a serialized writer lane validates the current ownership token and admission budget;
3. it allocates positions through the binding's Position Domain;
4. the selected WAL accepts frames carrying binding/incarnation, Storage Epoch, Owner Epoch, typed Protocol Coverage,
   length, and checksum;
5. WAL completion advances only that binding's contiguous typed durable frontier;
6. the protocol response succeeds only when the complete returned Protocol Coverage is durable and readable;
7. background workers later publish sealed/read-optimized generations.

Kafka allocation yields a half-open Kafka Offset Range. Pulsar allocation yields Pulsar Positions whose adjacency and
cross-ledger order are proven by the Ledger Chain and represented as ledger-keyed Pulsar Coverage. Neither path allocates
or persists a cross-protocol `long logicalOffset`.

The owner must not acknowledge coverage because a local future completed if its Owner Epoch or Storage Epoch authority
was fenced before the durability completion was validated.

## Typed protocol frontiers

Each Position Domain supplies comparison, adjacency, coverage, and frontier operations for:

- `AllocatedFrontier`: the next boundary reserved by the active writer lane;
- `DurableFrontier`: the highest contiguous boundary proven by the primary WAL;
- `ReadableFrontier`: the highest boundary exposed through the current logical read view;
- `TrimFrontier`: the lowest logically retained boundary.

Required ordering is `TrimFrontier <= ReadableFrontier <= DurableFrontier <= AllocatedFrontier`, where `<=` is
defined only by that binding's Position Domain. A normal successful append closes only when its returned coverage is
inside Readable and Durable coverage. A failed or uncertain append may leave allocation ahead of durability only until
owner-local resolution or recovery. Recovery never exposes an unproven gap.

Frontiers from different Topic Protocol Bindings, Topic Incarnations, or Position Domains are not comparable. Pulsar
cross-ledger ordering comes from the authoritative Ledger Chain; V2 does not derive a permanent
`ledgerBase + entryId` coordinate.

## Uncertain append

A timeout, connection reset, or lost provider response yields an uncertain result. Resolution uses the original
deterministic identity and verifies:

- Topic Protocol Binding, Topic Incarnation, Storage Epoch, and Owner Epoch;
- typed Protocol Coverage and frame/entry count;
- object/ledger identity;
- byte length and content checksum;
- contiguous predecessor coverage.

A retry may return the original success or fail closed. It may not allocate different successful protocol positions for
the same idempotency identity.

## Hot-path contract

Normal append must report both of these as zero:

- remote control-metadata reads;
- remote control-metadata mutations.

Topic Protocol Binding, active Storage Epoch, and ownership are cached only after an explicit open/acquire operation.
Cache misses stop admission and reload before allocating positions; they do not insert metadata access into the admitted
append path.

Relevant tradeoffs: `T-APPEND-01` and `T-POSITION-01`. Required scenarios: `V2-APP-001`, `V2-APP-002`,
`V2-APP-003`, and `V2-POSITION-001`.
