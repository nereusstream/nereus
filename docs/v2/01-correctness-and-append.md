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

V2 separates four authorities:

| Authority | Owns | Does not own |
| --- | --- | --- |
| ownership authority | current stream incarnation, writer epoch, admission | committed bytes |
| primary WAL | durable byte ranges and per-stream durable prefix | topic binding or protocol coordinator state |
| manifest authority | one logical read view over immutable physical generations | active-tail append linearization |
| protocol authority | Kafka/Pulsar coordinator, cursor, transaction, and visibility semantics | physical object retirement by itself |

Control metadata selects an owner and stores low-frequency roots. It is not consulted on every normal append.

## Append state machine

For one stream incarnation and writer epoch:

1. the protocol-native owner resolves the immutable topic binding;
2. a serialized writer lane validates the current ownership token and admission budget;
3. it allocates `[baseOffset, endOffset)` from process-local `nextOffset`;
4. the selected WAL accepts frames carrying stream identity, incarnation, writer epoch, range, length, and checksum;
5. WAL completion advances only that stream's contiguous durable prefix;
6. the protocol response succeeds only when the complete returned range is within that prefix;
7. background workers later publish sealed/read-optimized generations.

The owner must not acknowledge a range because a local future completed if its ownership epoch was fenced before the
durability completion was validated.

## Offset watermarks

Each protocol adapter projects its native position onto these internal watermarks where applicable:

- `allocatedEnd`: next offset reserved by the active writer lane;
- `durableEnd`: highest contiguous end proven by the primary WAL;
- `readableEnd`: highest end exposed through the current logical read view;
- `trimStart`: lowest logically retained offset.

Required ordering is `trimStart <= readableEnd <= durableEnd <= allocatedEnd`. A normal successful append closes with
`readableEnd >= endOffset`; a failed or uncertain append may leave `allocatedEnd > durableEnd` only until owner-local
resolution or recovery. Recovery never exposes an unproven gap.

For native Pulsar BookKeeper, `(ledgerId, entryId)` remains the protocol position. M2 must freeze whether the common
logical coordinate is derived from a durable sealed-ledger base or is only a lifecycle index; it must not silently
replace the ManagedLedger position authority.

## Uncertain append

A timeout, connection reset, or lost provider response yields an uncertain result. Resolution uses the original
deterministic identity and verifies:

- stream incarnation and writer epoch;
- range and frame count;
- object/ledger identity;
- byte length and content checksum;
- contiguous predecessor coverage.

A retry may return the original success or fail closed. It may not allocate a different successful range for the same
idempotency identity.

## Hot-path contract

Normal append must report both of these as zero:

- remote control-metadata reads;
- remote control-metadata mutations.

Topic binding and ownership are cached only after an explicit open/acquire operation. Cache misses stop admission and
reload before allocating offsets; they do not insert metadata access into the admitted append path.

Relevant tradeoff: `T-APPEND-01`. Required scenarios: `V2-APP-001`, `V2-APP-002`, and `V2-APP-003`.
