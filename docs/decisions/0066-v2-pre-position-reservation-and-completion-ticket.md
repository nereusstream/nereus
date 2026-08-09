# ADR 0066: V2 pre-position reservation and completion ticket

## Status

Accepted for the 0.2 `OBJECT_WAL` owner-local pre-position reservation and completion-ticket contract. It defines no
product wire, API, metadata schema, or configurable feature. Exact numeric budgets remain evidence/admission outputs;
implementation has not started at M0.

## Context

Reserving only a completion-tracker slot is insufficient: after protocol position allocation, the owner could discover
that no active-tail locator capacity remains and strand allocated coverage that cannot become readable or ACKable.
Adding a persisted append ordinal or a second slot generation would solve the wrong problem and create another durable
ordering domain.

## Decision

Before allocating any Kafka Offset Range or Pulsar Position, the owner atomically reserves enough local capacity for
both:

- one complete binding completion unit in the bounded tracker; and
- the corresponding active-tail locator accounting under the binding soft share and shard/Cell/host hard ceilings.

Failure to reserve both stops that binding before position allocation. The reservation is owner-local, bounded, and
reclaimable; it is neither remote metadata nor protocol truth. One reservation and one ticket represent exactly one
complete `KafkaAppendCommitSet` or one Pulsar ManagedLedger entry. Frames, Kafka records, Pulsar batched messages, and
directory rows do not receive independent tickets.

Only after the combined reservation succeeds does the Position Domain allocate exact Protocol Coverage. The owner then
allocates one checked 64-bit `CompletionTicket`. The complete 64-bit value is stored in its ring slot and full equality
is the slot ABA fence; there is no separate `ticketGeneration`. Wrap or reuse while a value can still be observed fails
closed/backpressures before allocating another position.

Exact Protocol Coverage remains the ordering authority. A cached expected predecessor may accelerate adjacency checks,
but it is derived from the Position Domain and cannot authorize release, comparison, replay, or recovery. The ticket is
never encoded in NWG1, checkpoint, Seal, manifest, metadata keys, idempotency identity, public API, product wire, or
configuration.

Provider completion reuses the reservation while it installs compact active-tail locator coverage and advances the
binding's contiguous frontiers. Once provider resolution permits buffer release, the slot retains only the already
accepted minimal completion facts. Cancellation before position allocation releases the whole reservation; after
allocation, failure/uncertainty retains only the bounded facts needed to converge or recover and never silently returns
capacity while an ACK remains possible.

Takeover discards all old-owner tickets and uncommitted local reservations after fencing the old instance. Recovery
uses bounded collection plus Position Domain sort/adjacency validation, then issues fresh owner-local tickets and
reuses the normal ring/window. A long-lived general ordered map is not part of the 0.2 normal or default recovery
contract; it may be reconsidered only if benchmark evidence proves bounded collect-and-sort cannot meet the declared
sparse/streaming recovery envelope.

## Consequences

- `V2-OPEN-OBJ-21` is resolved without introducing a persisted append ordinal, a second ABA generation, or a generic
  TreeMap contract.
- The normal path adds one combined local capacity check, one checked 64-bit increment, and array/ring access; it adds
  no Object or metadata I/O.
- Tracker and locator admission cannot disagree after protocol position allocation.
- Exact reservation sizing, soft-share values, hard ceilings, and collect/sort batch sizes remain evidence outputs,
  not ticket identity or Topic feature flags.
- M3 must prove pre-position atomic reservation, every cancel/unknown/completion release cut, one-ticket-per-commit-unit,
  full-value ABA checks, wrap rejection, takeover discard, bounded collect/sort reconstruction, and zero remote I/O.

This decision refines ADRs 0007, 0008, 0031, 0049, and 0064 and is tracked by `T-APPEND-01`, `T-PROTOCOL-01`,
`T-OBJECT-01`, `V2-APP-001..003`, and `V2-OBJ-002/006/021/023`.
