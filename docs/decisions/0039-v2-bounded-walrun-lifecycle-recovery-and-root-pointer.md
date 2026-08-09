# ADR 0039: V2 bounded WalRun lifecycle, recovery envelope, and root pointer

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

An unbounded run eventually exceeds any fixed recovery budget. Treating budgets only as takeover timeout settings would
permit normal ACKs to create a state that the next owner must fail to recover. ADR 0030 also assumes the current
WalRun Root can be obtained without defining whether recovery must perform a second provider-prefix LIST.

## Decision

Every immutable WalRun Root fixes hard `maxExtentCount`, `maxCanonicalBodyBytes`, `maxRunAge`, and
`maxRecoverablePredecessorRuns` values. Run IDs and shard run epochs are never reused. Before any limit can be exceeded,
the owner stops old-run admission, drains and reconciles its tail, seals it, and publishes a successor. Numeric defaults
are provider/RTO evidence outputs, not unconstrained implementation choices.

ACK/admission continuously preserves a cumulative worst-case recovery envelope. The envelope counts at least:

- live roots/runs and predecessor depth;
- LIST pages, keys, and key bytes;
- HEAD, GET, and full-GET requests plus canonical body bytes;
- decoded contexts, frames, and commit sets;
- memory, concurrency, retry attempts, and wall time.

Checkpoint/hint failure and fallback do not reset counters. Predicted envelope exhaustion stops admission and triggers
rollover, throttle, or fail-closed backpressure. Actual exhaustion never authorizes skipping an object, advancing a
frontier, publishing incomplete coverage, or executing GC.

Each shard has one low-frequency CAS-published
`CurrentWalRunPointer {walRunRootKey, walRunRootSha256, shardRunEpoch}`. Owner-open, rollover, and handoff read or update
this pointer; normal admitted group append performs no metadata-service I/O. The root may be stored as an immutable
metadata value or provider object, but the pointer always binds its exact identity/SHA. Each successor root records its
predecessor key/SHA, and recovery walks a bounded lineage from the current pointer to the published retirement
frontier. Every group header binds the root SHA.

After an uncertain pointer CAS, reread accepts only exact candidate equality or the already committed winner; local
merge is forbidden. Missing root, hash/epoch mismatch, lineage cycle/fork, predecessor-depth excess, or a pointer that
cannot reach the retirement frontier fails closed.

## Consequences

- `V2-OPEN-OBJ-11`, `V2-OPEN-OBJ-12`, and `V2-OPEN-OBJ-13` are resolved.
- More rollover/root control-plane operations and small tail groups buy a bounded recoverable state rather than an
  eventually unscannable prefix.
- Provider slowdown can create correctness-driven availability backpressure before capacity is exhausted.
- Exact root/pointer/lineage wire, seal/checkpoint/retirement authority, handoff cuts, and evidence-derived numeric
  values remain downstream gates.
- M3 must prove every cap and cumulative counter, no counter reset on fallback, pre-limit rollover, uncertain CAS,
  lineage fork/cycle/depth rejection, pointer/root substitution, and zero normal-append metadata I/O.

This decision refines ADR 0030 and is tracked by `T-APPEND-01`, `T-OBJECT-01`, `T-HANDOFF-01`, and
`V2-OBJ-005/009..011`.
