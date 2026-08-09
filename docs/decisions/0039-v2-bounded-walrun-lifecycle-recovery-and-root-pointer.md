# ADR 0039: V2 bounded WalRun lifecycle, recovery envelope, and root pointer

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

An unbounded run eventually exceeds any fixed recovery budget. Treating budgets only as takeover timeout settings would
permit normal ACKs to create a state that the next owner must fail to recover. ADR 0030 also assumes the current
WalRun Root can be obtained without defining whether recovery must perform a second provider-prefix LIST.

## Decision

Every immutable WalRun Root fixes hard aggregate `maxExtentCount`, `maxCanonicalBodyBytes`, `maxRunAge`, and
`maxRecoverablePredecessorRuns` values. ADR 0060 does not multiply any run/recovery budget by lane; only finite
per-lane uncovered age is additionally required. Run IDs and shard run epochs are never reused. Before any limit can be
exceeded, the owner stops old-run admission, drains and reconciles its tail, seals it, and publishes a successor.
Numeric defaults are provider/RTO evidence outputs, not unconstrained implementation choices.

ACK/admission continuously preserves a cumulative worst-case recovery envelope. The envelope counts at least:

- live roots/runs and predecessor depth;
- LIST pages, keys, and key bytes;
- HEAD, GET, and full-GET requests plus canonical body bytes;
- decoded contexts, frames, and commit sets;
- memory, concurrency, retry attempts, and wall time.

Checkpoint/hint failure and fallback do not reset counters. Predicted envelope exhaustion stops admission and triggers
rollover, throttle, or fail-closed backpressure. Actual exhaustion never authorizes skipping an object, advancing a
frontier, publishing incomplete coverage, or executing GC.

ADR 0053 makes checkpoint pages asynchronous accelerators with finite uncovered-tail extent/byte/age bounds. ADR 0060
uses one run-wide vector page chain over up to three lane-local sequences. Open-run recovery always LISTs each uncovered
tail, and the Seal binds one mandatory final gap-free vector chain.

Each shard has one low-frequency CAS-published
`CurrentWalRunPointer {walRunRootKey, walRunRootSha256, shardRunEpoch}`. Owner-open, rollover, and handoff read or update
this pointer; normal admitted group append performs no metadata-service I/O. ADR 0047 places immutable Root and Seal
records in Cell control metadata, requires successor publication before pointer CAS, and forbids reopening a sealed
run. Recovery walks a bounded lineage from the current pointer to the published retirement frontier. Every group
header binds the root SHA.

The one Root fixes format/encryption and hard recovery envelopes; it does not become one Topic-specific soft
target/linger identity. 0.2 does not multiply current pointers or run lineages by packing class. ADR 0060 fixes at most
three lazily instantiated lanes, lane-local sequences/ACK barriers, aggregate budgets, and one vector checkpoint/Seal
inventory. Exact class values and canonical lane binding/encoding remain an open policy/wire gate.

After an uncertain pointer CAS, reread accepts only exact candidate equality or the already committed winner; local
merge is forbidden. Missing root, hash/epoch mismatch, lineage cycle/fork, predecessor-depth excess, or a pointer that
cannot reach the retirement frontier fails closed.

## Consequences

- `V2-OPEN-OBJ-11`, `V2-OPEN-OBJ-12`, and `V2-OPEN-OBJ-13` are resolved.
- More rollover/root control-plane operations and small tail groups buy a bounded recoverable state rather than an
  eventually unscannable prefix.
- Provider slowdown can create correctness-driven availability backpressure before capacity is exhausted.
- Root/Seal/successor publication is refined by ADR 0047, checkpoint/open-tail authority by ADR 0053, and directory
  capacity by ADR 0058 and lazy-lane/vector inventory by ADR 0060. Exact remaining wire, retirement authority, and
  evidence-derived numeric values remain downstream gates.
- M3 must prove every cap and cumulative counter, no counter reset on fallback, pre-limit rollover, uncertain CAS,
  lineage fork/cycle/depth rejection, pointer/root substitution, and zero normal-append metadata I/O.

This decision is refined by ADRs 0047/0053/0058/0060, refines ADR 0030, and is tracked by `T-APPEND-01`,
`T-OBJECT-01`, `T-HANDOFF-01`, and `V2-OBJ-005/009..011/014..018`.
