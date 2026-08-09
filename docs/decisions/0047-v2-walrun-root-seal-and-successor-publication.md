# ADR 0047: V2 WalRun Root, seal, and successor publication

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0039 permits the immutable WalRun Root to be either metadata or a provider object and leaves sealing publication
open. A provider Root would require provider discovery before append can open. Mutating a Root to mark it sealed would
turn one allegedly immutable identity into another lifecycle authority and complicate uncertain-update recovery.

## Decision

`WalRunRootRecord` is a bounded immutable value in the owning Protocol Cell's control-metadata backend, addressed by
an exact metadata key and canonical SHA-256. `walRunRootKey` names that metadata key, not an Object key. Root creation
uses `putIfAbsent`; response loss is reconciled only by exact key/value reread equality before append opens.

Sealing never mutates the Root. After the owner stops admission and reconciles the complete tail, it publishes one
immutable `WalRunSealRecord` that binds the Root key/SHA, terminal lane-sequence vector, one final checkpoint-head SHA,
and exact typed terminal coverage. A successor Root references both the predecessor Root key/SHA and its Seal key/SHA.

Only after predecessor reconciliation and successor creation does the owner CAS
`CurrentWalRunPointer` from the exact predecessor tuple to the successor tuple. If a crash leaves the pointer on a
sealed Root, recovery deterministically completes or adopts the already matching successor and advances the pointer;
it never reopens the sealed run or publishes a locally merged lineage.

Root, Seal, successor, and pointer operations are low-frequency control-plane cuts. Normal admitted group append
performs no metadata-backend read or mutation.

ADRs 0053/0060 refine the tail between Root and Seal: checkpoint pages publish asynchronously after ACK through one
run-wide vector chain, open recovery always LISTs uncovered lane tails, and the Seal binds that final gap-free chain.
Checkpoint policy is Protocol Cell x shard scoped and persisted in the WalRun Root.

## Consequences

- `V2-OPEN-OBJ-16` is resolved.
- Two immutable records plus one CAS per rollover replace provider-Root discovery and mutable-Root ambiguity.
- A sealed run can be recovered without interpreting pointer lag as permission to append.
- Checkpoint-page authority and open-tail handoff are refined by ADRs 0053/0060. Exact remaining Root/Seal/pointer wire,
  retirement frontier, and GC order remain downstream recovery gates.
- M3 must prove lost Root/Seal/Pointer responses, sealed-pointer crash recovery, successor substitution/fork
  rejection, exact terminal coverage, and zero normal-append metadata I/O.

This decision is refined by ADRs 0053/0060, refines ADRs 0030, 0038, 0039, and 0046, and is tracked by `T-OBJECT-01`,
`T-HANDOFF-01`, and `V2-OBJ-005/009..011/014..018`.
