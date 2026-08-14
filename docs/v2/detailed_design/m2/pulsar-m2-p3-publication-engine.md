---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P3 sealed-ledger publication engine

P3 implements the deterministic two-Object attempt lifecycle after the native adapter has frozen one sealed,
non-current ledger and P1 has produced its staged NPD1 file. `PreparedAttempt` rejects a changed LAC, entry count,
logical length, entry range, or pre-existing BookKeeper delete state before provider I/O.

Publication verifies the staged NPD1 length and SHA with bounded streaming I/O, then opens only an exact-EOF validating
stream for the provider request. It immutable-creates and proves data first. A response-uncertain create is accepted only
when HEAD resolves the same length and SHA; a returned mismatch is a conflict. Only that immutable data version may enter
the locally round-tripped NPO1 root. The engine then immutable-creates and proves root and invokes the production
`readOffloaded` verifier. Its future succeeds only after that verifier confirms the pair as one ledger-equivalent view.

Cleanup derives both keys from persisted attempt facts. It proves root absent before touching data, then proves data
absent, then clears deterministic attempt-scoped multipart residue. A root-delete failure stops the chain, so cleanup
cannot expose a data-only deletion race. Every operation remains UUID-scoped and idempotent.

`v2M2PulsarP3Check` proves the publication/response-loss/cut/cleanup state machine with an in-memory immutable provider.
It does not yet claim the P4 production Object reader, native Pulsar integration, selected provider/block defaults,
runtime evidence, scenario promotion, or M2 PASS.
