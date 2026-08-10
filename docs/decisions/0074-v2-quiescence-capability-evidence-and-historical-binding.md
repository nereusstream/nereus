# ADR 0074: V2 quiescence capability evidence and historical binding

## Status

Accepted for the 0.2 closed quiescence capability variants, immutable admission-evidence envelope, historical
generation binding, fail-safe revocation, control-path verification, and configuration boundary. Exact canonical
binary encoding, token/receipt limits, backend conformance receipts, and admitted backend generations remain M4/M5
evidence work; implementation has not started at M0.

## Context

ADR 0071 permits unplanned owner expiry only when the backend proves every read/time-authority condition. A variant
name or current backend configuration cannot prove that historical reads were admitted under those conditions. Without
immutable evidence binding, an upgrade could retroactively authorize deletion for old Owner Epochs, or a generic lease
flag could silently promote writer fencing into reader quiescence.

## Decision

0.2 has exactly two quiescence capability discriminators:

- `DURABLE_DRAIN_ONLY_V1`, which accepts exact durable planned-drain evidence only; and
- `AUTHORITY_EXPIRY_V1`, which additionally admits exact authority-expiry evidence.

The discriminator is not authorization by itself. Every capability generation has one immutable evidence envelope
binding Protocol Cell/backend admission generation, backend adapter/protocol/config digest, ReadAdmissionEpoch contract
version, proof protocol/verifier version, conformance receipt identity/SHA, and capability-record version plus SHA of
its canonical record. The record stores only the receipt identity/digest, not the full test report.

`AUTHORITY_EXPIRY_V1` additionally binds read/time-authority identity, exact `notAfter` semantics,
`maxSourceAccessLifetime`, maximum clock skew, propagation grace, and the pause/recovery owner-fence/deadline recheck
contract. An ordinary lease, session-loss notice, or writer-only owner fence does not satisfy this envelope.

Every owner grant, `ReadAdmissionEpoch`, Binding read-selector state/closure anchor, epoch terminal cut, quiescence
proof, proof-window fold, `SourceRetirementBatch`, source/protection row, release CAS, and compact retirement fact binds
the exact capability admission generation and evidence digest. Historical facts are never reinterpreted by loading a
“current capability”. A later adapter/config/verifier/receipt change creates a new admission generation and cannot
retroactively qualify older reads.

`DURABLE_DRAIN_ONLY_V1` proves only that the verifier may validate a planned drain; each Owner Epoch still needs exact
drain evidence. `AUTHORITY_EXPIRY_V1` proves only that the verifier may validate qualified expiry; each Owner Epoch
still needs exact authority-time evidence. Missing verifier, failed validation, evidence mismatch, or safety revocation
produces `RETAIN`, never an inferred proof or capability downgrade.

Capability evidence is validated at Protocol Cell open, ownership/read-admission, source handoff, and GC/protection-
release control paths. A normal read uses its already validated cached owner/deadline fence and performs no remote
metadata access. Topic, Namespace, and host policy cannot promote a capability, disable fail-safe retention, or alter
historical evidence.

## Consequences

- `V2-OPEN-READ-07` is resolved without a generic capability flag or remote per-read verification.
- Incomplete backend evidence can retain source bytes indefinitely and consume Cell admission; this is the intended
  safety/availability tradeoff.
- M4/M5 must prove capability-generation activation, receipt digest/size bounds, historical non-reinterpretation,
  revocation/missing-verifier retention, planned versus expiry evidence, pause/recovery rechecks, zero normal-read
  metadata I/O, proofs/owner, retained bytes/age, and takeover-to-release p99.

This decision is refined by ADRs 0075..0078, refines ADRs 0049, 0069, 0071, and 0073 and is tracked by `T-POLICY-01`,
`T-MANIFEST-01`, `T-HANDOFF-01`, `V2-READ-006/009..013`, and `V2-OPEN-READ-09/14/15`.
