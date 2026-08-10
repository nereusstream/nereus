# ADR 0076: V2 Read Admission Epoch terminal cut and on-demand proof

## Status

Accepted for the 0.2 `OBJECT_WAL` irreversible epoch-terminal prerequisite, deterministic create-only epoch proof,
fenced publisher/verifier contract, first-valid-wins convergence, invalid-occupant quarantine, on-demand generation,
and configuration boundary. Exact terminal publication state machine, proof/window wire encoding, and numeric limits
remain M4/M5 work; implementation has not started at M0.

## Context

A deterministic proof key prevents duplicate rows but does not prove that its Read Admission Epoch stopped accepting
reads. Planned drain and qualified expiry can also produce different evidence bytes for the same epoch. Without one
irreversible terminal identity, a proof could be published and the same epoch reopened, or two proof paths could claim
different closure cuts while occupying one logical key.

## Decision

Before any quiescence proof may be published, the epoch has one immutable `ReadAdmissionEpochTerminalCut` binding at
least:

```text
Binding/incarnation
ReadAdmissionEpoch
OwnerEpoch
lastAdmittedAndDrainedReadViewCut
capabilityEvidenceDigest
admissionClosedFence OR qualifiedAuthorityNotAfter
```

The terminal cut is irreversible. The same Read Admission Epoch can never reopen; every later admitted read belongs to
E+1 or later. Planned-drain and qualified-expiry proof candidates for E must bind and validate the same exact terminal-
cut SHA. The capability variant and exact terminal evidence determine whether the closed verifier may accept that cut;
a key name alone never proves closure.

Each proof uses one deterministic Binding/incarnation/Read Admission Epoch key and an immutable canonical value.
Creation is create-only and allowed only to a currently fenced publisher authorized for that terminal cut. Before
creation, the closed verifier validates the candidate's exact Binding/incarnation, Read Admission Epoch, Owner Epoch,
drained read-view cut, applicable admission-closed or authority-time cut, terminal-cut SHA, capability
generation/digest, and proof identity.

Canonical proof bytes contain no random value, host-local time, or nondeterministic serialization. The first valid
proof wins and is never replaced. On an unknown response, exact reread equality proves the attempted value committed. A
different existing value may supply logical coverage only after the same closed verifier validates every binding and
terminal fact. An invalid or mismatched occupying value fails closed and is quarantined for evidence; it is never
overwritten, normalized, or ignored.

Proofs are generated on demand only for Read Admission Epochs intersecting at least one fallback-capable retirement
interval. No-fallback epochs are not prewritten merely to make the global epoch sequence dense. One valid proof is
source-independent and reusable by every intersecting batch. Exact proof-window/head/fold representation remains M4
evidence work under `V2-OPEN-READ-08`.

Terminal closure, fenced publication, closed verification, and on-demand eligibility are non-disableable correctness
contracts. Topic policy cannot create/skip a proof, reopen an epoch, or promote a publisher/verifier. Configuration is
limited to Cell/Binding admission ceilings, reconciler cadence, and evidence-derived capacity parameters.

## Consequences

- `V2-OPEN-READ-11` is resolved without a proof selector, replacement race, multiple candidates, or owner x batch
  writes.
- Each relevant epoch pays at most one low-frequency immutable proof write. First-valid-wins may preserve a later
  `safeAfter` and retain source bytes longer, but it adds no ordinary-read atomic operation or remote I/O.
- M4/M5 must prove terminal non-reopening, planned/expiry common-SHA validation, publisher fencing, pre-create verifier
  rejection, deterministic bytes, both conditional-put response outcomes, invalid occupant quarantine, on-demand/no-
  fallback omission, proof reuse, and no normal-read I/O.

This decision refines ADRs 0071, 0073, 0074, and 0075 and is tracked by `T-MANIFEST-01`, `T-HANDOFF-01`,
`V2-READ-006/008/009/011`, and `V2-OPEN-READ-13`.
