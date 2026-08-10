# ADR 0075: V2 Binding read selector and fallback-interval linearization

## Status

Accepted for the 0.2 `OBJECT_WAL` Binding/incarnation read-selector authority, takeover-versus-no-fallback
linearization, conservative whole-epoch fallback interval, first-epoch inheritance, conditional proof liability, and
configuration boundary. Exact backend encoding, numeric limits, and equivalent-transaction conformance receipts remain
M4 evidence work. ADR 0077 refines the no-fallback transition into one fused E-to-E+1 closure cut, while ADR 0078
refines mixed-first release to each source's own interval; implementation has not started at M0.

## Context

ADR 0073 defines one contiguous Read Admission Epoch order and immutable fallback-capable intervals. If takeover and
`PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY` update different keys and reconcile with application-side rereads, an old
owner can freeze `lastFallbackCapableReadAdmissionEpoch=E` after E+1 has already admitted fallback reads. Updating every
source on every takeover would avoid that omission only by recreating owner x source metadata writes.

## Decision

Each Binding incarnation owns one authoritative `BindingReadSelector`. Every takeover/read grant and every
`PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY` cut competes through the same selector CAS or a backend transaction proven
to have identical atomic conditional semantics. The condition compares at least the exact tuple:

```text
{selectedViewSha, OwnerEpoch, ReadAdmissionEpoch, readAdmissionState}
```

Cross-key application-side reread, watch/cache observation, or a sequence of independent conditional writes is not an
equivalent transaction.

For a fallback-bearing selector value `(PWF, O, E)`, the two competing transitions are:

```text
takeover/read grant:
  (PWF, O, E, ADMITTING) -> (PWF, O2, E+1, ADMITTING, closureAnchor[E])
no-fallback cut:
  (PWF, O, E, ADMITTING) -> (PO, O, E+1, ADMITTING, SourceRetirementBatch[last=E], closureAnchor[E])
```

Whichever transition commits first linearizes the result. The other receives a definitive conflict and must reread and
recompute from the new exact selector. An unknown response converges only by exact selector/view/batch equality and
admits no further E read while unresolved. If the no-fallback cut wins, the same CAS closes E and immediately grants
same-owner no-fallback E+1; if takeover wins, the stale no-fallback cut cannot freeze `last=E`.

`firstFallbackCapableReadAdmissionEpoch` is assigned only when one new fallback/source-protection identity first
appears. Later `PREFERRED_WITH_FALLBACK` generations carrying that same identity inherit the original first epoch; they
never reset it to the current epoch. Each source/protection row retains its own first epoch; a batch may summarize the
earliest first, but source i releases only against `[first_i, sharedLast]`. Removing and later reintroducing bytes under
a new protection identity starts a new interval and a fresh selector epoch even if the physical object key is the same.

Proof-window liability is reserved for a new Read Admission Epoch only when the current selector contains fallback or
the transition introduces fallback. A completely no-fallback epoch neither requires a quiescence proof nor blocks
takeover merely because an older retirement batch still exists. The selector/epoch cut remains low-frequency Binding
control metadata; ordinary reads use validated cached authority and perform no metadata I/O.

Selector linearization, interval inheritance, and conditional liability are non-disableable correctness contracts.
Topic policy cannot replace the selector, weaken its comparison tuple, reset first, or make a no-fallback epoch proof-
liable. Configuration is limited to Cell/Binding admission ceilings, reconciler scheduling, and evidence-derived
capacity values.

## Consequences

- `V2-OPEN-READ-10` is resolved without a sub-epoch counter or owner x source update.
- Stable-state reads gain no metadata access. Takeover/no-fallback control operations may conflict and retry, and
  whole-epoch coverage can retain fallback bytes longer; the cost is control-plane/storage cost rather than read
  throughput.
- M4/M5 must prove both CAS orders, unknown response, stale-owner fencing, selector substitution, inherited/renewed
  source identities, per-source mixed-first intervals, fallback-conditional liability, fused no-fallback E+1 grant,
  no-fallback takeover progress, and zero ordinary-read metadata I/O.

This decision is refined by ADRs 0077/0078, refines ADRs 0069, 0071, 0073, and 0074 and is tracked by
`T-MANIFEST-01`, `T-HANDOFF-01`, and `V2-READ-006/008/010/012/013`.
