# ADR 0077: V2 fused selector closure and no-fallback epoch cut

## Status

Accepted for the 0.2 `OBJECT_WAL` closed read-admission states, fused no-fallback/epoch transition, durable closure-
anchor requirement, fallback-introduction epoch cut, STOPPED recovery, response-unknown fence, closure-versus-
quiescence separation, and configuration boundary. Exact anchor wire/layout, bounded carry-forward representation,
terminal publisher convergence, and numeric limits remain M4/M5 work; implementation has not started at M0.

## Context

ADR 0075 originally froze `PWF(O,E) -> PO(O,E)` and required a later same-owner `PO(O,E) -> PO(O,E+1)` rollover
before E could become terminal. That adds one selector CAS, creates another unresolved closure interval, and retains an
already removed fallback until reconciler cadence advances the epoch. A selector value can instead remove fallback,
close E, and grant no-fallback E+1 atomically.

## Decision

`BindingReadSelector.readAdmissionState` is a closed discriminator with only:

- `ADMITTING`: the exact selected Owner Epoch and Read Admission Epoch may admit reads; and
- `STOPPED`: no read may be admitted until a later CAS grants a fresh never-reused Read Admission Epoch.

`CLOSING`, `DRAINING`, terminal-proof progress, and reconciler progress are not reader-visible selector states.
Closure only stops new admission; quiescence still waits for every already admitted source use to drain or for ADR
0074 qualified authority expiry.

For exact predecessor `(PWF, O, E, ADMITTING)`, the competing transitions are:

```text
takeover/read grant:
  (PWF, O, E, ADMITTING)
    -> (PWF, O2, E+1, ADMITTING, closureAnchor[E])

no-fallback cut:
  (PWF, O, E, ADMITTING)
    -> (PO, O, E+1, ADMITTING, SourceRetirementBatch[last=E], closureAnchor[E])
```

The no-fallback CAS performs four inseparable actions: freezes every removed source's shared last fallback-capable
epoch as E, irreversibly closes E, grants the same owner no-fallback reads under E+1, and persists E's closure anchor.
There is no intermediate selected `PO(O,E)` and no mandatory later same-owner rollover. Takeover and no-fallback use
the same exact expected selector tuple; whichever commits first forces the other to conflict, reread, and recompute.

A transition that introduces fallback uses a new source-protection identity and atomically advances to a fresh Read
Admission Epoch in the same selector CAS. A view update that leaves the exact fallback membership and protection
identities unchanged need not advance the epoch and must preserve every member's original first fallback-capable
epoch. Removing and later reintroducing the same physical bytes never revives the old identity.

Every closure anchor binds the canonical predecessor selector tuple/SHA, successor tuple/SHA, transition digest,
closed Read Admission Epoch, Owner Epoch, and exact capability-evidence generation/digest. The successor selector value
must carry that predecessor/transition digest, or a backend transaction must persist it atomically with the selector
CAS. A backend's old value, version history, watch stream, or CAS receipt is not assumed to remain queryable and cannot
be the sole durable anchor.

After a successful E-closing CAS, no new slot may publish E. Existing E slots retain their complete source lifetime;
only their terminal drain or qualified expiry permits asynchronous `ReadAdmissionEpochTerminalCut` publication. A
fallback-relevant anchor remains durably verifiable until that terminal cut exists. An epoch that never intersects a
fallback interval needs no terminal cut or proof; its transition anchor need only survive response-loss/recovery
convergence.

On a selector-CAS response unknown, the process admits no further read under E until exact reread converges. Exact
successor equality authorizes only the successor epoch; exact predecessor equality permits a fenced retry; any other
value requires conflict recomputation. Current-epoch comparison cannot manufacture a missing predecessor transition.

Hard-cap exhaustion may atomically close E and enter `STOPPED`; it may not keep E admitting reads while waiting for
capacity. Admission budgeting therefore reserves enough emergency selector/anchor capacity to persist that fence.
Recovery from `STOPPED` grants only a fresh never-reused epoch after all admission checks pass; it never reopens E.
Unresolved fallback anchors consume hard count/bytes/age budgets. Exhaustion blocks a new `ADMITTING` grant and never
drops an anchor, terminal cut, or proof.

Selector states, fused closure, anchor durability, STOPPED behavior, and response-unknown fencing are non-disableable
correctness contracts. Topic policy cannot add states, split the fused cut, reopen E, or bypass an anchor. Configuration
is limited to Cell/Binding hard caps, reconciler cadence, and evidence-derived capacity parameters. Ordinary reads use
the validated cached selector fence and perform no metadata I/O.

## Consequences

- `V2-OPEN-READ-12` is resolved without an extra same-owner selector CAS after fallback removal.
- Each fallback-relevant transition pays one selector CAS plus asynchronous terminal/proof publication. The risks are
  anchor backlog, takeover conflicts, and retained-source delay, not stable-state read throughput.
- M4/M5 must prove both CAS orders, fused four-action visibility, fallback reintroduction, membership-neutral updates,
  anchor persistence without backend history, response-unknown admission fencing, STOPPED emergency capacity and
  fresh-epoch recovery, terminal non-reopening, hard-cap backpressure, and zero ordinary-read metadata I/O.

This decision refines ADRs 0069, 0071, 0073, 0075, and 0076 and is tracked by `T-MANIFEST-01`, `T-HANDOFF-01`,
`V2-READ-006/008/010/012`, and `V2-OPEN-READ-14`.
