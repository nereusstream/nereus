# Stage B V4 RANGE-renewal acknowledged-proof reuse

- Design status: Accepted through ADR 0135
- Runtime status: production workflow correction pending exact-source diagnostic
- Selection authority: none

## Residual boundary

Exact `9fcbc7f2...` proves that unrelated installed grants no longer wait behind a foreign Cell reservation. Fixed-1000
drop falls to 156, while derived-800 remains zero-drop. Only 108 reservation-busy retries remain, but the fixed row
still executes 1,336 operations above four per observed workflow. Receipt `e7d0b45a...8c86` and archive identity
`fa985817...c4b` are immutable diagnostic-only history.

## Successful renewal chain

The bounded workflow begins with an exact combined Cell/Head read. Reserve uses an exact predecessor Cell CAS and
returns the exact reserved Cell. RANGE install then computes the same pure successor and performs the exact predecessor
Head CAS without rereading that Cell. Clear uses the acknowledged installed Head and exact reserved Cell predecessor
without rereading the Head. Its acknowledged cleared Cell and installed Head feed the already accepted store-observed
candidate-create and publish path.

The resulting first-grant success chain is:

`Cell+Head read -> reserve Cell CAS -> install Head CAS -> clear Cell CAS -> candidate create -> publish Head CAS`.

Each result is request/key/version bound. No caller-supplied success Boolean or unversioned aggregate becomes
authority.

## Fail-closed fallback

Only the initial acknowledged path skips duplicate reads. Indeterminate or predecessor-unchanged install/clear
results retry through the public proofful methods; definitive conflicts enter the existing exact reread/reconcile
branches. STRICT, direct public API, takeover, fault, response-loss, and same-Head contention semantics are unchanged.

## Next gate

The next pushed exact source must rerun the same 25ms fixed-1000 then derived-800 sequence. Both rows require zero
drop/failure/timeout before the complete 23-test/nine-suite canonical NADV4 and any V4 formal execution.
