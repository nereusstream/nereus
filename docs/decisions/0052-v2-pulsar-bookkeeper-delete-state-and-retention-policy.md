# ADR 0052: V2 Pulsar BookKeeper delete state and retention policy

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

The stock-compatible `bookkeeperDeleted` boolean can fence BookKeeper reads, but it cannot distinguish a physical delete
that has not been issued, an issued delete with a lost response, and a confirmed success/absence. V2 also needs an
explicit operator choice between retaining the verified BookKeeper copy and deleting it without turning deletion safety
steps into optional flags.

## Decision

Attempt-scoped native metadata uses the irreversible state
`BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`.

`bookkeeperDeleted=true` is only a compatibility fence meaning “BookKeeper has lost read eligibility”; it is true
exactly for INTENT and DONE. Only DONE is the durable fact that physical deletion succeeded or returned an authoritative
`NoSuchLedger`. Inconsistent boolean/three-state combinations fail closed. Retirement, audit, residue reporting, and
physical-capacity accounting use the three-state fact, never the boolean alone.

Topic/Namespace policy selects one closed retention class for an offload attempt:

- `RETAIN_BK`: keep state NONE and retain BookKeeper after Object publication;
- `DELETE_AFTER_VERIFIED`: after ADR 0045 pin fencing/drain and final Object revalidation, CAS NONE to INTENT with a
  deterministic operation ID over ledger ID, attempt UUID, and NPO1 Root SHA; then perform idempotent deletion and CAS
  the same INTENT to DONE after success/absence proof.

The resolved retention class is persisted in attempt/native metadata. Once INTENT is durable it can never move back to
NONE or `RETAIN_BK`; restart retries/reconciles the exact operation and never restores BookKeeper read eligibility.
Permanent delete failure leaves an alerting physical residue and blocks retirement but does not roll logical authority
back.

For `DELETE_AFTER_VERIFIED`, pin drain, Object revalidation, INTENT, physical deletion proof, and DONE cannot be skipped
or disabled. Delete/revalidation concurrency, bandwidth, retry delay, and timeout budgets are Cell/host policy and may
delay progress only.

## Consequences

- `V2-OPEN-BK-12` is resolved.
- `RETAIN_BK` preserves costlier physical redundancy; `DELETE_AFTER_VERIFIED` pays one additional metadata fact and may
  retain outage residue for deterministic recovery.
- The compatibility boolean is not a deletion receipt.
- M2/M5 must prove both policies, every CAS/delete response-loss cut, `NoSuchLedger`, inconsistent-state rejection,
  INTENT irreversibility, restart retries, capacity/audit accounting, and retirement veto before DONE.

This decision refines ADRs 0017, 0020, 0036, 0045, and 0049 and is tracked by `T-BK-01`, `T-POLICY-01`,
`V2-BK-010/011`.
