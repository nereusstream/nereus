# ADR 0038: V2 Object WAL provider-absent crash contract

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

A WalRun Root plus strong bounded LIST discovers final bodies that reached the provider. It cannot reconstruct a body
that is proven absent after the producing process lost its exact frame set, compression output, encryption key/header,
and ciphertext. A deterministic nonce alone cannot recreate all those bytes. Requiring exact retry would make a
pre-PUT broker-local fsync journal a second durability prerequisite for the cost-first profile.

## Decision

0.2 does not require or claim a broker-local exact-ciphertext recovery journal.

After process loss, recovery handles an in-flight group as follows:

1. if the exact content-addressed object is present, validate its key-derived length/SHA, provider/body proof, root
   binding, header, contexts, frames, commit sets, coverage, and idempotency before reconciling it;
2. if qualified LIST/HEAD/GET behavior proves the old conditional PUT absent and the group was never acknowledged,
   permanently fence the old run at its proven contiguous Durable Frontier and burn the old run/sequence identity;
3. only after that absence/fence proof may protocol idempotency rebuild the request in a fresh run/group;
4. if presence versus absence cannot be resolved inside the accepted recovery envelope, remain fail-closed and do not
   start a conflicting fresh attempt or advance a frontier.

The new owner never claims it can reproduce the original key/body and never continues the old run across the burned
gap. Any provider-present unacknowledged tail is reconciled by its verified idempotency/coverage facts or quarantined; it
is not exposed merely because LIST found it.

If a future requirement demands cross-process continuation of the exact original attempt, it requires a separate
accepted contract for a PUT-before fsynced journal containing the exact post-encryption ciphertext, length, final key,
and completion state. Deterministic nonce generation alone is insufficient.

## Consequences

- `V2-OPEN-OBJ-10` is resolved.
- A never-ACKed provider-absent attempt can fail and require client retry after process loss; this is the explicit
  availability cost of avoiding host-affine local durability.
- Provider admission and protocol idempotency evidence become correctness prerequisites for safe fresh retry.
- Exact uncertainty timeouts, quarantine visibility, and crash vectors remain downstream implementation/evidence
  gates.
- M3 must prove present/absent/unknown cuts, no reuse after a burned gap, no duplicate protocol positions, client retry
  convergence, and rejection of deterministic-nonce-only replay claims.

This decision refines ADRs 0018 and 0030 and is tracked by `T-APPEND-01`, `T-OBJECT-01`, `V2-APP-003`, and
`V2-OBJ-003/005/008`.
