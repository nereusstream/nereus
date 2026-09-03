# ADR 0147: V2 M5 target-scoped physical-delete authority amendment

## Status

Accepted on 2026-09-03 for the M5-D physical-delete implementation. This is the narrow additive
`M5-AMENDMENT-2-TARGET-SCOPED-PHYSICAL-DELETE-AUTHORITY-V1`. It changes only the linearization mechanism that the
immutable M5-D design assigned to a conditional multi-key metadata transaction. It preserves every target identity,
eligibility proof, veto, cleanup order, grace, budget, Cell boundary, Provider/KMS/storage/capability fence,
response-loss rule, evidence owner, scenario state, and M6/M7/M8 or production exclusion.

The amendment itself is design authority only. It grants no runtime physical-delete authority, source-bound child,
scenario promotion, staging authority, or production authority.

## Context

The immutable M5-D design requires a conditional metadata transaction, an external full-identity read, and the same
metadata transaction again before dispatch. The source-locked Oxia Java 0.9.4 client exposes exact same-key
conditional mutation but no atomic conditional multi-key transaction. `ExactMetadataTransactionStoreV1` correctly
forbids sequential-CAS emulation, and `Oxia09ExactMetadataTransactionStoreV1` returns `UNSUPPORTED` without issuing
I/O for that operation.

ADR 0146 solved the analogous M5-C issue by making one existing Binding or Pulsar aggregate cell the sole
linearization point and by requiring every proof-changing writer to participate in its ticket/fence protocol. That
decision explicitly did not amend M5-D. M5-D therefore needs its own narrower authority family rather than an
implicit extension of ADR 0146.

## Decision

Each independent physical-delete target has exactly one permanent target-scoped authority key and one canonical
`TargetDeleteAuthorityV1` value. The key is derived from the Protocol Cell Provider Scope and the immutable target
identity digest. It is never moved, deleted, reused for another target, or recreated after completion.

The authority value is the sole deletion linearization point and contains at least:

- the complete immutable physical target and target digest;
- a strictly increasing `authorityRevision` and predecessor-value digest;
- the state `OPEN_V1`, `READ_FENCED_V1`, `DELETE_INTENT_V1`, or terminal `DELETE_DONE_V1`;
- the monotonic closed-writer fence epoch, closed writer enrollment, and bounded active writer tickets;
- the exact proof-snapshot digest and every proof/fence version needed by M5-D eligibility;
- the exact observed external identity;
- one fixed delete-attempt identity, dispatch epoch, dispatch owner fence, and dispatch lease/ownership fact; and
- the terminal outcome plus final authoritative absence/completion-proof digest.

Every successful mutation of the authority value, including ticket acquire/release, fencing, intent, dispatch
takeover, and done, consumes exactly one new non-zero `authorityRevision`. A successor may never reproduce an earlier
canonical authority value. No-op CAS is forbidden. This monotonic revision closes business-field ABA even if a writer
changes an external fact and later restores the same apparent value.

### Closed-writer serialization

Every writer capable of changing any M5-D proof-bound fact for the target must first exact-CAS a deterministic ticket
into the same target authority cell. This includes root/fence, manifest/generation, trim/reference/protection,
replica, multipart inventory, task, owner/worker/dispatch ownership, Provider/KMS/storage/capability, open-handle,
writer-lease, projection, and recovery facts.

The writer protocol is:

1. exact-CAS an enrolled proof-bound writer ticket into `OPEN_V1`;
2. mutate or reconcile the external metadata under the ticket;
3. authoritatively reread the exact successor or prove definitive non-application;
4. exact-CAS the updated proof digest and ticket removal into the same authority cell.

Response loss or incomplete reconciliation retains the ticket. Time alone never clears a ticket. Stale-ticket
recovery must prove the old owner/lease/epoch is fenced and must reconcile the external mutation before removal. A
writer class not enrolled in this protocol makes the target `NOT_ELIGIBLE`.

### Two-CAS identity protocol

The first deletion CAS does not authorize external mutation. Starting from the exact `OPEN_V1` bytes, with complete
eligibility, current fences, zero active/stale/unresolved tickets, and hard-budget admission, it increments revision
and fence epoch and enters `READ_FENCED_V1`. This transition rejects all new writer tickets and all proof-changing
control mutations for the target.

Only after that fence is visible may the coordinator perform the exact external full-identity read. Object reads
must bind canonical length, full-body SHA-256, format/root/footer identity and immutable Provider version. BookKeeper
reads must bind the sealed ledger metadata fingerprint. Multipart reads must use exact object name, exact upload ID,
full bounded pagination, and foreign-upload veto; directory-prefix inference is not evidence.

The second deletion CAS compares the exact fenced bytes and verifies unchanged target, revision, fence epoch,
eligibility-proof digest, Provider identity, capability and dispatch admission. It then binds the observed external
identity, fixes one `deleteAttemptId`, assigns one `dispatchEpoch` and owner fence, increments revision, and enters
`DELETE_INTENT_V1`. Only successful exact application or exact same-key reconciliation of this second CAS grants
dispatch authority.

External deletion is forbidden before `DELETE_INTENT_V1`. It must use the exact version, generation, ledger identity,
or upload ID bound by that intent. There is no unconditional or best-effort downgrade.

### Recovery and terminal state

`DELETE_INTENT_V1` is durable and irreversible. Timeout, disconnect, cancellation, or response loss never creates a
new intent or delete attempt. Recovery reuses the same attempt and exact target:

- exact old identity remaining permits retry under the same intent and dispatch fencing;
- authoritative absence of that exact identity permits an exact same-key CAS to `DELETE_DONE_V1`;
- changed/recreated identity, foreign multipart upload, incomplete listing, ambiguous BookKeeper response, or stale
  dispatch owner remains pending or quarantined and never advances;
- a recovery node may take dispatch ownership only through another revision-incrementing exact same-key CAS; and
- `DELETE_DONE_V1` is permanent, rejects all tickets and scheduling, and cannot return to another state.

The done value binds intent digest, fixed attempt, terminal outcome, exact Provider/BookKeeper capability identity,
authoritative absence inventory, completion-proof digest, and final dispatch owner/epoch. It is evidence about this
target only and never authorizes deletion of another target.

### One-target boundary

This amendment supplies linearization for one physical target only. Every Object version, sealed ledger, replica,
and multipart upload target has its own authority record. A batch may sequence multiple independent target
authorities, but their successful CAS operations are not a cross-target transaction. Any invariant requiring
all-targets-or-none deletion remains unsupported.

## Rejected alternatives

- changing `conditionalTransaction` or an equivalent `commitExactMultiKey` seam to report unsupported Oxia batches
  as atomic;
- sequential CAS across multiple keys followed by a success matrix;
- copying proof digests into a second key while other writers bypass the target authority cell;
- using only business-field equality without a monotonic revision;
- clearing tickets on timeout or local belief;
- starting external deletion from `READ_FENCED_V1`, an unpersisted identity read, or an unresolved prior attempt; and
- weakening identity, foreign-object, replica, grace, budget, owner, worker, Provider, KMS, storage, or capability
  vetoes.

## Consequences

- Oxia 0.9.4 exact same-key CAS can implement the target authority without pretending to support multi-key
  transactions.
- Every proof-changing writer must be enrolled and routed through the target cell before that target is eligible.
- Crashes may leave durable tickets, fences, intents, or dispatch ownership that conservatively retain physical data
  until exact recovery.
- Permanent authority records and revisions consume bounded metadata capacity; cap exhaustion stops admission.
- Multi-target cleanup remains non-atomic and must report per-target state.
- M5-D remains non-promotable until its source-bound child proves real Oxia, real admitted Provider/BookKeeper
  deletion, response-loss recovery, concurrency races, `DELETE_DONE`, and all frozen negative cases.

This decision is implemented by the additive
[`M5-D target-scoped physical-delete authority amendment`](../v2/detailed_design/m5/m5-d-target-scoped-physical-delete-authority-amendment.md).
The immutable original M5-D document remains unchanged.
