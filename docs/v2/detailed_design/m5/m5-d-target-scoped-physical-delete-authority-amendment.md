---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesignAmendment
sourceTuple: v2-m1
---

# M5-D amendment: target-scoped physical-delete authority

## Authority and exact supersession

This is the accepted additive
`M5-AMENDMENT-2-TARGET-SCOPED-PHYSICAL-DELETE-AUTHORITY-V1` authorized by
[ADR 0147](../../../decisions/0147-v2-m5-target-scoped-physical-delete-authority-amendment.md). It chains the immutable
M5 hard-freeze at `c86fde3ed6f4319642987fd599022bd32e2cca5e` and the accepted
[M5-C amendment](m5-c-single-binding-retirement-authority-amendment.md), whose exact predecessor manifest is
`m5-design-amendment-1.json`.

It supersedes exactly:

- M5-D's requirement that one conditional multi-key metadata transaction validate the proof vector before and after
  the external identity read; and
- the corresponding M5-E physical-delete child requirement to demonstrate that multi-key primitive on the
  source-locked Oxia backend.

For one immutable physical target, those requirements become one target-scoped authority cell, complete
closed-writer enrollment, ticket/fence serialization, and exact same-key CAS before and after the external identity
read. No other frozen I0/A-E identity, target field, veto, cleanup order, terminal meaning, evidence ownership,
scenario state, historical M4 dependency, or M6/M7/M8/production exclusion changes.

This amendment creates no implementation PASS, delete intent, dispatch authority, physical-delete authority,
source-bound receipt, scenario promotion, staging authority, or production authority.

## Rejected backend capability

The source-locked Oxia Java client `0.9.4` at
`091a42c2780d92da56e9ec1f02ce1c3d988adc16` has exact same-key CAS but no public conditional multi-key transaction.
The source-locked Oxia server at `37a17bef17202d5fd6e23282da5fd26d94865484` does not turn its internal write batch
into all-conditions-or-zero-mutations semantics. `ExactMetadataTransactionStoreV1` therefore continues to reject
sequential emulation, and `Oxia09ExactMetadataTransactionStoreV1.conditionalTransaction` continues to return
`UNSUPPORTED` without I/O. Neither API is weakened or relabeled by this amendment.

## One permanent authority cell per target

The canonical key is:

```text
v2/physical-delete-m5/<CellProviderScopeId-hex>/<TargetIdentitySha256-hex>/authority-v1
```

Both digests are exactly 64 lowercase hexadecimal characters. The key is bounded by the existing metadata key cap.
The target digest is domain-separated over the complete frozen `PhysicalDeleteTargetV1`, including source kind,
logical range, physical identity, creation owner/epoch, M4 release identities, current manifest/selector/trim and
reference-proof roots, Provider Scope, Object/ledger/root/data/multipart identity, capability/fence generations,
operation policy and grace roots. The same physical target cannot derive two keys, and two target identities cannot
share one key.

The key is created once, never deleted, never moved, never reused, and never recreated after terminal completion.
It is the sole deletion authority. Other metadata keys remain proof inputs guarded by tickets; they are not parallel
delete authorities.

## Canonical `TargetDeleteAuthorityV1`

The exact authority value contains:

```text
TargetDeleteAuthorityV1
├── schemaVersion / authorityRevision / predecessorAuthoritySha256 / canonicalSha256
├── authorityKey / CellProviderScopeId / immutable PhysicalDeleteTargetV1 / targetIdentitySha256
├── state: OPEN_V1 | READ_FENCED_V1 | DELETE_INTENT_V1 | DELETE_DONE_V1
├── closedWriterFenceEpoch / writerEnrollmentRoot / proofSnapshotDigest
├── activeWriterTickets[]
│   └── writerClass / capability / ownerFence / operationId / externalFactsRoot / predecessorRevision
├── optional ReadFence
│   └── attemptId / fencedRevision / fenceEpoch / proofSnapshotDigest / eligibilityRoot
├── optional ExternalIdentity
│   └── exact Object version/body identity, BookKeeper fingerprint, or multipart key/upload ID inventory
├── optional DeleteIntent
│   └── deleteAttemptId / dispatchEpoch / dispatchOwnerFence / identityDigest / capabilityDigest
└── optional DeleteDone
    └── intentDigest / terminalOutcome / absenceInventoryRoot / completionProofDigest / final dispatch owner
```

All lists use canonical order and closed enum inventories. All byte strings, counts and aggregate bytes have finite
positive hard caps selected before admission. Unknown enum values, duplicate tickets, duplicate external identities,
an inconsistent optional phase, a zero digest/revision/epoch, an oversized value, or a canonical-self-digest mismatch
is `QUARANTINE`.

Every successful same-key CAS increments `authorityRevision` by exactly one and binds the exact predecessor value
digest. Revision zero, wraparound, gaps, reuse, rollback, and no-op CAS are forbidden. Ticket acquire/release, proof
refresh, read fence, intent, dispatch takeover and done all consume a revision. Consequently a value cannot move
`A -> B -> A` with byte-identical authority bytes.

## Closed writer inventory and admission

The enrollment is closed and must cover every writer that can change an M5-D eligibility fact:

1. M4 source-protection and release writer;
2. manifest, selector, generation and representation writer;
3. logical trim and retention-floor writer;
4. each closed reference-kind writer and shared-physical-member writer;
5. replica and topology writer;
6. multipart inventory and publication/response-loss writer;
7. materialization, compaction, projection, migration and recovery task writer;
8. owner, worker, lease, open-handle and pin writer;
9. Provider, KMS, storage/profile and delete-capability admission writer; and
10. dispatch owner/epoch and per-Cell reservation writer.

Enrollment binds the exact writer class inventory, capability generations/digests, implementation root and policy
root. Any target touched by an unenrolled or legacy writer is `NOT_ELIGIBLE`. Enrollment completeness is explicit
evidence; absence of a known concurrent writer is not inferred from quiet traffic.

An enrolled writer may mutate proof-bound external metadata only through:

```text
OPEN_V1 / NO_TICKET
  -- exact authority CAS, revision + 1 -->
OPEN_V1 / ACTIVE_WRITER_TICKET
  -- external mutation and authoritative reconciliation -->
OPEN_V1 / updated proofSnapshotDigest / NO_TICKET
  -- exact authority CAS, revision + 1 -->
```

The ticket binds target, writer class, exact capability, owner/lease fence, deterministic writer operation ID,
external key-set/root and predecessor revision. A ticket is visible before external dispatch. Response loss,
ambiguous reconciliation, incomplete pagination, changed owner, or failed mutation retains the ticket. A timeout is
never sufficient to clear it. Stale-ticket recovery must first prove the old owner or lease epoch fenced, then
reconcile the exact external operation and only then remove the ticket with another exact CAS.

`READ_FENCED_V1`, `DELETE_INTENT_V1`, and `DELETE_DONE_V1` reject new tickets and all proof-changing writes. A writer
whose acquire CAS loses to the read-fence CAS has not dispatched. A read-fence CAS that loses to a ticket does not
advance and must wait for exact ticket reconciliation.

## CAS-1: close writers and prepare the identity window

The coordinator reads the exact `OPEN_V1` authority bytes and independently validates every frozen M5-D predicate:

- target no longer selected as preferred, fallback, or only readable generation;
- logical trim and every retention floor cover the complete range;
- exact M5-C reference-free proof and complete version vector remain current;
- every applicable M4 protection is the exact matching `RELEASED` generation/value;
- no pin, handle, recovery, task, projection, response-loss row, audit grace, replica or shared member references the
  target;
- Cell, Provider Scope, namespace, credentials, KMS, storage/profile, owner, worker and capability fences match;
- grace deadline and per-Cell dispatch/unknown reservations are admitted; and
- enrollment is complete with zero active, stale or unresolved writer tickets.

It then exact-CASes those `OPEN_V1` bytes to `READ_FENCED_V1`, increments revision and
`closedWriterFenceEpoch`, and stores `ReadFence(attemptId, fencedRevision, fenceEpoch, proofSnapshotDigest,
eligibilityRoot)`. This first CAS is the writer-closure linearization point. It is not a delete intent and grants no
external dispatch authority.

A veto before CAS-1 leaves the exact open value unchanged. A CAS conflict rereads and restarts eligibility. Response
loss is reconciled from the same authority key: exact fenced successor means applied, exact open predecessor means
definitively not applied, and any other value is conflict/quarantine. No cross-key success matrix exists.

## External full-identity read under the fence

After CAS-1 is authoritatively reconciled, the coordinator rereads the exact fenced authority and executes only the
target-specific bounded identity read:

- Object: full GET with canonical length, full-body SHA-256, format/root/footer identity and immutable Provider
  version token;
- BookKeeper: sealed ledger ID and complete ensemble/digest/password/last-entry/length/metadata-version fingerprint;
- Pulsar: exact NPO1 root before NPD1 data, with their full body identities and immutable versions; or
- multipart: every persisted exact object-name/upload-ID pair, complete exact-key pagination and same-key foreign
  upload veto.

LIST membership, ETag, directory-prefix inference, a delete response, local cache, or a partial page is not identity
or absence proof. A missing exact source is recorded only as a typed exact-identity absence observation for CAS-2; an
ambiguous read retains the fence. A different body/version/generation, foreign upload or malformed inventory
quarantines and never dispatches.

## CAS-2: bind identity and create the only dispatch authority

The coordinator compares the byte-exact `READ_FENCED_V1` predecessor. It verifies unchanged revision, fence epoch,
target, proof snapshot, eligibility root, Provider/BookKeeper identity and capability, then binds the exact external
observation, fixes one `deleteAttemptId`, assigns one monotonic `dispatchEpoch` and exact `dispatchOwnerFence`,
increments revision, and enters `DELETE_INTENT_V1`.

Only exact application or exact same-key reconciliation of this CAS authorizes external deletion. The dispatch token
is the tuple:

```text
authorityKey + targetIdentitySha256 + intentAuthorityRevision
+ deleteAttemptId + dispatchEpoch + dispatchOwnerFence + exact external identity digest
```

An adapter must verify the whole tuple and exact target before calling Provider/BookKeeper. The request must carry the
bound Object version, sealed-ledger fingerprint, or exact upload ID. Unversioned Object deletion, key-only multipart
abort, ledger-ID-only deletion without the sealed fingerprint, and fallback to an older unconditional seam are
forbidden.

A revision change before CAS-2 rejects the observation and requires a new fenced attempt; it cannot be copied into a
later authority value. Response loss uses the same-key three-way reconciliation. No second BEGIN or new delete
attempt is allowed while an intent is unresolved.

## Dispatch ownership, response loss and recovery

`DELETE_INTENT_V1` is irreversible. A process may dispatch only while its exact dispatch owner/epoch matches the
authority. Crash before dispatch leaves a resumable intent. Crash after provider execution leaves the same intent.
Another node may take ownership only by proving the previous owner/lease fenced and exact-CASing a successor with the
same target, intent and delete attempt, a higher dispatch epoch, and a new authority revision.

Every retry uses the identical physical identity:

- exact Object version still present: retry version-match delete under the same intent;
- exact sealed ledger fingerprint still present: retry exact BookKeeper delete;
- exact owned upload still present: retry exact object-name/upload-ID abort;
- exact identity authoritatively absent: build the final absence inventory;
- changed/recreated Object, different ledger metadata, foreign multipart upload, partial/incomplete list, or unknown
  response: remain pending or quarantine.

`ALREADY_ABSENT` advances only when reconciliation proves absence of the exact version/generation/ledger/upload ID
bound by the intent. A later Object reusing the same key is different identity and is never deleted by the old intent.

## `DELETE_DONE_V1` and permanent ABA protection

After every target-specific operation and required root/data/multipart order is authoritatively absent, the
coordinator exact-CASes the current intent to terminal `DELETE_DONE_V1`. The successor increments revision and binds:

- exact intent canonical digest and intent authority revision;
- target, fixed delete attempt and final dispatch epoch/owner;
- typed terminal outcome;
- complete authoritative absence inventory root;
- exact Provider/BookKeeper capability and admission digest; and
- completion proof digest covering every reconciliation page/read and required ordering fact.

The done record is permanent. It rejects tickets, read fences, new intents, dispatch takeovers and repeated
scheduling. The authority key is never deleted or recreated. A matching done reread is `EXISTING_EXACT`; a changed
done, reconstructed open record, target mismatch, revision rollback or missing key is `QUARANTINE`.

## Multi-target and protocol ordering boundary

One authority key covers one target only. Object root, Object data, each replica, BookKeeper ledger and each exact
multipart upload are separate targets even when a business operation groups them. The existing Pulsar ordering core
still requires authoritative NPO1 root absence before NPD1 data and owned multipart absence. A batch coordinator may
sequence these target authorities but cannot call them one atomic transaction.

If a business invariant requires multiple physical targets to commit all-or-none, it remains unsupported by this
amendment. Partial physical cleanup is represented as exact per-target state and never promoted to aggregate done.

## Hard caps and fail-closed admission

Implementation/evidence must select finite positive caps for authority bytes, enrollment rows, active tickets,
ticket bytes, external identities, multipart uploads/pages/identity bytes, attempts, dispatch takeovers, unknown
outcomes, scan duration, per-Cell queues and retained done records. The effective cap is the minimum of protocol,
Cell, host, Provider and metadata backend limits. Cap exhaustion retains data and stops new admission.

There is no force-delete, force-ticket-clear, skip-identity, assume-absence, delete-any-version, key-only abort,
ledger-ID-only downgrade, cross-Cell borrowing, or timeout-authorized terminal path.

## Required implementation and evidence delta

Before the `PHYSICAL_DELETE_ORPHAN_RECONCILIATION` child can pass, implementation must prove:

- canonical target/authority codec, deterministic key/root, revision and predecessor-digest invariants;
- every closed writer class enrolled and both ticket-before-fence/fence-before-ticket race orders;
- writer crash after external metadata mutation retains a ticket until exact reconciliation;
- CAS-1 writer closure, identity reads under the fence, CAS-2 identity/attempt/dispatch binding, and no dispatch before
  exact intent;
- revision ABA rejection, changed revision before CAS-2, stale proof/fence/capability and every frozen veto;
- crash after CAS-2, crash before dispatch, provider response loss, exact retry, dispatch-owner takeover and restart;
- same-key Object recreation protection, exact sealed-ledger mutation conflict, same-key multiple multipart uploads,
  foreign upload veto, pagination failure/incompleteness and root-before-data ordering;
- exact terminal absence inventory, permanent done, no repeat scheduling and cap exhaustion; and
- per-target non-atomic batch reporting and per-Cell admission/isolation.

The child must run against real source-locked Oxia exact same-key CAS and the real admitted fixed Provider and
BookKeeper boundaries. In-memory tests, focused adapter gates, amendment acceptance, code existence, or partial
receipts cannot promote M5-D or any scenario.

## Amendment boundary

The original M5 hard-freeze, M5-C amendment and this M5-D amendment form one ordered governance chain. The immutable
original M5-D file is not edited. `m5-design-amendment-2.json` binds both predecessors and the exact bytes of ADR 0147
and this document.

No blocking design question remains for this amendment. Implementation remains `NotStarted`, evidence remains
`NotRun`, all M5 scenarios remain `PLANNED`, and no physical-delete or production authority exists at this boundary.
