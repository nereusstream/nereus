---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-D physical delete, orphan, and GC

## Goal and safety posture

M5-D is the first V2 phase that may execute irreversible physical reclamation. It therefore starts from exact
persisted authorities and fails closed. A delete request, controller decision, worker receipt, local zero-reader scan,
logical trim, retired metadata tombstone, age, or apparent provider absence cannot substitute for the full protocol.

If any required fact is missing, stale, unsupported, over budget, or ambiguous, the result is `RETAIN`. Integrity or
identity conflict is `QUARANTINE`. There is no force-delete path in 0.2.

## Exact delete target

`PhysicalDeleteTargetV1` is immutable and binds:

- the common M5 identity envelope and exact logical coverage;
- source kind and source identity, source generation, and creation task/Owner Epoch;
- M4 protection key, protection generation, full canonical `RELEASED` value SHA-256, released-by batch SHA-256, and
  proof-head SHA-256 for every M4-governed readable source;
- current selector/manifest/trim roots plus the fresh M5-C `ReferenceFreeProofV1` identity;
- Object key, canonical length, full-body SHA-256, format/root/footer identity, Provider Scope, immutable Provider
  version precondition, and multipart identities where applicable;
- BookKeeper ledger ID, ensemble/digest/password-credential identity, sealed last entry/range, native attempt UUID and
  metadata version where applicable;
- owner, worker, storage, provider-admission, KMS, and delete-capability generations/digests; and
- deterministic `DeleteOperationId` and policy/grace roots.

The operation ID is a domain-separated SHA-256 of all target fields and contains no retry/host/time value. A physical
key or ledger ID without this envelope is never a delete target.

## Final revalidation predicate

Immediately before creating intent, and again immediately before external dispatch, the coordinator proves:

1. the exact current manifest no longer selects the target as preferred, fallback, or the only readable generation;
2. typed logical trim and every retention floor cover the complete target range;
3. the M5-C reference-free proof version vector is still exact and every reference kind remains absent;
4. every applicable M4 protection record is the exact same `RELEASED` generation/value;
5. no generation pin, open handle, recovery root, materialization/compaction task, projection/migration, response-loss
   row, audit grace, or shared physical member references the target;
6. Protocol Cell, Provider Scope, namespace, credentials, KMS, storage/profile epoch, owner and worker fences match;
7. the external source still has the exact immutable identity, length, digest, format, and ledger/root relationship;
8. the evidence-admitted delete capability is current and the grace deadline has passed in authority time; and
9. per-Cell dispatch and unknown-outcome reservations are available.

Revalidation is a conditional metadata transaction plus an external full identity read. The transaction is rerun
after that read to close the metadata race. For Object deletion, the external delete itself additionally carries the
exact Provider version precondition observed by the full read, closing the Object replacement race. Any intervening
change rejects dispatch.

## Object delete capability

M5 admits only `VERSION_MATCH_DELETE_V1` for ordinary Object deletion. A concrete Provider adapter must prove:

- the conditional delete compares one canonical immutable version token for the exact key;
- a different/missing/recreated version is not deleted and has a typed precondition-failed result;
- success means the matched version was deleted or was already authoritatively absent under the same closed identity;
- response loss is typed separately from success/not-found/conflict;
- strongly consistent exact GET plus complete bounded LIST can prove final key absence;
- token bytes have a strict canonical binary cap and are never treated as payload identity; and
- exclusive Cell namespace, conditional-create/no-overwrite policy, credentials, and key non-reuse are enforced.

The full GET's canonical length and SHA-256 prove content identity. The Provider version token only fences the race
between validation and delete. ETag, LIST membership, user metadata, key naming, or no-overwrite policy alone is not
content or delete authority. If the Provider returns no admitted immutable precondition, M5 retains the Object.

The future shared `ObjectProviderTransport` extension must expose conditional delete and typed outcomes without
weakening the existing C1 create/read/list contract. Provider-product code remains in `nereus-storage-object-s3`.

## Object deletion state machine

The metadata state is irreversible:

```text
DELETE_NONE
  -- exact target/reference/fence transaction --> DELETE_INTENT
  -- conditional external delete + authoritative absence --> DELETE_DONE
```

`ObjectDeleteIntentV1` stores the complete target, reference-free proof, predecessor versions, capability binding,
and attempt ledger. Intent is written before dispatch. It permanently prevents any legal writer from recreating or
republishing that physical identity, but it is not proof the bytes are gone.

After intent, the coordinator:

1. rereads intent and all top-level fences;
2. performs the exact full GET and identity validation;
3. rereads the metadata fence;
4. issues conditional delete with the observed exact version token;
5. on success, not-found, or response unknown, performs bounded complete LIST and exact GET reconciliation;
6. aborts/list-reconciles every exact multipart upload owned by the operation when applicable; and
7. exact-CAS publishes `DELETE_DONE` only after the key and owned multipart residues are authoritatively absent.

If response is unknown and the exact old version still exists, the same operation may retry with that exact
precondition. If a different version or body exists, delete is not retried and the target quarantines. If absence
cannot be proven, intent remains pending. Restart resumes from intent; intent never rolls back to none.

`ObjectDeleteDoneV1` binds intent SHA-256, operation ID, final absence inventory root, exact provider/capability
identity, and response reconciliation. It is audit evidence, not permission to delete another object.

## Pulsar NPO1 root/data and multipart cleanup

For a Pulsar sealed-ledger attempt, the existing deterministic root/data identity is retained. Cleanup order is:

1. prove the exact attempt is no longer readable/referenced and create its M5 delete intent;
2. conditionally delete the NPO1 root and prove exact root absence;
3. only then conditionally delete the NPD1 data Object and prove exact data absence;
4. abort/reconcile exact owned multipart uploads for both keys; and
5. prove root, data, and multipart residue are all absent before `DELETE_DONE`.

Deleting data while a readable root exists is forbidden. A missing root does not by itself authorize data deletion;
the exact attempt identity, M4 release where applicable, reference-free proof, and intent are still required.

## BookKeeper deletion composition

M5 reuses, rather than replaces, the accepted M2/P4 native state machine:

```text
BK_DELETE_NONE
  -> BK_DELETE_INTENT + bookkeeperDeleted=true
  -> physical delete or authoritative BKNoSuchLedgerExists
  -> BK_DELETE_DONE
```

For every M4-governed BookKeeper fallback, exact M4 `RELEASED` is mandatory in addition to P4 eligibility. The
coordinator also requires `DELETE_AFTER_VERIFIED`, exact offload attempt UUID/version, verified Object root/data read
path, M5 retention/reference proof, and current owner/storage/provider fences.

Before intent it fences new BK pins, drains all already accepted pins, and performs final Object/read-path
revalidation. The exact native CAS publishes intent. From intent onward no new BookKeeper fallback is legal; restart
closes/invalidates the cached child, deletes the exact sealed ledger, reconciles response loss by authoritative
BookKeeper read/no-such-ledger, and only then CASes done.

`bookkeeperDeleted=true` is a compatibility view present from intent. It means BookKeeper is no longer a legal read
source; it does not mean physical capacity was reclaimed. Only exact `BK_DELETE_DONE` proves lifecycle completion.
Timeout or failure before intent may reopen pins only if the exact native metadata version remains current; failure
after intent never rolls back.

## Physical orphan taxonomy

An "orphan" is not any unreferenced-looking row. M5 uses this closed classification:

| Class | Examples | M5 action |
| --- | --- | --- |
| `PHYSICAL_OUTPUT_ORPHAN_CANDIDATE` | deterministic unselected materialization/compaction payload, index, spill, page, checkpoint Object | may enter mark/grace/rescan/delete protocol |
| `MULTIPART_RESIDUE_CANDIDATE` | exact upload ID/parts owned by a terminal or abandoned task | may abort only after task/fence/reference proof |
| `RELEASED_SOURCE_CANDIDATE` | old Object or sealed ledger with exact M4 release | ordinary delete protocol; release alone is insufficient |
| `PERMANENT_METADATA_FENCE` | retired batch tombstone, Pulsar generation selector/incarnation tombstone | never an orphan and never deleted in 0.2 |
| `ALLOCATOR_NO_REUSE_EVIDENCE` | Pulsar virtual-ledger allocator orphan/reservation candidate from ADR 0061 | permanent no-reuse evidence; no M5 GC |
| `UNKNOWN_OR_FOREIGN` | unparseable key, foreign Cell/Scope, unknown format/version/owner | do not touch; quarantine/alert only |

Object listing is discovery, never authority. An orphan candidate must have a parseable deterministic identity in the
exact Cell namespace and an authoritative task/manifest/reference scan showing no legal owner.

## Mark, grace, rescan, delete

Physical output and multipart candidates follow:

```text
DISCOVERED
  -> ORPHAN_MARK(first authoritative observation)
  -> required audit/provider grace
  -> full rescan under current fences
  -> DELETE_INTENT
  -> authoritative absence
  -> DELETE_DONE
```

The mark binds key/upload ID, length/digest/version when readable, discovery inventory root, missing-owner proof,
task/manifest versions, and authority-time grace deadline. A second complete scan must reproduce absence of every
reference and prove no response-loss create/publication path could still adopt the object. Age before the first mark,
mtime, LIST order, cache absence, or two identical local scans does not count.

Unknown create outcomes remain owned by their original task until reconciled; they are not orphaned by timeout. A
newly found exact object that matches a live deterministic task is adopted, not deleted.

## Per-Cell execution and admission

Workers, hosts, and transports may be shared, but the following are separately reserved and accounted for each Cell:

- candidate/intent/done counts and bytes;
- delete and reconciliation queues, oldest age, and response-unknown slots;
- Object GET/LIST/DELETE, multipart, BookKeeper, Oxia, KMS, and network concurrency;
- request/byte/I/O-rate budgets and retry budgets;
- cache bytes/entries and per-target buffers;
- scanner pages/keys/bytes and task/orphan grace backlog; and
- quarantine count/bytes/age.

Scheduling is weighted/fair only after hard per-Cell reservations. One Cell cannot borrow authority, namespace,
credentials, intent capacity, unknown-outcome capacity, or reserved minimum progress from another. When a Cell reaches
a cap, its new materialization/profile-handoff/GC work stops and alerts; other Cells continue within their own caps.
Global host ceilings may reduce everyone by minimum but never merge accounting.

## Required evidence later

M5-D evidence must include every final-revalidation veto; changed manifest/protection/reference/fence between each
step; Provider token mismatch and same-key recreation; delete success/not-found/conflict/retryable/unknown; exact old
version remaining; different version appearing; incomplete LIST; root-before-data enforcement; multipart residues;
BookKeeper intent response loss/delete response loss/no-such-ledger/restart/pin timeout; proof that the compatibility
boolean is not DONE; all orphan classes including allocator no-GC and foreign scope; grace boundaries; cap exhaustion;
Cell noisy-neighbor isolation; and no deletion without an exact source-bound intent.

No blocking design question remains for M5-D. Production credentials, provider admission, numeric budgets, and
performance thresholds must be selected by later current-source evidence and do not exist at this design boundary.
