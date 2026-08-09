# ADR 0036: V2 Pulsar native dual-source read and deletion safety

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

Native offload completion alone is not a final proof that Object bytes still satisfy the V2 pair contract at the later
BookKeeper deletion cut. The pinned ManagedLedger path also selects one read source and propagates an Object open/range
failure rather than safely retrying the complete range from the still-authorized BookKeeper source. Fallback that mixes
partial entries from both sources or reads physical BookKeeper residue after native deletion would be weaker than a
single native ledger view.

## Decision

Before changing native metadata to `bookkeeperDeleted=true`, ManagedLedger invokes a bounded
`revalidateOffloadedForSourceDeletion(ledgerId, attemptUuid, persistedDriverMetadata)` operation. It verifies:

- the exact persisted attempt and NPO1 root identity, complete root parse, and self-digest;
- the immutable data version, length, and SHA-256 through a qualified provider proof or bounded GET;
- CLOSED/LAC/entry-count/logical-length equivalence;
- the production reader's first, last, and sparse-boundary entries.

Object I/O never runs while holding the native metadata mutex. After successful revalidation, the native metadata CAS
rechecks the same attempt UUID and `complete && !bookkeeperDeleted` before committing the flag. Timeout, throttling,
missing data, or mismatch leaves the flag false, retains BookKeeper, and retries with per-ledger backoff. Permanent
Object corruption quarantines the attempt and alerts operators. Provider immutable-version/ACL retention remains a
required defense against the residual cross-provider TOCTOU window.

Read source selection is:

- before offload completion: BookKeeper only;
- `complete && !bookkeeperDeleted`: both sources are eligible under the bounded one-shot rules below;
- `bookkeeperDeleted=true`: Object only, even if physical BookKeeper residue still exists. ADR 0052 defines this as the
  compatibility read fence for `BK_DELETE_INTENT` or `BK_DELETE_DONE`; only DONE proves physical absence.

For an Object-first read, missing, timeout, unavailable, short-read, digest, or format failure releases all partial
entries and retries the entire inclusive range from BookKeeper at most once. For a BookKeeper-first read, only the
native `BKNoSuchLedgerExists` resolution may retry the entire range from Object; ordinary BookKeeper transient errors
use native retry and then propagate. Invalid range, cancellation, closed-handle, and unsupported-operation errors never
fall back.

One requested range is returned wholly from one source. Fallback never loops. If both sources fail, the primary error is
returned and the secondary is attached as a suppressed cause. Object integrity failure remains degraded/quarantined and
vetoes source deletion even when BookKeeper fallback succeeds.

## Consequences

- `V2-OPEN-BK-07` and `V2-OPEN-BK-08` are resolved.
- Final source deletion pays another bounded Object verification, while reads gain one native-authorized availability
  fallback without hiding corruption.
- ManagedLedger-owned read pins and composite-handle lifecycle are refined by ADR 0045. ADR 0052 owns physical-delete
  intent/fact, retention policy, and restart reconciliation.
- M2 must prove the exact state/error table, whole-range source purity, partial-entry release, no fallback loops,
  quarantine/deletion veto, CAS recheck cuts, timeout retention, and deletion-versus-read concurrency.

This decision is refined by ADRs 0044/0045/0052, refines ADRs 0017/0020/0029/0035, and is tracked by `T-BK-01`,
`V2-BK-002`, and `V2-BK-005..011`.
