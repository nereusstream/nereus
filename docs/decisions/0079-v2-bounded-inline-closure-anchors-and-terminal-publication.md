# ADR 0079: V2 bounded inline closure anchors and terminal publication

## Status

Accepted for the 0.2 `OBJECT_WAL` logical selector-owned unresolved-anchor set, small bounded inline canonical
representation, dedicated emergency `STOPPED` envelope, closed-verifier terminal safety, reconciler fencing, different-
valid terminal convergence, asynchronous batched pruning, and terminal-retirement boundary. Exact binary encoding and
evidence-selected count/byte/age limits remain M4/M5 work; implementation has not started at M0.

## Context

ADR 0077 requires every fallback-relevant closed Read Admission Epoch to retain a durable closure anchor until its
terminal cut exists. Repeated takeovers can therefore leave several unresolved anchors. The selector authority must
carry that bounded liability without forcing every selector CAS to fetch, deserialize, and revalidate a remote page or
without allowing ordinary admission to consume the capacity needed to stop safely.

Terminal creation also cannot depend on a non-transactional owner check remaining true between the check and the
create. A complete closed-verifiable candidate, rather than the publisher role name alone, must carry correctness.

## Decision

The Binding selector authority logically owns one ordered, bounded set of unresolved fallback-relevant closure
anchors. For 0.2 its physical baseline is one small inline canonical set. No anchor page, secondary index, multilevel
chain, or remote anchor lookup is admitted. A changed selector value is fully checked at its validation/recovery
boundary; a membership-neutral transition may copy already validated canonical bytes and their digest without
reinterpreting every member. The logical authority does not require per-read work or an O(K) remote lookup.

Before any successor may remain `ADMITTING`, admission proves with checked arithmetic:

```text
currentPendingAnchorBytes
  + newClosureAnchorBytes
  + completeEmergencyStoppedEnvelopeBytes
  <= backendHardValueOrTransactionCap
```

The emergency envelope is a dedicated reserve sufficient to close the current E and persist a `STOPPED` successor. A
normal `ADMITTING` transition, prune residue, Topic policy, or dynamic configuration cannot consume it. If normal
capacity is exhausted, the only safe transition closes E and enters `STOPPED`; the system neither drops an anchor nor
continues admission under E. A later `ADMITTING` grant uses a fresh never-reused epoch after the full admission check.

A deterministic create-only terminal candidate binds at least the exact:

```text
closureAnchorSha
closedReadAdmissionEpoch
OwnerEpoch
lastAdmittedAndDrainedReadViewCut
capabilityEvidenceGenerationAndDigest
plannedDrainReceipt OR qualifiedExpiryEvidence
```

Planned-drain and qualified-expiry variants pass the same closed verifier. On an unknown create response, exact bytes
prove the attempted candidate committed; different existing bytes are also adopted as the common terminal SHA when
the closed verifier proves that they are a complete valid variant for the same closure anchor. An invalid occupant is
quarantined and never overwritten.

For a backend transaction that can atomically check current selector authority and create the terminal, that check is
an additional fence. On a non-transactional backend, current owner/reconciler authority governs ACL, rate limiting,
and audit but is not the correctness proof: the candidate's immutable facts and closed verifier are. A Cell reconciler
must itself carry a monotonic reconciler epoch and cannot publish merely because it has a reconciler role name. A stale
owner cannot omit or fabricate drained-view, receipt, capability, or expiry evidence.

Once one terminal cut validates, its anchor becomes prune-eligible. Pruning is asynchronous, may remove several
eligible anchors in one selector CAS, and may piggyback another selector transition. A CAS conflict only requeues the
work; prune never enters ordinary read, append, or ACK cuts and is not required once per terminal. Every successor
either preserves the validated unresolved set or observes an exact validated pruned predecessor.

Terminal rows are not an unbounded second progress system. They may retire only after their truth has entered the
durable proof/window/fold authority selected under `V2-OPEN-READ-08` and no active interval, recovery, response-loss, or
audit reference remains. This ADR adds no terminal bitmap, terminal frontier, page chain, or mutable completion record.

Selector bytes, K, anchor age, terminal rows, emergency-envelope use, `STOPPED` duration, terminal-create rate, prune
conflicts, and response-loss residue have Binding/Cell hard caps and M4 metrics. Topic policy cannot enlarge K, borrow
the emergency reserve, choose an anchor layout, bypass the verifier, or force pruning. Ordinary reads continue to use
the cached selector fence and perform zero remote metadata I/O.

## Consequences

- `V2-OPEN-READ-14` is resolved without freezing a page/index/chain or making current-owner identity the safety proof.
- Stable reads add no metadata access. Control cost is O(K) bounded selector payload/copy at takeover, at most one
  terminal create per relevant epoch, and low-frequency batched prune CAS work.
- M4/M5 must derive a small K and byte cap from takeover p99, backend value/transaction limits, `STOPPED` duration,
  terminal throughput, and prune-conflict evidence; verify the emergency inequality at every admission cut; test
  transactional and non-transactional publisher races, monotonic reconciler epochs, both valid terminal variants,
  response loss, batched prune/piggyback, and zero ordinary-read metadata I/O.

This decision refines ADRs 0071, 0074, 0076, and 0077 and is tracked by `T-MANIFEST-01`, `T-HANDOFF-01`,
`V2-READ-006/009/011/012/014`, and evidence gates `V2-OPEN-READ-08/09`.
