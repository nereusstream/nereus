---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 10: prefix hints, packing lanes, and range takeover

Date: 2026-08-09

Round 9 partially accepted ADRs 0056..0058. Exact NPD1/Object/part values and block-class values now wait for
implementation/provider evidence. The current decision frontier is therefore the three independent designs that do not
depend on those measurements: a routine-read prefix hint, multi-policy scheduling under one WalRun, and failover-safe
range reuse. No recommendation below is normative until explicit confirmation.

## Source facts and corrected constraints

- ADR 0021/0025 now separate whole-Object PUT/durability proof from routine Root-bound directory/frame AEAD. A missing
  qualified `ProviderObjectProof` cannot by itself force every random read onto three GETs or a full GET.
- ADR 0030's current leaf key already carries sequence, exact body length, and complete SHA-256, while normal ACK has no
  per-group metadata commit. A hint available only in an asynchronous checkpoint/manifest cannot cover every open tail.
- ADR 0039/0047 fixes one `CurrentWalRunPointer` and one current Root/lineage per shard. A Topic packing class therefore
  cannot be a singular Root identity or cause one pointer per class.
- ADR 0053 currently defines one contiguous extent-sequence interval per checkpoint page and one terminal inventory in
  the Seal. Concurrent scheduling lanes cannot be added without deciding sequence, page, and Seal semantics together.
- The rejected RANGE proposal reacquired a range after every owner change. With one serialized `RANGE_RESERVED` and
  three 10-ms allocator steps, 10,000 ManagedLedgers have a 300-second idealized lower bound before queueing or errors.
- A range is already numerically never reused. Preserving its grant across owner epochs does not weaken that rule; the
  unresolved issue is which head/node facts prevent a stale owner from publishing a candidate.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-17` |
| Q2 | `V2-OPEN-OBJ-19` |
| Q3 | `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **Self-describing directory-prefix hint**: Where should `directoryEnd` live so routine reads can normally
plan one prefix GET without a provider proof or synchronous per-group metadata row?

➡️ Recommend adding a fixed-width `directoryPrefixEnd19` field to the immutable content-addressed leaf identity. The
exact grammar is finalized together with any lane component, but the hint is derived from the sealed NWG1 body and
co-located with the key's exact body length/SHA-256. The complete leaf is interpreted only under the exact prefix from
the known WalRun Root, and every cached copy binds that Root key/SHA plus the full Object key; a suffix detached from
that Root is not a usable hint. The hint is not cryptographic body proof; only the in-body header / directory AEAD
validates its meaning. A checkpoint, manifest, or in-memory descriptor may repeat it but is never its sole source. The
reader rejects a hint above the wire prefix cap before allocation, optionally pins an available provider version, and
requests `[0,hintEnd)` without requiring `ProviderObjectProof`.

After parsing the returned in-body header: if authoritative `headerEnd <= hintEnd`, reuse the exact subrange and ignore
safe extra bytes; if `headerEnd > hintEnd` but remains within the hard cap, retain the header and fetch only the missing
`[hintEnd,headerEnd)` bytes. Header/Root/key/version mismatch or directory AEAD failure fails or enters the bounded
fallback; the hint never authorizes a frame offset. Duplicate sequence/body identities with conflicting hints remain a
run conflict. The tradeoff is about twenty more key characters and a revised key grammar, in return for a no-metadata
two-GET path for every known extent rather than only asynchronously checkpointed objects.

❓ **Q2** - **Three bounded lanes under one WalRun lineage**: How can latency/cost classes build concurrently without
creating multiple current pointers or reintroducing a global gap/HOL through one extent sequence?

➡️ Recommend one Root/pointer with a format-level hard maximum of three scheduling lanes, but no Root-level packing
class. Each admitted packing class maps to one stable small `laneId`; a group contains only bindings resolved to that
class and records both class and actual close facts in its descriptor/header. Each lane owns one bounded builder,
lane-local `extentSequence`, in-flight/memory limits, and a lane-local contiguous checkpoint-page chain. Leaf identity,
header HKDF/nonce inputs, and descriptors bind `(laneId,laneSequence)` so uniqueness does not depend on cross-lane
completion order.

The Seal binds the exact terminal sequence and final checkpoint-head SHA for every instantiated lane, while the single
successor Root and `CurrentWalRunPointer` remain unchanged. Cell scheduling enforces bounded fairness and aggregate
resource ceilings. A binding may move to a newly resolved packing class only at a group boundary after its previous
lane has no unresolved append for that binding; it need not seal the WalRun. The tradeoff is lane-aware key/page/Seal
wire and up to three builders, in return for no extra lineage, no cost-lane linger blocking a latency lane, and no
global sequence ACK barrier between classes.

❓ **Q3** - **Incarnation-owned range and owner-only takeover**: Which head/node fields let a new broker preserve the
installed range while burning at most one stale candidate and keeping allocator clear off the use path?

➡️ Recommend one exact ManagedLedger head record with separate facts:

```text
ManagedLedgerHead {
  managedLedgerIncarnation
  visibleChainHeadNodeIdAndDigest
  ownerEpoch
  grantId
  rangeStart
  rangeEndExclusive
  nextLedgerId
  allocatorProtocolVersion
}
```

The allocator grant binds the ManagedLedger incarnation/grant/range and never an owner epoch. Takeover CAS changes only
`ownerEpoch` while preserving chain head, grant, range, and cursor. Candidate node identity binds ledger ID, grant ID,
creator owner epoch, and expected visible predecessor digest. Node creation is single-flight per ManagedLedger, and
only an exact head CAS can publish it.

The new owner point-reads/`putIfAbsent`s exact `nextLedgerId`: an already published node is preserved through the head
reread; an exact candidate from the current owner converges by value equality after response loss; an unpublished node
from an old epoch loses publication authority and causes one cursor-only head CAS that burns exactly that ID without
changing the visible chain; absence permits the new candidate. A late old-owner `putIfAbsent` conflict converges
through the same one-ID burn. Any failed head CAS fences that creator.

Once head installation durably contains the grant, range use may begin; allocator `RANGE_RESERVED -> IDLE` clear is a
recoverable background step, though the next grant cannot reserve until exact allocator/head reconciliation clears it.
Whole-tail burn is limited to Topic deletion, ManagedLedger retirement, protocol incompatibility, or unrecoverable head
corruption. Orphan nodes are permanent no-reuse evidence and count against byte/count admission. Capacity uses a
declared finite churn rate and planning horizon plus recovery reserve, continuously reporting remaining, committed,
burned, orphan, and reserved IDs. The tradeoff is one parallel head takeover CAS and at most one point lookup/burn per
uncertain ledger, in return for avoiding serialized range reacquisition across every ledger on a failed broker. This
still does not select RANGE_LEASED over STRICT_SERIALIZED.

## Deferred descendants

- `V2-OPEN-BK-11` NPD1 hard numeric values and `V2-OPEN-BK-13` final block classes wait for ADR 0056/0057
  implementation/provider receipts.
- Q1/Q2 must settle before the revised leaf/header/checkpoint/Seal wire and golden vectors can freeze.
- Q3 must settle before range size, grant/head/node wire IDs, allocator recovery implementation, and mode selection.
- Ledger-chain trimming, cursor/replication/transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 10 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and both
allocator modes remain in the open log.
