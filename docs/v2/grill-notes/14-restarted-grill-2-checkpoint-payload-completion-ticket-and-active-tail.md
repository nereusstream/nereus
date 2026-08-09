---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 12: checkpoint payload, completion ticket, and active-tail readability

Date: 2026-08-09

Round 11 fixed the permanent class/lane catalog and complete leaf grammar, provider-resolved checkpoint eligibility,
one publisher-epoch-fenced combiner, and the separation between physical lane resolution and binding Durable Frontier.
It also fixed a lazy reconstructible tracker shape rather than a persistent per-Topic TreeMap. The next independent
frontier is the minimal data carried by the physical checkpoint, the normal-path ring index, and the read-publication
cut required before B may ACK. No recommendation below is normative until explicit confirmation.

## Source facts and corrected constraints

- `laneSequence` is an ADR-0046 HKDF/nonce input. ADR 0062 therefore interprets the confirmed pre-allocation “seal” as
  immutable group membership/policy plan seal; sequence must precede final encryption/ciphertext-body seal.
- ADR 0063 makes checkpoint physical inventory. Copying `BindingDurableFrontier` into its page or Seal as authority
  would reunify the two states that Round 11 separated. Omitting every binding summary costs bounded header/directory
  prefix GETs during recovery but adds no new trust path.
- ADR 0064 accepts a normal-path ring/window but no durable `appendOrdinal` exists in the current V2 schema. A ring may
  use an owner-local ticket because serial allocation preserves local order, but recovery must use typed Position Domain
  adjacency from durable descriptors.
- The append contract requires acknowledged coverage to be both durable and readable. A provider-resolved B cannot ACK
  merely because its Object can enter checkpoint: before manifest materialization, an owner-local active-tail view must
  locate B's immutable frame without making checkpoint or a new per-group metadata row the read authority.
- NPD1 numeric caps, NWG1 prefix/row caps and target values, and allocator mode/wire remain evidence-blocked and are not
  part of this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-20` |
| Q2 | `V2-OPEN-OBJ-21` |
| Q3 | `V2-OPEN-READ-01` |

❓ **Q1** - **Physical checkpoint and Seal payload**: Should a checkpoint duplicate append-unit coverage/frontiers so
recovery can avoid directory reads, carry them only as hints, or remain a strictly physical extent inventory?

➡️ Recommend a physical-only 0.2 descriptor:

```text
ProviderResolvedExtentDescriptorV1 {
  walRunRootSha
  laneId
  laneSequence
  directoryPrefixEnd
  bodyLength
  objectSha256
  optionalProviderVersionAndQualifiedProof
}
```

The page adds its ordinal, predecessor SHA, body SHA, publisher epoch, and `LaneExtentResolvedThrough` vector. It stores
no copied `BindingDurableFrontier`, ACK bitmap, waiting-gap state, or per-binding coverage rows. The final Seal binds
Root, terminal physical vector, final page head/SHA, and aggregate extent/body counts needed for validation; it likewise
stores no binding frontier. Recovery uses each descriptor's bounded prefix GET to authenticate the in-body directory
and rebuild binding units. A qualified provider version/proof may avoid weaker provider verification but cannot replace
the leaf-derived expected identity.

This spends bounded recovery GETs to keep one authority, smaller checkpoint rows, and no stale frontier snapshot.
Only benchmark evidence that prefix verification misses the recovery SLO should reopen an optional non-authoritative
binding summary; it must not be added pre-emptively.

❓ **Q2** - **Owner-local completion ticket and ring indexing**: How should the O(1) normal-path ring identify order
without persisting a new append ordinal or treating a local integer as protocol truth?

➡️ Recommend an owner-local checked 64-bit `CompletionTicket`, allocated only after the binding has passed tracker
capacity admission and the Position Domain has allocated exact coverage. The serialized binding writer makes ticket
order match allocation order, but every slot also stores exact coverage and expected predecessor; release still
validates Position Domain adjacency.

The ticket exists only in the owner instance and waiting future. It is not encoded in NWG1, checkpoint, manifest,
idempotency identity, or a metadata key. A bounded ring slot carries ticket generation, coverage, expected predecessor,
completion state, descriptor reference, and optional future; after provider resolution it carries no payload. The owner
must backpressure before overwriting a live slot or numeric wrap. Takeover discards tickets and reconstructs with the
accepted bounded Position-Domain-aware ordered structure.

The tradeoff is two implementations—O(1) ring normally and ordered recovery fallback—but no durable sixth ordering
domain and no permanent TreeMap per Topic.

❓ **Q3** - **Active-tail read publication before ACK**: What makes B readable after independent frontier advancement
when its Object is not yet in a manifest and checkpoint is only physical inventory?

➡️ Recommend one owner-local derived `BindingActiveTailIndex` per active binding, instantiated lazily and storing only
compact authenticated frame/commit-set locator references. For each provider-resolved Object:

1. validate the shared Object/header/directory and B's complete unit;
2. install B's immutable read locator in the active-tail index under its typed coverage;
3. complete B's contiguous tracker and advance `BindingDurableFrontier` plus `ReadableFrontier` together for that
   coverage;
4. only then complete B's protocol ACK.

A can remain pending from the same Object. Index installation failure or local memory pressure backpressures that
binding before further position allocation and cannot produce an ACK. The index stores no payload and is not remote
authority. On owner open/takeover, bounded Root + checkpoint + LIST recovery reconstructs/validates it before append or
active-tail read admission; normal reads then use the local locator without a metadata call. Entries retire only after
the manifest-selected generation covers the same typed range and source protection/read pins permit removal.

This adds a compact per-binding active-tail index and takeover rebuild cost, but it satisfies the existing
durable-and-readable ACK contract without per-group metadata publication or making checkpoint a logical read view.

## Deferred descendants

- Q1 must settle before final checkpoint/Seal field IDs and page-size arithmetic freeze.
- Q2 must settle before completion APIs, ring wrap tests, and exact tracker numeric budgets freeze.
- Q3 must settle before the M3 active-tail reader/index API, ACK completion order, and takeover-open gate freeze.
- Exact numeric tracker/index/page budgets are evidence/admission outputs after the structures are selected.
- `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19`, and
  `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 12 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-blocked values/modes remain in the open log.
