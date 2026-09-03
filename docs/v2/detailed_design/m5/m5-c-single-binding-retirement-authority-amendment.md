---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesignAmendment
sourceTuple: v2-m1
---

# M5-C amendment: single-Binding retirement authority

## Authority and scope

This document is the accepted additive amendment to the immutable M5 hard-freeze at
`c86fde3ed6f4319642987fd599022bd32e2cca5e`. It is authorized by
[ADR 0146](../../../decisions/0146-v2-m5-single-binding-retirement-authority-amendment.md) and supersedes exactly:

- M5-C's separate batch-key externalization and required atomic multi-key transaction;
- M5-I0's placement of batch externalization in the Oxia adapter; and
- M5-E's requirement that `RETENTION_METADATA_RETIREMENT` use a real Oxia conditional multi-key transaction.

All other I0/A-E identities, vetoes, ordering, evidence ownership, scenario state, exclusions, and the historical M4
dependency remain exact. This amendment creates no runtime PASS, child receipt, scenario promotion, physical-delete
authority, M6/M7/M8 authority, or production authority.

## Rejected source capability

The source-locked Oxia Java client `0.9.4` at
`091a42c2780d92da56e9ec1f02ce1c3d988adc16` exposes exact single-key operations but no public conditional multi-key
transaction. The server base `37a17bef17202d5fd6e23282da5fd26d94865484` commits an internal RocksDB batch while
reporting an expected-version failure per operation and continuing the remaining puts. That batch cannot be promoted
to all-conditions-or-zero-mutations authority. Extending either source-locked project and sequentially mutating a
selector key plus a batch key are rejected.

## One physical authority cell

For Object-WAL, the physical key remains the current M4 Binding selector key:

```text
.../read-m4/<BindingId>/selector
```

Its post-amendment canonical value is `BindingRetirementAuthorityV1`:

```text
BindingRetirementAuthorityV1
├── schema / authorityGeneration / predecessorValueSha256 / canonicalSha256
├── exact Binding + capability generation/digest
├── selectorCore + pendingAnchors
├── batchSlots[BatchId]
│   ├── FULL_V1(canonical complete M4 SourceRetirementBatch)
│   └── RETIRED_V1(permanent compact tombstone)
├── optional REFERENCE_SCAN_FENCED_V1(target BatchId, attemptId, exact predecessor)
├── referenceMutationTickets[target, kind, writer, operationId]
└── exact bounded count/byte summaries
```

The M4 reader projection is deterministic: `BindingReadSelector.activeBatches` is exactly the sorted set of
`FULL_V1` slot payloads. `RETIRED_V1` slots, tickets, and a scan fence are never projected as read sources. All
selector mode/view/generation/admission/anchor meanings remain unchanged.

The legacy selector wire value is a migration predecessor only. One exact selector-key CAS wraps the unchanged
selector core and maps every canonical inline batch to a byte-identical `FULL_V1` slot. Absence, truncation,
reordering, duplicate BatchId, or a digest mismatch quarantines the migration. Once wrapped, all M4 control writers
must preserve the complete envelope and exact-CAS it; no writer may emit the legacy value again. A scan fence blocks
all selector/control mutations until retirement or exact abort, while ordinary reads continue from the projection.

## Ticketed mutation rule

The closed `ReferenceKindV1` and retention-floor inventories remain unchanged. Every writer that can change any
proof-bound authority fact for a retirement target must participate in the following target-specific protocol:

```text
NO_TICKET
  -- exact authority-key CAS, target OPEN -->
PENDING_REFERENCE_MUTATION_V1
  -- external mutation + authoritative exact reread -->
NO_TICKET
```

The ticket binds Binding, target kind/identity, reference kind, writer capability, deterministic operation identity,
external key set/root, and exact authority predecessor. The external mutation is not dispatched before the ticket is
visible. Response unknown, failed reread, partial pagination, mismatched successor, or an unenrolled writer retains
the ticket and vetoes the target. Clearing from timeout, cache, local completion, or intent alone is forbidden.

Acquiring a ticket for a `REFERENCE_SCAN_FENCED_V1` or `RETIRED_V1` target fails closed. The coordinator may install a
scan fence only from an exact open predecessor with zero target tickets. Therefore the two possible orders are:

- ticket CAS first: fencing fails until the external mutation is authoritatively reconciled and the ticket is
  removed; or
- fence CAS first: ticket acquisition fails and no proof-bound external mutation may start.

This is the required concurrency proof. Capability admission must show that every writer class in the closed
inventory enforces it before retirement can be enabled. Missing integration, a legacy writer, or an unknown record
kind returns `UNSUPPORTED` or `RETAIN`; it never narrows the inventory.

## Exact single-key retirement protocol

An Object-WAL batch advances only through:

```text
OPEN_V1 / FULL_V1
  -- exact selector-authority CAS -->
REFERENCE_SCAN_FENCED_V1 / FULL_V1
  -- complete stable scan and exact version-vector reread -->
REFERENCE_SCAN_FENCED_V1 / RETIRED_V1
  -- represented by one exact selector-authority CAS -- terminal
```

Fence installation additionally requires the exact canonical batch identity and lineage, every exact member
protection in matching M4 `RELEASED` state, admitted current capability, and all hard limits. The stable scan must
prove every frozen M5-C floor/reference/audit predicate. All proof-bound mutations require tickets, and the fence
accepts none, so the version vector cannot change before the retirement CAS. A mismatch before fencing or during the
scan aborts the attempt and requires a fresh proof.

The final candidate replaces exactly one BatchId slot with
`RetiredSourceRetirementBatchTombstoneV1`, binds the fenced predecessor version/value SHA-256 and reference-free proof
SHA-256, preserves every unrelated selector field/slot/ticket summary, and consumes one authority generation. The
single exact CAS at the existing selector key is the only linearization point. The tombstone is permanent, rejects
future tickets for its BatchId, grants no M4 release, and grants no physical-delete authority.

Response-loss reconciliation reads only the same authority key:

| Exact reread | Result |
| --- | --- |
| matching `RETIRED_V1` slot and preserved unrelated authority | `APPLIED_EXACT` or `EXISTING_EXACT` |
| exact fenced `FULL_V1` predecessor | `DEFINITIVELY_NOT_APPLIED`; retry the same CAS |
| exact open `FULL_V1` after an explicit abort | attempt aborted; build a new fence/proof |
| absent cell/slot, reconstructed `FULL_V1`, mismatched identity/digest/generation, or altered unrelated authority | `CONFLICT` or `QUARANTINED` |

No selector/batch-key split-state matrix remains because no second batch key exists. `FULL_V1 -> RETIRED_V1` means
irreversible replacement of the BatchId-addressed slot in the one physical Binding authority cell. The slot cannot
move, be reused, be removed, or return to full state.

## Pulsar aggregate retirement

Pulsar retains its distinct codec and its existing incarnation-scoped aggregate key as the one physical authority
cell. Its name-scoped generation selector must first be the accepted permanent exact `DELETED(generation)` value.
All writers that can change an aggregate reference must ticket through the aggregate authority cell; the same
open-to-fenced ordering makes the proof vector stable. Only after every M5-D physical cleanup row is `DELETE_DONE` or
authoritatively absent may one exact aggregate-key CAS replace the full aggregate with its permanent incarnation
tombstone. Object-WAL batch slots and Pulsar aggregates never share a codec or authority key.

## Caps, recovery, and evidence delta

The original 19 closed retention/admission limit kinds remain required. `EXTERNALIZATION_UNKNOWNS` is retained as a
historical metric code but counts unknown legacy migration or authority-envelope transition outcomes; it cannot
authorize a separate batch key. The implementation projection additionally fixes maximum authority bytes, full and
retired slot counts/bytes, tickets, ticket bytes, one concurrent scan fence, and bounded scan duration. Cap exhaustion
stops new fallback/handoff/reference admission and retains existing data.

The amended `RETENTION_METADATA_RETIREMENT` child must use real Oxia 0.9.4 exact single-key CAS and prove:

- byte-exact legacy-selector migration and M4 read/control projection compatibility;
- both ticket/fence race orders for every closed reference kind and every proof-bound fence class;
- ticket response loss, crash recovery, and refusal to clear ambiguous external mutations;
- stable paginated proof vectors under the fence and veto of any unenrolled writer/capability;
- one-slot retirement while unrelated selector fields and sibling full/tombstone slots remain byte-exact;
- same-key lost response, stale predecessor, retry, restart, delayed legacy create, cap exhaustion, and permanent
  tombstone behavior;
- Pulsar `DELETED(generation)` ABA protection and post-M5-D aggregate replacement; and
- negative proofs that neither tombstone family releases M4 protection or authorizes physical deletion.

The earlier multi-key core tests and capability projection remain source-bound rejected evidence only. The future
M5-C gate must not call them implementation-complete or silently reinterpret the internal Oxia write batch.

No blocking design question remains for this amendment.
