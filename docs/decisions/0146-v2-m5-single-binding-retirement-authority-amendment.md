# ADR 0146: V2 M5 single-Binding retirement authority amendment

## Status

Accepted on 2026-09-03 for the M5-C metadata-retirement implementation. This decision amends only the physical
authority placement and conditional-transition mechanism frozen by M5-C and its M5-E evidence row. It does not alter
M4 release eligibility, the closed reference inventory, permanent-tombstone semantics, M5-D physical-delete
prerequisites, scenario ownership, or any M6/M7/M8 or production authority.

## Context

The original M5-C freeze required one all-conditions-or-zero-mutations Oxia transaction to remove an inline M4
`SourceRetirementBatch`, create a separate deterministic `FULL_V1` value, and verify the proof version vector.
Source revalidation showed that the locked Oxia Java 0.9.4 client exposes only single-key conditional operations. Its
package-private write batch is transport batching, and the locked server continues processing later operations after
an individual expected-version failure. It is therefore not the transaction required by the freeze.

A sequential selector CAS followed by a batch-key CAS is unsafe because it creates two linearization points and an
observable split state. Merely copying a proof into another key has the same defect while M4 readers continue to take
the selector key as authority. Extending the Oxia client/server is outside the selected amendment.

## Decision

For an Object-WAL Binding, the existing M4 selector key is the one physical retirement-authority cell. Its canonical
value becomes `BindingRetirementAuthorityV1`, containing:

- the exact M4 `BindingReadSelector` projection;
- bounded BatchId-addressed slots in state `FULL_V1` or permanent `RETIRED_V1`;
- at most one target-specific `REFERENCE_SCAN_FENCED_V1` state;
- bounded reference-mutation tickets for every writer that can change a proof-bound authority fact; and
- the exact capability generation/digest, authority generation, predecessor digest, count/byte summaries, and
  canonical value digest.

The selector projection contains only the full batch slots as `activeBatches`. Tombstones and tickets never become
read sources. A legacy selector value is accepted only as an exact migration predecessor. One exact CAS at the same
selector key converts every inline batch to a byte-identical `FULL_V1` slot and preserves the complete selector
projection. After that migration, emitting a legacy selector value is forbidden. Every M4 selector mutation must
read, preserve, and exact-CAS the complete authority value; while a reference scan is fenced, Binding-control
mutations are rejected or retried rather than bypassing the fence.

ADR 0080's same-key rule is refined as follows: the irreversible storage lifecycle belongs to the BatchId-addressed
slot inside this single Binding authority cell. A successful exact CAS replaces that one `FULL_V1` slot with the
matching `RETIRED_V1` slot while preserving every unrelated selector field and slot. A BatchId slot is never reused,
deleted, moved to a second key, or reconstructed after retirement.

### Reference-mutation serialization

An absence scan is safe only if a concurrent writer cannot add or change a relevant fact between the scan and the
retirement CAS. Therefore every writer capable of changing any key/version/value named by the closed reference or
fence inventory must use this protocol for the exact target:

1. exact-CAS a deterministic `PENDING_REFERENCE_MUTATION_V1` ticket into the Binding authority cell before dispatch;
2. mutate or reconcile the external authority fact;
3. authoritatively reread the exact successor or a definitive non-application result; and
4. exact-CAS the ticket out of the authority cell.

Unknown dispatch or reconciliation retains the ticket. A ticket is never cleared from a timeout, local intent,
cache, or presumed absence. A writer whose capability generation does not advertise this protocol cannot acquire a
ticket, and retirement remains disabled until every writer class in the closed inventory is enrolled. A retired
BatchId permanently rejects new tickets.

The retirement coordinator may exact-CAS `OPEN_V1/FULL_V1` to
`REFERENCE_SCAN_FENCED_V1/FULL_V1` only when no ticket exists for the target, every exact M4 member protection is the
matching `RELEASED` value, the authority capability is admitted, and all hard limits permit the attempt. The fence
blocks new tickets and all proof-bound Binding-control mutations. Existing reads continue.

After the fence is visible, the coordinator scans and rereads the complete version vector. Because no proof-bound
writer can mutate without a ticket and the fenced authority accepts no ticket, the vector is stable until the next
authority-cell CAS. The coordinator then performs one exact single-key CAS from the fenced predecessor to the
matching permanent tombstone slot. This CAS is the sole retirement linearization point. A veto or failed scan may
exact-CAS back to `OPEN_V1/FULL_V1`; response loss is reconciled from the same key. A matching tombstone means applied,
the exact fenced predecessor means definitively not applied, and any other state is conflict or quarantine. There is
no cross-key success matrix and no sequential-CAS fallback.

The Pulsar aggregate family follows the same serialization rule at its existing incarnation-scoped aggregate key.
Its name-scoped selector must already be the accepted permanent `DELETED(generation)` fact; writers that can change a
proof-bound aggregate reference use tickets in the aggregate authority value. The exact aggregate-key CAS to its
permanent incarnation tombstone remains the sole Pulsar retirement linearization point and still occurs only after
M5-D physical cleanup is complete.

## Consequences

- Oxia 0.9.4 exact single-key compare-and-set is sufficient; its internal write batch grants no authority.
- Selector projection, full batch, proof fence, reference-mutation exclusion, and retirement state share one
  linearizable Binding cell, so selector/batch split states are unrepresentable.
- Retention may pause Binding-control mutations during a bounded scan. A crash leaves a durable fence or ticket that
  conservatively retains metadata until exact reconciliation.
- The authority value now carries lifetime tombstone bytes and bounded tickets. Existing hard count/byte admission
  limits remain mandatory and stop new fallback/handoff admission before the metadata value cap is reached.
- M4 selector codec/store/coordinator compatibility is affected and must be freshly certified at the eventual M5
  tested source. Historical M4 evidence is neither rewritten nor reused as current-source proof.
- The earlier multi-key implementation and its `UNSUPPORTED` Oxia projection remain valid rejected evidence. They do
  not satisfy the amended M5-C retirement gate.

This decision is implemented by the accepted
[`M5-C single-Binding retirement authority amendment`](../v2/detailed_design/m5/m5-c-single-binding-retirement-authority-amendment.md)
and supersedes only the conflicting multi-key externalization clauses of the original M5-C freeze and M5-E child
boundary.
