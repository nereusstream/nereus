---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeDesignIndex
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m4/final/canonical/m4-final.json
---

# M4 detailed-design index

M4 closes the stable Binding read view, deterministic source plan, complete asynchronous source lifetime, durable
read-quiescence proof, and exact protection-generation release. It starts from the accepted read contracts in ADRs
0069 through 0080 and the protocol-specific M2/M3 authorities. It does not introduce a global metadata snapshot, a
per-read durable ticket, a second physical-object pin authority, or physical source deletion.

## Current design state

- [M4-A read-view authority and batch lifetime](m4-a-read-view-authority.md) is accepted design input from Grill 32.
- [M4-B typed source plan and fallback](m4-b-source-plan-and-fallback.md) is accepted design input from Grill 33.
- [M4-C hazard-slot and reclamation races](m4-c-hazard-slot-reclamation.md) is accepted design input from Grill 34.
- [M4-D evidence ownership and freeze](m4-d-evidence-ownership-and-freeze.md) is accepted design input from Grill 35.
- The governance-only [freeze manifest](m4-design-freeze.json) binds the eight immutable design/review records.
- Implementation and formal evidence are complete at exact tested source
  `0a855a7354631f8eb42adac0d93eacb9ac591027`; the canonical immutable
  [M4 Final](../../evidence/v2-m4/final/canonical/m4-final.json) has SHA-256
  `ee95aa54a43847044e5bdd466edd3d3ac507494028967f529917265bcf4909fe`.

The design remains hard-frozen and unchanged. `v2M4DesignCheck` records the clean pre-implementation freeze;
`v2M4Check` is the post-publication authority. M5 physical deletion, M6 process activation, M8 native parity, and
production deployment authority remain excluded.

## Goal

For each bounded Binding-scoped protocol read batch, M4 must:

1. acquire one allocation-free coherent logical `BindingReadViewSnapshot` through the accepted generation-tagged
   hazard protocol;
2. derive one deterministic, position-ordered source plan from that capture without remote control-metadata I/O;
3. preserve atomic append-unit and declared whole-range source purity while allowing captured disjoint ranges to use
   different sources;
4. retain its exact hazard lease through provider retry/fallback, decode, and the final source-backed-buffer use; and
5. publish/verify terminal, proof-window/fold, capability, and per-source interval facts, then execute/reconcile the
   exact protection-generation release CAS without performing or claiming physical deletion.

## Design tree

```text
M4 stable read path
├── A. Binding read-view authority and lifetime          ACCEPTED INPUT (Grill 32)
├── B. protocol/profile source plan and fallback matrix ACCEPTED INPUT (Grill 33)
├── C. hazard-slot and reclamation race closure         ACCEPTED INPUT (Grill 34)
└── D. M4/M5 evidence and frozen-dependency boundary    ACCEPTED INPUT (Grill 35)
    └── hard-frozen M4 design                           READY / DESIGN-ONLY GATED
```

A bound design change requires explicit reviewed amendment and freeze-manifest update. A tentative physical
representation or numeric value remains an OPEN evidence item and is never promoted merely because it appeared in a
grill response.

## Frozen M3 dependency

M4 depends on, but does not regenerate or reinterpret, the following immutable M3 closure:

| Item | Frozen value |
| --- | --- |
| M3 tested source | `e5e53e62865c21845621037bea5f18c092bd4259` |
| M3 Final | `docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json` |
| M3 Final SHA-256 | `81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a` |
| Final-bound source-lock SHA-256 | `2a46f31c90912f3f3f10d2365b9f9ffcc8070a847c4c7e202177ea903cdc240b` |
| Allocator | `RANGE` / `RANGE_64` |
| Closure/navigation commit | `efab430aed37b3f7c32d09b88ae935c1aea1c902` |
| Closure | eleven child receipts, 26 scenarios, exclusions `M6_PROCESS_ACTIVATION` and `M8_NATIVE_PARITY` |

The historical path `docs/v2/evidence/v2-m3/final/m3-final.json` is not an alias for the e5 Final. Normal M4 work does
not broaden the M3 evidence-only descendant allowlist, rewrite the M3 tested source, or rerun the allocator campaign.
`v2M4HistoricalM3DependencyCheck` validates the exact immutable history without claiming current HEAD was M3-tested.

## Scope

M4 owns:

- generation-tagged coherent Binding read capture and the protocol-specific immutable references it binds;
- bounded cross-Binding hazard-slot admission, ABA-safe lease ownership, and complete terminal source drain;
- exact source-range candidate validation and deterministic per-range route/fallback planning;
- read-side corruption/quarantine signals and fail-safe dual-source exhaustion behavior;
- fused selector/anchor/terminal/proof-window/fold/capability writers and closed verifiers;
- exact per-source `[first_i,sharedLast]` release verification, protection-generation release CAS, and response-loss
  reconciliation;
- the physical proof-window/head/fold representation and measurements now selected by the M4 Final;
- the capability/receipt encodings and backend admissions now selected by the M4 Final; and
- the four future semantic evidence children and exact scenario/predicate ownership frozen in M4-D.

M4 does not own:

- changes to M3 allocator protocol, source locks, workload, qualification, RANGE size, harness, campaign, or Final;
- a global `readVersion`, deep-copy snapshot, synchronous metadata read, or durable ACK per read;
- a per-physical-Object `readingSlot` beside the accepted Binding/generation hazard slot;
- a proof-window lookup on the ordinary read hot path;
- M5 `FULL_V1 -> RETIRED_V1` compaction, final provider/source revalidation, GC executor, orphan scan, or physical
  deletion;
- M6 broker/controller process activation; or
- M8 native parity and scale claims.

## Vocabulary

`ReadSnapshot` is accepted only as protocol-local implementation vocabulary when it denotes immutable shared
references. The cross-protocol contract is `BindingReadViewSnapshot`. A singular/global `readVersion`, a
`min-read-snapshot ACK`, and a per-Object `readingSlot` are rejected. The accepted runtime terms are source generation
`G`, one generation-tagged coherent publication cell, one batch-owned hazard slot, and one ABA-safe `SlotLeaseWord`.

## Design-close conditions

The freeze commit must prove:

1. Grills 32 through 35 have complete preserved review responses and synchronized normative descendants;
2. the source/fallback matrix is deterministic for every admitted protocol/profile state and returns no silent data;
3. the race matrix proves no physical source can be deleted while an accepted read may still use it, including
   quarantine and process-loss boundaries;
4. M4/M5 scenario predicates and receipt ownership are exact and non-overlapping;
5. before formal evidence, `V2-OPEN-READ-08/09/15` remain OPEN wherever evidence is absent rather than being closed by prose;
6. the separate M4 historical-M3 dependency contract is exact and does not weaken `v2M3Check`; and
7. `v2DocumentationCheck`, `v2M4HistoricalM3DependencyCheck`, its contract tests, and `v2M4DesignCheck` pass on a clean
   commit pushed to `origin/main` with local/remote equality.

The design-only result is exactly `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`. Hard freeze authorizes implementation to
start. It is not an implementation, receipt, scenario PASS, M4 Final, protection-release execution, M5 physical-GC
authority, M6 activation, or M8 parity result.
