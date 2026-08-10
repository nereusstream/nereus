---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 19: evidence frontier

Date: 2026-08-10

Round 18 resolves the remaining decision-only 0.2 read-retirement frontier at M0. ADR 0079 freezes the logical bounded
inline closure-anchor set, dedicated STOPPED envelope, closed-verifier terminal safety, and asynchronous prune. ADR
0080 freezes permanent same-key `FULL_V1 -> RETIRED_V1` metadata compaction while explicitly rejecting tombstone
deletion as an unevidenced 0.2 authority.

## Current frontier

There is no user-decision question whose prerequisites are complete in this branch. The remaining descendants require
runtime/provider/backend facts and must not be answered by another prose-only round:

| Gate | Evidence required before another design choice |
| --- | --- |
| `V2-OPEN-READ-08` | M4 proof-window/head/fold representation, terminal-row retirement, response-loss, capacity, and throughput evidence |
| `V2-OPEN-READ-09` | M4/M5 canonical capability/receipt encoding, verifier lifetime/revocation, and admitted backend-generation evidence |
| `V2-OPEN-READ-15` | M4/M5 tombstone generation/lifetime capacity plus a concrete backend's gap-free activation history, monotonic conditional authority, lineage binding, stale-create, and recovery evidence |

Other active 0.2 gates—`V2-OPEN-BK-11/13`, `V2-OPEN-OBJ-17/19/22/24`, `V2-OPEN-PUL-OBJ-09`,
`V2-OPEN-BK-02`, and `V2-OPEN-BENCH-01`—are likewise evidence gates already assigned to their implementation
milestones. Deferred migration/projection branches remain outside the 0.2 runtime, and KoP remains documented but
deferred.

## Grill status

The pure-document M0 decision frontier is exhausted. Restart the grill only when one evidence gate produces its exact-
source receipt or when the 0.2 scope changes. Until then:

- the selector remains bounded inline; no anchor page/index/chain is accepted;
- terminal rows retire only through the future evidenced proof/fold authority, not a new progress machine;
- compact batch tombstones remain permanent under hard lifetime budgets; and
- no `BatchMetadataRetirementAuthority`, tombstone deletion, absence inference, or Topic switch is accepted.

This note records a blocked evidence frontier, not a normative design decision or a PASS claim.
