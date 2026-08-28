# ADR 0117: V2 M3 allocator V3 RANGE completion reservation-handoff amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADR 0113 for V3 RANGE completion handoff and ADRs 0115/0116 for the current-source diagnostic inventory
- Preserves: production allocator CAS/reread authority, V1/V2 behavior, all V3 campaign/checkpoint/evaluation/selection
  wire bytes, workload and rate identities, plan and action budgets, thresholds, SLOs, dispositions, and selection rules

## Context

The exact-clean `b9659232500f279fcffd4cea24989c91466cd1c2` formal attempt passed the complete current-source
NADV3 and V3 pre-campaign gates, executed all eight Native baseline rows, consumed the failed STRICT fixed and derived
rows, and constructed the RANGE-16 10k population. Its first RANGE-16 fixed-1000 interval completed all 30,000
measured requests with zero measured failure, timeout, or pre-admission drop, but 26 warm-up completions failed in the
harness callback with `IllegalStateException:allocator V3 completed workflow retained a Cell reservation`. The
campaign stopped fail-closed as `INFRASTRUCTURE_FAILED`, created neither evaluation nor selection, and did not run a
fault action.

The attempt contains 12 action attachments, 12 checkpoints, 25 regular files, and 48,240 bytes. Its campaign-result
SHA-256 is `4713350df1e2629dead64221edce745da451c1289e37cd6c2ed8c40834057eef`, final checkpoint SHA-256
is `aa5b4fefefcfd6aa51bba92ee860e747f05821c170c429719e59d597fe31b047`, and formal JUnit SHA-256 is
`b99b7b689c84e10fdcd74a11fc362cffcbdc8c7efee6b1e5bd022baa58c25f79`. The entire attempt and JUnit are
byte-identical in the read-only archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-b9659232-r1-infrastructure-failed-completed-cell-reservation`.
Its payload manifest SHA-256 is `cc3cdabe816c1aaf1f3915b5bb7a0ffca0169371fb173b46ec73ca5ea2a07a15` and
archive-identity SHA-256 is `19a936c24c912898d6c458fc10094a54e98cbdc0f3c9e3ddd9fd5fe400f3446d`. It is immutable,
non-promotable, and not future campaign input.

ADR 0113 assumed that every successful `BoundedVirtualLedgerAllocatorWorkflowV2.Result.exactCell` was a
reservation-free terminal snapshot. That is true for STRICT, but it is not guaranteed for concurrent RANGE. A RANGE
request can publish its exact candidate from an already installed grant while a different request has reserved the
next grant. If a same-key Cell refresh is required during create/publish reconciliation, the successful first request
can return that other request's exact reserved snapshot. The reservation owner must still clear it before that owner
can complete, but completion callbacks may arrive in any order. Treating the first request's exact intermediate
snapshot as its own uncleared reservation therefore converted a legal successful workflow into an infrastructure
failure.

## Decision

The harness-local population Cell proof remains reservation-free. A successful V3 completion first verifies that its
returned Cell has the same mode, protocol version, namespace, slice assignment, and exact slice geometry. If the
returned snapshot contains a reservation, the callback does not adopt it: it retains the current reservation-free
population proof and continues the exact Head handoff. A reservation-free completion remains eligible for ADR 0113's
monotonic cursor/grant merge. The merge rejects a reserved current population proof, partial cursor/grant ordering,
slice drift, and a lost exact Head predecessor.

This is not a relaxation of allocator correctness. The returned reserved snapshot is still an exact Oxia fact and is
not rewritten or cleared locally. The request that owns the reservation can complete only through the production
workflow's exact install/clear/reconcile chain. A successful, fully drained interval therefore eventually observes a
reservation-free completion for its last reserved grant; an interval with failure, timeout, or unresolved lifecycle
stops before any following fault/scale action. Normal allocation correctness remains exclusively in the production
CAS, same-key reread, exact-head, and bounded-retry protocol. No shared Cell Java lock or extra production metadata
operation is introduced.

The current-source real-Oxia diagnostic inventory adds one exact RANGE-16 sequence test. It performs the formal
scale-equivalent 10k population construction and the exact 1 ms fixed-1000 schedule with 10-second warm-up,
30-second measurement, and 5-second cleanup. It requires zero unexpected warm-up failure/timeout, 30,000 measured
completions, zero measured failure/timeout, complete drain, and global concurrency above four. Its output is
`diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`. NADV3 therefore binds six suites and 18 exact
testcase identities for this and later sources; older NADV3 bytes and parsers remain compatible.

The workload schedule digest remains
`b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798`, the Native execution profile remains
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`, and the zero-decision plan remains
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`. A new formal attempt still requires a new
exact clean pushed source, freshly sealed current-source NADV3, complete preflight, and a new immutable formal
directory.

## Consequences

- Concurrent RANGE completion order cannot turn another request's exact transient reservation into a false terminal
  callback failure.
- The population's later fault/scale proof remains reservation-free, monotonic, and fail-closed.
- A formal-equivalent RANGE-16 sequence is now part of the exact NADV3 inventory instead of relying only on a short
  64-request handoff diagnostic.
- The `b9659232...-r1` attempt remains invalid infrastructure evidence and cannot be resumed, resealed, selected, or
  promoted.
- No allocator rate, retry bound, candidate, threshold, SLO, disposition, evidence validator, or selection preference
  changes.
