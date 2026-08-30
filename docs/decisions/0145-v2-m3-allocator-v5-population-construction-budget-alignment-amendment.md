# ADR 0145: V2 M3 allocator V5 population-construction budget alignment amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADR 0103's harness-only RANGE construction timeout for new exact-source V5 execution
- Preserves: every prior V1/V2/V3/V4/V5 canonical byte and result; the V5 wire, zero-decision plan, execution
  profile, workload, candidates, rates, latency rows, admission caps, action inventory, zero-drop rule, SLOs,
  qualification thresholds, selection preference, production allocator lock, 48,000-second hard deadline, and M6
  activation boundary

## Context

Exact clean source `3b96a2982aa911a1aeb50680fb4398d561d5aa2f` passed the complete 26-test V5 diagnostic,
canonical NADV5, pre-campaign closure, and source closure. Its single formal attempt then reached RANGE-64's
10k-to-100k scale prerequisite. All 90,000 immutable-Cell Head creates completed, followed by 84,118 of the required
89,424 index-ordered initial `reserve -> installRangeReservedGrant -> clearReservation` chains. The harness stopped
at 600 seconds before any 100k interval or fault action could run. The campaign correctly terminated
`INFRASTRUCTURE_FAILED`; it created no evaluation or selection.

The failed source already charges 900 seconds to every initial-population and scale-construction path. ADR 0104's
frozen budget table likewise assigns 900 seconds per path. The implementation nevertheless retained ADR 0103's
older independent 600-second outer timeout. That smaller harness limit is not an evidence qualification threshold:
it cuts off the exact required population transition 300 seconds before the validator-accounted path budget is
exhausted. The observed progress also rules out an unbounded Head phase or a hidden action: the finite serial grant
chain was within 5,306 operations of completion.

The original formal directory remains immutable at
`nereus-metadata-oxia/build/m3-allocator-evidence/bounded-adaptive-formal/3b96a2982aa911a1aeb50680fb4398d561d5aa2f-r1`.
Its 106 regular files total 17,569,678 bytes. Campaign-result SHA-256 is
`9b4190c6c615ffb430e00fbd680de3618aa24ca6aa23314115e749397f2cb4e1`; final NACP5 SHA-256 is
`6342a476ed485a0f786f93da07a012da471489c27844b355a6414ed2b4206039`; formal JUnit SHA-256 is
`da3168fc25f882410abd70f06aaefdfe56d918950fe6091d60968332895818cd`. Its byte-identical external archive is
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/formal-3b96a298-v5-range64-100k-construction-timeout-r1`.
The archive identity SHA-256 is `27b2891691fdba1bf75cc522cdea76b3703f77b459ceff43012892e2c8d891de` and
its manifest SHA-256 is `e54770fe050c21e485cc1707d15078a84d24f3539fe1c764c78dcf5c88defe17`.

## Decision

1. New exact-source V5 population construction uses one source-governed 900-second construction-path constant. The
   formal budget adapter charges that same constant to both 10k initialization and 10k-to-100k scale, and the RANGE
   construction outer bound consumes it directly. A focused contract rejects drift between the runtime timeout and
   charged path budget.
2. The existing 120-second cap on each production SPI/Oxia operation remains unchanged. Timeout still interrupts the
   owned batch, drains its interrupt, preserves exact progress, produces an infrastructure-invalid terminal
   checkpoint/result, and cannot create evaluation or selection.
3. Head construction, exact captured-Cell proof, ascending initial-grant order, fault-reserved Heads, candidates,
   population sizes, measurement schedule, dispositions, and evidence bytes do not change. Unused time from another
   phase still cannot extend construction.
4. The existing aggregate budgets remain 900/5,400/7,200/5,400/13,120/1,640/600 seconds and the hard cap remains
   48,000 seconds. The V5 zero-decision plan remains
   `974857cab839ba9cfd02ad8694a51976cf0279a4f61d11fe767aef5518a72dea`; the execution profile remains
   `0bfa9670b8e3b1721ab83f03bd34ed368814e914288a5af772d17dec67ee3449`.
5. Before another formal campaign, the new exact clean pushed source must pass the real-Oxia RANGE-1024 100k
   construction-only guard, the complete 26-test V5 diagnostic and canonical NADV5, all source/documentation gates,
   and exact-source pre-campaign validation. The failed `3b96a298...-r1` attempt cannot be resumed or reused.

## Consequences

The runtime can consume the scale budget already reserved by the accepted campaign instead of failing at an older,
smaller harness boundary. This is not extra measurement time and cannot convert a failed interval into a pass. A
later exact-source formal attempt still receives no disposition, evaluation, or selection unless the unchanged
planner, validator, attachment, lifecycle, and promotion contracts all pass.
