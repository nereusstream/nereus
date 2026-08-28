# ADR 0112: V2 M3 allocator V3 exact-derived rate workload entry amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0108, 0109, 0110, and 0111 for V3 physical workload dispatch only
- Preserves: V1/V2 schedule entry semantics, all V3 wire bytes and parser goldens, fixed and derived rate identities,
  workload bytes and jitter, plan/action budgets, qualification thresholds, SLOs, dispositions, and selection rules

## Context

The exact-clean `1771b00021dfdc3e86b5ab4f8008bfc69b74c2f0` formal attempt completed all eight Native
baseline rows at 1000 requests per second, emitted one complete failed STRICT fixed-1000 row, and deterministically
resolved its next planner action to the distinct derived-800 slot. It then stopped before dispatching that derived
action. The campaign terminal is `INFRASTRUCTURE_FAILED/INVALID_ACTION_RESULT`, with detail
`IllegalArgumentException: offered rate differs from ADR 0094`, campaign-result SHA-256
`80dba066ca966df3d3143c27e373ead8f385be27d33935e0f2d1345ee6bb1ae0`, final checkpoint SHA-256
`84c61b0d7b14b28f4f64b96b77365100edf7684218b7cc9cd52172291bd941f1`, and formal JUnit SHA-256
`f777fdd4fc114e9e73a1201428f45179367d634f68815f45cecfe85d5c5158a7`. It contains 9 action attachments,
11 checkpoints, 21 regular files, and 39,950 bytes; it created neither evaluation nor selection.

The entire attempt and its JUnit are byte-identical in the read-only external archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-1771b000-r1-infrastructure-failed-derived-rate-workload`.
Its manifest SHA-256 is `62a2d2dccf12ae0d3f6242308bb39ffb06e2e0d1ed1a5bc6d58083e000d93f6b` and archive-identity
SHA-256 is `995b13aad5e77bb760ec7ac6b5bd528ed1b21a0241c3bfb1f563308a33a7ced9`. It is immutable,
non-promotable, and not future campaign input.

The V3 domain planner and validator already accept an exact-derived physical rate and bind it to a separate DERIVED
logical slot. The physical budget adapter also carries that resolved rate correctly. The remaining defect was in the
shared real workload generator: `M3AllocatorWorkloadPlan.requests` intentionally validates only the six ADR-0094
V1/V2 fixed rates, and the V3 formal candidate runtime still called that legacy entry. The V3 runner proof used
synthetic schedules and therefore did not prove this last physical entry connection.

## Decision

The existing V1/V2 `requests`, `requestCount`, `measuredRequestCount`, and
`AllocatorEvidenceScheduleV1.arrivalCursor` entries remain closed to exactly `200/250/333/500/750/1000`.
They continue rejecting 800, 600, 400, and 267, so no historical matrix or schedule contract is relaxed.

V3 receives explicit `v3Requests`, `v3RequestCount`, `v3MeasuredRequestCount`, and `arrivalCursorV3` entries. Their
closed executable rate set is the union of the six fixed rates and the exact outputs reconstructible from
`max(200, ceil(nativeRate * 4 / 5))`: `200/250/267/333/400/500/600/750/800/1000`. Any other value is rejected.
The V3 Native runtime and formal candidate runtime must both call this versioned entry; a source contract constructs
every distinct derived schedule through both physical paths before a campaign can be admitted.

The request ordinal remains the sole phase authority. Actor assignment, ledger cursor, 5/3/2 ENTRY/BYTE/AGE mix,
signed jitter bytes, 10-second warm-up, 20-second 0.5R steady segment, 10-second 2R storm, bounded queue and cleanup
semantics are unchanged. At each exact-derived rate the physical inventory remains exactly `40 * rate` requests and
the measured inventory exactly `30 * rate` requests.

This is an entry correction, not a new plan decision. The canonical workload schedule digest remains
`b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798`; the Native execution profile remains
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`; and the zero-decision plan remains
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`. A source change still produces a new
exact commit and executor artifact binding, a fresh NADV3, fresh preflight, and a new `<source>-r1` directory.

## Consequences

- V3 can physically dispatch its already-defined derived 800/600/400/267 rates without aliasing a fixed slot.
- V1/V2 callers cannot acquire derived-rate schedules through their legacy entry.
- No failed attachment is rewritten and no failed row is promoted into a disposition or evaluation.
- A later formal attempt must still prove the full candidate matrix under the unchanged zero-drop and SLO rules.
