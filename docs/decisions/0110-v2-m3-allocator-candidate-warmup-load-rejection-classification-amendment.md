# ADR 0110: V2 M3 allocator candidate warm-up load-rejection classification amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0108 and 0109 for V3 candidate-interval infrastructure classification only
- Preserves: every V1/V2/V3 wire byte and parser; the V3 plan, admission, rates, populations, latency rows, candidates,
  SLOs, qualification thresholds, adaptive ordering, selection rule, action budgets, and hard deadline

## Context

The first post-ADR-0109 V3 formal attempt ran at exact clean source
`e60327ae4054c3f9df1e79a28f44aa7d97b84bb5`, plan
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`, executor
`ee749b663670d1729149aceef35ca69508833cc9fea9dd76e4b13c8248c9fda4`, and unchanged Native profile
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`. All eight Native rows established a valid
1000-request/second baseline with zero measured drop, failure, timeout, or lifecycle residue. The ninth action was the
first STRICT 10k/1-millisecond/1000-request candidate interval.

That candidate row correctly recorded overload and bounded allocator rejections: measured execution admitted 3,519
of 30,000 requests, completed 997, and failed 2,522 after admission, with every request accounted and every queue,
permit, and operation drained. During the separate warm-up inventory, 931 admitted requests received the typed
`RECONCILE_RETRY_EXHAUSTED` rejection while contending on `RESERVATION_BUSY`. The harness incorrectly classified any
warm-up failure as infrastructure failure, so the adaptive planner could not consume the measured failed row and
descend to the next frozen rate.

This is a harness classification defect. ADR 0108 requires zero **measured** drop, failure, and timeout for a rate to
qualify. A typed, bounded allocator rejection during warm-up is evidence that the probed rate is under load; it is not
proof that the executor, service, source, lifecycle, or attachment infrastructure failed. Conversely, hiding the
warm-up rejection or treating an unexpected exception as ordinary load would weaken fail-closed evidence.

The failed attempt is immutable at
`nereus-metadata-oxia/build/m3-allocator-evidence/bounded-adaptive-formal/e60327ae4054c3f9df1e79a28f44aa7d97b84bb5-r1`.
Its 20 files and 33,506 bytes are archived byte-for-byte at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-e60327ae-r1-infrastructure-failed-candidate-warmup`.
The archive manifest SHA-256 is `138b4802ce1d1d4f805ea0ce629e2a793de46c90894373467343a87223822351`,
the archive identity SHA-256 is `a1cbd600cd5d1b1543629d8b5dc61a51754afe408980e92d1c39855fc568da6f`, and the
campaign-result SHA-256 is `67e8e86ae8a553379e6ebbfb6905912c1e43dcbac81d4f3d600cf9040896bb97`. It created
no evaluation or selection and is not reusable by another campaign.

## Decision

### Separate candidate load rejection from infrastructure failure

The V3 Runner accounts every warm-up terminal in the existing conservation inventory and additionally partitions
warm-up failures into:

- `warmupLoadRejectedAfterAdmission`: an explicitly enumerated `AllocatorProtocolException` outcome caused by bounded
  candidate contention, reconcile, range, grant, ownership, descriptor, or workflow-budget rejection;
- `warmupUnexpectedFailedAfterAdmission`: every other failure, including source, protocol, mode, slice, request-context,
  Head identity/geometry, non-canonical wire, arbitrary runtime, and unknown future failure codes.

The two counters must sum exactly to `warmupFailedAfterAdmission`. The first failure summary remains mandatory when the
sum is nonzero.

A Native interval retains the ADR-0109 rule: any warm-up failure or timeout is infrastructure-invalid. A candidate
interval is infrastructure-valid only when actor lanes stop at the fixed cleanup boundary, warm-up timeout is zero,
and `warmupUnexpectedFailedAfterAdmission` is zero. A nonzero typed load-rejection count does not qualify the rate; it
only permits the already-complete measured inventory to reach the validator and adaptive planner. Measured drop,
failure, timeout, duplicate/reused ID, SLO, or lifecycle residue continues to fail that rate exactly as before.

### No hidden warm-up failure

Every V3 interval attachment now records the complete warm-up offered, dropped, completed, load-rejected, unexpected-
failed, timed-out, first-failure, and lane-stop fields beside the measured evidence. Campaign failure detail retains
the same partition. The attachment is source/executor-bound and create-new; no old attachment or canonical V3 wire is
rewritten.

The V3 zero-decision plan digest, Native execution profile, workload schedule, 328/360/32/720 action maxima, and
48,000-second hard cap do not change. The implementation and its executor JAR do change, so another formal campaign
requires a new exact clean pushed SHA, a newly computed executor digest and NADV3, full pre-campaign gates, and a new
`<new-sha>-r1` directory.

### Failed-attempt archival

`archive-v2-m3-allocator-failed-formal.py` accepts only fail-closed `INFRASTRUCTURE_FAILED` or `INTERRUPTED` results
with `evaluationCreated=false` and `selectionCreated=false`. It verifies the campaign result, last checkpoint, formal
JUnit, file count, byte count, regular-file inventory, and collision-free archive path; copies the payload byte-for-
byte; emits `SHA256SUMS` and a create-new identity; and makes the archive read-only. Such an archive is permanently
non-promotable and never a future campaign input.

## Consequences

- The `e60327ae...-r1` attempt remains a valid immutable infrastructure-failure record, not a candidate evaluation.
- Expected warm-up load rejection is visible and conserved but no longer prevents deterministic rate descent.
- Unexpected warm-up failure, timeout, incomplete cleanup, source/runtime drift, and every measured failure remain
  fail-closed.
- No SLO, threshold, workload, candidate, rate, disposition, selection preference, or evidence-accounting rule is
  relaxed.
- A later formal run starts only at the new exact clean source and cannot resume or reuse the failed attempt.
