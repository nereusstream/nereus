# ADR 0126: M3 V4 RANGE latency attribution amendment

- Status: Accepted
- Date: 2026-08-28
- Supersedes: the 21-test/eight-suite current-source diagnostic inventory in ADR 0125
- Preserves: ADR 0125 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `c44a56c27aadc804231becc07bad33bfa82d794d` completed the first source-bound V4 formal
campaign in `bounded-adaptive-formal/c44a56c27aadc804231becc07bad33bfa82d794d-r1`. The campaign-result is
`f7fedbb4b07384dea8b9f9fd9b80c8c74b16d55dccbde3efdef4397f77cfcd3d`, final NACP4 is
`224873c5d5a275128705b097ccdcba0d471767ea05c964734ed2e9140b431e01`, canonical NAEV4 is
`5d1a35dd90ac4dcf4bcb13e30aaa8c242c579f63aaf27232068a90545387c315`, and the attachment root is
`31b632b48d18507cc8e8ba62465a9474d5e54b822f90e22b38eba7f845309947`. The formal JUnit result is 1/0/0/0 with
digest `4b186cb2123162635d2dcd8e06ee18c2efceb0d4edfbb8a82ce3900fc1420583`. Offline checkpoint validation, canonical
evaluation sealing, attachment/JUnit/source revalidation, and promotion checking all pass. The evaluation is
`NONE_QUALIFIED`, `selectionEligible=false`, allocator mode remains `UNSELECTED`, and no NARS4 exists.

The terminal inventory is 25 executed performance cells, 303 deterministic logical dispositions, seven aggregated
fault rows backed by 63 exact cut attachments, 11 scale attachments, and 33 checkpoints. All eight Native rows find a
1,000-request/second sustainable baseline, so every candidate row has exact derived floor 800. The terminal candidate
boundaries are:

| Candidate | First eliminating row | Derived-800 terminal counts | Workflow/Oxia p99 |
| --- | --- | --- | --- |
| STRICT | 10k / 1ms | 24,000 offered; 20,415 pre-admission drop; 1,003 complete; 2,582 failed | 2,242,968 / 2,242,528 us |
| RANGE-16 | 10k / 5ms | 24,000 offered; 10,194 pre-admission drop; 13,764 complete; 42 failed | 974,652 / 1,023,054 us |
| RANGE-64 | 10k / 10ms | 24,000 offered; 1,041 pre-admission drop; 22,959 complete; zero failed | 181,270 / 180,665 us |
| RANGE-256 | 10k / 10ms | 24,000 offered; 755 pre-admission drop; 23,245 complete; zero failed | 174,643 / 172,314 us |
| RANGE-1024 | 10k / 10ms | 24,000 offered; 1,335 pre-admission drop; 22,665 complete; zero failed | 180,070 / 177,199 us |

Every row preserves offered/admitted/terminal conservation and drains queue, bindings, permits, and operation
outstanding to zero. The V4 terminal admission correction is therefore effective, but the most permissive RANGE still
cannot sustain the derived 800 row under the 10ms controlled-latency composition. The existing V4 diagnostic proves
only the exact RANGE-16 1ms fixed-1000/derived-800 sequence and small single-operation latency rows. It does not
measure high-load delay-scheduler lag, callback lag, real RTT, metadata operation count, or operation outstanding at
the first common formal failure boundary.

The complete formal payload is permanently archived at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-c44a56c2-r1-v4-none-qualified`.
Its 135 regular payload files total 20,556,200 bytes, rehash under `SHA256SUMS` digest
`ab829692af6b015468cd652619ad07e92aadf7d83dccd4848ec9a64189a39db3`, and have archive-identity digest
`85b618fd923c8bf76064acd46aa89ac52c21f5f188acc6e6a67fa120db1b075f`. The source and payload were compared
byte-for-byte after copy. The archive and original formal directory are immutable non-promotable history and cannot
be reused by a later campaign.

## Decision

1. Add one diagnostic-only V4 suite that replays the exact RANGE-1024 10k/10ms fixed-1000 then derived-800 formal
   sequence through the same population, workload generator, four actor workflows, `4/64/256/1` admission, two-second
   terminal admission drain, and five-second cleanup.
2. Capture runner outstanding/queue/wait/scheduler/callback/terminal telemetry together with real Oxia operation kind,
   real RTT, controlled-delay scheduler lag, delay callback lag, and both end-to-end and pre-delay real-operation
   outstanding telemetry. The shared global peak must be measured concurrently across actors rather than derived by
   summing their independent maxima. The receipt must contain raw counts and percentiles; it cannot accept
   caller-supplied qualification or selection Booleans.
3. The current-source NADV4 inventory becomes exactly 22 tests in nine JUnit suites: the previous 21/eight inventory
   plus `M3V4RangeLatencyDiagnosticTest#exactRange1024TenMillisSequenceAttributesOperationAndSchedulerCapacity()`.
   Historical NADV4 and its eight-suite manifest remain valid only for their exact source.
4. This diagnostic is observational until the data identifies a concrete bottleneck. It requires conservation,
   lifecycle drain, nonempty operation samples, exact 10ms injection, and actual operation concurrency greater than
   four, but it does not reinterpret drop as pass.
5. The V4 plan digest, execution profile, logical inventory, rates, derived floor, candidates, workload bytes, jitter,
   admission caps, zero-drop rule, SLOs, budgets, selection preference, and NACP4/NAEV4/NARS4 semantics remain
   unchanged. Any later implementation correction requires a new exact clean source, new executor digest, fresh
   nine-suite NADV4, and a new formal output directory.

## Consequences

The `c44a56c2...-r1` result permanently proves `NONE_QUALIFIED` for its exact source. It does not authorize production
allocator selection, source-lock update, current-source M2 regression, child refresh, scenario promotion, or M3
Final. The next change must be justified by the new telemetry rather than by an assumed scheduler or workflow cause.
No second campaign may run until that implementation is source-bound, diagnostic-equivalent, fully gated, committed,
and pushed.
