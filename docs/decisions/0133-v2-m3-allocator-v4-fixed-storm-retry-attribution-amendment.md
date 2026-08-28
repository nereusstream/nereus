# ADR 0133: M3 V4 fixed-storm retry attribution amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0132
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `792c77de83198173a0ef1f06ff190f27388a2f31` passed the complete source gate after ADR 0132. Its
diagnostic-only RANGE-1024 10k/25ms receipt SHA-256 is
`a58a9c6b8d9bb068c9247ced8fa09a382842abfe3ff7215145d2800710d92435`; JUnit is 1/0/0/0. The derived-800 row
completed all 24,000 measured offers with zero drop/failure/timeout and zero retry. Its observed workflows executed
exactly two reads, one acknowledged create, and one acknowledged CAS each, proving the four-operation correction.

The fixed-1000 row still dropped 1,999 of 30,000 measured offers. It completed 28,001 with zero failure/timeout but
reported 25,780 reconcile retries. Its 37,712 observed workflows executed 127,758 reads, 37,712 creates, and 37,928
CAS operations. Real RTT, controlled-delay scheduler, and acknowledgement forwarding are no longer sufficient to
attribute those extra reads: the retry reason is required to distinguish stale request Head rebases, mutation
conflicts, Cell/grant renewal, and indeterminate-response recovery.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-792c77de-v4-25ms-fixed-storm-retries-r1`.
Archive-identity SHA-256 is `cd6be8fb612e2fbb6770421b2c9eed36bb51b53edab14b6bf32628947d899497`, manifest SHA-256 is
`8c7a6c5f4c7f5b33a25cdb603a2927423b23432ca78e28404fead4c0683f34ad`, and its three payload files total 3,907
bytes. The derived PASS does not make this failed two-row diagnostic promotable or reusable as formal input.

## Decision

1. `M3CandidateAllocatorPopulation` may expose an explicitly opened diagnostic-only retry capture around an interval.
   The capture counts the closed `RetryReason` enum at the exact source-governed backoff dispatch point.
2. Capture is inactive by default. Formal execution, fault actions, population construction, workflow Result, raw
   evidence, NACP4, NAEV4, NARS4, plan, and validator bytes remain unchanged.
3. The RANGE latency receipt adds a canonical `retryReasons` object per row. Its sum must equal the observer's
   completed-workflow reconcile count when the row has zero failed workflows. Begin/end capture must be single-owner
   and fail closed on overlap or absence.
4. A new exact-source diagnostic must use the reason inventory to select a code correction; no retry category may be
   relabeled, suppressed, or removed from accounting to manufacture zero drop.
5. The fixed-1000 and derived-800 rows must both reach zero drop/failure/timeout before the full 23-test/nine-suite
   NADV4 and any fresh formal campaign. V4 plan digest
   `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975` and every threshold remain unchanged.

## Consequences

This amendment adds diagnostic observability only. It does not yet assert whether the fixed-row retry source is a
runner, population cache, allocator workflow, or real conflict defect. The next correction must follow the exact
reason counts and retain all conflict/reconcile correctness paths. Allocator mode remains `UNSELECTED`.
