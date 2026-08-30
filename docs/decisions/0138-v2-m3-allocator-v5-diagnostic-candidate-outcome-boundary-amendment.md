# ADR 0138: M3 V5 diagnostic candidate-outcome boundary amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADR 0137 for current-source V5 diagnostic validation only
- Preserves: every V1/V2/V3/V4/V5 canonical wire; the frozen workload, candidates, rates, latency rows, zero-drop
  qualification rule, p99/fault/scale SLOs, selection preference, action budgets, and M6 activation boundary

## Context

Exact clean source `a1664de9ca84b1d162510e4163ef8c94d898c46f` passed `v2M3SourceCheck` and executed the
complete V5 diagnostic inventory. All ten JUnit suites and 24 tests passed without failure, error, or skip. The Native
rows, RANGE-16 sequence, RANGE-1024 10/25ms rows, and terminal-admission drain were lossless and drained completely.
The Native 10k/25ms/500 row reached 74 real ManagedLedger operations outstanding, proving that the old four-worker
composition was not present.

NADV5 sealing nevertheless failed closed. STRICT fixed-1000 offered 30,000 measured requests, admitted 4,100,
dropped 25,900 before admission, completed 1,162, and failed 2,938 after admission; derived-800 had the same bounded
contention shape. The short workflow attribution records `RECONCILE_RETRY_EXHAUSTED` after `RESERVATION_BUSY` and
Cell-CAS conflict. That is an exact candidate performance result under STRICT's per-ID Cell serialization, not a
missing terminal or harness failure. RANGE-16 fixed-1000 and the V5 terminal drain each completed all 30,000 requests
with no loss or residue.

The attempt is immutable diagnostic-only history at
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/diagnostic-a1664de9-v5-failed-strict-compatibility-r1`.
Its archive-identity SHA-256 is `63e47adc6c503d79140a12ff5a7494336d8ddf398cd691811dfce4b17d09cb68`, its
manifest SHA-256 is `65fafcf33318c0ab6d61214050e6d12a4abfb30778caaf526489ed72c64b97fd`, and its 31
payload files total 60,612 bytes. It contains no NADV5 and can never authorize formal execution.

ADR 0137 accidentally required the diagnostic raw gate to treat STRICT zero loss as a pre-campaign invariant. That
moves candidate qualification ahead of the formal planner and selector. It makes the valid outcome “STRICT does not
qualify, the smallest lossless RANGE qualifies” unreachable even though V5 evaluation explicitly represents it.
The same compatibility replay also constructed a V3 `64/256` runner with no terminal-admission drain while claiming
V5 formal equivalence.

## Decision

1. Candidate qualification remains exclusively a validator-reproved formal result. A diagnostic may record STRICT
   drop or admitted failure without becoming infrastructure-invalid. Formal V5 still requires zero measured drop,
   zero admitted failure, zero timeout, and every unchanged SLO for a candidate to qualify.
2. The STRICT and RANGE-16 compatibility replays use the V5 `4/128/512/1` runner, the exact 10-second warm-up,
   30-second measured interval, two-second terminal-admission drain, and five-second cleanup. Their receipts bind
   those timings explicitly.
3. STRICT raw validation reconstructs warm-up and measured conservation, source identity, real concurrency above
   four, zero unexpected warm-up failure/timeout, and zero lifecycle residue. It preserves the exact drop/failure
   outcome instead of converting that outcome into a diagnostic failure.
4. RANGE-16, RANGE-1024 10/25ms, Native baseline, and terminal-drain receipts retain their existing lossless hard
   gates. They prove that the formal-equivalent runtime has a feasible RANGE path; they do not select a range.
5. The NACP5/NAEV5/NADV5/NARS5 wire, plan digest
   `3e0aea42527e85c58276a51f5953af0ffaba5029b8916e7bbd85f377f434d23a`, execution-profile digest
   `76d9bc38ce6fa9c47b2fed926c9485db828adaee3e1533b962ab6e9c1157e1ce`, admission tuple, and all budgets remain
   unchanged. A new exact clean source and executor/raw-manifest/NADV5 digest isolate this correction naturally.

## Consequences

A fresh exact-source complete V5 diagnostic is still mandatory and must seal and parse-canonically validate NADV5
before formal output can be created. It must not suppress, rewrite, or reinterpret STRICT's negative performance
observation. The formal campaign may then disqualify STRICT and select only the smallest qualifying RANGE, or produce
another legal terminal state according to the unchanged selector. Production allocator mode remains `UNSELECTED`;
this decision updates no source lock, child receipt, scenario, or M3 Final.
