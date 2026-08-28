# ADR 0137: M3 V5 storm admission and diagnostic raw-integrity amendment

- Status: Accepted
- Date: 2026-08-29
- Amends: ADRs 0108, 0125, and 0136 for a new allocator evidence protocol only
- Preserves: every V1/V2/V3/V4 canonical byte and terminal result; the frozen workload, candidates, rates, latency
  rows, zero-drop rule, p99/fault/scale SLOs, qualification thresholds, selection preference, and M6 activation boundary

## Context

Exact clean source `bb928a0be61f293dbbbcba2535adddba3a82f567` passed the complete `v2M3SourceCheck` after
ADR 0136. A standalone exact RANGE-1024 10k/25ms diagnostic then completed fixed-1000 and derived-800 with zero
drop/failure/timeout. Its immutable archive is
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-bb928a0b-v4-25ms-controlled-delay-pass-r1`;
archive-identity and manifest SHA-256 values are
`8633644f5e434d8fd2392d13705d4b990b57f947ba3b185e92c97644bc76e8b5` and
`4b8d5f86de1a59d9df45cae280f3539c67d0e3ae1b40ec1803980812fec65db3`.

The subsequent complete 23-test/nine-suite diagnostic reused the same exact runtime but accumulated the full
formal-equivalent lifecycle before the RANGE-1024 rows. Its 10ms fixed-1000 and both derived-800 rows were lossless.
At 25ms, fixed-1000 offered 30,000, admitted and completed 29,876, and dropped 124 before admission, with no admitted
failure or timeout. It reached 256 Runner outstanding, 468 real Oxia operations outstanding, 1,405 queued requests,
1,078,356us queue-wait p99, 13,207us real-Oxia RTT p99, 11,358us controlled-delay scheduler lag p99, and 126,746us
end-to-end workflow p99. This is bounded overload, not missing completion delivery or allocator correctness failure.

The output exposed a diagnostic integrity gap: the V4 RANGE test checked conservation and drain but did not assert the
unchanged zero-drop rule. JUnit therefore reported 23/0/0/0 and the orchestration sealed an otherwise canonical NADV4
with SHA-256 `d5dce30522a549c305a08d15072eaec6ffe7050d68c47fcf3ced42b685d70da9`. That receipt is diagnostic-only and
non-authoritative because its raw hard gate failed. It cannot authorize a formal campaign or selection even though
its fixed wire remains parseable.

The complete attempt is byte-preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-bb928a0b-v4-full-zero-drop-gate-miss-r1-archive2`.
Its archive-identity SHA-256 is `404f9bddc87f0f47cf4d272fa64bdc94254d903038d8d54593cdeddc46f20cd7`, manifest
SHA-256 is `2010a324159902472945dd018ab64d457770e9c190270e6dcae1ec05b1462a80`, and its 40 payload files total
68,751 bytes. The earlier collision-free attempt directory ending in `gate-miss-r1` contains only a copied payload and
manifest because the old archiver rejected its two RANGE receipts before writing an identity. It also remains
untouched and is not an authoritative archive.

V4 retained the V3 `4/64/256/1` admission tuple. That tuple was derived from 1,000 requests/second at the 250ms
rollover SLO, but the already-frozen measured schedule contains a 10-second `2R` storm. For the maximum fixed rate,
the storm offers 2,000 requests/second. Even the optimistic runner-only bound at 250ms is only
`256 / 0.250 = 1,024` requests/second. With a two-second terminal drain, a constant service bound must satisfy
`(2000 - C) * 10 <= C * 2`, or `C >= 1,667` requests/second. V4 is therefore structurally capable of a legal
candidate p99 while still deterministically dropping the frozen storm tail. Repeating the same source or weakening a
threshold would not correct the protocol.

## Decision

1. V4 remains byte-for-byte stable. Current-source V4 diagnostic tests write raw receipts before asserting both fixed
   and derived rows have zero drop/failure/timeout and exact admitted/completed conservation. V4 sealing, validation,
   promotion, and selection entry points independently require those exact 10ms and 25ms raw receipts and reject the
   `bb928...` full-suite attempt. The NADV4 wire itself is not changed.
2. The diagnostic archiver accepts every sorted `v4-range1024-*-formal-sequence.json` receipt. Its identity adds a
   plural path/digest inventory while retaining the old singular fields when exactly one receipt exists. An archive
   target remains create-new and byte-verified.
3. The next formal-capable protocol is V5 with distinct `NACP5`, `NAEV5`, `NADV5`, and `NARS5` identities. No V4
   artifact is V5 input. V5 retains the exact V4 offer horizon, two-second terminal admission drain, five-second
   cleanup, schedule bytes, logical slots, dispositions, action maxima, budgets, evaluation states, and selection
   algebra.
4. V5 admission is `4 actors / 128 outstanding per actor / 512 global / 1 per normal binding`. The cap is derived
   from the maximum instantaneous frozen storm rate:

   ```text
   ceil((2 * 1000 requests/s) * 0.250 s / 4 actors) = 125; closed power-of-two cap = 128
   global cap = 4 * 128 = 512
   ```

   The pre-admission queue remains `2 * base offeredRate`, exactly the maximum one-second storm inventory. There is no
   unbounded queue, hidden executor queue, shared Java Cell correctness lock, or relaxation of per-binding
   single-flight.
5. V5 feasibility reports both the old steady-rate proof and a distinct storm/drain proof. It must classify
   `4/64/256/1` at 250ms/2,000 requests per second as `STORM_ADMISSION_INFEASIBLE` and the exact V5 tuple as
   `PLAN_FEASIBLE`. These are optimistic runner structural bounds, not allocator throughput promises.
6. NADV5 binds the exact diagnostic JUnit manifest and a canonical raw-output manifest. Sealing, parse-canonical
   validation, promotion, and selection all reconstruct both manifests. Caller-supplied pass booleans cannot replace
   raw zero-drop, conservation, lifecycle, or source/profile checks.
7. A new formal campaign is permitted only after a complete V5 diagnostic passes from a new exact clean pushed source
   and proves the 25ms fixed-1000 and derived-800 rows lossless under the exact V5 admission tuple. Every failed
   diagnostic remains diagnostic-only and immutable.
8. The external archive tooling is protocol-aware and create-new. V5 diagnostic archives bind an explicit PASSED or
   FAILED status, exact JUnit inventory, all diagnostic bytes, and the NADV5 presence bit; V5 completed non-promotable
   and failed formal archives carry distinct V5 identities. No archive changes or upgrades an evidence artifact's
   authority.

## Consequences

The V4 negative diagnostic is preserved as a protocol-feasibility observation, not hidden or reinterpreted. V5 adds
bounded capacity for the workload that was already frozen, without changing an offered request, latency injection,
qualification threshold, or selection rule. Allocator mode remains `UNSELECTED` until a unique, validator-reproved V5
formal evaluation seals a canonical selection.

The accepted implementation fixes the V5 zero-decision plan digest at
`3e0aea42527e85c58276a51f5953af0ffaba5029b8916e7bbd85f377f434d23a` and the Native execution-profile digest at
`76d9bc38ce6fa9c47b2fed926c9485db828adaee3e1533b962ab6e9c1157e1ce`. NADV5 independently binds the exact
24-test/ten-suite JUnit manifest and an exact 19-JSON raw manifest; NARS5 preserves both digests. This implementation
record is not diagnostic or formal evidence and does not select an allocator mode. The protocol-aware archive
contracts preserve V3/V4 defaults while closing collisions, foreign-protocol RANGE attribution, noncanonical dates,
and a PASSED diagnostic with any failure, error, or skip. Raw validation reconstructs the ten Native row ordinals and
their population/latency/rate identities, and rechecks measured conservation, zero loss, warm-up terminal accounting,
real concurrency, and lifecycle markers for STRICT, RANGE-16, RANGE-1024, and terminal-drain authority receipts.
Interrupted partial diagnostics have an explicit non-authoritative archive status and cannot seal NADV5.
