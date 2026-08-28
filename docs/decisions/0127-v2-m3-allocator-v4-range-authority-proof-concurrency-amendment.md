# ADR 0127: M3 V4 RANGE authority-proof concurrency amendment

- Status: Accepted
- Date: 2026-08-28
- Extends: ADR 0126
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

The exact-source diagnostic on `c4f442ea807154045dca1fbb2bbb0e9e36ce96bb` reproduced the V4 formal boundary.
RANGE-1024 at 10k/10ms derived-800 offered 24,000 requests, admitted/completed 22,688, dropped 1,312 before
admission, and had no failure or timeout. The runner reached 256 global outstanding and the Oxia adapter measured 256
simultaneous pre-delay real operations. Workflow p99 was 171,980us; real Oxia RTT p99 was 5,738us; controlled-delay
scheduler lag was 558us p50 and 6,234us p99. The row executed 306,800 metadata operations for 30,680 warmup plus
measured completions: per request, eight reads, one create, and one compare-and-set.

The ten operations are not ten necessary sequential stages. The existing production coordinator serializes three
pairs of independent exact-authority reads:

1. initial Cell and Head observation;
2. Cell and Head proof before candidate-node create;
3. Cell and exact node proof before Head publish CAS.

All mutation attempts still require the existing same-key reread in `ConditionalMutationEngine`; those rereads are
not redundant and remain unchanged. The measured single-thread controlled-delay scheduler is not changed: its median
lag is small, and expanding it before removing the proven serial proof chain would obscure the workflow cause.

The diagnostic task ended 23/2/0/0 across nine suites because the runner-only latency-grid test used a 20ms host
admission window. Under concurrent system load, the rate-250/latency-1 row dispatched none of its four t0 offers
before that synthetic cutoff. The second failure was the resulting `@AfterAll` inventory assertion. This is a
diagnostic-test timing defect, not a V4 row result. No NADV4 was sealed. The complete output and JUnit directory are
archived byte-for-byte at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-c4f442ea-v4-range-attribution-r1-failed`.
The archive identity digest is `3df061c7d7ec8fe3baa4d325ecffa0b1aae61b65fb479972ba21c31c8c7acacc`, its
manifest digest is `ba2a5b47c6fcf5c78ddc3990d6c4e8191eac06cde1c462c291241f45899d31e4`, and the
RANGE attribution receipt digest is `044bc00007ecdfd5951ffafb8ebea88dcf14a6a6ec5965a59dd7e7f9bb6c8ec0`.

## Decision

1. Dispatch each independent authority-proof pair concurrently and join both exact results before the next mutation.
   The installed RANGE steady-state path remains lock-free and retains the same ten metadata operations, exact values,
   same-key rereads, CAS predecessors, bounded retry, one-ID consumption, and late-completion no-next-dispatch guard.
2. The resulting uncontended chain has seven sequential metadata stages rather than ten: one initial authority stage,
   one pre-create proof stage, create plus reread, one pre-publish proof stage, and CAS plus reread.
3. Add deterministic store barriers proving both reads in every parallel pair are dispatched before either completes.
   Conflict, response-loss, deadline, and same-key reconciliation tests remain mandatory.
4. Increase only the runner-only test's synthetic admission and cleanup windows to 250ms and `latency+250ms`.
   Its rates, latency values, requests, admission caps, expected terminal inventory, and feasibility claims do not
   change.
5. Archive every failed diagnostic attempt with a create-new, symlink-rejecting, byte-verified manifest and exact
   JUnit summary. A failed diagnostic cannot produce NADV4 or become campaign input.
6. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975` and every
   frozen campaign/evaluation/selection contract remain unchanged. A fresh exact-source 22-test/nine-suite NADV4 must
   pass before another formal campaign.

## Consequences

This correction changes executor bytes and source identity, not V4 logical identity. It does not select a candidate,
update the production source lock, or reinterpret either `c44a56c2...-r1` or the failed `c4f442ea` diagnostic. Formal
execution is permitted only after the new diagnostic proves zero failure/error/skip, exact canonical NADV4, preserved
conservation/drain, and enough RANGE-1024/10ms capacity to clear the frozen fixed-1000 and derived-800 rows without
pre-admission drop.
