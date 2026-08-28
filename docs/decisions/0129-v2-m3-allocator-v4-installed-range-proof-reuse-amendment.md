# ADR 0129: M3 V4 installed-RANGE proof-reuse amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0128
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `d434f9107edd0bf1ff93c3e2d469d307ef373673` ran the diagnostic-only RANGE-1024
10k/25ms fixed-1000 then exact-derived-800 sequence in
`diagnostic-only/d434f9107edd0bf1ff93c3e2d469d307ef373673-v4-25ms-attribution-r1`. The receipt SHA-256 is
`42db8caa2bf024889e98b110c25a7db8e491516db565be6cdbaf2864c421b899`; its exact filtered JUnit inventory is
1/0/0/0. It is non-authoritative and cannot become campaign input.

The fixed row offered 30,000 requests, completed all 19,024 admitted requests, dropped 10,976 before admission, and
had no failure or timeout. The derived row offered 24,000, completed all 17,396 admitted requests, dropped 6,604, and
also had no failure or timeout. Both reached the 256 global outstanding cap and drained completely. Real Oxia RTT p99
was 6,359/7,329us and the one-thread delay scheduler p99 lag was 11,086/11,590us, while workflow p99 was
565,286/289,615us and queue-wait p99 was 3,122,071/2,859,382us. Timer and Oxia latency contribute but do not explain
the full capacity loss.

In an uncontended installed-RANGE request, the bounded workflow first reads exact Cell and Head. The public allocator
API then rereads the same Cell/Head before node creation and rereads Cell/node before Head publication. The node
create and Head CAS each independently retain their mandatory same-key reread in `ConditionalMutationEngine`. Thus
the common path performs ten metadata operations across seven sequential controlled-latency stages even though the
workflow already owns the exact authority snapshots and exact node returned by the mutation reread.

## Decision

1. Only when `acquireGrant` has just read exact Cell/Head, proved RANGE mode, and observed a usable installed grant,
   the bounded workflow may reuse that exact authority pair for candidate creation and reuse the exact node returned
   by the create mutation's same-key reread for publication.
2. The installed-RANGE common path removes only the two duplicate proof-read pairs. Initial Cell/Head reads remain;
   node create plus same-key reread remains; Head CAS plus same-key reread remains. The operation count becomes six
   and the uncontended sequential chain becomes five stages.
3. The public `ProductionVirtualLedgerAllocator#createCandidate` and `publishCandidate` APIs retain their independent
   proof reads. STRICT, grant install/renewal, fault cuts, create-conflict reconciliation, takeover, response-loss, and
   stale-node paths retain the existing proofful calls. No caller may select the proof-reuse entry without the
   workflow-owned store observation.
4. Every mutation keeps its exact predecessor, mandatory same-key reread, typed outcome, bounded retry/backoff, and
   workflow deadline. A conflict leaves the fast path and re-enters the existing explicit reread/reconcile path. No
   Java Cell lock, cache authority, extra queue, or late-completion dispatch is introduced.
5. The exact 23-test/nine-suite diagnostic must prove the new operation inventory, zero failure/error/skip, complete
   drain, and 25ms capacity before formal execution. This implementation slice by itself selects nothing.
6. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975`, admission,
   workload, rates, SLOs, zero-drop rule, budgets, selection order, and NACP4/NAEV4/NARS4/NADV4 bytes remain unchanged.

## Consequences

The correction changes production executor/source bytes but not durable allocator wire or V4 evidence semantics. The
`83193069...-r1` campaign remains immutable `NONE_QUALIFIED`; the `d434f910...` receipt remains diagnostic-only.
Another campaign requires a new exact clean pushed source, create-new canonical NADV4, full gates, and a create-new
formal directory. Allocator mode remains `UNSELECTED`; source locks, children, scenarios, and M3 Final remain open.
