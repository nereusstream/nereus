---
productLine: V2
designStatus: Accepted
implementationStatus: Implemented
evidenceStatus: FormalInfrastructureFailed
authority: DetailedDesign
sourceTuple: v2-m3-allocator-v5
---

# Stage B V5 population-construction budget alignment

[ADR 0145](../../../decisions/0145-v2-m3-allocator-v5-population-construction-budget-alignment-amendment.md)
closes a formal/runtime budget drift exposed by exact source `3b96a298...`. RANGE-64's 100k transition completed
90,000/90,000 Head creates and 84,118/89,424 initial grants before the old 600-second harness timeout, while the
formal scale action had already reserved a frozen 900-second path budget.

The formal adapter now owns one package-visible `CONSTRUCTION_PATH_SECONDS=900` constant. Both initial-population and
scale budget charges use it, and `M3CandidateAllocatorPopulation.POPULATION_DRAIN_TIMEOUT_SECONDS` aliases it. The
contract fails if the charged path and runtime cutoff drift. The per-operation 120-second cap, interrupt/drain path,
exact Cell proof, serial grant order, fault reservations, evidence wire, plan/profile digests, action totals, and
qualification semantics remain unchanged.

The focused closure also widens only the wall-clock margins of the existing V4 terminal-drain unit fixture after a
loaded host delivered its nominal 30 ms offer after a 40 ms test window. Testcase identity, V4 runtime behavior, offer
cutoff semantics, admission drain, and every canonical inventory remain unchanged; this removes no assertion.

The failed attempt is immutable and non-promotable. Its byte-identical external archive is
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/formal-3b96a298-v5-range64-100k-construction-timeout-r1`
with identity `27b28916...91de`, manifest `e54770fe...fe17`, 106 files, and 17,569,678 payload bytes. It has terminal
`INFRASTRUCTURE_FAILED`, final NACP5 `6342a476...6039`, no NAEV5, and no NARS5.

A fresh exact source must first pass the real-Oxia RANGE-1024 100k construction guard and the complete canonical V5
diagnostic. Only then may a new formal directory be created. No prior action, checkpoint, or raw attachment crosses
that source boundary.
