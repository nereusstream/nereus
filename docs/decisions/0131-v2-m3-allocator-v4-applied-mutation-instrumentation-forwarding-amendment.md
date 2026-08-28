# ADR 0131: M3 V4 applied-mutation instrumentation forwarding amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0130
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `3bc110885c5b9c0ac9367d0594a291231400adb3` passed the complete source gate and ran the
diagnostic-only RANGE-1024 10k/25ms sequence after ADR 0130. Its receipt SHA-256 is
`c5c24ec3076676c5d1ad51930e855d0b75b921d930cee9d50c6013f4ddc21892` and JUnit is 1/0/0/0. The fixed row
dropped 6,862 of 30,000 offers; the derived row dropped 3,150 of 24,000. Both had zero failure/timeout and complete
drain. Derived still executed 174,022 operations for 28,821 completed workflows, including 116,374 reads: about six
operations and four reads per workflow, rather than ADR 0130's four-operation target.

The production `AsyncOxiaConditionalClient` and allocator store used the new acknowledgement path. The shared formal
and diagnostic `M3RealOxiaActors.InstrumentedClient`, however, implemented only the legacy `Void` mutation methods.
Java therefore selected the interface defaults for acknowledgement calls; those defaults intentionally return an
empty acknowledgement after legacy success, so `ConditionalMutationEngine` correctly fell back to the same-key get.
The measurement is an exact formal-instrumentation composition defect, not evidence that the production adapter lost
the Oxia `PutResult`.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-3bc11088-v4-25ms-ack-wrapper-fallback-r1`.
Its archive-identity SHA-256 is `4dd9c05007f547222599982070de814b05439b10edde156e973f2b96f98b656f`, manifest
SHA-256 is `82466fb0c46ff14da402cf2730c675e6fe33e4cf7659f78a9772bcfcd1417cc7`, and its seven payload
files total 5,592 bytes. It is diagnostic-only and cannot become NADV4 or formal input.

## Decision

1. `InstrumentedClient` and its request-bound `BoundClient` must explicitly implement both acknowledged mutation
   methods and forward them to the current exact session delegate. Interface-default fallback is forbidden in the
   formal/diagnostic composition.
2. The existing mutation instrumentation becomes value-preserving and generic. It must return the exact
   acknowledgement through controlled latency, response-loss injection, crash barrier, diagnostic outstanding
   accounting, and request telemetry without manufacturing or dropping the value.
3. A response-loss or crash cut still hides the successful response and drives the production same-key reread. The
   wrapper records exactly one mutation operation; it must not synthesize a reread event when the successful
   acknowledgement reaches the caller.
4. A deterministic contract uses a delegate whose legacy mutation methods fail and whose acknowledged methods return
   exact versions. It must prove both outer and bound wrapper dispatch preserve the values and telemetry without
   legacy fallback.
5. V4 diagnostic inventory remains exactly 23 tests/nine suites. A new exact clean pushed source must pass the full
   source gate and formal-equivalent 25ms receipt before another campaign.
6. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975`, all workload,
   rates, SLOs, zero-drop, budgets, selection, and evidence bytes remain unchanged.

## Consequences

The correction changes only formal/diagnostic runtime composition and executor/source identity. It does not alter the
production acknowledgement rule or reinterpret the `3bc11088...` receipt. Allocator mode remains `UNSELECTED`; a
fresh canonical NADV4 and formal result are still required before any source-lock, child, scenario, or Final update.
