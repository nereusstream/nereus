# ADR 0132: M3 V4 evidence-store specialized mutation forwarding amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0131
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `e53b3af84ef25bb86a4f2da2e868e3f716e84803` passed the complete source gate and ran the
diagnostic-only RANGE-1024 10k/25ms sequence after ADR 0131. Receipt SHA-256 is
`e4b3972156711fa3cb81f9dff27eb9d6c4e7fef7f8e7bd4d1f1484a3de256aba`; JUnit is 1/0/0/0. The fixed row dropped
7,446 of 30,000 offers and the derived row dropped 3,029 of 24,000. Both completed with zero failure/timeout. The
derived row had zero reconcile retries but still executed 173,688 operations for 28,948 completed observed workflows:
28,948 creates, 28,948 CAS operations, and 115,792 reads, exactly six operations and four reads per workflow.

ADR 0131 correctly forwarded the acknowledgement through `InstrumentedClient` and `BoundClient`. The next outer
formal/diagnostic decorator, `M3EvidenceAllocatorStore`, implemented only the ordinary SPI create and Head-CAS
methods. Its inherited interface defaults routed both installed-RANGE specialized calls back to those ordinary
methods. The production adapter therefore intentionally used the proofful mutation path and performed one same-key
reread after each mutation. This is another exact formal composition defect; it does not change the production
allocator, the acknowledgement contract, or the diagnostic result.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-e53b3af8-v4-25ms-evidence-store-fallback-r1`.
Archive-identity SHA-256 is `4fd18526473b2bac06a9c6815300760bc9fb5d6e684d3e55f8514595766e4208`, manifest SHA-256 is
`bac9bf6e08c37935ba7c1ae00dda079caf17cf3c98b8cf8e86a4a92b30b7d23e`, and its three payload files total 3,918
bytes. It is diagnostic-only, failed the four-operation entry requirement, and cannot become NADV4 or formal input.

## Decision

1. `M3EvidenceAllocatorStore` must explicitly override `createNodeAfterStoreObservedRangeAuthorities` and
   `compareAndSetHeadAfterStoreObservedRangeNode`. Each override preserves the same exact request trace, authority
   key, operation kind, fault-cut terminal accounting, and production adapter used by the corresponding ordinary
   method, but dispatches the specialized SPI method.
2. Interface-default fallback for either installed-RANGE specialized operation is forbidden in formal and diagnostic
   composition. Public allocator, STRICT, renewal, conflict, takeover, and fault paths keep their existing proofful
   methods.
3. A deterministic contract must drive both methods through `M3EvidenceAllocatorStore` with legacy mutations set to
   fail. It must observe acknowledged create and CAS results, zero legacy calls, exactly two instrumented mutation
   samples, and no read sample.
4. A fresh exact-source 25ms receipt must prove four common-path operations and zero drop/failure/timeout for both the
   fixed and derived rows. The complete 23-test/nine-suite NADV4 and all source gates remain mandatory before formal
   execution.
5. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975`, rates, SLOs,
   zero-drop requirement, budgets, disposition, evaluation, selection, and evidence bytes remain unchanged.

## Consequences

The correction is limited to the shared evidence decorator and its source/executor identity. It restores equivalence
with the already accepted production installed-RANGE path without adding a new protocol version or reinterpreting any
historical result. Allocator mode remains `UNSELECTED`; source-lock, children, scenarios, and Final remain closed until
a fresh canonical diagnostic and formal campaign produce an eligible unique selection.
