# ADR 0134: M3 V4 independent installed-RANGE reservation amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0133
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `026dfddf436e4c80e9e38459fb098a0e2c2d8791` passed the complete source gate after ADR 0133. Its
diagnostic-only RANGE-1024 10k/25ms receipt SHA-256 is
`ca860b99d0cf3219f444a53d074bb0a7a0f8c01b5db2b46e1eaa22ea9c1817b1`; JUnit is 1/0/0/0. The derived-800 row
completed all 24,000 measured offers with zero drop/failure/timeout and zero retry. The fixed-1000 row completed
28,197 of 30,000 measured offers, dropped 1,803 before admission, and had no failure or timeout.

The fixed row's 25,890 reconcile retries have an exact closed attribution: 25,814 `RESERVATION_BUSY`, 50
`CELL_REREAD`, and 26 `CELL_CAS_CONFLICT`. One of the fixed fault-reserved bindings can therefore hold the Cell
reservation while renewing its exhausted Head grant, but the workflow tests that reservation before recognizing an
unrelated Head's already installed, unconsumed RANGE grant. The source-governed 20--23ms retry backoff then serializes
independent installed grants behind a Cell operation whose authority they do not consume.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-026dfddf-v4-25ms-foreign-reservation-blocking-r1`.
Archive-identity SHA-256 is `32e4f5244a70f9339f4c4386cfd14f876c5ad08ca48fea6aab76d4450be7091f`, manifest SHA-256 is
`5cdfac4fb8554aaab3c7561e5c9499525dd05364529b5375c2b02f5bf2518483`, and its three payload files total 4,009
bytes. This failed diagnostic remains non-authoritative and cannot be reused as formal input.

## Decision

1. After reading the exact Cell and request Head together and validating the request-bound Head predecessor, the
   bounded workflow recognizes an installed unconsumed RANGE grant before interpreting the Cell reservation.
2. The installed-grant branch continues to construct a request-bound candidate, create it through the acknowledged
   store-observed RANGE operation, and publish it with an exact predecessor Head CAS. A reservation for another Head
   neither authorizes nor invalidates those operations and remains untouched.
3. A reservation for the target Head is not bypassed. Its owning in-flight workflow retains exact install/clear
   reconciliation, while any new acquisition retains bounded `RESERVATION_BUSY`. A RANGE Head without a usable grant
   also still obeys any Cell reservation.
4. Deterministic contracts must prove both sides: an independent installed grant completes with zero Cell CAS and zero
   retry while the foreign reservation remains exact, and a Head needing a grant cannot bypass that reservation.
5. The V4 plan digest, protocol bytes, qualification threshold, zero-drop rule, retry budget, allocator modes, and
   evidence semantics do not change. No shared Java Cell lock or new grant authority is introduced.
6. Both frozen 25ms rows must reach zero drop/failure/timeout before the complete 23-test/nine-suite canonical NADV4
   and any fresh formal campaign.

## Consequences

Cell serialization remains authoritative for grant creation and renewal, while allocation from independent installed
Head grants is again concurrent by construction. Conflict, takeover, response-loss, same-Head publication, and grant
renewal paths retain their exact CAS/reread proofs and bounded retries. Allocator mode remains `UNSELECTED` until a new
formal evaluation produces one uniquely qualified candidate.
