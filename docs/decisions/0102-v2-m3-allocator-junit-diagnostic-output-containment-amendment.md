# ADR 0102: V2 M3 allocator JUnit diagnostic-output containment amendment

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR 0094's formal allocator JUnit execution/attachment boundary
- Preserves: ADRs 0055, 0061, 0091, 0094, 0097, 0100, and 0101; the fixed 16-MiB JUnit attachment cap, five-candidate
  workload, all raw event grammar and numeric bounds, closed selection rule, exact-source freshness, and M6 exclusion

## Context

The complete five-candidate matrix at exact Nereus source
`1ef4f108307cb95a06fd5c55950b041eebadc813` executed for 15,462.505 testcase seconds and returned exactly one test,
zero failures, zero errors, and zero skips. Sealing then failed closed because the exact Gradle JUnit XML was
113,519,059 bytes, above NAEA1's immutable `[1,16 MiB]` bound. `system-out` alone was 113,518,209 bytes and contained
970,241 copies of one warning from `org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl`:
`Ledger was already deleted`.

The warning is deterministic harness-cleanup noise. The exact native-Pulsar population releases each closed mock
ledger's retained 64-KiB payload before ManagedLedger's later asynchronous delete. That production delete observes
the expected `NoSuchLedger` result and logs a warning once per rollover. The test therefore passed, but the evidence
publisher correctly refused an oversized diagnostic attachment. It wrote no `evaluation.json`, `selection.nars`, or
`raw-verification.json`, selected no allocator mode or RANGE size, and promoted no scenario.

The failed run is retained as immutable diagnostic evidence under
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-1ef4f108-r5`. Its ten listed files
rehash against `SHA256SUMS`, whose SHA-256 is
`5d896755fb3434539e5817105bc871e2004124f431405bc92648ffe02c8a573d`. The raw archives cannot be resealed after a
source change: the evidence runner artifact digest is part of every NAEA source tuple.

Raising the JUnit cap, truncating or rewriting the XML after execution, deleting arbitrary WARN/ERROR output, or
reusing the old raw archives would weaken the accepted parser/freshness contract. The formal worker instead needs a
source-controlled, exact-message filter for this one expected cleanup outcome before the next complete rerun.

## Decision

The reproducible thin allocator evidence artifact contains and loads `log4j2-test.xml`. Its ManagedLedger logger owns
one ordered composite filter:

- only a `WARN` whose formatted message is exactly `Ledger was already deleted` returns `DENY`;
- every other ManagedLedger `WARN` returns `NEUTRAL` and remains visible through the root WARN appender;
- every non-WARN event, including an `ERROR` with the same text, returns `ACCEPT` and remains visible;
- the root logger remains `WARN`, and no other logger, message, level, test output, or Gradle JUnit field is filtered.

The formal matrix test must verify this runtime Log4j configuration before it constructs native population or opens
real Oxia actors. A focused runner contract must independently exercise the exact expected-WARN, different-WARN, and
same-message-ERROR decisions through Log4j events. Missing, renamed, broadened, or unloaded configuration fails before
the expensive workload.

The exact Gradle JUnit XML remains the only test attachment. It must parse to a non-empty count with zero
failure/error/skip and remain at most 16 MiB; no post-run sanitization or alternate synthetic report is permitted. A
new complete five-candidate real-Oxia/native-Pulsar run at the later exact source is mandatory before selection.

## Consequences

- The r5 matrix is a closed, non-promotable diagnostic even though its one testcase passed.
- The 16-MiB cap, strict XML parser, raw archives, SLOs, failure cuts, candidate inventory, and at-most-one selection
  rule are unchanged.
- Unexpected ManagedLedger WARNs and all ERRORs remain evidence-bearing output.
- Any source change, including this runner-only filter, invalidates r5 freshness and requires a complete rerun.
- C2 remains non-promotable, all M3-I0 exclusions remain intact, and native broker/controller activation remains M6.
