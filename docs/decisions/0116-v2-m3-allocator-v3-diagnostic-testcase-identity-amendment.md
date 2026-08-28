# ADR 0116: V2 M3 allocator V3 diagnostic testcase identity amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADR 0115 for one exact current-source NADV3 testcase identity
- Preserves: all V3 wire and attachment bytes, JUnit content, source/executor bindings, workload, action budgets,
  qualification, disposition, promotion, and selection semantics

## Context

ADR 0115 added the sixth diagnostic suite and seventeenth testcase to the closed NADV3 inventory. Its source used the
intended descriptive identity
`strictFixedThenDerivedFormalSchedulesDrainWithoutUnexpectedWarmupFailure()`, while the actual JUnit method is
`replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()`.

Exact clean source `bc86757948a3ee67d162c918daec343b759435a0` then completed the entire real-Oxia diagnostic at
six suites, 17 tests, and zero failure, error, or skip. The sealer progressed past file inventory and correctly
rejected the testcase-set mismatch as `allocator V3 diagnostic JUnit inventory or result differs`. It created no
NADV3. This is an exact-name wiring defect, not a workload failure or formal evidence.

The 15 diagnostic attachments, complete JUnit binary/XML inventory, Gradle log, and exit status are byte-preserved in
the read-only archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-bc867579-adr0115-r1-nadv3-testcase-identity-mismatch`.
The archive contains 25 payload files and 49,311 bytes. Its payload manifest SHA-256 is
`0e5320704b2e62aa50f2c0d760814dd012511a600315a58e87b16bee8928f625`, JUnit manifest SHA-256 is
`d28090797e1f0ebf45fee5aed172fe20b371109997d6754c5b855f96f5d64c44`, and archive-identity SHA-256 is
`30570535b0d6cf08293cb3f082d0c041a3df9810fcf1c36ba601ab77e2b47bfd`. It is diagnostic-only,
non-authoritative, non-promotable, has no receipt, and is not future campaign input.

## Decision

The seventeenth allowlisted identity is exactly
`M3RealAllocatorStrictIntervalDiagnosticTest#replaysTheExactFormalSequenceWithoutUnexpectedWarmupFailure()`.
The protocol CLI fixture and source checker must use that literal method name from the governed test source. A
negative unit contract substitutes ADR 0115's descriptive name and requires sealing to reject the mismatch.

The suite count, test count, source/executor/workload bindings, create-new behavior, canonical NADV3 parser, and all
other allowlisted identities remain unchanged. Existing diagnostic outputs and any previously valid NADV3 remain
immutable and parser-compatible.

## Consequences

- Current-source sealing binds the JUnit-emitted identity rather than a prose-derived approximation.
- Future drift of this exact method name is caught by both the offline source checker and protocol CLI unit fixture.
- The `bc867579...` diagnostic remains unsealed and cannot be reinterpreted or reused.
- No formal campaign, qualification result, promotion decision, or allocator selection is created by this amendment.
