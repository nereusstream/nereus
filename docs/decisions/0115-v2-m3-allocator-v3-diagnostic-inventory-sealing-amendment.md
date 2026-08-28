# ADR 0115: V2 M3 allocator V3 diagnostic inventory sealing amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0109 and 0114 for the exact current-source NADV3 JUnit inventory only
- Preserves: NACP3, NAEV3, NARS3, NADV3, interval attachment and diagnostic output bytes; source/executor bindings;
  workload, rate, latency, action budget, qualification, disposition, promotion, and selection semantics

## Context

Exact clean source `372ca9751b2f331df62381419dfd6600ff0ef5ca` completed the full current-source real-Oxia
diagnostic task. Gradle emitted six JUnit XML suites with 17 tests and zero failure, error, or skip. The new
`M3RealAllocatorStrictIntervalDiagnosticTest` exercised the exact formal STRICT fixed-1000 then derived-800 schedules
on one population and passed its zero-unexpected-failure, zero-timeout, drain, and concurrency requirements.

The NADV3 sealer nevertheless retained the pre-ADR-0114 five-suite/16-test allowlist. It rejected the six-file JUnit
directory as `allocator V3 diagnostic JUnit file inventory differs` and created no receipt. This is a fail-closed
sealing-wiring defect, not a diagnostic workload failure and not formal campaign evidence.

The diagnostic attachments, JUnit binary/XML inventory, Gradle log, and exit status are byte-preserved in the
read-only external archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-372ca975-adr0114-r1-nadv3-inventory-mismatch`.
The archive contains 25 payload files and 49,204 bytes. Its payload manifest SHA-256 is
`4919d8621efc0692f284327f98d1b4e143209d632c17114943143289d14c9e6d`, JUnit manifest SHA-256 is
`010a115bc05816f2324f434c4af9e5c0ea7f74dab8c2eb28000a44dc0b53daba`, and archive-identity SHA-256 is
`212f4b6d3ecba4dcd13dfbaf37045b7a8f7aa939a31dfa6cb1f09b468fccb011`. It has `diagnosticOnly=true`,
`authority=false`, `selectionEligible=false`, no NADV3, and is neither promotable nor future campaign input.

## Decision

The exact NADV3 allowlist contains six suite identities and 17 testcase identities. It includes
`M3RealAllocatorStrictIntervalDiagnosticTest#strictFixedThenDerivedFormalSchedulesDrainWithoutUnexpectedWarmupFailure()`
in addition to the existing ADR-0109/0110 inventory. The Gradle filter, sealer, parser, unit fixture, source checker,
and current documentation must name the same closed inventory.

The sealer continues to reject every missing, extra, duplicated, aliased, failed, errored, or skipped testcase. It
continues to bind the exact JUnit manifest, Nereus commit, Oxia image, dependency lock, executor artifact, and workload
schedule. Existing NADV3 bytes and historical five-suite/16-test receipts remain parser-compatible and immutable;
this change affects only which current-source JUnit inventory may be newly sealed.

No formal campaign is authorized by this diagnostic repair. A later campaign still requires a fresh exact clean
pushed source, a newly built executor digest, canonical current-source NADV3, full source/pre-campaign gates, and a
new absent exact-source formal directory.

## Consequences

- The exact diagnostic task and NADV3 sealer can no longer drift silently when a governed suite is added.
- A current-source NADV3 proves the ADR-0114 formal-equivalent STRICT sequence ran alongside the complete prior
  diagnostic inventory.
- The failed `372ca975...` diagnostic remains non-authoritative and cannot be resealed or reused.
- No formal evidence, allocator qualification, promotion, or selection rule changes.
