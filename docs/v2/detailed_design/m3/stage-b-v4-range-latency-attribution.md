# Stage B V4 RANGE latency attribution

- Design status: Accepted through ADR 0126
- Runtime status: diagnostic wiring only; current-source run pending publication
- Selection authority: none

## Immutable input

The exact `c44a56c27aadc804231becc07bad33bfa82d794d-r1` V4 campaign completed with NAEV4
`NONE_QUALIFIED`. NACP4 validation, canonical NAEV4 seal, all attachment/JUnit/source checks, and the non-promotable
promotion decision pass. No NARS4 exists. The byte-identical external archive is
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-c44a56c2-r1-v4-none-qualified`;
its manifest digest is `ab829692af6b015468cd652619ad07e92aadf7d83dccd4848ec9a64189a39db3` and identity digest is
`85b618fd923c8bf76064acd46aa89ac52c21f5f188acc6e6a67fa120db1b075f`.

## Attribution boundary

All Native rows sustain 1,000 requests/second, while RANGE-64/256/1024 first eliminate at 10k/10ms. RANGE-1024
derived-800 offers 24,000 requests, admits 22,665, completes all admitted work with zero post-admission failure or
timeout, and drains every lifecycle counter, but drops 1,335 requests before admission. Its workflow and Oxia p99 are
180,070us and 177,199us. This is a capacity boundary, not an invalid checkpoint or cutoff-accounting failure.

`M3V4RangeLatencyDiagnosticTest` replays the exact fixed-1000 then derived-800 sequence on one shared RANGE-1024
population at 10k/10ms. It records:

- runner actor/global outstanding, bounded queue, queue wait, offer-scheduler lag, callback lag, and rollover p99;
- real Oxia read/create/CAS count, real RTT, controlled-delay scheduler lag, delay callback lag, end-to-end operation
  outstanding, and the simultaneously measured global pre-delay real-operation outstanding peak;
- workflow completion/failure count and latency while preserving exact conservation and terminal drain.

The new suite is diagnostic-only, carries `authority=false` and `selectionEligible=false`, and expands the exact
current-source NADV4 inventory to 22 tests/nine suites. It cannot select a candidate. A later implementation change
must cite its measured bottleneck, retain every ADR-0125 frozen contract, publish a new exact clean SHA, and rerun the
entire diagnostic before any new formal campaign.
