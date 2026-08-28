# ADR 0135: M3 V4 RANGE-renewal acknowledged-proof reuse amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0134
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `9fcbc7f2b4cecd10afeb9dd2c251927097893186` passed `v2M3SourceCheck` in 8m03s after ADR 0134. Its
diagnostic-only RANGE-1024 10k/25ms receipt SHA-256 is
`e7d0b45aa5c7d3862d04a3db8a2c761e591258b24991814ac5e69a3d2fee8c86`; JUnit SHA-256 is
`4275cfb18c6989bb65c8a074bb573aa317628848374166a15aabd80dee07e263` with inventory 1/0/0/0.

The foreign-reservation correction reduced fixed-1000 pre-admission drop from 1,803 to 156 and reduced
`RESERVATION_BUSY` from 25,814 to 108. Fixed completed all 29,844 admitted requests without failure/timeout; its 178
retries were 108 reservation-busy, 56 Cell reread, and 14 Cell CAS conflict. Derived-800 again completed all 24,000
offers with zero drop/failure/timeout and zero retry.

The fixed row still executed 160,712 metadata operations for 39,844 observed workflows. Successful grant renewal
already receives the exact reserved Cell from its CAS, exact installed Head from its CAS, and exact cleared Cell from
its CAS, but the production allocator rereads the Cell before Head installation, rereads the installed Head before
clear, then rereads Cell/Head again before candidate creation. Those reads lengthen the globally serialized
reservation lifetime even though the acknowledged mutation results already bind the same keys and versions.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-9fcbc7f2-v4-25ms-residual-reservation-r1`.
Archive-identity SHA-256 is `fa9858171340c8abad29fa19e9e80679594273258da6133b5cb80a1714c08c4b`, manifest SHA-256 is
`adec5a8fb2aff9a8d2490312b862e03c66e4b0e2fc9787bba15f99e1d5e76d26`, and its three payload files total 4,001
bytes. It remains diagnostic-only and cannot authorize selection or a formal disposition.

## Decision

1. The bounded RANGE workflow may install a grant directly from the exact Cell and Head observed together at entry,
   including an exact reserved Cell returned by an acknowledged reserve CAS. The install remains an exact predecessor
   Head CAS.
2. After an acknowledged install Head CAS, the workflow may clear the request-bound reservation directly with the
   exact reserved Cell predecessor. The clear remains an exact predecessor Cell CAS.
3. After the acknowledged clear result, candidate create and publish use the existing store-observed installed-RANGE
   path. Candidate identity and the exact predecessor Head CAS remain unchanged.
4. Only the first successful dispatch uses this proof reuse. Any predecessor-unchanged, indeterminate, failed, or
   conflicting result retains the existing public proofful method, same-key reread, and bounded reconciliation path.
   Public allocator APIs and STRICT mode remain unchanged.
5. Deterministic contracts bind the successful first-grant workflow to one combined Cell/Head read, two Cell CAS,
   one install Head CAS, one acknowledged candidate create, and one acknowledged publish Head CAS, with no node read.
   Existing response-loss and conflict suites must remain green.
6. V4 plan, protocol bytes, schedule, rate, latency, admission, zero-drop rule, SLO, qualification, selection, and
   evidence semantics do not change. No shared Java lock or new authority is introduced.
7. Both frozen 25ms rows must reach zero drop/failure/timeout before canonical NADV4 or formal execution.

## Consequences

The common renewal path holds the global Cell reservation only across the three mutations that create, install, and
clear it; duplicate validation reads cannot serialize otherwise independent Heads. Every uncertain outcome still pays
the full reread proof cost. Allocator mode remains `UNSELECTED` pending a uniquely qualified formal evaluation.
