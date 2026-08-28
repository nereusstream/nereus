# ADR 0113: V2 M3 allocator V3 completed-workflow Cell reconciliation amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0108, 0109, 0110, 0111, and 0112 for V3 formal harness state handoff only
- Preserves: production allocator CAS/reconcile authority, V1/V2 execution, all V3 wire bytes and parser goldens,
  workload/rate identities, plan and action budgets, thresholds, SLOs, dispositions, and selection rules

## Context

The exact-clean `ba7e313fb4f5d08c84cfe6f3e43448f119aee4c8` formal attempt proved the ADR-0112
exact-derived workload entry: it completed all eight Native baseline rows, the failed STRICT fixed-1000 and
derived-800 rows, the RANGE-16 10k scale action, and the RANGE-16 fixed-1000 interval. The derived action was executed
at its exact 800 requests per second rather than rejected by the legacy fixed-rate entry.

After the successful RANGE interval, the first following fault action stopped fail-closed with
`INFRASTRUCTURE_FAILED/INVALID_ACTION_RESULT` and detail
`AllocatorProtocolException: allocator Head range/cursor lies outside the Cell consumed slice prefix`. Its
campaign-result SHA-256 is `cf9b50e4174fb568694e54ef7b4c05a30e9642dcc4178d150a9f9a76dc980bcd`, final
checkpoint SHA-256 is `79fc70ea74e0a2cb0f8f43e784538c5e82d5f3668ecb5152876a9bdb904ddcf3`, and formal
JUnit SHA-256 is `0c2d064a549d8bf69492e87fafbc05aadf2c04537f4b19d238d7e94091b60ae3`. It contains
12 action attachments, 13 checkpoints, 26 regular files, and 52,805 bytes. It created neither evaluation nor
selection.

The entire failed attempt and its formal JUnit are byte-identical in the read-only external archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-ba7e313f-r1-infrastructure-failed-head-slice-prefix`.
Its manifest SHA-256 is `94bd6d9f74c98c178aaa1773cb32d3e605a4207736c69076d4bf2b1b34ef3e3e` and archive-identity
SHA-256 is `f9f11e32d71662d4ab061f955e66aa4e8c15a46aa6927622ed8cb048c275aea4`. It is immutable,
non-promotable, and not future campaign input.

The production-neutral V3 workflow returned both `Result.exactHead` and `Result.exactCell` after its bounded Oxia
CAS/reread/reconcile chain. The formal population completion callback retained the exact Head but discarded the exact
Cell. Completion order is intentionally unconstrained, so the harness's local proof snapshot remained at its
pre-interval consumed prefix even after a later exact Head was installed. Normal V3 interval requests still used the
production workflow and store reads correctly; the defect appeared when the subsequent synchronous fault action
combined that exact Head with the stale harness Cell snapshot. The protocol validator correctly rejected that
impossible pair.

## Decision

Only the V3 async completion handoff is changed. Before releasing a successful binding completion, the formal
population merges `Result.exactCell` into its harness-local proof snapshot and then replaces the exact predecessor
Head. A lost predecessor replacement is an infrastructure failure rather than a silent cache miss.

The Cell merge is monotonic and completion-order independent. It verifies the same allocator mode, protocol version,
namespace, slice assignment, and exact slice geometry; rejects a completed result that still contains a reservation;
retains the newer local snapshot when an older completion arrives; accepts only a jointly increasing consumed cursor
and next-grant identifier; and rejects divergent partial ordering. Equal value snapshots retain the existing opaque
metadata version rather than guessing an ordering that the backend does not expose.

This snapshot is not allocator correctness authority. Each normal V3 request still enters
`BoundedVirtualLedgerAllocatorWorkflowV2`, reads exact Oxia authorities, and proves correctness through CAS, same-key
reread, bounded retry, and exact terminal results. No cross-actor Java lock is added, and V2's historical callback is
unchanged. The local snapshot exists only for the formal population's later diagnostic/fault helpers, which require
one internally consistent Cell/Head pair.

The canonical workload schedule digest remains
`b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798`, the Native execution profile remains
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`, and the zero-decision plan remains
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`. A source change still requires a new exact
clean pushed commit, fresh current-source NADV3 and preflight, and a new `<source>-r1` formal directory.

## Consequences

- A completed V3 interval hands its exact Cell and Head proofs to later scale/fault actions without completion-order
  regression.
- Fault execution cannot pair a post-interval Head with a pre-interval consumed-prefix snapshot.
- Production lock-free correctness, evidence bytes, qualification, and selection contracts remain unchanged.
- The failed `ba7e313f...-r1` directory remains invalid infrastructure evidence and can never be resumed or promoted.
