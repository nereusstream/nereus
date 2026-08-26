# ADR 0101: V2 M3 allocator Cell-proof concurrency scheduling amendment

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR 0094's formal candidate coordinator scheduling
- Preserves: ADRs 0055, 0061, 0091, 0094, 0097, and 0100; `NVAC1`/`NVAH1`/`NVAN1`, Oxia keys and transition
  outcomes; the five-candidate workload, all numeric bounds, raw event grammar, closed selection rule, and M6 exclusion

## Context

ADR 0091 requires the coordinator to prove an exact current versioned Cell before every Head or node mutation that
does not itself CAS the Cell. RANGE allocation is concurrent after grants are installed, while reserve/install/clear
remains one Cell-wide mutation chain. The formal runner cached the last exact Cell in one atomic reference and
serialized Cell mutation chains, but installed-range Head/node paths captured that reference outside the mutation
lock. A concurrent range renewal could therefore advance the real Cell between the cached read and the production
SPI's mandatory same-key reread.

The complete raw-matrix diagnostic at exact Nereus source
`d819500f6da8d024e77bc1bcb26ba7dcf4ee42da` exposed this defect after 5,071.065 seconds. JUnit is exactly one test,
one failure, zero errors, and zero skips. The first `AllocatorProtocolException` is `CELL_STATE_DRIFT` with message
`exact versioned Cell is not the current same-key store snapshot`; the other concurrent failures are retained as
suppressed exceptions. It emitted no `selection.nars`, evaluation, verifier receipt, selected mode, or RANGE size.
The closed diagnostic directory is
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-d819500f-r4`; all listed files rehash
against its `SHA256SUMS`, whose SHA-256 is
`bd2ae96c38ccb7a1fb416541dab9109684bdddfff3e0d734f74a84ce2805ca67`.

Weakening the exact Cell reread, treating drift as success, deleting failed events, or serializing all RANGE rollover
would contradict accepted correctness or invalidate the performance comparison. The runner instead needs an explicit
proof-phase scheduler whose contention stays inside the measured operation.

## Decision

The formal candidate coordinator owns one fair shared/exclusive Cell-proof lock for its exact in-process evidence
population:

- an installed RANGE grant takes the shared side from exact Cell snapshot capture through node creation and Head
  publication, so independent ManagedLedger Heads remain concurrent;
- owner-only Head takeover takes the same shared side around its exact Cell proof and Head CAS;
- STRICT rollover and RANGE reserve/install/clear take the exclusive side; when RANGE needs a fresh grant, the
  exclusive side remains held through the dependent node and Head publication for that request;
- the late-old-owner cut chooses the shared side for an installed RANGE grant and the exclusive side whenever it must
  mutate the Cell; its create/takeover/conflict/burn chain therefore observes one exact Cell proof phase;
- mass takeover remains concurrent through shared proofs, and ADR 0100's following no-allocation append-admission
  phase remains unchanged;
- RANGE population construction takes the exclusive side before it captures the exact Cell and holds it through Head
  create plus any initial reserve/install/clear. STRICT Head construction remains parallel because it does not mutate
  the unchanged Cell;
- population construction must observe controlled metadata latency exactly zero. Its elapsed time is reported only in
  the non-authoritative construction summary and cannot enter a throughput row.

The per-ManagedLedger Head lock remains the same single-flight boundary. The shared/exclusive proof lock is an
evidence-runner coordinator inside the already accepted four-session, one-JVM M3 harness. It does not claim a native
Pulsar broker process, distributed lock, controller integration, or M6 runtime activation. All lock waiting occurs
after the original request is offered and is therefore retained in end-to-end, queue, starvation, and append-stall
evidence rather than removed from the measured interval.

The contract gate must prove that unlocked RANGE population capture fails closed, write-locked capture is admitted,
STRICT construction does not acquire unnecessary Cell serialization, and nonzero injected construction latency is
rejected. A diagnostic-only real-Oxia RANGE task must construct 10,000 Heads and overlap installed-grant readers with
range renewals without writing selection evidence. Passing that diagnostic is still not formal evidence. The complete
five-candidate raw matrix must be rerun at a later exact clean source, and any subsequent source-lock/policy change
requires the final formal freshness rerun.

## Consequences

- Exact Cell validation remains strict; no parser, golden, failure code, SLO, timeout, or selection preference changes.
- Installed RANGE allocations preserve concurrent Head/node execution, while unavoidable Cell-wide grant chains expose
  their write-lock stall in the measured workload.
- The `d819500f...` execution is immutable failed diagnostic evidence and cannot select or promote anything.
- C2 remains non-promotable, all M3-I0 exclusions remain intact, and M6 native broker/controller activation remains
  outside M3.
