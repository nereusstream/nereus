# ADR 0055: V2 Pulsar virtual-ledger allocator evidence protocol

## Status

Accepted as the allocator-mode evidence protocol for Pulsar `OBJECT_WAL` in 0.2. Neither allocator mode is accepted.
ADR 0091 now supplies exact M3 production wire/key/transitions and local implementation tests; real/native
10,000/100,000 runtime evidence has not run.

## Context

The STRICT_SERIALIZED proposal reserves one ledger ID, publishes one immutable node, CASes one ManagedLedger head, and
clears the reservation: four successful metadata writes per rollover, serialized for the Protocol Cell. At 100,000
active ledgers and a ten-minute rollover interval, time rollover alone averages about 166.7 rollovers/second. Even an
idealized one millisecond per serialized write gives only about 250 rollovers/second before queueing, so a former 50%
admission rule would allow about 125/second and already miss that workload. This is an upper-bound calculation, not a
benchmark result.

Absolute queue-delay thresholds alone cannot establish native Pulsar parity, and persisting observed latency/rate
budgets in allocator identity would make performance evidence into a durable correctness format. RANGE_LEASED also
needs its correctness protocol designed before it can be selected; waiting for the final STRICT benchmark would delay
that work without reducing correctness risk.

## Decision

0.2 selects neither `STRICT_SERIALIZED` nor `RANGE_LEASED` until current-source evidence and the complete candidate
correctness contract exist. RANGE_LEASED fencing/recovery design proceeds in parallel with STRICT evidence.

M1 implements candidate SPI, deterministic fault cuts, telemetry, and receipt production only in the test/evidence
layer; none enters the production metadata SPI or persists a mode. Its receipt is explicitly
`HARNESS_CONFORMANCE_ONLY` with `selectionEligible=false`. M1 runs deterministic and small exact-source smoke workloads
to prove the harness itself. M3 owns the 10,000/100,000 multi-broker capacity run and the only receipt that may make a
candidate eligible for selection.

This harness receipt is distinct from `REGISTRY_CONFORMANCE`, which proves compatibility-namespace identity, complete
writer commitment/admission interlock, Registry CAS, and versioned derived slice views. Harness conformance cannot
promote or replace Registry evidence; Registry conformance does not select an allocator.

The allocator evidence protocol covers:

- 10,000 and 100,000 active ManagedLedgers per Protocol Cell;
- the measured production-like distribution and jitter of entry-, byte-, and age-triggered rollover, plus synchronized
  storms rather than active-ledger count alone;
- multiple brokers and controlled metadata latency/error profiles, including 1/5/10/25-ms p99 cases;
- crash and response loss at every candidate protocol write plus single-ledger and broker-wide owner/takeover recovery;
- takeover of 10,000 and 100,000 ManagedLedgers, including allocator operations caused by failover and the time until
  native append admission resumes without Cell-wide serial head-of-line blocking;
- sustained rollover rate, each operation latency, queue depth/age, per-topic starvation, Cell-wide append stall,
  metadata load, recovery time, and every error/fencing outcome.

The primary capacity result is the maximum sustainable rollover requests/second while every predeclared latency,
queue, starvation, append-stall, recovery, and error SLO remains satisfied. The phrase “serialized p99 capacity” is not
an accepted metric. The same harness records a native Pulsar rollover and append-stall baseline; a candidate must meet
both predeclared absolute safety bounds and predeclared relative-to-native acceptance bounds. Thresholds are frozen
before execution and cannot be relaxed after observing a result.

Allocator durable identity contains only the selected allocator mode, allocator protocol version, and the recovery /
fencing identities required by that protocol. Measured or admitted rate, queue, latency, and recovery budgets belong to
versioned Protocol Cell policy and evidence. Host resources remain runtime ceilings only. No value can let one host
select a different mode or weaken the persisted recovery protocol.

## Consequences

- `V2-OPEN-PUL-OBJ-10`'s evidence-protocol decision is resolved; its scenario remains PLANNED and cannot be cited as a
  performance pass.
- `V2-OPEN-PUL-OBJ-09` remains open and both modes remain unselected. ADR 0061 fixes the correctness constraints for an
  incarnation-owned RANGE grant, RESERVED takeover, owner-only head fencing, one stale-candidate burn, background
  clear, and bounded permanent-orphan accounting. ADR 0091 fixes exact wire/key/transitions and the allowed RANGE
  domain; real reservation concurrency/cut evidence, exact selected RANGE size, and mode selection remain open.
- A simple absolute queue threshold or active-ledger-count-only test cannot admit STRICT_SERIALIZED.
- M1 must publish source-qualified harness-conformance evidence without claiming performance or selection eligibility.
  M3 must execute the complete scale protocol against its pinned source before selecting an allocator mode.

This decision is refined by ADRs 0061/0082/0091, refines ADRs 0022, 0027, 0032, 0041, 0048, 0049, and 0054 and is
tracked by `T-POSITION-01`, `T-POLICY-01`, `V2-POSITION-010/011/017/018`, and `V2-OPEN-PUL-OBJ-09`.
