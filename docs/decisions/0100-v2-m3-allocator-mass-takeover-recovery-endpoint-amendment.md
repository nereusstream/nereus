# ADR 0100: V2 M3 allocator mass-takeover recovery endpoint amendment

- Status: Accepted before the replacement formal allocator execution
- Date: 2026-08-26
- Refines: ADRs 0055, 0061, 0091, and 0094
- Preserves: `NVAC1`/`NVAH1`/`NVAN1`, NARE1/NAEA1/NARS1 bytes, the five-candidate/eight-row/nine-cut inventory,
  the 30/60-second recovery limits, and the closed at-most-one selection rule

## Context

ADR 0094 defines broker-session crash recovery from owner-loss detection until every failed-actor ManagedLedger can
admit append under a fresh owner. ADR 0061 separately makes owner takeover an owner-epoch-only Head CAS: an installed
RANGE survives, while STRICT does not allocate a new ledger merely because its owner changes.

The first phased runner correctly completed every `HEAD_TAKEOVER_CAS` before it allowed shared Cell mutation, but its
second phase then invoked one complete allocator rollover for every affected ManagedLedger. At 10,000 ledgers this
added 2,500 unrelated ledger allocations; at 100,000 it would add 25,000. For STRICT those operations execute the
Cell-wide reserve/create/publish/clear path. They measure a synthetic failover-wide rollover burst, not the frozen
fresh-owner append-admission recovery endpoint, and can exceed the harness drain cap before the validator is able to
record an otherwise non-qualifying candidate.

The exact-source diagnostic at `e4f376c63e1b8458c8798d5ea9ca56cf39364377` exposed this mismatch after the native
matrix: the 10,000-ledger STRICT fresh-owner rollover phase did not drain 2,500 completions within 120 seconds. Its
one-test JUnit result has one failure, zero errors, and zero skips. It is not selection evidence and selects no mode.

## Decision

The broker-session crash/mass-takeover cut retains the first in-flight allocator rollover and then executes this exact
recovery sequence:

1. Detect actor `0` loss, close that real Oxia session with the mutation response still in flight, reopen a fresh
   session, and converge the in-flight mutation by the existing production reread rules.
2. Enumerate exactly ledger indices `0,4,8,...` below the active population. For each ledger, choose fresh actor
   `1 + ((ledger / 4) mod 3)` and successor `ownerEpoch + 1`.
3. Complete and drain every affected ledger's production `HEAD_TAKEOVER_CAS` before the append-admission phase. Every
   CAS retains its exact mutation dispatch, same-key reread, and typed terminal disposition proof.
4. Under the same per-Head lock used by production allocation, require the exact takeover Head and successor owner
   epoch, then emit the append admission start/release and `FRESH_OWNER_APPEND_COMPLETE` endpoint. This is a
   no-allocation admission probe. It does not reserve the Cell, create a candidate node, publish a new Head, clear the
   Cell, consume a grant ID, or claim execution of a native Pulsar broker append.
5. Measure recovery from the existing `OWNER_LOSS_DETECTED` event through the last exact fresh-owner completion. A
   completion at or before the frozen limit is terminal `COMPLETED`; a completion after the 30-second 10k or 60-second
   100k limit retains its successful fresh-owner proof but is terminal `TIMED_OUT`, so the production selector rejects
   that row.
6. Drain all predeclared takeover/admission tasks by an absolute cleanup deadline 120 seconds after the selection
   deadline. The cleanup allowance cannot extend the selection limit. Failure to drain by that cleanup deadline is a
   failed JUnit execution and invalid evidence, not a candidate timeout that can be converted to selection input.

The parser continues to require the complete failed-actor ledger inventory and to count any `TIMED_OUT` endpoint as
disqualifying. It does not accept partial inventory, synthesize a completion, or relax exact write proof. NARE1 already
has the required `COMPLETED`, `TIMED_OUT`, and `FRESH_OWNER_APPEND_COMPLETE` events, so no byte schema, golden, or
source-lock format changes.

## Consequences

- Mass takeover measures owner fencing plus restored append admission, while the normal interval matrix remains the
  authority for rollover allocation throughput and Cell append-stall behavior.
- RANGE still proves that installed grants survive owner takeover; STRICT no longer pays an invented full-population
  allocation burst merely to prove owner recovery.
- A slow but fully drained row is represented faithfully and disqualified by the frozen recovery/timeout bounds. A
  stuck Provider, undrained executor, missing affected ledger, or broken proof remains an invalid formal execution.
- Local and diagnostic results under this amendment remain non-promotable. The complete current-source native/fault/
  10k/100k run and production reparser are still required before selecting a mode or promoting a scenario.

This decision refines only the mass-takeover recovery endpoint and runner termination semantics. It is tracked by
`T-POSITION-01`, `T-POLICY-01`, `V2-POSITION-017/018`, and `V2-OPEN-PUL-OBJ-09`.
