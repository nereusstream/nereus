# Stage B V4 fixed-storm retry attribution

- Design status: Accepted through ADR 0133
- Runtime status: diagnostic instrumentation pending exact-source execution
- Selection authority: none

## Observed boundary

Exact `792c77de...` proves the clean derived-800 installed-RANGE path at 25ms: zero drop and exactly four metadata
operations. Fixed-1000 still drops 1,999 requests during its frozen 2R storm while completing every admitted request.
The row records 25,780 reconcile retries and 52,334 reads above the two-read common-path baseline, so retry reason—not
real RTT or acknowledgement presence—is the next missing discriminator.

Receipt `a58a9c6b...2435` and archive identity `cd6be8fb...9497` remain diagnostic-only and non-promotable.

## Capture boundary

Each candidate population owns a normally empty atomic retry-capture reference. The 25ms diagnostic opens one capture
immediately before the shared operation capture and closes it after the runner drains. The workflow's existing retry
scheduler records the exact closed enum before executing the unchanged deterministic 20–23ms backoff.

The receipt emits sorted retry-reason counts. With zero failed workflows, their sum must equal the sum of each
completed `Result.reconcileRetries`. Capture cannot overlap, cannot be ended while absent, and is never opened by the
formal action runtime.

## Next gate

The new exact receipt must distinguish initial `HEAD_REREAD`, mutation conflict/unresolved, Cell, reservation, and
node recovery. Only a category-specific, proof-preserving correction may follow. Both frozen 25ms rows still require
zero drop/failure/timeout before canonical NADV4 and formal execution.
