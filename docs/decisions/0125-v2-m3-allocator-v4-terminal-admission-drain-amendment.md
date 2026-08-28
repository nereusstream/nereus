# ADR 0125: V2 M3 allocator V4 terminal-admission-drain amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADRs 0108, 0118, 0120, 0122, and 0123 for a new allocator evidence protocol only
- Preserves: every V1/V2/V3 byte and terminal result; the frozen workload, candidates, rates, derived floors,
  zero-drop rule, p99/fault/scale SLOs, bounded admission, per-binding single-flight, and deterministic selection

## Context

The V3 campaign at exact clean source `0cc962e90e6e46b6460d889b5427d415f2191c21` completed normally. Its plan,
campaign-result, final NACP3, NAEV3, and attachment-root SHA-256 values are:

```text
5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283
cb969c7aa2bd6c653e379927ee661075ec7c422ba93633e5e6940f733f38b3ef
01cb2eac7e07f206b8bf72c197dcb628b871e323a3415bfa0f87cd23527b8194
ee42f91d962a8424892192f156eaa7679166c527c025ed885563264e6f077b9b
8a89b0aedeb3b796939fdcd70f354d6d4d31338605bc94c6492717d6b239023e
```

The canonical evaluation is the truthful, non-promotable status `NONE_QUALIFIED`; `selectionEligible=false`, no
NARS3 exists, and allocator mode remains `UNSELECTED`. Its 44-file/105,177-byte immutable archive is
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-0cc962e9-r1-none-qualified`.
The archive manifest and identity SHA-256 values are
`d64ebbe36ff0030fb714bd819b88cd01aad1cdd6d0ba0eb43d4740638e609c23` and
`feb00c62338232bc91ac531bb7ad4aa3125f5a52ee3e60510a0b5b134e4c3e97`. Neither the source directory nor archive may
be rewritten, resumed, converted, or used as V4 input.

All eight Native baselines sustained 1,000 requests/second with zero measured drop/failure/timeout. Every candidate
was then eliminated by its first 10k/1-millisecond exact-derived-800 row. STRICT had admitted workflow failures;
RANGE-16/64/256/1024 retained respectively 21/10/9/9 measured requests as `PRE_ADMISSION_CUTOFF`, with zero admitted
failure/timeout and complete drain. A source-bound promotion recheck consumed every physical attachment exactly once
and reproduced `NON_PROMOTABLE_EVALUATION`.

The RANGE boundary is not another offer-timer defect. At the same exact source the formal-equivalent cutoff diagnostic
reported the first derived-800 drop at request ordinal `31960`, binding `9730`, scheduler lag zero. The immutable V1
schedule gives that binding a preceding request at ordinal `31922`; their arrival gap is 23,875 microseconds and the
later request arrives only 25,000 microseconds before the physical cutoff. The same schedule contains a still shorter
3,624-microsecond same-binding gap at ordinal `31964`. At fixed 1,000 requests/second, ordinal `39943` repeats binding
`1269` after 9,750 microseconds and arrives 28,500 microseconds before cutoff. The current diagnostic measured RANGE
rollover p99 at 51,387/132,270 microseconds for fixed-1000/derived-800. The frozen 25-millisecond metadata row makes
the right boundary stronger: a request that requires even one controlled metadata operation may still own the
per-binding permit when its on-time successor reaches the last tens of milliseconds.

V3 therefore conflates two different events at one instant: closing the offered-load sample and refusing all
already-offered pre-admission work. Zero scheduler lag, bounded queues, and exact terminal conservation prove that the
tail requests were not late, hidden, retried, or lost. They were right-censored by the protocol while waiting for the
accepted per-binding single-flight permit. Making the implementation faster cannot make zero-drop qualification
stable for every frozen latency row and deterministic tail collision. Replaying V3 until a favorable timing sample
would violate immutable-evidence and no-resampling rules.

## Decision

### Strict V4 boundary

The correction is a new source-bound protocol with distinct canonical `NACP4`, `NAEV4`, `NARS4`, and diagnostic-only
`NADV4` identities. V1/V2/V3 encoders, parsers, goldens, status ordinals, campaign bytes, and selection semantics stay
unchanged. V4 may reuse the already frozen V3 logical Cell, candidate, planner, and disposition algebra only inside an
explicit V4 envelope that binds the V4 execution profile and V4 plan digest. A V3 artifact is never a V4 campaign,
evaluation, diagnostic, selection, child, scenario, or Final input, even when its logical observations are
structurally compatible.

### Offer close and final admission deadline

V4 retains the exact 10-second warm-up, 30-second measured arrival schedule, every request ordinal/binding/trigger,
the 20-second 0.5R phase, the 10-second 2R storm, and the `4/64/256/1` admission tuple. It separates the terminal
boundaries as follows:

```text
offer close                         = warm-up + measurement = 40 seconds
terminal admission drain            = 2 seconds
final admission deadline            = offer close + 2 seconds
shared admitted-work cleanup        = final admission deadline + 5 seconds
```

At offer close no new request may be created, offered early, resampled, retried, or substituted. Only requests that
were already offered before that instant and remain in the existing bounded pre-admission queues may continue to
acquire their already-defined actor/global permits and per-binding ownership. The two-second bound is not a new
performance allowance: it is exactly the existing frozen `starvationMaximumMicros <= 2,000,000` limit. The unchanged
`queueAgeP99Micros <= 1,000,000` and bounded queue/outstanding caps continue to reject a candidate that accumulates
sustained overload.

At the final admission deadline, every still-undispatched measured request terminates exactly once as
`OVERLOAD_DROPPED_BEFORE_ADMISSION`. Every request dispatched before that deadline shares the one fixed five-second
cleanup deadline and terminates exactly once as `COMPLETED`, `FAILED_AFTER_ADMISSION`, or
`TIMED_OUT_AFTER_ADMISSION`. A late callback still cannot dispatch the next metadata operation. Qualification still
requires zero measured pre-admission drop, failure, timeout, incomplete request, duplicate/reused ID, queue residue,
in-flight residue, or waiter residue. The terminal drain changes neither a counter nor the threshold used to judge it;
it removes deterministic right-censoring before that judgment.

The warm-up-to-measurement barrier is unchanged. Warm-up work cannot enter the measured inventory, and its existing
typed failure/drop attribution remains separate.

### Source-bound profile, feasibility, and budget

The V4 execution profile includes protocol version, `4/64/256/1`, 40-second offer horizon, two-second terminal
admission drain, five-second shared cleanup, queue cap `2 * offeredRate`, exact V1 schedule digest, Native execution
profile, and the no-hidden-queue invariant. Plan-only and preflight must report and validate every field before output
or service construction. A profile with a single offer/admission cutoff is `TERMINAL_CENSORING_INFEASIBLE`; the V4
profile must reconstruct both deterministic same-binding tail examples above.

Logical inventory and physical-action maxima remain `328/360/32/720`. Each interval now charges 42 seconds to the
interval phase instead of 40; cleanup remains a separate five seconds. The maximum interval budget is therefore
13,776 seconds. Independent budgets become `900/5400/7200/5400/13776/1640/600`, whose 34,916-second sum leaves 13,084
seconds inside the unchanged 48,000-second hard envelope.

### Diagnostic and formal entry

A bounded runner-only proof must cover an on-time final offer blocked by the same binding at offer close, admission
during the two-second drain, zero-drop conservation, and an over-cap request that remains queued until it is dropped
at the final admission deadline. It must also prove that V3 with zero drain retains its exact prior behavior.

A real-Oxia V4 diagnostic executes the exact RANGE-16 fixed-1000 then derived-800 formal schedule through the same
candidate runtime used by formal execution. It requires all measured offers to terminate with zero drop/failure/
timeout, complete drain, global concurrency above four, and unchanged p99/queue/starvation bounds. Its output is
`diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`; it can seal only NADV4 and cannot select a
mode.

Formal execution is default-off and requires a new exact clean pushed source, create-new `<source>-r1` directory,
V4 plan/profile/NADV4 tuple, locked worktrees/JAR/image, and all current-source gates. A V4 formal result is handled by
the same fail-closed rules: infrastructure failure produces no evaluation; NONE/BOTH remain legal non-promotable;
only one deterministic STRICT or smallest RANGE result can seal NARS4 and unlock downstream source-lock, child,
scenario, and Final governance.

## Consequences

- Every historical V3 result remains truthful and permanently non-promotable.
- V4 retains zero drop and every product SLO while making the finite offered-load sample independently observable
  from its bounded terminal admission drain.
- A candidate cannot hide overload in the drain because queue p99/max, zero final drop, and fixed caps remain hard
  qualification inputs.
- No production allocator mode changes until a unique V4 NARS4 is independently verified and published through the
  governed source lock.
- This ADR alone is design/source input, not diagnostic evidence, formal authorization, selection, child freshness,
  scenario promotion, or M3 Final.

## Verification

Publication requires V3 parser/golden compatibility, V4 canonical round trips and cross-version rejection, synthetic
STRICT/RANGE/NONE/BOTH/baseline-unavailable evaluation paths, source/profile/budget/action mismatch rejection,
runner cutoff/drain/timeout/exactly-once conservation, formal/diagnostic runtime equivalence, focused and ordinary
module tests, Checkstyle/Spotless, `v2DocumentationCheck`, `v2M3SourceCheck`, and a clean fast-forward push.
