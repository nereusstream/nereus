# ADR 0143: V2 M3 allocator V5 frozen-target delivery amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADRs 0122, 0125, and 0137 for new exact-source V5 execution only
- Preserves: every prior V1/V2/V3/V4/V5 canonical byte and result; the frozen schedule, workload, candidates,
  rates, latency rows, zero-drop rule, SLOs, qualification thresholds, selection preference, bounded admission,
  action budgets, and M6 activation boundary

## Context

Exact clean source `af2f603962602084a58cbf8cc76b3836641a9aa4` passed the complete source and pre-campaign gates.
Its first complete V5 diagnostic then ran all 24 tests in ten isolated suites. The Native
100k/10ms/200-requests-per-second row offered 6,000 measured requests, admitted and completed 5,996, and marked the
last four as `PRE_ADMISSION_CUTOFF`. There was no admitted failure or timeout, no hidden dispatch queue, and no
lifecycle residue. Real ManagedLedger operation concurrency reached 19. The first dropped request was ordinal 7,996
with 14,838 microseconds of scheduler firing lag; queue depth reached only one.

The row therefore did not expose Native throughput exhaustion. All four frozen targets were before the 40-second
offer close, but their per-actor offer producer was physically descheduled until just after that boundary. The
existing 50-millisecond spin window cannot guarantee real-time CPU service on a loaded host. Treating this host
scheduling delay as allocator overload makes a fixed logical schedule depend on an unrelated OS scheduling sample.
Repeating the same source until the host happens to schedule the producer earlier would violate the no-luck evidence
rule.

The diagnostic failed closed before NADV5 sealing and before any formal campaign. Its immutable diagnostic-only
archive is
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/diagnostic-af2f6039-v5-native-terminal-cutoff-failed-r1`.
The 29-file/61,058-byte payload is byte-identical to the original diagnostic and JUnit inputs. Its archive-identity
SHA-256 is `a2715b4b433470c42e82c37337c0e7f81ec711c2ba87aca578877ee9a46aea19`, its manifest SHA-256 is
`d2d704ec48b5fe956f11c1a083820a5815d8fcfdc5eeb6df674f1e1c62bd873c`, and its JUnit inventory is
24 tests, one failure, zero errors, zero skipped, ten suites. It is not formal evidence and cannot authorize a
campaign, evaluation, selection, source lock, child, scenario, or Final.

## Decision

1. V3 and V4 retain their exact physical-delivery rule. At or after offer close, a request that has not reached the
   pre-admission queue remains a pre-admission drop under those protocols. Their runners, wires, goldens, and prior
   evidence are unchanged.
2. For V5 at a new exact source, the frozen schedule's request ordinal and target offset are the logical offer
   authority. Every request is still created exactly once from the prevalidated schedule, and no target, jitter,
   binding, trigger, payload, phase, or inventory byte changes.
3. A V5 offer whose frozen target is strictly before offer close may be physically delivered to its existing bounded
   per-actor pre-admission queue until the unchanged final admission deadline. It may be admitted immediately when
   the unchanged actor/global/binding permits are free. Its physical scheduler lag remains measured and visible.
4. At or after the final admission deadline, an undelivered or queued request still terminates exactly once as
   `OVERLOAD_DROPPED_BEFORE_ADMISSION`. A full queue still drops. Dispatched work retains the same shared cleanup
   deadline and the same completed/failed/timed-out terminal partition. This amendment adds no drain time, retry,
   queue slot, outstanding permit, shared Java Cell lock, or hidden executor.
5. Formal Native and candidate execution and the diagnostic canary use the same V5 runner factory. Contract tests
   deterministically delay one per-actor producer beyond offer close and prove V5 admits the frozen target during the
   existing drain, V4 retains its previous drop, and V5 still drops when physical delivery misses the final admission
   deadline.
6. The V5 execution profile now binds
   `scheduledOfferAuthority=FROZEN_TARGET_OFFSET` and
   `scheduledOfferDeliveryDeadline=FINAL_ADMISSION_DEADLINE`. The resulting execution-profile SHA-256 is
   `0bfa9670b8e3b1721ab83f03bd34ed368814e914288a5af772d17dec67ee3449`; the resulting zero-decision plan SHA-256 is
   `974857cab839ba9cfd02ad8694a51976cf0279a4f61d11fe767aef5518a72dea`. The logical and physical action inventories
   remain 328 interval, 360 fault, 32 scale, and 720 total actions inside the unchanged 48,000-second hard cap.
7. Earlier V5 profile/plan digests and artifacts remain parseable and immutable. No old NADV5, NACP5, NAEV5, or
   NARS5 is rebound to the new profile. A formal campaign requires a fresh passed diagnostic, exact clean pushed
   source, new executor digest, and create-new output directory.

## Consequences

The evidence runner no longer converts bounded host scheduling latency between a frozen logical target and physical
producer execution into allocator overload when the already-governed terminal admission drain can still accept the
request. Zero drop remains a qualification requirement; real queue, permit, workflow, or Oxia overload still fails
that requirement. The old `af2f6039...` diagnostic remains a failed, non-authoritative observation.

This decision alone does not authorize or produce formal evidence. Publication requires the V3/V4 compatibility
contracts, V5 runner/profile/plan goldens, complete diagnostic/NADV5 validation, Checkstyle, documentation and source
closure, a clean fast-forward push, and then a separate fresh exact-source formal campaign under the existing goal.
