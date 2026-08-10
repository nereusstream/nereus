# ADR 0078: V2 per-source retirement interval and batch retirement

## Status

Accepted for the 0.2 `OBJECT_WAL` transition-exact immutable batch, per-source first epoch and release interval,
deterministic batch identity, selector-only activation, backend inline/reference boundary, explicit O(N) control cost,
independent protection release, rejection of mutable batch completion state, and mandatory derived batch retirement.
ADR 0080 selects the irreversible same-key `FULL_V1 -> RETIRED_V1` compact transition and permanent 0.2 tombstone;
exact source-protection/tombstone wire and numeric caps remain M4/M5 work. Tombstone deletion remains evidence-blocked;
implementation has not started at M0.

## Context

Using one batch-wide minimum first epoch as every member's release interval is safe but creates avoidable head-of-line
retention: an old proof hole can block a source introduced much later. Claiming that immutable batching eliminates
per-source work is also inaccurate. The design removes mutable batch-progress authority, but each exact protection
generation still needs its own release CAS and recovery must inspect authoritative member state.

## Decision

Each immutable source/protection row stores its own:

```text
firstFallbackCapableReadAdmissionEpoch = first_i
```

The identity inherits `first_i` across every fallback-bearing view in which membership is unchanged. One selector
transition removing N exact identities freezes one shared
`lastFallbackCapableReadAdmissionEpoch = sharedLast`. Source i may release only after continuous valid proof coverage
of its own closed interval `[first_i, sharedLast]` plus all exact pin, protection, capability, and source conditions.
The batch may store `min(first_i)` as a bounded summary, but that summary neither authorizes nor blocks an individual
source release. A proof gap before source i's `first_i` cannot retain i.

`SourceRetirementBatch` remains the exact immutable eligibility envelope activated by one selector transition. Its
canonical identity binds Binding/incarnation, old and successor selector/view identities, transition digest, sorted
exact source/protection row identities, every row's immutable `first_i` binding, `fallbackSetSha256`, `sharedLast`,
capability-evidence generation/digest, and format version. `SourceRetirementBatchId` is a domain-separated digest of
those bytes and contains no random value or host-local time. The admitted fallback view must fit one bounded batch;
0.2 does not add partial fallback-removal views to split an oversized transition.

The selector CAS is the sole activation point, with two admitted backend mappings:

- **inline**: the complete bounded batch is carried by the selector transition; control cost is one selector CAS;
- **reference**: one immutable batch create precedes one selector CAS, but is admitted only when the selector's backend
  transaction atomically proves that the exact immutable key/SHA exists while committing the selector. A cross-key
  pre-read followed by an independent CAS is not equivalent. A backend without this transaction must inline.

An unselected precreated batch is inert residue and consumes bounded count/bytes/age capacity. Create/select response
unknown converges only by exact canonical key/value/SHA and selector reread. A missing, mutable, mismatched, or
unverified referenced batch cannot be selected.

For N source/protection identities, the worst-case control work is explicit:

```text
inline activation:     1 selector CAS
reference activation:  1 immutable batch create + 1 selector CAS
protection release:    at most N independent release CAS operations
recovery/reconcile:    bounded O(N) authoritative protection-state scan
```

Each source-protection release is irreversible, idempotent, exact-generation fenced, capability-bound, and resolves an
unknown response by exact reread. One quarantined or retained source does not block an eligible sibling's release, but
it does block complete batch retirement and continues consuming batch/source count, bytes, and age admission. This is
real batch-level capacity impact, not source-level correctness coupling.

The batch never gains a mutable released bitmap, remaining count, progress row, or completion CAS. Completion is
derived from authoritative source-protection states. Once every member is `RELEASED` or otherwise proven retired and
no selector, lineage, recovery, or response-loss path references the full batch, the full record must become
retirable through ADR 0080's exact-version same-key `FULL_V1 -> RETIRED_V1` CAS. 0.2 retains the compact tombstone and
does not admit direct reclaim, a retired-through frontier, or age-based deletion. That operation compresses metadata
only. It cannot release source protection, authorize physical GC, repair a missing member release, or reinterpret a
quarantined source.

Selected/unselected batch count, full bytes, compact-retired bytes, age, source rows, quarantine, and bounded O(N)
scan work have Cell/Binding hard caps. Pressure may backpressure handoff/read admission and retain bytes; it cannot
rewrite batch membership, substitute the batch minimum for `first_i`, release protection, or claim retirement.

Membership, per-source interval, selector activation, release authority, and retirement prerequisites are non-
disableable correctness contracts. Topic policy cannot select inline/reference mode, enlarge caps, skip a member CAS,
or retain mutable progress. Configuration is limited to Cell/Binding hard caps, reconciler cadence/concurrency, and
evidence-derived capacity parameters.

## Consequences

- `V2-OPEN-READ-13` is resolved without batch-wide proof-hole head-of-line blocking for later sources or a mutable
  batch completion authority.
- The design accurately exposes N release CAS operations and bounded O(N) recovery work. Independent release reduces
  source retention, while a quarantined source still delays batch metadata retirement and consumes capacity.
- M4/M5 must prove every `first_i/sharedLast` boundary, batch-min non-authority, inline/reference atomicity, unknown
  create/select/release outcomes, N-member partial progress, sibling independence, quarantine budget impact, exact
  derived-completion scan, every full-batch retirement prerequisite, and that batch compaction never grants source GC.

This decision is refined by ADR 0080, refines ADRs 0069, 0071, 0073, 0074, 0075, and 0077 and is tracked by
`T-MANIFEST-01`, `T-HANDOFF-01`, `V2-READ-006/008/010/013/015`, and `V2-OPEN-READ-15`.
