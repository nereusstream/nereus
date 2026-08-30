# ADR 0141: V2 M3 Final common-tested-source recertification amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADR 0140's selected-source freshness sequence and ADR 0105's post-selection evidence transition
- Preserves: V5 workload, SLO, qualification, selection, plan, execution profile, and wire bytes; the selected
  production mode; every immutable diagnostic/formal attempt; all child kinds; and the M3 Final evidence-only
  descendant rule

## Context

ADR 0140 authorized the production source lock to move to `allocatorMode=RANGE` after exact
`d5b3569b7e09cb271067ba2955da9511977df9df` produced canonical `RANGE_SELECTED(RANGE_64)` evidence. The selected
source was then published as exact `54d0ca7c329248acb3eaaaef9d4bffd138dad061` and independently recertified rather
than reusing the d5 campaign.

The complete current-source V5 diagnostic at that source sealed canonical NADV5
`ee7f32b99ca6966ff9f6af2580f4c0d2616257e28cf300d5b26fd6394784ee06` from ten JUnit suites, 24 tests, and
nineteen diagnostic raw observations with zero failure, error, or skip. Its immutable external archive identity is
`0c26cd7daacab4fe7e56b36faa112352a006e7224384b8e631ddec816108eeeb`.

The subsequent bounded-adaptive campaign completed with 32 records, 33 checkpoints, 123 physical actions, and 306
validator-reconstructed dispositions. Final NACP5 is
`2d7aaaa2c8a9e834912d6766a1e8fb8b6245ef7b7886f6d3448bf4acc90c8526`, attachment root is
`1bc0bed3bdc9202532f4b7a2564f376c99cf0091d2cd8016e43adad6ca9f2a25`, canonical NAEV5 is
`10e333aec67ba56dd32b316b24fa534cb2a1dfbe9520719da299e2609aefbfae`, the promotion decision is
`d964de6216f91b1f2bbc7d1bd7666209cae1d712cec749bc012214103f3fea62`, and canonical NARS5 is
`a59aa566ab5c54699dba250db45cf86fad8f3251255988315f66e79db47305cc`. RANGE-64 is the sole qualified candidate.
The immutable 162-file formal archive identity is
`868947bf872a95a3a3e16ca48f81a2631954efe4d34a1d6410d2647ef926f7da`.

The governed allocator child was published at descendant `6133ab882a3f4a69b2b27a526b9c567e9e947b18`. A complete
current-source M2 regression subsequently passed 25 children and 688 tests with zero failure, error, or skip, and was
published at descendant `b6116a1a0a5c60d76f1ea53cc467123e082eac18`. Both are valid immutable intermediate
receipts. They do not satisfy M3 Final because the remaining accepted-contract synchronization in this amendment and
its implementation record is a non-evidence source change, and Final requires one common tested source for W1, all
ten children, every promoted scenario, and the aggregate.

## Decision

The exact clean commit containing this accepted amendment and its synchronized non-evidence documentation becomes
the next common-tested-source candidate. Before M3 Final is published, that exact source must independently produce:

1. a complete V5 diagnostic and a fresh formal campaign in new immutable directories, with canonical NADV5, NAEV5,
   promotion decision, and NARS5 that still uniquely select an allocator under the unchanged contract;
2. a complete current-source M2 regression receipt;
3. all ten governed M3 child receipts, including a newly sealed `ALLOCATOR_SELECTION` child derived only from the
   common source's own diagnostic/formal artifacts; and
4. all allowlisted scenario executions/promotions plus the canonical M3 Final receipt.

The `54d0ca7c...` diagnostic/formal, `6133ab88...` allocator child, and `b6116a1a...` W1 receipt remain permanent and
valid at their own exact sources. They are not copied, resealed, rebound, or used as authority inputs for the common
source. A later negative or infrastructure-invalid campaign remains immutable and cannot be replaced by another run
of the same exact source merely to obtain a preferred result.

After the common tested source is frozen, every descendant through Final must satisfy the existing Final checker's
evidence-only path allowlist and linear-history rule. No production source, accepted ADR, implementation record,
protocol, test, build wiring, general documentation, or source lock may change in those descendants. The explicitly
allowlisted M3 status/index documents and scenario manifest may change only as part of the source-bound evidence
publication chain. Every change is committed as a reviewable, non-merge slice and safely fast-forward pushed to
`origin/main`; force push is forbidden.

## Consequences

- Selected-source recertification at `54d0ca7c...` is closed and immutable; the remaining work is common-source
  freshness and aggregate publication, not allocator product choice.
- `allocatorMode=RANGE` and canonical RANGE-64 selection remain unchanged. This amendment does not create V6 or alter
  any V5 plan, threshold, workload, budget, disposition, evidence, or selection semantics.
- The published allocator child and W1 receipt are retained as audit checkpoints but cannot satisfy the later common
  tested-source equality checks.
- M3 is still `InProgress` and no scenario or Final is promoted until the fresh common-source chain validates in full.
