# ADR 0148: V2 M5 lifecycle contract amendment

## Status

Accepted on 2026-09-07 for implementation under `M5-AMENDMENT-3-LIFECYCLE-V2`.
The review baseline is `f05037bb16017f24e8147e99a61f26e539ed85fa`.
This decision authorizes the revised implementation work, not runtime deletion, scenario promotion, Final,
staging certification, or production deployment. Those require executable source-bound evidence.

## Context

Amendment 2 hashes changing eligibility into a supposedly permanent physical identity. Amendment 1 permanently
retains retired slots inside a bounded selector. Original M5-D applies logical expiry to replacement reclamation,
and M5-B offers only Object output although Kafka BK_ONLY, including both admitted internal topics, must work
without Object. The existing foundation and in-memory CAS coordinator do not resolve these integration gaps.

## Decision

Adopt the [lifecycle amendment](../v2/detailed_design/m5/m5-lifecycle-contract-amendment.md) and its exact
supersession table. It specifies:

1. A typed, canonical stable `PhysicalResourceIdV2`; revisioned `DeleteEligibilitySnapshotV2` is authority value
   state and never part of the key. Physical namespace identity is independent of owner, Binding, endpoint aliases,
   credentials and capability refresh.
2. Explicit replacement, logical-expiry and unpublished-output branches, with complete semantic coverage and all
   applicable M4/reference/native-authority protections.
3. Shared Kafka compaction semantics with separate sealed BookKeeper and Object carriers, full indexes and
   recovery. The two admitted internal topics remain BK_ONLY.
4. Bounded active selector state and authenticated immutable retirement history. One selector CAS publishes the
   history root and removes terminal slots; no tombstone deletion or BatchId reuse is authorized.
5. Target-relevant writer enrollment, multi-target ordering and recoverable READ_FENCED ownership before dispatch
   integration. Normal append and local read pins keep their accepted hot-path contracts.
6. Explicit M5 acceptance obligations and M6/M7/M8 interfaces, including every review 7/8 fault and capacity concern.

## Rejected alternatives and consequences

Opaque caller-provided identity bytes, trim-check removal without semantic substitution, Object-only internal-topic
compaction, larger lifetime slot caps, deletion of old tombstones, and ticket enum completeness as integration
evidence are rejected. History requires durable metadata proportional to completed history, but active selector
bytes and per-operation proofs are bounded. Historical storage capacity must be measured and admitted separately.
Permanent M5-D keys retain minimal done records; bounded caches may evict them without deleting durable authority.

The original freeze, amendments 1/2, source locks and receipts keep their exact bytes and historical meanings.
Current entry points select the successor clauses explicitly. Earlier gates remain scoped historical results;
revised source-bound children and aggregate are still required. M4 RELEASED and native Pulsar authority remain
mandatory where applicable. No original M3/M4 implementation, qualification or source lock is rewritten here.
