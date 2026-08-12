# Kafka replica-lag, protocol-checkpoint Head, and M1 closeout

## Status

The user confirmed this final documentation refinement on 2026-08-12 after reviewing Nereus
`d8a3a829492990a7fb8e11d5b8862c0c4815a608`. ADRs 0086/0087 remain accepted; no architecture rewrite or new large ADR
is required. This note preserves the two implementation contracts added before M2 code and the exact M1 validation
boundary. It does not claim implementation or executable evidence.

## Observed/Applied eligibility is hard-bounded

`replicaAppliedEndOffset <= replicaObservedEndOffset` remains valid, but Observed is eligible for native ISR/HW only
when the observation journal is durable through Observed, offset/byte/age apply lag stays within hard bounds, and one
verifiable source covers the entire unapplied interval. Before a bound is crossed, the implementation stops Observed,
removes the replica from ISR/HW eligibility, or backpressures the leader. An indefinitely unapplied follower may not
remain eligible merely because descriptors are cheap to validate.

The original BookKeeper source need not remain forever. A replacement generation is acceptable only with identical
Kafka coverage/content and compatible producer, transaction, leader-epoch, and checkpoint proof. Original-source
protection cannot drain before that replacement covers the unapplied interval. Observation-journal loss, corruption,
truncation, or disk loss rolls eligible Observed back to the highest contiguous surviving journal/Applied proof and
requires bounded catch-up. Numeric thresholds remain M2/M6 evidence rather than prose constants.

## NWKCP1 selection and terminal lifecycle

Object WAL uses one low-frequency, Root-bound `KafkaProtocolCheckpointHeadV1`, separate from physical extent
checkpoint/Seal authority. The Head binds publisher epoch, `OPEN|TERMINAL`, ordinal, predecessor digest, exact NWKCP1
key/length/digest, and covered-through vector. Publishing is conditional-create plus full Object verification followed
by an exact-predecessor Head CAS. Response loss uses exact reread; ordinal/fork/regression/publisher mismatches fail
closed.

After admission stops and the final compatible checkpoint exists, an irreversible same-Head `OPEN -> TERMINAL` CAS
is the protocol-closure fact. A successor Root binds that terminal Head independently from the predecessor physical
Root/Seal. Checkpoint/head deletion waits for the whole run's successor, manifest, recovery, retention, and source
dependencies; it cannot precede the WAL/source required for replay. This authority never grants ACK, skips physical
recovery, releases source protection, or authorizes source GC. Exact wire, key, vector caps, and backend mapping remain
M3 evidence.

## M1 status and validation boundary

- M1.1a is complete and M1.1b is exact-locally complete.
- M1.1b-Q1 readiness evidence is historical decision input and is not rerun for promotion.
- Foundation/NTA1 local tests remain ordinary regression CI.
- K1/P1/R1 functional tests, real-Oxia exact-source conformance, pure-V2 graph/V1 absence, N2 exact source/artifact
  checks, `v2M1FinalCheck`, and evidence-only N3 promotion remain required.
- Final aggregates referenced outcomes; it does not repeat the suites.
- Version 0.2 is fresh-deployment-only. M1 tests the pure-input name-inventory tool and rejection boundary; executing it
  against a real existing customer cluster is deferred migration evidence.
- 10k/100k scale, AutoMQ comparison, and chaos are M2/M3/M7/M8 evidence, not M1 promotion work.

The remaining M1 execution order is Registry writer-count evidence, receipt/parser caps, immutable N1 domain artifact,
K1/P1/R1, fast/exact/final gates, active-graph edge cut, independent mechanical V1/KoP-runtime deletion, N2 execution,
and receipt-only N3. K1/P1/R1 may proceed in parallel only after the immutable N1 artifact is fixed. M2 runtime is not
activated on main before M1 Final; only isolated state-machine, wire-design, and harness preparation may overlap.

## M2 implementation consequence

M2 first closes `BOOKKEEPER_WAL_ONLY`: partition state/fenced publication, exact NBKE2, run lifecycle, capacity-before-
offset admission and pipelined I/O, producer/transaction/leader-epoch state, RangeIndex reads, checkpoint/recovery,
compact descriptor plus observation journal/apply kernel, then real BookKeeper fault/scale evidence and `v2M2Check`.
The async-Object seam follows; M3 owns NWG1/NWKCP1 Object WAL. No prose decision here selects index/checkpoint cadence,
pipeline depth, rollover, handle-cache, latency, or scale thresholds.
