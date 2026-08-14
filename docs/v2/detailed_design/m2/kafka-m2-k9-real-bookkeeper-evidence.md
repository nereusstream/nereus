---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeImplementationSlice
sourceTuple: v2-m1
---

# M2-K9 real BookKeeper fault and scale evidence

K9 admits operational defaults only after the committed plan is executed against the exact K0 BookKeeper image and
configuration. The plan is
[`bookkeeper-scale-plan.properties`](../../../../config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties). Its byte
digest, the exact conformance-configuration digest, the tested Nereus commit, the BookKeeper client/server source and
image digests, each attachment length/digest, and the two tier results must be bound by the later K9 receipt. No result
may edit the persisted NBKE2 v1 bytes or enlarge a K0 hard cap.

The real provider and engine calibration already exercises quorum append/read, fresh-session open, fence/recovery,
old-writer rejection, response-loss reread, sealed absence, ordered two-batch publication, exact-entry Fetch,
checkpoint/footer close, and election-bounded takeover. Those calibration tests are prerequisites, not scale evidence
or a scenario receipt.

## Predeclared scale workload

Both the 10,000- and 100,000-partition tiers use a fresh three-bookie cluster from the exact digest-pinned image. Every
logical partition creates one real BookKeeper ledger with the admitted custom identity metadata and writes one
512-byte entry. Closed/cold ledgers are not pooled. The client keeps exactly 1,024 hot ledgers open, with at most 64
concurrent I/O operations. A fixed hot sample adds a 32-entry tail, performs targeted entry reads and fence/recovery,
then rolls 128 partitions to distinct successor ledgers. Successful exact provider proofs, not an extrapolation from a
smaller tier, supply the partition, ledger, append, metadata-mutation, handle, recovery, and rollover counts.

The harness samples its heap, direct buffers, and open file descriptors. The runner separately records Docker memory,
BookKeeper/ZooKeeper volume bytes, container identities, and logs. Create/append/close/read/recovery p50/p99/max,
overall elapsed time, throughput, entry/byte totals, and an ordered handle digest are emitted. Any non-exact provider
outcome, count mismatch, unexpected source/configuration digest, or predeclared threshold breach fails the run.

The source-only `v2M2KafkaK9PlanCheck` compiles the harness and verifies these preconditions. It intentionally contains
no formal scale output, selected default, scenario promotion, Kafka Final, or global M2 PASS. Formal evidence must be a
later evidence-only descendant of the clean tested commit so thresholds cannot be adjusted after observing results.

## Candidate defaults and promotion boundary

The plan predeclares the complete candidate set required by K0: checkpoint cadence; block and active-tail budgets;
entry/byte/time recovery envelope; partition/global pipeline limits; byte/entry/age rollover; handle/open admission;
Observed/Applied and journal bounds; waiter admission; and cursor coalescing. These remain candidates until both scale
tiers and the real fault matrix pass on the exact source tuple. Topic policy may only lower selected bounds, and
Cell/host pressure may backpressure, close handles, or roll earlier.

K9 may support the Kafka-owned M2 scenario receipt but does not itself promote a scenario. K10 owns scenario promotion
and `v2M2KafkaFinalCheck`. Kafka wire/runtime integration, native ISR/HW/election activation, Object WAL, and global M2
remain outside K9.

`KafkaBookKeeperOperationalDefaultsV1` is the production selection surface. Its independent canonical projection is
[`kafka-bookkeeper-m2-k9-selected-defaults-v1.json`](../../wire/kafka-bookkeeper-m2-k9-selected-defaults-v1.json).
It projects directly to the K0 recovery envelope, partition/global pipeline budgets, replica eligibility, and journal
bounds. The complete record constructor enforces cross-budget coverage, and `loweredBy` rejects any component-wise
enlargement by a Topic, Cell, or host override. The implementation and its six-test gate do not by themselves replace
the still-required current-source real-fault and two-tier scale receipt.
