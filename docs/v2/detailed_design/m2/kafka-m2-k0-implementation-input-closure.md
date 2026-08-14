---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M2-K0 Kafka implementation-input closure

## Delivery boundary

This slice translates the accepted semantics in
[ADR 0086](../../../decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md) and
[ADR 0087](../../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) into implementation inputs.
It does not reopen Kafka Offset, BookKeeper run, RangeIndex, Allocated/Durable/LEO/HW/LSO, transaction, recovery, or
Observed/Applied semantics. It closes five narrower subjects before the first storage writer is admitted:

1. exact BookKeeper carrier and local durable-journal wire;
2. checked numeric and allocation boundaries;
3. the minimum M2 module and artifact graph;
4. the Cell-scoped BookKeeper provider contract;
5. source-qualified evidence, receipt, and gate structure.

Acceptance of this design originally recorded the execution contract only. M2-K0 is now `InProgress`: K0-M has a
current immutable-input receipt and K0-P has its production provider/lifecycle surface plus non-zero local gate. The
aggregate remains incomplete until production codecs, immutable wire goldens, numeric admission, receipt parsing, and
`v2M2KafkaInputsCheck` land. No partial cut promotes a scenario or proves a Kafka runtime.

M2-K0 is Kafka-only. Pulsar NPD1/NPO1, `LedgerOffloader`, `DualSourceReadHandle`, and `BK_DELETE_*` remain separate
global-M2 work. Object-WAL `NWKCP1` bytes and its Head/key limits remain M3 work. Native Kafka transport and
`UnifiedLog`/`Partition`/`ReplicaManager`/purgatory activation remain M6 work.

## Closure matrix

| Subject | M2-K0 must freeze | May remain evidence-selected | Must not be claimed |
| --- | --- | --- | --- |
| wire | complete `NBKE2` type/field/length/checksum table; append-group terminal representation; packed locator representation; BookKeeper checkpoint carrier; K8 descriptor/journal wire process | encoding performance may select among already versioned candidates only before their first persisted write | `NWKCP1` Object bytes or Kafka replica-Fetch protocol activation |
| numeric | persisted decoder caps; allocation formulas; integer domains; provider/admission-cap formula; hard recovery dimensions | checkpoint cadence, pipeline depth, rollover, handle cache, apply-lag, waiter/cursor defaults and performance thresholds | a benchmark candidate as a format constant |
| modules | exact dependency graph, forbidden dependencies, immutable M1 input, source-qualified M2 publication | a testkit module only after a second production consumer proves reuse | republishing or modifying an M1 N1/P1 artifact as M2 |
| provider | Cell scope/session, capability snapshot, ownership, closed outcomes, response-unknown reconciliation, drain/close | compatible lower-level transport pooling after isolation evidence | one cross-Cell stateful singleton or per-append control metadata |
| evidence | gate layers, receipt kinds, source-lock fields, non-zero/zero-skip policy, Kafka/global-M2 boundary | performance defaults and scale thresholds selected by the dedicated receipt | M2/M3/M4/M5/M6 scenario completion from an engine-only receipt |

## Wire closure

### `NBKE2` package

M2-K0 owns one exact big-endian, closed-version package for these BookKeeper entries:

```text
RUN_HEADER
DATA
RANGE_INDEX_BLOCK
PROTOCOL_CHECKPOINT
RUN_FOOTER
```

The package must freeze, in one table checked by production tests:

- magic, major/minor version, frame-type code, legal flags, reserved-zero bytes, fixed/variable header lengths, and
  total-length domain;
- run identity and exact Binding/Topic/partition/Storage Epoch/creator Owner Epoch/Kafka leader epoch/Provider Scope /
  ledger binding rules;
- BookKeeper entry-ID cross-check and all signed/unsigned conversion rules;
- DATA raw assigned-RecordBatch length, coverage, member ordinal/count, storage-attempt identity, and terminal
  append-group descriptor placement;
- range-index anchor, locator row, predecessor/successor, covered-through, and physical-entry bounds;
- the compatible range/producer/transaction/leader-epoch checkpoint vector and component bounds;
- footer terminal logical/physical bounds, ordered index directory, qualified recovery/seal fence, and root domain;
- CRC32C/v1 and SHA-256/v1 field positions and exact digest domains, including how each checksum field is excluded or
  normalized while its own value is computed;
- canonical encoding, strict EOF, unknown-major/type/flag rejection, unknown-minor policy, duplicate/order checks, and
  truncation/trailing-byte behavior.

The initial DATA representation remains one complete raw broker-assigned Kafka RecordBatch per BookKeeper entry. K0
must select one terminal append-group representation before K2 implementation; it may not leave both “last DATA” and
“extra control entry” as runtime alternatives. Index/checkpoint/footer controls pass through the same entry sequencer
and may occur only between complete append groups.

`NBKE2` is not a TLV framework and has no unknown-field bag. A future incompatible extension uses a new accepted
major/version contract. A minor version may be accepted only when the current decoder has an explicit closed rule for
every added byte; “ignore the tail” is not such a rule.

### Machine projection and goldens

The implementation change must carry one machine-readable projection of the frozen wire table and immutable vectors
for minimum, representative, and maximum legal frames. Production constants and the projection are compared by a
test; neither is generated from the other during the gate. Negative vectors cover every one-before/at/one-after length,
count, offset, entry ID, epoch, ordinal, type, flag, CRC, SHA, ordering, truncation, overflow, and EOF boundary.

The Kafka fork remains the authority for native RecordBatch parsing, CRC/error precedence, producer/transaction deltas,
and duplicate results. A narrow exact-source adapter supplies those facts during append and recovery. Nereus stores,
orders, checks the adapter's exact assigned-byte facts, and rejects mismatch; it does not implement a second
`ProducerStateManager` or payload-based duplicate protocol.

K8's compact replica descriptor and observation journal are Nereus-local durable wire, not a new Kafka request schema.
Their exact bytes may land as a later K8 child of this closure process, but they must use the same closed table, parser
cap, immutable-vector, source-lock, and no-unknown-tail rules before the first durable journal write. M6 separately owns
the native replica-Fetch transport mapping.

## Numeric boundary model

M2 uses five non-interchangeable numeric classes:

1. **format hard cap**: a constant of the persisted major version; every historical decoder continues to accept the
   complete legal v1 domain;
2. **provider capability cap**: an immutable snapshot of the admitted Cell session and exact client/server/config tuple;
3. **new-write admission cap**: the checked minimum of the format, provider, and Kafka-native request/batch bounds;
4. **operational budget**: partition/Cell/host memory, count, byte, and time ceilings that may lower admission,
   backpressure, checkpoint, or seal early but never reinterpret stored bytes;
5. **evidence threshold/default**: a value selected by a source-qualified fault/scale receipt and then persisted at the
   run or session boundary when failover interpretation depends on it.

For a DATA write, the admission calculation is structurally:

```text
effectiveMaxDataFrameBytes = min(
    NBKE2_DATA_V1_FORMAT_MAX_BYTES,
    admittedBookKeeperMaxAddPayloadBytes,
    admittedKafkaCompleteRecordBatchBytes + exactNBKE2DataOverhead
)
```

The implementation must derive the exact BookKeeper payload allowance from the source-locked protocol/configuration,
not copy the upstream default `nettyMaxFrameSizeBytes` into the Nereus format. The currently build-selected BookKeeper
4.18.0 source default is 5 MiB, but M2 has not yet source-locked the server/configuration tuple, a deployment may
configure another value, and BookKeeper protocol/digest framing consumes bytes. K0 therefore freezes the formula and
the format ceiling; the provider capability snapshot supplies the exact admitted payload value. A complete Kafka
RecordBatch that does not fit is rejected before offset allocation.

Every decoded count/length first enters a non-negative wide domain, is checked against its field-specific cap, and only
then converts to a Java array/index type. All additions and multiplications use checked arithmetic. Unsigned 64-bit
values greater than `Long.MAX_VALUE`, negative Kafka offsets/BookKeeper entry IDs, zero or overflowing member counts,
and `baseOffset + lastOffsetDelta + 1` overflow fail before allocation. Decoder allocation follows validated actual
count/length, never the persisted maximum.

Recovery is always bounded simultaneously by entries, encoded bytes, and elapsed time. K0 freezes those three mandatory
dimensions and the lowering hierarchy. K9 evidence selects their exact defaults together with checkpoint cadence,
pipeline depth, active-tail/index budgets, run rollover, handle-cache admission, Observed/Applied lag, waiter count, and
cursor coalescing. A Topic may not enlarge any hard or evidence-selected safety bound; Cell/host pressure may only lower
or trigger earlier backpressure/seal.

## Minimum module and artifact graph

The initial Kafka M2 graph is intentionally small:

```text
exact immutable nereus-domain N1 artifact
                    |
                    v
          nereus-storage-api
                    |
          +---------+------------------+
          |                            |
          v                            v
nereus-storage-bookkeeper    nereus-kafka-bookkeeper
          |                            ^
          +----------------------------+
```

The responsibilities are closed as follows:

- `nereus-storage-api` contains typed protocol coverage/physical extent/frontier/run identities, checked ranges, the
  narrow provider/session/capability contracts, and `KafkaRunRootAuthority`. Its only production dependency is the
  exact immutable N1 domain artifact; it imports no BookKeeper, Kafka, Pulsar, Oxia, or Object SDK type.
- `nereus-storage-bookkeeper` contains the BookKeeper client adapter, ledger open/create/fence/recovery, explicit entry
  sequencing, buffer ownership, provider capability admission, and Cell-session lifecycle. It owns no Kafka offset,
  producer, transaction, ISR, or HW decision.
- `nereus-kafka-bookkeeper` contains `NBKE2`, Kafka run/index/checkpoint/read/publication primitives and the narrow
  Kafka-native semantics adapter boundary. It depends on the two modules above and imports no Oxia metadata facade.

K0 intentionally does not pre-split `nereus-storage-api` into storage-domain and storage-SPI artifacts. A later split
requires an actual independent consumer or dependency-cycle proof and must preserve the same Java-17/JDK-only API
surface. This keeps module count from becoming an architecture claim by itself.

Deterministic scheduler, fake BookKeeper, response-loss/corruption injectors, and resource probes begin as test fixtures
owned by these modules. `nereus-storage-testkit` becomes a published module only when another production implementation
actually consumes the same stable surface; K0 does not create it pre-emptively. Object-store and Pulsar-offload modules
are outside this Kafka slice.

M2 must not modify or republish the frozen N1/P1 artifacts. The three new modules publish only through a filtered
source-qualified `0.2.0-m2.<40-lowercase-source-sha>` bundle whose POM/Gradle metadata points to the exact N1 coordinate.
The existing M1 BOM is not rewritten to pretend it was an M2 artifact. An M2 bundle manifest binds every binary/source
JAR/POM/Gradle-metadata byte length and SHA-256, and publication refuses an existing source-SHA destination.
K0-M adds this version grammar and filtered publication atomically with the first modules; the current build does not
already authorize an M2 coordinate.

## BookKeeper provider contract

The minimum runtime boundary is one `BookKeeperCellSession` per Cell Provider Scope. Physical BookKeeper infrastructure
and a compatible stateless SDK transport pool may be shared, but a session exclusively owns:

- namespace/provider-scope identity and credential/KMS reference version;
- admitted capability snapshot and ledger configuration;
- permits, in-flight entries/bytes, retry/circuit-breaker state, handle cache, metrics, drain, and close;
- all run handles and response-unknown reconciliation started through that session.

Closing, throttling, fencing, rotating credentials, or exhausting one session cannot close another Cell session. The
shared transport owns no Kafka position, run root, checkpoint, cache authority, task, retention, or deletion decision.

The immutable capability snapshot binds at least the exact provider/client source and artifact identity, protocol mode,
client and server frame limits, derived maximum add payload, explicit-entry-ID capability, ensemble/write/ack quorum,
digest type, fencing/recovery support, timeout class, credential identity version, and configuration digest. Missing,
inconsistent, dynamically enlarged, or unverifiable capability fails profile admission before a run opens.

The production session surface is closed to these operation families:

| Operation | Required result authority |
| --- | --- |
| `createRunLedger` | exact new ledger identity/configuration or a closed mutation outcome; never run-root publication |
| `openRunLedger` | exact existing ledger/configuration match; mismatch/fenced/absent remain distinct |
| `appendExplicitEntry` | expected ledger and entry ID plus the four-state mutation outcome below |
| `readExactEntry` | exact entry bytes/identity, definitive absence, or a typed fence/provider failure |
| `fenceAndRecoverRunLedger` | exact last-add-confirmed/recovery/fence result bound to the opened ledger generation |
| `closeRunLedger` | exact close result or response-unknown reconciliation; not Kafka run-seal authority by itself |
| `drain` / `close` | completion only after the session owns no accepted operation, buffer, permit, or resolver |

It exposes no generic metadata CRUD, namespace scan, per-append run-root mutation, Kafka frontier, or retention/delete
decision. Run-root creation/seal/successor publication stays behind `KafkaRunRootAuthority`.

Provider write completion is a closed result, not a boolean:

```text
APPLIED_EXACT
DEFINITIVELY_NOT_APPLIED
OUTCOME_UNKNOWN
FENCED_OR_CONFLICT
```

`APPLIED_EXACT` includes the expected ledger/entry identity and quorum proof. `DEFINITIVELY_NOT_APPLIED` is legal only
when the exact provider operation proves absence. `OUTCOME_UNKNOWN` blocks the ordered queue and requires an exact
ledger/entry reread plus `NBKE2` identity/length/digest comparison or run fencing/recovery; it is never converted to
success by retry timing. `FENCED_OR_CONFLICT` stops admission and cannot be hidden as a generic retry.

The caller retains an immutable/replayable payload until terminal reconciliation. The provider contract states exact
reference-count ownership for submit success, synchronous throw, async completion, cancellation, timeout, close, and
response loss. Cancellation cannot erase an admitted entry or release its permits before the ordered resolver reaches a
terminal state. Drain completes only when no accepted operation or reconciliation remains owned by the session.

`KafkaRunRootAuthority` is a separate low-frequency interface for create/open/seal/successor roots. It is not a generic
metadata store and is never called once per Produce, RecordBatch, or Fetch. M2 may use a deterministic implementation in
engine tests; later milestone integration must preserve the same no-normal-path-control-I/O rule.

## Evidence and gate hierarchy

The planned Kafka gate stack is:

```text
v2M2KafkaInputsCheck
  -> v2M2KafkaFastCheck
  -> v2M2KafkaBookKeeperCheck
  -> v2M2KafkaScaleCheck
  -> v2M2KafkaExactSourceCheck
  -> v2M2KafkaFinalCheck
```

`v2M2KafkaInputsCheck` is non-promotable implementation readiness. It requires production codec/cap/provider/API
constants, immutable vectors, module/dependency/publication checks, source-lock schema, receipt parser, and non-zero
focused tests. It cannot claim a run writer, real BookKeeper conformance, scale, Kafka runtime, scenario PASS, or M2
Final.

The later gates have distinct authorities:

- Fast: deterministic state/codec/fake-provider/fault cuts, ordinary PR-safe and zero-skip;
- BookKeeper: exact real client/server/configuration plus create/add/read/fence/recover/seal/response-loss behavior;
- Scale: 10k/100k partitions, handles, heap/direct memory, metadata operations, bookie pressure, rollover, recovery,
  read amplification, throughput, and latency;
- Exact Source: clean exact Nereus/Kafka/BookKeeper inputs before and after, immutable artifact digests, configuration
  digest, and receipt/attachment validation;
- Final: freshness and aggregation of already produced results without rerunning or relabelling a suite.

An unimplemented gate is absent or fails with an explicit not-implemented condition. It must not be registered as an
empty task, use a zero-test success, or emit a PASS-shaped placeholder. Mandatory suites require discovered, executed,
and passed counts greater than zero with zero failure, skip, or abort. Sanitized logs are attachments with bounded bytes
and hashes; prose summaries are not result authority.

The receipt family is closed initially to:

```text
KAFKA_M2_INPUTS_ONLY          // promotionEligible=false
KAFKA_BOOKKEEPER_CONFORMANCE
KAFKA_BOOKKEEPER_SCALE
KAFKA_M2_FINAL_INDEX
```

Each receipt binds the exact Nereus commit, immutable N1 input, Kafka fork commit, BookKeeper source/client/server/image
and configuration, source-lock byte digest, artifact manifest, normalized test hierarchy, required metrics, and
allowlisted attachment path/length/SHA-256. A receipt never self-locks the commit that first adds itself; Final follows
the established tested-source then evidence-only descendant model.

Before the first implementation slice, the M2 development source tuple must add the M1 Final base, exact N1 artifact,
Kafka K1 base/fork, BookKeeper tag/commit and client artifacts, real server image digest, complete relevant BookKeeper
configuration digest, and capability-snapshot schema. This accepted document does not insert `NOT_PINNED` placeholders
or change the current root `sourceTupleId`; source-lock bytes change with the implementation that can verify them.

## Scenario and aggregate boundary

Kafka M2 completion is a sub-aggregate, not automatically the global M2 completion:

- `v2M2KafkaFinalCheck` may promote only complete Kafka claims owned entirely by M2;
- `V2-BK-003` and `V2-BK-014..017` are the Kafka BookKeeper rows;
- among `V2-KAF-DATA-001..022`, the current exactly-M2 rows are `001`, `002`, `004`, `005`, and `014`; only those may
  become `PASSED_CURRENT_SOURCE` from the Kafka M2 receipt;
- a row marked `M2/M3`, `M2/M4/M5`, or `M2/M6` remains `PLANNED` until every named owner supplies its required
  evidence; an M2 receipt may record an engine sub-claim but cannot promote the whole row;
- pure M6 row `V2-KAF-DATA-009`, Object-checkpoint row `V2-KAF-DATA-021`, source-retirement rows, and complete native
  client/process behavior are never Kafka M2 PASS claims.

Global `v2M2Check` remains the aggregate for every global-M2 Kafka and Pulsar requirement. It exists only after those
owners and their receipts exist. Passing `v2M2KafkaFinalCheck` alone must be reported as Kafka BookKeeper engine M2
completion, not global M2 Final, Kafka runtime activation, or product release readiness.

## Implementation order and exit

M2-K0 is delivered in five independently reviewable cuts, followed by one aggregate:

1. `K0-M`: module graph, immutable N1 dependency, filtered M2 coordinate, forbidden dependency checks;
2. `K0-P`: provider/session/capability/outcome/buffer-lifecycle API plus deterministic contract tests;
3. `K0-W`: exact `NBKE2` table, production codecs, machine projection, goldens, and corruption matrix;
4. `K0-N`: persisted caps, checked formulas, pre-offset admission boundary, and one-before/at/one-after tests;
5. `K0-E`: receipt/parser/source-lock/gate implementation with explicit non-promotion policy;
6. `v2M2KafkaInputsCheck`: aggregate the five non-empty cuts and publish `KAFKA_M2_INPUTS_ONLY`.

Wire may be reviewed before its writer, but the module, provider, and numeric inputs must be available before the final
wire cap table is accepted. No cut creates a long-lived compatibility facade, per-append Oxia/KRaft mapping, shared
cross-Cell correctness state, Kafka protocol reimplementation, Object-WAL carrier, or dormant PASS-shaped gate.

M2-K1 starts only after the aggregate is green on the exact source tuple. K9 may select operational defaults but cannot
change `NBKE2` v1 bytes or enlarge its parser caps; an evidence result that cannot operate inside the frozen format
blocks the profile or requires a new accepted version/layout decision.
