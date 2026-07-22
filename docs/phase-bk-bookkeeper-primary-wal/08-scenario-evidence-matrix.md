# Scenario and Evidence Matrix

## 1. Status and evidence levels

BK-01 through BK-10 executed successfully on 2026-07-19 through `bookKeeperPrimaryWalM1Check` and the 199-task
`bookKeeperPrimaryWalM1FinalCheck` aggregate against local Pulsar
`master@eaf7b9a704890a9265c21f30d9f351e02d00c600`。BK-M2 then completed on 2026-07-20 through the 107/107-task
`bookKeeperPrimaryWalM2Check --rerun-tasks` and the 212-task `bookKeeperPrimaryWalM2FinalCheck` aggregate against
current local Pulsar `master@2f9c1eb93be96e2036fbdc8c5e39545f21fa6200`。The final aggregate covers BK-11 through
BK-56 at the milestone's declared D/O/B and focused local-Pulsar boundary：all codecs/keyspaces/shards、allocation and
writer monotonicity、foreign and same-candidate authority、uncertain/late create、exact append/recovery/fencing/read、
generation-zero repair、resource/deadline contracts、logical trim、complete protection inventory and dual-absence
whole-ledger deletion。It includes real Oxia/BookKeeper response-loss、restart、rollover、cold-read、foreign quarantine
and retention cuts。Rows that additionally name production broker P/T or abrupt-process C evidence keep those suffixes
were assigned to BK-M5/BK-M6；both successor milestones are now complete/final-gated and they were never hidden BK-M2
completion prerequisites。Every implementation claim below still
requires an exact test method、owning gate、source lock、date and result；prose alone never covers a row。

The 2026-07-19 `bookKeeperPrimaryWalM3PhysicalRetirementCheck --rerun-tasks` passed 65/65 executable tasks。Its real
O/B/S chain covers the positive path of BK-58、BK-59、BK-61、BK-62、BK-65、BK-66 and BK-67 through dynamic source
release、three mandatory-reference retirements、whole-ledger physical deletion and exact post-delete Object reads；the
failure-cut portions called out in those rows remain open。

The 2026-07-19 `bookKeeperPrimaryWalM3ResponseLossCheck --rerun-tasks` passed 65/65 executable tasks and adds real
O/B/S fresh-runtime cuts for BK-60、BK-61 and
BK-67。It proves task creation precedes BK source protection，applied source-create and Object-PUT response loss reach
recoverable `RETRY_WAIT` without leaking protection，and a later runtime converges every claim/output/source-transfer/
generation-publication/task-publication/source-release response loss onto the same task、BK range、Object key and
higher generation。The next focused checkpoint closes real-load lag and the concrete unreadable-output negative cut。

The 2026-07-19 `bookKeeperPrimaryWalM3LagFailureCheck --rerun-tasks` passed 65/65 executable tasks and closes the
focused real-service parts of BK-63、BK-64 and BK-66。The exact
shared lag reader observes two uncovered records，the BK-only profile adapter rejects the next append before the real
writer record/BK range changes with `BACKPRESSURE_REJECTED/KNOWN_NOT_COMMITTED`，and Object coverage re-admits a later
append。A separate real chain deletes the exact COMMITTED Object and proves `OBJECT_NOT_FOUND` retirement failure、all
fixed BK references still ACTIVE、ledger presence and normal-read BK fallback。Abrupt process-kill variants remain in
the later chaos aggregate rather than this focused M3 checkpoint。

The 2026-07-20 `bookKeeperPrimaryWalM3FinalCheck` passed its 223-task aggregate over the final-gated BK-M2 predecessor
and every focused M3 chain。BK-57 through BK-67 are therefore complete at their declared M3 D/O/B/S levels；row suffixes
that explicitly require production broker rollout or process-loss C are closed by the final-gated BK-M5/BK-M6
evidence listed below。

BK-M4 subsequently final-gated BK-68 through BK-76 at their declared D/O/B/S boundaries。On 2026-07-22，
`bookKeeperPrimaryWalM5Check --rerun-tasks` passed 105/105 tasks and
`bookKeeperPrimaryWalM5FinalCheck --rerun-tasks` passed 231/231 fresh tasks in 27m42s against
`master@2f9c1eb93be96e2036fbdc8c5e39545f21fa6200`。The final gate executed BK-77 through BK-86 across exact
borrowed-client ownership、first-create/profile immutability、admin routing、BK_ONLY takeover、mixed BK_SYNC/BK_ASYNC/
Object-WAL coexistence、old-broker exclusion and deletion-proof readiness rollover。BK-87 through BK-96 formed the
explicit BK-M6 scale/chaos/aggregate boundary and are now complete/final-gated by document `09`。

The same deterministic recovery gate now converges applied-response-loss for every writer/root CAS from ACTIVE through
IDLE (BK-31 D) and serializes two process-run recovery contenders onto one replacement ledger (BK-33 D)。Their remaining
independent-process levels stay explicit in the rows below。

The deterministic BK-M2 checkpoint additionally executes
`BookKeeperStreamStorageIntegrationTest.strictBkOnlyAppendAndColdReadTraverseTheProviderNeutralL0Pipeline`：the
module-local profile resolver/runtime、exact pre-head BookKeeper proof reload、generation-zero index and cold range
reader are integrated。`NereusBookKeeperManagedLedgerIntegrationTest.facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero`
extends that D-level evidence through the
ManagedLedger facade，while pinned-Pulsar `NereusManagedLedgerStorageBookKeeperClientTest` freezes the borrowed stock
client boundary。These checkpoints do not replace the B/O/P final rows below。

Evidence levels：

| Level | Meaning |
| --- | --- |
| D | deterministic unit/contract/property/failure-injection test |
| O | real Oxia adapter/service |
| B | real BookKeeper 4.18 client/cluster |
| S | real Object store/format path where applicable |
| P | retry-disabled local-Pulsar broker integration |
| T | two-broker ownership/failover fixture |
| X | exact scale boundary |
| C | independent-process/response-loss/chaos cut |

## 2. Contract and provider-neutral foundation

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-01 | M0 | D | all nine code-level docs, source locks and local links are current | `bookKeeperPrimaryWalDocumentationCheck` |
| BK-02 | M1 | D | `BookKeeperEntryRangeReadTarget` golden bytes round-trip and malformed/overflow values fail | `BookKeeperEntryRangeReadTargetCodecV1Test.goldenAndRejectsMalformedValues` |
| BK-03 | M1 | D | NBKR1 checksum frames entry count/id/length and detects reorder/truncation | `BookKeeperRangeChecksumsTest.framesExactEntrySequence` |
| BK-04 | M1 | D | Object reader returns identical batches/accounting through provider-neutral result | `ProviderNeutralObjectReadRegressionTest.preservesLogicalAndPhysicalAccounting` |
| BK-05 | M1 | D | BK reader result contains target SHA and no fake ObjectId/index | `BookKeeperReadResultContractTest.usesOnlyCanonicalBookKeeperSourceIdentity` |
| BK-06 | M1 | D | generic `ReadCoordinator` rejects missing/duplicate/wrong target stats | `ProviderNeutralReadAccountingTest.rejectsIdentityAndByteDrift` |
| BK-07 | M1 | D | generic append commit/protection/gen0 accepts synthetic BK target without Object cast | `GenericPrimaryAppendContractTest.commitsTaggedBookKeeperTarget` |
| BK-08 | M1 | D | Object append protocol bytes/result/error cuts remain unchanged | `DefaultStreamStorageAppendTest.appendBuildsManifestAndAdvancesDenseOffsets` + `ObjectWalGuardedUploadTest` |
| BK-09 | M1 | D | BookKeeper/ManagedLedger imports obey module DAG | `check-bookkeeper-module-boundaries.sh` |
| BK-10 | M1 | D/O | config excludes secrets, alias cannot drift provider scope, namespace store rejects conflict/revoke, candidates round-trip prefix | `BookKeeperWalConfigurationTest.bindsNonSecretSemantics` + `BookKeeperLedgerIdNamespaceReservationStoreContractTest` |

## 3. Allocation, metadata, and lifecycle

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-11 | M2 | D/O | seven V1 writer/allocation/slot/root/reservation/protection/lease codecs share production/fake golden/failure contracts | `BookKeeperMetadataCodecContractTest` + `BookKeeperMetadataStoreContractScenario` |
| BK-12 | M2 | D | key strict inverse rejects wrong cluster/shard/depth/noncanonical components | `BookKeeperKeyspaceTest.strictlyInvertsEveryWriterPrefix` |
| BK-13 | M2 | O | intent -> durable slot CLAIMED/CREATE_STARTED -> root -> active writer survives every Oxia CAS response loss | full allocation-chain D checkpoint: `BookKeeperLedgerAllocatorTest.convergesEveryAppliedMetadataResponseLossInTheAllocationChain`; real Oxia production-adapter mutation/reload: `BookKeeperMetadataOxiaResponseLossIntegrationTest.productionAdapterReloadsEveryAppliedBookKeeperMutationResponseLoss` |
| BK-14 | M2 | B/O | exact create response loss persists slot + permanent hazard；matching metadata is recovery-opened/sealed because the write handle is unrecoverable | `BookKeeperWalOnlyOxiaBkIntegrationTest.createResponseLossRecoverySealsTheExactLedgerAndKeepsTheHazardSlot` |
| BK-15 | M2 | B/O | candidate already owned by stock/foreign ledger is never deleted and a new id wins | D checkpoint: `BookKeeperLedgerAllocatorTest.foreignCreateCollisionIsQuarantinedAndTheNextCandidateWinsWithoutDelete`; real B/O: `BookKeeperWalOnlyOxiaBkIntegrationTest.foreignLedgerCreatedAtProviderBoundaryIsQuarantinedAndNeverDeletedBeforeFreshCandidateWins` |
| BK-16 | M2 | D/O | two streams choose the same candidate; global root admits one allocation | D checkpoint: `BookKeeperLedgerAllocatorTest.globalRootSerializesTwoStreamsThatChooseTheSameCandidate`; real O/B: `BookKeeperAllocatorOxiaBkContentionIntegrationTest.twoStreamsChoosingOneCandidateConvergeToTwoOwnedLedgersWithoutDelete` |
| BK-17 | M2 | B/O/C | `CREATE_UNCERTAIN` stays slot-consuming；matching late create seals with permanent GC veto, foreign quarantines | real matching and foreign late-create + permanent GC-veto checkpoint: `BookKeeperWalOnlyOxiaBkIntegrationTest.createResponseLossRecoverySealsTheExactLedgerAndKeepsTheHazardSlot`; deterministic foreign checkpoint: `BookKeeperLedgerAllocatorTest.boundedUncertainSlotRecoveryQuarantinesForeignLateCreateWithoutDeletingIt`; BK-M6 fresh-runtime cut evidence is mapped in document `09` |
| BK-18 | M2 | D/O | writer state never reuses segment/entry ids and physical-byte/range counters never move backward after conflict/restart | D property checkpoint: `BookKeeperWriterStatePropertyTest.isMonotonicAcrossCasSchedules`; real O/B conflict/restart/rollover checkpoints: `BookKeeperAllocatorOxiaBkContentionIntegrationTest.twoStreamsChoosingOneCandidateConvergeToTwoOwnedLedgersWithoutDelete` + `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-19 | M2 | O | all 256 root + 16 fixed allocation-slot shards scan empty opaque continuations without omission | `BookKeeperWalOnlyOxiaBkIntegrationTest.realOxiaColdScanCoversEveryRootAndAllocationSlotShard` |
| BK-20 | M2 | D | invalid lifecycle fields/transitions and immutable identity drift fail closed | `BookKeeperLedgerTransitionsTest.rejectsIllegalTransitionsAndImmutableIdentityDrift` + `rejectsInvalidLifecycleFieldsBeforePersistence` |

## 4. Append, recovery, and fencing

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-21 | M2 | B/O | one ordinary entry is quorum durable, head reachable and returns stable offset | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-22 | M2 | B/O | multi-entry append occupies exact consecutive ids and one target checksum | `BookKeeperPrimaryWalAppenderTest.reservesWritesAndReturnsOneStableExactRange` + `BookKeeperWalOnlyOxiaBkIntegrationTest.multiEntryAppendUsesOneExactConsecutiveBookKeeperRange` |
| BK-23 | M2 | D/B | future/missing-adapter profile、invalid configuration and oversize batch reject before BK calls；both V1 durability values remain valid | `BookKeeperStorageProfileResolverTest.rejectsObjectFutureBkProfilesAndMissingAdaptersBeforeIo` + `BookKeeperStreamStorageIntegrationTest.unsupportedProfileAndOversizeBatchReachNoBookKeeperOperation` + `BookKeeperWalConfigurationTest` |
| BK-24 | M2 | D/B/O | first/middle/last write failure taints and seals ledger; no tail reuse | `BookKeeperPrimaryWalAppenderTest.partialWriteAbandonsRangeRecoverySealsLedgerAndNextAppendAllocatesFreshLedger` + `BookKeeperWalOnlyOxiaBkIntegrationTest.firstMiddleAndLastWriteFailureSealTheLedgerBeforeReuse` |
| BK-25 | M2 | D/B/O/C | crash after range + three mandatory RESERVED protection slots but before write becomes known-not-committed | `BookKeeperAppendRecoveryCoordinatorTest.nonDurableWritingCutIsSealedAndProvenNotCommitted` + BK-M6 fresh-runtime cut manifest in document `09` |
| BK-26 | M2 | D/B/O/C | full writes before commit intent resume only under unchanged session | `BookKeeperAppendRecoveryCoordinatorTest.currentSessionCommitsTheSameDurableRangeWithoutAnotherBookKeeperWrite` + real fresh-client/runtime `BookKeeperWalOnlyOxiaBkIntegrationTest.restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange` + BK-M6 fresh-runtime cut manifest in document `09` |
| BK-27 | M2 | D/B/O/C | new session abandons unreachable old-writer full range and uses new ledger | `BookKeeperAppendRecoveryCoordinatorTest.newSessionAbandonsUnreachableDurableRangeAndAllocatesAnotherLedger` (including exact abandoned-owner retirement) + real expired-session/fresh-client `BookKeeperWalOnlyOxiaBkIntegrationTest.restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange` + BK-M6 fresh-runtime cut manifest in document `09` |
| BK-28 | M2 | D/O/C | commit intent/protection/head response loss returns same committed target/result | D checkpoints: `BookKeeperAppendRecoveryCoordinatorTest.preparedIntentResponseLossRetriesTheSameRangeWithoutAnotherBookKeeperWrite` + `reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal`; real O/B applied intent/head response loss: `BookKeeperWalOnlyOxiaBkIntegrationTest.realOxiaStableAppendResponseLossReusesOneBookKeeperRangeAndRepairsGenerationZero`；BK-M6 fresh-runtime cut manifest in document `09` |
| BK-29 | M2 | D/O/C | head reachable/gen0 missing repairs same target without BK write | D checkpoints: `BookKeeperStreamStorageIntegrationTest.reachableHeadRecoveryRepairsGenerationZeroWithoutRewritingBookKeeper` + `BookKeeperAppendRecoveryCoordinatorTest.reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal`; real O/B head/gen0 response loss: `BookKeeperWalOnlyOxiaBkIntegrationTest.realOxiaStableAppendResponseLossReusesOneBookKeeperRangeAndRepairsGenerationZero`；BK-M6 fresh-runtime cut manifest in document `09` |
| BK-30 | M2 | B/O | entry/physical-byte/append-range/age rollover occurs before batch, never splits it and keeps dense offsets | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` + `byteRangeAndAgeRolloverPreserveWholeBatchesAndDenseOffsets` |
| BK-31 | M2 | B/O/C | crash in ACTIVE->SEALING->SEALED converges exact closed LAC/length | `BookKeeperLedgerRecoveryTest.recoversEverySealCut` + BK-M6 fresh-runtime cut manifest in document `09` |
| BK-32 | M2 | B/O | new owner recovery-open fences old handle; old owner cannot head-commit | `BookKeeperWalOnlyOxiaBkIntegrationTest.newOwnerRecoveryOpenFencesLiveOldHandleAndPreventsOldHeadCommit` |
| BK-33 | M2 | D/B/O | two recovery owners contend; one new active ledger wins | `BookKeeperLedgerRecoveryTest.serializesTwoRecoveryOwners` + real independent Oxia-runtime/BK-client `BookKeeperWalOnlyOxiaBkIntegrationTest.twoIndependentRecoveryProcessesElectOneRealOxiaWinnerAndOneReplacementLedger` |
| BK-34 | M2 | D | buffer/permit counts return to zero on success/failure/timeout/cancel/close | `BookKeeperAppenderResourceTest.releasesEveryOwnedResource` + `BookKeeperPreparedPrimaryAppendTest` + `BookKeeperClientApiContractTest` |
| BK-35 | M2 | D/B/O | one monotonic deadline spans allocation/write/commit and does not reset | `BookKeeperAppenderDeadlineTest.propagatesRemainingBudget` + real Oxia/BK delayed-create budget observation and committed cold read `BookKeeperWalOnlyOxiaBkIntegrationTest.realCreateDelayConsumesTheSingleAppendDeadlineBeforeWriteAndStableCommit` |
| BK-36 | M2 | D | typed configuration has no write-flag escape hatch and production create always passes an empty flag set, making `DEFERRED_SYNC` unrepresentable | `BookKeeperClientApiContractTest.defaultAdapterMakesDeferredSyncUnrepresentableAndAlwaysUsesEmptyWriteFlags` |

## 5. Read and Pulsar compatibility

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-37 | M2 | B/O | non-recovery open reads exact complete range and verifies NBKR1 | `BookKeeperPrimaryWalReaderTest.nonRecoveryOpenVerifiesWholeRangeBeforeReturningClippedExactEntries` + `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-38 | M2 | D/B | ordinary read never invokes recovery-open/fences writer | `BookKeeperPrimaryWalReaderTest` + real `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift` |
| BK-39 | M2 | B/O | middle-offset clipped read verifies full target then returns dense suffix | `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift` |
| BK-40 | M2 | B/O | checksum/count/id/config mismatch fails; no partial/empty result | `BookKeeperPrimaryWalReaderTest.checksumMismatchFailsClosedWithoutReturningPartialBytes` + `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderNeverRecoveryOpensVerifiesWholeRangeBeforeClippingAndFailsClosedOnChecksumDrift` + `realReaderFailsClosedOnCountIdAndConfigurationDrift` |
| BK-41 | M2 | B/O/C | fresh process reads history with no cached handle | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-42 | M2 | D/B/O | fixed reader slots cap concurrent processes race-free；lease blocks MARK/delete and final revalidation precedes return | `BookKeeperReaderLeaseManagerTest.oneProcessSharesOneRenewableSlotUntilItsFinalLocalRelease` + `fixedSlotsBoundIndependentProcessesWithoutDeletingForeignOccupants` + `finalRevalidationFailsWhenTheExactDurableLeaseDisappears` + `renewalFailureDoesNotLeakTheRememberedDurableSlot` + `BookKeeperWalRetentionGateTest.admitsOnlyACompleteRetiredInventoryWithoutReaderPins` + `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderSlotsArePerProcessBoundedAndFinalPinRevalidationFailsClosed` |
| BK-43 | M2 | P | raw Pulsar Entry properties/payload round-trip through BK generation zero | D properties/ordering checkpoint: `PulsarEntryOpaqueRoundTripTest.preservesUnbatchedAndCompressedBatchBytesPropertiesOrderingKeyAndMiddleBatchMessageId` + `NereusBookKeeperManagedLedgerIntegrationTest.facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero` + `rolloverReopenSeekAndDurableCursorRemainOnStableVirtualTruth`; real broker ordinary/LZ4-batch payload and direct generation-zero evidence: `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` |
| BK-44 | M2 | P | ordinary and batched `MessageIdAdv` use virtual identity, not BK ledger id | D checkpoint: `NereusBookKeeperManagedLedgerIntegrationTest.facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero` + `rolloverReopenSeekAndDurableCursorRemainOnStableVirtualTruth`; batched codec and real two-broker exact ledger/entry/partition/batch comparison: `PulsarEntryOpaqueRoundTripTest.preservesUnbatchedAndCompressedBatchBytesPropertiesOrderingKeyAndMiddleBatchMessageId` + `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` |
| BK-45 | M2 | P | seek/history after rollover/restart returns same MessageIds | D checkpoint across three physical ledgers and facade reopen: `NereusBookKeeperManagedLedgerIntegrationTest.rolloverReopenSeekAndDurableCursorRemainOnStableVirtualTruth`; real unload/failover/restart/rejoin exclusive+inclusive seek: `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` |
| BK-46 | M2 | P | F3 ack/snapshot/hydration remains logical across physical ledgers | D checkpoint: `NereusBookKeeperManagedLedgerIntegrationTest.rolloverReopenSeekAndDurableCursorRemainOnStableVirtualTruth` plus the F3 durable cursor/snapshot suites；final P broker test remains M5 |

## 6. Retention and deletion

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-47 | M2 | O/B | BK_ONLY trim below range end cannot retire/delete range | `BookKeeperWalOnlyOxiaBkIntegrationTest.partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes` |
| BK-48 | M2 | O/B | one ledger with trimmed + live ranges remains physical | `BookKeeperWalOnlyOxiaBkIntegrationTest.partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes` |
| BK-49 | M2 | O/B | all ranges durably trimmed retire owners/protections then whole ledger | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-50 | M2 | D/O | fixed protection-slot contention never exceeds Cartesian bound；invalid/unstable inventory vetoes collection | D checkpoint: `BookKeeperWalRetentionGateTest.failsClosedOnIncompleteAuthority`；real exact-bound scan/veto: `BookKeeperWalOnlyOxiaBkIntegrationTest.realOxiaProtectionCartesianBoundPreservesTaskRepairAndMandatoryVetoes` |
| BK-51 | M2 | D/B/O | reader/task/repair/reservation/writer vetoes are each enforced | D complete-owner checkpoint: `BookKeeperWalRetentionGateTest.enforcesEveryVetoDomain`；real reader: `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderSlotsArePerProcessBoundedAndFinalPinRevalidationFailsClosed`；real mandatory-range protection: `partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes`；real task/repair exact-bound veto: `realOxiaProtectionCartesianBoundPreservesTaskRepairAndMandatoryVetoes`；BK-M6 bounded inventory/fresh-runtime evidence closes the aggregate variant |
| BK-52 | M2 | D/O/C | reference appears after MARKED; root unmarks to SEALED | D: `BookKeeperWalRetentionGateTest.referenceAppearingAfterMarkUnmarksToSealedBeforeDelete`; O/B: `BookKeeperWalOnlyOxiaBkIntegrationTest.referenceAfterMarkUnmarksAndSafeGcModesNeverDelete`; BK-M6 fresh-runtime cut manifest in document `09` |
| BK-53 | M2 | B/O/C | delete response loss reloads metadata before same-intent retry | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-54 | M2 | B/O/C | namespace drift or late-create hazard stops before delete；foreign/reappeared same-id ledger is quarantined across validate/delete cut | B/O checkpoint: `BookKeeperWalOnlyOxiaBkIntegrationTest.foreignLedgerRecreationAndNamespaceDriftStopBeforePhysicalDelete`; BK-M6 fresh-runtime cut manifest in document `09` |
| BK-55 | M2 | D/B/O/C | two separated absence observations and root CAS loss converge DELETED | D every-root-CAS applied-response-loss: `BookKeeperWalRetentionGateTest.everyGcRootCasConvergesAfterAppliedResponseLoss`；fresh-process B/O dual absence: `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover`；BK-M6 fresh-runtime cut manifest in document `09` |
| BK-56 | M2/M5 | D/O | dry-run/default-off mode performs no root/provider mutation；enabled production pass scans all shards、routes only the exact binding and isolates one-root failure | `BookKeeperWalRetentionGateTest.disabledAndDryRunGcNeverMutateRootOrProvider` + `BookKeeperWalOnlyOxiaBkIntegrationTest.referenceAfterMarkUnmarksAndSafeGcModesNeverDelete` + `BookKeeperLedgerRetentionScannerTest.scansEveryShardAndRoutesOnlyTheExactBinding` + `oneRootFailureIsAccountedAndDoesNotStopLaterRootsOrShards` + `disabledAndDryRunModesNeverEvenScanTheDurableInventory` |

## 7. Async materialization

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-57 | M3 | D/O | BK `SourceGeneration` task V2 round-trips exact tagged target/identity | D checkpoint: `BookKeeperMaterializationTaskCodecTest.roundTripsBookKeeperSource`; real Oxia recovery remains O |
| BK-58 | M3 | B/O/S | async append acks at head while Object generation is held absent | D contract: `BookKeeperStorageProfileResolverTest.admitsAsyncObjectAtStableHeadOnly`; real O/B/S: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject` |
| BK-59 | M3 | B/O/S | reads use BK while task pending, then exact higher Object generation | D: `ExactSourceRangeReaderTest.readsBookKeeperSourceThroughRegisteredProviderWithoutObjectIdentityOrPin` + `GenerationReadResolverTest.bookKeeperAsyncFallsBackToProviderProtectedGenerationZeroWithoutObjectPin`; real O/B/S switch: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject` |
| BK-60 | M3 | O/C | task exists before source protection; gen0 retains and restart reconciles | D checkpoints: `BookKeeperMaterializationSourceProtectionAdapterTest.acquiresTransfersRevalidatesAndReleasesExactDynamicSlot` + `MaterializationTaskProtectionReconcilerTest.reconstructsBookKeeperProtectionThroughTheProviderRegistry`; real fresh-runtime task-create/source-create cuts: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.freshRuntimesConvergeAppliedTaskSourceOutputAndPublicationResponseLoss` |
| BK-61 | M3 | B/O/S/C | worker restart re-reads exact BK range and reuses task/output intent | Graceful reconstruction plus applied Object-PUT response loss, durable `RETRY_WAIT`, fresh-runtime same-task/output recovery: both `BookKeeperAsyncObjectOxiaBkS3IntegrationTest` methods |
| BK-62 | M3 | B/O/S | ordinary/batched Entry bytes and MessageIds equal before/after NCP1 | Exact opaque multi-entry bytes before/after NCP1: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject`; exact Pulsar ordinary/batched MessageId projection is closed by `NereusMixedPrimaryProfilesMultiBrokerTest.coexistsAcrossProfiles` |
| BK-63 | M3 | D/O | lag records/bytes/age derive from coverage, not ledger/task count | D: `DefaultMaterializationLagSnapshotReaderTest.measuresBookKeeperAsyncObjectWithTheSharedLagAuthority`; real Oxia coverage/recovery: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.sharedRealLagAdmissionRejectsBeforeBookKeeperIoAndRecoversAfterObjectCoverage` |
| BK-64 | M3 | D/B | lag throttle remeasures; reject occurs before next BK IO | `BookKeeperAsyncAdmissionTest.throttlesAndRejectsBeforeWal` + real writer/BK non-mutation and post-coverage admission in `sharedRealLagAdmissionRejectsBeforeBookKeeperIoAndRecoversAfterObjectCoverage` |
| BK-65 | M3 | D/O/S | sealed ledger final single source uses the one normal deterministic planner/task path | D: `MaterializationPlannerTest.plansFinalSingleBookKeeperGenerationZeroThroughTheOrdinaryPlanner` + `BookKeeperWalRetentionGateTest.sealedLedgerTriggerRevalidatesExactRootAndUsesTheSharedMaterializationScanner`; real ordinary scanner/worker single-source path: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject` |
| BK-66 | M3 | D/O/B/S | healthy replacement retires BK source; broken/PREPARED/missing-protection/checkpoint/unreadable output does not | D exhaustive metadata/read cuts: `CommittedObjectGenerationAuthorityTest.requiresExactCommittedIndexActiveRootVisibleProtectionAndCoveringCheckpoint` + `NormalPathCommittedObjectGenerationReadVerifierTest.retirementProofAcquiresNormalPinsAndReadsTheExactGenerationEndToEnd` + `BookKeeperWalRetentionGateTest.healthyCommittedObjectGenerationRetiresOnlyMandatoryAsyncSourceReferences`; real positive and physically-missing Object negative: `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.stableHeadFallsBackToBookKeeperThenFreshRuntimePublishesAndReadsExactObject` + `missingCommittedObjectVetoesBookKeeperRetirementAndFallsBackToExactRange` |
| BK-67 | M3 | D/O/B/S/C | generation/source/protection response-loss cuts precede whole-ledger delete | D terminal/provider release: `TerminalWorkflowMetadataRetirementTest.retiresTerminalTaskWithProviderOwnedBookKeeperSourceProtection`；real release/retire/delete/post-delete-read chain plus fresh-runtime task/source/output/publication response-loss matrix: both original `BookKeeperAsyncObjectOxiaBkS3IntegrationTest` methods；BK-M6 closes the aggregate process-loss boundary through the exact cut manifest in document `09` |

## 8. Sync completion

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-68 | M4 | D | BK sync maps to REQUIRED_OBJECT_GENERATION, not gen0 durability | `BookKeeperStorageProfileResolverTest.admitsSyncObjectOnlyWithExactCompletionBarrierAndStrongPublicPolicy` + `AsyncObjectWalAppendCoordinatorTest.requiredObjectPolicyWaitsForGenerationZeroThenExactHigherGenerationProof` |
| BK-69 | M4 | D | weaker explicit policy or missing completion seam is rejected before IO | `BookKeeperStorageProfileResolverTest.admitsSyncObjectOnlyWithExactCompletionBarrierAndStrongPublicPolicy` |
| BK-70 | M4 | B/O/S | no producer ack until exact COMMITTED Object generation reads successfully | `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.syncObjectAcknowledgesOnlyAfterExactObjectGenerationIsNormallyReadable` |
| BK-71 | M4 | B/O/S | consumer reads committed BK bytes while producer waits for Object | `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.syncObjectKeepsBkVisibleWhileProducerWaitsForObjectPublication` |
| BK-72 | M4 | D/O/S | task recovery、publication/read failure and producer retry reuse the same committed task/range；abrupt-process exhaustive cuts remain M6 | M3 final-gated task/source/output/publication response-loss matrix + `syncObjectUnreadablePublicationIsKnownCommittedAndRecoveryReusesBkRange` |
| BK-73 | M4 | O/S | already committed current-policy coverage satisfies a repeated request after exact proof | second completion in `syncObjectAcknowledgesOnlyAfterExactObjectGenerationIsNormallyReadable` preserves one task/generation |
| BK-74 | M4 | O/S | exact one-source task identity is deterministic for each sequential BK range and does not race the broad scanner | `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.syncObjectSequentialAppendsKeepOneDeterministicTaskPerBkRange` |
| BK-75 | M4 | B/O/S | Object normal-read failure after head exposes KNOWN_COMMITTED + same attempt | `BookKeeperAsyncObjectOxiaBkS3IntegrationTest.syncObjectUnreadablePublicationIsKnownCommittedAndRecoveryReusesBkRange` |
| BK-76 | M4 | D/B/O/S | recoverAppend/restart returns original stable result and never writes another BK range | real `syncObjectUnreadablePublicationIsKnownCommittedAndRecoveryReusesBkRange` + deterministic `BookKeeperAppendRecoveryCoordinatorTest.syncRestartWaitsForExactObjectProofWithoutAnotherBookKeeperWrite` |

## 9. Pulsar rollout and aggregate compatibility

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-77 | M5 | D/P | borrowed client is closed zero times by Nereus and once by stock owner | `NereusBookKeeperBorrowedClientTest.closesOnlyAtStockOwner` |
| BK-78 | M5 | D/P | exact namespace provision/terminal revoke plus missing/mismatched activation/config capability reject first-create before IO；runtime never auto-creates authority；exact deletion readiness must equal the live strongest-profile broker set；all root/stream/scope proofs are producer-owned and one-CAS/idempotent；authenticated REST cannot inject proofs | implemented `BookKeeperProtocolAdministrationTest.namespaceProvisionIsIdempotentAndRevokeIsTerminalVersionedCas`、`activationKeepsPublicationIdentityStableButRebindsEveryDeletionRecord`、`NereusBookKeeperPrimaryWalCapabilityTest.firstCreateRequiresTwoStableAllBrokerSnapshotsWithTheExactBinding`、`deletionReadinessUsesTheStablePublicationBindingAndInvalidatesOnDrift`、`BookKeeperRootCoverageProofProducerTest`、`BookKeeperStreamCoverageProofProducerTest`、`BookKeeperScopeCapabilityProbeTest`、`BookKeeperDeletionActivationCoordinatorTest` and `NereusBookKeeperPrimaryWalAdminTest.rejectsBeforeLookingUpStorageWhenSuperUserValidationFails/exposesNoCallerControlledDeletionProofFields/mapsPreparePublicationDeletionAndReadExactly` |
| BK-79 | M5 | P | first-create persists exact profile; reopen ignores changed default | `NereusBookKeeperProfileAdmissionTest.keepsImmutableProfile` |
| BK-80 | M5 | P | explicit existing-profile mutation/online migration is rejected | `NereusBookKeeperProfileAdmissionTest.rejectsProfileMutation` |
| BK-81 | M5 | P | loaded/unloaded/partitioned routes use the exact binding-generation/L0 durable profile；storage-dependent operations fail closed while unload/logical delete remain recoverable | `PersistentTopicsNereusDurableProfileRoutingTest.loadedNereusTopicUsesTheSameDurableStorageRoute/unloadedNereusBindingUsesTheDurableStorageRoute/partitionedParentValidatesEveryConcretePartition` + `NereusAdminOperationTest.durableProfileReadinessIsAppliedOnlyToStorageDependentOperations/durableProfileComesFromExactBindingGenerationAndL0Metadata` |
| BK-82 | M5 | T/B/O | unload/failover/restart/rejoin/reverse takeover preserves history/MessageIds | implemented E.3: `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` |
| BK-83 | M5 | T/B/O/S | async/sync topics and Object-WAL topics coexist on both brokers | `NereusMixedPrimaryProfilesMultiBrokerTest.coexistsAcrossProfiles` |
| BK-84 | M5 | T/B | stock BookKeeper control topic remains writable/readable through Nereus GC | implemented E.3 ownership/control-path evidence: `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers`; destructive-GC coexistence remains covered separately by the activated retention gates |
| BK-85 | M5 | T | old/noncapable broker is excluded from ownership; reads/writes fail closed | `NereusBookKeeperCapabilityRolloverTest.excludesOldBroker` |
| BK-86 | M5 | T/C | capability epoch changes invalidate activation and safely resume after proof | `NereusBookKeeperCapabilityRolloverTest.reestablishesExactAuthority` |

## 10. Scale and chaos

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-87 | M6 | X/O | 1,001 roots in one shard + every other shard paginate from cold restart | `BookKeeperPrimaryWalScaleTest.scansHotAndAllShards` |
| BK-88 | M6 | X/O | exact max range x protection-slot Cartesian scan；next range/dynamic owner rejects or rolls before IO | `BookKeeperPrimaryWalScaleTest.scansMaximumLedgerInventory` |
| BK-89 | M6 | X/O | every fixed reader slot + mixed protection slots restart from empty tokens without scan-count races | `BookKeeperPrimaryWalScaleTest.restartsCompleteInventory` |
| BK-90 | M6 | X/O | 10,000 terminal roots use stack-bounded sequential visitation | `BookKeeperPrimaryWalScaleTest.visitsTenThousandWithoutStackGrowth` |
| BK-91 | M6 | X/O/S | 128 mixed BK/Object sources and 1,048,576 records task round-trips/runs | `BookKeeperMixedSourceTaskScaleTest.executesBothTaskLimits` + `MaterializationPlannerScaleTest.plansAndDurablyRoundTripsOneTaskAtBothSourceAndRecordLimits` |
| BK-92 | M6 | X/O | BK_ONLY generation zero、4,096 candidates and 4,097 fail-closed | `GenerationReadResolverTest.bookKeeperWalOnlyResolvesGenerationZeroWithoutAdmittingHigherGenerations` + `admitsExactlyFourThousandNinetySixGenerationCandidates` + `candidateOverflowFailsInsteadOfSilentlyIgnoringAHigherGeneration` |
| BK-93 | M6 | C/B/O | fresh runtimes recover allocation/write/seal/head/task/publication/delete response-loss cuts | allocator/recovery/append/retention cut tests plus the real Oxia/BK and Oxia/BK/S3 response-loss fixtures listed in `09-m6-executable-evidence-matrix.md` |
| BK-94 | M6 | C/T/B/O/S | two allocators、two brokers and two workers contend without double ownership/head/delete | `BookKeeperLedgerAllocatorTest.globalRootSerializesTwoStreamsThatChooseTheSameCandidate` + `NereusMaterializationContentionMultiBrokerIntegrationTest.twoBrokerWorkerRuntimesContendOnTheSameStreamsAndConvergeExactReads` |
| BK-95 | M6 | X/T | configured tasks/generations/hazard slots stay bounded；full hazard set rejects before provider IO | `BookKeeperLedgerAllocatorTest.fullHazardSetRejectsBeforeProviderIoWithoutClearingAnySlot` + typed configuration bound tests |
| BK-96 | M6 | T/B/O/S | full mixed-profile history, cursor, trim, GC and stock BK acceptance | `NereusBookKeeperMultiBrokerIntegrationTest.preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers` + `NereusMixedPrimaryProfilesMultiBrokerTest.coexistsAcrossProfiles` + async retention/fallback fixtures |

## 11. Traceability requirements

`bookKeeperPrimaryWalM6ScenarioEvidenceCheck` parses a machine-readable companion manifest generated from this table or
maintained beside it. It requires：

- exactly BK-01 through BK-96, no gap/duplicate；
- each row mapped to one implemented test method and one ordinary/final gate；
- final-gate XML proves the method executed, not skipped/up-to-date；
- real-service rows record exact Nereus/Pulsar/BK/Object/Oxia source/image locks；
- failure cuts record injected cut and expected durable rows/provider calls；
- status in README/design index matches evidence。

The companion `09-m6-executable-evidence-matrix.md` is the machine-checked exact method/gate manifest. Its checker、
the 123-task ordinary M6 gate and the 236-task complete delivery final gate pass at the locked Pulsar source；BK-M6 is
complete/final-gated。
