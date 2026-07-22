# BK-M6 Executable Evidence Matrix

This matrix is the machine-checked completion surface for BK-87 through BK-96. Each token identifies an annotated
test method in the exact Nereus or locked local Pulsar source. A row is complete only when its owning gate executes
the listed focused test or an aggregate that contains it；a class name or prose-only claim is not evidence.

| ID | Executable evidence | Owning gate |
| --- | --- | --- |
| BK-87 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperPrimaryWalScaleTest.java#scansHotAndAllShards` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-88 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperPrimaryWalScaleTest.java#scansMaximumLedgerInventory` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-89 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperPrimaryWalScaleTest.java#restartsCompleteInventory` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-90 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperPrimaryWalScaleTest.java#visitsTenThousandWithoutStackGrowth` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-91 | `nereus:nereus-materialization/src/test/java/com/nereusstream/materialization/BookKeeperMixedSourceTaskScaleTest.java#executesBothTaskLimits`; `nereus:nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationPlannerScaleTest.java#plansAndDurablyRoundTripsOneTaskAtBothSourceAndRecordLimits` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-92 | `nereus:nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java#bookKeeperWalOnlyResolvesGenerationZeroWithoutAdmittingHigherGenerations`; `nereus:nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java#admitsExactlyFourThousandNinetySixGenerationCandidates`; `nereus:nereus-core/src/test/java/com/nereusstream/core/read/GenerationReadResolverTest.java#candidateOverflowFailsInsteadOfSilentlyIgnoringAHigherGeneration` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-93 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerAllocatorTest.java#convergesEveryAppliedMetadataResponseLossInTheAllocationChain`; `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerRecoveryTest.java#recoversEverySealCut`; `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java#preparedIntentResponseLossRetriesTheSameRangeWithoutAnotherBookKeeperWrite`; `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperAppendRecoveryCoordinatorTest.java#reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal`; `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalRetentionGateTest.java#everyGcRootCasConvergesAfterAppliedResponseLoss`; `nereus:nereus-pulsar-adapter/src/bkM2IntegrationTest/java/com/nereusstream/pulsar/BookKeeperWalOnlyOxiaBkIntegrationTest.java#realOxiaStableAppendResponseLossReusesOneBookKeeperRangeAndRepairsGenerationZero`; `nereus:nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java#freshRuntimesConvergeAppliedTaskSourceOutputAndPublicationResponseLoss` | `bookKeeperPrimaryWalM6ChaosCheck` |
| BK-94 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerAllocatorTest.java#globalRootSerializesTwoStreamsThatChooseTheSameCandidate`; `pulsar:pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMaterializationContentionMultiBrokerIntegrationTest.java#twoBrokerWorkerRuntimesContendOnTheSameStreamsAndConvergeExactReads`; `pulsar:pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMixedPrimaryProfilesMultiBrokerTest.java#coexistsAcrossProfiles` | `bookKeeperPrimaryWalM6CompatibilityCheck` |
| BK-95 | `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperLedgerAllocatorTest.java#fullHazardSetRejectsBeforeProviderIoWithoutClearingAnySlot`; `nereus:nereus-bookkeeper/src/test/java/com/nereusstream/bookkeeper/BookKeeperWalConfigurationTest.java#rejectsUnsafeBoundsAndDeletionDefaultsClosed`; `nereus:nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/NereusRuntimeConfigurationTest.java#bookKeeperConfigurationIsExplicitBoundedAndRequiredByBookKeeperDefault` | `bookKeeperPrimaryWalM6ScaleCheck` |
| BK-96 | `pulsar:pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusBookKeeperMultiBrokerIntegrationTest.java#preservesOwnershipProjectionAndStockIsolationAcrossBothTakeovers`; `pulsar:pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMixedPrimaryProfilesMultiBrokerTest.java#coexistsAcrossProfiles`; `pulsar:pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusAsyncRetentionMultiBrokerIntegrationTest.java#repairsAsyncHistoryAndLogicallyTrimsEvictedBacklogAcrossOwnershipCuts`; `nereus:nereus-pulsar-adapter/src/bkM3IntegrationTest/java/com/nereusstream/pulsar/BookKeeperAsyncObjectOxiaBkS3IntegrationTest.java#missingCommittedObjectVetoesBookKeeperRetirementAndFallsBackToExactRange` | `bookKeeperPrimaryWalM6CompatibilityCheck` |

The scale gate owns deterministic high-cardinality protocol limits. The chaos gate adds real Oxia/BookKeeper/S3
fresh-runtime response-loss recovery. The compatibility gate composes the retry-disabled production BK rollout with
the already final-gated two-broker/two-worker F4 contention and async retention fixtures；no single projection or
worker becomes a new correctness owner.

“Process loss” in this matrix has one precise executable meaning：inject the durable/provider cut，discard every
process-local handle/cache/future，then construct a fresh runtime from Oxia、BookKeeper and Object-store facts. It does
not introduce an OS-subprocess transport protocol or treat `kill -9` itself as correctness evidence. Real broker
concurrency and ownership loss are covered separately by the retry-disabled two-broker Pulsar fixtures in BK-94 and
BK-96. Together these two evidence forms prove restart independence without making process topology a new authority.

## Completion record

- `bookKeeperPrimaryWalM6ScaleCheck --rerun-tasks` passed 35/35 outer tasks；
- `bookKeeperPrimaryWalM6ChaosCheck --rerun-tasks` passed 76/76 outer tasks；
- `bookKeeperPrimaryWalM6Check --rerun-tasks` passed 123/123 outer tasks in 10m22s；
- `bookKeeperPrimaryWalFinalCheck --rerun-tasks` passed 236/236 outer tasks in 30m57s，including Phase 1.5、Phase
  2/3/4 final gates and BK-M1–M6 final predecessors；
- all current Pulsar-backed evidence is source-locked to
  `master@2f9c1eb93be96e2036fbdc8c5e39545f21fa6200`，with retry count zero for the production contention fixture.

BK-M6 and the complete F1-BK / BookKeeper Primary WAL Delivery are complete/final-gated on 2026-07-22. Online profile
migration remains a separate future delivery and is not implied by this completion claim.
