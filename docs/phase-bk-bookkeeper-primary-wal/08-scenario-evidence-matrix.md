# Scenario and Evidence Matrix

## 1. Status and evidence levels

BK-01 through BK-10 executed successfully on 2026-07-19 through `bookKeeperPrimaryWalM1Check` and the 199-task
`bookKeeperPrimaryWalM1FinalCheck` aggregate against local Pulsar
`master@eaf7b9a704890a9265c21f30d9f351e02d00c600`。BK-11、BK-12 and the deterministic all-shard precursor for BK-19
passed on 2026-07-19 under `bookKeeperPrimaryWalM2MetadataCheck`。BK-20 now passes its explicit D-level lifecycle /
immutable-drift contract, and BK-19 additionally passes a cold real-Oxia all-256-root/all-16-slot-shard scan。
The 2026-07-19 `bookKeeperPrimaryWalM2RealServiceCheck` checkpoint adds real Oxia + BookKeeper evidence for BK-14、
the matching-create/retention-veto portion of BK-17、BK-19、BK-21、BK-22、BK-24、BK-26、BK-27、BK-30、BK-32、BK-37、
BK-38、BK-39、BK-40、
BK-41、BK-42、BK-47、BK-48、BK-49、BK-52、BK-53、BK-54、BK-56
and BK-55, including
a delayed physical create after an absent probe and a fresh process between the two delete-absence observations；it
does not claim the remaining M2 rows。The focused allocator gate also adds D checkpoints for every applied metadata
response-loss operation in BK-13、foreign collision/new-candidate behavior in BK-15、global-root contention in BK-16
and randomized monotonic writer/range behavior in BK-18；their remaining O/B levels stay open。The deterministic
append/read gate additionally proves pre-provider profile/oversize rejection (BK-23)、same-target/no-rewrite
generation-zero repair (the D checkpoint of BK-29) and an unrepresentable `DEFERRED_SYNC` configuration plus an
adapter-forced empty write-flag set (BK-36)。The deterministic retention-authority checkpoint adds complete BK-50 and
BK-51 owner-domain cuts；only the reader and mandatory-range subsets currently have real B/O evidence。BK-13 through BK-96
otherwise remain required target evidence and are currently
**not complete**。During
implementation, each row
receives an exact test method、gate、source lock、date and result. No implementation row may be marked covered by prose
only.

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
| BK-13 | M2 | O | intent -> durable slot CLAIMED/CREATE_STARTED -> root -> active writer survives every Oxia CAS response loss | production-adapter D checkpoint: `BookKeeperLedgerAllocatorTest.convergesEveryAppliedMetadataResponseLossInTheAllocationChain`; remaining O evidence: `BookKeeperAllocatorOxiaIT.convergesEveryAllocationCut` |
| BK-14 | M2 | B/O | exact create response loss persists slot + permanent hazard；matching metadata is recovery-opened/sealed because the write handle is unrecoverable | `BookKeeperWalOnlyOxiaBkIntegrationTest.createResponseLossRecoverySealsTheExactLedgerAndKeepsTheHazardSlot` |
| BK-15 | M2 | B/O | candidate already owned by stock/foreign ledger is never deleted and a new id wins | D checkpoint: `BookKeeperLedgerAllocatorTest.foreignCreateCollisionIsQuarantinedAndTheNextCandidateWinsWithoutDelete`; remaining B/O evidence: `BookKeeperAllocatorIT.doesNotDeleteForeignCollision` |
| BK-16 | M2 | D/O | two streams choose the same candidate; global root admits one allocation | D checkpoint: `BookKeeperLedgerAllocatorTest.globalRootSerializesTwoStreamsThatChooseTheSameCandidate`; remaining O evidence: `BookKeeperAllocatorContentionIT.serializesGlobalLedgerIdentity` |
| BK-17 | M2 | B/O/C | `CREATE_UNCERTAIN` stays slot-consuming；matching late create seals with permanent GC veto, foreign quarantines | matching late-create + permanent GC-veto checkpoint: `BookKeeperWalOnlyOxiaBkIntegrationTest.createResponseLossRecoverySealsTheExactLedgerAndKeepsTheHazardSlot`; deterministic foreign checkpoint: `BookKeeperLedgerAllocatorTest.boundedUncertainSlotRecoveryQuarantinesForeignLateCreateWithoutDeletingIt`; remaining: real foreign late-create cut |
| BK-18 | M2 | D/O | writer state never reuses segment/entry ids and physical-byte/range counters never move backward after conflict/restart | D checkpoint: `BookKeeperWriterStatePropertyTest.isMonotonicAcrossCasSchedules`; remaining O evidence: `BookKeeperWriterStatePropertyOxiaIT` |
| BK-19 | M2 | O | all 256 root + 16 fixed allocation-slot shards scan empty opaque continuations without omission | `BookKeeperWalOnlyOxiaBkIntegrationTest.realOxiaColdScanCoversEveryRootAndAllocationSlotShard` |
| BK-20 | M2 | D | invalid lifecycle fields/transitions and immutable identity drift fail closed | `BookKeeperLedgerTransitionsTest.rejectsIllegalTransitionsAndImmutableIdentityDrift` + `rejectsInvalidLifecycleFieldsBeforePersistence` |

## 4. Append, recovery, and fencing

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-21 | M2 | B/O | one ordinary entry is quorum durable, head reachable and returns stable offset | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-22 | M2 | B/O | multi-entry append occupies exact consecutive ids and one target checksum | `BookKeeperPrimaryWalAppenderTest.reservesWritesAndReturnsOneStableExactRange` + `BookKeeperWalOnlyOxiaBkIntegrationTest.multiEntryAppendUsesOneExactConsecutiveBookKeeperRange` |
| BK-23 | M2 | D/B | future/missing-adapter profile、invalid configuration and oversize batch reject before BK calls；both V1 durability values remain valid | `BookKeeperStorageProfileResolverTest.rejectsObjectFutureBkProfilesAndMissingAdaptersBeforeIo` + `BookKeeperStreamStorageIntegrationTest.unsupportedProfileAndOversizeBatchReachNoBookKeeperOperation` + `BookKeeperWalConfigurationTest` |
| BK-24 | M2 | D/B/O | first/middle/last write failure taints and seals ledger; no tail reuse | `BookKeeperPrimaryWalAppenderTest.partialWriteAbandonsRangeRecoverySealsLedgerAndNextAppendAllocatesFreshLedger` + `BookKeeperWalOnlyOxiaBkIntegrationTest.firstMiddleAndLastWriteFailureSealTheLedgerBeforeReuse` |
| BK-25 | M2 | D/B/O/C | crash after range + three mandatory RESERVED protection slots but before write becomes known-not-committed | D checkpoint: `BookKeeperAppendRecoveryCoordinatorTest.nonDurableWritingCutIsSealedAndProvenNotCommitted`; real process cut remains open |
| BK-26 | M2 | D/B/O/C | full writes before commit intent resume only under unchanged session | `BookKeeperAppendRecoveryCoordinatorTest.currentSessionCommitsTheSameDurableRangeWithoutAnotherBookKeeperWrite` + real fresh-client/runtime `BookKeeperWalOnlyOxiaBkIntegrationTest.restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange`；abrupt-kill cut remains open |
| BK-27 | M2 | D/B/O/C | new session abandons unreachable old-writer full range and uses new ledger | `BookKeeperAppendRecoveryCoordinatorTest.newSessionAbandonsUnreachableDurableRangeAndAllocatesAnotherLedger` (including exact abandoned-owner retirement) + real expired-session/fresh-client `BookKeeperWalOnlyOxiaBkIntegrationTest.restartRecoveryReusesCurrentSessionRangeAndFencesExpiredSessionRange`；abrupt-kill cut remains open |
| BK-28 | M2 | D/O/C | commit intent/protection/head response loss returns same committed target/result | D checkpoints: `BookKeeperAppendRecoveryCoordinatorTest.preparedIntentResponseLossRetriesTheSameRangeWithoutAnotherBookKeeperWrite` + `reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal`; production Oxia response-loss cuts remain open |
| BK-29 | M2 | D/O/C | head reachable/gen0 missing repairs same target without BK write | D checkpoints: `BookKeeperStreamStorageIntegrationTest.reachableHeadRecoveryRepairsGenerationZeroWithoutRewritingBookKeeper` + `BookKeeperAppendRecoveryCoordinatorTest.reachableHeadResponseLossRepairsFromTheSameRangeAfterLedgerSeal`; production Oxia response-loss cut remains open |
| BK-30 | M2 | B/O | entry/physical-byte/append-range/age rollover occurs before batch, never splits it and keeps dense offsets | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` + `byteRangeAndAgeRolloverPreserveWholeBatchesAndDenseOffsets` |
| BK-31 | M2 | B/O/C | crash in ACTIVE->SEALING->SEALED converges exact closed LAC/length | D response-loss checkpoint: `BookKeeperLedgerRecoveryTest.recoversEverySealCut`; abrupt process cut remains open |
| BK-32 | M2 | B/O | new owner recovery-open fences old handle; old owner cannot head-commit | `BookKeeperWalOnlyOxiaBkIntegrationTest.newOwnerRecoveryOpenFencesLiveOldHandleAndPreventsOldHeadCommit` |
| BK-33 | M2 | B/O | two recovery owners contend; one new active ledger wins | D checkpoint: `BookKeeperLedgerRecoveryTest.serializesTwoRecoveryOwners`; real B/O contention remains open |
| BK-34 | M2 | D | buffer/permit counts return to zero on success/failure/timeout/cancel/close | `BookKeeperAppenderResourceTest.releasesEveryOwnedResource` + `BookKeeperPreparedPrimaryAppendTest` + `BookKeeperClientApiContractTest` |
| BK-35 | M2 | D/B | one monotonic deadline spans allocation/write/commit and does not reset | D checkpoint: `BookKeeperAppenderDeadlineTest.propagatesRemainingBudget`; real B deadline cut remains open |
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
| BK-43 | M2 | P | raw Pulsar Entry properties/payload round-trip through BK generation zero | D checkpoint: `PulsarEntryOpaqueRoundTripTest.preservesUnbatchedAndCompressedBatchBytesPropertiesOrderingKeyAndMiddleBatchMessageId` + `NereusBookKeeperManagedLedgerIntegrationTest.facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero`; final: `NereusBookKeeperEntryIntegrationTest.preservesOpaqueEntryBytes` |
| BK-44 | M2 | P | ordinary and batched `MessageIdAdv` use virtual identity, not BK ledger id | partial D checkpoint for ordinary virtual Position only: `NereusBookKeeperManagedLedgerIntegrationTest.facadePreservesEntryBytesAndVirtualPositionOverBookKeeperGenerationZero`; final: `NereusBookKeeperEntryIntegrationTest.preservesVirtualMessageIds` |
| BK-45 | M2 | P | seek/history after rollover/restart returns same MessageIds | `NereusBookKeeperEntryIntegrationTest.preservesSeekAcrossRollover` |
| BK-46 | M2 | P | F3 ack/snapshot/hydration remains logical across physical ledgers | `NereusBookKeeperCursorIntegrationTest.preservesCursorTruth` |

## 6. Retention and deletion

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-47 | M2 | O/B | BK_ONLY trim below range end cannot retire/delete range | `BookKeeperWalOnlyOxiaBkIntegrationTest.partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes` |
| BK-48 | M2 | O/B | one ledger with trimmed + live ranges remains physical | `BookKeeperWalOnlyOxiaBkIntegrationTest.partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes` |
| BK-49 | M2 | O/B | all ranges durably trimmed retire owners/protections then whole ledger | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-50 | M2 | D/O | fixed protection-slot contention never exceeds Cartesian bound；invalid/unstable inventory vetoes collection | D checkpoint: `BookKeeperWalRetentionGateTest.failsClosedOnIncompleteAuthority`；real O contention/scan cut remains open |
| BK-51 | M2 | D/B/O | reader/task/repair/reservation/writer vetoes are each enforced | D complete-owner checkpoint: `BookKeeperWalRetentionGateTest.enforcesEveryVetoDomain`；real reader: `BookKeeperWalOnlyOxiaBkIntegrationTest.realReaderSlotsArePerProcessBoundedAndFinalPinRevalidationFailsClosed`；real mandatory-range protection: `partialRangeAndMixedLedgerTrimNeverDeleteLiveBookKeeperBytes`；real task/repair/writer cuts remain open |
| BK-52 | M2 | D/O/C | reference appears after MARKED; root unmarks to SEALED | D: `BookKeeperWalRetentionGateTest.referenceAppearingAfterMarkUnmarksToSealedBeforeDelete`; O/B: `BookKeeperWalOnlyOxiaBkIntegrationTest.referenceAfterMarkUnmarksAndSafeGcModesNeverDelete`; independent-process C remains open |
| BK-53 | M2 | B/O/C | delete response loss reloads metadata before same-intent retry | `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover` |
| BK-54 | M2 | B/O/C | namespace drift or late-create hazard stops before delete；foreign/reappeared same-id ledger is quarantined across validate/delete cut | B/O checkpoint: `BookKeeperWalOnlyOxiaBkIntegrationTest.foreignLedgerRecreationAndNamespaceDriftStopBeforePhysicalDelete`; independent-process C remains open |
| BK-55 | M2 | B/O/C | two separated absence observations and root CAS loss converge DELETED | checkpoint fresh-process dual absence: `BookKeeperWalOnlyOxiaBkIntegrationTest.restartPreservesExactTargetsAndLostDeleteResponseConvergesAfterRollover`; remaining root-CAS-loss cut: `BookKeeperLedgerGcIT.convergesDualAbsenceAcrossRestart` |
| BK-56 | M2 | D/O | dry-run/default-off mode performs no root/provider mutation | `BookKeeperWalRetentionGateTest.disabledAndDryRunGcNeverMutateRootOrProvider` + `BookKeeperWalOnlyOxiaBkIntegrationTest.referenceAfterMarkUnmarksAndSafeGcModesNeverDelete` |

## 7. Async materialization

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-57 | M3 | D/O | BK `SourceGeneration` task V2 round-trips exact tagged target/identity | `BookKeeperMaterializationTaskCodecTest.roundTripsBookKeeperSource` |
| BK-58 | M3 | B/O/S | async append acks at head while Object generation is held absent | `BookKeeperAsyncObjectIT.acksBeforeObjectPublication` |
| BK-59 | M3 | B/O/S | reads use BK while task pending, then exact higher Object generation | `BookKeeperAsyncObjectIT.switchesGenerationWithoutByteDrift` |
| BK-60 | M3 | O/C | task exists before source protection; gen0 retains and restart reconciles | `BookKeeperSourceProtectionIT.recoversTaskProtectionCut` |
| BK-61 | M3 | B/O/S/C | worker restart re-reads exact BK range and reuses task/output intent | `BookKeeperMaterializationRecoveryIT.recoversEveryWorkerCut` |
| BK-62 | M3 | B/O/S | ordinary/batched Entry bytes and MessageIds equal before/after NCP1 | `BookKeeperAsyncObjectEntryIT.preservesPulsarProjection` |
| BK-63 | M3 | D/O | lag records/bytes/age derive from coverage, not ledger/task count | `BookKeeperMaterializationLagTest.usesSharedAuthoritativeLag` |
| BK-64 | M3 | D/B | lag throttle remeasures; reject occurs before next BK IO | `BookKeeperAsyncAdmissionTest.throttlesAndRejectsBeforeWal` |
| BK-65 | M3 | O/S | sealed ledger final single source creates normal deterministic task | `BookKeeperSealedLedgerFlushIT.materializesOneRemainingSource` |
| BK-66 | M3 | O/B/S | healthy replacement retires BK source; broken/PREPARED output does not | `BookKeeperSourceRetirementIT.requiresReadableCommittedReplacement` |
| BK-67 | M3 | O/B/S/C | generation/source/protection response-loss cuts precede whole-ledger delete | `BookKeeperSourceRetirementIT.recoversMetadataFirstCuts` |

## 8. Sync completion

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-68 | M4 | D | BK sync maps to REQUIRED_OBJECT_GENERATION, not gen0 durability | `BookKeeperCompletionPolicyTest.separatesDurabilityAndCompletion` |
| BK-69 | M4 | D | weaker explicit policy for sync stream is rejected before IO | `BookKeeperCompletionPolicyTest.rejectsWeakerSyncBoundary` |
| BK-70 | M4 | B/O/S | no producer ack until exact COMMITTED Object generation reads successfully | `BookKeeperSyncObjectIT.acksOnlyAfterExactObjectRead` |
| BK-71 | M4 | B/O/S | consumer reads committed BK bytes while producer waits for Object | `BookKeeperSyncObjectIT.keepsHeadAsVisibilityPoint` |
| BK-72 | M4 | O/S/C | crash at task create/claim/upload/publish/read/ack reuses same task/range | `BookKeeperSyncObjectRecoveryIT.recoversEveryCompletionCut` |
| BK-73 | M4 | D/O | already committed current-policy coverage satisfies request after exact proof | `RequiredObjectGenerationCoordinatorTest.reusesExistingCoverage` |
| BK-74 | M4 | D/O | one-source task id is deterministic and races converge | `RequiredObjectGenerationCoordinatorTest.convergesTaskCreation` |
| BK-75 | M4 | B/O/S | object failure/timeout after head exposes KNOWN_COMMITTED + same attempt | `BookKeeperSyncObjectIT.exposesKnownCommittedFailure` |
| BK-76 | M4 | B/O/S | recoverAppend returns original stable result and never writes another BK range | `BookKeeperSyncObjectRecoveryIT.neverDuplicatesPrimaryAppend` |

## 9. Pulsar rollout and aggregate compatibility

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-77 | M5 | D/P | borrowed client is closed zero times by Nereus and once by stock owner | `NereusBookKeeperBorrowedClientTest.closesOnlyAtStockOwner` |
| BK-78 | M5 | P | missing/mismatched/revoked provisioned namespace or broker config/capability rejects first-create before IO；runtime never auto-creates reservation | `NereusBookKeeperCapabilityTest.rejectsUnreadyCluster` |
| BK-79 | M5 | P | first-create persists exact profile; reopen ignores changed default | `NereusBookKeeperProfileAdmissionTest.keepsImmutableProfile` |
| BK-80 | M5 | P | explicit existing-profile mutation/online migration is rejected | `NereusBookKeeperProfileAdmissionTest.rejectsProfileMutation` |
| BK-81 | M5 | P | loaded/unloaded/partitioned routes use durable profile/readiness | `NereusBookKeeperAdminRoutingTest.routesExactDurableProfile` |
| BK-82 | M5 | T/B/O | unload/failover/restart/rejoin/reverse takeover preserves history/MessageIds | `NereusBookKeeperMultiBrokerIT.preservesOwnershipAndProjection` |
| BK-83 | M5 | T/B/O/S | async/sync topics and Object-WAL topics coexist on both brokers | `NereusMixedPrimaryProfilesMultiBrokerIT.coexistsAcrossProfiles` |
| BK-84 | M5 | T/B | stock BookKeeper control topic remains writable/readable through Nereus GC | `NereusBookKeeperMultiBrokerIT.neverTouchesStockLedger` |
| BK-85 | M5 | T | old/noncapable broker is excluded from ownership; reads/writes fail closed | `NereusBookKeeperCapabilityRolloverIT.excludesOldBroker` |
| BK-86 | M5 | T/C | capability epoch changes invalidate activation and safely resume after proof | `NereusBookKeeperCapabilityRolloverIT.reestablishesExactAuthority` |

## 10. Scale and chaos

| ID | Milestone | Level | Scenario | Target evidence |
| --- | --- | --- | --- | --- |
| BK-87 | M6 | X/O | 1,001 roots in one shard + every other shard paginate from cold restart | `BookKeeperLedgerRootScaleIT.scansHotAndAllShards` |
| BK-88 | M6 | X/O | exact max range x protection-slot Cartesian scan；next range/dynamic owner rejects or rolls before IO | `BookKeeperLedgerProtectionScaleIT.scansMaximumLedgerInventory` |
| BK-89 | M6 | X/O | every fixed reader slot + mixed protection slots restart from empty tokens without scan-count races | `BookKeeperLedgerReferenceScaleIT.restartsCompleteInventory` |
| BK-90 | M6 | X/O | 10,000 terminal roots use stack-bounded sequential visitation | `BookKeeperLedgerGcScaleIT.visitsTenThousandWithoutStackGrowth` |
| BK-91 | M6 | X/O/S | 128 mixed BK/Object sources and 1,048,576 records task round-trips/runs | `BookKeeperMixedSourceTaskScaleIT.executesBothTaskLimits` |
| BK-92 | M6 | X/O | 4,096 generation candidates resolve; 4,097 fails closed | `BookKeeperGenerationScaleIT.enforcesCandidateBoundary` |
| BK-93 | M6 | C/B/O | independent processes recover every allocation/write/seal/head/delete cut | `BookKeeperPrimaryWalProcessCutIT` |
| BK-94 | M6 | C/T/B/O/S | two brokers + workers contend on writer/task/GC without double head/delete | `NereusBookKeeperContentionMultiBrokerIT.convergesSharedAuthority` |
| BK-95 | M6 | X/T | configured topics/workers/deletes/hazard slots stay bounded；full hazard set rejects before next create and never clears for availability | `NereusBookKeeperLoadMultiBrokerIT.enforcesResourceBounds` |
| BK-96 | M6 | T/B/O/S | full mixed-profile history, cursor, trim, GC and stock BK acceptance | `NereusBookKeeperAggregateMultiBrokerIT.finalAcceptance` |

## 11. Traceability requirements

`bookKeeperPrimaryWalM6ScenarioEvidenceCheck` parses a machine-readable companion manifest generated from this table or
maintained beside it. It requires：

- exactly BK-01 through BK-96, no gap/duplicate；
- each row mapped to one implemented test method and one ordinary/final gate；
- final-gate XML proves the method executed, not skipped/up-to-date；
- real-service rows record exact Nereus/Pulsar/BK/Object/Oxia source/image locks；
- failure cuts record injected cut and expected durable rows/provider calls；
- status in README/design index matches evidence。

Until that checker and all final tasks pass, the aggregate remains “not implemented”.
