#!/usr/bin/env python3
"""Validate bounded READ_FENCED recovery without claiming native integration or dispatch."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-read-fenced-recovery-projection.json"
EXPECTED = {'schema': 'NEREUS_V2_M5_READ_FENCED_RECOVERY_PROJECTION_V2',
 'status': 'IMPLEMENTED_OBSERVATION_RECOVERY_NON_PROMOTABLE',
 'authorityWireVersion': 4,
 'observationBinding': ['PHYSICAL_RESOURCE',
                        'EXACT_FENCED_AUTHORITY_SHA256',
                        'READ_ATTEMPT',
                        'OBSERVATION_EPOCH',
                        'COORDINATOR_OWNER_FACT',
                        'CAPABILITY_FACT',
                        'ELIGIBILITY_SNAPSHOT'],
 'recovery': {'exactSameKeyCas': True,
              'refreshRequiresExactNextRevisionAndEpoch': True,
              'closedAdmissionNeverReopens': True,
              'resourceAndReadAttemptRemainFixed': True,
              'ownerChangeRequiresNativeFencingVerifier': True,
              'missingNativeVerifierRejectsCas1AndCas2': True,
              'fullFactVectorReread': True,
              'oldObservationCanBindNewEpoch': False,
              'sameOwnerCapabilityRefreshSupported': True,
              'failedRefreshPreservesPredecessor': True,
              'cas2DerivesOwnerAndCapabilityFromFence': True,
              'timeoutClearsFence': False,
              'failedQualifiedRefreshRecordsVetoAtSameKey': True,
              'failedCas2ValidationRecordsVetoAtSameKey': True,
              'vetoBlocksIntentUntilQualifiedRefresh': True,
              'identicalVetoRetryDoesNotGrowRevision': True,
              'lateRejectedValidationCannotOverwriteWinningRefresh': True,
              'cancelledObserverCannotAbandonVetoPersistence': True,
              'proofCollectionFailureCanRecordVetoWithoutFabricatedSnapshot': True},
 'focusedTests': {'coordinatorAndGuardTests': 35,
                  'newObservationRecoveryTests': 7,
                  'failures': 0,
                  'errors': 0,
                  'skipped': 0,
                  'newRecoveryVetoAndCompatibilityTests': 7,
                  'newDispatchRecoveryTests': 9},
 'nativeProtocolOwnerAdaptersIntegrated': False,
 'externalFullIdentityReaderIntegrated': False,
 'durableRecoveryVetoRecordImplemented': True,
 'intentTakeoverCapabilityRefreshIntegrated': True,
 'realOxiaRecoveryExecutionPresent': True,
 'completePhysicalDeleteComposition': False,
 'sourceBoundReceiptPresent': False,
 'physicalDeleteAuthority': False,
 'productionAuthority': False,
 'nativeMetadataExecution': {'gate': 'v2M5ReadFencedOxiaCheck',
                             'runner': 'scripts/run-v2-m5-read-fenced-oxia-check.sh',
                             'semanticAuthority': 'SYNTHETIC_FACTS_WITH_NATIVE_KEYS_VERSIONS_AND_HASHES',
                             'integrationCases': 6,
                             'separateJvmRestartPhases': 2,
                             'sameServerContainerRestarted': True,
                             'oldCallbackRejectedAfterRefresh': True,
                             'heldRefreshCannotOverwriteWinningNativeCas': True,
                             'sameBytesNewFactVersionRejected': True,
                             'missingOwnerVerifierRejectsRecovery': True,
                             'checkpointContainsAuthorityBodies': False,
                             'physicalDeletionInvoked': False,
                             'durableVetoSurvivedServerRestart': True,
                             'qualifiedRefreshClearsVetoAfterServerRestart': True,
                             'typedIntentSurvivedServerRestart': True},
 'recoveryVetoAuthorityWireVersion': 5,
 'ordinaryVersionFourBytesPreserved': True,
 'intentRecovery': {'wireVersion': 6,
                    'lastRefreshContextsOnly': True,
                    'exactNextDispatchAndObservationEpoch': True,
                    'originalReadFenceAndExternalIdentityPreserved': True,
                    'fullFreshEligibilityRequired': True,
                    'nativeOwnerVerifiedBeforeAndAfterExternalRead': True,
                    'nativeExternalIdentityRereadRequired': True,
                    'tokenBindsCapabilityEligibilityAndRefresh': True,
                    'hashOnlyTakeoverRejected': True,
                    'hashOnlyCompletionReadOnly': True,
                    'nativeAbsenceRequiredForNewTerminal': True,
                    'cancelledObserverCannotAbandonAcceptedCas': True,
                    'legacyHashOnlyTakeoverContextRejected': True,
                    'nativeBookKeeperIdentityReaderImplemented': True,
                    'nativeBookKeeperAndOxiaCompositionTest': True,
                    'nativeProtocolOwnerAndSemanticAuthority': False,
                    'physicalDeleteDispatchAdapterIntegrated': False,
                    'allProviderIdentityReadersIntegrated': False,
                    'authoritativeAbsenceCannotReappear': True}}

def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("read-fenced recovery projection differs or overclaims authority")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    base = root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc"
    checks = {
        "M5TargetDeleteAuthorityStateMachineV1.java": [
            "refreshIdentityRead", "Math.addExact(previousContext.observationEpoch(), 1)",
            "ownerChanged != successor.predecessorOwnerFenced().isPresent()",
            "snapshot.generation() != revision", "previous.attemptIdSha256()",
            "current.closedWriterFenceEpoch()", "externalIdentity.fencedAuthoritySha256()",
            "another resource or observation epoch",
            "context.coordinatorOwner().valueSha256()", "context.capability().valueSha256()",
            "recordRecoveryVeto", "current.recoveryVeto().isPresent()", "authoritatively absent immutable target cannot reappear during refresh",
            "recovery veto requires a qualified observation refresh before intent"],
        "M5TargetDeleteAuthorityCoordinatorV1.java": [
            "DeleteObservationAuthorityVerifierV2.unsupported()", "observationAuthority.requirePredecessorFenced(",
            "observationAuthority.requireCurrent(", "requireObservationAuthority(candidate)",
            "requireObservationAuthority(exact.authority())", "observation and eligibility authority conflict",
            "validateOrRecordRecoveryVeto", "new RecoveryRejectedException", "recordRecoveryVeto(exact, reason",
            "veto.rejectedContextSha256().equals(contextSha)", "refreshDispatch", "completeAbsent",
            "externalReader.rereadExact(current.externalIdentity().orElseThrow())",
            "requireObservationAuthority(candidate, successor, snapshot)", "return operation.thenApply(result -> result)",
            "hash-only dispatch takeover requires typed context"],
        "M5TargetDeleteAuthorityCodecV1.java": [
            "BASE_VERSION = 4", "VETO_VERSION = 5", "VERSION = 6", "writeDispatchRefresh", "readDispatchRefresh", "writeObservationContext", "readObservationContext", "identity.fencedAuthoritySha256()"],
        "M5TargetDeleteAuthorityRecordsV1.java": [
            "PhysicalResourceIdV2 resourceId", "Sha256Digest fencedAuthoritySha256",
            "DeleteObservationContextV2 observationContext"],
        "DeleteObservationAuthorityVerifierV2.java": [
            "UnsupportedOperationException", "native deletion observation authority is not installed",
            "native deletion owner fencing is not installed"],
        "DeleteDispatchRefreshV2.java": ["predecessorAuthoritySha256", "previous", "current", "externalObservation",
            "changedOwner != current.predecessorOwnerFenced().isPresent()"],
        "DeleteExternalIdentityReaderV2.java": ["UnsupportedOperationException", "rereadExact"],
    }
    for name, literals in checks.items():
        source = (base / name).read_text()
        if any("".join(literal.split()) not in "".join(source.split()) for literal in literals):
            raise ValueError("read-fenced recovery source omits required predicate: " + name)
    native_reader = (root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperDeleteIdentityReaderV2.java").read_text()
    for literal in ("client.captureExactTarget(handle)", "expected.resourceId().equals(resource)",
                    "expected.exactIdentityBytes().equals(encodeIdentity(result.exactTarget()))",
                    "case DEFINITIVELY_ABSENT", "exact.metadataSha256()"):
        if "".join(literal.split()) not in "".join(native_reader.split()):
            raise ValueError("native BK identity reader omits exact predicate: " + literal)
    print("PASS_V2_M5_READ_FENCED_RECOVERY_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
