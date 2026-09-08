#!/usr/bin/env python3
"""Validate the typed eligibility projection without claiming native proof-production evidence."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-delete-eligibility-projection.json"
EXPECTED = {'schema': 'NEREUS_V2_M5_DELETE_ELIGIBILITY_PROJECTION_V2',
 'status': 'IMPLEMENTED_TYPED_PREDICATES_NON_PROMOTABLE',
 'wire': {'magic': 'M5ES',
          'version': 2,
          'maximumSnapshotBytes': 786432,
          'maximumMembers': 64,
          'maximumReplacements': 256,
          'authorityWireVersion': 4},
 'reasons': ['REPLACED_REPRESENTATION', 'LOGICAL_EXPIRY', 'UNPUBLISHED_ARTIFACT'],
 'referenceScopes': ['LOGICAL_OBLIGATION', 'OLD_PHYSICAL_REFERENCE', 'BOTH'],
 'semanticAspects': ['READ_RESULTS',
                     'RECOVERY',
                     'PRODUCER_STATE',
                     'TRANSACTIONS',
                     'LEADER_EPOCH',
                     'TIMESTAMP_LOOKUP',
                     'INDEX_COVERAGE',
                     'NATIVE_SOURCE_AUTHORITY'],
 'gates': {'unqualifiedOpenMayFence': False,
           'writerInvalidatesQualifiedSnapshot': True,
           'ticketClearRestoresSnapshot': False,
           'qualificationRequiresExactNextRevision': True,
           'fullVectorReadBeforeQualificationCas1Cas2': True,
           'replacementRequiresLogicalExpiry': False,
           'replacementRequiresCompleteSemanticTransfer': True,
           'expiryRequiresTrimAndEveryFloor': True,
           'publishedRequiresExactM4Released': True,
           'unpublishedRequiresTerminalNoAdoptionAndProtectionClosure': True,
           'nativeRetainVetoesReplacement': True},
 'nativeProtocolProofProducersIntegrated': False,
 'namespaceRouteAdmissionIntegrated': False,
 'completePhysicalDeleteComposition': False,
 'realDependencyReclamationEvidencePresent': False,
 'sourceBoundReceiptPresent': False,
 'physicalDeleteAuthority': False,
 'productionAuthority': False}


def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("typed deletion eligibility projection differs or overclaims evidence")


def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    base = root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc"
    records = (base / "DeleteEligibilitySnapshotV2.java").read_text()
    state = (base / "M5TargetDeleteAuthorityStateMachineV1.java").read_text()
    coordinator = (base / "M5TargetDeleteAuthorityCoordinatorV1.java").read_text()
    codec = (base / "M5TargetDeleteAuthorityCodecV1.java").read_text()
    for source, literals in (
        (records, EXPECTED["reasons"] + EXPECTED["referenceScopes"] + EXPECTED["semanticAspects"] + [
            "MAX_SNAPSHOT_BYTES = 786432", "MAX_MEMBERS = 64", "MAX_REPLACEMENTS = 256",
            "replacement omits",
            "minimumSafeFloor() < trim.newFrontier()", "M4ReadControlCodecV1.decodeProtection",
            "neverAdmittedProtection().isEmpty()", "authorityFacts()"]),
        (state, ["qualifyEligibility", "CAS-1 requires a complete typed eligibility snapshot",
                 "snapshot.generation() != Math.addExact(current.authorityRevision(), 1)"]),
        (coordinator, ["requireFreshEligibility(snapshot)", "snapshot.authorityFacts()",
                       "eligibility authority changed", "eligibility authority is absent"]),
        (codec, ["BASE_VERSION = 4", "DeleteEligibilityCodecV2.decode", "current.eligibilitySnapshot()",
                 "tickets.isEmpty()", "DeleteEligibilityCodecV2.encode"]),
    ):
        if any(literal not in source for literal in literals):
            raise ValueError("typed eligibility source omits a required predicate")
    for name in ("prepareIdentityRead", "bindDeleteIntent"):
        method = coordinator[coordinator.index("public CompletionStage<MutationResultV1> " + name):]
        method = method[:method.index("\n    public ", 1)]
        if "requireObservationAuthority(" not in method:
            raise ValueError("CAS window no longer rereads the full eligibility vector")
    verifier = coordinator[coordinator.index("private CompletionStage<Void> requireObservationAuthority"):]
    if "authorityFacts()" not in verifier or "requireFreshFacts(List.copyOf(facts.values()))" not in verifier:
        raise ValueError("CAS observation authority no longer includes the full eligibility vector")
    print("PASS_V2_M5_DELETE_ELIGIBILITY_NON_PROMOTABLE")


if __name__ == "__main__":
    validate(ROOT)
