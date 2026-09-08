/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.gc;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.MemberSnapshot;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.NativeDisposition;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReclamationReason;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.ReplacementEvidence;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.SemanticTransfer;
import com.nereusstream.storage.object.gc.DeleteEligibilitySnapshotV2.UnpublishedEvidence;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.ProtocolCoverage;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.ProtectionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtection;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SourceProtectionIdentity;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BindingTrimFrontierV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.M4ReleaseBindingV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceDispositionV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceFreeProofV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceScanSummaryV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorObservationV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetentionFloorSnapshotV1;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/** Structurally complete synthetic evidence for pure/in-memory tests, never native integration evidence. */
final class M5DeleteEligibilityTestFixtures {
    private M5DeleteEligibilityTestFixtures() {}

    static DeleteObservationContextV2 observation() {
        return new DeleteObservationContextV2(
                1, fact("/dispatch/owner/one"), fact("/dispatch/capability/one"), Optional.empty());
    }

    static DeleteObservationAuthorityVerifierV2 syntheticObservationVerifier() {
        return new DeleteObservationAuthorityVerifierV2() {
            @Override
            public java.util.concurrent.CompletionStage<Void> requireCurrent(
                    PhysicalResourceIdV2 resource, DeleteObservationContextV2 current) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public java.util.concurrent.CompletionStage<Void> requirePredecessorFenced(
                    PhysicalResourceIdV2 resource,
                    DeleteObservationContextV2 previous,
                    DeleteObservationContextV2 successor) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
        };
    }

    static DeleteEligibilitySnapshotV2 replacement(PhysicalResourceIdV2 resource, long generation) {
        return snapshot(
                resource, ReclamationReason.REPLACED_REPRESENTATION, generation, 0, PositionDomain.KAFKA_OFFSET);
    }

    static DeleteEligibilitySnapshotV2 snapshot(
            PhysicalResourceIdV2 resource,
            ReclamationReason reason,
            long generation,
            long floor,
            PositionDomain domain) {
        return snapshot(resource, reason, generation, floor, domain, M5DeleteEligibilityTestFixtures::fact);
    }

    @FunctionalInterface
    interface FactFactory {
        AuthorityFactV1 create(String key, CanonicalBytes value);

        default AuthorityFactV1 create(String key) {
            return create(key, bytes(key));
        }
    }

    static DeleteEligibilitySnapshotV2 snapshot(
            PhysicalResourceIdV2 resource,
            ReclamationReason reason,
            long generation,
            long floor,
            PositionDomain domain,
            FactFactory facts) {
        var binding =
                new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("epoch"));
        var capability = new CapabilityBinding(1, digest("capability"));
        var identity = new IdentityEnvelope(digest("cell"), digest("scope"), binding, 1, 1, 1, capability);
        var coverage = new ProtocolCoverage(domain, 0, 100);
        var floorRows = Arrays.stream(FloorClassV1.values())
                .map(kind -> new RetentionFloorObservationV1(
                        kind, facts.create("/floor/" + kind), domain, floor, true, true))
                .toList();
        var floors = M5RetentionCodecV1.finalizeSnapshot(new RetentionFloorSnapshotV1(
                identity,
                domain,
                1,
                0,
                digest("retention-policy"),
                facts.create("/owner"),
                facts.create("/storage"),
                1,
                4096,
                floorRows,
                digest("placeholder")));
        var trim = new BindingTrimFrontierV1(
                identity,
                domain,
                0,
                floor,
                digest("retention-policy"),
                floors.snapshotRootSha256(),
                facts.create("/owner"),
                facts.create("/storage"),
                1,
                capability);
        var trimBytes = M5RetentionCodecV1.encodeTrimFrontier(trim);
        var releaseBytes = M4ReadControlCodecV1.encodeProtection(new SourceProtection(
                binding,
                new SourceProtectionIdentity(digest("m4-source"), 1, 1, 1, capability),
                ProtectionState.RELEASED,
                Optional.of(digest("m4-batch")),
                Optional.of(digest("m4-proof-head"))));
        var release = new M4ReleaseBindingV1(
                digest("m4-source"),
                1,
                facts.create("/protection", releaseBytes),
                releaseBytes,
                digest("m4-batch"),
                digest("m4-proof-head"));
        var references = Arrays.stream(ReferenceKindV1.values())
                .map(kind -> new ReferenceObservationV1(
                        kind,
                        facts.create("/reference/" + kind),
                        resource.sha256(),
                        coverage,
                        ReferenceDispositionV1.ABSENT,
                        true))
                .toList();
        var summaries = Arrays.stream(ReferenceKindV1.values())
                .map(kind -> new ReferenceScanSummaryV1(kind, 1, 1, 256, true))
                .toList();
        var proof = M5RetentionCodecV1.finalizeProof(new ReferenceFreeProofV1(
                identity,
                reason == ReclamationReason.UNPUBLISHED_ARTIFACT
                        ? ReferenceTargetKindV1.UNSELECTED_OUTPUT
                        : ReferenceTargetKindV1.READABLE_SOURCE,
                resource.sha256(),
                coverage,
                facts.create("/selector"),
                facts.create("/manifest"),
                facts.create("/trim", trimBytes),
                floors.snapshotRootSha256(),
                M5RetentionCodecV1.calculateObservationsRoot(references),
                reason == ReclamationReason.UNPUBLISHED_ARTIFACT ? List.of() : List.of(release),
                facts.create("/owner"),
                facts.create("/worker"),
                facts.create("/storage"),
                facts.create("/provider"),
                1,
                2,
                summaries,
                references,
                digest("placeholder")));
        Optional<ReplacementEvidence> replacement = Optional.empty();
        Optional<BindingTrimFrontierV1> expiry = Optional.empty();
        Optional<UnpublishedEvidence> unpublished = Optional.empty();
        NativeDisposition disposition;
        switch (reason) {
            case REPLACED_REPRESENTATION -> {
                PhysicalResourceIdV2 output = new PhysicalResourceIdV2.BookKeeperLedger(
                        new PhysicalResourceIdV2.Namespace(
                                PhysicalResourceIdV2.ProviderKind.BOOKKEEPER,
                                CanonicalUtf8.fromString("replacement-cluster"),
                                CanonicalUtf8.fromString("ledger-namespace")),
                        900);
                replacement = Optional.of(new ReplacementEvidence(
                        facts.create("/manifest"),
                        List.of(output),
                        DeleteEligibilitySnapshotV2.requiredSemantics(domain).stream()
                                .map(aspect -> new SemanticTransfer(
                                        aspect,
                                        coverage,
                                        digest("semantic-" + aspect),
                                        digest("semantic-" + aspect),
                                        facts.create("/semantic/" + aspect)))
                                .toList()));
                disposition = NativeDisposition.REPLACEMENT_ALLOWED;
            }
            case LOGICAL_EXPIRY -> {
                expiry = Optional.of(trim);
                disposition = NativeDisposition.LOGICAL_EXPIRY_ALLOWED;
            }
            case UNPUBLISHED_ARTIFACT -> {
                unpublished = Optional.of(new UnpublishedEvidence(
                        digest("task-attempt"),
                        facts.create("/task-terminal"),
                        facts.create("/task-owner-fenced"),
                        facts.create("/adoption-closed"),
                        Optional.of(facts.create("/never-admitted"))));
                disposition = NativeDisposition.UNPUBLISHED_CLEANUP_ALLOWED;
            }
            default -> throw new IllegalArgumentException("unknown fixture reason");
        }
        return new DeleteEligibilitySnapshotV2(
                resource,
                reason,
                generation,
                facts.create("/physical-namespace"),
                facts.create("/complete-member-inventory"),
                List.of(new MemberSnapshot(
                        floors,
                        proof,
                        facts.create("/native/" + reason),
                        disposition,
                        replacement,
                        expiry,
                        unpublished)));
    }

    static List<VersionedValue> metadataValues(DeleteEligibilitySnapshotV2 snapshot) {
        var exact = new LinkedHashMap<String, CanonicalBytes>();
        snapshot.authorityFacts().forEach(fact -> exact.put(fact.key(), bytes(fact.key())));
        for (MemberSnapshot member : snapshot.members()) {
            member.physicalReferences()
                    .m4Releases()
                    .forEach(release ->
                            exact.put(release.protectionAuthority().key(), release.canonicalProtectionBytes()));
            var floors = member.floors();
            var trim = new BindingTrimFrontierV1(
                    floors.identity(),
                    floors.domain(),
                    0,
                    floors.minimumSafeFloor(),
                    floors.retentionPolicyRootSha256(),
                    floors.snapshotRootSha256(),
                    floors.ownerFence(),
                    floors.storageFence(),
                    1,
                    floors.identity().capability());
            exact.put(member.physicalReferences().trimRoot().key(), M5RetentionCodecV1.encodeTrimFrontier(trim));
        }
        return snapshot.authorityFacts().stream()
                .map(fact -> new VersionedValue(
                        fact.key(), exact.get(fact.key()), fact.valueSha256(), fact.metadataVersion()))
                .toList();
    }

    static AuthorityFactV1 fact(String key) {
        return fact(key, bytes(key));
    }

    static AuthorityFactV1 fact(String key, CanonicalBytes value) {
        return new AuthorityFactV1(key, new MetadataVersion(bytes("version-one")), Sha256Digest.hash(value));
    }

    static CanonicalBytes bytes(String value) {
        return CanonicalUtf8.fromString(value).bytes();
    }

    static Sha256Digest digest(String value) {
        return Sha256Digest.hash(bytes(value));
    }
}
