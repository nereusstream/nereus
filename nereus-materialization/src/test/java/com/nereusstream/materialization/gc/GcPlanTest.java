/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcReference;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GcPlanTest {
    private static final String CANDIDATE_ID = "a".repeat(52);
    private static final String ATTEMPT_ID = "b".repeat(52);
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');
    private static final Checksum CRC = new Checksum(ChecksumType.CRC32C, "01020304");

    @Test
    void candidateAndPlanAreBoundToExactActiveAndMarkedRoots() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        List<GcReferenceSnapshot> snapshots = List.of(
                snapshot(query, "generation-v1", List.of(authority("/a", 1))));
        List<GcPlannedProtectionRemoval> protections = List.of(protection(
                candidate.object().objectKeyHash(), 1, 9, SHA_A));
        List<GcPlannedMetadataRemoval> metadataRemovals = List.of(metadata(
                "/metadata/generation-a", 12, SHA_A));
        Checksum digest = GcPlan.computeReferenceSetSha256(
                config, candidate, snapshots, protections, metadataRemovals);
        VersionedPhysicalObjectRoot marked = markedRoot(active, ATTEMPT_ID, digest, 300, 500);

        GcPlan plan = GcPlan.fromMarkedRoot(
                config,
                ATTEMPT_ID,
                candidate,
                snapshots,
                protections,
                metadataRemovals,
                marked);

        assertThat(plan.referenceSetSha256()).isEqualTo(digest);
        assertThat(plan.markedRootMetadataVersion()).isEqualTo(8);
        assertThat(plan.markedRootLifecycleEpoch()).isEqualTo(5);
        assertThat(plan.deleteNotBeforeMillis()).isEqualTo(500);
        assertThat(plan.candidate().rootMetadataVersion()).isEqualTo(7);
        assertThat(plan.plannedProtectionRemovals().getFirst().protection().metadataVersion())
                .isEqualTo(9);
        assertThat(plan.plannedMetadataRemovals().getFirst().metadataVersion()).isEqualTo(12);
    }

    @Test
    void referenceSetDigestUsesExactFactsNotEphemeralCandidateIdentity() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        List<GcReferenceSnapshot> snapshots = List.of(
                snapshot(query, "generation-v1", List.of(authority("/a", 1))));
        List<GcPlannedMetadataRemoval> metadata = List.of(metadata("/metadata/a", 12, SHA_A));

        Checksum first = GcPlan.computeReferenceSetSha256(
                config, candidate, snapshots, List.of(), metadata);
        Checksum second = GcPlan.computeReferenceSetSha256(
                config, candidate, snapshots, List.of(), metadata);
        Checksum changedAuthority = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                List.of(snapshot(query, "generation-v1", List.of(authority("/a", 2)))),
                List.of(),
                metadata);
        Checksum changedMetadataVersion = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                snapshots,
                List.of(),
                List.of(metadata("/metadata/a", 13, SHA_B)));

        assertThat(second).isEqualTo(first);
        assertThat(changedAuthority).isNotEqualTo(first);
        assertThat(changedMetadataVersion).isNotEqualTo(first);
    }

    @Test
    void everyDomainReferenceRequiresAnExactPlannedRemoval() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        GcReference reference = new GcReference(
                "generation-index",
                "stream-a/1/42",
                "/metadata/generation-a",
                12,
                SHA_A);
        GcReferenceSnapshot snapshot = GcReferenceSnapshot.create(
                "generation-v1",
                1,
                query.queryIdentitySha256(),
                true,
                false,
                1,
                1,
                List.of(authority("/metadata/generation-a", 12)),
                List.of(reference));

        assertThat(GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot),
                        List.of(),
                        List.of(metadata("/metadata/generation-a", 12, SHA_A))))
                .isNotNull();
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config, candidate, List.of(snapshot), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact planned metadata removal");
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot),
                        List.of(),
                        List.of(metadata("/metadata/generation-a", 13, SHA_A))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact planned metadata removal");
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot),
                        List.of(),
                        List.of(metadata("/metadata/generation-a", 12, SHA_B))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact planned metadata removal");
    }

    @Test
    void incompleteVetoingWrongQueryAndUnsortedDomainsCannotAuthorizeAPlan() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        GcReferenceSnapshot incomplete = GcReferenceSnapshot.create(
                "generation-v1",
                1,
                query.queryIdentitySha256(),
                false,
                true,
                2,
                0,
                List.of(authority("/a", 1)),
                List.of());
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config, candidate, List.of(incomplete), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete");

        GcReferenceQuery otherQuery = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                query.object(),
                List.of(new StreamId("stream-b")),
                SHA_A);
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot(otherQuery, "generation-v1", List.of(authority("/a", 1)))),
                        List.of(),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query-bound");

        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(
                                snapshot(query, "projection-generation-v1", List.of(authority("/b", 1))),
                                snapshot(query, "generation-v1", List.of(authority("/a", 1)))),
                        List.of(),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sorted");
    }

    @Test
    void configuredDomainLimitsRejectTruncationAndOversizedProofs() {
        PhysicalGcConfig config = configWithDomainLimits(1, 1);
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        GcReferenceSnapshot oversized = snapshot(
                query,
                "generation-v1",
                List.of(authority("/a", 1), authority("/b", 1)));

        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config, candidate, List.of(oversized), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configured bounds");
        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot(query, "generation-v1", List.of(authority("/a", 1)))),
                        List.of(),
                        List.of(metadata("/a", 1, SHA_A), metadata("/b", 1, SHA_A))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bound");
    }

    @Test
    void protectionForAnotherObjectCannotEnterMarkDigest() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        GcPlannedProtectionRemoval unrelated = protection(
                ObjectKeyHash.from(new ObjectKey("objects/other")), 1, 9, SHA_A);

        assertThatThrownBy(() -> GcPlan.computeReferenceSetSha256(
                        config,
                        candidate,
                        List.of(snapshot(query, "generation-v1", List.of(authority("/a", 1)))),
                        List.of(unrelated),
                        List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("candidate ACTIVE root");
    }

    @Test
    void protectionOwnerVersionAndEnvelopeArePartOfTheMarkDigest() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        List<GcReferenceSnapshot> snapshots = List.of(
                snapshot(query, "generation-v1", List.of(authority("/a", 1))));

        Checksum first = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                snapshots,
                List.of(protection(candidate.object().objectKeyHash(), 1, 9, SHA_A)),
                List.of());
        Checksum changed = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                snapshots,
                List.of(protection(candidate.object().objectKeyHash(), 2, 10, SHA_B)),
                List.of());

        assertThat(changed).isNotEqualTo(first);
    }

    @Test
    void responseLossReconstructionRequiresExactRootAttemptAndDigest() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        List<GcReferenceSnapshot> snapshots = List.of(
                snapshot(query, "generation-v1", List.of(authority("/a", 1))));
        Checksum digest = GcPlan.computeReferenceSetSha256(
                config, candidate, snapshots, List.of(), List.of());

        assertThatThrownBy(() -> GcPlan.fromMarkedRoot(
                        config,
                        ATTEMPT_ID,
                        candidate,
                        snapshots,
                        List.of(),
                        List.of(),
                        markedRoot(active, "c".repeat(52), digest, 300, 500)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact candidate plan");
        assertThatThrownBy(() -> GcPlan.fromMarkedRoot(
                        config,
                        ATTEMPT_ID,
                        candidate,
                        snapshots,
                        List.of(),
                        List.of(),
                        markedRoot(active, ATTEMPT_ID, SHA_B, 300, 500)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact candidate plan");
    }

    @Test
    void reconstructsPlanFromMarkedRootWithoutInventingPreviousOxiaVersion() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate activeCandidate = candidate(config, active, query);
        List<GcReferenceSnapshot> snapshots = List.of(
                snapshot(query, "generation-v1", List.of(authority("/a", 1))));
        Checksum digest = GcPlan.computeReferenceSetSha256(
                config, activeCandidate, snapshots, List.of(), List.of());
        VersionedPhysicalObjectRoot marked = markedRoot(
                active, ATTEMPT_ID, digest, 300, 500, 42);
        GcCandidate recovered = GcCandidate.fromMarkedRoot(
                config, "c".repeat(52), marked, query, SHA_A, 900);

        GcPlan plan = GcPlan.fromMarkedRoot(
                config,
                ATTEMPT_ID,
                recovered,
                snapshots,
                List.of(),
                List.of(),
                marked);

        assertThat(recovered.rootState()).isEqualTo(GcCandidateRootState.MARKED_RECOVERY);
        assertThat(recovered.rootMetadataVersion()).isEqualTo(42);
        assertThat(recovered.notBeforeMillis()).isEqualTo(500);
        assertThat(recovered.discoveredAtMillis()).isEqualTo(900);
        assertThat(plan.markedRootMetadataVersion()).isEqualTo(42);
    }

    @Test
    void candidateRejectsNonActiveAndPrematureRootEvidence() {
        PhysicalGcConfig config = PhysicalGcConfig.defaults();
        VersionedPhysicalObjectRoot active = activeRoot();
        GcReferenceQuery query = query(PhysicalObjectIdentity.from(active.value()));
        GcCandidate candidate = candidate(config, active, query);
        Checksum digest = GcPlan.computeReferenceSetSha256(
                config,
                candidate,
                List.of(snapshot(query, "generation-v1", List.of(authority("/a", 1)))),
                List.of(),
                List.of());

        assertThatThrownBy(() -> GcCandidate.fromActiveRoot(
                        config,
                        CANDIDATE_ID,
                        markedRoot(active, ATTEMPT_ID, digest, 300, 500),
                        query,
                        SHA_A,
                        300,
                        500))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ACTIVE");
        assertThatThrownBy(() -> GcCandidate.fromActiveRoot(
                        config, CANDIDATE_ID, active, query, SHA_A, 150, 150))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eligibility");
    }

    @Test
    void secureIdsConsumeFresh128BitEntropyAndReturnCanonicalBase32() {
        CapturingSecureRandom random = new CapturingSecureRandom();
        String id = new SecureGcIdGenerator(random).next();

        assertThat(random.requestedBytes).isEqualTo(16);
        assertThat(id).hasSize(52).matches("[a-z2-7]{52}");
    }

    private static GcCandidate candidate(
            PhysicalGcConfig config,
            VersionedPhysicalObjectRoot active,
            GcReferenceQuery query) {
        return GcCandidate.fromActiveRoot(
                config, CANDIDATE_ID, active, query, SHA_A, 250, 250);
    }

    private static GcPlannedProtectionRemoval protection(
            ObjectKeyHash object,
            long ownerMetadataVersion,
            long metadataVersion,
            Checksum durableValueSha256) {
        ObjectProtectionRecord value = new ObjectProtectionRecord(
                1,
                object.value(),
                ObjectProtectionType.VISIBLE_GENERATION.wireId(),
                "generation-a",
                "/owner/generation-a",
                ownerMetadataVersion,
                (ownerMetadataVersion == 1 ? SHA_A : SHA_B).value(),
                4,
                100,
                0,
                metadataVersion);
        return new GcPlannedProtectionRemoval(new VersionedObjectProtection(
                "/protections/" + object.value() + "/generation-a",
                value,
                metadataVersion,
                durableValueSha256));
    }

    private static GcPlannedMetadataRemoval metadata(
            String key, long version, Checksum durableValueSha256) {
        return new GcPlannedMetadataRemoval(
                "generation-index", key, version, durableValueSha256);
    }

    private static GcReferenceQuery query(PhysicalObjectIdentity object) {
        return GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(new StreamId("stream-a")),
                SHA_A);
    }

    private static GcReferenceSnapshot snapshot(
            GcReferenceQuery query, String domainId, List<GcAuthorityToken> authorities) {
        return GcReferenceSnapshot.create(
                domainId,
                1,
                query.queryIdentitySha256(),
                true,
                false,
                authorities.size(),
                0,
                authorities,
                List.of());
    }

    private static GcAuthorityToken authority(String key, long version) {
        return new GcAuthorityToken(key, version, version == 1 ? SHA_A : SHA_B);
    }

    private static VersionedPhysicalObjectRoot activeRoot() {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.create(
                new ObjectKey("objects/one"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                42,
                CRC,
                Optional.of(SHA_A),
                Optional.of("etag"));
        PhysicalObjectRootRecord record = rootRecord(
                object, PhysicalObjectLifecycle.ACTIVE, 4, "", "", 0, 0, 0, 7);
        return new VersionedPhysicalObjectRoot("/root/one", record, 7, SHA_A);
    }

    private static VersionedPhysicalObjectRoot markedRoot(
            VersionedPhysicalObjectRoot active,
            String attemptId,
            Checksum digest,
            long markedAtMillis,
            long deleteNotBeforeMillis) {
        return markedRoot(active, attemptId, digest, markedAtMillis, deleteNotBeforeMillis, 8);
    }

    private static VersionedPhysicalObjectRoot markedRoot(
            VersionedPhysicalObjectRoot active,
            String attemptId,
            Checksum digest,
            long markedAtMillis,
            long deleteNotBeforeMillis,
            long metadataVersion) {
        PhysicalObjectRootRecord record = rootRecord(
                PhysicalObjectIdentity.from(active.value()),
                PhysicalObjectLifecycle.MARKED,
                active.value().lifecycleEpoch() + 1,
                attemptId,
                digest.value(),
                markedAtMillis,
                deleteNotBeforeMillis,
                0,
                metadataVersion);
        return new VersionedPhysicalObjectRoot(active.key(), record, metadataVersion, SHA_B);
    }

    private static PhysicalObjectRootRecord rootRecord(
            PhysicalObjectIdentity object,
            PhysicalObjectLifecycle lifecycle,
            long lifecycleEpoch,
            String attemptId,
            String referenceSetSha256,
            long markedAtMillis,
            long deleteNotBeforeMillis,
            long deleteStartedAtMillis,
            long metadataVersion) {
        return new PhysicalObjectRootRecord(
                1,
                object.objectKeyHash().value(),
                object.objectKey().value(),
                "",
                object.kind().wireId(),
                object.objectLength(),
                object.storageChecksum().type().name(),
                object.storageChecksum().value(),
                object.contentSha256().orElseThrow().value(),
                object.etag().orElse(""),
                lifecycle,
                lifecycleEpoch,
                100,
                200,
                attemptId,
                referenceSetSha256,
                markedAtMillis,
                deleteNotBeforeMillis,
                deleteStartedAtMillis,
                0,
                0,
                "",
                "",
                metadataVersion);
    }

    private static PhysicalGcConfig configWithDomainLimits(int maxAuthorities, int maxReferences) {
        PhysicalGcConfig defaults = PhysicalGcConfig.defaults();
        return new PhysicalGcConfig(
                defaults.enabled(),
                defaults.dryRun(),
                defaults.metadataScanPageSize(),
                defaults.objectListPageSize(),
                defaults.maxConcurrentDeletes(),
                defaults.maxStreamsPerCandidate(),
                maxAuthorities,
                maxReferences,
                defaults.scanInterval(),
                defaults.readerLeaseDuration(),
                defaults.readerLeaseRenewInterval(),
                defaults.maximumClockSkew(),
                defaults.drainGrace(),
                defaults.pendingProtectionDuration(),
                defaults.orphanGrace(),
                defaults.tombstoneAuditGrace(),
                defaults.operationTimeout(),
                defaults.closeTimeout());
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }

    private static final class CapturingSecureRandom extends SecureRandom {
        private int requestedBytes;

        @Override
        public void nextBytes(byte[] bytes) {
            requestedBytes = bytes.length;
            for (int index = 0; index < bytes.length; index++) {
                bytes[index] = (byte) index;
            }
        }
    }
}
