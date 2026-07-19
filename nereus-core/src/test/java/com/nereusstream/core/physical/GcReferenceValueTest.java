/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ProjectionRef;
import com.nereusstream.api.ProjectionType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.capability.DomainValidatedDeletionSubject;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.LiveProjectionSubject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GcReferenceValueTest {
    private static final Checksum SHA_A = sha('a');
    private static final Checksum SHA_B = sha('b');
    private static final Checksum CRC = new Checksum(ChecksumType.CRC32C, "01020304");

    @Test
    void physicalAndQueryIdentitiesAreRecomputedAndViewBound() {
        PhysicalObjectIdentity object = object();
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object,
                List.of(new StreamId("stream-a"), new StreamId("stream-b")),
                SHA_A);

        assertThat(object.identitySha256().type()).isEqualTo(ChecksumType.SHA256);
        assertThat(query.queryIdentitySha256().type()).isEqualTo(ChecksumType.SHA256);
        assertThatThrownBy(() -> new GcReferenceQuery(
                        query.kind(), query.object(), query.affectedStreams(), query.candidateEvidenceSha256(), SHA_B))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcReferenceQuery.create(
                        GcReferenceQueryKind.REFERENCED_OBJECT,
                        object,
                        List.of(new StreamId("stream-b"), new StreamId("stream-a")),
                        SHA_A))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcReferenceQuery.create(
                        GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                        object,
                        List.of(new StreamId("stream-a")),
                        SHA_A))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void snapshotsAreCanonicalCompleteOrVetoProofs() {
        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(),
                List.of(new StreamId("stream-a")),
                SHA_A);
        List<GcAuthorityToken> authorities = List.of(
                new GcAuthorityToken("/authority/a", 1, SHA_A),
                new GcAuthorityToken("/authority/b", 2, SHA_B));
        List<GcReference> references = List.of(
                new GcReference("generation", "g-1", "/owner/a", 3, SHA_A));

        GcReferenceSnapshot snapshot = GcReferenceSnapshot.create(
                "generation-v1",
                1,
                query.queryIdentitySha256(),
                true,
                true,
                authorities.size(),
                references.size(),
                authorities,
                references);
        assertThat(snapshot.snapshotSha256().type()).isEqualTo(ChecksumType.SHA256);

        assertThatThrownBy(() -> GcReferenceSnapshot.create(
                        "generation-v1", 1, query.queryIdentitySha256(),
                        false, false, 3, 1, authorities, references))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcReferenceSnapshot.create(
                        "generation-v1", 1, query.queryIdentitySha256(),
                        true, false, 3, 1, authorities, references))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcReferenceSnapshot.create(
                        "generation-v1", 1, query.queryIdentitySha256(),
                        true, false, 2, 1,
                        List.of(authorities.get(1), authorities.get(0)), references))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void activationProofLocksLiveAndDeletionSubjectCombinations() {
        LiveProjectionSubject live = new LiveProjectionSubject(
                new StreamId("stream-a"),
                new ProjectionRef(ProjectionType.VIRTUAL_LEDGER, "projection-a"),
                SHA_A);
        GenerationActivationProof publication = GenerationActivationProof.create(
                GenerationOperation.GENERATION_PUBLISH,
                live,
                7,
                8,
                9,
                SHA_B,
                true,
                false,
                10);
        assertThat(publication.subjectSha256())
                .isEqualTo(GenerationActivationProof.subjectSha256(live));
        GenerationActivationProof logicalTrim =
                GenerationActivationProof.create(
                        GenerationOperation.LOGICAL_TRIM,
                        live,
                        7,
                        8,
                        9,
                        SHA_B,
                        true,
                        false,
                        10);
        assertThat(logicalTrim.publicationEnabled()).isTrue();
        assertThat(logicalTrim.deletionEnabled()).isFalse();

        GcReferenceQuery query = GcReferenceQuery.create(
                GcReferenceQueryKind.REFERENCED_OBJECT,
                object(),
                List.of(new StreamId("stream-a")),
                SHA_A);
        DomainValidatedDeletionSubject deletion = new DomainValidatedDeletionSubject(query, SHA_B);
        assertThat(GenerationActivationProof.create(
                        GenerationOperation.PHYSICAL_DELETE,
                        deletion,
                        0,
                        8,
                        9,
                        SHA_A,
                        false,
                        true,
                        10).operation())
                .isEqualTo(GenerationOperation.PHYSICAL_DELETE);

        assertThatThrownBy(() -> GenerationActivationProof.create(
                        GenerationOperation.PHYSICAL_DELETE,
                        live,
                        7,
                        8,
                        9,
                        SHA_A,
                        true,
                        true,
                        10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GenerationActivationProof.create(
                        GenerationOperation.PHYSICAL_DELETE,
                        deletion,
                        1,
                        8,
                        9,
                        SHA_A,
                        false,
                        true,
                        10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static PhysicalObjectIdentity object() {
        return PhysicalObjectIdentity.create(
                new ObjectKey("objects/one"),
                Optional.empty(),
                PhysicalObjectKind.COMMITTED_COMPACTED,
                42,
                CRC,
                Optional.of(SHA_A),
                Optional.of("etag"));
    }

    private static Checksum sha(char value) {
        return new Checksum(ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
