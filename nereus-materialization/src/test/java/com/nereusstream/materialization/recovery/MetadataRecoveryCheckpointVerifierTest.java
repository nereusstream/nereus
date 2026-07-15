/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatException;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class MetadataRecoveryCheckpointVerifierTest {
    private final MetadataRecoveryCheckpointVerifier verifier = new MetadataRecoveryCheckpointVerifier();

    @Test
    void acceptsOnlyCanonicalVersionZeroCommittedGenerationAndGenericCommitEvidence() {
        GenerationIndexRecord generation = F4MetadataTestValues.generation(GenerationLifecycle.COMMITTED);
        StreamCommitTargetRecord commit = commit("commit-1");
        RecoveryCheckpointWriteRequest header = header();
        RecoveryCheckpointPublication publication = publication(generation);
        RecoveryCheckpointEntry entry = entry(commit, "commit-1");

        assertThatCode(() -> verifier.verifyPublication(header, publication)).doesNotThrowAnyException();
        assertThatCode(() -> verifier.verifyEntry(header, entry)).doesNotThrowAnyException();
    }

    @Test
    void rejectsDurableMetadataVersionAndDuplicatedIdentityDrift() {
        GenerationIndexRecord durable = F4MetadataTestValues
                .generation(GenerationLifecycle.COMMITTED)
                .withMetadataVersion(9);
        StreamCommitTargetRecord wrongCommit = commit("different-commit");

        assertThatThrownBy(() -> verifier.verifyPublication(header(), publication(durable)))
                .isInstanceOf(RecoveryCheckpointFormatException.class)
                .hasMessageContaining("generation index does not match");
        assertThatThrownBy(() -> verifier.verifyEntry(header(), entry(wrongCommit, "commit-1")))
                .isInstanceOf(RecoveryCheckpointFormatException.class)
                .hasMessageContaining("generic commit does not match");
    }

    private static RecoveryCheckpointWriteRequest header() {
        return new RecoveryCheckpointWriteRequest(
                F4MetadataTestValues.CLUSTER,
                new StreamId(F4MetadataTestValues.STREAM),
                1,
                "a".repeat(26),
                new OffsetRange(0, 2),
                1,
                1,
                0,
                100,
                "commit-1",
                "commit-1",
                "commit-1",
                1,
                sha256("projection"),
                1,
                1);
    }

    private static RecoveryCheckpointPublication publication(GenerationIndexRecord record) {
        byte[] canonical = new GenerationIndexRecordCodecV1().encode(record);
        return new RecoveryCheckpointPublication(
                record.generation(),
                new PublicationId(record.publicationId()),
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                ByteBuffer.wrap(canonical).asReadOnlyBuffer(),
                sha256(canonical));
    }

    private static RecoveryCheckpointEntry entry(
            StreamCommitTargetRecord record,
            String declaredCommitId) {
        byte[] canonical = MetadataRecordCodecFactory.encodeEnvelope(
                record, StreamCommitTargetRecord.class);
        return new RecoveryCheckpointEntry(
                record.commitVersion(),
                new OffsetRange(record.offsetStart(), record.offsetEnd()),
                record.cumulativeSize(),
                declaredCommitId,
                record.previousCommitId(),
                ByteBuffer.wrap(canonical).asReadOnlyBuffer(),
                sha256(canonical),
                List.of(0));
    }

    private static StreamCommitTargetRecord commit(String commitId) {
        return new StreamCommitTargetRecord(
                F4MetadataTestValues.STREAM,
                commitId,
                "",
                0,
                2,
                0,
                100,
                1,
                "writer",
                "writer-run-hash",
                1,
                "fence-hash",
                F4MetadataTestValues.readTarget(),
                "PULSAR_ENTRY_BATCH",
                2,
                2,
                100,
                List.of(),
                "projection-f4",
                0,
                0,
                100,
                0);
    }

    private static Checksum sha256(String value) {
        return sha256(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static Checksum sha256(byte[] value) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
