/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.recovery;

import com.nereusstream.api.ReadView;
import com.nereusstream.metadata.oxia.codec.GenerationIndexRecordCodecV1;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointEntry;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointFormatException;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointPublication;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointVerifier;
import com.nereusstream.objectstore.checkpoint.RecoveryCheckpointWriteRequest;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Authoritative F4 metadata-codec verifier for NRC1 embedded repair evidence. */
public final class MetadataRecoveryCheckpointVerifier implements RecoveryCheckpointVerifier {
    private final GenerationIndexRecordCodecV1 generationCodec = new GenerationIndexRecordCodecV1();

    @Override
    public void verifyPublication(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointPublication publication) {
        try {
            byte[] canonical = bytes(publication.canonicalGenerationIndexRecord());
            GenerationIndexRecord record = generationCodec.decode(canonical);
            if (!Arrays.equals(canonical, generationCodec.encode(record))) {
                throw corrupt("embedded generation index is not canonically encoded");
            }
            if (record.metadataVersion() != 0
                    || record.readViewId() != ReadView.COMMITTED.wireId()
                    || record.lifecycle() != GenerationLifecycle.COMMITTED
                    || !record.streamId().equals(header.streamId().value())
                    || record.generation() != publication.generation()
                    || !record.publicationId().equals(publication.publicationId().value())
                    || record.offsetStart() != publication.coverage().startOffset()
                    || record.offsetEnd() != publication.coverage().endOffset()
                    || record.firstCommitVersion() < header.firstCommitVersion()
                    || record.lastCommitVersion() > header.lastCommitVersion()
                    || record.cumulativeSizeAtStart() < header.cumulativeSizeAtStart()
                    || record.cumulativeSizeAtEnd() > header.cumulativeSizeAtEnd()) {
                throw corrupt("embedded generation index does not match its NRC1 publication/header facts");
            }
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new RecoveryCheckpointFormatException(
                    "cannot strictly decode embedded generation index", failure);
        }
    }

    @Override
    public void verifyEntry(
            RecoveryCheckpointWriteRequest header,
            RecoveryCheckpointEntry entry) {
        try {
            byte[] canonical = bytes(entry.canonicalCommitRecord());
            StreamCommitTargetRecord record = MetadataRecordCodecFactory.decodeEnvelope(
                    canonical, StreamCommitTargetRecord.class);
            if (!Arrays.equals(
                    canonical,
                    MetadataRecordCodecFactory.encodeEnvelope(record, StreamCommitTargetRecord.class))) {
                throw corrupt("embedded generic commit envelope is not canonically encoded");
            }
            if (record.metadataVersion() != 0
                    || record.generation() != 0
                    || !record.streamId().equals(header.streamId().value())
                    || !record.commitId().equals(entry.commitId())
                    || !record.previousCommitId().equals(entry.previousCommitId())
                    || record.commitVersion() != entry.commitVersion()
                    || record.offsetStart() != entry.range().startOffset()
                    || record.offsetEnd() != entry.range().endOffset()
                    || record.cumulativeSize() != entry.cumulativeSizeAtEnd()) {
                throw corrupt("embedded generic commit does not match its NRC1 entry/header facts");
            }
        } catch (RecoveryCheckpointFormatException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new RecoveryCheckpointFormatException(
                    "cannot strictly decode embedded generic commit", failure);
        }
    }

    private static byte[] bytes(ByteBuffer value) {
        ByteBuffer copy = value.asReadOnlyBuffer();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static RecoveryCheckpointFormatException corrupt(String message) {
        return new RecoveryCheckpointFormatException(message);
    }
}
