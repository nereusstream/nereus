/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import java.util.Objects;

/** Exact source commit plus the canonical NRC1 envelope identity derived from it. */
public record VersionedGenerationZeroCommit(
        String key,
        StreamId streamId,
        String commitId,
        AppendRecoveryCommitEncoding sourceEncoding,
        GenerationZeroMarkerIdentity markerIdentity,
        StreamCommitTargetRecord canonicalCommit,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        Checksum canonicalCommitRecordSha256,
        long metadataVersion,
        Checksum durableValueSha256) {
    public VersionedGenerationZeroCommit {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(commitId, "commitId");
        Objects.requireNonNull(sourceEncoding, "sourceEncoding");
        Objects.requireNonNull(markerIdentity, "markerIdentity");
        Objects.requireNonNull(canonicalCommit, "canonicalCommit");
        if (key.isBlank()
                || commitId.isBlank()
                || offsetStart < 0
                || offsetEnd <= offsetStart
                || commitVersion <= 0
                || metadataVersion < 0) {
            throw new IllegalArgumentException("generation-zero commit facts are invalid");
        }
        if (!canonicalCommit.streamId().equals(streamId.value())
                || !canonicalCommit.commitId().equals(commitId)
                || canonicalCommit.generation() != 0
                || canonicalCommit.metadataVersion() != 0
                || canonicalCommit.offsetStart() != offsetStart
                || canonicalCommit.offsetEnd() != offsetEnd
                || canonicalCommit.commitVersion() != commitVersion) {
            throw new IllegalArgumentException(
                    "canonical generation-zero commit does not match its wrapper");
        }
        canonicalCommitRecordSha256 = RetirementMetadataSupport.requireSha256(
                canonicalCommitRecordSha256, "canonicalCommitRecordSha256");
        Checksum expectedCanonicalSha = RetirementMetadataSupport.sha256(
                MetadataRecordCodecFactory.encodeEnvelope(
                        canonicalCommit, StreamCommitTargetRecord.class));
        if (!canonicalCommitRecordSha256.equals(expectedCanonicalSha)) {
            throw new IllegalArgumentException(
                    "canonical generation-zero commit SHA does not match its record");
        }
        durableValueSha256 = RetirementMetadataSupport.requireSha256(
                durableValueSha256, "durableValueSha256");
    }
}
