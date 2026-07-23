/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.AcquiredAppendSession;
import com.nereusstream.api.AppendAuthority;
import com.nereusstream.api.AppendSession;
import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StableStreamHeadSnapshot;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.AppendSessionSnapshotRecord;
import com.nereusstream.metadata.oxia.records.StreamHeadRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

/** Canonical conversion from the exact durable StreamHead record to the protocol-neutral public snapshot. */
public final class StableStreamHeadSnapshots {
    private StableStreamHeadSnapshots() {}

    public static StableStreamHeadSnapshot from(StreamHeadRecord head) {
        StreamId streamId = new StreamId(head.streamId());
        return new StableStreamHeadSnapshot(
                streamId,
                StreamState.valueOf(head.state()),
                StorageProfile.valueOf(head.profile()),
                head.trimOffset(),
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion(),
                head.lastCommitId(),
                session(streamId, head.appendSession()),
                sha256(durableBytes(head)),
                head.metadataVersion());
    }

    private static Optional<AcquiredAppendSession> session(
            StreamId streamId, AppendSessionSnapshotRecord snapshot) {
        if (snapshot.isEmpty()) return Optional.empty();
        AppendSession session = new AppendSession(
                streamId,
                snapshot.writerId(),
                snapshot.epoch(),
                snapshot.fencingToken(),
                snapshot.leaseVersion(),
                snapshot.expiresAtMillis());
        Optional<AppendAuthority> authority = snapshot.hasAuthority()
                ? Optional.of(new AppendAuthority(
                        snapshot.authorityType(),
                        snapshot.authorityId(),
                        snapshot.authorityEpoch(),
                        snapshot.authorityOwnerId(),
                        snapshot.authorityOwnerEpoch()))
                : Optional.empty();
        return Optional.of(new AcquiredAppendSession(session, authority));
    }

    private static byte[] durableBytes(StreamHeadRecord head) {
        StreamHeadRecord canonical = new StreamHeadRecord(
                head.streamId(),
                head.streamName(),
                head.streamNameHash(),
                head.state(),
                head.profile(),
                head.attributes(),
                head.createdAtMillis(),
                head.policyVersion(),
                head.committedEndOffset(),
                head.cumulativeSize(),
                head.commitVersion(),
                head.trimOffset(),
                head.lastCommitId(),
                head.appendSession(),
                0);
        return MetadataRecordCodecFactory.encodeEnvelope(canonical, StreamHeadRecord.class);
    }

    private static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
