/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.ProjectionIdentity;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Canonical identity for a projection-free native stream registration. */
public final class DirectMaterializationStreamAuthority {
    private static final String DOMAIN = "nereus-direct-materialization-stream-v1";
    private static final String ABSENT_PROJECTION = ProjectionIdentity.encode(Optional.empty());

    private DirectMaterializationStreamAuthority() {}

    public static String encodedProjectionRef() {
        return ABSENT_PROJECTION;
    }

    public static Checksum identitySha256(StreamId streamId, StorageProfile profile) {
        StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
        StorageProfile exactProfile = Objects.requireNonNull(profile, "profile").canonical();
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
        update(digest, DOMAIN);
        update(digest, exactStream.value());
        update(digest, exactProfile.name());
        update(digest, ABSENT_PROJECTION);
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
