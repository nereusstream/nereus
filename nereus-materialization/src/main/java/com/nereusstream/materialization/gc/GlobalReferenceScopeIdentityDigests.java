/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Domain-separated identities for durable global-scope absence facts. */
final class GlobalReferenceScopeIdentityDigests {
    private GlobalReferenceScopeIdentityDigests() {
    }

    static Checksum activationAbsence(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, "generation-protocol-activation-absence-v1");
            update(digest, key);
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
