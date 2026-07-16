/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Small canonical SHA-256 writer for hierarchical backfill coverage proofs. */
final class PhysicalRootBackfillDigest {
    private final MessageDigest digest = sha256();

    PhysicalRootBackfillDigest(String domain) {
        text(domain);
    }

    void text(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        raw(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(bytes.length)
                .array());
        raw(bytes);
    }

    void int32(int value) {
        raw(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value)
                .array());
    }

    void int64(long value) {
        raw(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(value)
                .array());
    }

    void bool(boolean value) {
        raw(new byte[] {(byte) (value ? 1 : 0)});
    }

    void checksum(Checksum value) {
        text(value.type().name());
        text(value.value());
    }

    void authority(GcAuthorityToken authority) {
        text("authority");
        text(authority.authorityKey());
        int64(authority.metadataVersion());
        checksum(authority.identitySha256());
    }

    void object(PhysicalObjectIdentity object) {
        text("object");
        text(object.objectKey().value());
        text(object.objectKeyHash().value());
        text(object.objectId().map(value -> value.value()).orElse(""));
        int32(object.kind().wireId());
        int64(object.objectLength());
        checksum(object.storageChecksum());
        text(object.contentSha256().map(Checksum::value).orElse(""));
        text(object.etag().orElse(""));
        checksum(object.identitySha256());
    }

    void root(VersionedPhysicalObjectRoot root) {
        text("root");
        text(root.key());
        int64(root.metadataVersion());
        int64(root.value().lifecycleEpoch());
        int32(root.value().lifecycle().wireId());
        checksum(root.durableValueSha256());
    }

    void protection(ObjectProtection protection) {
        text("protection");
        text(protection.identity().object().value());
        int32(protection.identity().type().wireId());
        text(protection.identity().referenceId());
        text(protection.owner().ownerKey());
        int64(protection.owner().metadataVersion());
        checksum(protection.owner().identitySha256());
        int64(protection.rootLifecycleEpoch());
        int64(protection.metadataVersion());
        checksum(protection.durableValueSha256());
    }

    Checksum finish() {
        return new Checksum(
                ChecksumType.SHA256,
                HexFormat.of().formatHex(digest.digest()));
    }

    static String resourceIdentity(String domain, String resource) {
        PhysicalRootBackfillDigest writer =
                new PhysicalRootBackfillDigest(
                        "physical-root-backfill-resource-v1");
        writer.text(domain);
        writer.text(resource);
        return writer.finish().value();
    }

    private void raw(byte[] bytes) {
        digest.update(bytes);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }
}
