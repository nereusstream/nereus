/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/** Length-delimited canonical SHA-256 encodings for F4 core proof values. */
final class PhysicalValueDigests {
    private PhysicalValueDigests() {
    }

    static Checksum physicalIdentity(PhysicalObjectIdentity value) {
        DigestWriter writer = writer("nereus-physical-object-identity-v1");
        writer.text(value.objectKey().value());
        writer.text(value.objectKeyHash().value());
        writer.optionalText(value.objectId().map(id -> id.value()));
        writer.int32(value.kind().wireId());
        writer.int64(value.objectLength());
        writer.checksum(value.storageChecksum());
        writer.optionalChecksum(value.contentSha256());
        writer.optionalText(value.etag());
        return writer.finish();
    }

    static Checksum query(
            GcReferenceQueryKind kind,
            PhysicalObjectIdentity object,
            List<StreamId> streams,
            Checksum evidence) {
        DigestWriter writer = writer("nereus-gc-reference-query-v1");
        writer.int32(kind.ordinal() + 1);
        writer.checksum(physicalIdentity(object));
        writer.int32(streams.size());
        streams.forEach(stream -> writer.text(stream.value()));
        writer.checksum(evidence);
        return writer.finish();
    }

    static Checksum snapshot(
            String domainId,
            int protocolVersion,
            Checksum queryIdentity,
            boolean complete,
            boolean veto,
            long authorityCount,
            long referenceCount,
            List<GcAuthorityToken> authorities,
            List<GcReference> references) {
        DigestWriter writer = writer("nereus-gc-reference-snapshot-v1");
        writer.text(domainId);
        writer.int32(protocolVersion);
        writer.checksum(queryIdentity);
        writer.bool(complete);
        writer.bool(veto);
        writer.int64(authorityCount);
        writer.int64(referenceCount);
        writer.int32(authorities.size());
        for (GcAuthorityToken authority : authorities) {
            writer.text(authority.authorityKey());
            writer.int64(authority.metadataVersion());
            writer.checksum(authority.identitySha256());
        }
        writer.int32(references.size());
        for (GcReference reference : references) {
            writer.text(reference.referenceType());
            writer.text(reference.referenceId());
            writer.text(reference.ownerKey());
            writer.int64(reference.ownerMetadataVersion());
            writer.checksum(reference.ownerIdentitySha256());
        }
        return writer.finish();
    }

    static DigestWriter writer(String domain) {
        return new DigestWriter(domain);
    }

    static final class DigestWriter {
        private final MessageDigest digest;

        private DigestWriter(String domain) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
            text(domain);
        }

        DigestWriter bool(boolean value) {
            digest.update((byte) (value ? 1 : 0));
            return this;
        }

        DigestWriter int32(int value) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
            return this;
        }

        DigestWriter int64(long value) {
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
            return this;
        }

        DigestWriter text(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            int32(bytes.length);
            digest.update(bytes);
            return this;
        }

        DigestWriter optionalText(java.util.Optional<String> value) {
            bool(value.isPresent());
            value.ifPresent(this::text);
            return this;
        }

        DigestWriter checksum(Checksum value) {
            text(value.type().name());
            text(value.value());
            return this;
        }

        DigestWriter optionalChecksum(java.util.Optional<Checksum> value) {
            bool(value.isPresent());
            value.ifPresent(this::checksum);
            return this;
        }

        Checksum finish() {
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
        }
    }
}
