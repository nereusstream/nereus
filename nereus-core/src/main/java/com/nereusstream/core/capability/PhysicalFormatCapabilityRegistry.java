/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.read.ReadTargetReaderKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Immutable exact-format admission registry and canonical broker capability digest. */
public final class PhysicalFormatCapabilityRegistry {
    private final Map<ReadTargetReaderKey, PhysicalFormatCapability> capabilities;
    private final Checksum digestSha256;

    public PhysicalFormatCapabilityRegistry(Collection<PhysicalFormatCapability> capabilities) {
        Objects.requireNonNull(capabilities, "capabilities");
        try {
            this.capabilities = capabilities.stream()
                    .map(value -> Objects.requireNonNull(value, "capability"))
                    .collect(Collectors.toUnmodifiableMap(
                            PhysicalFormatCapability::readerKey,
                            Function.identity(),
                            (left, right) -> {
                                throw new IllegalArgumentException(
                                        "duplicate exact physical format capability: " + left.readerKey());
                            }));
        } catch (IllegalStateException duplicate) {
            throw new IllegalArgumentException("duplicate exact physical format capability", duplicate);
        }
        digestSha256 = digest(this.capabilities.values());
    }

    public Checksum digestSha256() {
        return digestSha256;
    }

    public PhysicalFormatCapability requireReadable(
            ReadTarget target,
            ReadView view,
            PayloadFormat payloadFormat) {
        return require(ReadTargetReaderKey.from(target), view, payloadFormat, false);
    }

    public PhysicalFormatCapability requireWritable(
            ReadTargetReaderKey key,
            ReadView view,
            PayloadFormat payloadFormat) {
        return require(key, view, payloadFormat, true);
    }

    /** Fails admission unless every exact required capability is present with at least the same access bits. */
    public void requireSupersetOf(PhysicalFormatCapabilityRegistry required) {
        Objects.requireNonNull(required, "required");
        for (PhysicalFormatCapability expected : required.capabilities.values()) {
            PhysicalFormatCapability actual = capabilities.get(expected.readerKey());
            if (actual == null
                    || actual.readView() != expected.readView()
                    || actual.payloadFormat() != expected.payloadFormat()
                    || (expected.readable() && !actual.readable())
                    || (expected.writable() && !actual.writable())) {
                throw unsupported("runtime lacks required exact format capability: " + expected.readerKey());
            }
        }
    }

    public int size() {
        return capabilities.size();
    }

    private PhysicalFormatCapability require(
            ReadTargetReaderKey key,
            ReadView view,
            PayloadFormat payloadFormat,
            boolean write) {
        PhysicalFormatCapability capability = capabilities.get(Objects.requireNonNull(key, "key"));
        if (capability == null
                || capability.readView() != Objects.requireNonNull(view, "view")
                || capability.payloadFormat() != Objects.requireNonNull(payloadFormat, "payloadFormat")
                || (write ? !capability.writable() : !capability.readable())) {
            throw unsupported("exact physical/logical format is not admitted: " + key);
        }
        return capability;
    }

    private static Checksum digest(Collection<PhysicalFormatCapability> capabilities) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
        List<String> identities = capabilities.stream()
                .map(PhysicalFormatCapability::canonicalIdentity)
                .sorted(Comparator.naturalOrder())
                .toList();
        update(digest, "nereus-physical-format-capabilities-v1");
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(identities.size()).array());
        identities.forEach(value -> update(digest, value));
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static NereusException unsupported(String message) {
        return new NereusException(ErrorCode.UNSUPPORTED_READ_TARGET, false, message);
    }
}
