/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.codec;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ReadTargetIdentities;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.api.target.ReadTargetType;
import com.nereusstream.metadata.oxia.records.ReadTargetRecord;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Immutable registry and identity checker for canonical target codecs. */
public final class ReadTargetCodecRegistry {
    public static final String PAYLOAD_ENCODING = "canonical-target-v1";
    private final Map<Key, ReadTargetCodec<?>> codecs;

    public ReadTargetCodecRegistry(List<ReadTargetCodec<?>> codecs) {
        this.codecs = List.copyOf(codecs).stream().collect(Collectors.toUnmodifiableMap(
                codec -> new Key(codec.targetType(), codec.targetVersion()), Function.identity()));
    }

    public static ReadTargetCodecRegistry phase15() {
        return new ReadTargetCodecRegistry(List.of(
                new ObjectSliceReadTargetCodecV1(), new BookKeeperEntryRangeReadTargetCodecV1()));
    }

    public ReadTargetRecord encode(ReadTarget target) {
        Objects.requireNonNull(target, "target");
        ReadTargetCodec<ReadTarget> codec = codec(target.type(), target.version());
        if (!codec.targetClass().isInstance(target)) {
            throw new MetadataCodecException("target discriminator and Java type disagree");
        }
        byte[] payload = codec.encode(target);
        String identity = ReadTargetIdentities.sha256(target).value();
        return new ReadTargetRecord(target.type().name(), target.version(), PAYLOAD_ENCODING,
                payload, ChecksumType.SHA256.name(), identity);
    }

    public ReadTarget decode(ReadTargetRecord record) {
        Objects.requireNonNull(record, "record");
        if (!PAYLOAD_ENCODING.equals(record.payloadEncoding())
                || !ChecksumType.SHA256.name().equals(record.identityChecksumType())) {
            throw new MetadataCodecException("unsupported read target encoding or identity checksum");
        }
        String expected = identity(record.targetType(), record.targetVersion(), record.payload());
        if (!MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),
                record.identityChecksumValue().getBytes(StandardCharsets.US_ASCII))) {
            throw new MetadataCodecException("read target identity checksum mismatch");
        }
        ReadTargetType type;
        try {
            type = ReadTargetType.valueOf(record.targetType());
        } catch (IllegalArgumentException e) {
            throw new MetadataCodecException("unknown read target type", e);
        }
        return codec(type, record.targetVersion()).decode(record.payload());
    }

    @SuppressWarnings("unchecked")
    private ReadTargetCodec<ReadTarget> codec(ReadTargetType type, int version) {
        ReadTargetCodec<?> codec = codecs.get(new Key(type, version));
        if (codec == null) throw new MetadataCodecException("unsupported read target codec: " + type + "/" + version);
        return (ReadTargetCodec<ReadTarget>) codec;
    }

    static String identity(String targetType, int version, byte[] payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(StrictUtf8.encode(targetType));
            digest.update((byte) 0);
            digest.update(Integer.toString(version).getBytes(StandardCharsets.US_ASCII));
            digest.update((byte) 0);
            digest.update(payload);
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
    }

    private record Key(ReadTargetType type, int version) { }
}
