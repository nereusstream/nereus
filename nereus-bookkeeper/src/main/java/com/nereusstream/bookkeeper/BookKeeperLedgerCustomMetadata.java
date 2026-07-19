/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Canonical immutable NBKL1 ownership map carried by every Nereus BookKeeper ledger. */
public final class BookKeeperLedgerCustomMetadata {
    public static final String FORMAT = "NBKL1";

    private final Map<String, byte[]> values;
    private final Checksum sha256;

    private BookKeeperLedgerCustomMetadata(Map<String, byte[]> supplied) {
        LinkedHashMap<String, byte[]> copied = new LinkedHashMap<>();
        supplied.entrySet().stream().sorted(Map.Entry.comparingByKey(unsignedUtf8Comparator()))
                .forEach(entry -> copied.put(text(entry.getKey(), "metadata key"),
                        Objects.requireNonNull(entry.getValue(), "metadata value").clone()));
        values = Collections.unmodifiableMap(copied);
        sha256 = digest(values);
    }

    public static BookKeeperLedgerCustomMetadata create(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerIdNamespaceReservation reservation,
            StreamId streamId,
            long segmentSequence,
            String allocationId) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(reservation, "reservation");
        Objects.requireNonNull(streamId, "streamId");
        if (segmentSequence < 0) throw new IllegalArgumentException("segmentSequence must be non-negative");
        Map<String, byte[]> map = new LinkedHashMap<>();
        put(map, "nereus.format", FORMAT);
        put(map, "nereus.cluster-sha256", sha256(text(cluster, "cluster")));
        put(map, "nereus.cluster-alias", configuration.clusterAlias());
        put(map, "nereus.provider-scope", configuration.providerScopeSha256());
        put(map, "nereus.ledger-namespace", reservation.ledgerIdNamespaceSha256().value());
        put(map, "nereus.stream-sha256", sha256(streamId.value()));
        put(map, "nereus.segment-sequence", Long.toUnsignedString(segmentSequence));
        put(map, "nereus.allocation-id", text(allocationId, "allocationId"));
        put(map, "nereus.config-sha256", configuration.configurationBindingSha256().value());
        return new BookKeeperLedgerCustomMetadata(map);
    }

    /** Reconstructs the exact provider map from a durable root without requiring a live allocation handle. */
    public static BookKeeperLedgerCustomMetadata fromRoot(
            String cluster,
            BookKeeperWalConfiguration configuration,
            BookKeeperLedgerRootRecord root) {
        Objects.requireNonNull(configuration, "configuration");
        BookKeeperLedgerRootRecord exact = Objects.requireNonNull(root, "root");
        if (!exact.clusterAlias().equals(configuration.clusterAlias())
                || !exact.providerScopeSha256().equals(configuration.providerScopeSha256())
                || !exact.configurationBindingSha256().equals(
                        configuration.configurationBindingSha256().value())) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper root configuration binding cannot reconstruct provider metadata");
        }
        Map<String, byte[]> map = new LinkedHashMap<>();
        put(map, "nereus.format", FORMAT);
        put(map, "nereus.cluster-sha256", sha256(text(cluster, "cluster")));
        put(map, "nereus.cluster-alias", configuration.clusterAlias());
        put(map, "nereus.provider-scope", configuration.providerScopeSha256());
        put(map, "nereus.ledger-namespace", exact.ledgerIdNamespaceSha256());
        put(map, "nereus.stream-sha256", sha256(exact.streamId()));
        put(map, "nereus.segment-sequence", Long.toUnsignedString(exact.segmentSequence()));
        put(map, "nereus.allocation-id", exact.allocationId());
        put(map, "nereus.config-sha256", configuration.configurationBindingSha256().value());
        BookKeeperLedgerCustomMetadata reconstructed = new BookKeeperLedgerCustomMetadata(map);
        if (!reconstructed.sha256().value().equals(exact.customMetadataSha256())) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper root custom-metadata digest cannot be reconstructed");
        }
        return reconstructed;
    }

    public Map<String, byte[]> values() {
        return values.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().clone()));
    }

    public Checksum sha256() {
        return sha256;
    }

    public Checksum requireExactImmutableLedgerMetadata(
            long ledgerId, BookKeeperWalConfiguration configuration, LedgerMetadata metadata) {
        Objects.requireNonNull(configuration, "configuration");
        LedgerMetadata actual = Objects.requireNonNull(metadata, "metadata");
        if (actual.getLedgerId() != ledgerId
                || actual.getEnsembleSize() != configuration.ensembleSize()
                || actual.getWriteQuorumSize() != configuration.writeQuorumSize()
                || actual.getAckQuorumSize() != configuration.ackQuorumSize()
                || actual.getDigestType() != configuration.digestType().toClientType()
                || !sameMap(values, actual.getCustomMetadata())) {
            throw new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false,
                    "BookKeeper ledger immutable metadata does not match its durable allocation identity");
        }
        MessageDigest digest = sha256Digest();
        frame(digest, "NBKM1".getBytes(StandardCharsets.US_ASCII));
        frame(digest, ByteBuffer.allocate(Long.BYTES).putLong(ledgerId).array());
        frame(digest, ByteBuffer.allocate(Integer.BYTES).putInt(actual.getEnsembleSize()).array());
        frame(digest, ByteBuffer.allocate(Integer.BYTES).putInt(actual.getWriteQuorumSize()).array());
        frame(digest, ByteBuffer.allocate(Integer.BYTES).putInt(actual.getAckQuorumSize()).array());
        frame(digest, actual.getDigestType().name().getBytes(StandardCharsets.US_ASCII));
        frame(digest, HexFormat.of().parseHex(sha256.value()));
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static boolean sameMap(Map<String, byte[]> expected, Map<String, byte[]> actual) {
        if (actual == null || expected.size() != actual.size() || !expected.keySet().equals(actual.keySet())) {
            return false;
        }
        return expected.entrySet().stream().allMatch(entry ->
                Arrays.equals(entry.getValue(), actual.get(entry.getKey())));
    }

    private static Checksum digest(Map<String, byte[]> map) {
        MessageDigest digest = sha256Digest();
        frame(digest, "NBKMD1".getBytes(StandardCharsets.US_ASCII));
        map.forEach((key, value) -> {
            frame(digest, key.getBytes(StandardCharsets.UTF_8));
            frame(digest, value);
        });
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static Comparator<String> unsignedUtf8Comparator() {
        return (left, right) -> Arrays.compareUnsigned(
                left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }

    private static void put(Map<String, byte[]> map, String key, String value) {
        map.put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(String value) {
        return HexFormat.of().formatHex(sha256Digest().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256Digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static void frame(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
