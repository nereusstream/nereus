/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.bookkeeper;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.BookKeeperWriterMetadataStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.StreamMetadataSnapshot;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerProtectionRecord;
import com.nereusstream.metadata.oxia.records.ProtectionLifecycle;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production BK_ONLY bridge from monotonic L0 trim/abandoned-reservation facts to protection retirement. */
public final class BookKeeperWalOnlyRetirementAuthority implements BookKeeperWalRetirementAuthority {
    private final String cluster;
    private final OxiaMetadataStore l0;
    private final BookKeeperWriterMetadataStore writerMetadata;
    private final OxiaKeyspace keys;

    public BookKeeperWalOnlyRetirementAuthority(
            String cluster,
            OxiaMetadataStore l0,
            BookKeeperWriterMetadataStore writerMetadata) {
        this.cluster = text(cluster, "cluster");
        this.l0 = Objects.requireNonNull(l0, "l0");
        this.writerMetadata = Objects.requireNonNull(writerMetadata, "writerMetadata");
        this.keys = new OxiaKeyspace(cluster);
    }

    public CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveLogicalTrim(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> exact =
                Objects.requireNonNull(protection, "protection");
        if (exact.value().lifecycle() != ProtectionLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(invariant(
                    "logical trim can retire only an exact ACTIVE BookKeeper protection"));
        }
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                Objects.requireNonNull(timeout, "timeout"));
        StreamId stream = new StreamId(exact.value().streamId());
        return deadline.bound(l0.getStreamSnapshot(cluster, stream)).thenApply(snapshot -> {
            requireBookKeeperStream(stream, snapshot);
            if (snapshot.trim().trimOffset() < exact.value().offsetEnd()) return Optional.empty();
            return Optional.of(new BookKeeperProtectionRetirementProof(
                    exact.key(),
                    exact.metadataVersion(),
                    exact.durableValueSha256(),
                    exact.value().ownerKey(),
                    exact.value().ownerMetadataVersion(),
                    sha(exact.value().ownerIdentitySha256()),
                    keys.streamHeadKey(stream),
                    snapshot.metadataVersion(),
                    snapshotDigest(snapshot),
                    BookKeeperProtectionRetirementProof.Reason.LOGICAL_TRIM));
        });
    }

    public CompletableFuture<Optional<BookKeeperProtectionRetirementProof>> proveAbandonedAppend(
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> exact =
                Objects.requireNonNull(protection, "protection");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                Objects.requireNonNull(timeout, "timeout"));
        StreamId stream = new StreamId(exact.value().streamId());
        return deadline.bound(writerMetadata.getReservation(cluster, stream, exact.value().referenceId()))
                .thenApply(optional -> optional.flatMap(reservation -> {
                    if (reservation.value().lifecycle() != AppendReservationLifecycle.ABANDONED) {
                        return Optional.empty();
                    }
                    requireReservation(exact.value(), reservation.value());
                    String ownerKey = exact.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.key() : exact.value().ownerKey();
                    long ownerVersion = exact.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.metadataVersion() : exact.value().ownerMetadataVersion();
                    Checksum ownerIdentity = exact.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.durableValueSha256() : sha(exact.value().ownerIdentitySha256());
                    return Optional.of(new BookKeeperProtectionRetirementProof(
                            exact.key(),
                            exact.metadataVersion(),
                            exact.durableValueSha256(),
                            ownerKey,
                            ownerVersion,
                            ownerIdentity,
                            reservation.key(),
                            reservation.metadataVersion(),
                            reservation.durableValueSha256(),
                            BookKeeperProtectionRetirementProof.Reason.ABANDONED_APPEND));
                }));
    }

    @Override
    public CompletableFuture<Void> revalidate(
            BookKeeperProtectionRetirementProof proof,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            Duration timeout) {
        BookKeeperProtectionRetirementProof expected = Objects.requireNonNull(proof, "proof");
        BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> current =
                Objects.requireNonNull(protection, "protection");
        BookKeeperOperationDeadline deadline = new BookKeeperOperationDeadline(
                Objects.requireNonNull(timeout, "timeout"));
        return switch (expected.reason()) {
            case LOGICAL_TRIM -> revalidateTrim(expected, current, deadline);
            case ABANDONED_APPEND -> revalidateAbandoned(expected, current, deadline);
            case HEALTHY_HIGHER_GENERATION -> CompletableFuture.failedFuture(new NereusException(
                    ErrorCode.UNSUPPORTED_STORAGE_PROFILE,
                    false,
                    "BK_ONLY retirement authority cannot prove a higher Object generation"));
        };
    }

    private CompletableFuture<Void> revalidateTrim(
            BookKeeperProtectionRetirementProof proof,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            BookKeeperOperationDeadline deadline) {
        StreamId stream = new StreamId(protection.value().streamId());
        return deadline.bound(l0.getStreamSnapshot(cluster, stream)).thenAccept(snapshot -> {
            requireBookKeeperStream(stream, snapshot);
            if (!proof.authorityKey().equals(keys.streamHeadKey(stream))
                    || proof.authorityMetadataVersion() != snapshot.metadataVersion()
                    || !proof.authorityRecordSha256().equals(snapshotDigest(snapshot))
                    || snapshot.trim().trimOffset() < protection.value().offsetEnd()) {
                throw condition("BookKeeper logical-trim retirement authority changed");
            }
        });
    }

    private CompletableFuture<Void> revalidateAbandoned(
            BookKeeperProtectionRetirementProof proof,
            BookKeeperVersionedValue<BookKeeperLedgerProtectionRecord> protection,
            BookKeeperOperationDeadline deadline) {
        StreamId stream = new StreamId(protection.value().streamId());
        return deadline.bound(writerMetadata.getReservation(
                        cluster, stream, protection.value().referenceId()))
                .thenAccept(optional -> {
                    var reservation = optional.orElseThrow(() -> condition(
                            "abandoned BookKeeper reservation disappeared during protection retirement"));
                    requireReservation(protection.value(), reservation.value());
                    String expectedOwnerKey = protection.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.key() : protection.value().ownerKey();
                    long expectedOwnerVersion = protection.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.metadataVersion() : protection.value().ownerMetadataVersion();
                    Checksum expectedOwnerIdentity = protection.value().lifecycle() == ProtectionLifecycle.RESERVED
                            ? reservation.durableValueSha256() : sha(protection.value().ownerIdentitySha256());
                    if (reservation.value().lifecycle() != AppendReservationLifecycle.ABANDONED
                            || !proof.authorityKey().equals(reservation.key())
                            || proof.authorityMetadataVersion() != reservation.metadataVersion()
                            || !proof.authorityRecordSha256().equals(reservation.durableValueSha256())
                            || !proof.ownerKey().equals(expectedOwnerKey)
                            || proof.ownerMetadataVersion() != expectedOwnerVersion
                            || !proof.ownerIdentitySha256().equals(expectedOwnerIdentity)) {
                        throw condition("abandoned BookKeeper reservation authority changed");
                    }
                });
    }

    private static void requireBookKeeperStream(StreamId stream, StreamMetadataSnapshot snapshot) {
        if (!snapshot.metadata().streamId().equals(stream.value())
                || !snapshot.committedEnd().streamId().equals(stream.value())
                || !snapshot.trim().streamId().equals(stream.value())) {
            throw invariant("BookKeeper retirement snapshot belongs to another stream");
        }
        StorageProfile profile;
        try {
            profile = StorageProfile.valueOf(snapshot.metadata().profile()).canonical();
        } catch (RuntimeException failure) {
            throw invariant("BookKeeper retirement snapshot has an unknown storage profile");
        }
        if (!profile.usesBookKeeperWal()) {
            throw invariant("BookKeeper protection belongs to a non-BookKeeper stream profile");
        }
    }

    private static void requireReservation(
            BookKeeperLedgerProtectionRecord protection,
            BookKeeperAppendReservationRecord reservation) {
        if (!reservation.reservationId().equals(protection.referenceId())
                || !reservation.streamId().equals(protection.streamId())
                || reservation.ledgerId() != protection.ledgerId()
                || reservation.ledgerRootEpoch() != protection.rootLifecycleEpoch()
                || reservation.ledgerRangeSlot() != protection.ledgerRangeSlot()
                || reservation.firstEntryId() != protection.firstEntryId()
                || reservation.entryCount() != protection.entryCount()
                || !reservation.rangeChecksumSha256().equals(protection.rangeChecksumSha256())
                || reservation.expectedStartOffset() != protection.offsetStart()
                || Math.addExact(reservation.expectedStartOffset(), reservation.recordCount())
                        != protection.offsetEnd()) {
            throw invariant("BookKeeper abandoned reservation does not match its protection range");
        }
    }

    private static Checksum snapshotDigest(StreamMetadataSnapshot snapshot) {
        MessageDigest digest = digest();
        frame(digest, "NBKTRIM1");
        frame(digest, snapshot.metadata().streamId());
        frame(digest, snapshot.metadata().streamName());
        frame(digest, snapshot.metadata().streamNameHash());
        frame(digest, snapshot.metadata().state());
        frame(digest, snapshot.metadata().profile());
        snapshot.metadata().attributes().forEach((key, value) -> {
            frame(digest, key);
            frame(digest, value);
        });
        number(digest, snapshot.metadata().createdAtMillis());
        number(digest, snapshot.metadata().policyVersion());
        number(digest, snapshot.committedEnd().committedEndOffset());
        number(digest, snapshot.committedEnd().cumulativeSize());
        number(digest, snapshot.committedEnd().commitVersion());
        number(digest, snapshot.trim().trimOffset());
        number(digest, snapshot.metadataVersion());
        return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest.digest()));
    }

    private static Checksum sha(String value) {
        return new Checksum(ChecksumType.SHA256, value);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void frame(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void number(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }
}
