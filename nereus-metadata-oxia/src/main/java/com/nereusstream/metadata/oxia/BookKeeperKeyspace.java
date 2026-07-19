/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.StreamId;
import com.nereusstream.api.keys.KeyComponentCodec;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/** Strict durable key/partition builder for BookKeeper primary-WAL metadata. */
public final class BookKeeperKeyspace {
    public static final int LEDGER_SHARDS = 256;
    public static final int ALLOCATION_SLOT_SHARDS = 16;

    private final OxiaKeyspace oxia;
    private final int maxAppendRangesPerLedger;
    private final int protectionSlotsPerRange;
    private final int maxReaderLeasesPerLedger;
    private final int maxUncertainAllocations;

    public BookKeeperKeyspace(String cluster, int maxAppendRangesPerLedger, int protectionSlotsPerRange,
            int maxReaderLeasesPerLedger, int maxUncertainAllocations) {
        this.oxia = new OxiaKeyspace(cluster);
        this.maxAppendRangesPerLedger = positive(maxAppendRangesPerLedger, "maxAppendRangesPerLedger");
        this.protectionSlotsPerRange = positive(protectionSlotsPerRange, "protectionSlotsPerRange");
        this.maxReaderLeasesPerLedger = positive(maxReaderLeasesPerLedger, "maxReaderLeasesPerLedger");
        this.maxUncertainAllocations = positive(maxUncertainAllocations, "maxUncertainAllocations");
        Math.multiplyExact(maxAppendRangesPerLedger, protectionSlotsPerRange);
    }

    public String writerStateKey(StreamId streamId) {
        return streamPrefix(streamId) + "/bookkeeper/v1/writer-state";
    }

    public String allocationKey(StreamId streamId, String allocationId) {
        return streamPrefix(streamId) + "/bookkeeper/v1/allocations/"
                + KeyComponentCodec.encodeComponent(text(allocationId, "allocationId"));
    }

    public String allocationPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/bookkeeper/v1/allocations";
    }

    public String appendReservationKey(StreamId streamId, String reservationId) {
        return streamPrefix(streamId) + "/bookkeeper/v1/append-reservations/"
                + KeyComponentCodec.encodeComponent(text(reservationId, "reservationId"));
    }

    public String appendReservationPrefix(StreamId streamId) {
        return streamPrefix(streamId) + "/bookkeeper/v1/append-reservations";
    }

    public PartitionKey streamPartitionKey(StreamId streamId) {
        return oxia.streamPartitionKey(streamId);
    }

    public String ledgerIdentitySha256(String providerScopeSha256, long ledgerId) {
        byte[] scope = decodeSha256(providerScopeSha256, "providerScopeSha256");
        if (ledgerId <= 0) throw new IllegalArgumentException("ledgerId must be positive");
        MessageDigest digest = sha256();
        digest.update(scope);
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(ledgerId).array());
        return HexFormat.of().formatHex(digest.digest());
    }

    public int ledgerShard(String ledgerIdentitySha256) {
        return Byte.toUnsignedInt(decodeSha256(ledgerIdentitySha256, "ledgerIdentitySha256")[0]);
    }

    public PartitionKey ledgerPartitionKey(String providerScopeSha256, long ledgerId) {
        return ledgerPartitionKeyByShard(ledgerShard(ledgerIdentitySha256(providerScopeSha256, ledgerId)));
    }

    public PartitionKey ledgerPartitionKeyByShard(int shard) {
        requireIndex(shard, LEDGER_SHARDS, "ledgerShard");
        return new PartitionKey("physical-bookkeeper-v1-" + digits(shard, 3));
    }

    public String ledgerRootKey(String providerScopeSha256, long ledgerId) {
        String identity = ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return ledgerRootShardPrefix(ledgerShard(identity)) + "/" + identity;
    }

    public String ledgerRootShardPrefix(int shard) {
        requireIndex(shard, LEDGER_SHARDS, "ledgerShard");
        return physicalShardPrefix(shard) + "/roots";
    }

    public String protectionKey(String providerScopeSha256, long ledgerId, int rangeSlot, int protectionSlot) {
        requireIndex(rangeSlot, maxAppendRangesPerLedger, "rangeSlot");
        requireIndex(protectionSlot, protectionSlotsPerRange, "protectionSlot");
        String identity = ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return ledgerPrefix(identity) + "/protections/" + digits(rangeSlot, 5) + "/" + digits(protectionSlot, 2);
    }

    public String protectionPrefix(String providerScopeSha256, long ledgerId) {
        return ledgerPrefix(ledgerIdentitySha256(providerScopeSha256, ledgerId)) + "/protections";
    }

    public String readerLeaseKey(String providerScopeSha256, long ledgerId, int readerSlot) {
        requireIndex(readerSlot, maxReaderLeasesPerLedger, "readerSlot");
        String identity = ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return ledgerPrefix(identity) + "/readers/" + digits(readerSlot, 5);
    }

    public String readerLeasePrefix(String providerScopeSha256, long ledgerId) {
        return ledgerPrefix(ledgerIdentitySha256(providerScopeSha256, ledgerId)) + "/readers";
    }

    public String retirementManifestKey(String providerScopeSha256, long ledgerId, String gcAttemptId) {
        String identity = ledgerIdentitySha256(providerScopeSha256, ledgerId);
        return ledgerPrefix(identity) + "/retirement/"
                + KeyComponentCodec.encodeComponent(text(gcAttemptId, "gcAttemptId")) + "/manifest";
    }

    public int allocationSlotShard(int slot) {
        requireIndex(slot, maxUncertainAllocations, "slot");
        return slot & 0x0f;
    }

    public void requireLedgerRangeSlot(int rangeSlot) {
        requireIndex(rangeSlot, maxAppendRangesPerLedger, "rangeSlot");
    }

    public void requireProtectionSlot(int protectionSlot) {
        requireIndex(protectionSlot, protectionSlotsPerRange, "protectionSlot");
    }

    public void requireReaderSlot(int readerSlot) {
        requireIndex(readerSlot, maxReaderLeasesPerLedger, "readerSlot");
    }

    public void requireAllocationSlot(int slot) {
        requireIndex(slot, maxUncertainAllocations, "slot");
    }

    public PartitionKey allocationSlotPartitionKey(int slotShard) {
        requireIndex(slotShard, ALLOCATION_SLOT_SHARDS, "slotShard");
        return new PartitionKey("physical-bookkeeper-allocation-v1-" + digits(slotShard, 2));
    }

    public String allocationSlotKey(int slot) {
        int shard = allocationSlotShard(slot);
        return allocationSlotShardPrefix(shard) + "/" + digits(slot, 5);
    }

    public String allocationSlotShardPrefix(int shard) {
        requireIndex(shard, ALLOCATION_SLOT_SHARDS, "slotShard");
        return oxia.prefix() + "/physical-bookkeeper/v1/allocation-slots/" + digits(shard, 2);
    }

    public RootKeyIdentity parseRootKey(String key, String providerScopeSha256, long ledgerId) {
        String identity = ledgerIdentitySha256(providerScopeSha256, ledgerId);
        int shard = ledgerShard(identity);
        if (!ledgerRootKey(providerScopeSha256, ledgerId).equals(text(key, "rootKey"))) {
            throw new IllegalArgumentException("BookKeeper root key is not the canonical exact identity");
        }
        return new RootKeyIdentity(shard, identity, ledgerId);
    }

    public ProtectionKeyIdentity parseProtectionKey(String key, String providerScopeSha256, long ledgerId) {
        String prefix = protectionPrefix(providerScopeSha256, ledgerId) + "/";
        String supplied = text(key, "protectionKey");
        if (!supplied.startsWith(prefix)) throw new IllegalArgumentException("protection key has wrong ledger scope");
        String suffix = supplied.substring(prefix.length());
        int separator = suffix.indexOf('/');
        if (separator <= 0 || separator == suffix.length() - 1 || suffix.indexOf('/', separator + 1) >= 0) {
            throw new IllegalArgumentException("protection key has invalid depth");
        }
        int rangeSlot = parseDigits(suffix.substring(0, separator), 5, maxAppendRangesPerLedger, "rangeSlot");
        int protectionSlot = parseDigits(suffix.substring(separator + 1), 2, protectionSlotsPerRange, "protectionSlot");
        if (!protectionKey(providerScopeSha256, ledgerId, rangeSlot, protectionSlot).equals(supplied)) {
            throw new IllegalArgumentException("protection key is not canonical");
        }
        return new ProtectionKeyIdentity(ledgerIdentitySha256(providerScopeSha256, ledgerId), ledgerId,
                rangeSlot, protectionSlot);
    }

    public ReaderKeyIdentity parseReaderLeaseKey(String key, String providerScopeSha256, long ledgerId) {
        String prefix = readerLeasePrefix(providerScopeSha256, ledgerId) + "/";
        String supplied = text(key, "readerLeaseKey");
        if (!supplied.startsWith(prefix)) throw new IllegalArgumentException("reader key has wrong ledger scope");
        String suffix = supplied.substring(prefix.length());
        if (suffix.indexOf('/') >= 0) throw new IllegalArgumentException("reader key has invalid depth");
        int readerSlot = parseDigits(suffix, 5, maxReaderLeasesPerLedger, "readerSlot");
        if (!readerLeaseKey(providerScopeSha256, ledgerId, readerSlot).equals(supplied)) {
            throw new IllegalArgumentException("reader key is not canonical");
        }
        return new ReaderKeyIdentity(ledgerIdentitySha256(providerScopeSha256, ledgerId), ledgerId, readerSlot);
    }

    public AllocationSlotKeyIdentity parseAllocationSlotKey(String key) {
        String supplied = text(key, "allocationSlotKey");
        String prefix = oxia.prefix() + "/physical-bookkeeper/v1/allocation-slots/";
        if (!supplied.startsWith(prefix)) throw new IllegalArgumentException("allocation slot key has wrong namespace");
        String suffix = supplied.substring(prefix.length());
        int separator = suffix.indexOf('/');
        if (separator <= 0 || separator == suffix.length() - 1 || suffix.indexOf('/', separator + 1) >= 0) {
            throw new IllegalArgumentException("allocation slot key has invalid depth");
        }
        int shard = parseDigits(suffix.substring(0, separator), 2, ALLOCATION_SLOT_SHARDS, "slotShard");
        int slot = parseDigits(suffix.substring(separator + 1), 5, maxUncertainAllocations, "slot");
        if (allocationSlotShard(slot) != shard || !allocationSlotKey(slot).equals(supplied)) {
            throw new IllegalArgumentException("allocation slot key has noncanonical shard");
        }
        return new AllocationSlotKeyIdentity(shard, slot);
    }

    private String streamPrefix(StreamId streamId) {
        return oxia.prefix() + "/streams/" + KeyComponentCodec.encodeComponent(
                Objects.requireNonNull(streamId, "streamId").value());
    }

    private String physicalShardPrefix(int shard) {
        return oxia.prefix() + "/physical-bookkeeper/v1/" + digits(shard, 3);
    }

    private String ledgerPrefix(String ledgerIdentitySha256) {
        return physicalShardPrefix(ledgerShard(ledgerIdentitySha256)) + "/ledgers/" + ledgerIdentitySha256;
    }

    private static int parseDigits(String value, int width, int upperExclusive, String name) {
        if (value.length() != width) throw new IllegalArgumentException(name + " has wrong width");
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) < '0' || value.charAt(index) > '9') {
                throw new IllegalArgumentException(name + " is not decimal");
            }
        }
        int decoded;
        try { decoded = Integer.parseInt(value); }
        catch (NumberFormatException failure) { throw new IllegalArgumentException(name + " is out of range", failure); }
        requireIndex(decoded, upperExclusive, name);
        if (!digits(decoded, width).equals(value)) throw new IllegalArgumentException(name + " is not canonical");
        return decoded;
    }

    private static String digits(int value, int width) {
        return String.format(Locale.ROOT, "%0" + width + "d", value);
    }

    private static void requireIndex(int value, int upperExclusive, String name) {
        if (value < 0 || value >= upperExclusive) {
            throw new IllegalArgumentException(name + " is outside its configured bound");
        }
    }

    private static int positive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }

    private static String text(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " cannot be blank");
        return value;
    }

    private static byte[] decodeSha256(String value, String name) {
        text(value, name);
        if (value.length() != 64) throw new IllegalArgumentException(name + " must be lowercase SHA-256 hex");
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException(name + " must be lowercase SHA-256 hex");
            }
        }
        return HexFormat.of().parseHex(value);
    }

    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    public record RootKeyIdentity(int shard, String ledgerIdentitySha256, long ledgerId) { }
    public record ProtectionKeyIdentity(String ledgerIdentitySha256, long ledgerId, int rangeSlot, int protectionSlot) { }
    public record ReaderKeyIdentity(String ledgerIdentitySha256, long ledgerId, int readerSlot) { }
    public record AllocationSlotKeyIdentity(int shard, int slot) { }
}
