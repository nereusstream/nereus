/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Expiring exact-broker-set proof used to admit one Kafka storage activation epoch. */
public record KafkaStorageReadinessRecord(
        int recordVersion,
        String kafkaClusterId,
        long readinessEpoch,
        long kraftMetadataOffset,
        List<KafkaBrokerIdentity> brokers,
        byte[] brokerSetSha256,
        byte[] capabilitySha256,
        byte[] providerScopeSha256,
        long createdAtMillis,
        long expiresAtMillis,
        long metadataVersion) {
    public static final int RECORD_VERSION = 1;
    public static final int MAX_BROKERS = 16_384;

    public KafkaStorageReadinessRecord {
        if (recordVersion != RECORD_VERSION) {
            throw new IllegalArgumentException("recordVersion must be 1");
        }
        kafkaClusterId = KafkaMetadataValidation.text(kafkaClusterId, "kafkaClusterId");
        brokers = KafkaMetadataValidation.list(brokers, MAX_BROKERS, "brokers");
        if (brokers.isEmpty()) {
            throw new IllegalArgumentException("brokers must be non-empty");
        }
        for (int index = 1; index < brokers.size(); index++) {
            if (brokers.get(index - 1).compareTo(brokers.get(index)) >= 0) {
                throw new IllegalArgumentException("brokers must be strictly sorted and unique");
            }
        }
        brokerSetSha256 = KafkaMetadataValidation.sha256(
                brokerSetSha256, "brokerSetSha256", false);
        if (!Arrays.equals(brokerSetSha256, brokerSetSha256(brokers))) {
            throw new IllegalArgumentException("brokerSetSha256 does not match the canonical broker set");
        }
        capabilitySha256 = KafkaMetadataValidation.sha256(
                capabilitySha256, "capabilitySha256", false);
        providerScopeSha256 = KafkaMetadataValidation.sha256(
                providerScopeSha256, "providerScopeSha256", false);
        if (readinessEpoch <= 0 || kraftMetadataOffset < 0 || createdAtMillis <= 0
                || expiresAtMillis <= createdAtMillis || metadataVersion < 0) {
            throw new IllegalArgumentException("invalid Kafka storage readiness numeric fields");
        }
    }

    public KafkaStorageReadinessRecord withMetadataVersion(long version) {
        return new KafkaStorageReadinessRecord(
                recordVersion,
                kafkaClusterId,
                readinessEpoch,
                kraftMetadataOffset,
                brokers,
                brokerSetSha256,
                capabilitySha256,
                providerScopeSha256,
                createdAtMillis,
                expiresAtMillis,
                version);
    }

    public static byte[] brokerSetSha256(List<KafkaBrokerIdentity> supplied) {
        List<KafkaBrokerIdentity> brokers = List.copyOf(Objects.requireNonNull(supplied, "brokers"));
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            ByteBuffer encoded = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            for (KafkaBrokerIdentity broker : brokers) {
                encoded.clear();
                encoded.putInt(broker.brokerId());
                encoded.putLong(broker.brokerEpoch());
                digest.update(encoded.array());
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    @Override public byte[] brokerSetSha256() { return brokerSetSha256.clone(); }
    @Override public byte[] capabilitySha256() { return capabilitySha256.clone(); }
    @Override public byte[] providerScopeSha256() { return providerScopeSha256.clone(); }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof KafkaStorageReadinessRecord that
                && recordVersion == that.recordVersion && readinessEpoch == that.readinessEpoch
                && kraftMetadataOffset == that.kraftMetadataOffset
                && createdAtMillis == that.createdAtMillis && expiresAtMillis == that.expiresAtMillis
                && metadataVersion == that.metadataVersion && kafkaClusterId.equals(that.kafkaClusterId)
                && brokers.equals(that.brokers) && Arrays.equals(brokerSetSha256, that.brokerSetSha256)
                && Arrays.equals(capabilitySha256, that.capabilitySha256)
                && Arrays.equals(providerScopeSha256, that.providerScopeSha256);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                recordVersion, kafkaClusterId, readinessEpoch, kraftMetadataOffset, brokers,
                createdAtMillis, expiresAtMillis, metadataVersion);
        result = 31 * result + Arrays.hashCode(brokerSetSha256);
        result = 31 * result + Arrays.hashCode(capabilitySha256);
        return 31 * result + Arrays.hashCode(providerScopeSha256);
    }
}
