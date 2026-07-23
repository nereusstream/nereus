/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaPayloadMapping;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable local broker facts and timing used to publish one epoch-scoped capability. */
public final class KafkaBrokerCapabilitySpecification {
    private final String kafkaClusterId;
    private final KafkaBrokerIdentity identity;
    private final String runtimeInstanceId;
    private final String kafkaVersion;
    private final String nereusBuild;
    private final String javaVersion;
    private final List<String> supportedStorageProfiles;
    private final String defaultStorageProfile;
    private final byte[] configCompatibilitySha256;
    private final byte[] codeCapabilitySha256;
    private final byte[] providerScopeSha256;
    private final Duration heartbeatInterval;
    private final Duration expiry;

    public KafkaBrokerCapabilitySpecification(
            String kafkaClusterId,
            KafkaBrokerIdentity identity,
            String runtimeInstanceId,
            String kafkaVersion,
            String nereusBuild,
            String javaVersion,
            Set<StorageProfile> supportedStorageProfiles,
            StorageProfile defaultStorageProfile,
            byte[] configCompatibilitySha256,
            byte[] codeCapabilitySha256,
            byte[] providerScopeSha256,
            Duration heartbeatInterval,
            Duration expiry) {
        this.kafkaClusterId = nonblank(kafkaClusterId, "kafkaClusterId");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.runtimeInstanceId = nonblank(runtimeInstanceId, "runtimeInstanceId");
        this.kafkaVersion = nonblank(kafkaVersion, "kafkaVersion");
        this.nereusBuild = nonblank(nereusBuild, "nereusBuild");
        this.javaVersion = nonblank(javaVersion, "javaVersion");
        this.supportedStorageProfiles = canonicalProfiles(supportedStorageProfiles);
        StorageProfile exactDefault = Objects.requireNonNull(defaultStorageProfile, "defaultStorageProfile");
        if (exactDefault.canonical() != exactDefault
                || !this.supportedStorageProfiles.contains(exactDefault.name())) {
            throw new IllegalArgumentException("defaultStorageProfile must be canonical and supported");
        }
        this.defaultStorageProfile = exactDefault.name();
        this.configCompatibilitySha256 = sha256(configCompatibilitySha256, "configCompatibilitySha256");
        this.codeCapabilitySha256 = sha256(codeCapabilitySha256, "codeCapabilitySha256");
        this.providerScopeSha256 = sha256(providerScopeSha256, "providerScopeSha256");
        this.heartbeatInterval = positive(heartbeatInterval, "heartbeatInterval");
        this.expiry = positive(expiry, "expiry");
        long minimumExpiry;
        try {
            minimumExpiry = Math.multiplyExact(this.heartbeatInterval.toMillis(), 3L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("heartbeatInterval is too large", failure);
        }
        if (this.expiry.toMillis() < minimumExpiry) {
            throw new IllegalArgumentException("expiry must be at least three heartbeat intervals");
        }
    }

    public KafkaBrokerCapabilityRecord initialRecord(long nowMillis) {
        if (nowMillis <= 0) {
            throw new IllegalArgumentException("capability time must be positive");
        }
        return record(nowMillis, nowMillis, addExact(nowMillis, expiry.toMillis()));
    }

    public KafkaBrokerCapabilityRecord heartbeatRecord(
            KafkaBrokerCapabilityRecord current, long nowMillis) {
        KafkaBrokerCapabilityRecord exact = Objects.requireNonNull(current, "current");
        if (!matchesImmutableFacts(exact)) {
            throw new IllegalArgumentException("stored capability does not match local immutable facts");
        }
        long heartbeat = Math.max(nowMillis, addExact(exact.heartbeatAtMillis(), 1));
        long expires = Math.max(
                addExact(heartbeat, expiry.toMillis()),
                addExact(exact.expiresAtMillis(), 1));
        return record(exact.startedAtMillis(), heartbeat, expires);
    }

    public boolean matchesImmutableFacts(KafkaBrokerCapabilityRecord value) {
        Objects.requireNonNull(value, "value");
        return kafkaClusterId.equals(value.kafkaClusterId())
                && identity.equals(value.identity())
                && runtimeInstanceId.equals(value.runtimeInstanceId())
                && kafkaVersion.equals(value.kafkaVersion())
                && nereusBuild.equals(value.nereusBuild())
                && javaVersion.equals(value.javaVersion())
                && supportedStorageProfiles.equals(value.supportedStorageProfiles())
                && Arrays.equals(configCompatibilitySha256, value.configCompatibilitySha256())
                && Arrays.equals(codeCapabilitySha256, value.codeCapabilitySha256())
                && Arrays.equals(providerScopeSha256, value.providerScopeSha256());
    }

    private KafkaBrokerCapabilityRecord record(long started, long heartbeat, long expires) {
        return new KafkaBrokerCapabilityRecord(
                KafkaBrokerCapabilityRecord.RECORD_VERSION,
                kafkaClusterId,
                identity.brokerId(),
                identity.brokerEpoch(),
                runtimeInstanceId,
                kafkaVersion,
                nereusBuild,
                javaVersion,
                KafkaStorageProtocolActivationRecord.PROTOCOL_VERSION,
                KafkaStorageProtocolActivationRecord.API_VERSION,
                KafkaStorageProtocolActivationRecord.STREAM_HEAD_SESSION_VERSION,
                KafkaStorageProtocolActivationRecord.BINDING_VERSION,
                KafkaPayloadMapping.KAFKA_RECORD_BATCH_V1.wireId(),
                KafkaStorageProtocolActivationRecord.OBJECT_WAL_ENTRY_INDEX_VERSION,
                KafkaStorageProtocolActivationRecord.NCP_VERSION,
                KafkaStorageProtocolActivationRecord.NTC_VERSION,
                KafkaStorageProtocolActivationRecord.CHECKPOINT_VERSION,
                KafkaStorageProtocolActivationRecord.COMPACTION_STRATEGY_VERSION,
                KafkaStorageProtocolActivationRecord.KAFKA_FEATURE_LEVEL,
                supportedStorageProfiles,
                configCompatibilitySha256,
                codeCapabilitySha256,
                providerScopeSha256,
                started,
                heartbeat,
                expires,
                0);
    }

    public String kafkaClusterId() { return kafkaClusterId; }
    public KafkaBrokerIdentity identity() { return identity; }
    public List<String> supportedStorageProfiles() { return supportedStorageProfiles; }
    public String defaultStorageProfile() { return defaultStorageProfile; }
    public byte[] providerScopeSha256() { return providerScopeSha256.clone(); }
    public Duration heartbeatInterval() { return heartbeatInterval; }

    private static List<String> canonicalProfiles(Set<StorageProfile> supplied) {
        Set<StorageProfile> exact = Set.copyOf(Objects.requireNonNull(supplied, "supportedStorageProfiles"));
        if (exact.isEmpty()) {
            throw new IllegalArgumentException("supportedStorageProfiles must be non-empty");
        }
        for (StorageProfile profile : exact) {
            if (profile.canonical() != profile) {
                throw new IllegalArgumentException("supportedStorageProfiles cannot contain aliases");
            }
        }
        return exact.stream().map(Enum::name).sorted(Comparator.naturalOrder()).toList();
    }

    private static String nonblank(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must be nonblank");
        return value;
    }

    private static byte[] sha256(byte[] supplied, String name) {
        byte[] exact = Objects.requireNonNull(supplied, name).clone();
        if (exact.length != 32) throw new IllegalArgumentException(name + " must contain 32 bytes");
        return exact;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative() || value.toMillis() <= 0) {
            throw new IllegalArgumentException(name + " must be positive and millisecond-representable");
        }
        return value;
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("capability timestamp overflow", failure);
        }
    }
}
