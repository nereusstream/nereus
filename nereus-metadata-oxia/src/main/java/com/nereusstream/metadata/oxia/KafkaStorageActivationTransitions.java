/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import java.util.Arrays;

/** Monotonic replacement guards for F9 activation control-plane records. */
final class KafkaStorageActivationTransitions {
    private KafkaStorageActivationTransitions() { }

    static void requireActivationReplacement(
            KafkaStorageProtocolActivationRecord current,
            KafkaStorageProtocolActivationRecord replacement) {
        if (!sameActivationFacts(current, replacement)) {
            throw invariant("Kafka storage activation changed immutable protocol facts");
        }
        if (current.lifecycle() == KafkaStorageActivationLifecycle.ACTIVE) {
            if (replacement.lifecycle() != KafkaStorageActivationLifecycle.ACTIVE
                    || current.activatedAtMillis() != replacement.activatedAtMillis()) {
                throw invariant("ACTIVE Kafka storage protocol cannot be changed in place");
            }
            return;
        }
        if (replacement.lifecycle() == KafkaStorageActivationLifecycle.PREPARED
                && replacement.activatedAtMillis() != 0) {
            throw invariant("PREPARED Kafka storage activation cannot carry an activation time");
        }
    }

    static void requireCapabilityHeartbeat(
            KafkaBrokerCapabilityRecord current,
            KafkaBrokerCapabilityRecord replacement) {
        if (!sameCapabilityFacts(current, replacement)) {
            throw invariant("Kafka broker heartbeat changed immutable capability facts");
        }
        if (replacement.heartbeatAtMillis() <= current.heartbeatAtMillis()
                || replacement.expiresAtMillis() <= current.expiresAtMillis()) {
            throw invariant("Kafka broker heartbeat must strictly extend time and expiry");
        }
    }

    static void requireReadinessReplacement(
            KafkaStorageReadinessRecord current,
            KafkaStorageReadinessRecord replacement) {
        if (!current.kafkaClusterId().equals(replacement.kafkaClusterId())) {
            throw invariant("Kafka readiness replacement changed cluster identity");
        }
        if (replacement.readinessEpoch() <= current.readinessEpoch()
                || replacement.kraftMetadataOffset() < current.kraftMetadataOffset()
                || replacement.createdAtMillis() <= current.createdAtMillis()) {
            throw invariant("Kafka readiness replacement did not advance its authority identity");
        }
    }

    private static boolean sameActivationFacts(
            KafkaStorageProtocolActivationRecord left,
            KafkaStorageProtocolActivationRecord right) {
        return left.recordVersion() == right.recordVersion()
                && left.kafkaClusterId().equals(right.kafkaClusterId())
                && left.protocolVersion() == right.protocolVersion()
                && left.apiVersion() == right.apiVersion()
                && left.streamHeadSessionVersion() == right.streamHeadSessionVersion()
                && left.bindingVersion() == right.bindingVersion()
                && left.payloadMappingId() == right.payloadMappingId()
                && left.objectWalEntryIndexVersion() == right.objectWalEntryIndexVersion()
                && left.ncpVersion() == right.ncpVersion()
                && left.ntcVersion() == right.ntcVersion()
                && left.checkpointVersion() == right.checkpointVersion()
                && left.compactionStrategyVersion() == right.compactionStrategyVersion()
                && left.allowedStorageProfiles().equals(right.allowedStorageProfiles())
                && left.defaultStorageProfile().equals(right.defaultStorageProfile())
                && Arrays.equals(
                        left.requiredCapabilitySha256(), right.requiredCapabilitySha256())
                && Arrays.equals(left.requiredBrokerSetSha256(), right.requiredBrokerSetSha256())
                && left.kafkaFeatureLevel() == right.kafkaFeatureLevel()
                && left.preparedAtMetadataOffset() == right.preparedAtMetadataOffset()
                && left.activationEpoch() == right.activationEpoch()
                && left.preparedAtMillis() == right.preparedAtMillis();
    }

    private static boolean sameCapabilityFacts(
            KafkaBrokerCapabilityRecord left,
            KafkaBrokerCapabilityRecord right) {
        return left.recordVersion() == right.recordVersion()
                && left.kafkaClusterId().equals(right.kafkaClusterId())
                && left.identity().equals(right.identity())
                && left.runtimeInstanceId().equals(right.runtimeInstanceId())
                && left.kafkaVersion().equals(right.kafkaVersion())
                && left.nereusBuild().equals(right.nereusBuild())
                && left.javaVersion().equals(right.javaVersion())
                && left.protocolVersion() == right.protocolVersion()
                && left.apiVersion() == right.apiVersion()
                && left.streamHeadSessionVersion() == right.streamHeadSessionVersion()
                && left.bindingVersion() == right.bindingVersion()
                && left.payloadMappingId() == right.payloadMappingId()
                && left.objectWalEntryIndexVersion() == right.objectWalEntryIndexVersion()
                && left.ncpVersion() == right.ncpVersion()
                && left.ntcVersion() == right.ntcVersion()
                && left.checkpointVersion() == right.checkpointVersion()
                && left.compactionStrategyVersion() == right.compactionStrategyVersion()
                && left.kafkaFeatureLevel() == right.kafkaFeatureLevel()
                && left.supportedStorageProfiles().equals(right.supportedStorageProfiles())
                && Arrays.equals(
                        left.configCompatibilitySha256(), right.configCompatibilitySha256())
                && Arrays.equals(left.codeCapabilitySha256(), right.codeCapabilitySha256())
                && Arrays.equals(left.providerScopeSha256(), right.providerScopeSha256())
                && left.startedAtMillis() == right.startedAtMillis();
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
