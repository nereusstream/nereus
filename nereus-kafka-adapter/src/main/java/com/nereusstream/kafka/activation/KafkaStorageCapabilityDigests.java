/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import com.nereusstream.metadata.oxia.records.KafkaBrokerCapabilityRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/** Domain-separated canonical capability digest shared by readiness and activation records. */
public final class KafkaStorageCapabilityDigests {
    private static final byte[] DOMAIN = "nereus-kafka-capability-v1".getBytes(StandardCharsets.US_ASCII);

    private KafkaStorageCapabilityDigests() { }

    /**
     * Hashes compatibility facts, not broker identity, build labels or lease times. Compatible rolling binaries may
     * therefore share one readiness proof while the broker-set digest independently fences registration epochs.
     */
    public static byte[] compatibilitySha256(KafkaBrokerCapabilityRecord value) {
        KafkaBrokerCapabilityRecord exact = Objects.requireNonNull(value, "value");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(DOMAIN.length);
            output.write(DOMAIN);
            output.writeInt(exact.protocolVersion());
            output.writeInt(exact.apiVersion());
            output.writeInt(exact.streamHeadSessionVersion());
            output.writeInt(exact.bindingVersion());
            output.writeInt(exact.payloadMappingId());
            output.writeInt(exact.objectWalEntryIndexVersion());
            output.writeInt(exact.ncpVersion());
            output.writeInt(exact.ntcVersion());
            output.writeInt(exact.checkpointVersion());
            output.writeInt(exact.compactionStrategyVersion());
            output.writeInt(exact.kafkaFeatureLevel());
            output.writeInt(exact.supportedStorageProfiles().size());
            for (String profile : exact.supportedStorageProfiles()) {
                writeText(output, profile);
            }
            output.write(exact.configCompatibilitySha256());
            output.write(exact.codeCapabilitySha256());
            output.flush();
            return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory Kafka capability encoding failed", failure);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }
}
