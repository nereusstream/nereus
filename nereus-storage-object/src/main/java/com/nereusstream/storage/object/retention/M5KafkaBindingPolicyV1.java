/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Native Kafka policy state carried by its existing bootstrap/selector authority, never a separate root. */
public record M5KafkaBindingPolicyV1(
        PhysicalNamespaceAuthorityBindingV2 physicalBinding,
        long generation,
        long configurationMetadataOffset,
        short kafkaFeatureLevel,
        CanonicalBytes nativeLogConfiguration,
        Sha256Digest providerConfigurationSha256,
        CanonicalBytes admittedAuditPolicy,
        long providerMinimumMillis,
        long auditMinimumMillis,
        long graceDurationMillis,
        List<AuditHold> auditHolds) {
    public static final String RETENTION_POLICY = "NATIVE_KAFKA_LOG_RETENTION_V1";
    public static final String GC_PROFILE = "M5_SEALED_TARGET_CONFIRMED_DRAIN_V1";
    public static final int MAX_INPUT_BYTES = 65_536;
    public static final int MAX_HOLDS = 64;
    public static final int MAX_BYTES = 262_144;
    private static final int MAGIC = 0x4d354b50; // M5KP

    /** Business holds do not expire from a local timer; the authority must explicitly release them. */
    public record AuditHold(
            Sha256Digest obligationId,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            long inclusiveStart,
            long exclusiveEnd,
            long admissionGeneration) {
        public AuditHold {
            M5RetentionRecordsV1.requireDigest(obligationId, "obligationId");
            Objects.requireNonNull(resource, "resource");
            if (inclusiveStart < 0 || exclusiveEnd <= inclusiveStart || admissionGeneration <= 0) {
                throw new IllegalArgumentException("Kafka audit hold has invalid coverage or admission generation");
            }
        }
    }

    public M5KafkaBindingPolicyV1 {
        Objects.requireNonNull(physicalBinding, "physicalBinding");
        M5RetentionRecordsV1.requireDigest(providerConfigurationSha256, "providerConfigurationSha256");
        requireInput(nativeLogConfiguration);
        requireInput(admittedAuditPolicy);
        auditHolds = List.copyOf(Objects.requireNonNull(auditHolds, "auditHolds"));
        if (generation <= 0
                || configurationMetadataOffset < 0
                || kafkaFeatureLevel < 0
                || physicalBinding.physicalNamespace().providerKind() != PhysicalResourceIdV2.ProviderKind.BOOKKEEPER
                || providerMinimumMillis < 0
                || auditMinimumMillis < 0
                || graceDurationMillis <= 0
                || graceDurationMillis > Long.MAX_VALUE / 1_000_000
                || graceDurationMillis < providerMinimumMillis
                || graceDurationMillis < auditMinimumMillis) {
            throw new IllegalArgumentException(
                    "Kafka policy requires actual identity/configuration and admitted positive grace");
        }
        if (auditHolds.size() > MAX_HOLDS
                || !auditHolds.equals(auditHolds.stream()
                        .sorted(Comparator.comparing(
                                value -> value.obligationId().toHex()))
                        .toList())
                || auditHolds.stream().map(AuditHold::obligationId).distinct().count() != auditHolds.size()
                || auditHolds.stream()
                        .anyMatch(value -> !value.resource().namespace().equals(physicalBinding.physicalNamespace())
                                || value.admissionGeneration() > generation)) {
            throw new IllegalArgumentException(
                    "Kafka audit holds are not a bounded exact-namespace admission inventory");
        }
    }

    public M5KafkaBindingPolicyV1 withConfiguration(long metadataOffset, short featureLevel, CanonicalBytes bytes) {
        if (metadataOffset < configurationMetadataOffset) {
            throw new IllegalArgumentException("Kafka policy configuration provenance regresses");
        }
        return new M5KafkaBindingPolicyV1(
                physicalBinding,
                Math.addExact(generation, 1),
                metadataOffset,
                featureLevel,
                bytes,
                providerConfigurationSha256,
                admittedAuditPolicy,
                providerMinimumMillis,
                auditMinimumMillis,
                graceDurationMillis,
                auditHolds);
    }

    public boolean sameAdmittedProfile(M5KafkaBindingPolicyV1 other) {
        return physicalBinding.equals(other.physicalBinding)
                && providerConfigurationSha256.equals(other.providerConfigurationSha256)
                && admittedAuditPolicy.equals(other.admittedAuditPolicy)
                && providerMinimumMillis == other.providerMinimumMillis
                && auditMinimumMillis == other.auditMinimumMillis
                && graceDurationMillis == other.graceDurationMillis;
    }

    public M5KafkaBindingPolicyV1 withAuditHolds(List<AuditHold> holds) {
        return new M5KafkaBindingPolicyV1(
                physicalBinding,
                Math.addExact(generation, 1),
                configurationMetadataOffset,
                kafkaFeatureLevel,
                nativeLogConfiguration,
                providerConfigurationSha256,
                admittedAuditPolicy,
                providerMinimumMillis,
                auditMinimumMillis,
                graceDurationMillis,
                holds);
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeInt(1);
                write(out, physicalBinding.encode());
                out.writeLong(generation);
                out.writeLong(configurationMetadataOffset);
                out.writeShort(kafkaFeatureLevel);
                write(out, nativeLogConfiguration);
                out.write(providerConfigurationSha256.bytes().toByteArray());
                write(out, admittedAuditPolicy);
                out.writeLong(providerMinimumMillis);
                out.writeLong(auditMinimumMillis);
                out.writeLong(graceDurationMillis);
                out.writeInt(auditHolds.size());
                for (var hold : auditHolds) {
                    out.write(hold.obligationId().bytes().toByteArray());
                    write(out, hold.resource().canonicalBytes());
                    out.writeLong(hold.inclusiveStart());
                    out.writeLong(hold.exclusiveEnd());
                    out.writeLong(hold.admissionGeneration());
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("Kafka policy exceeds its canonical byte bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static M5KafkaBindingPolicyV1 decode(CanonicalBytes bytes) {
        if (bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("Kafka policy exceeds its canonical byte bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IllegalArgumentException("Kafka policy preamble differs");
            }
            var physical =
                    PhysicalNamespaceAuthorityBindingV2.decode(read(in, PhysicalNamespaceAuthorityBindingV2.MAX_BYTES));
            long generation = in.readLong();
            long offset = in.readLong();
            short feature = in.readShort();
            var configuration = read(in, MAX_INPUT_BYTES);
            var provider = digest(in);
            var audit = read(in, MAX_INPUT_BYTES);
            long providerMinimum = in.readLong();
            long auditMinimum = in.readLong();
            long duration = in.readLong();
            int count = in.readInt();
            if (count < 0 || count > MAX_HOLDS) {
                throw new IllegalArgumentException("Kafka audit inventory exceeds its bound");
            }
            var holds = new ArrayList<AuditHold>();
            for (int i = 0; i < count; i++) {
                var obligation = digest(in);
                var resource =
                        PhysicalResourceIdCodecV2.decode(read(in, PhysicalResourceIdV2.MAX_COMPONENT_BYTES * 2 + 64));
                if (!(resource instanceof PhysicalResourceIdV2.BookKeeperLedger ledger)) {
                    throw new IllegalArgumentException("Kafka audit hold is not a BK resource");
                }
                holds.add(new AuditHold(obligation, ledger, in.readLong(), in.readLong(), in.readLong()));
            }
            var value = new M5KafkaBindingPolicyV1(
                    physical,
                    generation,
                    offset,
                    feature,
                    configuration,
                    provider,
                    audit,
                    providerMinimum,
                    auditMinimum,
                    duration,
                    holds);
            if (in.read() != -1 || !value.encode().equals(bytes)) {
                throw new IllegalArgumentException("Kafka policy bytes are not canonical");
            }
            return value;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Kafka policy is truncated", failure);
        }
    }

    private static void requireInput(CanonicalBytes value) {
        Objects.requireNonNull(value, "policy input");
        if (value.length() <= 0 || value.length() > MAX_INPUT_BYTES) {
            throw new IllegalArgumentException("Kafka policy input exceeds its byte bound");
        }
    }

    private static void write(DataOutputStream out, CanonicalBytes bytes) throws IOException {
        out.writeInt(bytes.length());
        out.write(bytes.toByteArray());
    }

    private static CanonicalBytes read(DataInputStream in, int maximum) throws IOException {
        int count = in.readInt();
        if (count <= 0 || count > maximum) {
            throw new IllegalArgumentException("Kafka policy component exceeds its byte bound");
        }
        var value = new byte[count];
        in.readFully(value);
        return CanonicalBytes.copyOf(value);
    }

    private static Sha256Digest digest(DataInputStream in) throws IOException {
        var value = new byte[Sha256Digest.LENGTH];
        in.readFully(value);
        return Sha256Digest.copyOf(value);
    }
}
