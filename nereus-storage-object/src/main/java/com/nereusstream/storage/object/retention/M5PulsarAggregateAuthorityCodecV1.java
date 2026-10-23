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

import com.nereusstream.domain.aggregate.TopicBindingAggregateV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateAuthorityStateV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateRetirementAuthorityV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityRecordsV1.PulsarAggregateScanFenceV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.FloorClassV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Strict canonical wire and NTA1 projection for one Pulsar aggregate authority cell. */
public final class M5PulsarAggregateAuthorityCodecV1 {
    private static final int MAGIC = 0x4d355041; // M5PA
    private static final int VERSION = 1;
    private static final Sha256Digest PLACEHOLDER = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);

    private M5PulsarAggregateAuthorityCodecV1() {}

    public static boolean isAuthorityValue(CanonicalBytes value) {
        byte[] bytes = value.toByteArray();
        if (bytes.length < Integer.BYTES) {
            return false;
        }
        int magic =
                ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
        return magic == MAGIC;
    }

    /** Wrap one exact legacy NTA1 aggregate without changing its reader projection bytes. */
    public static PulsarAggregateRetirementAuthorityV1 migrateLegacy(
            CanonicalBytes exactLegacyAggregate, CapabilityBinding capability) {
        return migrateLegacy(exactLegacyAggregate, capability, Optional.empty());
    }

    /** Preserve the actual immutable NTA1 predecessor token while its C envelope advances independently. */
    public static PulsarAggregateRetirementAuthorityV1 migrateLegacy(
            ExactMetadataTransactionStoreV1.VersionedValue exactLegacyAggregate, CapabilityBinding capability) {
        return migrateLegacy(
                exactLegacyAggregate.canonicalStoredBytes(),
                capability,
                Optional.of(exactLegacyAggregate.metadataVersion()));
    }

    private static PulsarAggregateRetirementAuthorityV1 migrateLegacy(
            CanonicalBytes exactLegacyAggregate,
            CapabilityBinding capability,
            Optional<MetadataVersion> originalVersion) {
        TopicBindingAggregateV1 aggregate = Nta1CodecV1.decode(exactLegacyAggregate);
        if (!(aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException(
                    "Pulsar authority migration requires one canonical Pulsar NTA1 aggregate");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                incarnation,
                aggregate.binding().bindingId(),
                1,
                Optional.of(Sha256Digest.hash(exactLegacyAggregate)),
                PulsarAggregateAuthorityStateV1.OPEN_V1,
                exactLegacyAggregate,
                Sha256Digest.hash(exactLegacyAggregate),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                capability,
                PLACEHOLDER,
                originalVersion));
    }

    /** A new P1 key starts closed; no existing aggregate can acquire this policy by migration. */
    public static PulsarAggregateRetirementAuthorityV1 initialNativeOnly(
            CanonicalBytes aggregateBytes, ReferenceWriterEnrollmentV1 enrollment) {
        return initialNativeOnly(
                aggregateBytes,
                enrollment,
                M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_V1);
    }

    public static PulsarAggregateRetirementAuthorityV1 initialNativeOnly(
            CanonicalBytes aggregateBytes,
            ReferenceWriterEnrollmentV1 enrollment,
            M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1 policy) {
        return initialNativeOnly(aggregateBytes, enrollment, policy, Optional.empty());
    }

    public static PulsarAggregateRetirementAuthorityV1 initialNativeOnly(
            CanonicalBytes aggregateBytes,
            ReferenceWriterEnrollmentV1 enrollment,
            M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1 policy,
            Optional<M5PulsarAuditDomainV1> initialAudits) {
        java.util.Objects.requireNonNull(policy, "policy");
        if (initialAudits.filter(domain -> !domain.holds().isEmpty()).isPresent()) {
            throw new IllegalArgumentException("audit domain must be initialized before its first physical reference");
        }
        var aggregate = Nta1CodecV1.decode(aggregateBytes);
        if (!(aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity incarnation)) {
            throw new IllegalArgumentException("Native-only policy requires a Pulsar aggregate");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                incarnation,
                aggregate.binding().bindingId(),
                1,
                Optional.empty(),
                PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5,
                aggregateBytes,
                Sha256Digest.hash(aggregateBytes),
                Optional.empty(),
                List.of(),
                Optional.of(enrollment),
                enrollment.capability(),
                PLACEHOLDER,
                Optional.empty(),
                Optional.of(new M5LogicalTrimRecordsV2.LogicalTrimStateV2(Optional.empty(), Optional.empty())),
                Optional.empty(),
                Optional.of(policy),
                Optional.empty(),
                initialAudits));
    }

    /** Bind the actual first-create version before opening any P1/native admission. */
    public static PulsarAggregateRetirementAuthorityV1 activateNativeOnly(
            ExactMetadataTransactionStoreV1.VersionedValue initialized) {
        var current = decodeAuthority(initialized.canonicalStoredBytes());
        if (current.state() != PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5) {
            throw new IllegalArgumentException("Native-only activation requires its exact initializing predecessor");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                current.incarnation(),
                current.bindingId(),
                2,
                Optional.of(Sha256Digest.hash(initialized.canonicalStoredBytes())),
                PulsarAggregateAuthorityStateV1.OPEN_V1,
                current.canonicalAggregateBytes(),
                current.originalAggregateSha256(),
                Optional.empty(),
                List.of(),
                current.writerEnrollment(),
                current.capability(),
                PLACEHOLDER,
                Optional.of(initialized.metadataVersion()),
                current.logicalTrim(),
                Optional.empty(),
                current.retentionPolicy(),
                current.firstNativeFactoryScope(),
                current.auditDomain(),
                current.nativeRetention()));
    }

    static PulsarAggregateRetirementAuthorityV1 firstNativeFactorySuccessor(
            PulsarAggregateRetirementAuthorityV1 current, Sha256Digest scope) {
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.retentionPolicy().isEmpty()
                || current.firstNativeFactoryScope().isPresent()
                || !current.referenceMutationTickets().isEmpty()) {
            throw new IllegalArgumentException("first Native factory claim requires its idle new policy");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                current.incarnation(),
                current.bindingId(),
                Math.addExact(current.authorityGeneration(), 1),
                Optional.of(Sha256Digest.hash(encodeAuthority(current))),
                current.state(),
                current.canonicalAggregateBytes(),
                current.originalAggregateSha256(),
                current.scanFence(),
                current.referenceMutationTickets(),
                current.writerEnrollment(),
                current.capability(),
                PLACEHOLDER,
                current.originalAggregateVersion(),
                current.logicalTrim(),
                current.logicalTrimSourceScan(),
                current.retentionPolicy(),
                Optional.of(scope),
                current.auditDomain(),
                current.nativeRetention()));
    }

    public static Sha256Digest nativeOnlyPolicyRoot(PulsarAggregateRetirementAuthorityV1 value) {
        if (value.retentionPolicy().isEmpty()) {
            throw new IllegalArgumentException("Pulsar aggregate has no explicit Native-only policy");
        }
        return Sha256Digest.hash(encode(output -> {
            output.writeUTF(
                    value.retentionPolicy().orElseThrow()
                                    == M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1.NATIVE_PULSAR_ONLY_V1
                            ? "NEREUS_NATIVE_PULSAR_ONLY_V1"
                            : "NEREUS_NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2");
            writeDigest(output, value.originalAggregateSha256());
            writeCapability(output, value.capability());
            writeDigest(output, value.writerEnrollment().orElseThrow().implementationRootSha256());
        }));
    }

    public static PulsarAggregateRetirementAuthorityV1 successor(
            PulsarAggregateRetirementAuthorityV1 current,
            PulsarAggregateAuthorityStateV1 state,
            Optional<PulsarAggregateScanFenceV1> fence,
            List<ReferenceMutationTicketV1> tickets) {
        return successor(current, state, fence, tickets, current.writerEnrollment());
    }

    public static PulsarAggregateRetirementAuthorityV1 successor(
            PulsarAggregateRetirementAuthorityV1 current,
            PulsarAggregateAuthorityStateV1 state,
            Optional<PulsarAggregateScanFenceV1> fence,
            List<ReferenceMutationTicketV1> tickets,
            Optional<ReferenceWriterEnrollmentV1> enrollment) {
        CanonicalBytes predecessor = encodeAuthority(current);
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                current.incarnation(),
                current.bindingId(),
                Math.addExact(current.authorityGeneration(), 1),
                Optional.of(Sha256Digest.hash(predecessor)),
                state,
                current.canonicalAggregateBytes(),
                current.originalAggregateSha256(),
                fence,
                tickets,
                enrollment,
                current.capability(),
                PLACEHOLDER,
                current.originalAggregateVersion(),
                current.logicalTrim(),
                current.logicalTrimSourceScan(),
                current.retentionPolicy(),
                current.firstNativeFactoryScope(),
                current.auditDomain(),
                current.nativeRetention()));
    }

    static PulsarAggregateRetirementAuthorityV1 logicalTrimSuccessor(
            PulsarAggregateRetirementAuthorityV1 current, M5LogicalTrimRecordsV2.LogicalTrimStateV2 trim) {
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                && current.state() != PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2) {
            throw new IllegalArgumentException("retirement scan fence cannot become a logical trim fence");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                current.incarnation(),
                current.bindingId(),
                Math.addExact(current.authorityGeneration(), 1),
                Optional.of(Sha256Digest.hash(encodeAuthority(current))),
                trim.fence().isPresent()
                        ? PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2
                        : PulsarAggregateAuthorityStateV1.OPEN_V1,
                current.canonicalAggregateBytes(),
                current.originalAggregateSha256(),
                current.scanFence(),
                current.referenceMutationTickets(),
                current.writerEnrollment(),
                current.capability(),
                PLACEHOLDER,
                current.originalAggregateVersion(),
                Optional.of(trim),
                Optional.empty(),
                current.retentionPolicy(),
                current.firstNativeFactoryScope(),
                current.auditDomain(),
                current.nativeRetention()));
    }

    static PulsarAggregateRetirementAuthorityV1 sourceScanSuccessor(
            PulsarAggregateRetirementAuthorityV1 current,
            Optional<M5PulsarAggregateAuthorityRecordsV1.PulsarLogicalTrimSourceScanV3> scan,
            M5LogicalTrimRecordsV2.LogicalTrimStateV2 trim) {
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                && current.state() != PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SOURCE_SCAN_FENCED_V3) {
            throw new IllegalArgumentException("another scan owns the Pulsar authority");
        }
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                current.incarnation(),
                current.bindingId(),
                Math.addExact(current.authorityGeneration(), 1),
                Optional.of(Sha256Digest.hash(encodeAuthority(current))),
                scan.isPresent()
                        ? PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SOURCE_SCAN_FENCED_V3
                        : trim.fence().isPresent()
                                ? PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2
                                : PulsarAggregateAuthorityStateV1.OPEN_V1,
                current.canonicalAggregateBytes(),
                current.originalAggregateSha256(),
                current.scanFence(),
                current.referenceMutationTickets(),
                current.writerEnrollment(),
                current.capability(),
                PLACEHOLDER,
                current.originalAggregateVersion(),
                Optional.of(trim),
                scan,
                current.retentionPolicy(),
                current.firstNativeFactoryScope(),
                current.auditDomain(),
                current.nativeRetention()));
    }

    public static CanonicalBytes encodeAuthority(PulsarAggregateRetirementAuthorityV1 value) {
        PulsarAggregateRetirementAuthorityV1 placeholder = copyWithCanonicalSha(value, PLACEHOLDER);
        Sha256Digest expected = Sha256Digest.hash(encodeUnchecked(placeholder));
        if (!value.authorityCanonicalSha256().equals(expected)) {
            throw new IllegalArgumentException("Pulsar aggregate authority canonical SHA-256 differs");
        }
        CanonicalBytes encoded = encodeUnchecked(value);
        if (encoded.length() > M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("Pulsar aggregate authority exceeds the metadata value hard cap");
        }
        return encoded;
    }

    public static PulsarAggregateRetirementAuthorityV1 decodeAuthority(CanonicalBytes encoded) {
        if (encoded.length() > M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES) {
            throw new IllegalArgumentException("Pulsar aggregate authority exceeds the metadata value hard cap");
        }
        PulsarAggregateRetirementAuthorityV1 value = decode(encoded, input -> {
            if (input.readInt() != MAGIC) {
                throw new IllegalArgumentException("Pulsar aggregate authority preamble differs");
            }
            int version = input.readInt();
            if (version != VERSION
                    && version != 2
                    && version != 3
                    && version != 4
                    && version != 5
                    && version != 6
                    && version != 7) {
                throw new IllegalArgumentException("Pulsar aggregate authority version differs");
            }
            Optional<MetadataVersion> originalVersion = version == 2 || version >= 3 && input.readBoolean()
                    ? Optional.of(new MetadataVersion(readBytes(input, 4096)))
                    : Optional.empty();
            long generation = input.readLong();
            Optional<Sha256Digest> predecessor =
                    input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty();
            CapabilityBinding capability = readCapability(input);
            CanonicalBytes aggregateBytes = readBytes(input, M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES);
            Sha256Digest originalAggregateSha256 = readDigest(input);
            Optional<PulsarAggregateScanFenceV1> fence = Optional.empty();
            if (input.readBoolean()) {
                fence = Optional.of(new PulsarAggregateScanFenceV1(
                        readDigest(input), readDigest(input), readDigest(input), readAuthorityFact(input)));
            }
            int ticketCount = input.readInt();
            if (ticketCount < 0 || ticketCount > M5PulsarAggregateAuthorityRecordsV1.MAX_REFERENCE_MUTATION_TICKETS) {
                throw new IllegalArgumentException("Pulsar authority ticket count exceeds its hard cap");
            }
            List<ReferenceMutationTicketV1> tickets = new ArrayList<>(ticketCount);
            for (int index = 0; index < ticketCount; index++) {
                tickets.add(new ReferenceMutationTicketV1(
                        enumValue(ReferenceTargetKindV1.values(), input.readUnsignedByte(), "ticket target kind"),
                        readDigest(input),
                        enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "reference kind"),
                        readCapability(input),
                        readDigest(input),
                        readDigest(input)));
            }
            Optional<ReferenceWriterEnrollmentV1> enrollment = Optional.empty();
            if (input.readBoolean()) {
                CapabilityBinding enrollmentCapability = readCapability(input);
                int floorCount = boundedCount(input.readInt(), FloorClassV1.values().length, "floor classes");
                List<FloorClassV1> floors = new ArrayList<>(floorCount);
                for (int index = 0; index < floorCount; index++) {
                    floors.add(enumValue(FloorClassV1.values(), input.readUnsignedByte(), "floor class"));
                }
                int referenceCount =
                        boundedCount(input.readInt(), ReferenceKindV1.values().length, "reference classes");
                List<ReferenceKindV1> references = new ArrayList<>(referenceCount);
                for (int index = 0; index < referenceCount; index++) {
                    references.add(enumValue(ReferenceKindV1.values(), input.readUnsignedByte(), "reference class"));
                }
                enrollment = Optional.of(
                        new ReferenceWriterEnrollmentV1(enrollmentCapability, floors, references, readDigest(input)));
            }
            PulsarAggregateAuthorityStateV1 state = enumValue(
                    PulsarAggregateAuthorityStateV1.values(), input.readUnsignedByte(), "Pulsar authority state");
            Optional<M5LogicalTrimRecordsV2.LogicalTrimStateV2> trim = Optional.empty();
            if (version >= 3) {
                Optional<M5RetentionRecordsV1.BindingTrimFrontierV1> frontier = input.readBoolean()
                        ? Optional.of(M5RetentionCodecV1.decodeTrimFrontier(
                                readBytes(input, M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES)))
                        : Optional.empty();
                Optional<M5LogicalTrimRecordsV2.LogicalTrimFenceV2> trimFence = input.readBoolean()
                        ? Optional.of(new M5LogicalTrimRecordsV2.LogicalTrimFenceV2(
                                readDigest(input), readDigest(input), readAuthorityFact(input)))
                        : Optional.empty();
                trim = Optional.of(new M5LogicalTrimRecordsV2.LogicalTrimStateV2(frontier, trimFence));
            }
            Optional<M5PulsarAggregateAuthorityRecordsV1.PulsarLogicalTrimSourceScanV3> sourceScan =
                    version == 4 || version >= 5 && input.readBoolean()
                            ? Optional.of(new M5PulsarAggregateAuthorityRecordsV1.PulsarLogicalTrimSourceScanV3(
                                    readDigest(input), readAuthorityFact(input)))
                            : Optional.empty();
            Optional<Sha256Digest> firstFactory =
                    version >= 5 && input.readBoolean() ? Optional.of(readDigest(input)) : Optional.empty();
            Optional<M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1> policy = version >= 5
                    ? Optional.of(enumValue(
                            M5PulsarAggregateAuthorityRecordsV1.PulsarRetentionPolicyV1.values(),
                            input.readUnsignedByte(),
                            "Pulsar retention policy"))
                    : Optional.empty();
            Optional<M5PulsarAuditDomainV1> audits = version == 6 || version == 7 && input.readBoolean()
                    ? Optional.of(M5PulsarAuditDomainV1.decode(readBytes(input, M5PulsarAuditDomainV1.MAX_BYTES)))
                    : Optional.empty();
            Optional<M5PulsarRetentionConfigurationV1> nativeRetention = version == 7
                    ? Optional.of(M5PulsarRetentionConfigurationV1.decode(
                            readBytes(input, M5PulsarRetentionConfigurationV1.MAX_BYTES)))
                    : Optional.empty();
            Sha256Digest canonicalSha = readDigest(input);
            TopicBindingAggregateV1 aggregate = Nta1CodecV1.decode(aggregateBytes);
            if (!(aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity incarnation)) {
                throw new IllegalArgumentException("Pulsar authority contains a non-Pulsar aggregate");
            }
            return new PulsarAggregateRetirementAuthorityV1(
                    incarnation,
                    aggregate.binding().bindingId(),
                    generation,
                    predecessor,
                    state,
                    aggregateBytes,
                    originalAggregateSha256,
                    fence,
                    tickets,
                    enrollment,
                    capability,
                    canonicalSha,
                    originalVersion,
                    trim,
                    sourceScan,
                    policy,
                    firstFactory,
                    audits,
                    nativeRetention);
        });
        if (!encoded.equals(encodeAuthority(value))) {
            throw new IllegalArgumentException("Pulsar aggregate authority is not canonical");
        }
        return value;
    }

    /** Project an authority envelope back to the exact canonical NTA1 bytes expected by M1 readers. */
    public static CanonicalBytes projectAggregate(CanonicalBytes stored) {
        if (isAuthorityValue(stored)) {
            var authority = decodeAuthority(stored);
            if (authority.state() == PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5) {
                throw new IllegalArgumentException("Native-only P1 admission is not activated");
            }
            return authority.canonicalAggregateBytes();
        }
        Nta1CodecV1.decode(stored);
        return stored;
    }

    static PulsarAggregateRetirementAuthorityV1 retentionConfigurationSuccessor(
            PulsarAggregateRetirementAuthorityV1 current, M5PulsarRetentionConfigurationV1 configured) {
        var next = successor(current, current.state(), current.scanFence(), current.referenceMutationTickets());
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                next.incarnation(),
                next.bindingId(),
                next.authorityGeneration(),
                next.predecessorValueSha256(),
                next.state(),
                next.canonicalAggregateBytes(),
                next.originalAggregateSha256(),
                next.scanFence(),
                next.referenceMutationTickets(),
                next.writerEnrollment(),
                next.capability(),
                next.authorityCanonicalSha256(),
                next.originalAggregateVersion(),
                next.logicalTrim(),
                next.logicalTrimSourceScan(),
                next.retentionPolicy(),
                next.firstNativeFactoryScope(),
                next.auditDomain(),
                Optional.of(configured)));
    }

    static PulsarAggregateRetirementAuthorityV1 auditSuccessor(
            PulsarAggregateRetirementAuthorityV1 current, M5PulsarAuditDomainV1 audits) {
        if (current.state() != PulsarAggregateAuthorityStateV1.OPEN_V1
                || current.auditDomain().isEmpty()) {
            throw new IllegalArgumentException("audit mutation requires its existing OPEN domain");
        }
        var next = successor(current, current.state(), current.scanFence(), current.referenceMutationTickets());
        return finalizeAuthority(new PulsarAggregateRetirementAuthorityV1(
                next.incarnation(),
                next.bindingId(),
                next.authorityGeneration(),
                next.predecessorValueSha256(),
                next.state(),
                next.canonicalAggregateBytes(),
                next.originalAggregateSha256(),
                next.scanFence(),
                next.referenceMutationTickets(),
                next.writerEnrollment(),
                next.capability(),
                PLACEHOLDER,
                next.originalAggregateVersion(),
                next.logicalTrim(),
                next.logicalTrimSourceScan(),
                next.retentionPolicy(),
                next.firstNativeFactoryScope(),
                Optional.of(audits),
                next.nativeRetention()));
    }

    private static PulsarAggregateRetirementAuthorityV1 finalizeAuthority(PulsarAggregateRetirementAuthorityV1 draft) {
        PulsarAggregateRetirementAuthorityV1 placeholder = copyWithCanonicalSha(draft, PLACEHOLDER);
        return copyWithCanonicalSha(draft, Sha256Digest.hash(encodeUnchecked(placeholder)));
    }

    private static PulsarAggregateRetirementAuthorityV1 copyWithCanonicalSha(
            PulsarAggregateRetirementAuthorityV1 value, Sha256Digest canonicalSha) {
        return new PulsarAggregateRetirementAuthorityV1(
                value.incarnation(),
                value.bindingId(),
                value.authorityGeneration(),
                value.predecessorValueSha256(),
                value.state(),
                value.canonicalAggregateBytes(),
                value.originalAggregateSha256(),
                value.scanFence(),
                value.referenceMutationTickets(),
                value.writerEnrollment(),
                value.capability(),
                canonicalSha,
                value.originalAggregateVersion(),
                value.logicalTrim(),
                value.logicalTrimSourceScan(),
                value.retentionPolicy(),
                value.firstNativeFactoryScope(),
                value.auditDomain(),
                value.nativeRetention());
    }

    private static CanonicalBytes encodeUnchecked(PulsarAggregateRetirementAuthorityV1 value) {
        return encode(output -> {
            output.writeInt(MAGIC);
            output.writeInt(
                    value.nativeRetention().isPresent()
                            ? 7
                            : value.auditDomain().isPresent()
                                    ? 6
                                    : value.retentionPolicy().isPresent()
                                            ? 5
                                            : value.logicalTrimSourceScan().isPresent()
                                                    ? 4
                                                    : value.logicalTrim().isPresent()
                                                            ? 3
                                                            : value.originalAggregateVersion()
                                                                            .isPresent()
                                                                    ? 2
                                                                    : VERSION);
            if (value.logicalTrim().isPresent()) {
                output.writeBoolean(value.originalAggregateVersion().isPresent());
            }
            if (value.originalAggregateVersion().isPresent()) {
                writeBytes(
                        output, value.originalAggregateVersion().orElseThrow().value(), 4096);
            }
            output.writeLong(value.authorityGeneration());
            output.writeBoolean(value.predecessorValueSha256().isPresent());
            if (value.predecessorValueSha256().isPresent()) {
                writeDigest(output, value.predecessorValueSha256().orElseThrow());
            }
            writeCapability(output, value.capability());
            writeBytes(
                    output, value.canonicalAggregateBytes(), M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES);
            writeDigest(output, value.originalAggregateSha256());
            output.writeBoolean(value.scanFence().isPresent());
            if (value.scanFence().isPresent()) {
                PulsarAggregateScanFenceV1 fence = value.scanFence().orElseThrow();
                writeDigest(output, fence.targetIdentitySha256());
                writeDigest(output, fence.attemptIdSha256());
                writeDigest(output, fence.openAuthorityValueSha256());
                writeAuthorityFact(output, fence.deletedSelectorAuthority());
            }
            output.writeInt(value.referenceMutationTickets().size());
            for (ReferenceMutationTicketV1 ticket : value.referenceMutationTickets()) {
                output.writeByte(ticket.targetKind().ordinal());
                writeDigest(output, ticket.targetIdentitySha256());
                output.writeByte(ticket.referenceKind().ordinal());
                writeCapability(output, ticket.writerCapability());
                writeDigest(output, ticket.operationIdSha256());
                writeDigest(output, ticket.externalAuthorityRootSha256());
            }
            output.writeBoolean(value.writerEnrollment().isPresent());
            if (value.writerEnrollment().isPresent()) {
                ReferenceWriterEnrollmentV1 enrollment =
                        value.writerEnrollment().orElseThrow();
                writeCapability(output, enrollment.capability());
                output.writeInt(enrollment.floorClasses().size());
                for (FloorClassV1 floor : enrollment.floorClasses()) {
                    output.writeByte(floor.ordinal());
                }
                output.writeInt(enrollment.referenceKinds().size());
                for (ReferenceKindV1 reference : enrollment.referenceKinds()) {
                    output.writeByte(reference.ordinal());
                }
                writeDigest(output, enrollment.implementationRootSha256());
            }
            output.writeByte(value.state().ordinal());
            if (value.logicalTrim().isPresent()) {
                var trim = value.logicalTrim().orElseThrow();
                output.writeBoolean(trim.frontier().isPresent());
                if (trim.frontier().isPresent()) {
                    writeBytes(
                            output,
                            M5RetentionCodecV1.encodeTrimFrontier(
                                    trim.frontier().orElseThrow()),
                            M5PulsarAggregateAuthorityRecordsV1.MAX_AUTHORITY_BYTES);
                }
                output.writeBoolean(trim.fence().isPresent());
                if (trim.fence().isPresent()) {
                    var trimFence = trim.fence().orElseThrow();
                    writeDigest(output, trimFence.attemptIdSha256());
                    writeDigest(output, trimFence.floorSnapshotRootSha256());
                    writeAuthorityFact(output, trimFence.openAuthority());
                }
            }
            if (value.retentionPolicy().isPresent()) {
                output.writeBoolean(value.logicalTrimSourceScan().isPresent());
            }
            if (value.logicalTrimSourceScan().isPresent()) {
                var scan = value.logicalTrimSourceScan().orElseThrow();
                writeDigest(output, scan.attemptIdSha256());
                writeAuthorityFact(output, scan.openAuthority());
            }
            if (value.retentionPolicy().isPresent()) {
                output.writeBoolean(value.firstNativeFactoryScope().isPresent());
                if (value.firstNativeFactoryScope().isPresent()) {
                    writeDigest(output, value.firstNativeFactoryScope().orElseThrow());
                }
                output.writeByte(value.retentionPolicy().orElseThrow().ordinal());
            }
            if (value.nativeRetention().isPresent()) {
                output.writeBoolean(value.auditDomain().isPresent());
            }
            if (value.auditDomain().isPresent()) {
                writeBytes(output, value.auditDomain().orElseThrow().encode(), M5PulsarAuditDomainV1.MAX_BYTES);
            }
            if (value.nativeRetention().isPresent()) {
                writeBytes(
                        output,
                        value.nativeRetention().orElseThrow().encode(),
                        M5PulsarRetentionConfigurationV1.MAX_BYTES);
            }
            writeDigest(output, value.authorityCanonicalSha256());
        });
    }

    private static void writeAuthorityFact(DataOutputStream output, AuthorityFactV1 fact) throws IOException {
        writeBytes(output, CanonicalUtf8.fromString(fact.key()).bytes(), ExactMetadataTransactionStoreV1.MAX_KEY_BYTES);
        writeBytes(output, fact.metadataVersion().value(), ExactMetadataTransactionStoreV1.MAX_KEY_BYTES);
        writeDigest(output, fact.valueSha256());
    }

    private static AuthorityFactV1 readAuthorityFact(DataInputStream input) throws IOException {
        String key = CanonicalUtf8.fromBytes(readBytes(input, ExactMetadataTransactionStoreV1.MAX_KEY_BYTES)
                        .toByteArray())
                .value();
        MetadataVersion version = new MetadataVersion(readBytes(input, ExactMetadataTransactionStoreV1.MAX_KEY_BYTES));
        return new AuthorityFactV1(key, version, readDigest(input));
    }

    private static void writeCapability(DataOutputStream output, CapabilityBinding capability) throws IOException {
        output.writeLong(capability.generation());
        writeDigest(output, capability.evidenceSha256());
    }

    private static CapabilityBinding readCapability(DataInputStream input) throws IOException {
        return new CapabilityBinding(input.readLong(), readDigest(input));
    }

    private static void writeDigest(DataOutputStream output, Sha256Digest digest) throws IOException {
        output.write(digest.bytes().toByteArray());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        byte[] bytes = input.readNBytes(Sha256Digest.LENGTH);
        if (bytes.length != Sha256Digest.LENGTH) {
            throw new EOFException("truncated SHA-256 digest");
        }
        return Sha256Digest.copyOf(bytes);
    }

    private static void writeBytes(DataOutputStream output, CanonicalBytes bytes, int maximum) throws IOException {
        if (bytes.isEmpty() || bytes.length() > maximum) {
            throw new IllegalArgumentException("nested Pulsar authority bytes exceed their hard cap");
        }
        output.writeInt(bytes.length());
        output.write(bytes.toByteArray());
    }

    private static CanonicalBytes readBytes(DataInputStream input, int maximum) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum) {
            throw new IllegalArgumentException("nested Pulsar authority byte length exceeds its hard cap");
        }
        byte[] value = input.readNBytes(length);
        if (value.length != length) {
            throw new EOFException("truncated nested Pulsar authority bytes");
        }
        return CanonicalBytes.copyOf(value);
    }

    private static int boundedCount(int count, int maximum, String label) {
        if (count <= 0 || count > maximum) {
            throw new IllegalArgumentException(label + " count exceeds its hard cap");
        }
        return count;
    }

    private static <T> T enumValue(T[] values, int ordinal, String label) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IllegalArgumentException(label + " code is unknown");
        }
        return values[ordinal];
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writer.write(output);
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode Pulsar aggregate authority", exception);
        }
    }

    private static <T> T decode(CanonicalBytes encoded, Reader<T> reader) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded.toByteArray()))) {
            T value = reader.read(input);
            if (input.available() != 0) {
                throw new IllegalArgumentException("Pulsar aggregate authority has trailing bytes");
            }
            return value;
        } catch (EOFException exception) {
            throw new IllegalArgumentException("Pulsar aggregate authority is truncated", exception);
        } catch (IOException exception) {
            throw new IllegalArgumentException("failed to decode Pulsar aggregate authority", exception);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream input) throws IOException;
    }
}
