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
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.codec.Nta1CodecV1;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimStateV2;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Closed records stored in one incarnation-scoped Pulsar aggregate authority cell before retirement. */
public final class M5PulsarAggregateAuthorityRecordsV1 {
    public static final int MAX_REFERENCE_MUTATION_TICKETS = 256;
    public static final int MAX_AUTHORITY_BYTES = 1_048_576;

    private M5PulsarAggregateAuthorityRecordsV1() {}

    public enum PulsarAggregateAuthorityStateV1 {
        OPEN_V1,
        REFERENCE_SCAN_FENCED_V1,
        LOGICAL_TRIM_SCAN_FENCED_V2,
        LOGICAL_TRIM_SOURCE_SCAN_FENCED_V3,
        NATIVE_PULSAR_ONLY_INITIALIZING_V5
    }

    /** Fixed for an incarnation; absence retains all legacy protocol-retention obligations. */
    public enum PulsarRetentionPolicyV1 {
        NATIVE_PULSAR_ONLY_V1,
        NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2;

        public boolean excludesReference(ReferenceKindV1 kind) {
            return excludesKafkaReference(kind)
                    || this == NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2
                            && kind == ReferenceKindV1.PROJECTION_MIGRATION_OR_EXPORT;
        }

        public boolean excludesAccessProjection() {
            return this == NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2;
        }

        public boolean excludesKafkaReference(ReferenceKindV1 kind) {
            return switch (Objects.requireNonNull(kind, "kind")) {
                case KAFKA_GROUP_RETENTION,
                        KAFKA_PRODUCER_RECOVERY,
                        KAFKA_TRANSACTION_OR_ABORTED_RECOVERY,
                        KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY -> true;
                default -> false;
            };
        }
    }

    /** Durable scan fence bound to the exact permanent DELETED generation selector. */
    public record PulsarAggregateScanFenceV1(
            Sha256Digest targetIdentitySha256,
            Sha256Digest attemptIdSha256,
            Sha256Digest openAuthorityValueSha256,
            AuthorityFactV1 deletedSelectorAuthority) {
        public PulsarAggregateScanFenceV1 {
            M5RetentionRecordsV1.requireDigest(targetIdentitySha256, "targetIdentitySha256");
            M5RetentionRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
            M5RetentionRecordsV1.requireDigest(openAuthorityValueSha256, "openAuthorityValueSha256");
            Objects.requireNonNull(deletedSelectorAuthority, "deletedSelectorAuthority");
        }
    }

    /** Freezes source writers before their actual floor snapshot exists. */
    public record PulsarLogicalTrimSourceScanV3(Sha256Digest attemptIdSha256, AuthorityFactV1 openAuthority) {
        public PulsarLogicalTrimSourceScanV3 {
            M5RetentionRecordsV1.requireDigest(attemptIdSha256, "attemptIdSha256");
            Objects.requireNonNull(openAuthority, "openAuthority");
        }
    }

    /** Entire canonical value at the existing incarnation-scoped aggregate key before its final tombstone. */
    public record PulsarAggregateRetirementAuthorityV1(
            PulsarTopicIncarnationIdentity incarnation,
            TopicBindingId bindingId,
            long authorityGeneration,
            Optional<Sha256Digest> predecessorValueSha256,
            PulsarAggregateAuthorityStateV1 state,
            CanonicalBytes canonicalAggregateBytes,
            Sha256Digest originalAggregateSha256,
            Optional<PulsarAggregateScanFenceV1> scanFence,
            List<ReferenceMutationTicketV1> referenceMutationTickets,
            Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
            CapabilityBinding capability,
            Sha256Digest authorityCanonicalSha256,
            Optional<MetadataVersion> originalAggregateVersion,
            Optional<LogicalTrimStateV2> logicalTrim,
            Optional<PulsarLogicalTrimSourceScanV3> logicalTrimSourceScan,
            Optional<PulsarRetentionPolicyV1> retentionPolicy,
            Optional<Sha256Digest> firstNativeFactoryScope,
            Optional<M5PulsarAuditDomainV1> auditDomain,
            Optional<M5PulsarRetentionConfigurationV1> nativeRetention) {
        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion,
                Optional<LogicalTrimStateV2> logicalTrim,
                Optional<PulsarLogicalTrimSourceScanV3> logicalTrimSourceScan,
                Optional<PulsarRetentionPolicyV1> retentionPolicy,
                Optional<Sha256Digest> firstNativeFactoryScope,
                Optional<M5PulsarAuditDomainV1> auditDomain) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    logicalTrim,
                    logicalTrimSourceScan,
                    retentionPolicy,
                    firstNativeFactoryScope,
                    auditDomain,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion,
                Optional<LogicalTrimStateV2> logicalTrim,
                Optional<PulsarLogicalTrimSourceScanV3> logicalTrimSourceScan,
                Optional<PulsarRetentionPolicyV1> retentionPolicy,
                Optional<Sha256Digest> firstNativeFactoryScope) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    logicalTrim,
                    logicalTrimSourceScan,
                    retentionPolicy,
                    firstNativeFactoryScope,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion,
                Optional<LogicalTrimStateV2> logicalTrim,
                Optional<PulsarLogicalTrimSourceScanV3> logicalTrimSourceScan,
                Optional<PulsarRetentionPolicyV1> retentionPolicy) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    logicalTrim,
                    logicalTrimSourceScan,
                    retentionPolicy,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion,
                Optional<LogicalTrimStateV2> logicalTrim,
                Optional<PulsarLogicalTrimSourceScanV3> logicalTrimSourceScan) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    logicalTrim,
                    logicalTrimSourceScan,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion,
                Optional<LogicalTrimStateV2> logicalTrim) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    logicalTrim,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256,
                Optional<MetadataVersion> originalAggregateVersion) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    originalAggregateVersion,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1(
                PulsarTopicIncarnationIdentity incarnation,
                TopicBindingId bindingId,
                long authorityGeneration,
                Optional<Sha256Digest> predecessorValueSha256,
                PulsarAggregateAuthorityStateV1 state,
                CanonicalBytes canonicalAggregateBytes,
                Sha256Digest originalAggregateSha256,
                Optional<PulsarAggregateScanFenceV1> scanFence,
                List<ReferenceMutationTicketV1> referenceMutationTickets,
                Optional<ReferenceWriterEnrollmentV1> writerEnrollment,
                CapabilityBinding capability,
                Sha256Digest authorityCanonicalSha256) {
            this(
                    incarnation,
                    bindingId,
                    authorityGeneration,
                    predecessorValueSha256,
                    state,
                    canonicalAggregateBytes,
                    originalAggregateSha256,
                    scanFence,
                    referenceMutationTickets,
                    writerEnrollment,
                    capability,
                    authorityCanonicalSha256,
                    Optional.empty());
        }

        public PulsarAggregateRetirementAuthorityV1 {
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(bindingId, "bindingId");
            predecessorValueSha256 = Objects.requireNonNull(predecessorValueSha256, "predecessorValueSha256");
            predecessorValueSha256.ifPresent(
                    value -> M5RetentionRecordsV1.requireDigest(value, "predecessorValueSha256"));
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(canonicalAggregateBytes, "canonicalAggregateBytes");
            M5RetentionRecordsV1.requireDigest(originalAggregateSha256, "originalAggregateSha256");
            scanFence = Objects.requireNonNull(scanFence, "scanFence");
            referenceMutationTickets =
                    List.copyOf(Objects.requireNonNull(referenceMutationTickets, "referenceMutationTickets"));
            writerEnrollment = Objects.requireNonNull(writerEnrollment, "writerEnrollment");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(authorityCanonicalSha256, "authorityCanonicalSha256");
            originalAggregateVersion = Objects.requireNonNull(originalAggregateVersion, "originalAggregateVersion");
            logicalTrim = Objects.requireNonNull(logicalTrim, "logicalTrim");
            logicalTrimSourceScan = Objects.requireNonNull(logicalTrimSourceScan, "logicalTrimSourceScan");
            retentionPolicy = Objects.requireNonNull(retentionPolicy, "retentionPolicy");
            firstNativeFactoryScope = Objects.requireNonNull(firstNativeFactoryScope, "firstNativeFactoryScope");
            nativeRetention = Objects.requireNonNull(nativeRetention, "nativeRetention");
            if (nativeRetention.isPresent()) {
                var configured = nativeRetention.orElseThrow();
                if (retentionPolicy.isEmpty()
                        || firstNativeFactoryScope.isEmpty()
                        || !firstNativeFactoryScope.equals(Optional.of(configured.originalFactoryScope()))
                        || configured.generation() > authorityGeneration) {
                    throw new IllegalArgumentException("Native retention lacks original factory policy authority");
                }
            }
            auditDomain = Objects.requireNonNull(auditDomain, "auditDomain");
            if (auditDomain.isPresent()) {
                if (retentionPolicy.isEmpty() || writerEnrollment.isEmpty()) {
                    throw new IllegalArgumentException("audit domain requires its original Native policy/enrollment");
                }
                var nativeAggregate = Nta1CodecV1.decode(canonicalAggregateBytes);
                var expected = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity(
                        bindingId,
                        Sha256Digest.hash(
                                com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1.encode(incarnation)),
                        nativeAggregate.initialEpoch().storageEpochId().digest());
                for (var hold : auditDomain.orElseThrow().holds()) {
                    if (!hold.coverage().binding().equals(expected)
                            || hold.admissionGeneration() > authorityGeneration) {
                        throw new IllegalArgumentException("audit hold Binding or admission generation differs");
                    }
                }
                auditDomain.orElseThrow().encode();
                if (referenceMutationTickets.stream()
                        .anyMatch(ticket -> ticket.referenceKind() == ReferenceKindV1.AUDIT_GRACE)) {
                    throw new IllegalArgumentException(
                            "inline audit domain forbids unregistered external audit writes");
                }
            }

            firstNativeFactoryScope.ifPresent(
                    value -> M5RetentionRecordsV1.requireDigest(value, "firstNativeFactoryScope"));
            if (firstNativeFactoryScope.isPresent()
                    && (retentionPolicy.isEmpty()
                            || state == PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5)) {
                throw new IllegalArgumentException("first Native factory requires its activated explicit policy");
            }
            boolean initializing = state == PulsarAggregateAuthorityStateV1.NATIVE_PULSAR_ONLY_INITIALIZING_V5;
            if (initializing && retentionPolicy.isEmpty()) {
                throw new IllegalArgumentException("Native-only initialization requires its explicit policy");
            }
            if (retentionPolicy.isPresent()) {
                if (writerEnrollment.isEmpty()
                        || logicalTrim.isEmpty()
                        || initializing != originalAggregateVersion.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Native-only policy lacks closed enrollment or its original P1 version");
                }
                if (initializing
                        && (authorityGeneration != 1
                                || predecessorValueSha256.isPresent()
                                || !referenceMutationTickets.isEmpty()
                                || logicalTrim.orElseThrow().frontier().isPresent())) {
                    throw new IllegalArgumentException("Native-only initialization must precede every admitted writer");
                }
                var policy = retentionPolicy.orElseThrow();
                if (referenceMutationTickets.stream()
                        .anyMatch(ticket -> policy.excludesReference(ticket.referenceKind()))) {
                    throw new IllegalArgumentException("Native-only policy excludes this reference writer");
                }
            }
            if ((state == PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SOURCE_SCAN_FENCED_V3)
                    != logicalTrimSourceScan.isPresent()) {
                throw new IllegalArgumentException("Pulsar source scan state and fact disagree");
            }
            if (logicalTrimSourceScan.isPresent()) {
                var sourceScan = logicalTrimSourceScan.orElseThrow();
                if (logicalTrim.isEmpty()
                        || logicalTrim.orElseThrow().fence().isPresent()
                        || writerEnrollment.isEmpty()
                        || !referenceMutationTickets.isEmpty()
                        || !predecessorValueSha256
                                .filter(sourceScan.openAuthority().valueSha256()::equals)
                                .isPresent()) {
                    throw new IllegalArgumentException(
                            "Pulsar source scan requires its exact ticket-free OPEN predecessor");
                }
            }
            if ((state == PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2)
                    != logicalTrim.flatMap(LogicalTrimStateV2::fence).isPresent()) {
                throw new IllegalArgumentException("Pulsar logical trim state and fence disagree");
            }
            if (logicalTrim.flatMap(LogicalTrimStateV2::fence).isPresent()) {
                var trimFence = logicalTrim.flatMap(LogicalTrimStateV2::fence).orElseThrow();
                if (!predecessorValueSha256
                        .filter(trimFence.openAuthority().valueSha256()::equals)
                        .isPresent()) {
                    throw new IllegalArgumentException("logical trim fence differs from its exact OPEN predecessor");
                }
            }
            if (state == PulsarAggregateAuthorityStateV1.LOGICAL_TRIM_SCAN_FENCED_V2
                    && (!referenceMutationTickets.isEmpty() || writerEnrollment.isEmpty())) {
                throw new IllegalArgumentException("logical trim requires a ticket-free enrolled authority");
            }
            logicalTrim.flatMap(LogicalTrimStateV2::frontier).ifPresent(frontier -> {
                var nativeAggregate = Nta1CodecV1.decode(canonicalAggregateBytes);
                var expectedBinding =
                        new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity(
                                bindingId,
                                Sha256Digest.hash(com.nereusstream.domain.codec.TopicIncarnationIdentityCodecV1.encode(
                                        incarnation)),
                                nativeAggregate.initialEpoch().storageEpochId().digest());
                if (!frontier.identity().binding().equals(expectedBinding)
                        || !frontier.capability().equals(capability)
                        || frontier.pulsarContext().isEmpty()) {
                    throw new IllegalArgumentException("logical trim frontier differs from its Pulsar Binding");
                }
            });
            originalAggregateVersion.ifPresent(value -> {
                if (value.value().isEmpty() || value.value().length() > 4096) {
                    throw new IllegalArgumentException("original aggregate version exceeds its bound");
                }
            });
            if (authorityGeneration <= 0 || (authorityGeneration > 1 && predecessorValueSha256.isEmpty())) {
                throw new IllegalArgumentException("authority generation and predecessor presence disagree");
            }
            if (canonicalAggregateBytes.isEmpty() || canonicalAggregateBytes.length() > MAX_AUTHORITY_BYTES) {
                throw new IllegalArgumentException("canonical aggregate bytes exceed their hard cap");
            }
            TopicBindingAggregateV1 aggregate = Nta1CodecV1.decode(canonicalAggregateBytes);
            if (!(aggregate.binding().incarnationIdentity() instanceof PulsarTopicIncarnationIdentity pulsar)
                    || !pulsar.equals(incarnation)
                    || !aggregate.binding().bindingId().equals(bindingId)
                    || !Sha256Digest.hash(canonicalAggregateBytes).equals(originalAggregateSha256)) {
                throw new IllegalArgumentException(
                        "Pulsar authority identity differs from its canonical NTA1 aggregate");
            }
            if (referenceMutationTickets.size() > MAX_REFERENCE_MUTATION_TICKETS) {
                throw new IllegalArgumentException("Pulsar authority exceeds its ticket-count hard cap");
            }
            validateTickets(referenceMutationTickets, originalAggregateSha256, capability, writerEnrollment);
            if (writerEnrollment.isPresent()
                    && !writerEnrollment.orElseThrow().capability().equals(capability)) {
                throw new IllegalArgumentException("writer enrollment capability differs from Pulsar authority");
            }
            if ((state == PulsarAggregateAuthorityStateV1.REFERENCE_SCAN_FENCED_V1) != scanFence.isPresent()) {
                throw new IllegalArgumentException("Pulsar authority state and scan fence disagree");
            }
            if (scanFence.isPresent()) {
                PulsarAggregateScanFenceV1 fence = scanFence.orElseThrow();
                if (!fence.targetIdentitySha256().equals(originalAggregateSha256)
                        || !referenceMutationTickets.isEmpty()) {
                    throw new IllegalArgumentException("Pulsar scan fence lacks one ticket-free aggregate target");
                }
            }
        }
    }

    private static void validateTickets(
            List<ReferenceMutationTicketV1> tickets,
            Sha256Digest originalAggregateSha256,
            CapabilityBinding capability,
            Optional<ReferenceWriterEnrollmentV1> writerEnrollment) {
        List<ReferenceMutationTicketV1> sorted = tickets.stream()
                .sorted(java.util.Comparator.comparing((ReferenceMutationTicketV1 value) ->
                                value.targetIdentitySha256().toHex())
                        .thenComparing(ReferenceMutationTicketV1::referenceKind)
                        .thenComparing(value -> value.operationIdSha256().toHex()))
                .toList();
        if (!tickets.equals(sorted) || (!tickets.isEmpty() && writerEnrollment.isEmpty())) {
            throw new IllegalArgumentException("Pulsar reference tickets are not canonical or lack writer enrollment");
        }
        Set<Sha256Digest> operations = new HashSet<>();
        for (ReferenceMutationTicketV1 ticket : tickets) {
            if (ticket.targetKind() != ReferenceTargetKindV1.PULSAR_AGGREGATE
                    || !ticket.targetIdentitySha256().equals(originalAggregateSha256)
                    || !ticket.writerCapability().equals(capability)
                    || !operations.add(ticket.operationIdSha256())) {
                throw new IllegalArgumentException(
                        "Pulsar reference ticket lacks one unique admitted aggregate target");
            }
        }
    }
}
