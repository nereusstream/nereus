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
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceMutationTicketV1;
import com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.AuthorityFactV1;
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
        REFERENCE_SCAN_FENCED_V1
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
            Sha256Digest authorityCanonicalSha256) {
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
