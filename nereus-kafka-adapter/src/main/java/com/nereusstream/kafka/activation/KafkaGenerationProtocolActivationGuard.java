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

package com.nereusstream.kafka.activation;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.core.capability.GenerationActivationProof;
import com.nereusstream.core.capability.GenerationActivationSubject;
import com.nereusstream.core.capability.GenerationOperation;
import com.nereusstream.core.capability.GenerationProtocolActivationGuard;
import com.nereusstream.core.capability.LiveStreamSubject;
import com.nereusstream.materialization.DirectMaterializationStreamAuthority;
import com.nereusstream.metadata.oxia.F4Keyspace;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.VersionedMaterializationStreamRegistration;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-native generation publication admission rooted in ACTIVE/readiness and a projection-free
 * stream registration.
 *
 * <p>Partition leadership is deliberately not inferred here. The compaction caller supplies its
 * partition-lock/KRaft guard separately to {@code GenerationCommitter.publish}; both authorities
 * are revalidated before the Generation COMMITTED CAS.
 */
public final class KafkaGenerationProtocolActivationGuard
        implements GenerationProtocolActivationGuard {
    private static final Checksum REFERENCE_DOMAIN_SHA256 =
            sha256("nereus-kafka-direct-stream-publication-domain-v1");

    private final String cluster;
    private final F4Keyspace keyspace;
    private final GenerationMetadataStore generations;
    private final KafkaStorageActivationVerifier activationVerifier;
    private final Clock clock;

    public KafkaGenerationProtocolActivationGuard(
            String cluster,
            GenerationMetadataStore generations,
            KafkaStorageActivationVerifier activationVerifier,
            Clock clock) {
        this.cluster = requireText(cluster, "cluster");
        this.keyspace = new F4Keyspace(this.cluster);
        this.generations = Objects.requireNonNull(generations, "generations");
        this.activationVerifier = Objects.requireNonNull(activationVerifier, "activationVerifier");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletableFuture<GenerationActivationProof> requireReady(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        LiveStreamSubject live;
        try {
            live = requireCombination(operation, subject, activateLiveProjectionIfAbsent);
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return loadRegistration(live)
                .thenCompose(
                        registration ->
                                activationVerifier
                                        .verifyCurrent()
                                        .thenApply(
                                                active ->
                                                        GenerationActivationProof.create(
                                                                operation,
                                                                live,
                                                                registration.metadataVersion(),
                                                                active.activation()
                                                                        .metadataVersion(),
                                                                active.readiness()
                                                                        .value()
                                                                        .readinessEpoch(),
                                                                REFERENCE_DOMAIN_SHA256,
                                                                true,
                                                                false,
                                                                Math.max(0, clock.millis()))))
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> revalidate(GenerationActivationProof proof) {
        GenerationActivationProof exact;
        LiveStreamSubject live;
        try {
            exact = Objects.requireNonNull(proof, "proof");
            live = requireCombination(exact.operation(), exact.subject(), false);
            if (!exact.referenceDomainSetSha256().equals(REFERENCE_DOMAIN_SHA256)
                    || !exact.publicationEnabled()
                    || exact.deletionEnabled()) {
                throw invariant("Kafka generation activation proof uses another capability domain");
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return loadRegistration(live)
                .thenCompose(
                        registration -> {
                            if (registration.metadataVersion()
                                    != exact.subjectValidationVersion()) {
                                return CompletableFuture.failedFuture(
                                        condition(
                                                "Kafka direct-stream registration changed after"
                                                    + " activation proof"));
                            }
                            return activationVerifier
                                    .verifyCurrent()
                                    .thenAccept(
                                            active -> {
                                                if (active.activation().metadataVersion()
                                                                != exact
                                                                        .clusterActivationMetadataVersion()
                                                        || active.readiness()
                                                                        .value()
                                                                        .readinessEpoch()
                                                                != exact
                                                                        .brokerCapabilityReadinessEpoch()) {
                                                    throw condition(
                                                            "Kafka ACTIVE/readiness authority"
                                                                + " changed after activation"
                                                                + " proof");
                                                }
                                            });
                        })
                .toCompletableFuture();
    }

    private CompletableFuture<VersionedMaterializationStreamRegistration> loadRegistration(
            LiveStreamSubject subject) {
        return generations
                .getStreamRegistration(cluster, subject.streamId())
                .thenApply(
                        optional -> {
                            VersionedMaterializationStreamRegistration registration =
                                    optional.orElseThrow(
                                            () ->
                                                    condition(
                                                            "Kafka direct-stream registration is"
                                                                + " absent"));
                            MaterializationStreamRegistrationRecord value = registration.value();
                            StorageProfile profile;
                            try {
                                profile =
                                        StorageProfile.valueOf(value.storageProfile()).canonical();
                            } catch (IllegalArgumentException failure) {
                                throw invariant(
                                        "Kafka direct-stream registration contains an unknown"
                                            + " profile",
                                        failure);
                            }
                            Checksum expected =
                                    DirectMaterializationStreamAuthority.identitySha256(
                                            subject.streamId(), profile);
                            if (!registration
                                            .key()
                                            .equals(
                                                    keyspace.materializationRegistryKey(
                                                            subject.streamId()))
                                    || !value.streamId().equals(subject.streamId().value())
                                    || !profile.objectMaterializationEnabled()
                                    || !value.projectionRef()
                                            .equals(
                                                    DirectMaterializationStreamAuthority
                                                            .encodedProjectionRef())
                                    || !value.projectionIdentitySha256().equals(expected.value())
                                    || !subject.streamIdentitySha256().equals(expected)) {
                                throw condition(
                                        "Kafka direct-stream registration no longer matches"
                                            + " publication authority");
                            }
                            return registration;
                        });
    }

    private static LiveStreamSubject requireCombination(
            GenerationOperation operation,
            GenerationActivationSubject subject,
            boolean activateLiveProjectionIfAbsent) {
        GenerationOperation exactOperation = Objects.requireNonNull(operation, "operation");
        GenerationActivationSubject exactSubject = Objects.requireNonNull(subject, "subject");
        if ((exactOperation
                        != GenerationOperation
                                .TOPIC_COMPACTED_PUBLISH
                        && exactOperation
                                != GenerationOperation
                                        .GENERATION_PUBLISH)
                || activateLiveProjectionIfAbsent
                || !(exactSubject instanceof LiveStreamSubject live)) {
            throw new IllegalArgumentException(
                    "Kafka generation guard admits only non-activating direct-stream generation publication");
        }
        return live;
    }

    private static Checksum sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of()
                            .formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8))));
        } catch (NoSuchAlgorithmException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static NereusException condition(String message) {
        return new NereusException(ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private static NereusException invariant(String message) {
        return invariant(message, null);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
